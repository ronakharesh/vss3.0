#!/usr/bin/env bash
# ============================================================
# 03-download-models.sh
# Downloads the two ONNX model files required by the
# DeepStream perception container into the models-data PVC.
#
# Models:
#   • rtdetr-its/model_epoch_035.fp16.onnx
#     (trafficcamnet RT-DETR, ~400 MB)
#   • gdino/mgdino_mask_head_pruned_dynamic_batch.onnx
#     (Grounding DINO, ~200 MB)
#
# Strategy: launch a temporary K8s Job that mounts the
# models-data PVC and runs NGC CLI inside the cluster.
# ============================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [[ ! -f "$SCRIPT_DIR/.env" ]]; then
  echo "[ERROR] .env not found."
  exit 1
fi
source "$SCRIPT_DIR/.env"

: "${NGC_CLI_API_KEY:?NGC_CLI_API_KEY must be set in .env}"
: "${NAMESPACE:=vss-alerts}"

echo "[INFO] Checking if models PVC exists..."
kubectl -n "$NAMESPACE" get pvc models-data &>/dev/null || {
  echo "[INFO] PVC models-data not found. Applying manifest first to create PVCs..."
  echo "       Run 04-deploy.sh first, or apply only the PVC section."
  exit 1
}

# Delete any previous download job
kubectl -n "$NAMESPACE" delete job models-download --ignore-not-found=true
echo "[INFO] Launching model download Job in namespace $NAMESPACE ..."

kubectl -n "$NAMESPACE" apply -f - <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: models-download
  namespace: ${NAMESPACE}
spec:
  backoffLimit: 2
  template:
    spec:
      restartPolicy: OnFailure
      containers:
        - name: downloader
          image: ubuntu:22.04
          command:
            - bash
            - -c
            - |
              set -e
              echo "[INFO] Installing dependencies..."
              apt-get update -qq && apt-get install -y -qq wget unzip curl

              echo "[INFO] Installing NGC CLI..."
              wget -q "https://api.ngc.nvidia.com/v2/resources/nvidia/ngc-apps/ngc_cli/versions/3.52.0/files/ngccli_linux.zip" \
                -O /tmp/ngccli.zip
              unzip -q /tmp/ngccli.zip -d /tmp/ngc
              chmod +x /tmp/ngc/ngc-cli/ngc
              export PATH="/tmp/ngc/ngc-cli:\$PATH"
              ngc --version

              mkdir -p /models/rtdetr-its /models/gdino
              cd /tmp

              echo "[INFO] Downloading trafficcamnet RT-DETR ONNX model (~400 MB)..."
              NGC_CLI_API_KEY="\${NGC_CLI_API_KEY}" ngc registry model download-version \
                nvidia/tao/trafficcamnet_transformer_lite:deployable_resnet50_v2.0 \
                --dest /tmp
              mv /tmp/trafficcamnet_transformer_lite_vdeployable_resnet50_v2.0/resnet50_trafficcamnet_rtdetr.fp16.onnx \
                /models/rtdetr-its/model_epoch_035.fp16.onnx
              rm -rf /tmp/trafficcamnet_transformer_lite_vdeployable_resnet50_v2.0

              echo "[INFO] Downloading Grounding DINO ONNX model (~200 MB)..."
              NGC_CLI_API_KEY="\${NGC_CLI_API_KEY}" ngc registry model download-version \
                nvidia/tao/mask_grounding_dino:mask_grounding_dino_swin_tiny_commercial_deployable_v2.1_wo_mask_arm \
                --dest /tmp
              mv /tmp/mask_grounding_dino_vmask_grounding_dino_swin_tiny_commercial_deployable_v2.1_wo_mask_arm/mgdino_mask_head_pruned_dynamic_batch.onnx \
                /models/gdino/mgdino_mask_head_pruned_dynamic_batch.onnx
              rm -rf /tmp/mask_grounding_dino_vmask_grounding_dino_swin_tiny_commercial_deployable_v2.1_wo_mask_arm

              chmod -R 777 /models
              echo "[DONE] Models on PVC:"
              find /models -type f -exec ls -lh {} \;
          env:
            - name: NGC_CLI_API_KEY
              valueFrom:
                secretKeyRef:
                  name: vss-secrets
                  key: NGC_CLI_API_KEY
          volumeMounts:
            - name: models
              mountPath: /models
          resources:
            requests:
              memory: "2Gi"
              cpu: "1"
            limits:
              memory: "4Gi"
              cpu: "2"
      volumes:
        - name: models
          persistentVolumeClaim:
            claimName: models-data
EOF

echo "[INFO] Waiting for model download to complete (may take 5-15 min)..."
kubectl -n "$NAMESPACE" wait --for=condition=complete job/models-download --timeout=900s \
  && echo "[DONE] Models downloaded successfully." \
  || {
    echo "[ERROR] Download job failed. Check logs:"
    echo "        kubectl -n $NAMESPACE logs job/models-download"
    exit 1
  }
