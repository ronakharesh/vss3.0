#!/usr/bin/env bash
# ============================================================
# 04-deploy.sh
# Substitutes .env values into the manifest and applies it
# to the cluster. Safe to re-run (kubectl apply is idempotent).
# ============================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [[ ! -f "$SCRIPT_DIR/.env" ]]; then
  echo "[ERROR] .env not found."
  exit 1
fi
source "$SCRIPT_DIR/.env"

: "${NGC_CLI_API_KEY:?}"
: "${YOUR_REGISTRY:?}"
: "${NODE_IP:?}"
: "${NAMESPACE:=vss-alerts}"
: "${RT_CV_DEVICE_ID:=3}"  # default 3; GPU 0 often has MPS server conflicts
: "${LLM_DEVICE_ID:=1}"
: "${VLM_DEVICE_ID:=2}"
: "${NUM_SENSORS:=1}"

MANIFEST="$SCRIPT_DIR/k8s-alert-verification.yaml"

echo "[INFO] Generating manifest from template..."

# Substitute all CHANGE_ME placeholders using sed
RENDERED=$(sed \
  -e "s|CHANGE_ME_YOUR_NGC_CLI_API_KEY|${NGC_CLI_API_KEY}|g" \
  -e "s|CHANGE_ME_YOUR_REGISTRY|${YOUR_REGISTRY}|g" \
  -e "s|CHANGE_ME_HOST_IP|${NODE_IP}|g" \
  -e "s|CHANGE_ME_RT_CV_DEVICE_ID|${RT_CV_DEVICE_ID}|g" \
  -e "s|CHANGE_ME_LLM_DEVICE_ID|${LLM_DEVICE_ID}|g" \
  -e "s|CHANGE_ME_VLM_DEVICE_ID|${VLM_DEVICE_ID}|g" \
  -e "s|CHANGE_ME_NUM_SENSORS|${NUM_SENSORS}|g" \
  "$MANIFEST")

echo "[INFO] Applying manifest to namespace: $NAMESPACE"
echo "$RENDERED" | kubectl apply -f -

echo ""
echo "[DONE] Manifest applied. Now run 05-wait-verify.sh to track readiness."
echo "       Or watch all pods: kubectl -n $NAMESPACE get pods -w"
