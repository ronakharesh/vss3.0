#!/usr/bin/env bash
# =============================================================================
# deploy-vss-3.1.3.sh — VSS Alert Verification Workflow — Single-Node K8s
#
# One-click deployment of the VSS 3.1 alert verification pipeline on a
# single-node k3s cluster with NVIDIA GPUs. Covers the full dev-profile-alerts
# workflow: DeepStream → Kafka → Behavior Analytics → VLM NIM verification →
# LLM NIM agent → VSS Agent UI.
#
# ─── Quick Start ──────────────────────────────────────────────────────────────
#
#   cp k8-deployment/.env.example k8-deployment/.env
#   # Fill in: NGC_CLI_API_KEY, YOUR_REGISTRY, NODE_IP
#   chmod +x deploy-vss-3.1.3.sh
#   ./deploy-vss-3.1.3.sh
#
# ─── All Options ──────────────────────────────────────────────────────────────
#
#   ./deploy-vss-3.1.3.sh
#     Full deploy: setup → build images → download models → apply manifest → wait
#     Idempotent — safe to re-run on a running cluster.
#
#   ./deploy-vss-3.1.3.sh --reset
#     Tear down the namespace then do a full fresh deploy.
#
#   ./deploy-vss-3.1.3.sh --teardown
#     Delete the namespace and all resources. Does not redeploy.
#
#   ./deploy-vss-3.1.3.sh --status
#     Show pod status and service URLs.
#
#   ./deploy-vss-3.1.3.sh --install-k3s
#     Install k3s + NVIDIA GPU Operator first, then do a full deploy.
#     Run this on a fresh machine before anything else.
#
#   ./deploy-vss-3.1.3.sh --build-only
#     Build and push the 5 custom images, skip deploy.
#
#   ./deploy-vss-3.1.3.sh --models-only
#     Download ONNX model files only, skip build and deploy.
#
# ─── .env Configuration (k8-deployment/.env) ─────────────────────────────────
#
#   Required:
#   NGC_CLI_API_KEY   NGC API key (nvapi- key) — image pulls + model downloads
#   YOUR_REGISTRY     Docker registry for custom images (e.g. localhost:5000)
#   NODE_IP           This machine's IP address (used for NodePort URLs)
#
#   GPU assignment (defaults shown — change to match your machine):
#   RT_CV_DEVICE_ID   GPU index for DeepStream (must NOT have MPS server)
#   VLM_DEVICE_ID     GPU index for VLM NIM (Cosmos-Reason2-8B)
#   LLM_DEVICE_ID     GPU hint for LLM NIM (device plugin overrides for TP=2)
#   NUM_SENSORS       Number of concurrent RTSP camera streams (default: 1)
#
#   Optional:
#   NAMESPACE         K8s namespace (default: vss-alerts)
#
# ─── GPU Requirements ─────────────────────────────────────────────────────────
#
#   Minimum 5 × L40S (or equivalent 40+ GB) GPUs:
#     1 × DeepStream perception  (RT_CV_DEVICE_ID — must be MPS-free)
#     1 × VLM NIM Cosmos-8B      (VLM_DEVICE_ID)
#     2 × LLM NIM Nemotron-9B    (auto-assigned by K8s device plugin, TP=2)
#     1 × spare
#
#   Confirmed working layout (this machine):
#     RT_CV_DEVICE_ID=3, VLM_DEVICE_ID=4, LLM auto-selects GPUs 1+2
#     GPU 0 has CUDA MPS server — DeepStream cannot share it (CUDA error 35)
#
# ─── Services Deployed ────────────────────────────────────────────────────────
#
#   Infrastructure : Kafka, Elasticsearch, Redis, Kibana, Logstash, Phoenix, PG
#   VST            : sensor, ingress, envoy, stream-processing, MCP, NVStreamer
#   Perception     : perception-alerts (DeepStream + GDINO + RT-DETR), sdr
#   Analytics      : behavior-analytics, video-analytics-api, alert-verification
#   AI Models      : llm-nim (Nemotron 9B × 2 GPU), vlm-nim (Cosmos-Reason 8B)
#   Agent          : vss-agent-mcp, vss-agent, vss-agent-ui
#
# ─── Custom Images (built from source) ───────────────────────────────────────
#
#   vss-elasticsearch:3.1.0      custom ES config + plugins
#   vss-elastic-init:3.1.0       ILM policies + index templates
#   vss-broker-health-check:3.1.0  Kafka readiness probe
#   vss-perception-alerts:3.1.0  DeepStream + GDINO + RT-DETR (heaviest, ~10min)
#   vss-kibana-init-alerts:3.1.0 Kibana dashboard importer
#
# ─── Known Issues & Fixes (baked in) ─────────────────────────────────────────
#
#   • DeepStream Kafka broker: image hardcodes localhost;9092 — manifest startup
#     command patches it to kafka;9092 before invoking ds-start.sh
#   • ONNX download: NGC CLI requires --org flag (fails silently). This script
#     uses the NGC REST API (curl) to download models directly.
#   • LLM NIM memory: Nemotron-9B is a Mamba hybrid model; SSM state cache needs
#     ~34 GiB on a single L40S. Fix: nvidia.com/gpu: 2 → NIM auto-selects TP=2
#   • perception-sdr Redis localhost:6379 errors are non-critical (SDR uses VST
#     REST API as stream source via WDM_INITIALIZE_FROM_VST=true)
#
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
K8_DIR="$SCRIPT_DIR/k8-deployment"
MANIFEST="$K8_DIR/k8s-alert-verification.yaml"
ENV_FILE="$K8_DIR/.env"

# ─── Colour helpers ───────────────────────────────────────────────────────────
info()    { echo -e "\033[0;32m[INFO]\033[0m  $*"; }
warn()    { echo -e "\033[0;33m[WARN]\033[0m  $*"; }
die()     { echo -e "\033[0;31m[ERROR]\033[0m $*" >&2; exit 1; }
section() { echo -e "\n\033[1;36m════════════════════════════════════════\033[0m"; \
            echo -e "\033[1;36m $*\033[0m"; \
            echo -e "\033[1;36m════════════════════════════════════════\033[0m"; }

# ─── Argument parsing ─────────────────────────────────────────────────────────
MODE="deploy"
SKIP_BUILD=0
for arg in "$@"; do
  case "$arg" in
    --reset)       MODE="reset" ;;
    --teardown)    MODE="teardown" ;;
    --status)      MODE="status" ;;
    --install-k3s) MODE="install-k3s" ;;
    --build-only)  MODE="build-only" ;;
    --models-only) MODE="models-only" ;;
    --skip-build)  SKIP_BUILD=1 ;;
    --help|-h)
      grep '^#' "$0" | grep -v '^#!/' | sed 's/^# \{0,2\}//'
      exit 0
      ;;
    *) die "Unknown argument: $arg  (run with --help for usage)" ;;
  esac
done

# ─── Load .env ────────────────────────────────────────────────────────────────
load_env() {
  if [[ ! -f "$ENV_FILE" ]]; then
    die ".env not found at $ENV_FILE\n       Copy k8-deployment/.env.example → k8-deployment/.env and fill in values."
  fi
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  : "${NGC_CLI_API_KEY:?NGC_CLI_API_KEY must be set in k8-deployment/.env}"
  : "${YOUR_REGISTRY:?YOUR_REGISTRY must be set in k8-deployment/.env}"
  : "${NODE_IP:?NODE_IP must be set in k8-deployment/.env}"
  NAMESPACE="${NAMESPACE:-vss-alerts}"
  RT_CV_DEVICE_ID="${RT_CV_DEVICE_ID:-3}"
  LLM_DEVICE_ID="${LLM_DEVICE_ID:-1}"
  VLM_DEVICE_ID="${VLM_DEVICE_ID:-2}"
  NUM_SENSORS="${NUM_SENSORS:-1}"
  MODELS_DIR="${SCRIPT_DIR}/deployments/data-dir/models"
}

# ─── Prerequisite check ───────────────────────────────────────────────────────
check_prereqs() {
  section "Checking Prerequisites"
  for cmd in kubectl docker curl python3; do
    command -v "$cmd" &>/dev/null \
      && info "$(command -v $cmd): $(${cmd} --version 2>&1 | head -1)" \
      || die "'$cmd' not found in PATH"
  done

  if ! command -v nvidia-smi &>/dev/null; then
    die "nvidia-smi not found. NVIDIA drivers must be installed on the host."
  fi
  info "NVIDIA driver: $(nvidia-smi --query-gpu=driver_version --format=csv,noheader | head -1)"

  if ! kubectl cluster-info &>/dev/null; then
    die "Cannot reach the Kubernetes cluster. Check KUBECONFIG."
  fi
  info "K8s cluster reachable: $(kubectl config current-context)"

  local gpu_count
  gpu_count=$(kubectl get nodes -o json 2>/dev/null \
    | python3 -c "
import json,sys
nodes=json.load(sys.stdin)['items']
print(sum(int(n['status'].get('capacity',{}).get('nvidia.com/gpu','0')) for n in nodes))
" 2>/dev/null || echo "0")

  if [[ "$gpu_count" -lt 5 ]]; then
    warn "Found $gpu_count allocatable GPU(s). This workflow needs 5 (perception + VLM + 2×LLM + spare)."
    warn "Continuing — adjust GPU assignments in .env if your machine differs."
  else
    info "$gpu_count GPU(s) available in cluster."
  fi

  info "GPU layout:"
  nvidia-smi --query-gpu=index,name,memory.total --format=csv,noheader \
    | while IFS=, read -r idx name mem; do
        printf "        GPU %s: %s (%s)\n" "$idx" "$(echo $name | xargs)" "$(echo $mem | xargs)"
      done
  echo ""
  info "Configured assignments:"
  info "  DeepStream perception → GPU $RT_CV_DEVICE_ID"
  info "  VLM NIM (Cosmos-8B)   → GPU $VLM_DEVICE_ID"
  info "  LLM NIM (Nemotron-9B) → 2 GPUs auto-assigned (TP=2)"
}

# ─── Install k3s + GPU Operator ───────────────────────────────────────────────
install_k3s() {
  section "Installing k3s + NVIDIA GPU Operator"

  info "Installing k3s (single-node, containerd runtime)..."
  curl -sfL https://get.k3s.io | INSTALL_K3S_EXEC="server \
    --disable traefik \
    --kubelet-arg='--container-runtime-endpoint=unix:///run/k3s/containerd/containerd.sock'" \
    sh -

  mkdir -p "$HOME/.kube"
  sudo cp /etc/rancher/k3s/k3s.yaml "$HOME/.kube/config"
  sudo chown "$USER":"$USER" "$HOME/.kube/config"
  export KUBECONFIG="$HOME/.kube/config"

  info "Waiting for k3s node to be Ready..."
  kubectl wait --for=condition=ready node --all --timeout=120s
  info "k3s node is Ready."

  if ! command -v helm &>/dev/null; then
    info "Installing Helm..."
    curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
  fi

  info "Installing NVIDIA GPU Operator via Helm..."
  helm repo add nvidia https://helm.ngc.nvidia.com/nvidia --force-update
  helm repo update
  kubectl create namespace gpu-operator 2>/dev/null || true
  helm upgrade --install gpu-operator nvidia/gpu-operator \
    --namespace gpu-operator \
    --set driver.enabled=false \
    --set toolkit.enabled=true \
    --wait --timeout=10m

  info "Waiting for GPU container toolkit daemonset..."
  kubectl -n gpu-operator rollout status daemonset/nvidia-container-toolkit-daemonset --timeout=600s

  sudo systemctl restart k3s
  sleep 10

  info "GPU Operator ready."
  kubectl get nodes -o custom-columns="NAME:.metadata.name,GPUs:.status.capacity.nvidia\.com/gpu"
}

# ─── Namespace + secrets ─────────────────────────────────────────────────────
setup_namespace() {
  section "Namespace & Secrets"

  kubectl create namespace "$NAMESPACE" 2>/dev/null \
    && info "Namespace '$NAMESPACE' created." \
    || info "Namespace '$NAMESPACE' already exists."

  kubectl -n "$NAMESPACE" create secret docker-registry nvcr-secret \
    --docker-server=nvcr.io \
    --docker-username='$oauthtoken' \
    --docker-password="${NGC_CLI_API_KEY}" \
    --dry-run=client -o yaml | kubectl apply -f -
  info "Image pull secret 'nvcr-secret' updated."

  kubectl -n "$NAMESPACE" create secret generic vss-secrets \
    --from-literal=NGC_CLI_API_KEY="${NGC_CLI_API_KEY}" \
    --from-literal=NVIDIA_API_KEY="${NGC_CLI_API_KEY}" \
    --dry-run=client -o yaml | kubectl apply -f -
  info "Secret 'vss-secrets' updated."

  info "Logging in to nvcr.io for image builds..."
  echo "${NGC_CLI_API_KEY}" | docker login nvcr.io \
    --username '$oauthtoken' --password-stdin
  info "nvcr.io login OK."
}

# ─── Build + push custom images ───────────────────────────────────────────────
build_images() {
  section "Building Custom Images"

  local repo_root="$SCRIPT_DIR"
  local tag="3.1.0"

  _build_push() {
    local name="$1" dockerfile="$2" context="$3"
    shift 3
    local img="${YOUR_REGISTRY}/${name}:${tag}"
    info "Building $img ..."
    docker build -t "$img" -f "$dockerfile" "$@" "$context"
    info "Pushing $img ..."
    docker push "$img"
    info "Done: $img"
  }

  _build_push "vss-elasticsearch" \
    "${repo_root}/deployments/foundational/Dockerfiles/elasticsearch.Dockerfile" \
    "${repo_root}/deployments/foundational"

  _build_push "vss-elastic-init" \
    "${repo_root}/deployments/foundational/Dockerfiles/elastic-init.Dockerfile" \
    "${repo_root}/deployments/foundational"

  _build_push "vss-broker-health-check" \
    "${repo_root}/deployments/foundational/Dockerfiles/kafka-health-check.Dockerfile" \
    "${repo_root}/deployments/foundational"

  info "Building vss-perception-alerts (DeepStream + GDINO — may take 10+ min)..."
  _build_push "vss-perception-alerts" \
    "${repo_root}/deployments/developer-workflow/dev-profile-alerts/Dockerfiles/perception.Dockerfile" \
    "${repo_root}/deployments/developer-workflow/dev-profile-alerts" \
    --build-arg "PERCEPTION_IMAGE=nvcr.io/nvidia/vss-core/vss-rt-cv" \
    --build-arg "PERCEPTION_TAG=3.1.0"

  _build_push "vss-kibana-init-alerts" \
    "${repo_root}/deployments/developer-workflow/dev-profile-alerts/Dockerfiles/kibana-dashboard.Dockerfile" \
    "${repo_root}/deployments/developer-workflow/dev-profile-alerts"

  info "All 5 custom images built and pushed to ${YOUR_REGISTRY}."
}

# ─── Download ONNX model files ────────────────────────────────────────────────
#
# Uses NGC REST API directly — NGC CLI v3.52.0+ requires --org when
# authenticated and silently fails, creating empty directories instead of files.
# The manifest mounts a hostPath at deployments/data-dir/models/ — models must
# be placed there, NOT in a PVC (the 03-download-models.sh job targets a
# different PVC and is unreliable).
#
download_models() {
  section "Downloading ONNX Model Files"

  local models_dir="$MODELS_DIR"
  mkdir -p "${models_dir}/gdino" "${models_dir}/rtdetr-its"

  # Copy resnet50_market1501.etlt from the image if missing
  # (the models volume mount shadows the file inside the container)
  local etlt_path="${models_dir}/rtdetr-its/resnet50_market1501.etlt"
  if [[ ! -f "$etlt_path" ]]; then
    info "Copying resnet50_market1501.etlt from image..."
    docker run --rm \
      -v "${models_dir}:/out" \
      --entrypoint bash "${YOUR_REGISTRY}/vss-perception-alerts:3.1.0" \
      -c "cp /opt/nvidia/deepstream/deepstream/sources/apps/sample_apps/metropolis_perception_app/models/rtdetr-its/resnet50_market1501.etlt /out/rtdetr-its/resnet50_market1501.etlt" \
      && info "resnet50_market1501.etlt copied." \
      || warn "Could not copy resnet50_market1501.etlt — perception may fail on first start."
  else
    info "resnet50_market1501.etlt already present."
  fi

  # Helper: download only if the destination is not already a real file
  _ngc_download() {
    local dest="$1" url="$2" label="$3"
    if [[ -f "$dest" && $(stat -c%s "$dest" 2>/dev/null || echo 0) -gt 1000000 ]]; then
      info "$label already present ($(du -sh "$dest" | cut -f1))."
      return
    fi
    # Remove if it exists as an empty directory (created by broken NGC CLI job)
    [[ -d "$dest" ]] && rm -rf "$dest" && warn "Removed empty directory at $dest"
    info "Downloading $label (~${4:-?} MB via NGC REST API)..."
    curl -fL -o "$dest" \
      -H "Authorization: ApiKey ${NGC_CLI_API_KEY}" \
      "$url" \
      && info "$label downloaded ($(du -sh "$dest" | cut -f1))." \
      || die "Failed to download $label from NGC. Check NGC_CLI_API_KEY and NGC access."
  }

  # GDINO — Grounding DINO object detection (~686 MB)
  _ngc_download \
    "${models_dir}/gdino/mgdino_mask_head_pruned_dynamic_batch.onnx" \
    "https://api.ngc.nvidia.com/v2/models/nvidia/tao/mask_grounding_dino/versions/mask_grounding_dino_swin_tiny_commercial_deployable_v2.1_wo_mask_arm/files/mgdino_mask_head_pruned_dynamic_batch.onnx" \
    "GDINO ONNX (mgdino_mask_head_pruned_dynamic_batch.onnx)" \
    "686"

  # RT-DETR — Traffic camera object detection / re-ID (~84 MB)
  _ngc_download \
    "${models_dir}/rtdetr-its/model_epoch_035.fp16.onnx" \
    "https://api.ngc.nvidia.com/v2/models/nvidia/tao/trafficcamnet_transformer_lite/versions/deployable_resnet50_v2.0/files/resnet50_trafficcamnet_rtdetr.fp16.onnx" \
    "RT-DETR ONNX (model_epoch_035.fp16.onnx)" \
    "84"

  info "Model files ready at: $models_dir"
  info "Note: TensorRT plan compilation (~4 min on L40S) happens inside the"
  info "      perception-alerts pod on first start and is cached on the PVC."
}

# ─── Apply manifest ───────────────────────────────────────────────────────────
apply_manifest() {
  section "Applying Kubernetes Manifest"

  [[ -f "$MANIFEST" ]] || die "Manifest not found: $MANIFEST"

  info "Substituting .env values into manifest..."
  sed \
    -e "s|CHANGE_ME_YOUR_NGC_CLI_API_KEY|${NGC_CLI_API_KEY}|g" \
    -e "s|CHANGE_ME_YOUR_REGISTRY|${YOUR_REGISTRY}|g" \
    -e "s|CHANGE_ME_HOST_IP|${NODE_IP}|g" \
    -e "s|CHANGE_ME_RT_CV_DEVICE_ID|${RT_CV_DEVICE_ID}|g" \
    -e "s|CHANGE_ME_LLM_DEVICE_ID|${LLM_DEVICE_ID}|g" \
    -e "s|CHANGE_ME_VLM_DEVICE_ID|${VLM_DEVICE_ID}|g" \
    -e "s|CHANGE_ME_NUM_SENSORS|${NUM_SENSORS}|g" \
    "$MANIFEST" | kubectl apply -f -

  info "Manifest applied to namespace: $NAMESPACE"
}

# ─── Wait for all pods ────────────────────────────────────────────────────────
wait_for_pods() {
  section "Waiting for Services to be Ready"

  _wait_deploy() {
    local name="$1" timeout="${2:-300}"
    echo -n "  [WAIT] deployment/$name ... "
    kubectl -n "$NAMESPACE" rollout status deployment/"$name" --timeout="${timeout}s" \
      && echo "ready" || { echo "TIMEOUT — check: kubectl -n $NAMESPACE logs deployment/$name"; return 1; }
  }

  _wait_job() {
    local name="$1" timeout="${2:-300}"
    echo -n "  [WAIT] job/$name ... "
    kubectl -n "$NAMESPACE" wait --for=condition=complete job/"$name" \
      --timeout="${timeout}s" \
      && echo "done" || { echo "TIMEOUT/FAILED — check: kubectl -n $NAMESPACE logs job/$name"; return 1; }
  }

  info "Phase 1 — Core Infrastructure"
  _wait_deploy kafka            120
  _wait_deploy elasticsearch    180
  _wait_deploy redis             60
  _wait_deploy phoenix           60
  _wait_deploy postgres          60

  info "Phase 2 — Init Jobs"
  _wait_job kafka-topic-init    300
  _wait_job elasticsearch-init  300

  info "Phase 3 — VST + Streaming"
  _wait_deploy kibana            300
  _wait_job    kibana-init       180
  _wait_deploy logstash          120
  _wait_deploy vst-stream-processing 120
  _wait_deploy vst-envoy          60
  _wait_deploy vst-sensor        120
  _wait_deploy vst-ingress        60
  _wait_deploy vst-mcp            60
  _wait_deploy nvstreamer        120

  info "Phase 4 — Perception + Analytics"
  # perception-alerts takes time on first boot: trtexec compiles GDINO ONNX
  # → TRT plan (~4 min on L40S). Plan is cached on PVC for subsequent starts.
  info "  perception-alerts first start compiles TRT plan (~4 min) — please wait..."
  _wait_deploy perception-alerts  600
  _wait_deploy perception-sdr     120
  _wait_deploy behavior-analytics 120
  _wait_deploy video-analytics-api 120
  _wait_deploy alert-verification  120

  info "Phase 5 — NIM Models (first boot: 20-30 min, warm cache: 1-3 min)"
  warn "LLM NIM startup probe allows 22 min. If this was a fresh image pull, it may take longer."
  _wait_deploy llm-nim 1500
  _wait_deploy vlm-nim 1500

  info "Phase 6 — Agent + UI"
  _wait_deploy vss-agent-mcp 120
  _wait_deploy vss-agent     300
  _wait_deploy vss-agent-ui  120
}

# ─── Print URLs ───────────────────────────────────────────────────────────────
print_urls() {
  section "Service Endpoints"
  echo ""
  echo "  VSS Agent UI (main)   →  http://${NODE_IP}:30301"
  echo "  VST Dashboard         →  http://${NODE_IP}:30888/vst/#/dashboard"
  echo "  NVStreamer UI          →  http://${NODE_IP}:31000/#/dashboard"
  echo "  Kibana                →  http://${NODE_IP}:30561/app/home#/"
  echo "  Phoenix (telemetry)   →  http://${NODE_IP}:30606/projects"
  echo ""
  echo "  Workflow:"
  echo "    1. VST Dashboard → add RTSP stream (or upload MP4 in NVStreamer)"
  echo "    2. Kibana → ITS Dashboard — watch mdx-raw-*, mdx-incidents-*"
  echo "    3. VSS Agent UI → Alerts → Verified Alerts"
  echo "    4. Chat: 'Generate a report for alert <id>'"
  echo ""
}

# ─── Status ───────────────────────────────────────────────────────────────────
show_status() {
  load_env
  section "Pod Status — namespace: $NAMESPACE"
  kubectl -n "$NAMESPACE" get pods \
    -o custom-columns="NAME:.metadata.name,READY:.status.containerStatuses[0].ready,STATUS:.status.phase,RESTARTS:.status.containerStatuses[0].restartCount,AGE:.metadata.creationTimestamp" \
    --no-headers 2>/dev/null \
    | awk '{print $1, $3, "restarts="$4}' \
    | column -t \
    || kubectl -n "$NAMESPACE" get pods 2>/dev/null \
    || warn "Namespace $NAMESPACE not found — has the deployment run yet?"
  echo ""
  print_urls
}

# ─── Teardown ─────────────────────────────────────────────────────────────────
teardown() {
  section "Teardown"
  if kubectl get namespace "$NAMESPACE" &>/dev/null; then
    info "Deleting namespace $NAMESPACE (all pods, PVCs, services)..."
    kubectl delete namespace "$NAMESPACE"
    info "Waiting for namespace to be fully removed..."
    kubectl wait --for=delete namespace/"$NAMESPACE" --timeout=120s 2>/dev/null || true
    info "Namespace $NAMESPACE deleted."
  else
    info "Namespace $NAMESPACE does not exist — nothing to delete."
  fi
  info "Note: NIM model caches in Docker volumes are preserved."
  info "      TRT plan on perception-storage PVC is deleted (will recompile on next start)."
}

# ─── Full deploy ──────────────────────────────────────────────────────────────
full_deploy() {
  check_prereqs
  setup_namespace
  if [[ "${SKIP_BUILD:-0}" == "1" ]]; then
    info "Skipping image build (--skip-build). Using existing images in ${YOUR_REGISTRY}."
  else
    build_images
  fi
  download_models
  apply_manifest
  wait_for_pods

  section "Deployment Complete"
  kubectl -n "$NAMESPACE" get pods --no-headers | awk '{print "  "$1, $2, $3}'
  echo ""
  print_urls
  info "All services are ready."
}

# ─── Main ─────────────────────────────────────────────────────────────────────

echo ""
echo "╔══════════════════════════════════════════════════════════════════╗"
echo "║  VSS 3.1 — Alert Verification Workflow — Single-Node K8s        ║"
echo "╚══════════════════════════════════════════════════════════════════╝"
echo "  Mode: $MODE"
echo ""

case "$MODE" in
  deploy)
    load_env
    full_deploy
    ;;
  reset)
    load_env
    teardown
    full_deploy
    ;;
  teardown)
    load_env
    teardown
    ;;
  status)
    show_status
    ;;
  install-k3s)
    install_k3s
    load_env
    full_deploy
    ;;
  build-only)
    load_env
    check_prereqs
    build_images
    ;;
  models-only)
    load_env
    download_models
    ;;
esac
