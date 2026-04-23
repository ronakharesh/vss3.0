#!/usr/bin/env bash
# ============================================================
# 01-setup.sh
# Creates the namespace, image-pull secret for nvcr.io,
# and verifies kubectl + GPU access.
# Run once before building images or deploying.
# ============================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ---- Load env ----
if [[ ! -f "$SCRIPT_DIR/.env" ]]; then
  echo "[ERROR] .env file not found. Copy .env.example → .env and fill in values."
  exit 1
fi
# shellcheck disable=SC1091
source "$SCRIPT_DIR/.env"

: "${NGC_CLI_API_KEY:?NGC_CLI_API_KEY must be set in .env}"
: "${NAMESPACE:=vss-alerts}"

# ---- Check prerequisites ----
for cmd in kubectl docker; do
  if ! command -v "$cmd" &>/dev/null; then
    echo "[ERROR] '$cmd' is not installed or not in PATH."
    exit 1
  fi
done
echo "[OK] kubectl: $(kubectl version --client --short 2>/dev/null | head -1)"
echo "[OK] docker:  $(docker --version)"

# ---- Verify cluster is reachable ----
if ! kubectl cluster-info &>/dev/null; then
  echo "[ERROR] Cannot reach the Kubernetes cluster. Check your KUBECONFIG."
  exit 1
fi
echo "[OK] Kubernetes cluster is reachable."

# ---- Verify GPU nodes ----
GPU_COUNT=$(kubectl get nodes -o json 2>/dev/null \
  | python3 -c "
import json,sys
nodes=json.load(sys.stdin)['items']
total=0
for n in nodes:
    total+=int(n['status'].get('capacity',{}).get('nvidia.com/gpu','0'))
print(total)
" 2>/dev/null || echo "0")

if [[ "$GPU_COUNT" -lt 3 ]]; then
  echo "[WARN] Found $GPU_COUNT allocatable GPU(s). This workflow needs 3."
  echo "       Ensure NVIDIA GPU Operator is installed (see 00-install-k3s.sh)."
else
  echo "[OK] $GPU_COUNT GPU(s) available in cluster."
fi

# ---- Create namespace ----
kubectl create namespace "$NAMESPACE" 2>/dev/null \
  && echo "[INFO] Namespace '$NAMESPACE' created." \
  || echo "[INFO] Namespace '$NAMESPACE' already exists."

# ---- Create nvcr.io image pull secret ----
kubectl -n "$NAMESPACE" create secret docker-registry nvcr-secret \
  --docker-server=nvcr.io \
  --docker-username='$oauthtoken' \
  --docker-password="${NGC_CLI_API_KEY}" \
  --dry-run=client -o yaml | kubectl apply -f -
echo "[INFO] Image pull secret 'nvcr-secret' created/updated."

# ---- Create vss-secrets (API keys) ----
kubectl -n "$NAMESPACE" create secret generic vss-secrets \
  --from-literal=NGC_CLI_API_KEY="${NGC_CLI_API_KEY}" \
  --from-literal=NVIDIA_API_KEY="${NVIDIA_API_KEY:-}" \
  --from-literal=OPENAI_API_KEY="${OPENAI_API_KEY:-}" \
  --from-literal=HF_TOKEN="${HF_TOKEN:-}" \
  --from-literal=POSTGRES_PASSWORD="vst" \
  --dry-run=client -o yaml | kubectl apply -f -
echo "[INFO] Secret 'vss-secrets' created/updated."

# ---- Docker login to nvcr.io (needed for image builds) ----
echo "[INFO] Logging in to nvcr.io..."
echo "${NGC_CLI_API_KEY}" | docker login nvcr.io \
  --username '$oauthtoken' \
  --password-stdin
echo "[OK] Docker login to nvcr.io succeeded."

echo ""
echo "[DONE] Setup complete. Namespace: $NAMESPACE"
echo "       Next: run 02-build-images.sh"
