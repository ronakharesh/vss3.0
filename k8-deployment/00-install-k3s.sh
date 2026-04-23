#!/usr/bin/env bash
# ============================================================
# 00-install-k3s.sh
# Install k3s + NVIDIA GPU Operator on this machine.
# Run this ONCE on the host before anything else.
# Requires: root/sudo, NVIDIA drivers already installed.
# ============================================================
set -euo pipefail

# ---- Verify NVIDIA drivers are present ----
if ! command -v nvidia-smi &>/dev/null; then
  echo "[ERROR] nvidia-smi not found. Install NVIDIA drivers first."
  exit 1
fi
echo "[OK] NVIDIA driver: $(nvidia-smi --query-gpu=driver_version --format=csv,noheader | head -1)"

# ---- Install k3s (single-node, with NVIDIA container runtime) ----
echo "[INFO] Installing k3s..."
curl -sfL https://get.k3s.io | INSTALL_K3S_EXEC="server \
  --disable traefik \
  --kubelet-arg='--container-runtime-endpoint=unix:///run/k3s/containerd/containerd.sock'" \
  sh -

# Copy kubeconfig for the current user
mkdir -p "$HOME/.kube"
sudo cp /etc/rancher/k3s/k3s.yaml "$HOME/.kube/config"
sudo chown "$USER":"$USER" "$HOME/.kube/config"
export KUBECONFIG="$HOME/.kube/config"

echo "[INFO] Waiting for k3s node to be Ready..."
kubectl wait --for=condition=ready node --all --timeout=120s

# ---- Install Helm ----
if ! command -v helm &>/dev/null; then
  echo "[INFO] Installing Helm..."
  curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
fi

# ---- Install NVIDIA GPU Operator ----
echo "[INFO] Installing NVIDIA GPU Operator via Helm..."
helm repo add nvidia https://helm.ngc.nvidia.com/nvidia --force-update
helm repo update

kubectl create namespace gpu-operator 2>/dev/null || true

helm upgrade --install gpu-operator nvidia/gpu-operator \
  --namespace gpu-operator \
  --set driver.enabled=false \
  --set toolkit.enabled=true \
  --wait --timeout=10m

echo "[INFO] Waiting for GPU operator to be ready (this can take 5-10 min)..."
kubectl -n gpu-operator rollout status daemonset/nvidia-container-toolkit-daemonset --timeout=600s

# ---- Configure containerd runtime class for k3s ----
# k3s uses its own containerd; the GPU operator patches it automatically.
# Restart k3s to pick up the new runtime config:
sudo systemctl restart k3s
sleep 10

# ---- Verify GPU is visible to K8s ----
echo "[INFO] GPU node labels:"
kubectl get nodes -o json | python3 -c "
import json,sys
nodes=json.load(sys.stdin)['items']
for n in nodes:
    caps = n['status'].get('capacity',{})
    gpus = caps.get('nvidia.com/gpu','0')
    print(f\"  Node {n['metadata']['name']}: {gpus} GPU(s)\")
"

echo ""
echo "[DONE] k3s + GPU Operator installed."
echo "       KUBECONFIG is at: $HOME/.kube/config"
echo "       Run: kubectl get nodes"
