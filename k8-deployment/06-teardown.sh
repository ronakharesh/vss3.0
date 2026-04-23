#!/usr/bin/env bash
# ============================================================
# 06-teardown.sh
# Deletes the entire vss-alerts namespace (all pods, services,
# PVCs, jobs, configmaps, secrets).
# WARNING: This is destructive. PVs may be retained depending
# on your storage provisioner's reclaimPolicy.
# ============================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/.env" 2>/dev/null || true
NAMESPACE="${NAMESPACE:-vss-alerts}"

echo ""
echo "WARNING: This will delete namespace '$NAMESPACE' and all resources in it."
echo "         NIM model caches (PVCs) will also be deleted."
echo ""
read -rp "Type 'yes' to confirm: " CONFIRM
if [[ "$CONFIRM" != "yes" ]]; then
  echo "Aborted."
  exit 0
fi

echo "[INFO] Deleting namespace $NAMESPACE ..."
kubectl delete namespace "$NAMESPACE" --ignore-not-found=true

echo "[INFO] Namespace deletion initiated (may take 30-60 s for pods to terminate)."
echo "       Watch: kubectl get namespace $NAMESPACE"
