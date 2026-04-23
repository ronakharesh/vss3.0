#!/usr/bin/env bash
# ============================================================
# 02-build-images.sh
# Builds the 5 custom Docker images required by the manifest
# and pushes them to YOUR_REGISTRY.
#
# Images:
#   1. vss-elasticsearch:3.1.0
#   2. vss-elastic-init:3.1.0
#   3. vss-broker-health-check:3.1.0
#   4. vss-perception-alerts:3.1.0
#   5. vss-kibana-init-alerts:3.1.0
# ============================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# ---- Load env ----
if [[ ! -f "$SCRIPT_DIR/.env" ]]; then
  echo "[ERROR] .env not found. Run 01-setup.sh first."
  exit 1
fi
source "$SCRIPT_DIR/.env"

: "${YOUR_REGISTRY:?YOUR_REGISTRY must be set in .env (e.g. docker.io/youruser)}"
TAG="3.1.0"

build_and_push() {
  local name="$1"
  local dockerfile="$2"
  local context="$3"
  shift 3
  local extra_args=("$@")

  local full_image="${YOUR_REGISTRY}/${name}:${TAG}"
  echo ""
  echo "══════════════════════════════════════════════"
  echo "[BUILD] ${full_image}"
  echo "        Dockerfile: ${dockerfile}"
  echo "        Context:    ${context}"
  echo "══════════════════════════════════════════════"

  docker build \
    -t "${full_image}" \
    -f "${dockerfile}" \
    "${extra_args[@]}" \
    "${context}"

  echo "[PUSH]  ${full_image}"
  docker push "${full_image}"
  echo "[OK]    ${full_image}"
}

# 1. Elasticsearch (custom config + plugins)
build_and_push \
  "vss-elasticsearch" \
  "${REPO_ROOT}/deployments/foundational/Dockerfiles/elasticsearch.Dockerfile" \
  "${REPO_ROOT}/deployments/foundational"

# 2. Elasticsearch init (ILM policy, index templates, ingest pipelines)
build_and_push \
  "vss-elastic-init" \
  "${REPO_ROOT}/deployments/foundational/Dockerfiles/elastic-init.Dockerfile" \
  "${REPO_ROOT}/deployments/foundational"

# 3. Broker health check (waits for Kafka/Redis to be ready)
build_and_push \
  "vss-broker-health-check" \
  "${REPO_ROOT}/deployments/foundational/Dockerfiles/kafka-health-check.Dockerfile" \
  "${REPO_ROOT}/deployments/foundational"

# 4. Perception (DeepStream + GDino/RT-DETR — the heaviest build, ~10 min)
build_and_push \
  "vss-perception-alerts" \
  "${REPO_ROOT}/deployments/developer-workflow/dev-profile-alerts/Dockerfiles/perception.Dockerfile" \
  "${REPO_ROOT}/deployments/developer-workflow/dev-profile-alerts" \
  --build-arg "PERCEPTION_IMAGE=nvcr.io/nvidia/vss-core/vss-rt-cv" \
  --build-arg "PERCEPTION_TAG=3.1.0"

# 5. Kibana dashboard init
build_and_push \
  "vss-kibana-init-alerts" \
  "${REPO_ROOT}/deployments/developer-workflow/dev-profile-alerts/Dockerfiles/kibana-dashboard.Dockerfile" \
  "${REPO_ROOT}/deployments/developer-workflow/dev-profile-alerts"

echo ""
echo "[DONE] All 5 images built and pushed to ${YOUR_REGISTRY}."
echo "       Next: run 03-download-models.sh"
