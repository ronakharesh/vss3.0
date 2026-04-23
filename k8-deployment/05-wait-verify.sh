#!/usr/bin/env bash
# ============================================================
# 05-wait-verify.sh
# Waits for services to become ready in the correct order,
# then prints all service URLs.
# ============================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/.env" 2>/dev/null || true

NAMESPACE="${NAMESPACE:-vss-alerts}"
NODE_IP="${NODE_IP:-localhost}"

wait_deploy() {
  local name="$1"
  local timeout="${2:-300}"
  echo -n "[WAIT] $name ... "
  kubectl -n "$NAMESPACE" rollout status deployment/"$name" --timeout="${timeout}s" \
    && echo "ready" || { echo "TIMEOUT"; return 1; }
}

wait_job() {
  local name="$1"
  local timeout="${2:-300}"
  echo -n "[WAIT] job/$name ... "
  kubectl -n "$NAMESPACE" wait --for=condition=complete job/"$name" --timeout="${timeout}s" \
    && echo "done" || { echo "TIMEOUT/FAILED"; return 1; }
}

echo ""
echo "════════════════════════════════════════════"
echo " Phase 1: Core Infrastructure"
echo "════════════════════════════════════════════"
wait_deploy kafka         120
wait_deploy elasticsearch 180
wait_deploy redis         60
wait_deploy phoenix       60
wait_deploy postgres      60

echo ""
echo "════════════════════════════════════════════"
echo " Phase 2: Init Jobs"
echo "════════════════════════════════════════════"
wait_job kafka-topic-init    300
wait_job elasticsearch-init  300

echo ""
echo "════════════════════════════════════════════"
echo " Phase 3: VST + Streaming"
echo "════════════════════════════════════════════"
wait_deploy kibana            300
wait_job    kibana-init       180
wait_deploy logstash          120
wait_deploy vst-stream-processing 120
wait_deploy vst-envoy         60
wait_deploy vst-sensor        120
wait_deploy vst-ingress       60
wait_deploy vst-mcp           60
wait_deploy nvstreamer        120

echo ""
echo "════════════════════════════════════════════"
echo " Phase 4: Perception + Analytics"
echo "════════════════════════════════════════════"
wait_deploy perception-alerts  300
wait_deploy perception-sdr     120
wait_deploy behavior-analytics 120
wait_deploy video-analytics-api 120
wait_deploy alert-verification  120

echo ""
echo "════════════════════════════════════════════"
echo " Phase 5: NIM Models (may take 20+ min)"
echo "════════════════════════════════════════════"
echo "[INFO] Waiting up to 25 min for LLM NIM startup probe..."
wait_deploy llm-nim 1500
echo "[INFO] Waiting up to 25 min for VLM NIM startup probe..."
wait_deploy vlm-nim 1500

echo ""
echo "════════════════════════════════════════════"
echo " Phase 6: Agent + UI"
echo "════════════════════════════════════════════"
wait_deploy vss-agent-mcp 120
wait_deploy vss-agent     300
wait_deploy vss-agent-ui  120

echo ""
echo "════════════════════════════════════════════"
echo " Pod status summary"
echo "════════════════════════════════════════════"
kubectl -n "$NAMESPACE" get pods -o wide

echo ""
echo "════════════════════════════════════════════"
echo " Service Endpoints"
echo "════════════════════════════════════════════"
echo ""
echo "  VSS Agent UI (main UI)  →  http://${NODE_IP}:30301"
echo "  VST Dashboard           →  http://${NODE_IP}:30888/vst/#/dashboard"
echo "  NVStreamer UI            →  http://${NODE_IP}:31000/#/dashboard"
echo "  Kibana                  →  http://${NODE_IP}:30561/app/home#/"
echo "  Phoenix (telemetry)     →  http://${NODE_IP}:30606/projects"
echo ""
echo "[DONE] All services are ready."
