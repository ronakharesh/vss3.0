# VSS Alert Verification — Complete Operations & Architecture Reference

> Covers: fresh deployment, teardown, modular GPU configs, model swapping,
> alert customization, service communication internals, and every config knob.

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [Full Architecture & Communication Map](#2-full-architecture--communication-map)
3. [Data Flow — Step by Step](#3-data-flow--step-by-step)
4. [GPU Configuration Guide](#4-gpu-configuration-guide)
5. [Fresh Deployment Walkthrough](#5-fresh-deployment-walkthrough)
6. [Teardown & Cleanup](#6-teardown--cleanup)
7. [Model Configuration & Swapping](#7-model-configuration--swapping)
8. [Alert & Detection Customization](#8-alert--detection-customization)
9. [Service Reference](#9-service-reference)
10. [Kafka Topics & Elasticsearch Indices](#10-kafka-topics--elasticsearch-indices)
11. [Persistent Volumes & Storage](#11-persistent-volumes--storage)
12. [Secrets & ConfigMaps](#12-secrets--configmaps)
13. [Initialization Jobs](#13-initialization-jobs)
14. [Custom Images](#14-custom-images)
15. [K8s vs Docker Compose Differences](#15-k8s-vs-docker-compose-differences)
16. [Troubleshooting](#16-troubleshooting)
17. [Service URLs Quick Reference](#17-service-urls-quick-reference)

---

## 1. System Overview

This deployment runs an **AI-powered PPE violation detection system** on Kubernetes. A video feed is analyzed in real time — when a person is detected on a ladder without a hard hat or safety vest, the system creates an alert, verifies it with a VLM, stores it, and makes it queryable through an AI chat agent.

```
VIDEO IN → DETECT → ANALYZE → VERIFY (VLM) → STORE → QUERY (LLM Agent)
```

The system has four major layers:

| Layer | What it does | Key services |
|---|---|---|
| **Video** | Ingests RTSP streams, stores clips | NVStreamer, VST, perception-sdr |
| **Perception** | Detects objects in video frames | perception-alerts (DeepStream) |
| **Analytics** | Identifies incidents, verifies with VLM | behavior-analytics, alert-verification, vlm-nim |
| **Agent** | Answers questions, generates reports | vss-agent, llm-nim, vss-agent-mcp |

---

## 2. Full Architecture & Communication Map

### High-Level System Diagram

```
╔══════════════════════════════════════════════════════════════════════════════╗
║                           EXTERNAL ACCESS                                    ║
║                                                                              ║
║  :30301  VSS Agent UI    :30888  VST Dashboard    :31000  NVStreamer          ║
║  :30561  Kibana          :30606  Phoenix           :31554  RTSP               ║
╚══════════╤═══════════════════╤════════════════════╤═════════════════════════╝
           │                   │                    │
           ▼                   ▼                    ▼
╔══════════════════╗  ╔════════════════════╗  ╔══════════════════════════════╗
║   AGENT LAYER    ║  ║    VST LAYER       ║  ║    VIDEO SOURCE LAYER        ║
║                  ║  ║                    ║  ║                              ║
║  vss-agent-ui    ║  ║  vst-ingress:80    ║  ║  nvstreamer                  ║
║       │          ║  ║  (NodePort 30888)  ║  ║    HTTP :31000               ║
║       ▼          ║  ║    │               ║  ║    RTSP :31554               ║
║  vss-agent:8000  ║  ║    ├─ vst-sensor   ║  ║                              ║
║    │ │ │ │ │     ║  ║    │   :30000       ║  ╚════════════╤═════════════════╝
║    │ │ │ │ └─────╫──╫───▶│─ vst-mcp      ║               │ RTSP URL
║    │ │ │ └───────╫──╫───▶│   :8001        ║               │
║    │ │ └─────────╫──╫───▶│─ vst-stream-  ║               ▼
║    │ │       ES  ║  ║    │   processing   ║  ╔══════════════════════════════╗
║    │ └───────────╫──╫───▶└─ :9010         ║  ║   PERCEPTION PIPELINE        ║
║    │    :9200    ║  ║                     ║  ║                              ║
║    └────LLM──────╫──╫──┐  perception-sdr  ║  ║  perception-alerts           ║
║         nim      ║  ║  │    :4001          ║  ║  (DeepStream + GPU)          ║
║    VLM ──────────╫──╫──┤  watches VST ────╫──╫─▶ RT-DETR + GDINO + Re-ID   ║
║    nim:8000      ║  ║  │  routes streams   ║  ║    │                         ║
╚══════════════════╝  ╚══╪═══════════════════╝  ║    ▼                         ║
                         │                      ║  Kafka mdx-raw (protobuf)    ║
    phoenix:6006 ◀───────┘                      ╚═══════════╤══════════════════╝
    (telemetry)                                              │
                                                             ▼
╔════════════════════════════════════════════════════════════════════════════╗
║                         KAFKA  (kafka:9092)                                 ║
║                                                                             ║
║  mdx-raw ──────────────────▶ behavior-analytics ──▶ mdx-incidents           ║
║  mdx-raw ──────────────────▶ logstash ─────────────▶ elasticsearch          ║
║  mdx-incidents ────────────▶ alert-verification ───▶ mdx-vlm-incidents      ║
╚════════════════════════════════════════════════════════════════════════════╝
           │                          │
           ▼                          ▼
╔═════════════════╗         ╔══════════════════════════════════════════════╗
║  ELASTICSEARCH  ║         ║         ALERT VERIFICATION                   ║
║  :9200          ║         ║                                              ║
║  mdx-raw-*      ║         ║  alert-verification:9080                     ║
║  mdx-incidents-*║         ║    1. reads mdx-incidents from Kafka         ║
║  mdx-vlm-       ║◀────────║    2. checks redis:6379 for dedup            ║
║    incidents-*  ║         ║    3. fetches video clip from vst-ingress    ║
║  mdx-vlm-       ║         ║    4. samples frames at 4 fps               ║
║    alerts-*     ║         ║    5. calls vlm-nim:8000/v1 with frames     ║
╚═════════════════╝         ║    6. writes verdict to elasticsearch        ║
                            ╚══════════════════════════════════════════════╝
```

### Internal Service DNS Map

Every service calls every other service using K8s cluster DNS (`<service-name>.<namespace>.svc.cluster.local`, shortened to just `<service-name>` within the same namespace).

```
SERVICE               PORT    CALLED BY
──────────────────────────────────────────────────────────────────────
kafka                 9092    behavior-analytics, alert-verification,
                              kafka-topic-init, logstash, perception-alerts
elasticsearch         9200    logstash, kibana, video-analytics-api,
                              vss-agent, vss-agent-mcp, alert-verification,
                              elasticsearch-init, kibana-init
redis                 6379    alert-verification
postgres              5432    vst-sensor
phoenix               6006    vss-agent (OTLP traces)
vst-ingress           30888   alert-verification, vss-agent, vss-agent-ui,
                              perception-sdr, vst-mcp
vst-sensor            30000   (called via vst-ingress)
vst-stream-processing 9010    vst-sensor, perception-sdr
vst-mcp               8001    vss-agent
vst-envoy             9011    vst-stream-processing (gRPC)
nvstreamer            31000   (browser direct)
perception-sdr        4001    (called by VST webhook)
perception-alerts     9010    perception-sdr (stream add/remove)
behavior-analytics    —       (Kafka consumer only, no HTTP)
video-analytics-api   8081    vss-agent-mcp
alert-verification    9080    (health checks only)
llm-nim               8000    vss-agent
vlm-nim               8000    alert-verification, vss-agent
vss-agent-mcp         9901    vss-agent
vss-agent             8000    vss-agent-ui
```

---

## 3. Data Flow — Step by Step

### Step 1 — Video Source Setup

```
User uploads video to NVStreamer UI (port 31000)
    │
    ▼
NVStreamer transcodes → serves as RTSP at rtsp://<NODE_IP>:31554/<stream-id>

User adds RTSP URL to VST via VSS Agent UI → Video Management tab
    │
    ▼
VST Sensor registers stream → stores metadata in postgres:5432
    │
    ▼
VST Sensor calls vst-stream-processing:9010 to begin tracking

perception-sdr polls http://vst-ingress:30888/vst/api/v1/live/streams
    │
    ▼
perception-sdr sends HTTP POST to perception-alerts:9010/api/v1/stream/add
    {"rtsp_url": "rtsp://...", "sensor_id": "..."}
```

### Step 2 — Frame-Level Detection (continuous)

```
perception-alerts DeepStream pipeline:
┌─────────────────────────────────────────────────────────────────────┐
│ RTSP source → nvvidconv → nvinfer (RT-DETR) → nvinfer (GDINO)      │
│             → nvtracker (Re-ID) → nvmsgconv (protobuf) → nvmsgbroker│
└─────────────────────────────────────────────────────────────────────┘
    │
    ▼
Kafka topic: mdx-raw  (8 partitions, 4h retention)
Message format: protobuf with:
  - sensor_id, timestamp, frame_id
  - list of objects: {class, bbox [x,y,w,h], tracking_id, confidence}
  - zone annotations (which FOV zones the object is in)
```

### Step 3 — Behavior Analysis

```
behavior-analytics consumes mdx-raw
(consumer group: mdx-spatial-analytics-2d-app)

For each message, evaluates rules:
  fovCountViolationIncident:
    IF count(persons in ladder-zone) >= threshold (2)
    AND sustained for > expirationWindow (0.5s)
    THEN emit incident

Output → Kafka topic: mdx-incidents
Incident message contains:
  - sensor_id, start_time, end_time
  - incident_type: "FOV Count Violation"
  - object_count, zone_name
  - camera metadata

Also: logstash reads mdx-raw → indexes to elasticsearch mdx-raw-*
```

### Step 4 — VLM Alert Verification

```
alert-verification consumes mdx-incidents
(Kafka consumer group: kafka-incidents-dumper)

For each incident:

  1. DEDUP CHECK
     GET redis:6379 key "alert-bridge-input-stream:<incident_hash>"
     → If exists (5s TTL) → skip (duplicate)

  2. FETCH VIDEO SEGMENT
     GET http://vst-ingress:30888/api/v1/storage/file/path
         ?sensor_id=<id>&start=<epoch>&end=<epoch+10s>
     → Returns file path on vst-data PVC
     → Read file, extract 10s window around incident time

  3. SAMPLE FRAMES
     Extract frames at 4 fps from the 10s segment
     = ~40 frames per verification call

  4. VLM INFERENCE
     POST http://vlm-nim:8000/v1/chat/completions
     {
       "model": "nvidia/cosmos-reason2-8b",
       "max_tokens": 4096,
       "messages": [{
         "role": "system",
         "content": "You are a helpful assistant."
       }, {
         "role": "user",
         "content": [
           {"type": "text",
            "text": "Is anyone on the ladder without a hardhat and safety vest? Answer yes or no."},
           {"type": "image_url", "image_url": {"url": "data:image/jpeg;base64,..."}},
           ... (40 frames)
         ]
       }]
     }

  5. WRITE VERDICT
     POST http://elasticsearch:9200/mdx-vlm-incidents/_doc
     {
       "sensor_id": ..., "incident_type": "Ladder PPE Violation",
       "verdict": "yes", "timestamp": ..., "clip_path": ...
     }
     Also writes to mdx-vlm-alerts

  6. PROTECT VERDICT
     SET redis:6379 "alert-bridge-input-stream:<hash>" TTL=600s
     (prevents re-verification of same confirmed incident for 10 min)
```

### Step 5 — Agent Query

```
User types in chat: "Show me the last 5 PPE violations"
    │
    ▼
vss-agent-ui → POST http://vss-agent:8000/chat
    │
    ▼
vss-agent ReAct loop (max 15 iterations):

  THOUGHT: "I need to query incidents from Elasticsearch"
  ACTION: call tool get_incidents via MCP

  POST http://vss-agent-mcp:9901/mcp
  {"tool": "get_incidents", "params": {"limit": 5, "verified": true}}
      │
      ▼
  vss-agent-mcp → GET http://elasticsearch:9200/mdx-vlm-incidents/_search
  {"query": {"bool": {"filter": [{"term": {"verified": true}}]}}, "size": 5}
      │
      ▼
  Returns incident list

  THOUGHT: "I have the incidents, format a response"

  POST http://llm-nim:8000/v1/chat/completions
  {"model": "nvidia/nvidia-nemotron-nano-9b-v2", "messages": [...context...]}
      │
      ▼
  LLM generates natural language response

  All calls traced → phoenix:6006 (OpenTelemetry)
    │
    ▼
vss-agent responds to vss-agent-ui with formatted answer
```

---

## 4. GPU Configuration Guide

### Understanding GPU Requirements

| Workload | Min VRAM | Notes |
|---|---|---|
| DeepStream perception | 4–8 GiB | RT-DETR + GDINO + TensorRT engines |
| VLM NIM (cosmos-reason2-8b) | ~34 GiB | Fits on single L40S; cannot share |
| LLM NIM (nemotron-nano-9b-v2) | ~18 GiB × 2 GPUs | Mamba hybrid — needs TP=2, cannot fit on 1×46GiB GPU |

### Configuration Profiles

---

#### Profile A: 4 GPUs (Recommended Minimum for Full Stack)

Best for: L40S, A100, H100 (40 GiB+ VRAM per GPU)

```
GPU 0  ── DeepStream perception    (RT_CV_DEVICE_ID=0)
GPU 1  ── VLM NIM cosmos-8b       (VLM_DEVICE_ID=1)
GPU 2  ──┐
GPU 3  ──┘ LLM NIM nemotron-9b    (nvidia.com/gpu: 2, TP=2)
```

`.env` settings:
```bash
RT_CV_DEVICE_ID=0
VLM_DEVICE_ID=1
LLM_DEVICE_ID=2      # informational only — device plugin assigns the 2 GPUs
NUM_SENSORS=1
```

**Important**: GPU 0 must NOT have a CUDA MPS server running. On a fresh machine this is fine. If GPU 0 has MPS (check with `nvidia-smi`), bump all IDs up by 1 and use 5 GPUs.

---

#### Profile B: 8 GPUs — This Machine (GPU 0 has MPS)

```
GPU 0  ── MPS server (other workloads) — AVOID
GPU 1  ──┐
GPU 2  ──┘ LLM NIM nemotron-9b    (device plugin auto-picks 2 free GPUs)
GPU 3  ── DeepStream perception   (RT_CV_DEVICE_ID=3)
GPU 4  ── VLM NIM cosmos-8b      (VLM_DEVICE_ID=4)
GPU 5–7 ── idle
```

`.env` settings (current):
```bash
RT_CV_DEVICE_ID=3
VLM_DEVICE_ID=4
LLM_DEVICE_ID=1      # ignored by device plugin for 2-GPU request
NUM_SENSORS=1
```

---

#### Profile C: 2 GPUs — Inference Only (No Perception)

Use this if you have only 2 GPUs and want to run the agent + verification pipeline against pre-existing data.

```
GPU 0  ── VLM NIM cosmos-8b      (VLM_DEVICE_ID=0)
GPU 1  ──┐
         └ (LLM NIM needs 2 GPUs — only works if you have 2×46GiB)
```

To disable perception with 2 GPUs:

1. Set `nvidia.com/gpu: 0` for `perception-alerts` and remove `runtimeClassName: nvidia`
2. Remove `perception-sdr` deployment (not needed without live perception)
3. VLM on GPU 0, LLM on GPU 0+1 with TP=2

Alternatively, use cloud LLM (skip LLM NIM entirely — see [Model Configuration](#7-model-configuration--swapping)).

---

#### Profile D: 6+ GPUs — Multiple Sensors

Run 2 simultaneous camera streams:

```
GPU 0  ──┐
GPU 1  ──┘ LLM NIM  (TP=2)
GPU 2  ── VLM NIM
GPU 3  ── DeepStream sensor 1  (RT_CV_DEVICE_ID=3)
GPU 4  ── DeepStream sensor 2  (second perception pod)
GPU 5  ── spare
```

`.env` changes:
```bash
NUM_SENSORS=2
```

To add a second perception pod, duplicate the `perception-alerts` Deployment in the manifest, rename it `perception-alerts-2`, change `NVIDIA_VISIBLE_DEVICES` to GPU 4, and give it its own PVC.

---

### Changing GPU Assignment

**Step 1** — Edit `.env`:
```bash
RT_CV_DEVICE_ID=<gpu_index>   # DeepStream
VLM_DEVICE_ID=<gpu_index>     # VLM NIM
# LLM NIM: change nvidia.com/gpu: 2 to 1 only if using a different model
```

**Step 2** — Redeploy:
```bash
bash 04-deploy.sh
kubectl -n vss-alerts rollout restart deployment/perception-alerts
kubectl -n vss-alerts rollout restart deployment/vlm-nim
```

**Note on LLM NIM GPU**: `LLM_DEVICE_ID` in `.env` sets `NVIDIA_VISIBLE_DEVICES` in the pod spec, but the K8s NVIDIA device plugin **overrides this** when `nvidia.com/gpu: 2` is requested. The device plugin picks any 2 free GPUs from its pool. To pin LLM to specific GPUs you would need to use CUDA device ordinals or node labels — for most cases, just ensure 2 GPUs are free.

---

## 5. Fresh Deployment Walkthrough

This section walks through deploying from an empty machine to a fully running system.

### Prerequisites Checklist

```bash
# Verify NVIDIA drivers
nvidia-smi                     # must show GPU list
# Verify Docker
docker info                    # must be running
# Verify enough disk
df -h /var/lib/docker          # need 150+ GiB free for NIM caches + images
df -h /home/shadeform          # need 10+ GiB for model files
# Check GPU count and VRAM
nvidia-smi --query-gpu=index,name,memory.total --format=csv
```

Minimum requirements:
- 4× NVIDIA L40S (or equivalent 40+ GiB VRAM GPUs) for full stack
- 2× for inference-only (no live perception)
- Ubuntu 20.04/22.04 with NVIDIA drivers ≥ 525
- Docker 24+, 200 GiB free disk

---

### Phase 0 — Clone and Configure

```bash
cd /home/shadeform
git clone <repo-url> video-search-and-summarization
cd video-search-and-summarization/k8-deployment

# Create your environment file
cp .env.example .env
```

Edit `.env` — every value matters:

```bash
# Your NGC API key (from https://org.ngc.nvidia.com/setup/api-key)
NGC_CLI_API_KEY=nvapi-XXXXXXXXXX

# Your local Docker registry (k3s will pull images from here)
# For single-node: use localhost:5000 (start a local registry)
YOUR_REGISTRY=localhost:5000

# The IP address of THIS machine (used for NodePort URLs and reports)
NODE_IP=<your-machine-ip>       # e.g., 172.19.4.52
                                # NOT 127.0.0.1 — must be reachable from browser

# Namespace (leave as-is)
NAMESPACE=vss-alerts

# GPU assignments — check nvidia-smi first
# DeepStream CANNOT share GPU with CUDA MPS server
RT_CV_DEVICE_ID=0    # or 3 if GPU 0 has MPS
LLM_DEVICE_ID=1      # informational; device plugin assigns actual GPUs
VLM_DEVICE_ID=2      # or 4 on this machine
NUM_SENSORS=1        # number of concurrent camera streams
```

If using `localhost:5000` as registry, start it now:

```bash
docker run -d -p 5000:5000 --name local-registry \
  --restart=always \
  -v /var/lib/docker/registry:/var/lib/registry \
  registry:2
```

---

### Phase 1 — Install Kubernetes (k3s + GPU Operator)

Run once per machine. Skip if k3s is already installed.

```bash
bash 00-install-k3s.sh
```

This script:
1. Downloads and installs k3s (lightweight Kubernetes)
2. Installs Helm
3. Deploys the NVIDIA GPU Operator (device plugin + container runtime)
4. Configures the `nvidia` RuntimeClass for GPU pods

Verify it worked:
```bash
kubectl get nodes
# Should show:   Ready   master
kubectl get nodes -o custom-columns="NAME:.metadata.name,GPUs:.status.capacity.nvidia\.com/gpu"
# Should show GPU count
kubectl -n gpu-operator get pods
# All pods should be Running
```

---

### Phase 2 — Create Namespace, Secrets, Pull Secrets

```bash
bash 01-setup.sh
```

This creates:
- Namespace `vss-alerts`
- `nvcr-secret` — image pull secret for `nvcr.io` (uses your NGC key)
- `vss-secrets` — secret containing all API keys used by running services

Verify:
```bash
kubectl get ns vss-alerts
kubectl -n vss-alerts get secrets
# Should show: nvcr-secret, vss-secrets
```

---

### Phase 3 — Build Custom Images

Five images must be built from the source repo before deploying:

```bash
bash 02-build-images.sh
```

Images built and pushed to `YOUR_REGISTRY`:

| Image | Build time | Size |
|---|---|---|
| `vss-elasticsearch:3.1.0` | ~2 min | ~1.5 GiB |
| `vss-elastic-init:3.1.0` | ~1 min | ~200 MiB |
| `vss-broker-health-check:3.1.0` | ~1 min | ~100 MiB |
| `vss-perception-alerts:3.1.0` | ~5 min | ~8 GiB (pulls vss-rt-cv base) |
| `vss-kibana-init-alerts:3.1.0` | ~1 min | ~200 MiB |

The perception image build pulls `nvcr.io/nvidia/vss-core/vss-rt-cv:3.1.0` from NGC — this is the DeepStream + metropolis app base image. `01-setup.sh` handles the NGC docker login.

Verify images are in your registry:
```bash
curl http://localhost:5000/v2/_catalog
# Should list all 5 images
```

---

### Phase 4 — Deploy the Manifest

```bash
bash 04-deploy.sh
```

This substitutes all `CHANGE_ME_*` placeholders in `k8s-alert-verification.yaml` using `sed` and applies with `kubectl apply -f -`.

What gets created (in order):
1. Namespace, Secrets (if not already present)
2. All 10 PersistentVolumeClaims
3. All 7 ConfigMaps
4. All ~24 Deployments and their Services
5. 3 init Jobs (kafka-topic-init, elasticsearch-init, kibana-init)

Watch pods start:
```bash
kubectl -n vss-alerts get pods -w
```

Expected sequence:
```
# First ~30s: infrastructure comes up
kafka             0/1  ContainerCreating → Running
elasticsearch     0/1  ContainerCreating → Running
redis             0/1  ContainerCreating → Running
postgres          0/1  ContainerCreating → Running
phoenix           0/1  ContainerCreating → Running

# ~60s: init jobs fire
kafka-topic-init    0/1  Init → Running → Completed
elasticsearch-init  0/1  Init → Running → Completed

# ~2-5 min: rest of services
kibana            0/1  Running → 1/1 Running
kibana-init       0/1  Init → Running → Completed
logstash          ...
vst-*             ...
nvstreamer        ...
perception-*      ...

# ~5-30 min: NIM models (longest)
llm-nim           0/1  Running (startup probe, loading weights)
vlm-nim           0/1  Running (startup probe, loading weights)

# After NIMs are Ready:
vss-agent-mcp     0/1 → 1/1
vss-agent         0/1 → 1/1  (readiness probe has 4 min grace period)
vss-agent-ui      0/1 → 1/1
```

---

### Phase 5 — Download Perception Models

```bash
bash 03-download-models.sh
```

Downloads ONNX model files to `deployments/data-dir/models/` (hostPath):
- `rtdetr-its/model_epoch_035.fp16.onnx` — RT-DETR detector (~400 MB)
- `gdino/mgdino_mask_head_pruned_dynamic_batch.onnx` — Grounding DINO (~200 MB)

If the Re-ID model is missing (perception crashes with `resnet50_market1501.etlt: No such file`):
```bash
docker run --rm \
  -v /home/shadeform/video-search-and-summarization/deployments/data-dir/models:/out \
  --entrypoint bash localhost:5000/vss-perception-alerts:3.1.0 \
  -c "cp /opt/nvidia/deepstream/deepstream/sources/apps/sample_apps/metropolis_perception_app/models/rtdetr-its/resnet50_market1501.etlt /out/rtdetr-its/ && echo done"
```

---

### Phase 6 — Wait for Everything to Be Ready

```bash
bash 05-wait-verify.sh
```

Or check manually:
```bash
# Quick status
kubectl -n vss-alerts get pods --no-headers | awk '{print $2, $3, $1}' | sort | column -t

# Watch NIM startup (can take 1-25 min)
kubectl -n vss-alerts logs -f deployment/llm-nim | grep -E "Loading|GPU blocks|routes|OOM|Error"
kubectl -n vss-alerts logs -f deployment/vlm-nim | grep -E "Loading|GPU blocks|routes|OOM|Error"
```

The system is ready when:
```bash
kubectl -n vss-alerts get pods --no-headers | grep -v Completed | grep -v "1/1"
# Should return nothing (all 1/1 Running or Completed)
```

---

### Phase 7 — Load a Video and Test

```bash
# 1. Open NVStreamer and upload a test video
open http://172.19.4.52:31000/#/dashboard
# Upload: sample-warehouse-ladder.mp4
# Start streaming → copy the RTSP URL shown

# 2. Open VSS Agent UI
open http://172.19.4.52:30301
# Go to: Video Management → + Add RTSP
# Paste the RTSP URL from NVStreamer
# Wait ~30s for stream to register

# 3. Watch Kibana for detections
open http://172.19.4.52:30561/app/home#/
# Open: ITS Dashboard
# After ~1-2 min of video, should see mdx-raw-* populating

# 4. Chat with the agent
open http://172.19.4.52:30301
# Click Chat tab
# Ask: "Are there any PPE violations?"
# Or: "Generate a report for the last hour"
```

---

## 6. Teardown & Cleanup

### Standard Teardown (preserves NIM caches)

```bash
bash 06-teardown.sh
```

This deletes:
- All pods, services, deployments
- All PVCs (and their data — Kafka, ES, Redis, video clips, etc.)
- The `vss-alerts` namespace

This does **NOT** delete:
- NIM model caches in Docker volumes (`/var/lib/docker/volumes/mdx_*_cache/`)
- The local Docker registry and its images
- Pulled nvcr.io images cached by containerd

NIM caches are kept intentionally — they take 1-3 hours to re-download and the models rarely change.

### Full Teardown (delete everything including caches)

```bash
# Delete the K8s namespace
bash 06-teardown.sh

# Delete NIM model caches (will require re-download next deploy)
docker volume rm mdx_nvidia_nemotron_nano_9b_v2_cache
docker volume rm mdx_cosmos_reason2_8b_cache

# Remove custom images from local registry
docker rmi localhost:5000/vss-perception-alerts:3.1.0
docker rmi localhost:5000/vss-elasticsearch:3.1.0
docker rmi localhost:5000/vss-elastic-init:3.1.0
docker rmi localhost:5000/vss-broker-health-check:3.1.0
docker rmi localhost:5000/vss-kibana-init-alerts:3.1.0

# Remove the local registry itself
docker rm -f local-registry

# Uninstall k3s (if desired)
/usr/local/bin/k3s-uninstall.sh
```

### Partial Teardown — Reset Data Only

Reset all alert data and re-initialize Elasticsearch/Kafka without restarting services:

```bash
# Delete and recreate Elasticsearch data
kubectl -n vss-alerts delete pvc elasticsearch-data
kubectl -n vss-alerts rollout restart deployment/elasticsearch
kubectl -n vss-alerts delete job elasticsearch-init
bash 04-deploy.sh   # recreates PVC + job

# Reset Kafka topics (delete all messages)
kubectl -n vss-alerts delete pvc kafka-data
kubectl -n vss-alerts rollout restart deployment/kafka
kubectl -n vss-alerts delete job kafka-topic-init
bash 04-deploy.sh

# Reset Kibana dashboards
kubectl -n vss-alerts delete job kibana-init
bash 04-deploy.sh
```

### Re-deploy a Single Service

```bash
# After changing a ConfigMap or env var:
bash 04-deploy.sh   # applies changes idempotently
kubectl -n vss-alerts rollout restart deployment/<name>

# Examples:
kubectl -n vss-alerts rollout restart deployment/alert-verification
kubectl -n vss-alerts rollout restart deployment/vss-agent
kubectl -n vss-alerts rollout restart deployment/behavior-analytics
```

---

## 7. Model Configuration & Swapping

### LLM Model (used by vss-agent for reasoning)

**Current**: `nvidia/nvidia-nemotron-nano-9b-v2`

The LLM model is configured in two places:

**1. k8s-alert-verification.yaml — llm-nim Deployment** (the NIM container itself):
```yaml
containers:
  - name: llm-nim
    image: nvcr.io/nim/nvidia/nvidia-nemotron-nano-9b-v2:1  # ← change image here
```

**2. k8s-alert-verification.yaml — vss-agent Deployment** (tells agent which model to call):
```yaml
env:
  - name: LLM_NAME
    value: "nvidia/nvidia-nemotron-nano-9b-v2"      # ← match model name here
  - name: LLM_BASE_URL
    value: "http://llm-nim:8000/v1"
```

**To swap to a different NIM LLM** (e.g., `llama-3.1-8b-instruct`):

```yaml
# llm-nim Deployment:
image: nvcr.io/nim/meta/llama-3.1-8b-instruct:1

# vss-agent env:
- name: LLM_NAME
  value: "meta/llama-3.1-8b-instruct"
```

Also check GPU requirements — if the new model fits on 1 GPU, change `nvidia.com/gpu: 2` → `nvidia.com/gpu: 1`.

**To skip LLM NIM entirely and use a cloud LLM** (e.g., NVIDIA API catalog):

```yaml
# In vss-agent env:
- name: LLM_MODE
  value: "cloud"
- name: LLM_NAME
  value: "nvidia/llama-3.1-nemotron-70b-instruct"
- name: LLM_BASE_URL
  value: "https://integrate.api.nvidia.com/v1"
# NVIDIA_API_KEY must be set in vss-secrets
```

Then set `nvidia.com/gpu: 0` (or remove) on the llm-nim Deployment.

---

### VLM Model (used by alert-verification for PPE checking)

**Current**: `nvidia/cosmos-reason2-8b`

The VLM model is configured in three places:

**1. vlm-nim Deployment** (the NIM container):
```yaml
image: nvcr.io/nim/nvidia/cosmos-reason2-8b:1.6.0  # ← NIM image
```

**2. alert-verification ConfigMap** (`alert-verification-config`):
```yaml
vlm:
  base_url: http://vlm-nim:8000/v1
  model: nvidia/cosmos-reason2-8b    # ← model name sent in API calls
  max_tokens: 4096
  enable_sampling: true
  sampling_fps: 4
```

**3. vss-agent env** (for agent's own visual reasoning):
```yaml
- name: VLM_NAME
  value: "nvidia/cosmos-reason2-8b"
- name: VLM_BASE_URL
  value: "http://vlm-nim:8000/v1"
```

**To swap to a different VLM** (e.g., `llava-v1.6-mistral-7b`):
1. Change `vlm-nim` image
2. Update `alert-verification-config` ConfigMap in the manifest
3. Update `vss-agent` env vars
4. Rebuild and redeploy:
   ```bash
   bash 04-deploy.sh
   kubectl -n vss-alerts rollout restart deployment/vlm-nim deployment/alert-verification deployment/vss-agent
   ```

---

### Detection Models (used by DeepStream perception)

Model files are on the host filesystem at:
```
deployments/data-dir/models/
├── rtdetr-its/
│   ├── model_epoch_035.fp16.onnx   ← RT-DETR object detector
│   └── resnet50_market1501.etlt    ← Re-ID network
└── gdino/
    └── mgdino_mask_head_pruned_dynamic_batch.onnx  ← Grounding DINO
```

These are **mounted as a hostPath** into the `perception-alerts` pod. To swap models:
1. Replace the ONNX file at the host path
2. The DeepStream config (`run_config-api-rtdetr-protobuf.txt`) in the image may need updating to reference the new model filename
3. Restart perception: `kubectl -n vss-alerts rollout restart deployment/perception-alerts`

To change the **DeepStream pipeline config** (e.g., resolution, batch size, model input size), those configs are baked into `vss-perception-alerts:3.1.0`. You need to modify the files in `deployments/developer-workflow/dev-profile-alerts/deepstream/configs/` and rebuild the image:
```bash
bash 02-build-images.sh    # rebuilds vss-perception-alerts:3.1.0
kubectl -n vss-alerts rollout restart deployment/perception-alerts
```

---

### NIM Context Length (affects LLM performance vs GPU memory)

The LLM NIM's context window is set via env var in the manifest:
```yaml
# llm-nim Deployment env:
- name: NIM_MAX_MODEL_LEN
  value: "32768"    # default: 32K tokens
                    # reduce to 16384 for less KV cache memory use
                    # increase to 65536 for longer conversations (needs more VRAM)
```

For the nemotron Mamba hybrid, this controls the **attention layer** KV cache only (Mamba state cache is unaffected). At 32768 tokens the total per-GPU memory is ~18 GiB.

---

## 8. Alert & Detection Customization

### Changing the VLM Verification Prompt

The prompt sent to the VLM for each alert type is in the `alert-verification-config` ConfigMap inside `k8s-alert-verification.yaml`:

```yaml
# Find this section in k8s-alert-verification.yaml (around line 380-430)
data:
  alert_type_config.json: |
    {
      "version": "1.0",
      "alerts": [
        {
          "alert_type": "FOV Count Violation",
          "output_category": "Ladder PPE Violation",
          "prompts": {
            "system": "You are a helpful assistant.",
            "user": "Is anyone on the ladder without a hardhat and safety vest? Answer yes or no."
          }
        }
      ]
    }
```

**To change the verification question** (e.g., detect fire extinguisher blocking):
```json
{
  "alert_type": "FOV Count Violation",
  "output_category": "Fire Extinguisher Blocked",
  "prompts": {
    "system": "You are a safety compliance expert.",
    "user": "Is the fire extinguisher visible and unobstructed? Answer yes or no."
  }
}
```

After editing the manifest:
```bash
bash 04-deploy.sh
kubectl -n vss-alerts rollout restart deployment/alert-verification
```

---

### Changing Behavior Analytics Rules

The incident detection rules are in the `behavior-analytics-config` ConfigMap:

```yaml
# In k8s-alert-verification.yaml, behavior-analytics-config section:
fovCountViolationIncident:
  objectThreshold: 1       # min count to start tracking
  threshold: 2             # count that triggers incident
  expirationWindow: 0.5    # seconds without detection before incident closes
  objectType: person       # object class to watch (person, vehicle, etc.)
```

**To make it more sensitive** (trigger on any single person):
```yaml
objectThreshold: 1
threshold: 1
expirationWindow: 1.0
```

**To watch for vehicles instead of people**:
```yaml
objectType: vehicle
threshold: 3
```

After changes:
```bash
bash 04-deploy.sh
kubectl -n vss-alerts rollout restart deployment/behavior-analytics
```

---

### Changing VLM Sampling Rate

In `alert-verification-config` ConfigMap (within `k8s-alert-verification.yaml`):

```yaml
vlm:
  max_tokens: 4096
  enable_sampling: true
  sampling_fps: 4          # frames per second extracted from clip
  # At 4 fps × 10s window = 40 frames per VLM call
  # Reduce to 2 fps for faster (cheaper) verification
  # Increase to 8 fps for more accurate but slower verification
```

Also the clip window size:
```yaml
vst_config:
  segment_duration_seconds: 10   # how many seconds of video to clip per incident
  segment_anchor: end            # anchor clip at end of incident ('end' or 'start')
```

---

### Changing Number of Camera Streams

To handle more simultaneous streams:

1. Edit `.env`: `NUM_SENSORS=2`
2. Run `bash 04-deploy.sh`
3. DeepStream will handle multiple RTSP inputs within the same pod

For 4+ streams, consider splitting across multiple `perception-alerts` pods (each on its own GPU). Duplicate the Deployment in the manifest with a new name and GPU assignment.

---

### Agent Config (tools, reasoning, reports)

The agent's full behavior is configured in:
```
deployments/developer-workflow/dev-profile-alerts/vss-agent/configs/config.yml
```

This file is mounted directly into the `vss-agent` pod via hostPath:
```yaml
# In vss-agent Deployment:
volumeMounts:
  - name: deployments
    mountPath: /vss-agent/deployments
volumes:
  - name: deployments
    hostPath:
      path: /home/shadeform/video-search-and-summarization/deployments
```

**You can edit this file and restart the agent without rebuilding any image**:
```bash
vim deployments/developer-workflow/dev-profile-alerts/vss-agent/configs/config.yml
kubectl -n vss-alerts rollout restart deployment/vss-agent
```

Key sections you might change:

```yaml
# Agent reasoning model
llms:
  nim_llm:
    type: nim
    model_name: nvidia/nvidia-nemotron-nano-9b-v2   # ← change model
    temperature: 0.0                                  # ← 0=deterministic, 1=creative
    max_tokens: 1024                                  # ← max LLM response length

# Report template location
functions:
  video_report_gen:
    video_report_gen:
      base_url: http://<NODE_IP>:30801/static/       # ← report file server URL

# ReAct loop settings
workflow:
  type: react_agent
  tool_names: [video_analytics]
  max_iterations: 15      # ← max reasoning steps per query
```

---

## 9. Service Reference

### Complete Service Table

| Service | Image | Ports (internal) | NodePort | GPU | Storage | Memory limit |
|---|---|---|---|---|---|---|
| kafka | confluentinc/cp-kafka:8.1.1 | 9092 | — | — | kafka-data 20G | 10 GiB |
| elasticsearch | vss-elasticsearch:3.1.0 | 9200 | — | — | es-data 50G | 3 GiB |
| redis | redis:8.2.2-alpine | 6379 | — | — | redis-data 5G | 1 GiB |
| kibana | kibana:8.12.0 | 5601 | 30561 | — | — | 2 GiB |
| logstash | logstash:8.12.0 | 5044 | — | — | — | 3 GiB |
| phoenix | phoenix:8.12.1 | 6006 | 30606 | — | phoenix-data 10G | 1 GiB |
| postgres | postgres:17.6-alpine | 5432 | — | — | postgres-data 20G | 1 GiB |
| vst-ingress | vss-vios-ingress:3.1.0 | **80** | 30888 | — | — | 512 MiB |
| vst-sensor | vss-vios-sensor:3.1.0 | 30000 | — | — | — | 2 GiB |
| vst-stream-processing | vss-vios-streamprocessing:3.1.0 | 9010 | — | — | vst-data 100G | 2 GiB |
| vst-mcp | vss-vios-mcp:3.1.0 | 8001 | — | — | — | 512 MiB |
| vst-envoy | envoy-proxy:3.1.0 | 9011 | — | — | — | 512 MiB |
| nvstreamer | vss-vios-nvstreamer:3.1.0 | 31000, 31554 | 31000, 31554 | — | nvstreamer-data 50G | 4 GiB |
| perception-sdr | sdr:3.1.0 | 4001 | — | — | — | 512 MiB |
| perception-alerts | vss-perception-alerts:3.1.0 | 9010 | — | 1× | perception-storage 10G | — |
| behavior-analytics | vss-behavior-analytics:3.1.0 | — | — | — | — | 2 GiB |
| video-analytics-api | vss-video-analytics-api:3.1.0 | 8081 | — | — | va-data 10G | 1 GiB |
| alert-verification | vss-alert-verification:3.1.0 | 9080 | — | — | — | 2 GiB |
| llm-nim | nim/nvidia-nemotron-nano-9b-v2:1 | 8000 | — | **2×** | NIM cache hostPath | 60 GiB |
| vlm-nim | nim/cosmos-reason2-8b:1.6.0 | 8000 | — | 1× | NIM cache hostPath | 40 GiB |
| vss-agent-mcp | vss-agent:3.1.0 | 9901 | — | — | — | 1 GiB |
| vss-agent | vss-agent:3.1.0 | 8000 | 30801 | — | agent-eval 10G | 4 GiB |
| vss-agent-ui | vss-agent-ui:3.1.0 | 3000 | 30301 | — | — | 512 MiB |

---

## 10. Kafka Topics & Elasticsearch Indices

### Kafka Topics (all created by kafka-topic-init job)

Settings for all topics: **8 partitions**, replication-factor=1, retention=4 hours

| Topic | Written by | Read by | Description |
|---|---|---|---|
| `mdx-raw` | perception-alerts | behavior-analytics, logstash | Protobuf: detected objects per frame |
| `mdx-incidents` | behavior-analytics | alert-verification | Structured FOV violation incidents |
| `mdx-notification` | behavior-analytics | — | Notification events |
| `mdx-vlm-alerts` | alert-verification | — | VLM-verified alert records |
| `mdx-vlm-incidents` | alert-verification | — | VLM-verified incident records |
| `mdx-behavior` | behavior-analytics | — | Behavior analytics output |
| `mdx-behavior-plus` | behavior-analytics | — | Extended analytics |
| `mdx-events` | VST services | — | VST platform events |
| `mdx-frames` | perception | — | Frame-level data (optional) |
| `mdx-embed` | embedding services | — | Vector embeddings |
| `mdx-embed-filtered` | embedding services | — | Filtered embeddings |
| `mdx-bev` through `mdx-vlm` | various | — | Other analytics (inactive in this profile) |

### Elasticsearch Indices

| Index pattern | Written by | Read by | Description |
|---|---|---|---|
| `mdx-raw-*` | logstash | video-analytics-api, Kibana | Raw detections with bboxes, timestamps |
| `mdx-incidents-*` | behavior-analytics | video-analytics-api, Kibana | Incidents before verification |
| `mdx-vlm-incidents-*` | alert-verification | vss-agent, vss-agent-mcp, Kibana | VLM-verified incidents with verdict |
| `mdx-vlm-alerts-*` | alert-verification | vss-agent-ui alerts tab, Kibana | Deduplicated verified alerts |

ILM policy: indices auto-rollover at 4h min age (`BP_PROFILE=bp_developer_alerts`).

---

## 11. Persistent Volumes & Storage

All PVCs use k3s `local-path` provisioner — **`ReadWriteOnce` only** (one pod, one node).

| PVC name | Size | Used by | What's stored |
|---|---|---|---|
| `kafka-data` | 20 GiB | kafka | KRaft metadata + topic message segments |
| `elasticsearch-data` | 50 GiB | elasticsearch | All mdx-* index shards and segments |
| `redis-data` | 5 GiB | redis | RDB snapshot (dedup state, verdict cache) |
| `phoenix-data` | 10 GiB | phoenix | LLM trace spans, datasets |
| `postgres-data` | 20 GiB | postgres | VST stream metadata, camera registry |
| `vst-data` | 100 GiB | vst-stream-processing | Video clips generated for VLM verification |
| `nvstreamer-data` | 50 GiB | nvstreamer | Uploaded MP4s, transcoded streams |
| `video-analytics-api-data` | 10 GiB | video-analytics-api | Working cache |
| `perception-storage` | 10 GiB | perception-alerts | DeepStream buffers, output files |
| `agent-eval` | 10 GiB | vss-agent | Report files, evaluation artifacts |

Additionally, **hostPath mounts** (not PVCs):
- NIM model caches: `/var/lib/docker/volumes/mdx_*_cache/_data` → `/opt/nim/.cache`
- Perception models: `deployments/data-dir/models/` → `/opt/.../models/`
- Agent config: `deployments/` → `/vss-agent/deployments/`
- Docker socket: `/var/run/docker.sock` → `/var/run/docker.sock` (perception-sdr)

---

## 12. Secrets & ConfigMaps

### `vss-secrets` (created by 01-setup.sh)

| Key | Used by | Purpose |
|---|---|---|
| `NGC_CLI_API_KEY` | llm-nim, vlm-nim | NIM container auth to pull NGC profiles |
| `NVIDIA_API_KEY` | vss-agent | Cloud NIM API fallback |
| `OPENAI_API_KEY` | vss-agent | Cloud OpenAI fallback |
| `HF_TOKEN` | embedding services | HuggingFace model downloads |
| `POSTGRES_PASSWORD` | postgres | DB password |

### ConfigMaps (all in k8s-alert-verification.yaml)

#### `behavior-analytics-config`
Controls incident detection rules. Key section:
```json
"fovCountViolationIncident": {
  "objectThreshold": 1,
  "threshold": 2,
  "expirationWindow": 0.5,
  "objectType": "person"
}
```

#### `alert-verification-config`
Two files: `config.yml` (VLM settings, Kafka, VST, Redis connections) and `alert_type_config.json` (per-alert-type prompts). **Most commonly customized**.

#### `vss-agent-mcp-config`
MCP server tool definitions and LLM settings. Rarely changed unless adding new tools.

#### `nvstreamer-config`
NVStreamer HTTP/RTSP port config and Elasticsearch connection for video metadata indexing.

#### `redis-config`
Redis persistence policy and eviction settings. Default is `noeviction` with RDB snapshots.

#### `sdr-config`
Maps the SDR workload manager to the `perception-alerts` pod. Contains `docker_cluster_config.json` — the `provisioning_address` points to where perception's stream API is (`vst-stream-processing:9010`).

#### `video-analytics-api-config`
Elasticsearch node and index prefix. Generally unchanged.

---

## 13. Initialization Jobs

These run once at deployment and are not restarted automatically.

### kafka-topic-init
Creates 19 Kafka topics. Run after Kafka is ready.
```bash
# Rerun:
kubectl -n vss-alerts delete job kafka-topic-init
bash 04-deploy.sh
```

### elasticsearch-init
Applies ILM policy, index templates, and ingest pipelines to Elasticsearch.
```bash
# Rerun:
kubectl -n vss-alerts delete job elasticsearch-init
bash 04-deploy.sh
```

### kibana-init
Imports 24 saved objects (ITS Dashboard, index patterns, visualizations) into Kibana.

The job overrides the container command to patch a localhost → K8s DNS issue in the init script before running:
```bash
sed -i 's|localhost:9200|elasticsearch:9200|g; s|localhost:5601|kibana:5601|g' \
  /opt/mdx/init-scripts/kibana-import-dashboard.sh
exec bash /opt/mdx/init-scripts/kibana-import-dashboard.sh
```

```bash
# Rerun:
kubectl -n vss-alerts delete job kibana-init
bash 04-deploy.sh
```

---

## 14. Custom Images

### What needs to be built vs pulled

```
PULLED FROM nvcr.io (no build needed):
├── confluentinc/cp-kafka:8.1.1
├── redis:8.2.2-alpine
├── docker.elastic.co/kibana/kibana:8.12.0
├── docker.elastic.co/logstash/logstash:8.12.0
├── arizephoenix/phoenix:version-8.12.1
├── postgres:17.6-alpine
├── nvcr.io/nvidia/vss-core/vss-vios-* (all VST services)
├── nvcr.io/nvidia/vss-core/sdr:3.1.0
├── nvcr.io/nvidia/vss-core/vss-behavior-analytics:3.1.0
├── nvcr.io/nvidia/vss-core/vss-video-analytics-api:3.1.0
├── nvcr.io/nvidia/vss-core/vss-alert-verification:3.1.0
├── nvcr.io/nvidia/vss-core/vss-agent:3.1.0
├── nvcr.io/nvidia/vss-core/vss-agent-ui:3.1.0
├── nvcr.io/nim/nvidia/nvidia-nemotron-nano-9b-v2:1
└── nvcr.io/nim/nvidia/cosmos-reason2-8b:1.6.0

BUILT BY 02-build-images.sh AND PUSHED TO YOUR_REGISTRY:
├── vss-elasticsearch:3.1.0      ← adds ES plugins
├── vss-elastic-init:3.1.0       ← ILM + template init scripts
├── vss-broker-health-check:3.1.0← Kafka health checker
├── vss-perception-alerts:3.1.0  ← DeepStream + profile configs (FROM vss-rt-cv)
└── vss-kibana-init-alerts:3.1.0 ← dashboard import JSON
```

### Rebuilding a Custom Image

```bash
# Rebuild just perception (e.g., after changing DeepStream configs):
docker build \
  -f deployments/developer-workflow/dev-profile-alerts/Dockerfiles/perception.Dockerfile \
  --build-arg PERCEPTION_IMAGE=nvcr.io/nvidia/vss-core/vss-rt-cv \
  --build-arg PERCEPTION_TAG=3.1.0 \
  -t localhost:5000/vss-perception-alerts:3.1.0 \
  deployments/developer-workflow/dev-profile-alerts
docker push localhost:5000/vss-perception-alerts:3.1.0
kubectl -n vss-alerts rollout restart deployment/perception-alerts
```

---

## 15. K8s vs Docker Compose Differences

Every difference from the original Docker Compose setup and why it exists:

| Problem | Docker Compose | Kubernetes Fix |
|---|---|---|
| **Kafka exits on startup** | No K8s service env injection | `enableServiceLinks: false` — blocks `KAFKA_PORT=tcp://...` injection |
| **Elasticsearch 401 on health probes** | Security off by default in older versions | `xpack.security.enabled: "false"` — disables auth for single-node dev use |
| **kibana-init fails (localhost)** | Script connects to `localhost:9200` | Command override with `sed` replaces `localhost` → K8s DNS names |
| **alert-verification config** | `env-substitute.py` resolves `${VAR}` at boot | Skip env-substitute; mount pre-resolved ConfigMap at `/app/runtime/config.yml` |
| **vst-ingress probe wrong port** | Nginx listens on 80, not 30888 | `containerPort: 80`, probe port 80, service `targetPort: 80` |
| **PVC access mode** | Docker volumes are flexible | k3s local-path: `ReadWriteOnce` only — no `ReadWriteMany` |
| **LLM NIM OOM** | Single GPU works in Docker | `nvidia.com/gpu: 2` + TP=2 — Mamba state cache needs 2×GPU |
| **URL double-suffix** | Config reads env vars as-is | `VIDEO_ANALYSIS_MCP_URL` without `/mcp`, `PHOENIX_ENDPOINT` without `/v1/traces` — config appends them |
| **Reports URL** | `HOST_IP` env var | `VSS_AGENT_REPORTS_BASE_URL: http://<NODE_IP>:30801/static/` explicitly set |
| **NIM cache** | Docker volumes survive container restarts | hostPath to Docker volume path — survives pod restarts |
| **NIM GPU pinning** | `--gpus "device=1"` | `NVIDIA_VISIBLE_DEVICES` + `nvidia.com/gpu: N` (device plugin overrides for LLM) |
| **Service DNS** | `localhost:*` everywhere | All configs use K8s DNS names via ConfigMaps |

---

## 16. Troubleshooting

### Quick Diagnostic Commands

```bash
# All pod status with restart counts
kubectl -n vss-alerts get pods --no-headers | awk '{print $2, $3, $4, $1}' | sort -k3 -rn | column -t

# Recent events (scheduling failures, probe failures)
kubectl -n vss-alerts get events --sort-by='.lastTimestamp' | tail -20

# Logs for any service
kubectl -n vss-alerts logs -f deployment/<name> [--previous]

# Describe pod (node, resources, events)
kubectl -n vss-alerts describe pod <pod-name>

# GPU utilization
nvidia-smi
kubectl get nodes -o custom-columns="NODE:.metadata.name,GPUs:.status.capacity.nvidia\.com/gpu,AllocGPUs:.status.allocatable.nvidia\.com/gpu"
```

---

### LLM NIM: CUDA OOM (`Tried to allocate 33.75 GiB`)

**Root cause**: `nvidia-nemotron-nano-9b-v2` is a Mamba hybrid model. Its SSM recurrent state cache is per-sequence and cannot be reduced by lowering `max_model_len`. On a single L40S (44.4 GiB CUDA visible): weights (17.5 GiB) + state cache (33.75 GiB) = 51.25 GiB → OOM.

**Fix**: The manifest uses `nvidia.com/gpu: 2`. NIM auto-selects TP=2 profile, splitting everything across 2 GPUs (~18 GiB each).

```bash
nvidia-smi          # ensure 2 GPUs show ~0 MiB used
kubectl -n vss-alerts rollout restart deployment/llm-nim
kubectl -n vss-alerts logs -f deployment/llm-nim | grep -E "profile|tensor_parallel|OOM|GPU blocks"
```

Healthy startup log sequence:
```
Detected 2 compatible profile(s).
Selected profile: ...vllm-bf16-tp2-pp1...
tensor_parallel_size: 2
Loading weights took 4.01 seconds        ← fast because NIM cache is warm
Memory profiling takes 55 seconds
# cuda blocks: 80126
Available routes are: /v1/chat/completions ...
```

---

### Kafka: Exits with `KAFKA_PORT is deprecated`

**Root cause**: K8s injects `KAFKA_PORT=tcp://10.x.x.x:9092` from the kafka Service into all pods — including Kafka itself. Kafka's entrypoint treats this as a legacy config value and exits.

**Fix** (already applied): `enableServiceLinks: false` on Kafka pod spec.

```bash
kubectl -n vss-alerts get deployment kafka -o yaml | grep enableServiceLinks
# Should show: enableServiceLinks: false
# If missing, re-apply: bash 04-deploy.sh
```

---

### Kafka PVC Corrupted (lock file after force-delete)

```bash
kubectl -n vss-alerts logs deployment/kafka | grep -i "lock\|error\|meta.properties"
```

**Fix**:
```bash
kubectl -n vss-alerts delete deployment kafka
kubectl -n vss-alerts delete pvc kafka-data
bash 04-deploy.sh
# Wait for kafka-topic-init to re-create all topics
```

---

### Elasticsearch: 401 on Health Probes (pod keeps restarting)

**Root cause**: ES 8.x security is on by default. HTTP health probe → `/_cluster/health` → 401 Unauthorized → probe fails → K8s kills pod.

**Fix** (already applied): `xpack.security.enabled: "false"` in ES deployment env.

```bash
kubectl -n vss-alerts exec deployment/elasticsearch -- \
  curl -s http://localhost:9200/_cluster/health
# Should return: {"status":"green",...} not 401
```

---

### kibana-init: `Unable to connect` or `localhost refused`

The init script inside the image hardcodes `localhost`. The job command patches this before running.

```bash
# Check job logs:
kubectl -n vss-alerts logs job/kibana-init
# Should see: "Importing Dashboards ... successCount:24"
```

If the job is in Error state, rerun:
```bash
kubectl -n vss-alerts delete job kibana-init
bash 04-deploy.sh
```

---

### perception-alerts: Missing model file

```
cp: cannot stat '.../resnet50_market1501.etlt': No such file or directory
```

**Fix**:
```bash
docker run --rm \
  -v /home/shadeform/video-search-and-summarization/deployments/data-dir/models:/out \
  --entrypoint bash localhost:5000/vss-perception-alerts:3.1.0 \
  -c "cp /opt/nvidia/deepstream/deepstream/sources/apps/sample_apps/metropolis_perception_app/models/rtdetr-its/resnet50_market1501.etlt /out/rtdetr-its/"
kubectl -n vss-alerts rollout restart deployment/perception-alerts
```

---

### perception-alerts: CUDA Error 35 (MPS Conflict)

**Root cause**: GPU 0 (or whichever GPU is assigned) has a CUDA MPS server running. DeepStream cannot use MPS-managed GPUs.

```bash
nvidia-smi                              # identify which GPUs have MPS processes
cat /proc/$(pgrep -f "nvidia-cuda-mps")/environ 2>/dev/null | tr '\0' '\n' | grep CUDA
```

**Fix**: Change `RT_CV_DEVICE_ID` in `.env` to a GPU without MPS, then redeploy:
```bash
# Edit .env: RT_CV_DEVICE_ID=3
bash 04-deploy.sh
kubectl -n vss-alerts rollout restart deployment/perception-alerts
```

---

### vst-ingress: Readiness Probe Failing (port mismatch)

**Root cause**: nginx inside the container listens on port 80. Any probe checking port 30888 will always fail.

```bash
kubectl -n vss-alerts describe pod -l app=vst-ingress | grep -A5 "Readiness\|containerPort"
# containerPort: 80 (correct), readinessProbe: port 80 (correct)
```

**Fix** (already applied): `containerPort: 80`, `readinessProbe.tcpSocket.port: 80`, `service.targetPort: 80`.

---

### vss-agent: Pydantic Validation Error at Startup

```
functions.video_report_gen.video_report_gen.base_url
  Input should be a valid string [type=string_type]
```

**Root cause**: `VSS_AGENT_REPORTS_BASE_URL` not set. Agent config expects a string but gets None.

**Fix** (already applied): env var `VSS_AGENT_REPORTS_BASE_URL: "http://<NODE_IP>:30801/static/"` is set in the deployment.

---

### NIM Takes 22+ Minutes to Start

On **first boot** (empty NIM cache), the model must be downloaded and profiled — this can take 20–30+ minutes on slow networks or first compile.

On **warm restart** (cache populated at `/var/lib/docker/volumes/mdx_*_cache/`), startup takes 1–3 minutes.

```bash
# Check if cache is populated:
du -sh /var/lib/docker/volumes/mdx_nvidia_nemotron_nano_9b_v2_cache/_data/
# >10 GiB = cache exists

# Watch startup progress:
kubectl -n vss-alerts logs -f deployment/llm-nim | \
  grep -E "profile|Loading|GPU|blocks|routes|OOM|Error"

# The startup probe allows 130 × 10s = ~22 min
# If it expires: extend the probe in manifest, re-apply, delete pod
```

---

### Do Not Use `envsubst` to Apply the Manifest

```bash
# WRONG — envsubst only handles $VAR syntax, not CHANGE_ME_* tokens
kubectl apply -f <(envsubst < k8s-alert-verification.yaml)
# Result: image names become "CHANGE_ME_YOUR_REGISTRY/vss-..." → InvalidImageName
```

```bash
# ALWAYS use:
bash 04-deploy.sh
```

---

### perception-alerts: ONNX model files are empty directories / trtexec never runs

**Symptom**: perception-alerts enters CrashLoopBackOff. Triton logs show `unable to find 'gdino_trt/1/model.plan'`. No `trtexec` output in the logs.

**Root cause**: `03-download-models.sh` uses the NGC CLI (v3.52.0+) which requires `--org` when authenticated. The CLI silently fails and `mkdir -p` creates empty directories at the hostPath location. Additionally, the job targets a `models-data` PVC while perception-alerts uses a **hostPath** (`deployments/data-dir/models/`) — a completely different storage location. `ds-start.sh` runs `cp *.onnx /opt/storage/` which silently skips directories (`cp: -r not specified; omitting directory`), so `trtexec` is never invoked and DeepStream starts with no compiled plan.

**Diagnose**:
```bash
ls -la /home/shadeform/video-search-and-summarization/deployments/data-dir/models/gdino/
ls -la /home/shadeform/video-search-and-summarization/deployments/data-dir/models/rtdetr-its/
# Files that show as drwxr-xr-x (directories) instead of -rw-r--r-- are broken
```

**Fix**: Remove the empty directories and download real files via NGC REST API:
```bash
rm -rf deployments/data-dir/models/gdino/mgdino_mask_head_pruned_dynamic_batch.onnx
rm -rf deployments/data-dir/models/rtdetr-its/model_epoch_035.fp16.onnx

# GDINO model (~686 MB)
curl -L -o deployments/data-dir/models/gdino/mgdino_mask_head_pruned_dynamic_batch.onnx \
  -H "Authorization: ApiKey $NGC_CLI_API_KEY" \
  "https://api.ngc.nvidia.com/v2/models/nvidia/tao/mask_grounding_dino/versions/mask_grounding_dino_swin_tiny_commercial_deployable_v2.1_wo_mask_arm/files/mgdino_mask_head_pruned_dynamic_batch.onnx"

# RT-DETR model (~84 MB)
curl -L -o deployments/data-dir/models/rtdetr-its/model_epoch_035.fp16.onnx \
  -H "Authorization: ApiKey $NGC_CLI_API_KEY" \
  "https://api.ngc.nvidia.com/v2/models/nvidia/tao/trafficcamnet_transformer_lite/versions/deployable_resnet50_v2.0/files/resnet50_trafficcamnet_rtdetr.fp16.onnx"

kubectl -n vss-alerts rollout restart deployment/perception-alerts
```

On first start, `trtexec` compiles the ONNX → TRT plan (~4 min on L40S) and saves it to the `perception-storage` PVC. Subsequent restarts reuse the cached plan instantly.

---

### perception-alerts: DeepStream connects to `localhost:9092` instead of Kafka

**Symptom**: Perception-alerts starts (trtexec succeeds) but no events appear in Kibana `mdx-raw-*`. Logs show `msg-broker-conn-str=localhost;9092;mdx-raw`.

**Root cause**: `run_config-api-rtdetr-protobuf.txt` inside the `vss-perception-alerts:3.1.0` image hardcodes `localhost;9092` as the Kafka broker. In Kubernetes, `localhost` is the pod itself — the correct K8s DNS name for Kafka is `kafka`.

**Fix** (already applied in manifest): The perception-alerts Deployment overrides the startup command to patch the broker address before `ds-start.sh` runs:
```yaml
command:
  - bash
  - -c
  - |
    sed -i 's/msg-broker-conn-str=localhost;9092/msg-broker-conn-str=kafka;9092/g' run_config-api-rtdetr-protobuf.txt
    exec bash ds-start.sh run_config-api-rtdetr-protobuf.txt
```

Verify after restart:
```bash
kubectl -n vss-alerts logs deployment/perception-alerts | grep "msg-broker-conn-str"
# Must show: msg-broker-conn-str=kafka;9092;mdx-raw
```

---

### perception-sdr: `localhost:6379 Connection refused` (Redis — non-critical)

**Symptom**: `perception-sdr` logs repeatedly show:
```
redis.exceptions.ConnectionError: Error 111 connecting to localhost:6379. Connection refused.
Exception ignored in: <function Consumer.__del__>
```

**Root cause**: The SDR binary (`/wdm/dist/sdr`) is a compiled PyInstaller executable with `localhost:6379` baked in. Kubernetes injects `REDIS_SERVICE_HOST` via service links but the binary ignores it.

**Assessment**: **Non-critical — safe to ignore.** SDR uses `WDM_INITIALIZE_FROM_VST=true`, meaning it discovers RTSP streams via the VST REST API, not Redis. The Redis errors come from a cleanup destructor path only. SDR's core function — routing RTSP stream events from VST to the perception-alerts HTTP API — is unaffected.

---

## 17. Service URLs Quick Reference

For this deployment (`NODE_IP=172.19.4.52`):

| Service | URL | What you do here |
|---|---|---|
| **VSS Agent UI** | http://172.19.4.52:30301 | Main interface — chat, alerts, video mgmt |
| **VST Dashboard** | http://172.19.4.52:30888/vst/#/dashboard | Manage streams, view storage |
| **NVStreamer UI** | http://172.19.4.52:31000/#/dashboard | Upload videos, start RTSP streams |
| **Kibana** | http://172.19.4.52:30561/app/home#/ | ITS Dashboard, raw data explore |
| **Phoenix** | http://172.19.4.52:30606/projects | LLM call traces, latency, token counts |

```
Replace 172.19.4.52 with the value of NODE_IP in your .env file.
```
