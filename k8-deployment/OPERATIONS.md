# VSS Alert Verification — Full Operations Guide

> **What this covers**: How every service talks to every other service,
> how data flows end-to-end, how to run it fresh, how to tear it down,
> how to reconfigure GPUs for 2/4/8 GPU machines, and every config knob
> you can turn without touching the codebase.

---

## Table of Contents

1. [What This System Does](#1-what-this-system-does)
2. [Service Map & Communication](#2-service-map--communication)
3. [End-to-End Data Flow](#3-end-to-end-data-flow)
4. [GPU Layout — Modular Profiles](#4-gpu-layout--modular-profiles)
5. [Before You Deploy — Prerequisites](#5-before-you-deploy--prerequisites)
6. [Fresh Deployment — Step by Step](#6-fresh-deployment--step-by-step)
7. [Verifying Everything Works](#7-verifying-everything-works)
8. [Tearing Down](#8-tearing-down)
9. [Changing Models](#9-changing-models)
10. [Customizing Alerts & Detection](#10-customizing-alerts--detection)
11. [Re-deploying a Single Service](#11-re-deploying-a-single-service)
12. [Troubleshooting Reference](#12-troubleshooting-reference)
13. [Quick-Reference Cheatsheet](#13-quick-reference-cheatsheet)

---

## 1. What This System Does

PPE (Personal Protective Equipment) violation detection on live video, with AI-powered alert verification and an LLM chat agent.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                                                                         │
│   CAMERA / VIDEO FILE                                                   │
│         │                                                               │
│         ▼  RTSP stream                                                  │
│   ┌─────────────┐   object detection   ┌──────────────────┐            │
│   │  DeepStream │ ──────────────────▶  │  Kafka (mdx-raw) │            │
│   │  (GPU)      │                      └────────┬─────────┘            │
│   └─────────────┘                               │                      │
│                                                 ▼                      │
│                                    ┌──────────────────────┐            │
│                                    │  Behavior Analytics  │            │
│                                    │  "2 persons on ladder│            │
│                                    │   → emit incident"   │            │
│                                    └──────────┬───────────┘            │
│                                               │                        │
│                                               ▼                        │
│                                    ┌──────────────────────┐            │
│                                    │  Alert Verification  │            │
│                                    │  ┌──────────────────┐│            │
│                                    │  │ VLM (GPU)        ││            │
│                                    │  │ "Is anyone on    ││            │
│                                    │  │  ladder without  ││            │
│                                    │  │  hard hat? yes"  ││            │
│                                    │  └──────────────────┘│            │
│                                    └──────────┬───────────┘            │
│                                               │                        │
│                                               ▼                        │
│                                    ┌──────────────────────┐            │
│                                    │   Elasticsearch      │            │
│                                    │   (verified alert    │            │
│                                    │    stored)           │            │
│                                    └──────────┬───────────┘            │
│                                               │                        │
│                                               ▼                        │
│   User: "Any violations today?" ──▶ ┌──────────────────────┐          │
│                                     │  LLM Agent (GPU)     │          │
│                                     │  queries ES, formats │          │
│                                     │  natural language    │          │
│   User: "Generate report" ◀──────── │  response            │          │
│                                     └──────────────────────┘          │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Service Map & Communication

### Full Architecture Diagram

```
╔════════════════════════════════════════════════════════════════════════════╗
║  BROWSER / EXTERNAL                                                         ║
║                                                                             ║
║  :30301 → vss-agent-ui    :30888 → VST Dashboard    :31000 → NVStreamer UI  ║
║  :30561 → Kibana          :30606 → Phoenix           :31554 → RTSP input    ║
╚═════╤════════════════╤══════════════════╤═══════════════════╤══════════════╝
      │                │                  │                   │
      ▼                ▼                  ▼                   ▼
╔═════════════╗  ╔══════════════╗  ╔════════════╗  ╔══════════════════════╗
║ AGENT LAYER ║  ║  VST LAYER   ║  ║  TELEMETRY ║  ║  VIDEO SOURCE LAYER  ║
║             ║  ║              ║  ║            ║  ║                      ║
║ vss-agent   ║  ║ vst-ingress  ║  ║ phoenix    ║  ║ nvstreamer           ║
║  :8000      ║  ║   :80        ║  ║  :6006     ║  ║  :31000 (HTTP)       ║
║   │         ║  ║  (NP:30888)  ║  ║            ║  ║  :31554 (RTSP out)   ║
║   │ MCP     ║  ║   ├ vst-     ║  ╚════════════╝  ╚══════════╤═══════════╝
║   ├──────▶  ║  ║   │ sensor   ║                              │ RTSP URL
║   │ vss-    ║  ║   │  :30000  ║                              │ registered
║   │ agent-  ║  ║   ├ vst-mcp  ║                              ▼
║   │ mcp     ║  ║   │  :8001   ║  ╔══════════════════════════════════════╗
║   │  :9901  ║  ║   ├ vst-     ║  ║  PERCEPTION PIPELINE                 ║
║   │         ║  ║   │ stream-  ║  ║                                      ║
║   │ LLM     ║  ║   │ proc     ║  ║  perception-sdr :4001                ║
║   ├──────▶  ║  ║   │  :9010   ║  ║  (watches VST for stream events)     ║
║   │ llm-nim ║  ║   └ vst-     ║  ║    │ stream add/remove               ║
║   │  :8000  ║  ║     envoy    ║  ║    ▼                                 ║
║   │         ║  ║      :9011   ║  ║  perception-alerts :9010             ║
║   │ VLM     ║  ║              ║  ║  (DeepStream + RT-DETR + GDINO)      ║
║   ├──────▶  ║  ║              ║  ║  GPU pod                             ║
║   │ vlm-nim ║  ║              ║  ║    │ protobuf frames                 ║
║   │  :8000  ║  ║              ║  ║    ▼                                 ║
║   │         ║  ║              ║  ║  Kafka: mdx-raw                      ║
║   │ ES      ║  ║              ║  ╚═════════════════╤════════════════════╝
║   └──────▶  ║  ╚══════════════╝                    │
║     elastic ║                                       ▼
║     search  ║  ╔══════════════════════════════════════════════════════════╗
║      :9200  ║  ║  ANALYTICS + STORAGE LAYER                               ║
╚═════════════╝  ║                                                           ║
                 ║  behavior-analytics ──▶ Kafka: mdx-incidents             ║
                 ║  (consumes mdx-raw)                                       ║
                 ║                                                           ║
                 ║  alert-verification :9080                                 ║
                 ║  (consumes mdx-incidents)                                 ║
                 ║    ├── redis :6379  (dedup check)                        ║
                 ║    ├── vst-ingress :80  (fetch video clip)               ║
                 ║    ├── vlm-nim :8000  (VLM inference)                    ║
                 ║    └── elasticsearch :9200  (store verdict)              ║
                 ║                                                           ║
                 ║  logstash ──▶ elasticsearch (mdx-raw indexing)           ║
                 ║  kibana :5601 (NP:30561) reads elasticsearch             ║
                 ║                                                           ║
                 ║  Supporting:                                              ║
                 ║    postgres :5432  (VST stream metadata)                 ║
                 ║    redis :6379  (dedup + verdict cache)                  ║
                 ║    video-analytics-api :8081  (used by vss-agent-mcp)   ║
                 ╚══════════════════════════════════════════════════════════╝
```

### Who Calls Who — Complete Table

```
SERVICE                PORT    CALLED BY
────────────────────────────────────────────────────────────────────────────
kafka                  9092    perception-alerts, behavior-analytics,
                               alert-verification, logstash, kafka-topic-init

elasticsearch          9200    logstash, kibana, kibana-init,
                               elasticsearch-init, video-analytics-api,
                               alert-verification, vss-agent, vss-agent-mcp

redis                  6379    alert-verification (dedup + result cache)

postgres               5432    vst-sensor (stream registry)

vst-ingress            80      alert-verification (video clip fetch),
(NodePort 30888)               vss-agent-ui, vss-agent, perception-sdr,
                               vst-mcp

vst-sensor             30000   via vst-ingress only

vst-stream-processing  9010    vst-sensor, perception-sdr

vst-envoy              9011    vst-stream-processing (gRPC proxy)

vst-mcp                8001    vss-agent (VST control tools over MCP)

vss-agent-mcp          9901    vss-agent (video analytics tools over MCP)

video-analytics-api    8081    vss-agent-mcp

nvstreamer             31000   browser (HTTP UI)
nvstreamer             31554   perception-sdr (RTSP URL source), cameras

perception-sdr         4001    VST webhook (stream add/remove events)

perception-alerts      9010    perception-sdr (add/remove RTSP stream)

llm-nim                8000    vss-agent (chat completions)

vlm-nim                8000    alert-verification (PPE verification),
                               vss-agent (visual reasoning tools)

phoenix                6006    vss-agent (OpenTelemetry traces via OTLP)

vss-agent              8000    vss-agent-ui
```

### MCP (Model Context Protocol) — How the Agent Uses Tools

`vss-agent` uses two MCP servers to get tools:

```
vss-agent
    │
    ├── POST http://vss-agent-mcp:9901/mcp
    │   Available tools:
    │     get_incidents     → queries mdx-vlm-incidents in ES
    │     get_alerts        → queries mdx-vlm-alerts in ES
    │     get_raw_events    → queries mdx-raw in ES
    │     generate_report   → creates HTML/PDF report
    │     get_video_clip    → fetches clip URL from VST
    │     (calls video-analytics-api:8081 internally)
    │
    └── POST http://vst-mcp:8001/mcp
        Available tools:
          list_streams     → GET /vst/api/v1/live/streams
          add_stream       → POST /vst/api/v1/live/streams
          remove_stream    → DELETE /vst/api/v1/live/streams/{id}
          get_stream_info  → GET /vst/api/v1/live/streams/{id}
```

The agent runs a **ReAct loop** (max 15 iterations):
```
THOUGHT → ACTION (call MCP tool) → OBSERVATION → THOUGHT → ...
All LLM calls go to: POST http://llm-nim:8000/v1/chat/completions
All calls traced to: phoenix:6006 (OpenTelemetry)
```

---

## 3. End-to-End Data Flow

### Step 1 — Video Enters the System

```
1a. Upload video to NVStreamer
    POST http://172.19.4.52:31000/api/...  (MP4 upload)
    NVStreamer transcodes → RTSP served at rtsp://172.19.4.52:31554/<id>

1b. Register stream with VST
    Via VSS Agent UI → Video Management → + Add RTSP
    POST http://vst-ingress:80/vst/api/v1/live/streams
    {rtsp_url: "rtsp://172.19.4.52:31554/<id>", sensor_id: "cam-01"}
    → vst-sensor writes to postgres:5432
    → vst-stream-processing:9010 starts tracking

1c. perception-sdr picks it up
    Polls: GET http://vst-ingress:80/vst/api/v1/live/streams  (every ~5s)
    → Sees new stream
    POST http://perception-alerts:9010/api/v1/stream/add
    {rtsp_url: "rtsp://...", sensor_id: "cam-01"}
    → DeepStream pipeline starts consuming RTSP
```

### Step 2 — Frame Detection (continuous, ~30fps)

```
DeepStream pipeline inside perception-alerts (GPU pod):

  RTSP source
    → nvvidconv (color format)
    → nvinfer  (RT-DETR: detects persons, vehicles, objects)
    → nvinfer  (Grounding DINO: zone classification)
    → nvtracker (Re-ID: assigns persistent tracking IDs)
    → nvmsgconv (serialize detections to protobuf)
    → nvmsgbroker

  Output: Kafka topic mdx-raw
  Message fields:
    sensor_id, timestamp_unix_ms, frame_id
    objects[]:
      class: "person"
      tracking_id: 42
      confidence: 0.94
      bbox: {x: 120, y: 80, w: 60, h: 180}
      zones: ["ladder-zone-1"]
```

### Step 3 — Behavior Analytics (incident detection)

```
behavior-analytics
  Consumer group: mdx-spatial-analytics-2d-app
  Reads: mdx-raw

  Rule engine evaluates per-frame:

  fovCountViolationIncident:
    IF objects matching objectType("person") in zone("ladder-zone-1")
       count >= threshold(2)
       sustained for expirationWindow(0.5s)
    THEN publish incident:

  Output: Kafka topic mdx-incidents
  Message:
    sensor_id, incident_type: "FOV Count Violation"
    start_time, end_time, object_count
    zone_name, camera_metadata

  Parallel: logstash also reads mdx-raw
    → indexes all frames to elasticsearch: mdx-raw-{YYYY.MM.dd}
```

### Step 4 — VLM Alert Verification

```
alert-verification
  Consumer group: kafka-incidents-dumper
  Reads: mdx-incidents

  For each incident:

  ① DEDUP:
     GET redis:6379 key="alert-bridge-input-stream:<incident_hash>"
     EXISTS → skip (already processing this incident in last 5s)

  ② FETCH CLIP:
     GET http://vst-ingress:80/api/v1/storage/file/path
         ?sensor_id=cam-01&start=<epoch_ms>&end=<epoch_ms+10000>
     → returns path on vst-data PVC (100 GiB volume)
     → read video file, extract 10s window

  ③ SAMPLE FRAMES:
     Extract at sampling_fps=4 → ~40 JPEG frames per clip
     Encode each as base64

  ④ VLM CALL:
     POST http://vlm-nim:8000/v1/chat/completions
     {
       model: "nvidia/cosmos-reason2-8b",
       max_tokens: 4096,
       messages: [{
         role: "user",
         content: [
           {type: "text",
            text: "Is anyone on the ladder without a hardhat and safety vest? Answer yes or no."},
           {type: "image_url", url: "data:image/jpeg;base64,<frame1>"},
           ... × 40 frames
         ]
       }]
     }
     Response: "yes" or "no" + reasoning

  ⑤ STORE VERDICT:
     POST http://elasticsearch:9200/mdx-vlm-incidents/_doc
     {sensor_id, incident_type: "Ladder PPE Violation",
      verdict: "yes", timestamp, clip_path, vlm_response}

     POST http://elasticsearch:9200/mdx-vlm-alerts/_doc
     (deduplicated alert record)

  ⑥ CACHE:
     SET redis key TTL=600s (prevents re-verifying same incident for 10 min)
```

### Step 5 — Agent Query

```
User: "Show me the last 5 PPE violations"

vss-agent-ui → POST http://vss-agent:8000/chat
               {message: "Show me the last 5 PPE violations", session_id: "..."}

vss-agent ReAct loop:

  ITER 1:
    LLM POST http://llm-nim:8000/v1/chat/completions
    THOUGHT: "I should query verified incidents from ES"
    ACTION:  call tool get_incidents

    POST http://vss-agent-mcp:9901/mcp
    {tool: "get_incidents", params: {limit: 5, verified: true, sort: "desc"}}

    vss-agent-mcp → GET http://elasticsearch:9200/mdx-vlm-incidents/_search
    {query: {bool: {filter: [{term: {verdict: "yes"}}]}}, size: 5, sort: [{timestamp: desc}]}

    Returns: 5 incident records with clip paths, timestamps, verdicts

  ITER 2:
    LLM POST http://llm-nim:8000/v1/chat/completions
    THOUGHT: "I have the data, format a response"
    FINAL ANSWER: "Here are the last 5 PPE violations: ..."

  All LLM calls traced → phoenix:6006 (OTLP)

Response back to vss-agent-ui
```

---

## 4. GPU Layout — Modular Profiles

### GPU Requirements Per Workload

```
┌──────────────────────────────────────────────────┐
│ Workload             │ VRAM needed  │ Notes       │
├──────────────────────┼──────────────┼─────────────┤
│ DeepStream           │ 4–8 GiB      │ TensorRT    │
│  (perception-alerts) │              │ compiled    │
├──────────────────────┼──────────────┼─────────────┤
│ VLM NIM              │ ~34 GiB      │ Single L40S │
│  (cosmos-reason2-8b) │              │ (46 GiB)    │
├──────────────────────┼──────────────┼─────────────┤
│ LLM NIM              │ ~18 GiB × 2  │ Mamba SSM   │
│  (nemotron-nano-9b)  │ = 36 GiB     │ MUST use    │
│                      │ across 2 GPU │ TP=2        │
└──────────────────────┴──────────────┴─────────────┘
```

> **Why LLM needs 2 GPUs**: `nvidia-nemotron-nano-9b-v2` is a **Mamba hybrid model**
> (SSM + attention). Its recurrent state cache is per-sequence and fixed at ~33.75 GiB
> regardless of context length. On a single L40S (44.4 GiB): weights (17.5 GiB) +
> state cache (33.75 GiB) = 51.25 GiB → OOM. With 2 GPUs, NIM auto-selects TP=2
> profile (`vllm-bf16-tp2-pp1`): each GPU carries ~8.3 GiB weights + ~10 GiB state = 18 GiB.

---

### Profile A — 4 GPUs (Minimum Full Stack)

Use this on a fresh machine with 4× L40S, A100-40G, or H100-40G.

```
┌─────┬──────────────────────────────────┬────────────┐
│ GPU │ Workload                         │ .env var   │
├─────┼──────────────────────────────────┼────────────┤
│  0  │ DeepStream (perception-alerts)   │ RT_CV=0    │
│  1  │ VLM NIM (cosmos-reason2-8b)      │ VLM=1      │
│  2  │ LLM NIM ─┐ (nemotron-nano-9b)   │ LLM=2 *    │
│  3  │          ─┘ TP=2 auto-assigned  │            │
└─────┴──────────────────────────────────┴────────────┘
* LLM_DEVICE_ID is informational — device plugin auto-picks 2 free GPUs
```

`.env` for Profile A:
```bash
RT_CV_DEVICE_ID=0
VLM_DEVICE_ID=1
LLM_DEVICE_ID=2      # device plugin overrides this — just a hint
NUM_SENSORS=1
```

**Precondition**: GPU 0 must NOT have a CUDA MPS server running.
Check: `nvidia-smi | grep MPS` — if anything shows for GPU 0, use Profile B instead.

---

### Profile B — 5+ GPUs, GPU 0 Has MPS (This Machine)

This machine runs a CUDA MPS server on GPU 0 (for other workloads).
DeepStream cannot share a GPU with MPS (gets CUDA error 35).

```
┌─────┬──────────────────────────────────┬──────────────────┐
│ GPU │ Workload                         │ .env var         │
├─────┼──────────────────────────────────┼──────────────────┤
│  0  │ MPS server — AVOID               │ —                │
│  1  │ LLM NIM ─┐ (device plugin picks) │ LLM=1 (hint)    │
│  2  │          ─┘                      │                  │
│  3  │ DeepStream (perception-alerts)   │ RT_CV=3          │
│  4  │ VLM NIM (cosmos-reason2-8b)      │ VLM=4            │
│ 5+  │ idle / spare                     │ —                │
└─────┴──────────────────────────────────┴──────────────────┘
```

`.env` for Profile B (current):
```bash
RT_CV_DEVICE_ID=3
VLM_DEVICE_ID=4
LLM_DEVICE_ID=1      # device plugin auto-picks 2 free GPUs from pool
NUM_SENSORS=1
```

---

### Profile C — 2 GPUs, Inference Only (No Live Perception)

Use when you have only 2 GPUs and want to run the agent + VLM pipeline
against pre-recorded/existing data, without live video analysis.

```
┌─────┬──────────────────────────────────┬────────────┐
│ GPU │ Workload                         │ .env var   │
├─────┼──────────────────────────────────┼────────────┤
│  0  │ VLM NIM (cosmos-reason2-8b)      │ VLM=0      │
│  1  │ LLM NIM ─┐ (nemotron-nano-9b)   │ LLM=0 *    │
│  0  │          ─┘ (shares with VLM?)  │            │
└─────┴──────────────────────────────────┴────────────┘
```

> Note: VLM and LLM cannot share a GPU. With only 2 GPUs (each 46 GiB):
> VLM on GPU 0 (~34 GiB used), LLM on GPU 1 with TP=2 won't work (needs 2 GPUs).
> 
> **Practical 2-GPU option**: Use cloud LLM (skip llm-nim entirely):

To use cloud LLM with 2 GPUs:
```bash
# 1. In k8s-alert-verification.yaml, vss-agent Deployment env:
- name: LLM_BASE_URL
  value: "https://integrate.api.nvidia.com/v1"
- name: LLM_NAME
  value: "nvidia/llama-3.1-nemotron-70b-instruct"

# 2. Set nvidia.com/gpu: 0 on llm-nim (or delete the deployment)
# 3. VLM on GPU 0: VLM_DEVICE_ID=0
# 4. Make sure NVIDIA_API_KEY is in vss-secrets
```

---

### Profile D — 6+ GPUs, Multiple Camera Streams

Run 2 simultaneous camera streams, each on its own GPU.

```
┌─────┬──────────────────────────────────┬────────────┐
│ GPU │ Workload                         │ .env var   │
├─────┼──────────────────────────────────┼────────────┤
│  0  │ LLM NIM ─┐                       │ LLM=0 *    │
│  1  │          ─┘ TP=2                 │            │
│  2  │ VLM NIM                          │ VLM=2      │
│  3  │ DeepStream — sensor 1            │ RT_CV=3    │
│  4  │ DeepStream — sensor 2 (copy pod) │ (manual)   │
│  5  │ spare                            │ —          │
└─────┴──────────────────────────────────┴────────────┘
```

`.env`:
```bash
RT_CV_DEVICE_ID=3
VLM_DEVICE_ID=2
LLM_DEVICE_ID=0
NUM_SENSORS=2
```

For sensor 2, duplicate the `perception-alerts` Deployment in `k8s-alert-verification.yaml`:
```yaml
# Copy the entire perception-alerts Deployment, rename to perception-alerts-2
# Change: NVIDIA_VISIBLE_DEVICES to "4"
# Change: name labels to perception-alerts-2
# Change: its PVC to perception-storage-2 (add a second PVC block too)
```

---

### Changing GPU Assignment

1. Edit `.env` with new GPU IDs
2. Re-apply and restart the affected pods:

```bash
# After editing .env:
bash 04-deploy.sh
kubectl -n vss-alerts rollout restart deployment/perception-alerts
kubectl -n vss-alerts rollout restart deployment/vlm-nim
# LLM NIM auto-gets reassigned by device plugin on next pod restart:
kubectl -n vss-alerts rollout restart deployment/llm-nim
```

Verify which GPUs are actually in use:
```bash
nvidia-smi
# Look for processes: python (NIM), ds-start.sh (DeepStream)
kubectl get nodes -o custom-columns="NODE:.metadata.name,GPUs:.status.capacity.nvidia\.com/gpu,Alloc:.status.allocatable.nvidia\.com/gpu"
```

---

## 5. Before You Deploy — Prerequisites

```bash
# 1. NVIDIA drivers installed
nvidia-smi
# Must show: driver version, GPU list with VRAM sizes

# 2. Docker installed and running
docker info

# 3. Enough disk space
df -h /var/lib/docker    # need 150+ GiB (NIM model caches ~80 GiB, images ~30 GiB)
df -h /home/shadeform    # need 10+ GiB (perception models, repo)

# 4. Check GPU count and VRAM
nvidia-smi --query-gpu=index,name,memory.total --format=csv,noheader
# Example output:
# 0, NVIDIA L40S, 46068 MiB
# 1, NVIDIA L40S, 46068 MiB
# ...

# 5. Check for MPS conflicts on your intended DeepStream GPU
# If GPU 0 has MPS, do NOT set RT_CV_DEVICE_ID=0
ps aux | grep nvidia-cuda-mps
```

Minimum hardware:
```
Full stack (live perception + VLM + LLM agent):  4× L40S or better
Inference only (no perception):                  2× L40S (with cloud LLM)
Storage:                                         200+ GiB free on /var/lib/docker
RAM:                                             32+ GiB system RAM
```

---

## 6. Fresh Deployment — Step by Step

### Phase 0 — Repo and Config

```bash
cd /home/shadeform
git clone <repo-url> video-search-and-summarization
cd video-search-and-summarization/k8-deployment

# Create environment file
cp .env.example .env
```

Edit `.env` — fill in every value:

```bash
# From https://org.ngc.nvidia.com/setup/api-key
NGC_CLI_API_KEY=nvapi-XXXXXXXXXXXXXXXX

# Docker registry that k3s can pull from
# For single-node: run a local registry on localhost:5000
YOUR_REGISTRY=localhost:5000

# IP of this machine — visible from your browser
# Do NOT use 127.0.0.1
NODE_IP=172.19.4.52

# Namespace (leave as-is)
NAMESPACE=vss-alerts

# GPU layout — check nvidia-smi before setting these
RT_CV_DEVICE_ID=3      # DeepStream — must NOT have MPS
VLM_DEVICE_ID=4        # VLM NIM
LLM_DEVICE_ID=1        # hint only — device plugin auto-assigns 2 GPUs for LLM
NUM_SENSORS=1          # concurrent camera streams
```

If using a local registry (single-node), start it now:

```bash
docker run -d -p 5000:5000 --name local-registry \
  --restart=always \
  -v /var/lib/docker/registry:/var/lib/registry \
  registry:2
```

---

### Phase 1 — Install Kubernetes (run once per machine)

Skip if k3s is already installed (`kubectl get nodes` works).

```bash
bash 00-install-k3s.sh
```

What it does:
- Installs k3s (lightweight single-node Kubernetes)
- Installs Helm
- Deploys NVIDIA GPU Operator (device plugin + container runtime)
- Configures the `nvidia` RuntimeClass so GPU pods work

Verify:
```bash
kubectl get nodes
# NAME          STATUS   ROLES                  AGE
# <hostname>    Ready    control-plane,master   2m

kubectl get nodes -o custom-columns="NAME:.metadata.name,GPUs:.status.capacity.nvidia\.com/gpu"
# NAME          GPUs
# <hostname>    8          ← should match your GPU count

kubectl -n gpu-operator get pods
# All pods: Running
```

---

### Phase 2 — Namespace, Secrets, Pull Credentials

```bash
bash 01-setup.sh
```

What it creates:
- Namespace `vss-alerts`
- `nvcr-secret` — image pull secret so k3s can pull from `nvcr.io` (uses your NGC key)
- `vss-secrets` — runtime API keys used by pods (NGC key, optional NVIDIA/OpenAI/HF keys)
- Docker login to nvcr.io (for image builds in the next step)

Verify:
```bash
kubectl get ns vss-alerts
kubectl -n vss-alerts get secrets
# nvcr-secret    kubernetes.io/dockerconfigjson
# vss-secrets    Opaque
```

---

### Phase 3 — Build Custom Images

```bash
bash 02-build-images.sh
```

Five images are built from source and pushed to `YOUR_REGISTRY`:

```
Image                          What it contains              Build time
──────────────────────────────────────────────────────────────────────
vss-elasticsearch:3.1.0        Elasticsearch + custom config  ~2 min
vss-elastic-init:3.1.0         ILM policy + index templates   ~1 min
vss-broker-health-check:3.1.0  Kafka readiness checker        ~1 min
vss-perception-alerts:3.1.0    DeepStream + RT-DETR + GDINO   ~10 min
                               (pulls vss-rt-cv:3.1.0 from NGC)
vss-kibana-init-alerts:3.1.0   Kibana dashboard JSON          ~1 min
```

Verify images are available:
```bash
curl -s http://localhost:5000/v2/_catalog | python3 -m json.tool
# Should list all 5 image names
```

---

### Phase 4 — Download Perception Models

```bash
bash 03-download-models.sh
```

Downloads ONNX model files to `deployments/data-dir/models/` (mounted as hostPath into perception pod):

```
deployments/data-dir/models/
├── rtdetr-its/
│   ├── model_epoch_035.fp16.onnx     ← RT-DETR object detector (~400 MB)
│   └── resnet50_market1501.etlt      ← Re-ID network (from image if missing)
└── gdino/
    └── mgdino_mask_head_pruned_dynamic_batch.onnx  ← Grounding DINO (~200 MB)
```

If `resnet50_market1501.etlt` is missing (perception crashes with "No such file"):
```bash
docker run --rm \
  -v /home/shadeform/video-search-and-summarization/deployments/data-dir/models:/out \
  --entrypoint bash localhost:5000/vss-perception-alerts:3.1.0 \
  -c "cp /opt/nvidia/deepstream/deepstream/sources/apps/sample_apps/metropolis_perception_app/models/rtdetr-its/resnet50_market1501.etlt /out/rtdetr-its/"
```

---

### Phase 5 — Deploy the Manifest

```bash
bash 04-deploy.sh
```

This runs `sed` to substitute all `CHANGE_ME_*` placeholders in `k8s-alert-verification.yaml`
using your `.env` values, then applies with `kubectl apply -f -`.

> **Never use** `kubectl apply -f <(envsubst < k8s-alert-verification.yaml)`.
> `envsubst` only handles `$VAR` syntax, not `CHANGE_ME_*` tokens. It will leave
> image names as `CHANGE_ME_YOUR_REGISTRY/vss-...` → `InvalidImageName` errors.

Watch pods start:
```bash
kubectl -n vss-alerts get pods -w
```

Expected startup sequence and timing:

```
Time    Pods becoming Ready
──────  ────────────────────────────────────────────────
0:00    kafka, elasticsearch, redis, postgres, phoenix
0:30    kafka-topic-init (job), elasticsearch-init (job)
1:00    kibana, logstash, vst-*, nvstreamer
2:00    kibana-init (job)
3:00    perception-alerts, perception-sdr, behavior-analytics
3:00    video-analytics-api, alert-verification
3:00    vss-agent-mcp
5–30m   llm-nim (startup probe: 130×10s = 22 min max)
5–30m   vlm-nim (startup probe: 130×10s = 22 min max)
+1min   vss-agent (waits for LLM), vss-agent-ui
```

NIM startup time:
- **First boot** (empty cache): 20–30 min (model download + profile selection)
- **Warm restart** (cache exists): 1–3 min

---

### Phase 6 — Wait for Full Readiness

```bash
bash 05-wait-verify.sh
```

Or check manually:
```bash
# All pods status
kubectl -n vss-alerts get pods --no-headers | awk '{print $2, $3, $1}' | sort | column -t

# Ready when this returns nothing:
kubectl -n vss-alerts get pods --no-headers | grep -v Completed | grep -v "1/1"

# Watch NIM loading progress:
kubectl -n vss-alerts logs -f deployment/llm-nim | grep -E "profile|Loading|GPU blocks|routes|OOM"
kubectl -n vss-alerts logs -f deployment/vlm-nim | grep -E "profile|Loading|GPU blocks|routes|OOM"
```

Healthy LLM NIM log sequence (TP=2):
```
Detected 2 compatible profile(s).
Selected profile: ...vllm-bf16-tp2-pp1...
tensor_parallel_size: 2
Loading weights took 4.01 seconds   ← fast if cache is warm
Memory profiling takes 55 seconds
# cuda blocks: 80126
Available routes are: /v1/chat/completions /v1/models /v1/completions
```

---

### Phase 7 — Load Video and Test

```bash
# 1. Upload a test video to NVStreamer
open http://172.19.4.52:31000/#/dashboard
# Click + → Upload → select sample-warehouse-ladder.mp4
# Click Play → copy the RTSP URL

# 2. Register the stream
open http://172.19.4.52:30301
# Video Management tab → + Add RTSP
# Paste the RTSP URL
# Wait ~30s for stream to register

# 3. Watch for detections in Kibana
open http://172.19.4.52:30561/app/home#/
# Open: ITS Dashboard
# After ~1-2 min, mdx-raw-* index should show frame count increasing

# 4. Chat with the agent
open http://172.19.4.52:30301
# Chat tab
# "Are there any PPE violations?"
# "Generate a safety report for the last hour"
# "Show me alerts from camera cam-01"
```

---

## 7. Verifying Everything Works

### Smoke Test Checklist

```bash
# 1. All pods 1/1 Running
kubectl -n vss-alerts get pods --no-headers | grep -v Completed | grep -v "1/1"
# Should return empty

# 2. Kafka topics exist
kubectl -n vss-alerts exec deployment/kafka -- \
  kafka-topics --list --bootstrap-server localhost:9092 | grep mdx
# Should list mdx-raw, mdx-incidents, mdx-vlm-alerts, etc.

# 3. Elasticsearch healthy
kubectl -n vss-alerts exec deployment/elasticsearch -- \
  curl -s http://localhost:9200/_cluster/health | python3 -m json.tool
# "status": "green" or "yellow" (yellow is ok for single-node)

# 4. LLM NIM responding
kubectl -n vss-alerts exec deployment/vss-agent -- \
  curl -s http://llm-nim:8000/v1/models | python3 -m json.tool
# Should list nvidia/nvidia-nemotron-nano-9b-v2

# 5. VLM NIM responding
kubectl -n vss-alerts exec deployment/alert-verification -- \
  curl -s http://vlm-nim:8000/v1/models | python3 -m json.tool
# Should list nvidia/cosmos-reason2-8b

# 6. Agent health
curl -s http://172.19.4.52:30301
# Should return the UI HTML (no 502/504)

# 7. Kibana dashboards imported
kubectl -n vss-alerts logs job/kibana-init | grep successCount
# "successCount":24
```

---

## 8. Tearing Down

### Standard Teardown (keeps NIM caches)

```bash
bash 06-teardown.sh
```

Deletes: all pods, services, PVCs, ConfigMaps, secrets, the namespace.

Does **NOT** delete:
- NIM model caches in Docker volumes (`/var/lib/docker/volumes/mdx_*_cache/`)
- Custom images in local Docker registry
- Pulled nvcr.io images cached by containerd

NIM caches are preserved on purpose — they take 1–3 hours to re-download.

---

### Full Teardown (reset everything)

```bash
# 1. Standard teardown first
bash 06-teardown.sh

# 2. Delete NIM caches (will force re-download on next deploy)
docker volume ls | grep mdx
docker volume rm mdx_nvidia_nemotron_nano_9b_v2_cache
docker volume rm mdx_cosmos_reason2_8b_cache

# 3. Remove custom images from local registry
for img in vss-perception-alerts vss-elasticsearch vss-elastic-init vss-broker-health-check vss-kibana-init-alerts; do
  docker rmi localhost:5000/${img}:3.1.0 2>/dev/null || true
done

# 4. Remove local registry (optional)
docker rm -f local-registry

# 5. Uninstall k3s (optional — removes all K8s)
sudo /usr/local/bin/k3s-uninstall.sh
```

---

### Partial Reset — Clear Data Without Restarting Services

Reset all alert data (Elasticsearch + Kafka) while keeping services running:

```bash
# Reset Elasticsearch indices (deletes all alert data)
kubectl -n vss-alerts delete pvc elasticsearch-data --wait=true
kubectl -n vss-alerts rollout restart deployment/elasticsearch
kubectl -n vss-alerts delete job elasticsearch-init --ignore-not-found=true
bash 04-deploy.sh
# Wait for elasticsearch-init job to complete

# Reset Kafka (deletes all queued messages)
kubectl -n vss-alerts delete pvc kafka-data --wait=true
kubectl -n vss-alerts rollout restart deployment/kafka
kubectl -n vss-alerts delete job kafka-topic-init --ignore-not-found=true
bash 04-deploy.sh
# Wait for kafka-topic-init job to complete

# Reset video storage (deletes all recorded clips)
kubectl -n vss-alerts delete pvc vst-data --wait=true
kubectl -n vss-alerts rollout restart deployment/vst-stream-processing
```

---

### Re-deploy After Config Change

```bash
# Edit k8s-alert-verification.yaml or .env, then:
bash 04-deploy.sh
# kubectl apply is idempotent — only changed resources update

# Then restart the specific service to pick up the change:
kubectl -n vss-alerts rollout restart deployment/<service-name>

# Watch rollout:
kubectl -n vss-alerts rollout status deployment/<service-name>
```

---

## 9. Changing Models

### Swap the LLM (used by vss-agent for reasoning)

Current: `nvidia/nvidia-nemotron-nano-9b-v2` (Mamba hybrid, requires 2 GPUs)

Edit `k8s-alert-verification.yaml` — two locations:

```yaml
# Location 1: llm-nim Deployment (the NIM container)
containers:
  - name: llm-nim
    image: nvcr.io/nim/meta/llama-3.1-8b-instruct:1    # ← new image

# Location 2: vss-agent Deployment env vars
- name: LLM_NAME
  value: "meta/llama-3.1-8b-instruct"                  # ← match model name
- name: LLM_BASE_URL
  value: "http://llm-nim:8000/v1"                       # unchanged
```

If new model fits on 1 GPU (e.g., llama-3.1-8b):
```yaml
# In llm-nim resources:
nvidia.com/gpu: 1    # ← change from 2 to 1
```

Apply:
```bash
bash 04-deploy.sh
kubectl -n vss-alerts rollout restart deployment/llm-nim deployment/vss-agent
```

---

### Use Cloud LLM (skip llm-nim entirely)

```yaml
# In vss-agent Deployment env:
- name: LLM_BASE_URL
  value: "https://integrate.api.nvidia.com/v1"
- name: LLM_NAME
  value: "nvidia/llama-3.1-nemotron-70b-instruct"
# NVIDIA_API_KEY must be in vss-secrets (set in 01-setup.sh from .env NVIDIA_API_KEY)
```

Also set `nvidia.com/gpu: 0` on llm-nim (or remove that Deployment entirely from the manifest).

---

### Swap the VLM (used by alert-verification for PPE checking)

Current: `nvidia/cosmos-reason2-8b`

Three locations to update:

```yaml
# 1. vlm-nim Deployment — the NIM container image
containers:
  - name: vlm-nim
    image: nvcr.io/nim/nvidia/llava-v1.6-mistral-7b:1.1   # ← new image

# 2. alert-verification-config ConfigMap (in k8s-alert-verification.yaml)
#    Find the config.yml data block, vlm section:
vlm:
  model: nvidia/llava-v1.6-mistral-7b                       # ← match model name
  base_url: http://vlm-nim:8000/v1

# 3. vss-agent Deployment env vars
- name: VLM_NAME
  value: "nvidia/llava-v1.6-mistral-7b"                     # ← match
```

Apply:
```bash
bash 04-deploy.sh
kubectl -n vss-alerts rollout restart deployment/vlm-nim deployment/alert-verification deployment/vss-agent
```

---

### Adjust LLM Context Window

```yaml
# In llm-nim Deployment env:
- name: NIM_MAX_MODEL_LEN
  value: "32768"    # default: 32K tokens
                    # 16384 → less KV cache memory (saves ~5 GiB per GPU)
                    # 65536 → longer conversations (needs more VRAM)
```

For nemotron Mamba hybrid: this controls only the **attention KV cache**.
The Mamba state cache size is unaffected by this setting.

---

### Swap Detection Models (DeepStream)

Model files live on the host at `deployments/data-dir/models/` and are mounted into the perception pod.

To replace a model:
```bash
# Replace the ONNX file:
cp /path/to/new-detector.onnx \
  /home/shadeform/video-search-and-summarization/deployments/data-dir/models/rtdetr-its/model_epoch_035.fp16.onnx

# Restart DeepStream to pick it up:
kubectl -n vss-alerts rollout restart deployment/perception-alerts
```

To change model filename or pipeline config (resolution, batch size, input size),
those are baked into the image — edit configs and rebuild:
```bash
# Edit: deployments/developer-workflow/dev-profile-alerts/deepstream/configs/
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

## 10. Customizing Alerts & Detection

### Change the VLM Verification Prompt

In `k8s-alert-verification.yaml`, find the `alert-verification-config` ConfigMap, `alert_type_config.json` key:

```json
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

Example — change to fire extinguisher compliance:
```json
{
  "alert_type": "FOV Count Violation",
  "output_category": "Fire Extinguisher Blocked",
  "prompts": {
    "system": "You are a safety compliance expert.",
    "user": "Is the fire extinguisher visible and completely unobstructed? Answer yes or no."
  }
}
```

Apply:
```bash
bash 04-deploy.sh
kubectl -n vss-alerts rollout restart deployment/alert-verification
```

---

### Change Behavior Analytics Thresholds

In `k8s-alert-verification.yaml`, `behavior-analytics-config` ConfigMap:

```yaml
fovCountViolationIncident:
  objectThreshold: 1     # min count to START tracking
  threshold: 2           # count that TRIGGERS incident emission
  expirationWindow: 0.5  # seconds without detection before incident closes
  objectType: person     # object class to watch
```

Examples:
```yaml
# More sensitive — trigger on any single person
objectThreshold: 1
threshold: 1
expirationWindow: 1.0

# Watch vehicles instead
objectType: vehicle
threshold: 1

# Less sensitive — require 3 persons sustained for 2 seconds
objectThreshold: 2
threshold: 3
expirationWindow: 2.0
```

Apply:
```bash
bash 04-deploy.sh
kubectl -n vss-alerts rollout restart deployment/behavior-analytics
```

---

### Change VLM Sampling Rate

In `alert-verification-config` ConfigMap, `config.yml` key, `vlm` section:

```yaml
vlm:
  enable_sampling: true
  sampling_fps: 4        # 4 fps × 10s = 40 frames per VLM call (default)
                         # 2 fps = 20 frames (faster, less accurate)
                         # 8 fps = 80 frames (slower, more accurate)
```

Video clip window:
```yaml
vst_config:
  segment_duration_seconds: 10   # seconds of video around incident
  segment_anchor: end            # clip ends at incident time ('end' or 'start')
```

---

### Edit Agent Behavior (no image rebuild needed)

The agent config file is on the host and mounted live into the pod:
```
deployments/developer-workflow/dev-profile-alerts/vss-agent/configs/config.yml
```

Edit it and restart — **no image rebuild**:
```bash
vim deployments/developer-workflow/dev-profile-alerts/vss-agent/configs/config.yml
kubectl -n vss-alerts rollout restart deployment/vss-agent
```

Key fields:
```yaml
llms:
  nim_llm:
    temperature: 0.0        # 0=deterministic responses, 1=creative
    max_tokens: 1024        # max tokens in each LLM response

workflow:
  max_iterations: 15        # max ReAct loop steps per user query
```

---

## 11. Re-deploying a Single Service

The manifest is idempotent — re-applying it only updates what changed.

```bash
# General pattern:
bash 04-deploy.sh                                        # apply manifest changes
kubectl -n vss-alerts rollout restart deployment/<name>  # restart pod to pick up change

# Restart specific services:
kubectl -n vss-alerts rollout restart deployment/alert-verification
kubectl -n vss-alerts rollout restart deployment/behavior-analytics
kubectl -n vss-alerts rollout restart deployment/vss-agent
kubectl -n vss-alerts rollout restart deployment/perception-alerts
kubectl -n vss-alerts rollout restart deployment/llm-nim
kubectl -n vss-alerts rollout restart deployment/vlm-nim

# Re-run a one-time init job (e.g., after Elasticsearch reset):
kubectl -n vss-alerts delete job kibana-init --ignore-not-found=true
bash 04-deploy.sh     # recreates it
kubectl -n vss-alerts wait --for=condition=complete job/kibana-init --timeout=5m
```

---

## 12. Troubleshooting Reference

### Quick Diagnostic Commands

```bash
# Pod status with restart counts (sorted by restarts desc)
kubectl -n vss-alerts get pods --no-headers \
  | awk '{print $4, $2, $3, $1}' | sort -rn | column -t

# Recent events (probe failures, scheduling errors, image pulls)
kubectl -n vss-alerts get events --sort-by='.lastTimestamp' | tail -30

# Logs (add --previous for crashed container logs)
kubectl -n vss-alerts logs -f deployment/<name>
kubectl -n vss-alerts logs -f deployment/<name> --previous

# Describe pod (node, resources, events, probe config)
kubectl -n vss-alerts describe pod <pod-name>

# GPU allocation
nvidia-smi
kubectl get nodes -o custom-columns="NODE:.metadata.name,GPU-cap:.status.capacity.nvidia\.com/gpu,GPU-alloc:.status.allocatable.nvidia\.com/gpu"
```

---

### LLM NIM: CUDA OOM (33.75 GiB allocation)

```
Error: CUDA out of memory. Tried to allocate 33.75 GiB.
```

Root cause: nemotron-nano-9b-v2 Mamba state cache cannot fit on 1 GPU.

```bash
# Verify manifest has 2 GPUs:
grep -A5 "name: llm-nim" k8s-alert-verification.yaml | grep "nvidia.com/gpu"
# Must show: nvidia.com/gpu: 2

# If not, re-apply:
bash 04-deploy.sh

# Verify 2 free GPUs exist:
nvidia-smi | grep -E "MiB|GPU"
# Need 2 GPUs with ~0 MiB used

# Restart if zombie processes hold GPU memory from a crashed pod:
kubectl -n vss-alerts rollout restart deployment/llm-nim
```

---

### Kafka: `KAFKA_PORT is deprecated` exit

```bash
# Verify fix is applied:
kubectl -n vss-alerts get deployment kafka -o yaml | grep enableServiceLinks
# Must show: enableServiceLinks: false

# If missing: re-apply manifest
bash 04-deploy.sh
kubectl -n vss-alerts rollout restart deployment/kafka
```

---

### Kafka PVC Corrupted (lock file after force-delete)

```bash
kubectl -n vss-alerts logs deployment/kafka | grep -iE "lock|meta.properties|error"
# Fix:
kubectl -n vss-alerts delete deployment kafka
kubectl -n vss-alerts delete pvc kafka-data
bash 04-deploy.sh
```

---

### Elasticsearch: 401 on Health Probes

```bash
# Verify security is disabled:
kubectl -n vss-alerts exec deployment/elasticsearch -- \
  curl -s http://localhost:9200/_cluster/health
# Must return JSON, not 401

# If returning 401: check manifest has xpack.security.enabled: "false"
grep "xpack.security" k8s-alert-verification.yaml
```

---

### kibana-init Job Fails

```bash
# Check logs:
kubectl -n vss-alerts logs job/kibana-init
# Success: "successCount":24
# Failure: "localhost refused" or "ECONNREFUSED"

# Re-run:
kubectl -n vss-alerts delete job kibana-init --ignore-not-found=true
bash 04-deploy.sh
kubectl -n vss-alerts wait --for=condition=complete job/kibana-init --timeout=3m
```

---

### perception-alerts: Missing model file

```bash
kubectl -n vss-alerts logs deployment/perception-alerts | grep -i "error\|No such"
# If: "resnet50_market1501.etlt: No such file or directory"
docker run --rm \
  -v /home/shadeform/video-search-and-summarization/deployments/data-dir/models:/out \
  --entrypoint bash localhost:5000/vss-perception-alerts:3.1.0 \
  -c "cp .../resnet50_market1501.etlt /out/rtdetr-its/"
kubectl -n vss-alerts rollout restart deployment/perception-alerts
```

---

### perception-alerts: CUDA Error 35 (MPS Conflict)

```bash
# Identify which GPU has MPS:
ps aux | grep nvidia-cuda-mps
nvidia-smi  # look for MPS server process

# Move DeepStream off that GPU:
# Edit .env: RT_CV_DEVICE_ID=<different gpu>
bash 04-deploy.sh
kubectl -n vss-alerts rollout restart deployment/perception-alerts
```

---

### NIM Takes 22+ Minutes (startup probe expired)

```bash
# Check cache state:
du -sh /var/lib/docker/volumes/mdx_nvidia_nemotron_nano_9b_v2_cache/_data/
# >10 GiB means cache is warm → startup should be <3 min
# <1 GiB means cold → first boot, 20–30 min expected

# Watch progress:
kubectl -n vss-alerts logs -f deployment/llm-nim \
  | grep -E "profile|Loading|GPU blocks|routes|OOM|Error"
```

---

### Wrong `envsubst` Command Used

Symptom: pods show `InvalidImageName` with `CHANGE_ME_YOUR_REGISTRY/vss-...`

```bash
# Fix: undo the bad apply
kubectl -n vss-alerts rollout undo deployment/elasticsearch deployment/kibana deployment/perception-alerts

# Then always use:
bash 04-deploy.sh      # uses sed, handles CHANGE_ME_* tokens
```

---

### perception-alerts: ONNX files are empty directories / trtexec never runs

**Symptom**: CrashLoopBackOff, Triton logs `unable to find 'gdino_trt/1/model.plan'`, no `trtexec` output.

**Root cause**: `03-download-models.sh` uses the NGC CLI (v3.52.0+) which requires `--org` when authenticated. The CLI silently fails and `mkdir -p` creates empty directories at the hostPath. Additionally, the job targets a `models-data` PVC while perception-alerts uses a **hostPath** (`deployments/data-dir/models/`) — a completely different mount. `cp *.onnx` inside `ds-start.sh` silently skips directories, so `trtexec` is never invoked.

**Diagnose**:
```bash
ls -la /home/shadeform/video-search-and-summarization/deployments/data-dir/models/gdino/
ls -la /home/shadeform/video-search-and-summarization/deployments/data-dir/models/rtdetr-its/
# If entries are directories (drwx...) instead of files → apply fix below
```

**Fix**: Download via NGC REST API (bypasses NGC CLI org issue):
```bash
rm -rf deployments/data-dir/models/gdino/mgdino_mask_head_pruned_dynamic_batch.onnx
rm -rf deployments/data-dir/models/rtdetr-its/model_epoch_035.fp16.onnx

# GDINO (~686 MB)
curl -L -o deployments/data-dir/models/gdino/mgdino_mask_head_pruned_dynamic_batch.onnx \
  -H "Authorization: ApiKey $NGC_CLI_API_KEY" \
  "https://api.ngc.nvidia.com/v2/models/nvidia/tao/mask_grounding_dino/versions/mask_grounding_dino_swin_tiny_commercial_deployable_v2.1_wo_mask_arm/files/mgdino_mask_head_pruned_dynamic_batch.onnx"

# RT-DETR (~84 MB)
curl -L -o deployments/data-dir/models/rtdetr-its/model_epoch_035.fp16.onnx \
  -H "Authorization: ApiKey $NGC_CLI_API_KEY" \
  "https://api.ngc.nvidia.com/v2/models/nvidia/tao/trafficcamnet_transformer_lite/versions/deployable_resnet50_v2.0/files/resnet50_trafficcamnet_rtdetr.fp16.onnx"

kubectl -n vss-alerts rollout restart deployment/perception-alerts
# First boot: trtexec runs ~4 min, then DeepStream starts
# Subsequent boots: plan loaded from perception-storage PVC, instant
```

---

### perception-alerts: DeepStream uses `localhost:9092` for Kafka

**Symptom**: Perception starts, but Kibana `mdx-raw-*` index is empty. Logs show `msg-broker-conn-str=localhost;9092;mdx-raw`.

**Root cause**: `run_config-api-rtdetr-protobuf.txt` inside the image hardcodes `localhost;9092`. K8s DNS requires `kafka;9092`.

**Fix** (already applied): startup command in manifest patches the file before invoking `ds-start.sh`:
```bash
sed -i 's/msg-broker-conn-str=localhost;9092/msg-broker-conn-str=kafka;9092/g' run_config-api-rtdetr-protobuf.txt
```

Verify:
```bash
kubectl -n vss-alerts logs deployment/perception-alerts | grep "msg-broker-conn-str"
# Must show: msg-broker-conn-str=kafka;9092;mdx-raw
```

---

### perception-sdr: `localhost:6379 Connection refused` (Redis — non-critical)

**Symptom**: SDR logs repeatedly show `redis.exceptions.ConnectionError: Error 111 connecting to localhost:6379`.

**Root cause**: The SDR binary is a compiled PyInstaller executable with `localhost:6379` baked in. K8s service-link env vars (`REDIS_SERVICE_HOST`) are injected but ignored by the binary.

**Assessment**: **Non-critical — safe to ignore.** SDR operates via `WDM_INITIALIZE_FROM_VST=true`, meaning it uses the VST REST API to discover streams. Redis is only used for a destructor cleanup path (`Exception ignored in: <function Consumer.__del__>`). RTSP stream routing to perception-alerts functions normally despite these errors.

---

## 13. Quick-Reference Cheatsheet

```bash
# ─── Deploy ───────────────────────────────────────────────────────
bash 00-install-k3s.sh          # first time: install k3s + GPU operator
bash 01-setup.sh                # namespace + secrets
bash 02-build-images.sh         # build 5 custom images
bash 03-download-models.sh      # download ONNX model files
bash 04-deploy.sh               # apply manifest (ALWAYS use this, not envsubst)
bash 05-wait-verify.sh          # wait for all services + print URLs

# ─── Teardown ──────────────────────────────────────────────────────
bash 06-teardown.sh             # delete namespace + all resources

# ─── Status ────────────────────────────────────────────────────────
kubectl -n vss-alerts get pods --no-headers | awk '{print $2,$3,$1}' | sort | column -t
kubectl -n vss-alerts get events --sort-by='.lastTimestamp' | tail -20
nvidia-smi

# ─── Logs ──────────────────────────────────────────────────────────
kubectl -n vss-alerts logs -f deployment/llm-nim
kubectl -n vss-alerts logs -f deployment/vlm-nim
kubectl -n vss-alerts logs -f deployment/alert-verification
kubectl -n vss-alerts logs -f deployment/vss-agent
kubectl -n vss-alerts logs -f deployment/perception-alerts
kubectl -n vss-alerts logs -f deployment/behavior-analytics

# ─── Restart Services ──────────────────────────────────────────────
kubectl -n vss-alerts rollout restart deployment/<name>
kubectl -n vss-alerts rollout status  deployment/<name>

# ─── Re-run Init Jobs ──────────────────────────────────────────────
kubectl -n vss-alerts delete job kibana-init && bash 04-deploy.sh
kubectl -n vss-alerts delete job kafka-topic-init && bash 04-deploy.sh
kubectl -n vss-alerts delete job elasticsearch-init && bash 04-deploy.sh

# ─── URLs (NODE_IP=172.19.4.52) ────────────────────────────────────
# VSS Agent UI    →  http://172.19.4.52:30301
# VST Dashboard   →  http://172.19.4.52:30888/vst/#/dashboard
# NVStreamer UI   →  http://172.19.4.52:31000/#/dashboard
# Kibana          →  http://172.19.4.52:30561/app/home#/
# Phoenix traces  →  http://172.19.4.52:30606/projects

# ─── .env Keys ─────────────────────────────────────────────────────
# RT_CV_DEVICE_ID   GPU index for DeepStream (must not have MPS)
# VLM_DEVICE_ID     GPU index for VLM NIM (cosmos-reason2-8b)
# LLM_DEVICE_ID     GPU hint for LLM NIM (device plugin overrides for 2-GPU TP)
# NUM_SENSORS       concurrent camera streams
# NODE_IP           machine IP for NodePort URLs
# YOUR_REGISTRY     Docker registry k3s pulls custom images from
# NGC_CLI_API_KEY   NGC API key for pulling NIM images and model profiles
```
