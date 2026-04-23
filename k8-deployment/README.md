# VSS Alert Verification Workflow — Kubernetes Deployment

Converts the `scripts/dev-profile.sh up -p alerts -m verification` Docker Compose deployment into Kubernetes manifests for a single-node cluster.

## What gets deployed

| Service | Image | GPU |
|---|---|---|
| Kafka | confluentinc/cp-kafka:8.1.1 | — |
| Elasticsearch | `vss-elasticsearch:3.1.0` (custom) | — |
| Redis | redis:8.2.2-alpine | — |
| Kibana | docker.elastic.co/kibana/kibana:8.12.0 | — |
| Logstash | docker.elastic.co/logstash/logstash:8.12.0 | — |
| Phoenix (telemetry) | arizephoenix/phoenix:8.12.1 | — |
| PostgreSQL | postgres:17.6-alpine | — |
| VST Sensor | `vss-vios-sensor:3.1.0` | — |
| VST Ingress | `vss-vios-ingress:3.1.0` | — |
| VST Stream Processing | `vss-vios-streamprocessing:3.1.0` | — |
| VST MCP | `vss-vios-mcp:3.1.0` | — |
| NVStreamer | `vss-vios-nvstreamer:3.1.0` | — |
| SDR (Stream Data Router) | `sdr:3.1.0` | — |
| Perception (DeepStream) | `vss-perception-alerts:3.1.0` (custom) | 1 GPU (`RT_CV_DEVICE_ID`) |
| Behavior Analytics | `vss-behavior-analytics:3.1.0` | — |
| Video Analytics API | `vss-video-analytics-api:3.1.0` | — |
| Alert Verification | `vss-alert-verification:3.1.0` | — |
| LLM NIM (Nemotron 9B) | `nim/nvidia-nemotron-nano-9b-v2:1` | **2 GPUs** (TP=2, see note) |
| VLM NIM (Cosmos Reason 8B) | `nim/nvidia/cosmos-reason2-8b:1.6.0` | 1 GPU (`VLM_DEVICE_ID`) |
| VSS Agent MCP | `vss-agent:3.1.0` | — |
| VSS Agent | `vss-agent:3.1.0` | — |
| VSS Agent UI | `vss-agent-ui:3.1.0` | — |

### About `vss-rt-cv` and Perception

`vss-rt-cv` is **not a separate running service**. It is the upstream NVIDIA base image used when building `vss-perception-alerts`. The build command is:

```bash
docker build -f deployments/developer-workflow/dev-profile-alerts/Dockerfiles/perception.Dockerfile \
  --build-arg PERCEPTION_IMAGE=nvcr.io/nvidia/vss-core/vss-rt-cv \
  --build-arg PERCEPTION_TAG=3.1.0 \
  deployments/developer-workflow/dev-profile-alerts
```

The resulting `vss-perception-alerts:3.1.0` image contains both DeepStream and the metropolis perception app. It runs entirely inside Kubernetes as the `perception-alerts` Deployment — no Docker container is spawned separately.

The **SDR** (Stream Data Router, `perception-sdr`) also runs as a K8s pod. It mounts `/var/run/docker.sock` for legacy compatibility but its primary role in this K8s deployment is to watch VST for stream add/remove events and forward them to the perception pod via HTTP on port 9010.

## Prerequisites

- NVIDIA L40S (or equivalent) GPUs — minimum **5 GPUs** for this workload:
  - 1 for DeepStream perception
  - 1 for VLM NIM
  - **2 for LLM NIM** (Mamba hybrid model requires tensor parallelism — see GPU note)
  - 1 spare / for other processes
- NVIDIA drivers installed on the host
- Docker installed (SDR mounts the host Docker socket)
- `kubectl` configured (single-node: run `00-install-k3s.sh`)
- NGC account with API key
- ~150 GB free disk space (NIM model caches)

### GPU Assignment

`nvidia-nemotron-nano-9b-v2` is a **Mamba hybrid model** (SSM + attention layers). Its recurrent state cache is allocated per-sequence — not per-token — and requires ~34 GiB on a single L40S (46 GiB), leaving insufficient headroom after loading 17.5 GiB of weights. Reducing `max_model_len` does **not** fix this.

The fix is tensor parallelism: with `nvidia.com/gpu: 2`, NIM auto-selects the TP=2 profile which splits weights (~8.3 GiB/GPU) and state cache (~10 GiB/GPU) across both GPUs. Each GPU uses ~18 GiB, well within the 46 GiB limit.

DeepStream **cannot share a GPU with an active CUDA MPS server**. On this machine GPU 0 runs the MPS server, so DeepStream is assigned to GPU 3.

Confirmed working GPU layout for this machine:

| Container | GPU(s) | `.env` var |
|---|---|---|
| DeepStream Perception | 3 | `RT_CV_DEVICE_ID=3` |
| VLM NIM (Cosmos 8B) | 4 | `VLM_DEVICE_ID=4` |
| LLM NIM (Nemotron 9B) | auto (2 GPUs) | `LLM_DEVICE_ID=1` (informational only — K8s device plugin overrides) |

For LLM NIM, the Kubernetes NVIDIA device plugin allocates 2 free GPUs automatically. The `LLM_DEVICE_ID` value in `.env` is substituted into the manifest but has no effect since the device plugin overrides `NVIDIA_VISIBLE_DEVICES` when `nvidia.com/gpu: 2` is requested.

## Quick Start

```bash
# 1. Configure environment
cp .env.example .env
# Edit .env — fill in NGC_CLI_API_KEY, YOUR_REGISTRY, NODE_IP
# Key values for this machine:
#   NODE_IP=172.19.4.52
#   RT_CV_DEVICE_ID=3
#   VLM_DEVICE_ID=4

# 2. (First time only) Install k3s + GPU Operator
bash 00-install-k3s.sh

# 3. Create namespace, secrets, and RBAC
bash 01-setup.sh

# 4. Build and push the 5 custom images to YOUR_REGISTRY
bash 02-build-images.sh

# 5. Apply the manifest
bash 04-deploy.sh

# 6. Download ONNX models into the models hostPath
bash 03-download-models.sh

# 7. Wait for all services and print URLs
bash 05-wait-verify.sh
```

## Service URLs

| Service | URL |
|---|---|
| VSS Agent UI | `http://<NODE_IP>:30301` |
| VST Dashboard | `http://<NODE_IP>:30888/vst/#/dashboard` |
| NVStreamer UI | `http://<NODE_IP>:31000/#/dashboard` |
| Kibana | `http://<NODE_IP>:30561/app/home#/` |
| Phoenix (telemetry) | `http://<NODE_IP>:30606/projects` |

## Workflow (after all pods are Ready)

1. Open VSS Agent UI at `http://<NODE_IP>:30301`
2. Go to **Video Management** → **+ Add RTSP**
3. Get the RTSP URL from NVStreamer UI (`rtsp://<NODE_IP>:31554/...`)
4. Upload `sample-warehouse-ladder.mp4` via NVStreamer UI if no live camera is available
5. Open Kibana → Discover and verify these indices are populating:
   - `mdx-raw-*` — raw detection events
   - `mdx-incidents-*` — behavior analytics alerts
   - `mdx-vlm-incidents-*` — VLM-verified alerts
6. Go to the **Alerts** tab → **Verified Alerts**
7. Chat: `Generate a report for alert <id>`

## Teardown

```bash
bash 06-teardown.sh
```

## Key Differences from Docker Compose

| Docker Compose | Kubernetes |
|---|---|
| `network_mode: host` | ClusterIP services; pods use DNS names |
| `localhost:9092` | `kafka:9092` |
| `localhost:9200` | `elasticsearch:9200` |
| `localhost:5601` | `kibana:5601` |
| `localhost:30888` | `vst-ingress` (ClusterIP) |
| `localhost:6379` | `redis:6379` |
| `--device-id` GPU flag | `NVIDIA_VISIBLE_DEVICES` + `nvidia.com/gpu` resource limit |
| Docker socket (SDR) | SDR K8s pod mounts `/var/run/docker.sock` from host |
| `${HOST_IP}` in config files | All configs use K8s DNS names in ConfigMaps |
| `env-substitute.py` writes runtime config | Command overridden; pre-resolved ConfigMap mounted at `/app/runtime/config.yml` |
| Named volumes | PersistentVolumeClaims (k3s local-path, `ReadWriteOnce`) |
| Kafka: env var `KAFKA_PORT` | `enableServiceLinks: false` — prevents K8s from injecting `KAFKA_PORT=tcp://...` which breaks Kafka startup |
| Elasticsearch: security enabled by default in 8.x | `xpack.security.enabled: "false"` env var required for unauthenticated health probes |

## Custom Images

Five images must be built from the repo Dockerfiles before deploying:

| Image | Dockerfile |
|---|---|
| `vss-elasticsearch:3.1.0` | `deployments/foundational/Dockerfiles/elasticsearch.Dockerfile` |
| `vss-elastic-init:3.1.0` | `deployments/foundational/Dockerfiles/elastic-init.Dockerfile` |
| `vss-broker-health-check:3.1.0` | `deployments/foundational/Dockerfiles/kafka-health-check.Dockerfile` |
| `vss-perception-alerts:3.1.0` | `deployments/developer-workflow/dev-profile-alerts/Dockerfiles/perception.Dockerfile` |
| `vss-kibana-init-alerts:3.1.0` | `deployments/developer-workflow/dev-profile-alerts/Dockerfiles/kibana-dashboard.Dockerfile` |

`02-build-images.sh` handles all of these automatically.

## Troubleshooting

```bash
# Check all pod statuses
kubectl -n vss-alerts get pods

# Follow logs for a specific pod
kubectl -n vss-alerts logs -f deployment/vss-agent

# Describe a pod for scheduling events / errors
kubectl -n vss-alerts describe pod <pod-name>

# Check GPU allocation
kubectl get nodes -o custom-columns="NAME:.metadata.name,GPUs:.status.capacity.nvidia\.com/gpu"
nvidia-smi
```

### LLM NIM: CUDA out of memory

**Symptom**: `CUDA out of memory. Tried to allocate 33.75 GiB. GPU has a total capacity of 44.40 GiB of which 26.88 GiB is free.`

**Cause**: `nvidia-nemotron-nano-9b-v2` is a Mamba hybrid model. The recurrent SSM state cache is per-sequence and fixed at ~34 GiB regardless of context length. Reducing `NIM_MAX_MODEL_LEN` does not help.

**Fix**: The manifest already has `nvidia.com/gpu: 2`. NIM auto-selects the TP=2 profile which distributes model and cache across both GPUs (~18 GiB per GPU). Ensure at least 2 free GPUs are available when LLM NIM starts.

If you see this error, check GPU utilization:
```bash
nvidia-smi
# Ensure at least 2 GPUs show ~0 MiB used
```

If a previous crashed LLM NIM pod left GPU memory allocated (zombie processes), restart it:
```bash
kubectl -n vss-alerts rollout restart deployment/llm-nim
```

### Kafka: `Port is deprecated` exit on startup

**Symptom**: Kafka pod exits immediately with `KAFKA_PORT` deprecation error.

**Cause**: Kubernetes auto-injects service-discovery env vars (e.g., `KAFKA_PORT=tcp://10.x.x.x:9092`) for all services in the namespace. Kafka treats any env var named `KAFKA_*_PORT` with a non-integer value as a fatal configuration error.

**Fix** (already applied in manifest): `enableServiceLinks: false` in the Kafka pod spec prevents K8s from injecting these vars.

### Kafka PVC lock corruption after force-delete

**Symptom**: New Kafka pod fails to start because `meta.properties` or `.lock` file is held by a deleted pod.

**Fix**:
```bash
kubectl -n vss-alerts delete pvc kafka-data
kubectl -n vss-alerts rollout restart deployment/kafka
```

### Elasticsearch: liveness probe keeps killing the pod

**Symptom**: Elasticsearch restarts repeatedly; `curl /_cluster/health` returns `401 Unauthorized`.

**Cause**: Elasticsearch 8.x has security (TLS + auth) enabled by default. The HTTP probe receives 401 and treats it as failure.

**Fix** (already applied): `xpack.security.enabled: "false"` in the Elasticsearch deployment env vars.

### kibana-init job: `Unable to connect to ES` or dashboard import fails

**Symptom**: Job pod errors with connection refused or localhost resolution failure.

**Cause**: The `kibana-import-dashboard.sh` script inside `vss-kibana-init-alerts` hardcodes `localhost:9200` and `localhost:5601`. In Kubernetes, pods cannot reach other pods via `localhost`.

**Fix** (already applied): The job overrides the container command to `sed` the script before running it:
```bash
sed -i 's|localhost:9200|elasticsearch:9200|g; s|localhost:5601|kibana:5601|g' \
  /opt/mdx/init-scripts/kibana-import-dashboard.sh
exec bash /opt/mdx/init-scripts/kibana-import-dashboard.sh
```

To rerun the job after a failure:
```bash
kubectl -n vss-alerts delete job kibana-init
bash 04-deploy.sh
```

### vst-ingress: readiness probe keeps failing

**Symptom**: `vst-ingress` pod never becomes Ready; events show readiness probe failures on port 30888.

**Cause**: The nginx container inside `vss-vios-ingress` listens on port **80** internally. Port 30888 is the NodePort exposed by the Service, not the container port.

**Fix** (already applied): container `readinessProbe` checks port 80; Service `targetPort` is 80.

### Perception-alerts: missing model file `resnet50_market1501.etlt`

**Symptom**: `ds-start.sh` exits early with a missing file error for `rtdetr-its/resnet50_market1501.etlt`.

**Cause**: The models volume mounts over the directory that normally contains this file inside the image.

**Fix**: Copy the file from the image to the host models directory:
```bash
docker run --rm \
  -v /home/shadeform/video-search-and-summarization/deployments/data-dir/models:/out \
  --entrypoint bash localhost:5000/vss-perception-alerts:3.1.0 \
  -c "cp /opt/nvidia/deepstream/deepstream/sources/apps/sample_apps/metropolis_perception_app/models/rtdetr-its/resnet50_market1501.etlt /out/rtdetr-its/resnet50_market1501.etlt"
```

### Do not use `envsubst` to apply the manifest

**Wrong**:
```bash
kubectl apply -f <(envsubst < k8s-alert-verification.yaml)   # DO NOT USE
```

`envsubst` only substitutes `$VAR` shell syntax. The manifest uses `CHANGE_ME_*` placeholders that require `sed`. Using `envsubst` leaves these placeholders literal and creates `InvalidImageName` errors on re-deployed pods.

**Always use**:
```bash
bash 04-deploy.sh
```

### NIM startup takes too long / startup probe fails

NIM containers can take **10–25 minutes** on first boot (model profile selection + cache setup). On subsequent boots with a warm cache the startup time is 1–3 minutes.

The startup probe is configured with 130 attempts × 10s = ~22 minutes. If the probe still fails:
```bash
# Check if NIM is loading or erroring
kubectl -n vss-alerts logs -f deployment/llm-nim
kubectl -n vss-alerts logs -f deployment/vlm-nim
```

### perception-alerts: ONNX model files are empty directories / trtexec never runs

**Symptom**: Perception-alerts enters CrashLoopBackOff. Logs show Triton failing with `unable to find 'gdino_trt/1/model.plan'`. No `trtexec` output appears at all.

**Cause**: `03-download-models.sh` uses the NGC CLI which (v3.52.0+) requires `--org` to be specified when authenticated. The CLI silently fails and `mkdir -p` leaves empty directories at the hostPath location. Additionally, the job downloads to a `models-data` PVC while perception-alerts uses a **hostPath** (`deployments/data-dir/models/`) — a completely different storage location. The `cp *.onnx /opt/storage/` inside `ds-start.sh` silently skips directories (`cp: -r not specified; omitting directory`), so `trtexec` is never called, and DeepStream starts without the plan.

**Diagnosis**:
```bash
# Check if ONNX paths are files or directories:
ls -la /home/shadeform/video-search-and-summarization/deployments/data-dir/models/gdino/
ls -la /home/shadeform/video-search-and-summarization/deployments/data-dir/models/rtdetr-its/
# If you see `drwxr-xr-x` entries instead of files, they're empty dirs — fix below
```

**Fix**: Remove the empty directories and download directly via NGC REST API:
```bash
# Remove empty directories
rm -rf /home/shadeform/video-search-and-summarization/deployments/data-dir/models/gdino/mgdino_mask_head_pruned_dynamic_batch.onnx
rm -rf /home/shadeform/video-search-and-summarization/deployments/data-dir/models/rtdetr-its/model_epoch_035.fp16.onnx

# Download GDINO model (~686 MB)
curl -L -o /home/shadeform/video-search-and-summarization/deployments/data-dir/models/gdino/mgdino_mask_head_pruned_dynamic_batch.onnx \
  -H "Authorization: ApiKey $NGC_CLI_API_KEY" \
  "https://api.ngc.nvidia.com/v2/models/nvidia/tao/mask_grounding_dino/versions/mask_grounding_dino_swin_tiny_commercial_deployable_v2.1_wo_mask_arm/files/mgdino_mask_head_pruned_dynamic_batch.onnx"

# Download RT-DETR model (~84 MB)
curl -L -o /home/shadeform/video-search-and-summarization/deployments/data-dir/models/rtdetr-its/model_epoch_035.fp16.onnx \
  -H "Authorization: ApiKey $NGC_CLI_API_KEY" \
  "https://api.ngc.nvidia.com/v2/models/nvidia/tao/trafficcamnet_transformer_lite/versions/deployable_resnet50_v2.0/files/resnet50_trafficcamnet_rtdetr.fp16.onnx"

kubectl -n vss-alerts rollout restart deployment/perception-alerts
```

On first start after this fix, `ds-start.sh` runs `trtexec` to compile the ONNX → `.plan` file (~4 minutes on L40S). The compiled plan is stored on the `perception-storage` PVC and reused on all subsequent restarts.

### perception-alerts: DeepStream connects to `localhost:9092` instead of Kafka

**Symptom**: Perception-alerts starts but no events appear in Kibana `mdx-raw-*`. Logs show `msg-broker-conn-str=localhost;9092;mdx-raw`. Connection to Kafka fails silently.

**Cause**: `run_config-api-rtdetr-protobuf.txt` baked into the `vss-perception-alerts:3.1.0` image hardcodes `localhost;9092` as the Kafka broker. In Kubernetes, `localhost` does not resolve to Kafka — the correct DNS name is `kafka`.

**Fix** (already applied in manifest): The perception-alerts Deployment overrides the startup command to patch the broker address before calling `ds-start.sh`:
```yaml
command:
  - bash
  - -c
  - |
    sed -i 's/msg-broker-conn-str=localhost;9092/msg-broker-conn-str=kafka;9092/g' run_config-api-rtdetr-protobuf.txt
    exec bash ds-start.sh run_config-api-rtdetr-protobuf.txt
```

Verify:
```bash
kubectl -n vss-alerts logs deployment/perception-alerts | grep "msg-broker-conn-str"
# Should show: msg-broker-conn-str=kafka;9092;mdx-raw
```

### perception-sdr: `localhost:6379 Connection refused` (Redis)

**Symptom**: `perception-sdr` logs show `redis.exceptions.ConnectionError: Error 111 connecting to localhost:6379. Connection refused.` on every startup.

**Cause**: The SDR binary (`/wdm/dist/sdr`) is a compiled PyInstaller binary with `localhost:6379` hardcoded. Kubernetes injects `REDIS_SERVICE_HOST` via service links but the binary ignores it.

**Assessment**: **Non-critical.** `WDM_INITIALIZE_FROM_VST=true` means SDR uses the VST REST API as its primary stream source, not Redis. The Redis errors come from a destructor cleanup path (`Exception ignored in: <function Consumer.__del__>`) and do not affect SDR's core function of routing RTSP streams from VST to the perception-alerts HTTP API.

No fix available (compiled binary; no environment variable to override the Redis host). Safe to ignore.
