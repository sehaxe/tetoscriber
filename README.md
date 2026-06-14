# TetoScribe

TetoScribe is a local-first speech pipeline for turning audio into named-speaker Markdown transcripts. The current repository is a Rust workspace with:

- `teto-worker`: Redis-backed archive/live dispatcher and optional NVIDIA Riva ASR client
- `teto-controller`: Redis stream processor, Qdrant memory, and `/ws/live` WebSocket endpoint
- `teto-brain`: Redis identity resolver with a regex fallback and a local command backend boundary
- `teto-protocol`: shared serde schemas for Redis, Qdrant, Brain, and WebSocket messages

The stack is designed for sovereign/local operation: Redis for tasking, Qdrant for voice/transcript memory, Riva for ASR/diarization, and no cloud ASR/LLM/vector defaults.

## Quick start

### Build and test without ASR

You can build and test the Rust workspace before setting up GPU services:

```bash
cargo check
cargo test --workspace
```

### Local full-stack setup

For a local x86_64 Linux machine with an NVIDIA GPU, the intended path is:

```text
Redis -> TetoScribe worker -> Speech NIM ASR gRPC -> Redis -> TetoScribe controller -> WebSocket
```

#### 1. Install Arch Linux base dependencies

On Arch Linux, run:

```bash
sudo ./scripts/install-arch-deps.sh
```

This installs the base packages needed to build TetoScribe and run the Docker-based local stack:

- `base-devel`
- `cargo`
- `curl`
- `docker`
- `git`
- `redis`
- `rust`
- `sqlite`

It also adds your user to the `docker` group if needed. Log out and back in after that change.

This script intentionally does **not** install NVIDIA drivers or the NVIDIA Container Toolkit, because Arch has multiple valid driver paths such as `nvidia`, `nvidia-dkms`, `nvidia-lts`, and `nvidia-open`.

#### 2. Install and verify NVIDIA GPU access

Install the NVIDIA driver and NVIDIA Container Toolkit for your kernel. Then verify the host and Docker can see the GPU:

```bash
nvidia-smi
docker run --rm --runtime=nvidia --gpus all ubuntu nvidia-smi
```

The Docker command should print your driver version, CUDA version, and available GPU(s).

#### 3. Configure NGC access with `.env`

Create a local environment file:

```bash
cp .env.example .env
```

Edit `.env` and put your NGC API key there:

```text
NGC_API_KEY=<your-key-value>
```

`.env` is ignored by Git, so it will not be committed. The helper scripts load it automatically.

Authenticate Docker with NVIDIA Container Registry:

```bash
set -a
source .env
set +a

echo "$NGC_API_KEY" | docker login nvcr.io --username '$oauthtoken' --password-stdin
```

The username is the literal `$oauthtoken`; the password is the NGC API key value.

#### 4. Start local services

From the repository root:

```bash
./scripts/start-local.sh
```

This script starts:

- Redis on `redis://127.0.0.1/`
- Qdrant on `http://localhost:6334`
- Speech NIM ASR gRPC on `http://127.0.0.1:50051`
- Speech NIM HTTP/WebSocket on `http://127.0.0.1:9000`

It also waits for Speech NIM readiness through:

```bash
curl http://127.0.0.1:9000/v1/health/ready
```

The first Speech NIM start downloads the model and may build a TensorRT engine for your GPU. This can take 30-45 minutes. Later starts are faster because the model is cached.

You can also set these in `.env` instead of passing them on every command:

```text
CONTAINER_ID=parakeet-1-1b-ctc-en-us
NIM_TAGS_SELECTOR="name=parakeet-1-1b-ctc-en-us,mode=all"
NIM_GRPC_API_PORT=50051
NIM_HTTP_API_PORT=9000
```

`mode=all` enables both streaming and offline inference. It uses more GPU memory, but it is convenient for local testing. If you only need live streaming, use:

```text
NIM_TAGS_SELECTOR="name=parakeet-1-1b-ctc-en-us,mode=str"
```

If the Parakeet CTC model reports `Too many open files`, keep this in `.env`:

```text
NIM_ULIMIT_NOFILE=2048:2048
```

#### 4.1. Check Speech NIM readiness and copy a sample file

Check readiness manually:

```bash
./scripts/check-nim.sh
```

Copy the sample WAV from the running NIM container:

```bash
./scripts/copy-nim-sample.sh
```

Or copy it somewhere else:

```bash
./scripts/copy-nim-sample.sh ./samples/en-US_sample.wav
```

You can also use your own mono 16-bit WAV, OPUS, or FLAC file for ASR testing.

#### 5. Run the controller

In one terminal:

```bash
./scripts/run-controller.sh
```

The controller exposes live transcript updates at:

```text
ws://127.0.0.1:3030/ws/live
```

#### 6. Run the worker with Riva/Speech NIM protos

In another terminal, set your Riva proto path in `.env`:

```text
RIVA_PROTO_DIR=/path/to/nvidia-riva/common/riva/proto
RIVA_ASR_ENDPOINT=http://127.0.0.1:50051
```

Then start the worker:

```bash
./scripts/run-worker.sh
```

If your proto files are not in the standard directory layout, use `RIVA_PROTO_FILES` in `.env` instead:

```text
RIVA_PROTO_FILES=/path/to/riva_asr.proto,/path/to/riva_audio.proto,/path/to/riva_common.proto
RIVA_ASR_ENDPOINT=http://127.0.0.1:50051
```

#### 7. Enqueue the bundled sample

In another terminal:

```bash
./scripts/enqueue-sample.sh
```

To enqueue a different file:

```bash
./scripts/enqueue-sample.sh /path/to/audio.wav
```

The worker will read the sample, stream it to ASR, publish transcription segments to Redis, and the controller will emit WebSocket events.

### Manual service startup

If you do not want to use `scripts/start-local.sh`, start the supporting services manually:

```bash
docker run -d --name redis -p 6379:6379 redis:7
docker run -d --name qdrant -p 6333:6333 -p 6334:6334 qdrant/qdrant
```

Then start Speech NIM ASR:

```bash
export CONTAINER_ID=parakeet-1-1b-ctc-en-us
export NIM_TAGS_SELECTOR="name=parakeet-1-1b-ctc-en-us,mode=all"
export NGC_API_KEY=<your-key-value>

docker run -d --name "$CONTAINER_ID" \
  --runtime=nvidia \
  --gpus '"device=0"' \
  --shm-size=8GB \
  --ulimit nofile=2048:2048 \
  -e NGC_API_KEY \
  -e NIM_HTTP_API_PORT=9000 \
  -e NIM_GRPC_API_PORT=50051 \
  -p 9000:9000 \
  -p 50051:50051 \
  -e NIM_TAGS_SELECTOR \
  nvcr.io/nim/nvidia/"$CONTAINER_ID":latest
```

## Helper scripts

The `scripts/` directory contains small local helpers:

| Script | Purpose |
|---|---|
| `scripts/install-arch-deps.sh` | Install Arch base packages and add your user to the Docker group. |
| `scripts/start-local.sh` | Load `.env`, verify Docker GPU access, and start Redis, Qdrant, and Speech NIM. |
| `scripts/check-nim.sh` | Wait for Speech NIM readiness using the `/v1/health/ready` endpoint. |
| `scripts/copy-nim-sample.sh [dest]` | Copy `/opt/riva/wav/en-US_sample.wav` from the NIM container. |
| `scripts/run-controller.sh` | Load `.env` and start `teto-controller`. |
| `scripts/run-worker.sh` | Load `.env`, require `RIVA_PROTO_DIR` or `RIVA_PROTO_FILES`, and start `teto-worker --features riva`. |
| `scripts/enqueue-sample.sh [path]` | Load `.env` and enqueue `./mike_and_nick.wav` or another audio file. |

`.env` is local-only and ignored by Git. Use `.env.example` as the template.

## Speech NIM prerequisites

For x86_64 Linux deployments, TetoScribe should use NVIDIA Speech NIM for the ASR backend rather than the embedded ARM64 Riva SDK Quick Start. Speech NIM runs as a GPU-accelerated Docker container and exposes the same gRPC ASR port used by TetoScribe by default:

```text
gRPC ASR: 50051
HTTP/WebSocket: 9000
```

Verify the following before deploying Speech NIM:

### License and hardware

- NVIDIA AI Enterprise (NVAIE) license for self-hosted Speech NIMs.
- NVIDIA GPU with model-specific memory requirements. Check the Speech NIM support matrix for supported GPU/model combinations.
- Linux x86_64 host.
- Linux distribution supported by the NVIDIA Container Toolkit.
- `glibc >= 2.35` (`ld -v`).
- `curl` for readiness checks.
- CUDA driver installed from a Linux package manager. Do not install the CUDA toolkit just for NIM; the container bundles the required runtime libraries.
- Open GPU kernel modules matching your driver version.

Supported driver major versions include:

| Major version | EOL | Data Center / RTX / Quadro | GeForce |
|---:|---|:---:|:---:|
| > 550 | TBD | Yes | Yes |
| 550 | Feb 2025 | Yes | Yes |
| 545 | Oct 2023 | Yes | Yes |
| 535 | June 2026 | Yes | No |
| 525 | Nov 2023 | Yes | No |
| 470 | Sept 2024 | Yes | No |

### Docker and NVIDIA Container Toolkit

Install Docker Engine and confirm the daemon is running. Your user should be able to run Docker commands without `sudo`; if needed:

```bash
sudo usermod -aG docker $USER
```

Log out and back in for the group change to take effect.

Install and configure the NVIDIA Container Toolkit so Docker can access the host GPU, then restart Docker:

```bash
sudo systemctl restart docker
```

Verify GPU access from containers:

```bash
docker run --rm --runtime=nvidia --gpus all ubuntu nvidia-smi
```

The output should show the driver version, CUDA version, and available GPUs.

### NGC access setup

Configure NGC access so Docker can pull Speech NIM images and download models.

1. Open **Generate Personal Key**.
2. Create a key and ensure at least **NGC Catalog** is included in **Services Included**. Add more services if you use the key for other NGC features.

Personal keys support expiration, revocation, and rotation. For tighter security, store the key in a file and read it when needed, or use a password manager.

Export the API key value as `NGC_API_KEY` in your shell, or put it in `.env`:

```bash
export NGC_API_KEY=<your-key-value>
```

```text
NGC_API_KEY=<your-key-value>
```

To persist across sessions:

```bash
# Bash
echo "export NGC_API_KEY=<your-key-value>" >> ~/.bashrc

# Zsh
echo "export NGC_API_KEY=<your-key-value>" >> ~/.zshrc
```

Authenticate with the NVIDIA Container Registry:

```bash
echo "$NGC_API_KEY" | docker login nvcr.io --username '$oauthtoken' --password-stdin
```

Notes:

- The username is the literal `$oauthtoken`.
- The password is the value of `NGC_API_KEY`.
- After login, `docker pull nvcr.io/nim/nvidia/<image>:<tag>` and NIM container runs that pull from NGC should succeed.

### WSL2 and Python clients

For Windows deployments with WSL2, check the Speech NIM support matrix for WSL2-compatible models. You may need to adjust WSL memory allocation with `.wslconfig` and use Podman instead of Docker.

Some Riva Python client scripts import system audio libraries at module load time. Install these only if you use those scripts:

```bash
sudo apt-get install -y portaudio19-dev
python3 -m pip install pyaudio
```

`sox` is optional and only needed for one TTS HTTP streaming example; the same WAV header step can be done in Python with the standard library `wave` module.

## Environment variables

Set these in your shell or in `.env`. The helper scripts load `.env` automatically.

### Redis

| Variable | Default | Used by |
|---|---:|---|
| `REDIS_URL` | `redis://127.0.0.1/` | all services |
| `TETO_ARCHIVE_JOBS` | `teto_archive_jobs` | worker CLI/worker |
| `TETO_AUDIO_LIVE` | `teto_audio_live` | worker/controller live audio |
| `TETO_TRANSCRIPTION_STREAM` | `teto_transcription_stream` | worker/controller |
| `TETO_TRANSCRIPTION_FIELD` | `segment` | worker/controller |
| `TETO_CONTROLLER_GROUP` | `teto_controller` | controller |
| `TETO_CONTROLLER_CONSUMER` | `teto_controller` | controller |
| `TETO_BRAIN_QUEUE` | `teto_brain_queue` | controller/brain |
| `TETO_BRAIN_RESOLUTIONS_QUEUE` | `brain_resolutions` | brain/controller |
| `TETO_LIVE_CHANNEL` | `teto_live_transcripts` | controller WebSocket broadcasts |

### Controller

| Variable | Default | Notes |
|---|---:|---|
| `TETO_CONTROLLER_ADDR` | `127.0.0.1:3030` | Axum bind address |
| `TETO_STREAM_BLOCK_MS` | `5000` | Redis stream read block |
| `TETO_STREAM_BATCH_SIZE` | `10` | Redis stream batch size |
| `TETO_BRAIN_REQUEST_SEGMENTS` | `5` | Unknown-speaker buffer size before Brain request |

### Qdrant memory

| Variable | Default | Notes |
|---|---:|---|
| `QDRANT_URL` | `http://localhost:6334` | Qdrant endpoint |
| `TETO_VOICE_COLLECTION` | `teto_voices` | 192-dim cosine voice collection |
| `TETO_TRANSCRIPT_COLLECTION` | `teto_transcripts` | text embedding collection |
| `TETO_TRANSCRIPT_DIM` | `1024` | transcript embedding dimension |
| `TETO_VOICE_MATCH_THRESHOLD` | `0.85` | cosine score threshold |

The controller times out Qdrant bootstrap attempts and keeps running in degraded mode if Qdrant is unavailable.

### Worker and Riva

| Variable | Default | Notes |
|---|---:|---|
| `RIVA_ASR_ENDPOINT` | `http://127.0.0.1:50051` | Riva ASR gRPC endpoint |
| `RIVA_LANGUAGE_CODE` | `en-US` | ASR language |
| `RIVA_SAMPLE_RATE_HZ` | `16000` | PCM sample rate |
| `RIVA_CHUNK_DURATION_MS` | `100` | audio chunk duration |
| `RIVA_MAX_SPEAKER_COUNT` | `2` | diarization max speakers |
| `RIVA_ASR_MODEL` | empty | optional Riva model selection |
| `RIVA_CUSTOM_CONFIGURATION` | empty | comma-separated `key=value` config |
| `RIVA_INTERIM_RESULTS` | `true` | interim results are requested but filtered before publishing |
| `TETO_FILE_CHUNK_BYTES` | `32768` | archive file chunk size |
| `TETO_RECONNECT_DELAY_SECS` | `2` | Redis reconnect delay |

`cargo run -p teto-worker --features riva` requires either:

```bash
RIVA_PROTO_DIR=/path/to/nvidia-riva/common/riva/proto
```

or:

```bash
RIVA_PROTO_FILES=/path/to/riva_asr.proto,/path/to/riva_audio.proto,/path/to/riva_common.proto
```

The worker accepts raw PCM and simple PCM WAV files for archive jobs. Live Pub/Sub audio is expected to be raw PCM matching the Riva config.

### Brain identity resolver

| Variable | Default | Notes |
|---|---:|---|
| `TETO_BRAIN_BACKEND` | `regex` | `regex` or `command` |
| `TETO_KNOWN_NAMES_JSON` | empty | JSON object mapping canonical names to aliases |
| `TETO_BRAIN_COMMAND` | required for command backend | local executable invoked with Brain request JSON on stdin |
| `TETO_BRAIN_RECONNECT_DELAY_SECS` | `2` | Redis reconnect delay |

Example known names:

```json
{
  "Алиса": ["alice", "алиса"],
  "Борис": ["bob", "борис"]
}
```

The regex backend is a deterministic fallback/demo. The `command` backend lets you wire a local LLM wrapper without adding cloud services.

## Identity loop

1. Worker publishes diarized `TranscriptionSegment` values to Redis.
2. Controller tries Qdrant voice matching.
3. Unknown speakers are buffered.
4. Controller asks Brain for identity resolution.
5. Brain returns a speaker-tag-to-name map.
6. Controller upserts voice identity to Qdrant and replays resolved transcript segments.
7. WebSocket clients receive transcript segments and `IdentityResolved` events.

Voice fingerprints are 192-dimensional vectors. The worker currently derives local acoustic fingerprints from decoded PCM segments and falls back to deterministic speaker-tag fingerprints when audio slices are unavailable.

## Current limits

- No production local LLM model is bundled.
- No trained speaker-recognition embedding model is bundled.
- No FFmpeg/audio codec layer beyond PCM WAV/raw PCM handling.
- Riva protobufs are external and must be provided at build time.
- Docker Compose/service deployment files are not included yet.
