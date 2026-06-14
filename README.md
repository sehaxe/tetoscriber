# TetoScribe

TetoScribe is a local-first speech pipeline for turning audio into named-speaker Markdown transcripts. The current repository is a Rust workspace with:

- `teto-worker`: Redis-backed archive/live dispatcher and optional NVIDIA Riva ASR client
- `teto-controller`: Redis stream processor, Qdrant memory, and `/ws/live` WebSocket endpoint
- `teto-brain`: Redis identity resolver with a regex fallback and a local command backend boundary
- `teto-protocol`: shared serde schemas for Redis, Qdrant, Brain, and WebSocket messages

The stack is designed for sovereign/local operation: Redis for tasking, Qdrant for voice/transcript memory, Riva for ASR/diarization, and no cloud ASR/LLM/vector defaults.

## Quick start

```bash
cargo check
cargo test --workspace
```

Run the controller:

```bash
cargo run -p teto-controller
```

Run the worker with Riva protobufs available:

```bash
RIVA_PROTO_DIR=/path/to/nvidia-riva/common/riva/proto \
RIVA_ASR_ENDPOINT=http://127.0.0.1:50051 \
cargo run -p teto-worker --features riva
```

Enqueue an archive file:

```bash
REDIS_URL=redis://127.0.0.1/ \
cargo run -p teto-cli -- process ./mike_and_nick.wav
```

Connect to live transcript updates:

```text
ws://127.0.0.1:3030/ws/live
```

## Environment variables

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
