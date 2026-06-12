# PROJECT KNOWLEDGE BASE

**Generated:** 2026-06-11
**Commit:** 009358a
**Branch:** main

## OVERVIEW
TetoScribe is a seed repository for a sovereign local speech pipeline: Rust/Axum/Tonic controller, NVIDIA Riva ASR/diarization, TensorRT-LLM/llama-cpp reasoning, Redis tasking, and Qdrant voice/transcript memory. Current implementation is a Rust workspace scaffold with `teto-brain`, `teto-controller`, `teto-protocol`, and `teto-worker`; the controller now includes a Redis queue layer and Axum WebSocket live endpoint, `teto-worker` includes a feature-gated Riva ASR streaming client and dispatcher, and `teto-brain` includes a Redis-backed mock/regex identity loop ready for heavier local LLM inference later.

## STRUCTURE
```text
.
├── README.md      # placeholder project name only
├── LICENSE        # license terms
├── AGENTS.md      # generated project knowledge base
├── Cargo.toml     # Rust workspace manifest
└── crates/
    ├── teto-brain/       # Redis-backed identity reasoning binary
    ├── teto-controller/  # pure Rust API/Redis/Qdrant coordinator
    ├── teto-protocol/    # shared serde schemas for Redis/Qdrant/Brain messages
    └── teto-worker/      # Rust worker scaffold with optional Riva gRPC generation
```

## WHERE TO LOOK
| Task | Location | Notes |
|------|----------|-------|
| Project intent and target stack | This file | User brief defines Riva + Rust + TensorRT-LLM + Qdrant |
| Workspace dependencies | `Cargo.toml` | Centralized Rust workspace dependencies and crate members |
| Teto-Brain / Identity Loop | `crates/teto-brain/src/main.rs` | Redis brain queue consumer, mock/regex speaker-name inference, and Redis resolution response producer |
| Shared protocol schemas | `crates/teto-protocol/` | Serde JSON types for transcription segments, Redis tasks, Qdrant memory upserts, and Brain identity responses |
| Controller scaffold | `crates/teto-controller/` | Pure Rust binary; Redis tasking, Axum WebSocket live endpoint, Qdrant memory here |
| Worker scaffold | `crates/teto-worker/` | Tonic worker; optional Riva client generated from `RIVA_PROTO_DIR` |
| Riva worker client | `crates/teto-worker/src/riva_client.rs` | Feature-gated Riva ASR streaming client, diarization request config, and protocol-to-segment mapping |
| Worker dispatcher | `crates/teto-worker/src/job_processor.rs` | Redis archive job consumer, live Pub/Sub audio consumer, file streaming through `tokio-util::io::ReaderStream`, and Redis Stream publishing |
| Riva protobuf build hook | `crates/teto-worker/build.rs` | Compiles Riva ASR/audio/common protos only when `RIVA_PROTO_DIR` or `RIVA_PROTO_FILES` is set |
| Redis queue and HTTP/WebSocket API | `crates/teto-controller/src/queue.rs` and `main.rs` | Stream listener, brain request dispatch, live Pub/Sub/WebSocket broadcast |
| Memory/indexing | `crates/teto-controller/src/storage.rs` | Qdrant client and voice/transcript vectors |
| Future LLM reasoning | Future crate/module | Local TensorRT-LLM/llama.cpp integration boundary |

## CODE MAP
| Symbol | Type | Location | Role |
|--------|------|----------|------|
| `main` | Rust entry point | `crates/teto-brain/src/main.rs` | Initializes tracing, loads Redis brain queue config, consumes identity requests, and produces identity responses |
| `BrainConfig` | Rust struct | `crates/teto-brain/src/main.rs` | Environment-driven Redis brain queue, resolution queue, and reconnect delay settings |
| `TetoBrain` | Rust struct | `crates/teto-brain/src/main.rs` | Redis brain listener and response producer |
| `RegexIdentityEngine` | Rust struct | `crates/teto-brain/src/main.rs` | Configurable regex identity engine for bootstrapping the Redis message loop before heavier local LLM inference |
| `KnownNames` | Rust struct | `crates/teto-brain/src/main.rs` | Environment-loaded JSON map of canonical names to aliases; no speaker names are hardcoded |
| `main` | Rust entry point | `crates/teto-controller/src/main.rs` | Initializes tracing, Redis/Qdrant config, stream and brain listeners, and `/ws/live` Axum endpoint |
| `StorageConfig` | Rust struct | `crates/teto-controller/src/storage.rs` | Environment-driven Qdrant memory configuration with safe defaults |
| `SovereignMemory` | Rust struct | `crates/teto-controller/src/storage.rs` | Qdrant-backed memory client for collection bootstrapping, voice matching, voice upsert, and transcript indexing |
| `IdentityStorage` | Rust trait | `crates/teto-controller/src/storage.rs` | Testable storage seam for voice matching, voice upserts, and transcript indexing |
| `MatchedVoice` | Rust struct | `crates/teto-controller/src/storage.rs` | Voice identity match result returned by Qdrant query API |
| `QueueConfig` | Rust struct | `crates/teto-controller/src/queue.rs` | Environment-driven Redis stream, group, brain queue, live channel, and controller bind settings |
| `PendingSession` | Rust struct | `crates/teto-controller/src/queue.rs` | In-memory buffer for unknown-speaker segments awaiting brain identity resolution |
| `StreamListener` | Rust struct | `crates/teto-controller/src/queue.rs` | Redis Streams consumer that decodes segments, emits live transcripts, matches voices, and dispatches brain requests |
| `BrainResolutionListener` | Rust struct | `crates/teto-controller/src/queue.rs` | Redis queue consumer for brain identity responses, voice identity upserts, IdentityResolved events, and buffered segment replay |
| `LiveState` | Rust struct | `crates/teto-controller/src/queue.rs` | Axum WebSocket state wrapping transcript and identity broadcast senders |
| `live_ws_handler` | Rust function | `crates/teto-controller/src/queue.rs` | Axum WebSocket upgrade handler for `/ws/live` |
| `VoiceFingerprint` | Rust newtype | `crates/teto-protocol/src/lib.rs` | 192-dim voice vector wrapper with dimension validation |
| `BrainIdentityRequest` | Rust struct | `crates/teto-protocol/src/lib.rs` | Request sent to local LLM/Brain for speaker-name resolution |
| `BrainIdentityResponse` | Rust struct | `crates/teto-protocol/src/lib.rs` | Brain response mapping speaker tags to resolved display names |
| `IdentityResolution` | Rust struct | `crates/teto-protocol/src/lib.rs` | Controller-side identity decision with source and confidence |
| `IdentityResolved` | Rust struct | `crates/teto-protocol/src/lib.rs` | WebSocket event emitted when Brain resolves a speaker tag to a stable name |
| `main` | Rust entry point | `crates/teto-worker/src/main.rs` | Initializes tracing and prints worker scaffold status |
| `worker_ready` | Rust function | `crates/teto-worker/src/lib.rs` | Library status helper used by tests and binary |
| `RivaClientConfig` | Rust struct | `crates/teto-worker/src/riva_client.rs` | Environment-driven Riva ASR endpoint, audio, chunking, diarization, and model settings |
| `RivaClient` | Rust struct | `crates/teto-worker/src/riva_client.rs` | Feature-gated Riva ASR streaming client that maps Riva results to `teto_protocol::TranscriptionSegment` |
| `WorkerConfig` | Rust struct | `crates/teto-worker/src/job_processor.rs` | Environment-driven Redis queues, live channel, transcription stream, file chunk size, and reconnect delay |
| `JobProcessor` | Rust struct | `crates/teto-worker/src/job_processor.rs` | Redis archive/live dispatcher that reads audio, streams it through Riva, bridges placeholder fingerprints, and publishes segments |

## CONVENTIONS
- Rust-first controller: async `tokio`, `tonic` gRPC, `axum` HTTP/WebSocket.
- No Python runtime workers for the production ASR pipeline.
- Treat Riva as the industrial ASR/diarization server; Rust owns orchestration only.
- Keep generated protobuf code generated, not hand-edited.
- Prefer feature flags for optional integrations: `riva`, `qdrant`, `llm`, `tensorrt`, `web`.
- Use environment variables or local config files for endpoints, credentials, model paths, and GPU selection.
- Keep shared Redis/Qdrant/Brain payloads in `teto-protocol`; do not duplicate wire schemas in controller/worker crates.
- Keep voice identity vectors at 192 dimensions unless the model boundary is explicitly changed.
- Keep CLI workflow aligned with `tetoscribe process ./mike_and_nick.wav`.

## ANTI-PATTERNS (THIS PROJECT)
- Do not add cloud ASR, cloud LLM, or cloud vector APIs as default behavior.
- Do not add Python runtime workers for the production ASR pipeline.
- Do not block async Rust code on synchronous CUDA/gRPC calls.
- Do not hard-code VRAM/GPU assumptions; detect or configure hardware limits.
- Do not store secrets, model credentials, or generated Riva artifacts in the repo.
- Do not create child `AGENTS.md` files until real subdirectories/modules exist.
- Do not invent code before `Cargo.toml`, `src/`, and `.proto`/Riva client boundaries are established.

## UNIQUE STYLES
- Component names from the project brief: `Teto-Ядро`, `Teto-Контроллер`, `Teto-Мозг`, `Teto-Память`.
- Output target: Markdown transcript with named speaker turns and identity confidence notes.
- Naming loop target: Riva speaker tags become stable local identities via Qdrant voice vectors.
- Operational target: local, sovereign, 24/7 service with minimal runtime dependencies.

## COMMANDS
```bash
cargo check
cargo test
cargo run -p teto-controller
cargo run -p teto-worker
cargo run -p teto-worker --features riva
```

## NOTES
- Current repo is a seed, but `teto-controller` now contains the Redis queue layer and WebSocket live endpoint.
- Identity Loop implementation starts in `teto-protocol`, then uses controller Redis/Qdrant state handling.
- Qdrant collections for Identity Loop are `teto_voices` (192-dim cosine) and `teto_transcripts` / `teto_history` (text embeddings).
- `crates/teto-controller/src/storage.rs` contains `StorageConfig`, `SovereignMemory`, and `MatchedVoice`; it bootstraps Qdrant collections and uses the modern `query` API for voice matching, not legacy `search_points`.
- `crates/teto-brain/src/main.rs` contains the Redis brain queue consumer, response producer, and configurable regex identity engine. Known names are loaded from `TETO_KNOWN_NAMES_JSON`, a JSON object mapping canonical display names to aliases; no speaker names are hardcoded.
- `crates/teto-controller/src/queue.rs` owns `QueueConfig`, `StreamListener`, `BrainResolutionListener`, `LiveState`, and `/ws/live`. It uses Redis Streams with a consumer group, Redis list dispatch for brain requests, Redis Pub/Sub for live transcripts, Axum WebSocket broadcast for clients, replay of buffered segments after identity resolution, and `IdentityResolved` WebSocket events.
- `crates/teto-controller/tests/identity_loop.rs` simulates the Identity Loop without Riva using a Redis testcontainer, a mock `IdentityStorage`, synthetic voice fingerprints, a Brain response, replayed transcript segments, and fast-path voice matching.
- Controller startup attempts Qdrant bootstrap inside timeouts and logs failures instead of crashing, matching the "controller never falls" requirement. Redis listener startup reconnects instead of crashing when Redis is unavailable.
- `teto-worker --features riva` requires `RIVA_PROTO_DIR` or `RIVA_PROTO_FILES` pointing at NVIDIA Riva proto files, e.g. `nvidia-riva/common/riva/proto`.
- `teto-worker/src/riva_client.rs` implements streaming ASR with diarization enabled and maps final/interim Riva results into `TranscriptionSegment`; Riva Speaker Recognition voice embeddings are not wired yet because the public `nvidia-riva/common` proto set does not include a speaker-recognition service.
- `teto-worker/src/job_processor.rs` owns `WorkerConfig` and `JobProcessor`. It consumes archive paths from `teto_archive_jobs` via `BRPOP`, streams files with `ReaderStream`, consumes live audio from `teto_audio_live` Pub/Sub, publishes `TranscriptionSegment` JSON into `teto_transcription_stream`, and uses deterministic 192-dim placeholder fingerprints per session/speaker until real biometrics are available.
- Riva model precision/quantization is configured server-side by the deployed Riva pipeline/model; the worker client can select `RIVA_ASR_MODEL` and `RIVA_CUSTOM_CONFIGURATION` but should not hard-code VRAM assumptions.
- Child knowledge bases should be added only after meaningful directories such as `src/`, `proto/`, `crates/`, or `examples/` exist.
