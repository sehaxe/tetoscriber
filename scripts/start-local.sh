#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

source "$ROOT_DIR/scripts/lib/load-env.sh"
load_dotenv "$ROOT_DIR/.env"

if [[ -z "${NGC_API_KEY:-}" ]]; then
  echo "NGC_API_KEY is not set. Put it in .env or export it before starting Speech NIM."
  exit 1
fi

if ! docker info >/dev/null 2>&1; then
  echo "Docker daemon is not reachable. Start Docker or add your user to the docker group."
  exit 1
fi

if ! docker run --rm --runtime=nvidia --gpus all ubuntu nvidia-smi >/dev/null 2>&1; then
  echo "GPU access from Docker failed. Install/configure NVIDIA Container Toolkit first."
  exit 1
fi

if ! docker ps -a --format '{{.Names}}' | grep -qx redis; then
  docker rm -f redis >/dev/null 2>&1 || true
  docker run -d --name redis -p 6379:6379 redis:7 >/dev/null
fi

if ! docker ps -a --format '{{.Names}}' | grep -qx qdrant; then
  docker rm -f qdrant >/dev/null 2>&1 || true
  docker run -d --name qdrant -p 6333:6333 -p 6334:6334 qdrant/qdrant >/dev/null
fi

export CONTAINER_ID="${CONTAINER_ID:-parakeet-1-1b-ctc-en-us}"
export NIM_TAGS_SELECTOR="${NIM_TAGS_SELECTOR:-name=parakeet-1-1b-ctc-en-us,mode=all}"
export NIM_HTTP_API_PORT="${NIM_HTTP_API_PORT:-9000}"
export NIM_GRPC_API_PORT="${NIM_GRPC_API_PORT:-50051}"
export RIVA_ASR_ENDPOINT="${RIVA_ASR_ENDPOINT:-http://127.0.0.1:${NIM_GRPC_API_PORT}}"

ulimit_args=()
if [[ -n "${NIM_ULIMIT_NOFILE:-}" ]]; then
  ulimit_args=(--ulimit "nofile=${NIM_ULIMIT_NOFILE}")
fi

if ! docker ps -a --format '{{.Names}}' | grep -qx "$CONTAINER_ID"; then
  docker rm -f "$CONTAINER_ID" >/dev/null 2>&1 || true
  docker run -d --name "$CONTAINER_ID" \
    --runtime=nvidia \
    --gpus '"device=0"' \
    --shm-size=8GB \
    "${ulimit_args[@]}" \
    -e NGC_API_KEY \
    -e NIM_HTTP_API_PORT \
    -e NIM_GRPC_API_PORT \
    -p "${NIM_HTTP_API_PORT}:9000" \
    -p "${NIM_GRPC_API_PORT}:50051" \
    -e NIM_TAGS_SELECTOR \
    nvcr.io/nim/nvidia/"$CONTAINER_ID":latest
fi

./scripts/check-nim.sh

echo "Redis: ${REDIS_URL:-redis://127.0.0.1/}"
echo "Qdrant: ${QDRANT_URL:-http://localhost:6334}"
echo "Speech NIM gRPC: $RIVA_ASR_ENDPOINT"
echo "Speech NIM HTTP: http://127.0.0.1:${NIM_HTTP_API_PORT}"
echo ""
echo "Run the controller in another terminal:"
echo "  ./scripts/run-controller.sh"
echo ""
echo "Run the worker in another terminal:"
echo "  ./scripts/run-worker.sh"
echo ""
echo "Enqueue the bundled sample:"
echo "  ./scripts/enqueue-sample.sh"
