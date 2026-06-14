#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

source "$ROOT_DIR/scripts/lib/load-env.sh"
load_dotenv "$ROOT_DIR/.env"

if [[ -z "${RIVA_PROTO_DIR:-}" && -z "${RIVA_PROTO_FILES:-}" ]]; then
  echo "Set RIVA_PROTO_DIR or RIVA_PROTO_FILES in .env before running the worker."
  exit 1
fi

export RIVA_ASR_ENDPOINT="${RIVA_ASR_ENDPOINT:-http://127.0.0.1:${NIM_GRPC_API_PORT:-50051}}"

if [[ -n "${RIVA_PROTO_DIR:-}" ]]; then
  exec env RIVA_PROTO_DIR="$RIVA_PROTO_DIR" RIVA_ASR_ENDPOINT="$RIVA_ASR_ENDPOINT" cargo run -p teto-worker --features riva
fi

exec env RIVA_PROTO_FILES="$RIVA_PROTO_FILES" RIVA_ASR_ENDPOINT="$RIVA_ASR_ENDPOINT" cargo run -p teto-worker --features riva
