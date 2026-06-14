#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

source "$ROOT_DIR/scripts/lib/load-env.sh"
load_dotenv "$ROOT_DIR/.env"

export CONTAINER_ID="${CONTAINER_ID:-parakeet-1-1b-ctc-en-us}"

dest="${1:-./en-US_sample.wav}"

docker cp "$CONTAINER_ID:/opt/riva/wav/en-US_sample.wav" "$dest"
echo "Copied sample audio to $dest"
