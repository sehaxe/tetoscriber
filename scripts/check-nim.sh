#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

source "$ROOT_DIR/scripts/lib/load-env.sh"
load_dotenv "$ROOT_DIR/.env"

export NIM_HTTP_API_PORT="${NIM_HTTP_API_PORT:-9000}"
export NIM_READY_INTERVAL_SECS="${NIM_READY_INTERVAL_SECS:-10}"
export NIM_READY_RETRIES="${NIM_READY_RETRIES:-180}"

health_url="http://127.0.0.1:${NIM_HTTP_API_PORT}/v1/health/ready"

for attempt in $(seq 1 "$NIM_READY_RETRIES"); do
  response="$(curl -fsS "$health_url" 2>/dev/null || true)"
  if [[ "$response" == '{"status":"ready"}' ]]; then
    echo "Speech NIM is ready: $health_url"
    exit 0
  fi

  sleep "$NIM_READY_INTERVAL_SECS"
done

echo "Speech NIM did not become ready after $((NIM_READY_INTERVAL_SECS * NIM_READY_RETRIES)) seconds."
echo "Check container logs with: docker logs -f ${CONTAINER_ID:-parakeet-1-1b-ctc-en-us}"
exit 1
