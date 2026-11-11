#!/usr/bin/env bash
# Run the collector and both example services locally without Docker (SQLite storage).
#   scripts/run_local.sh          start everything, Ctrl-C stops it
set -euo pipefail
cd "$(dirname "$0")/.."

mkdir -p .run
export PULSETRACE_ENDPOINT="http://127.0.0.1:9000/v1/events"
export PULSETRACE_ENVIRONMENT="local"

pids=()
cleanup() { kill "${pids[@]}" 2>/dev/null || true; wait 2>/dev/null || true; }
trap cleanup EXIT INT TERM

DATABASE_URL="sqlite+aiosqlite:///.run/collector.db" \
  uv run uvicorn collector.main:create_app --factory --port 9000 --log-level warning &
pids+=($!)
uv run uvicorn examples.fraud_service.main:app --port 8001 --log-level warning &
pids+=($!)
FRAUD_SERVICE_URL="http://127.0.0.1:8001" PAYMENTS_DATABASE_URL="sqlite+aiosqlite:///.run/payments.db" \
  uv run uvicorn examples.payment_service.main:app --port 8000 --log-level warning &
pids+=($!)

echo "payment-service http://127.0.0.1:8000   fraud-service http://127.0.0.1:8001"
echo "collector       http://127.0.0.1:9000   viewer http://127.0.0.1:9000/ui/issues"
wait
