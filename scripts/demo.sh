#!/usr/bin/env bash
# The three-minute demo, scripted (requires Docker Compose):
#   1. start the stack   2. send traffic   3. stop the collector, keep serving
#   4. restore it and watch the queued events arrive   5. open the viewer
set -euo pipefail
cd "$(dirname "$0")/.."

step() { printf '\n\033[1;35m== %s\033[0m\n' "$*"; }

step "Starting payment-service, fraud-service, collector and PostgreSQL"
docker compose up --build -d
until curl -fsS http://localhost:8000/health >/dev/null && curl -fsS http://localhost:9000/health >/dev/null; do sleep 1; done

step "Sending a mix of successful, slow and failing requests"
uv run python scripts/traffic.py --requests 40 --base-url http://localhost:8000

step "Breaking the collector: the application must keep working"
docker compose stop collector
for i in 1 2 3 4 5; do
  curl -s -o /dev/null -w "refund request $i -> HTTP %{http_code} in %{time_total}s\n" -X POST "http://localhost:8000/refunds/pay-$i"
done
curl -s http://localhost:8000/debug/telemetry | python3 -m json.tool | grep -E '"(queue_depth|retries|last_error)"'

step "Restoring the collector: retries deliver what was queued, deduplication keeps counts exact"
docker compose start collector
sleep 8
curl -s http://localhost:8000/debug/telemetry | python3 -m json.tool | grep -E '"(queue_depth|delivered|retries|dropped_retries_exhausted)"'
curl -s http://localhost:9000/stats | python3 -m json.tool

step "Open the viewer"
echo "Issues: http://localhost:9000/ui/issues   Events: http://localhost:9000/ui/events"
