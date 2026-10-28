#!/usr/bin/env bash
# Codespaces start hook: API + two workers on PostgreSQL, then one run of every demo workflow.
set -euo pipefail
cd "$(dirname "$0")/.."

docker compose up -d --build

echo "waiting for the API on :8000 ..."
for _ in $(seq 1 60); do
  if curl -fsS http://127.0.0.1:8000/health >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

start() {
  curl -fsS -X POST "http://127.0.0.1:8000/v1/workflows/$1/runs" \
    -H 'content-type: application/json' -d "$2" >/dev/null
}
# run_id makes these idempotent: restarting the Codespace does not start them twice.
start money_transfer '{"run_id":"demo-transfer","input":{"from":"ACC-1001","to":"ACC-2002","amount":2500,"debit_flaky_attempts":1}}'
start money_transfer '{"run_id":"demo-refund","input":{"from":"ACC-1001","to":"ACC-3003","amount":900,"fail_credit":true}}'
start document_approval '{"run_id":"demo-approval","input":{"document":"Q3 budget","reviewer":"lead@example.com","remind_after_seconds":60}}'
start user_onboarding '{"run_id":"demo-onboarding","input":{"user":"nadia@example.com","delay_seconds":30}}'
start regional_report '{"run_id":"demo-report","input":{"flaky_attempts":1}}'

cat <<'MSG'

Durable Workflow Engine is up: API + 2 worker containers on PostgreSQL.
  Dashboard:  forwarded port 8000, path /        (API docs at /docs)
  Approve:    curl -X POST localhost:8000/v1/runs/demo-approval/signals/decision \
                -H 'content-type: application/json' -d '{"payload":{"approved":true}}'
  Chaos:      docker compose kill worker   then   docker compose up -d worker
MSG
