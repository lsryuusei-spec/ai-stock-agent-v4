#!/usr/bin/env bash
set -euo pipefail

HOST="${AI_STOCK_AGENT_HOST:-0.0.0.0}"
PORT="${AI_STOCK_AGENT_PORT:-8765}"
LOG_PATH="/tmp/ai-stock-agent-dashboard.log"

if command -v lsof >/dev/null 2>&1 && lsof -iTCP:"${PORT}" -sTCP:LISTEN >/dev/null 2>&1; then
  echo "AI Stock Agent dashboard is already listening on port ${PORT}."
  exit 0
fi

mkdir -p data
nohup python -m ai_stock_agent.cli --db data/agent.db serve-gui --host "${HOST}" --port "${PORT}" > "${LOG_PATH}" 2>&1 &
SERVER_PID=$!
sleep 1

if ! kill -0 "${SERVER_PID}" >/dev/null 2>&1; then
  echo "AI Stock Agent dashboard failed to start. Last log lines:"
  tail -n 50 "${LOG_PATH}" || true
  exit 1
fi

echo "AI Stock Agent dashboard started on ${HOST}:${PORT}. Logs: ${LOG_PATH}"
