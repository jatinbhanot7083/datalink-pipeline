#!/usr/bin/env bash
# Stop the background Azurite process started by azurite-up.sh.
# Idempotent: if not running, silent no-op.

set -euo pipefail

PID_FILE="${HOME}/.azurite.pid"

if [ ! -f "$PID_FILE" ]; then
  echo "Azurite not running (no pid file)"
  exit 0
fi

PID="$(cat "$PID_FILE")"

if kill -0 "$PID" 2>/dev/null; then
  kill "$PID" 2>/dev/null || true
  # Wait up to 5s for graceful shutdown, then SIGKILL.
  for _ in $(seq 1 10); do
    if ! kill -0 "$PID" 2>/dev/null; then
      break
    fi
    sleep 0.5
  done
  kill -9 "$PID" 2>/dev/null || true
  echo "Azurite stopped (PID $PID)"
else
  echo "Azurite pid file stale (PID $PID not alive)"
fi

rm -f "$PID_FILE"
