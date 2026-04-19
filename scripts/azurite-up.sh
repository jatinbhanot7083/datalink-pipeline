#!/usr/bin/env bash
# Start Azurite (local ADLS Gen2 emulator) as a background process.
#
# Why not a docker-compose service? Docker Desktop 4.51.0 blocks pulls from
# mcr.microsoft.com (where the azurite image lives). Azurite is pure Node.js
# and runs identically outside Docker — prod still uses real ADLS Gen2, the
# adapter code is unchanged.
#
# Idempotent: if Azurite is already running, this is a no-op.

set -euo pipefail

PID_FILE="${HOME}/.azurite.pid"
LOG_FILE="${HOME}/.azurite.log"
DATA_DIR="${HOME}/.azurite-data"

NODE_BIN="${HOME}/.local/bin/node"
AZURITE_JS="${HOME}/.local/bin/azurite"

if [ ! -x "$NODE_BIN" ] || [ ! -f "$AZURITE_JS" ]; then
  echo "FAIL: node or azurite not installed at ${HOME}/.local/bin/ — run:"
  echo "  curl -fsSL https://nodejs.org/dist/v20.18.0/node-v20.18.0-linux-x64.tar.xz | tar -xJ -C ~/.local --strip-components=1"
  echo "  node ~/.local/lib/node_modules/npm/bin/npm-cli.js install -g --prefix ~/.local azurite"
  exit 1
fi

# Already running?
if [ -f "$PID_FILE" ] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
  echo "Azurite already running (PID $(cat "$PID_FILE"))"
  exit 0
fi

mkdir -p "$DATA_DIR"

# Fully daemonize — setsid creates a new session, nohup ignores SIGHUP,
# < /dev/null detaches stdin. Survives even when the invoking WSL shell
# exits (which WSL2 would otherwise reap).
setsid nohup "$NODE_BIN" "$AZURITE_JS" \
  --location "$DATA_DIR" \
  --blobHost 127.0.0.1 --blobPort 10000 \
  --queueHost 127.0.0.1 --queuePort 10001 \
  --tableHost 127.0.0.1 --tablePort 10002 \
  --skipApiVersionCheck \
  --silent \
  < /dev/null > "$LOG_FILE" 2>&1 &

AZURITE_PID=$!
echo "$AZURITE_PID" > "$PID_FILE"
disown 2>/dev/null || true

# Health-wait: up to 15s for the Blob port to accept connections.
for _ in $(seq 1 30); do
  if exec 3<>/dev/tcp/127.0.0.1/10000 2>/dev/null; then
    exec 3<&-
    echo "Azurite up on 127.0.0.1:10000 (PID $AZURITE_PID, data=$DATA_DIR, log=$LOG_FILE)"
    exit 0
  fi
  sleep 0.5
done

echo "FAIL: Azurite did not accept connections on 127.0.0.1:10000 within 15s"
echo "  log: $LOG_FILE"
tail -20 "$LOG_FILE" 2>/dev/null || true
rm -f "$PID_FILE"
exit 1
