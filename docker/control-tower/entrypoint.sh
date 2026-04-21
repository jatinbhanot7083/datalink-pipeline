#!/bin/bash
# NOTE: deliberately not `set -e` so a single failing step doesn't
# abort the whole entrypoint. Each step handles its own failure.

# Install datalink editable from the bind-mount so code edits are live.
echo "[entrypoint] installing datalink (editable)..."
if [ -f /opt/datalink/pyproject.toml ]; then
    pip install --no-deps -e /opt/datalink || echo "[entrypoint] WARN: pip install failed, datalink imports may fail"
else
    echo "[entrypoint] ERROR: /opt/datalink/pyproject.toml not found — bind mount broken?"
fi

# Phase 6 fix: pre-create warehouse.duckdb + CONTROL schema + seed 7
# clients' baselines (84 suites) BEFORE streamlit starts, so every page
# can open the file read-only without erroring. Verbose=True emits per
# -step diagnostics to stderr (visible in docker logs).
echo "[entrypoint] running warehouse bootstrap..."
python -c "
from datalink.ui._bootstrap import ensure_warehouse_exists, default_warehouse_path
ensure_warehouse_exists(default_warehouse_path(), verbose=True)
" || echo "[entrypoint] WARN: bootstrap script exited non-zero (pages will still render with empty state)"

# Confirm the file is actually there before starting streamlit.
if [ -f /opt/datalink/warehouse.duckdb ]; then
    echo "[entrypoint] OK warehouse.duckdb exists — $(stat -c '%s bytes, mode %a' /opt/datalink/warehouse.duckdb)"
else
    echo "[entrypoint] ERROR: warehouse.duckdb MISSING after bootstrap — every page will stack-trace"
fi

echo "[entrypoint] starting streamlit..."
exec streamlit run /opt/datalink/datalink/ui/control_tower.py \
    --server.address 0.0.0.0 \
    --server.port 8000 \
    --server.headless true \
    --browser.gatherUsageStats false \
    --server.fileWatcherType poll \
    --theme.primaryColor "#d4af37" \
    --theme.backgroundColor "#ffffff" \
    --theme.secondaryBackgroundColor "#f3f4f8" \
    --theme.textColor "#0a1a3e"
