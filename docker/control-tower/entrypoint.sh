#!/bin/bash
# NOTE: deliberately not `set -e` so a single failing step doesn't
# abort the whole entrypoint. Each step handles its own failure.

# PYTHONPATH=/opt/datalink is set in the Dockerfile ENV, so `import
# datalink.*` works from the bind-mount alone. The editable install
# below is a NICE-TO-HAVE (updates pip metadata + console-scripts) but
# not required for the UI to function.
echo "[entrypoint] PYTHONPATH=$PYTHONPATH"
echo "[entrypoint] verifying datalink is importable..."
python -c "import datalink, sys; print('[entrypoint] datalink OK, from', datalink.__file__)" \
    || echo "[entrypoint] ERROR: datalink NOT importable — bind mount broken?"

# Phase 6 fix: pre-create data/generated/ with 0o777 so the Airflow
# worker (uid 50000, different from control-tower's root uid 0) can
# create per-client subdirs when task_bronze_ingest generates the
# synthetic CSVs. Without this, airflow hits:
#     PermissionError: [Errno 13] Permission denied: '/opt/datalink/data/generated'
echo "[entrypoint] ensuring /opt/datalink/data/generated is world-writable..."
mkdir -p /opt/datalink/data/generated && chmod 0777 /opt/datalink/data/generated \
    || echo "[entrypoint] WARN: could not chmod data/generated — airflow writes may fail"

# Optional editable install, skipped if it hangs or errors. Pages work
# without it thanks to PYTHONPATH.
echo "[entrypoint] attempting editable install (optional, max 30s)..."
if [ -f /opt/datalink/pyproject.toml ]; then
    timeout 30 pip install --no-deps --no-build-isolation -e /opt/datalink 2>&1 \
        | sed 's|^|[entrypoint pip] |'
    rc=${PIPESTATUS[0]}
    if [ "$rc" -eq 0 ]; then
        echo "[entrypoint] editable install OK"
    elif [ "$rc" -eq 124 ]; then
        echo "[entrypoint] WARN: pip install timed out — PYTHONPATH fallback still works"
    else
        echo "[entrypoint] WARN: pip install exit $rc — PYTHONPATH fallback still works"
    fi
else
    echo "[entrypoint] WARN: /opt/datalink/pyproject.toml not found — bind-mount may be broken"
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
