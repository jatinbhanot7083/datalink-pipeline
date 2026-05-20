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

# Phase 6 fix: pre-create every dir Airflow (uid 50000) needs to write
# into BUT which lives on the host bind-mount (owned by the dev user, uid
# 1000 in dev, whatever in prod). Without chmod 0o777 here Airflow hits:
#     PermissionError: [Errno 13] Permission denied: '/opt/datalink/<dir>'
# for each write. control_tower runs as root so it can chmod the bind-mount
# host-side, which persists across container restarts (but NOT across
# `docker compose down -v` + host rm — the host rm wipes the perms).
echo "[entrypoint] ensuring host bind-mount dirs are writable by airflow..."
for dir in \
    /opt/datalink/data/generated \
    /opt/datalink/dbt \
    /opt/datalink/dbt/logs \
    /opt/datalink/dbt/target \
    /opt/datalink/gx/uncommitted; do
    mkdir -p "$dir" && chmod 0777 "$dir" \
        && echo "[entrypoint]   OK $dir (0o777)" \
        || echo "[entrypoint]   WARN could not chmod $dir"
done

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

# Phase 22 — ensure runtime deps that aren't baked into the base image but
# are required by UI features added in Phase 22+.  ``openpyxl`` is needed
# for pandas to read the xlsx Product Catalogue uploads.  Cheap idempotent
# install — skipped if already present.
echo "[entrypoint] ensuring Phase 22 runtime deps (openpyxl) are installed..."
python -c "import openpyxl" 2>/dev/null \
    && echo "[entrypoint]   openpyxl already installed" \
    || timeout 30 pip install --no-build-isolation 'openpyxl>=3.1,<4.0' 2>&1 \
        | sed 's|^|[entrypoint pip] |'

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
