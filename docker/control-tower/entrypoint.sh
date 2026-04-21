#!/bin/bash
set -e

# Install datalink editable from the bind-mount so code edits are live.
if [ -f /opt/datalink/pyproject.toml ]; then
    pip install --quiet --no-deps -e /opt/datalink 2>/dev/null || true
fi

# Phase 6 fix: pre-create warehouse.duckdb + CONTROL schema so every
# Streamlit page can open it read-only without erroring on fresh boot.
# Belt-and-braces — every page also calls ensure_warehouse_exists(), but
# doing it once here means Streamlit's own page-list probe sees a valid
# DB on the first tick of the server.
python -c "
from datalink.ui._bootstrap import ensure_warehouse_exists, default_warehouse_path
ensure_warehouse_exists(default_warehouse_path())
" 2>/dev/null || true

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
