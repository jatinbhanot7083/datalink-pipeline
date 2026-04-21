#!/bin/bash
set -e

# Install datalink editable from the bind-mount so code edits are live.
if [ -f /opt/datalink/pyproject.toml ]; then
    pip install --quiet --no-deps -e /opt/datalink 2>/dev/null || true
fi

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
