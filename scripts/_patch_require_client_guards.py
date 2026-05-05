"""Batch-patch every page that calls require_client() to handle None
gracefully. Phase 16.6 (Jatin's directive): default = ALL clients.

Inserts a uniform None-guard right after the require_client() call:

    selected_client = require_client()
    if selected_client is None:
        st.info("🌐 **All-clients view** — pick a client above to scope.")
        st.stop()

(For pages that have a meaningful all-clients view, the guard can be
swapped to render that view instead of stopping. We do that for the
flagship pages individually.)
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path("/home/jatin/dev/DataPipelinesWithGX")

UNIVERSAL_GUARD = """
# Phase 16.6 — when no client picked (default = All clients), gate
# the per-client content with a friendly notice. Pages with a true
# all-clients view (Pipeline Architect) handle this differently.
if {var_name} is None:
    import streamlit as _st  # noqa: PLC0415
    _st.info(
        "🌐 **All-clients view.** Pick a client from the dropdown above "
        "to load this client-scoped page. Cross-tenant dashboards "
        "(Control Tower, PHI Governance, Cost & Tokens, Lineage) live "
        "elsewhere and don't need a client picker."
    )
    _st.stop()
"""


PAGES = [
    ("datalink/ui/pages/10_Pipeline_Control.py", "client_id"),
    ("datalink/ui/pages/15_Run_Monitor.py", "selected_client"),
    ("datalink/ui/pages/3_Executive_Dashboard.py", "selected_client"),
    ("datalink/ui/pages/5_DQ_Dashboard.py", "selected_client"),
    ("datalink/ui/pages/6_DQ_Suite_Registry.py", "selected_client"),
    ("datalink/ui/pages/8_DQ_AI_Architect.py", "selected_client"),
    ("datalink/ui/pages/9_Schema_Drift.py", "client_id"),
]


def patch(path: Path, var_name: str) -> bool:
    """Insert the guard immediately after the var_name = require_client() line.
    Returns True if a patch was applied (False if already present)."""
    text = path.read_text(encoding="utf-8")

    if "Phase 16.6 — when no client picked" in text:
        return False  # already patched

    # Match `selected_client = require_client()` (or client_id =) — exact line
    pattern = rf"({re.escape(var_name)}\s*=\s*require_client\(\))\s*\n"
    guard = UNIVERSAL_GUARD.format(var_name=var_name).strip("\n")
    replacement = rf"\1\n{guard}\n"

    new_text, n = re.subn(pattern, replacement, text, count=1)
    if n == 0:
        print(f"  [skip] {path.name} — no exact match for `{var_name} = require_client()`")
        return False
    path.write_text(new_text, encoding="utf-8")
    return True


def main() -> int:
    for rel_path, var_name in PAGES:
        path = ROOT / rel_path
        if not path.exists():
            print(f"  [miss] {rel_path}")
            continue
        if patch(path, var_name):
            print(f"  [OK]   {rel_path}")
        else:
            print(f"  [noop] {rel_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
