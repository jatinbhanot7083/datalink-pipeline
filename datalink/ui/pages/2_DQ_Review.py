"""DQ Review — reviewer-facing queue + diff + approve/reject actions.

Paired with /DQ_Author. Every submission lands here; the reviewer:
  1. Sees the queue of every PENDING_REVIEW suite (across all clients).
  2. Picks one → sees side-by-side diff between the LIVE version and the
     submitted version (new expectations / removed / threshold changes).
  3. Approves (+ activates = promotes to LIVE and archives the old LIVE)
     OR requests changes (back to DRAFT with notes) OR rejects (terminal).

No direct file edits — everything audit-logged in CONTROL.dq_suite_audit_log.
"""

from __future__ import annotations

import json
import os
from contextlib import contextmanager
from typing import Any

import duckdb
import pandas as pd
import streamlit as st

from datalink.quality.registry import (
    SuiteRegistry,
    SuiteVersion,
)

# ============================================================================
# CONFIG + THEME
# ============================================================================

WAREHOUSE_PATH = os.environ.get("DL_CT_WAREHOUSE_PATH", "/opt/datalink/warehouse.duckdb")

# Phase 6 fix: bootstrap warehouse file + CONTROL schema before any
# read-only connection attempt (fresh-boot fix).
from datalink.ui._bootstrap import ensure_warehouse_exists  # noqa: E402

ensure_warehouse_exists(WAREHOUSE_PATH)

st.set_page_config(
    page_title="DataLink — DQ Review",
    page_icon="👁️",
    layout="wide",
    initial_sidebar_state="expanded",
)

_NAVY = "#0a1a3e"
_GOLD = "#d4af37"

st.markdown(
    f"""
    <style>
      h1 {{color: {_NAVY}; border-bottom: 3px solid {_GOLD}; padding-bottom: .4rem;}}
      h2 {{color: {_NAVY}; margin-top: 1.5rem;}}
      .diff-add {{background:#d1fae5;padding:.2rem .5rem;border-radius:3px}}
      .diff-remove {{background:#fee2e2;padding:.2rem .5rem;border-radius:3px;text-decoration:line-through}}
      .diff-change {{background:#fef3c7;padding:.2rem .5rem;border-radius:3px}}
      .badge-HIGH {{background:#b91c1c;color:#fff;padding:.1rem .4rem;border-radius:3px;font-size:.8rem}}
      .badge-MEDIUM {{background:#d97706;color:#fff;padding:.1rem .4rem;border-radius:3px;font-size:.8rem}}
      .badge-LOW {{background:#047857;color:#fff;padding:.1rem .4rem;border-radius:3px;font-size:.8rem}}
    </style>
    """,
    unsafe_allow_html=True,
)


# ============================================================================
# REGISTRY HELPERS (same pattern as DQ_Author)
# ============================================================================


class _DuckAdapter:
    def __init__(self, conn: duckdb.DuckDBPyConnection) -> None:
        self._conn = conn

    def query(self, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        cur = self._conn.execute(sql, params) if params else self._conn.execute(sql)
        cols = [d[0] for d in cur.description] if cur.description else []
        return [dict(zip(cols, row, strict=False)) for row in cur.fetchall()]

    def execute(self, sql: str, params: dict[str, Any] | None = None) -> None:
        if params:
            self._conn.execute(sql, params)
        else:
            self._conn.execute(sql)


from collections.abc import Iterator  # noqa: E402


@contextmanager
def _registry(*, readonly: bool = True) -> Iterator[SuiteRegistry]:
    """Open + close a fresh SuiteRegistry connection per operation. DuckDB
    disallows mixed read-only / read-write handles to the same file within
    one process; this pattern guarantees no handle outlives its use."""
    conn = duckdb.connect(WAREHOUSE_PATH, read_only=readonly)
    try:
        yield SuiteRegistry(_DuckAdapter(conn))  # type: ignore[arg-type]
    finally:
        conn.close()


# ============================================================================
# DIFF ENGINE — compare two expectation lists
# ============================================================================


def _exp_fingerprint(e: dict[str, Any]) -> str:
    """A stable identifier for matching expectations across versions.
    Uses expectation_type + column (or column_A for pairwise checks)."""
    t = e.get("expectation_type", "")
    kw = e.get("kwargs", {}) or {}
    col = kw.get("column") or kw.get("column_A") or kw.get("column_list", ["<table>"])[0]
    return f"{t}::{col}"


def _diff_suites(
    old: list[dict[str, Any]] | None,
    new: list[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    """Return {added, removed, changed} lists of expectations."""
    old = old or []
    old_by_fp = {_exp_fingerprint(e): e for e in old}
    new_by_fp = {_exp_fingerprint(e): e for e in new}
    old_keys = set(old_by_fp)
    new_keys = set(new_by_fp)
    added = [new_by_fp[k] for k in new_keys - old_keys]
    removed = [old_by_fp[k] for k in old_keys - new_keys]
    changed: list[dict[str, Any]] = []
    for k in old_keys & new_keys:
        if json.dumps(old_by_fp[k], sort_keys=True) != json.dumps(new_by_fp[k], sort_keys=True):
            changed.append({"old": old_by_fp[k], "new": new_by_fp[k]})
    return {"added": added, "removed": removed, "changed": changed}


def _exp_row(e: dict[str, Any]) -> str:
    """Short human-readable description of one expectation."""
    t = e.get("expectation_type", "")
    kw = e.get("kwargs", {}) or {}
    col = kw.get("column") or kw.get("column_A") or "—"
    severity = (e.get("meta", {}) or {}).get("severity", "")
    dim = (e.get("meta", {}) or {}).get("dq_dimension", "")
    extra = ", ".join(
        f"{k}={v}"
        for k, v in kw.items()
        if k not in {"column", "column_A", "column_B", "column_list"}
    )
    extra_s = f" ({extra})" if extra else ""
    sev_html = f'<span class="badge-{severity}">{severity}</span>' if severity else ""
    dim_html = f"<code>{dim}</code>" if dim else ""
    return f"{t} on <b>{col}</b>{extra_s} {sev_html} {dim_html}"


# ============================================================================
# RENDER PANELS (defined before page logic runs)
# ============================================================================


def _render_review_panel(selected: SuiteVersion) -> None:
    """The bottom half of the page — diff + action buttons for the picked suite."""
    st.markdown(f"## Review: `{selected.client_id}` / `{selected.suite_name}` v{selected.version}")

    # Open a fresh read connection just for this panel's reads.
    with _registry(readonly=True) as reg:
        live = reg.get_live(selected.client_id, selected.suite_name)
    live_exps = live.expectations if live else []

    diff = _diff_suites(live_exps, selected.expectations)
    c_add, c_rem, c_chg = len(diff["added"]), len(diff["removed"]), len(diff["changed"])
    st.markdown(
        f"**Diff vs LIVE** (v{live.version if live else '—'}): "
        f"<span class='diff-add'>+{c_add} added</span> · "
        f"<span class='diff-remove'>−{c_rem} removed</span> · "
        f"<span class='diff-change'>~{c_chg} changed</span>",
        unsafe_allow_html=True,
    )

    with st.expander(f"➕ Added ({c_add})", expanded=c_add > 0):
        for e in diff["added"]:
            st.markdown("- " + _exp_row(e), unsafe_allow_html=True)
    with st.expander(f"➖ Removed ({c_rem})", expanded=c_rem > 0):
        for e in diff["removed"]:
            st.markdown("- " + _exp_row(e), unsafe_allow_html=True)
    with st.expander(f"🔀 Changed ({c_chg})", expanded=c_chg > 0):
        for pair in diff["changed"]:
            st.markdown("**was:** " + _exp_row(pair["old"]), unsafe_allow_html=True)
            st.markdown("**now:** " + _exp_row(pair["new"]), unsafe_allow_html=True)
            st.markdown("---")

    st.markdown("### Full expectation list in this draft")
    full_df = pd.DataFrame(
        [
            {
                "expectation_type": e.get("expectation_type", ""),
                "column": (e.get("kwargs", {}) or {}).get(
                    "column", (e.get("kwargs", {}) or {}).get("column_A", "—")
                ),
                "dq_dimension": (e.get("meta", {}) or {}).get("dq_dimension", ""),
                "severity": (e.get("meta", {}) or {}).get("severity", ""),
                "description": (e.get("meta", {}) or {}).get("description", ""),
            }
            for e in selected.expectations
        ]
    )
    st.dataframe(full_df, use_container_width=True, hide_index=True)

    st.markdown("### Reviewer actions")
    actor = st.text_input(
        "Your identity (reviewer)",
        value=st.session_state.get("user", "reviewer@local"),
        key=f"actor_{selected.suite_id}",
    )
    notes = st.text_area(
        "Review notes",
        key=f"notes_{selected.suite_id}",
        placeholder="e.g. LGTM — aligns with DataQuality_Metrics.docx §1.4 Accuracy.",
    )
    a, b, c = st.columns(3)
    with a:
        if st.button(
            "✅ Approve & Activate → LIVE",
            use_container_width=True,
            type="primary",
            key=f"approve_{selected.suite_id}",
        ):
            try:
                with _registry(readonly=False) as reg:
                    reg.approve(selected.suite_id, actor=actor, notes=notes)
                    reg.activate(selected.suite_id, actor=actor)
                st.success(
                    f"Activated v{selected.version} as LIVE for "
                    f"`{selected.client_id}`/`{selected.suite_name}`. "
                    f"Previous LIVE v{live.version if live else '?'} archived."
                )
                st.rerun()
            except Exception as e:
                st.error(f"Approve failed: {e}")
    with b:
        if st.button(
            "📝 Request Changes → DRAFT",
            use_container_width=True,
            key=f"request_{selected.suite_id}",
        ):
            if not notes.strip():
                st.error("Please leave review notes explaining what to change.")
            else:
                try:
                    with _registry(readonly=False) as reg:
                        reg.request_changes(selected.suite_id, actor=actor, notes=notes)
                    st.success("Sent back to DRAFT for author revisions.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Request-changes failed: {e}")
    with c:
        if st.button(
            "❌ Reject (terminal)", use_container_width=True, key=f"reject_{selected.suite_id}"
        ):
            if not notes.strip():
                st.error("Please leave review notes explaining the rejection.")
            else:
                try:
                    with _registry(readonly=False) as reg:
                        reg.reject(selected.suite_id, actor=actor, notes=notes)
                    st.success("Rejected. A new draft will need to be started from scratch.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Reject failed: {e}")

    with st.expander("🧾 Audit log for this suite"):
        _show_audit(selected.suite_id)


def _show_audit(suite_id: str) -> None:
    try:
        conn = duckdb.connect(WAREHOUSE_PATH, read_only=True)
        df = conn.execute(
            """SELECT ts, from_status, to_status, actor, notes
               FROM CONTROL.dq_suite_audit_log
               WHERE suite_id = ?
               ORDER BY ts DESC""",
            [suite_id],
        ).df()
        conn.close()
        if df.empty:
            st.info("No audit entries yet.")
        else:
            st.dataframe(df, use_container_width=True, hide_index=True)
    except Exception as e:
        st.error(f"Audit log read failed: {e}")


# ============================================================================
# PAGE RENDER (top-to-bottom execution)
# ============================================================================


st.markdown(
    f"<h1>👁️ <span style='color:{_NAVY}'>DataLink</span> "
    f"<span style='color:{_GOLD}'>DQ Review</span></h1>",
    unsafe_allow_html=True,
)

st.markdown(
    '<p style="color:#4a5a7e">Every submission from /DQ_Author lands here. '
    "Review the diff vs the current LIVE, then approve + activate, request changes, or reject. "
    "All actions are audit-logged in <code>CONTROL.dq_suite_audit_log</code>.</p>",
    unsafe_allow_html=True,
)

try:
    with _registry(readonly=True) as reg:
        queue = reg.list_pending_reviews()
except Exception as e:
    st.error(f"Can't reach the warehouse: {e}")
    st.stop()

st.markdown("## Pending Reviews")

if not queue:
    st.success(
        "✓ Inbox zero. No suites waiting on review. "
        "When a DQ analyst clicks 📤 Submit on the Author page, they appear here."
    )
else:
    st.write(f"**{len(queue)}** suite(s) awaiting approval:")
    queue_df = pd.DataFrame(
        [
            {
                "client_id": v.client_id,
                "suite_name": v.suite_name,
                "version": v.version,
                "submitted_by": v.created_by,
                "submitted_at": v.submitted_at.strftime("%Y-%m-%d %H:%M")
                if v.submitted_at
                else "—",
                "expectations": len(v.expectations),
                "source": v.source.value,
                "suite_id": v.suite_id[:8],
            }
            for v in queue
        ]
    )
    st.dataframe(queue_df, use_container_width=True, hide_index=True)

    selected_label = st.selectbox(
        "Pick one to review",
        options=[f"{v.client_id} / {v.suite_name} v{v.version} ({v.suite_id[:8]})" for v in queue],
        index=0,
    )
    selected_id = selected_label.split("(")[-1].rstrip(")")
    selected = next((v for v in queue if v.suite_id.startswith(selected_id)), None)

    if selected is not None:
        _render_review_panel(selected)
