"""DQ Review — Phase 19.3 (factory-pattern, peer review).

Reviewer-facing queue for DQ suites awaiting promotion.  Per (dataset, layer):
  * See the DRAFT / PENDING_REVIEW suite next to the current LIVE (if any)
  * Side-by-side expectations diff (added / changed / removed)
  * Approve & Promote → DRAFT becomes LIVE; prior LIVE archived
  * Reject + notes → DRAFT becomes ARCHIVED with reviewer feedback

No client filter — suites are universal in the factory pattern.
"""

from __future__ import annotations

import json
import sys
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import streamlit as st

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

st.set_page_config(
    page_title="DQ Review",
    page_icon="✅",
    layout="wide",
    initial_sidebar_state="expanded",
)

from datalink.agents.dq_ai_architect import approve_and_promote  # noqa: E402
from datalink.quality.control import CONTROL_SCHEMA, create_control_tables  # noqa: E402
from datalink.ui._nav import render_sidebar  # noqa: E402
from datalink.ui._query import warehouse_ctx  # noqa: E402

render_sidebar(active="DQ Review")

_NAVY = "#0a1a3e"
_GOLD = "#d4af37"

st.markdown(
    f"""
    <style>
      h1 {{color: {_NAVY}; border-bottom: 3px solid {_GOLD}; padding-bottom: .4rem;}}
      h2 {{color: {_NAVY}; margin-top: 1.2rem;}}
    </style>
    """,
    unsafe_allow_html=True,
)


@contextmanager
def _warehouse(*, readonly: bool = True) -> Iterator[Any]:
    with warehouse_ctx(readonly=readonly) as wh:
        yield wh


with _warehouse(readonly=False) as _wh_boot:
    create_control_tables(_wh_boot)


st.markdown("# ✅ DQ Review")
st.caption(
    "Peer-review queue for DQ suites awaiting promotion.  Compare the "
    "DRAFT against the current LIVE for the same `(dataset, layer)`, "
    "approve to promote, or reject with notes."
)


# ---------------------------------------------------------------------------
# Pickers
# ---------------------------------------------------------------------------
@st.cache_data(ttl=30)
def _pending_suites() -> list[dict[str, Any]]:
    with warehouse_ctx(readonly=True) as wh:
        try:
            return list(
                wh.query(
                    f"""
                    SELECT suite_id, suite_name, dataset_code, layer, version, status,
                           expectations, dq_dimensions, ai_rationale,
                           ai_token_count, created_by, created_at
                      FROM {CONTROL_SCHEMA}.dq_suites
                     WHERE dataset_code IS NOT NULL AND layer IS NOT NULL
                       AND status IN ('DRAFT', 'PENDING_REVIEW')
                     ORDER BY created_at DESC
                    """
                )
            )
        except Exception:
            return []


@st.cache_data(ttl=30)
def _live_suite_for(dataset_code: str, layer: str) -> dict[str, Any] | None:
    with warehouse_ctx(readonly=True) as wh:
        try:
            rows = list(
                wh.query(
                    f"""
                    SELECT suite_id, suite_name, version, expectations,
                           ai_rationale, activated_at, reviewed_by
                      FROM {CONTROL_SCHEMA}.dq_suites
                     WHERE LOWER(dataset_code) = LOWER($ds)
                       AND UPPER(layer) = UPPER($lyr)
                       AND status = 'LIVE'
                     ORDER BY version DESC LIMIT 1
                    """,
                    {"ds": dataset_code, "lyr": layer},
                )
            )
            return rows[0] if rows else None
        except Exception:
            return None


pending = _pending_suites()
qp = st.query_params

# Allow ?suite_id=X deep-link from Suite Registry
deep_pick = qp.get("suite_id") if isinstance(qp, dict) else None

m1, m2, m3 = st.columns(3)
m1.metric("📝 DRAFT pending review", len(pending))
m2.metric("Distinct datasets", len({p.get("dataset_code") for p in pending}))
m3.metric(
    "Distinct (dataset, layer) pairs",
    len({(p.get("dataset_code"), p.get("layer")) for p in pending}),
)

if not pending:
    st.success(
        "🎉 No suites awaiting review.  All caught up.  Use **🧪 DQ AI Architect** "
        "to design new suites."
    )
    st.stop()


# Build picker
options = {
    f"{p.get('suite_name')} · v{p.get('version')} · "
    f"{p.get('status')} · created {(str(p.get('created_at') or ''))[:19]}": str(p.get("suite_id"))
    for p in pending
}

# Find label that matches deep_pick
default_label = "— select a suite —"
if deep_pick:
    for label, sid in options.items():
        if sid == deep_pick:
            default_label = label
            break

pick_labels = ["— select a suite —", *options.keys()]
default_idx = pick_labels.index(default_label) if default_label in pick_labels else 0

picked_label = st.selectbox(
    "Suite to review",
    options=pick_labels,
    index=default_idx,
    key="dqr_pick",
)

if picked_label == "— select a suite —":
    st.info("Pick a suite above to start review.")
    st.stop()

picked_suite_id = options[picked_label]
picked = next(p for p in pending if str(p.get("suite_id")) == picked_suite_id)
ds = picked.get("dataset_code")
layer = picked.get("layer")
version = picked.get("version")

try:
    new_exps = json.loads(picked.get("expectations") or "[]")
except Exception:
    new_exps = []

live = _live_suite_for(ds, layer)
try:
    live_exps = json.loads((live or {}).get("expectations") or "[]")
except Exception:
    live_exps = []


# ---------------------------------------------------------------------------
# Header strip
# ---------------------------------------------------------------------------
st.markdown(f"## 📝 Reviewing `{picked.get('suite_name')}` · v{version}")
strip_cols = st.columns(4)
strip_cols[0].metric("Dataset", ds)
strip_cols[1].metric("Layer", layer)
strip_cols[2].metric("Expectations (this DRAFT)", len(new_exps))
strip_cols[3].metric(
    "Expectations (current LIVE)",
    len(live_exps) if live else "(none yet)",
)


# ---------------------------------------------------------------------------
# Diff
# ---------------------------------------------------------------------------
def _exp_signature(e: dict[str, Any]) -> str:
    """Stable identifier for a single expectation (for set diff)."""
    et = str(e.get("expectation_type") or "")
    kwargs = e.get("kwargs") or {}
    col = str(kwargs.get("column") or kwargs.get("column_list") or "")
    extras = sorted(
        f"{k}={v}"
        for k, v in kwargs.items()
        if k not in ("column", "column_list") and v is not None
    )
    return f"{et}|{col}|{';'.join(extras)}"


live_sigs = {_exp_signature(e): e for e in live_exps}
new_sigs = {_exp_signature(e): e for e in new_exps}
added = [new_sigs[s] for s in new_sigs.keys() - live_sigs.keys()]
removed = [live_sigs[s] for s in live_sigs.keys() - new_sigs.keys()]
unchanged = [new_sigs[s] for s in new_sigs.keys() & live_sigs.keys()]

dc1, dc2, dc3 = st.columns(3)
dc1.metric("➕ Added", len(added))
dc2.metric("➖ Removed", len(removed))
dc3.metric("🔁 Unchanged", len(unchanged))


tab_diff, tab_full, tab_rationale = st.tabs(["📑 Diff", "✅ Full DRAFT suite", "🤖 AI Rationale"])

with tab_diff:
    if added:
        st.markdown("### ➕ Added in DRAFT")
        for e in added:
            meta = e.get("meta") or {}
            st.markdown(
                f"- `{e.get('expectation_type')}` on "
                f"`{(e.get('kwargs') or {}).get('column', '—')}`  "
                f"· **{meta.get('dq_dimension', '—')}** / {meta.get('severity', '—')}"
                f" — {meta.get('description', '')[:160]}"
            )
    if removed:
        st.markdown("### ➖ Removed from LIVE")
        for e in removed:
            meta = e.get("meta") or {}
            st.markdown(
                f"- `{e.get('expectation_type')}` on "
                f"`{(e.get('kwargs') or {}).get('column', '—')}`  "
                f"· **{meta.get('dq_dimension', '—')}** / {meta.get('severity', '—')}"
            )
    if not added and not removed:
        st.info(
            "No structural diff vs current LIVE — DRAFT proposes the same "
            "expectation set.  Promotion is still meaningful if metadata "
            "(rationale, kwargs detail) changed."
        )

with tab_full:
    for i, e in enumerate(new_exps):
        meta = e.get("meta") or {}
        with st.container(border=True):
            st.markdown(
                f"**{i+1}.** `{e.get('expectation_type')}` on "
                f"`{(e.get('kwargs') or {}).get('column', '—')}`  "
                f"· {meta.get('dq_dimension', '—')} / {meta.get('severity', '—')}"
            )
            if meta.get("description"):
                st.caption(meta["description"])
            if meta.get("rationale"):
                st.markdown(f"_{meta['rationale']}_")

with tab_rationale:
    rat = (picked.get("ai_rationale") or "").strip()
    if rat:
        st.markdown(rat)
    else:
        st.caption("_(no rationale recorded)_")


# ---------------------------------------------------------------------------
# Approve / Reject
# ---------------------------------------------------------------------------
st.markdown("---")
ac1, ac2 = st.columns([1, 1])

with ac1:
    notes_a = st.text_input(
        "Approval notes (optional)",
        value="",
        key="dqr_approve_notes",
        placeholder="e.g., 'reviewed by jbhanot — promote'",
    )
    if st.button(
        "🚀 Approve & Promote (LIVE)",
        type="primary",
        use_container_width=True,
        key="dqr_approve",
    ):
        try:
            with _warehouse(readonly=False) as wh:
                _r = approve_and_promote(
                    warehouse=wh,
                    suite_id=picked_suite_id,
                    actor="ui:dq_review",
                    notes=notes_a,
                )
            st.toast(
                f"🚀 Promoted {ds}/{layer} v{version}",
                icon="✅",
            )
            _pending_suites.clear()
            _live_suite_for.clear()
            st.rerun()
        except Exception as exc:
            st.error(f"Promote failed: {type(exc).__name__}: {exc}")

with ac2:
    notes_r = st.text_input(
        "Rejection notes (REQUIRED)",
        value="",
        key="dqr_reject_notes",
        placeholder="e.g., 'too aggressive — relax phone-format regex'",
    )
    if st.button(
        "🚫 Reject (Archive DRAFT)",
        use_container_width=True,
        key="dqr_reject",
        disabled=not notes_r.strip(),
        help="Archives the DRAFT with reviewer feedback.  No change to LIVE.",
    ):
        try:
            with _warehouse(readonly=False) as wh:
                wh.execute(
                    f"""
                    UPDATE {CONTROL_SCHEMA}.dq_suites
                       SET status = 'ARCHIVED',
                           archived_at = CURRENT_TIMESTAMP(),
                           reviewed_by = $by, reviewed_at = CURRENT_TIMESTAMP(),
                           review_notes = $notes
                     WHERE suite_id = $id
                    """,
                    {"id": picked_suite_id, "by": "ui:dq_review", "notes": notes_r},
                )
                wh.execute(
                    f"""
                    INSERT INTO {CONTROL_SCHEMA}.dq_suite_audit_log
                      (audit_id, suite_id, from_status, to_status, actor, notes)
                    VALUES ($aid, $sid, $from, 'ARCHIVED', 'ui:dq_review', $notes)
                    """,
                    {
                        "aid": str(uuid.uuid4()),
                        "sid": picked_suite_id,
                        "from": picked.get("status"),
                        "notes": notes_r,
                    },
                )
            st.toast(f"🚫 Rejected {ds}/{layer} v{version}", icon="🗄")
            _pending_suites.clear()
            st.rerun()
        except Exception as exc:
            st.error(f"Reject failed: {type(exc).__name__}: {exc}")
