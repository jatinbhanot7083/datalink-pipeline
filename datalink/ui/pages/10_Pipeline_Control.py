"""Pipeline Control — Phase 12 dedicated operator page.

Two halves:

  1. **Live state grid** — every (client, pipeline) pair we track, with
     current runtime status (RUNNING / PAUSED / RESUMING / ABORTED) +
     the LIVE policy that gates it (or DEFAULT if none registered).
     One-click Resume / Force Resume per row from this page so operators
     don't have to flip back to the main Control Tower.

  2. **Policy editor** — pick a (client, pipeline), see the LIVE policy
     and version history, edit thresholds. Edits go through the
     Phase-10-style HITL state machine (DRAFT → PENDING_REVIEW →
     APPROVED → LIVE), and editing a LIVE policy LOCKS the picker to
     HITL — no senior steward can unilaterally bump the abort threshold
     to 75% on a Friday afternoon. That's the "two-layer HITL" Jatin
     specified: config-time review, runtime automatic.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import pandas as pd
import streamlit as st

from datalink.pipeline.control_policy import (
    DEFAULT_POLICY,
    DriftAction,
    PipelineControlPolicyRegistry,
    PolicyDraft,
    PolicyStatus,
)
from datalink.quality.control import PipelineControl
from datalink.quality.registry import ApprovalMode

WAREHOUSE_PATH = os.environ.get("DL_CT_WAREHOUSE_PATH", "/opt/datalink/warehouse.duckdb")

from datalink.ui._bootstrap import ensure_warehouse_exists  # noqa: E402

ensure_warehouse_exists(WAREHOUSE_PATH)

st.set_page_config(
    page_title="DataLink — Pipeline Control",
    page_icon="🚦",
    layout="wide",
    initial_sidebar_state="expanded",
)

from datalink.ui._nav import render_sidebar, require_client  # noqa: E402

render_sidebar(active="Pipeline Control")
client_id = require_client()

_NAVY = "#0a1a3e"
_GOLD = "#d4af37"

st.markdown(
    f"""
    <style>
      h1 {{color: {_NAVY}; border-bottom: 3px solid {_GOLD}; padding-bottom: .4rem;}}
      h2 {{color: {_NAVY}; margin-top: 1.5rem;}}
      .state-RUNNING  {{background:#d1fae5;color:#047857;padding:.15rem .5rem;border-radius:3px;font-weight:600}}
      .state-PAUSED   {{background:#fef3c7;color:#92400e;padding:.15rem .5rem;border-radius:3px;font-weight:600}}
      .state-RESUMING {{background:#dbeafe;color:#1e40af;padding:.15rem .5rem;border-radius:3px;font-weight:600}}
      .state-ABORTED  {{background:#fee2e2;color:#991b1b;padding:.15rem .5rem;border-radius:3px;font-weight:700}}
      .pol-LIVE     {{background:#fef3c7;color:#92400e;padding:.1rem .4rem;border-radius:3px;font-weight:700;font-size:.8rem}}
      .pol-DEFAULT  {{background:#e2e8f0;color:#475569;padding:.1rem .4rem;border-radius:3px;font-size:.75rem}}
      .pol-DRAFT    {{background:#dbeafe;color:#1e40af;padding:.1rem .4rem;border-radius:3px;font-size:.8rem}}
      .pol-PENDING  {{background:#fde68a;color:#92400e;padding:.1rem .4rem;border-radius:3px;font-size:.8rem}}
    </style>
    """,
    unsafe_allow_html=True,
)


@contextmanager
def _warehouse(*, readonly: bool = True) -> Iterator[Any]:
    from datalink.ui._query import warehouse_ctx

    with warehouse_ctx(readonly=readonly) as wh:
        yield wh


PIPELINE_IDS = ["bronze_ingest", "silver_build", "gold_egress"]


# ============================================================================
# HEADER
# ============================================================================

st.markdown("# 🚦 Pipeline Control")
st.caption(
    f"Threshold policies + live state for **{client_id}**. "
    f"Edits go through HITL review (Phase-10 state machine); runtime checks "
    f"fire automatically against the LIVE policy."
)


# ============================================================================
# 1. LIVE STATE GRID
# ============================================================================

st.markdown("## Live state — all pipelines")
st.caption(
    "Current runtime state and the policy gating it. PAUSE / ABORT / Resume "
    "actions live on the main Control Tower; this page is for **policy** edits "
    "(thresholds, schema-drift action)."
)

with _warehouse(readonly=True) as wh:
    control = PipelineControl(wh)
    control.ensure()
    policy_reg = PipelineControlPolicyRegistry(wh)

    grid_rows: list[dict[str, Any]] = []
    for pid in PIPELINE_IDS:
        # control.current returns PipelineState (StrEnum) | None. We only
        # surface the runtime state label here; reason / actor live in
        # pipeline_control_audit_log which the main Control Tower page
        # already renders — no need to duplicate here.
        state = control.current(pid, client_id=client_id)
        policy = policy_reg.get_or_default(client_id, pid)
        grid_rows.append(
            {
                "Pipeline": pid,
                "Runtime state": state.value if state else "—",
                "Policy": (
                    f"v{policy.version} (LIVE)"
                    if not policy.is_default
                    else "DEFAULT (no policy registered)"
                ),
                "Pause >": f"{policy.fail_rate_pause_pct}%",
                "Abort >": f"{policy.fail_rate_abort_pct}%",
                "Drift action": policy.schema_drift_action,
            }
        )

st.dataframe(
    pd.DataFrame(grid_rows),
    use_container_width=True,
    hide_index=True,
)


# ============================================================================
# 2. POLICY EDITOR
# ============================================================================

st.markdown("## Threshold policy editor")

c1, c2 = st.columns([1, 4])
with c1:
    pipeline_id = st.selectbox(
        "Pipeline",
        options=PIPELINE_IDS,
        index=0,
        help="Pick which pipeline's threshold policy to view / edit.",
    )

with _warehouse(readonly=True) as wh:
    policy_reg = PipelineControlPolicyRegistry(wh)
    active = policy_reg.get_active_policy(client_id, pipeline_id)
    versions = policy_reg.list_versions(client_id, pipeline_id)
    latest_draft = next((v for v in versions if v.status is PolicyStatus.DRAFT), None)

# ---- Active policy ---------------------------------------------------------

st.markdown("### Active policy")

if active is None:
    st.info(
        f"⚠️ No registered policy for **{client_id} / {pipeline_id}**. "
        f"Runtime falls back to **DEFAULT_POLICY**: pause if any failure, "
        f"abort if > 25% fail rate, schema drift action = `PAUSE`. "
        f"Author your first policy below to gain per-tenant control."
    )

    with st.expander("➕ Author first policy from DEFAULT", expanded=True):
        st.caption(
            "Pre-fills the form with the DEFAULT_POLICY values so this is a "
            "no-behavior-change starting point — adjust the numbers, route "
            "through HITL review, activate."
        )
        with st.form(key=f"create_{pipeline_id}", clear_on_submit=False):
            c_a, c_b = st.columns(2)
            with c_a:
                pause_pct = st.number_input(
                    "Pause if fail % >",
                    min_value=0.0,
                    max_value=100.0,
                    value=DEFAULT_POLICY.fail_rate_pause_pct,
                    step=1.0,
                    help="Pipeline pauses (reversible Resume) when fail "
                    "rate exceeds this. 0.0 = pause on any failure.",
                )
                abort_pct = st.number_input(
                    "Abort if fail % >",
                    min_value=0.0,
                    max_value=100.0,
                    value=DEFAULT_POLICY.fail_rate_abort_pct,
                    step=1.0,
                    help="Pipeline aborts (terminal — Force Resume only) "
                    "when fail rate exceeds this. Must be ≥ pause threshold.",
                )
            with c_b:
                drift_action = st.selectbox(
                    "Schema drift action (FATAL drift)",
                    options=[a.value for a in DriftAction],
                    index=0,  # PAUSE
                )
                row_drop_pct = st.number_input(
                    "Row count drop % (reserved)",
                    min_value=0.0,
                    max_value=100.0,
                    value=DEFAULT_POLICY.row_count_drop_pct,
                    step=1.0,
                    help="Phase 12.5 placeholder — runtime check not yet "
                    "wired. Threshold field reserved so we don't need a "
                    "DDL change later.",
                )
            notes = st.text_input(
                "Notes (audit trail)",
                value=f"initial policy for {pipeline_id}",
            )
            mode_pick = st.radio(
                "Submit as:",
                options=[
                    ApprovalMode.DRAFT,
                    ApprovalMode.HITL,
                    ApprovalMode.AUTO_APPROVE,
                ],
                index=1,  # HITL default
                horizontal=True,
                format_func=lambda m: {
                    ApprovalMode.DRAFT: "📝 DRAFT",
                    ApprovalMode.HITL: "👁️ HITL (review queue)",
                    ApprovalMode.AUTO_APPROVE: "⚡ AUTO_APPROVE (skip review)",
                }[m],
            )
            submitted = st.form_submit_button("📌 Create policy", type="primary")

        if submitted:
            try:
                draft = PolicyDraft(
                    client_id=client_id,
                    pipeline_id=pipeline_id,
                    fail_rate_pause_pct=float(pause_pct),
                    fail_rate_abort_pct=float(abort_pct),
                    schema_drift_action=str(drift_action),
                    row_count_drop_pct=float(row_drop_pct),
                    created_by=st.session_state.get("user", "anonymous@local"),
                    notes=notes,
                )
                with _warehouse(readonly=False) as wh:
                    reg = PipelineControlPolicyRegistry(wh)
                    new_id = reg.create_draft(draft)
                    reg.submit_with_policy(
                        new_id,
                        mode=mode_pick,
                        actor=st.session_state.get("user", "anonymous@local"),
                        approval_notes=(
                            "DQ Author auto-approve"
                            if mode_pick is ApprovalMode.AUTO_APPROVE
                            else None
                        ),
                    )
                final_state = {
                    ApprovalMode.DRAFT: "DRAFT (still editable)",
                    ApprovalMode.HITL: "PENDING_REVIEW",
                    ApprovalMode.AUTO_APPROVE: "LIVE",
                }[mode_pick]
                st.success(f"✅ Policy created — `{new_id[:8]}…` is now **{final_state}**.")
                st.rerun()
            except ValueError as e:
                st.error(f"Validation: {e}")
            except Exception as e:
                st.error(f"Create failed: {type(e).__name__}: {e}")
else:
    h1, h2, h3 = st.columns([3, 1, 1])
    with h1:
        st.markdown(
            f"**v{active.version}** · `{active.policy_id[:8]}…` · "
            f"by `{active.created_by}` · "
            f"activated {active.activated_at.strftime('%Y-%m-%d %H:%M') if active.activated_at else '—'}"
        )
        if active.notes:
            st.caption(f"_{active.notes}_")
    with h2:
        st.metric("Pause >", f"{active.fail_rate_pause_pct}%")
    with h3:
        st.metric("Abort >", f"{active.fail_rate_abort_pct}%")

    df_active = pd.DataFrame(
        [
            {
                "Field": "fail_rate_pause_pct",
                "Value": f"{active.fail_rate_pause_pct}%",
                "Effect": "Pipeline PAUSES when fail rate exceeds this (reversible).",
            },
            {
                "Field": "fail_rate_abort_pct",
                "Value": f"{active.fail_rate_abort_pct}%",
                "Effect": "Pipeline ABORTS when fail rate exceeds this (Force Resume only).",
            },
            {
                "Field": "schema_drift_action",
                "Value": active.schema_drift_action,
                "Effect": "What happens when Bronze drift pre-check classifies FATAL drift.",
            },
            {
                "Field": "row_count_drop_pct (reserved)",
                "Value": f"{active.row_count_drop_pct}%",
                "Effect": "Phase 12.5 placeholder — runtime check not yet wired.",
            },
        ]
    )
    st.dataframe(df_active, use_container_width=True, hide_index=True)

    if len(versions) > 1:
        with st.expander(f"🕰 Version history ({len(versions)} versions)"):
            df_v = pd.DataFrame(
                [
                    {
                        "version": v.version,
                        "status": v.status.value,
                        "pause %": v.fail_rate_pause_pct,
                        "abort %": v.fail_rate_abort_pct,
                        "drift action": v.schema_drift_action,
                        "by": v.created_by,
                        "created": v.created_at.strftime("%Y-%m-%d %H:%M"),
                        "reviewed_by": v.reviewed_by or "—",
                    }
                    for v in versions
                ]
            )
            st.dataframe(df_v, use_container_width=True, hide_index=True)

    # ---- Edit / fork action -------------------------------------------------

    st.markdown("### Edit thresholds")
    st.caption(
        f"🔒 **Editing v{active.version} (LIVE)** — by Phase-10 policy this fork "
        f"will be locked to **HITL review**. Senior stewards cannot bypass review "
        f"on production threshold edits."
    )

    if latest_draft is None:
        if st.button(
            f"🔀 Fork v{active.version} to a new DRAFT for editing",
            type="primary",
        ):
            try:
                with _warehouse(readonly=False) as wh:
                    reg = PipelineControlPolicyRegistry(wh)
                    new_id = reg.fork_for_edit(
                        active.policy_id,
                        actor=st.session_state.get("user", "anonymous@local"),
                    )
                st.success(
                    f"Forked v{active.version} → DRAFT v{active.version + 1} "
                    f"(`{new_id[:8]}…`). Reload to edit."
                )
                st.rerun()
            except Exception as e:
                st.error(f"Fork failed: {type(e).__name__}: {e}")
    else:
        # Editable form for the existing draft.
        with st.form(key=f"edit_{latest_draft.policy_id}", clear_on_submit=False):
            st.markdown(
                f"**Editing v{latest_draft.version} DRAFT** " f"`{latest_draft.policy_id[:8]}…`"
            )
            c_a, c_b = st.columns(2)
            with c_a:
                e_pause = st.number_input(
                    "Pause if fail % >",
                    min_value=0.0,
                    max_value=100.0,
                    value=latest_draft.fail_rate_pause_pct,
                    step=1.0,
                )
                e_abort = st.number_input(
                    "Abort if fail % >",
                    min_value=0.0,
                    max_value=100.0,
                    value=latest_draft.fail_rate_abort_pct,
                    step=1.0,
                )
            with c_b:
                e_drift = st.selectbox(
                    "Schema drift action",
                    options=[a.value for a in DriftAction],
                    index=[a.value for a in DriftAction].index(latest_draft.schema_drift_action),
                )
                e_row = st.number_input(
                    "Row count drop % (reserved)",
                    min_value=0.0,
                    max_value=100.0,
                    value=latest_draft.row_count_drop_pct,
                    step=1.0,
                )
            e_notes = st.text_input(
                "Notes",
                value=latest_draft.notes or "",
            )
            # Edit-of-LIVE → HITL forced (no AUTO_APPROVE option).
            mode_pick = st.radio(
                "On Submit, route as:",
                options=[ApprovalMode.DRAFT, ApprovalMode.HITL],
                index=1,
                horizontal=True,
                format_func=lambda m: {
                    ApprovalMode.DRAFT: "📝 Save DRAFT (edit later)",
                    ApprovalMode.HITL: "👁️ Submit for Review (HITL)",
                }[m],
            )
            edit_submitted = st.form_submit_button(
                "💾 Save / submit",
                type="primary",
            )

        if edit_submitted:
            try:
                actor = st.session_state.get("user", "anonymous@local")
                with _warehouse(readonly=False) as wh:
                    reg = PipelineControlPolicyRegistry(wh)
                    reg.update_draft(
                        latest_draft.policy_id,
                        fail_rate_pause_pct=float(e_pause),
                        fail_rate_abort_pct=float(e_abort),
                        schema_drift_action=str(e_drift),
                        row_count_drop_pct=float(e_row),
                        actor=actor,
                        notes=e_notes,
                    )
                    if mode_pick is ApprovalMode.HITL:
                        reg.submit_with_policy(
                            latest_draft.policy_id,
                            mode=ApprovalMode.HITL,
                            actor=actor,
                        )
                    final = (
                        "PENDING_REVIEW (waiting on reviewer)"
                        if mode_pick is ApprovalMode.HITL
                        else "DRAFT (still editable)"
                    )
                st.success(f"✅ Draft updated. Status: **{final}**.")
                st.rerun()
            except ValueError as e:
                st.error(f"Validation: {e}")
            except Exception as e:
                st.error(f"Save failed: {type(e).__name__}: {e}")


# ============================================================================
# 3. PENDING REVIEWS (cross-cutting)
# ============================================================================

st.markdown("## Pending policy reviews")
st.caption(
    "Every policy currently awaiting reviewer approval, across all pipelines "
    "for this client. Reviewers approve / reject from this list."
)

with _warehouse(readonly=True) as wh:
    reg = PipelineControlPolicyRegistry(wh)
    pending = [p for p in reg.list_pending_reviews() if p.client_id == client_id]

if not pending:
    st.info("No policy reviews pending for this client.")
else:
    for p in pending:
        with st.container(border=True):
            r_l, r_r = st.columns([3, 1])
            with r_l:
                st.markdown(
                    f"**{p.pipeline_id}** v{p.version} · `{p.policy_id[:8]}…` · "
                    f"submitted by `{p.created_by}` · "
                    f"{p.submitted_at.strftime('%Y-%m-%d %H:%M') if p.submitted_at else '—'}"
                )
                st.caption(
                    f"pause > **{p.fail_rate_pause_pct}%** · "
                    f"abort > **{p.fail_rate_abort_pct}%** · "
                    f"drift = **{p.schema_drift_action}**"
                )
                if p.notes:
                    st.caption(f"_{p.notes}_")
            with r_r:
                approve_notes = st.text_input(
                    "Approval notes",
                    value="LGTM",
                    key=f"appr_{p.policy_id}",
                )
                ba, br = st.columns(2)
                if ba.button(
                    "✅ Approve & Activate",
                    key=f"ok_{p.policy_id}",
                    type="primary",
                    use_container_width=True,
                ):
                    try:
                        actor = st.session_state.get("user", "anonymous@local")
                        with _warehouse(readonly=False) as wh:
                            reg = PipelineControlPolicyRegistry(wh)
                            reg.approve(p.policy_id, actor=actor, notes=approve_notes)
                            reg.activate(p.policy_id, actor=actor)
                        st.success(f"v{p.version} → LIVE.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Approve failed: {e}")
                if br.button(
                    "❌ Reject",
                    key=f"rej_{p.policy_id}",
                    use_container_width=True,
                ):
                    try:
                        actor = st.session_state.get("user", "anonymous@local")
                        with _warehouse(readonly=False) as wh:
                            reg = PipelineControlPolicyRegistry(wh)
                            reg.reject(p.policy_id, actor=actor, notes=approve_notes)
                        st.warning(f"v{p.version} → REJECTED.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Reject failed: {e}")
