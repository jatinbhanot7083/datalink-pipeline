"""Pipeline Control — Phase 12 + Phase 12.5 operator page.

Three layers of policy granularity (most-specific wins):

  1. **Per-client + per-source** — e.g. ``aetna / bronze_ingest / CLAIMS``
  2. **Per-client pipeline-wide** — e.g. ``aetna / bronze_ingest / (any)``
  3. **Global per-source / global pipeline-wide** — ``* / bronze_ingest / …``

Anything that isn't covered cascades down to ``DEFAULT_POLICY``, the
hardcoded fallback that mirrors the pre-Phase-12 behaviour
(pause > 0%, abort > 25%, drift = PAUSE).

The page has four sections:

  * **Resolved policies** grid — for every (pipeline x source_type) tuple
    we show which tier won the cascade for the current client.
  * **Scope picker** — author for the current client OR for the global
    ``*`` tier (which all clients inherit when they don't override).
  * **Editor** — pick a pipeline + source_type, see / edit / fork / submit
    the LIVE policy for that exact tuple. HITL state machine identical
    to Phase 10/12.
  * **Pending reviews** + **Cross-client clone** action on each LIVE
    policy.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import pandas as pd
import streamlit as st

from datalink.pipeline.control_policy import (
    GLOBAL_CLIENT,
    KNOWN_SOURCE_TYPES,
    DriftAction,
    PipelineControlPolicyRegistry,
    PolicyDraft,
    PolicyStatus,
    ThresholdPolicy,
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
# Phase 16.6 — when no client picked (default = All clients), gate
# the per-client content with a friendly notice. Pages with a true
# all-clients view (Pipeline Architect) handle this differently.
if client_id is None:
    import streamlit as _st

    _st.info(
        "🌐 **All-clients view.** Pick a client from the dropdown above "
        "to load this client-scoped page. Cross-tenant dashboards "
        "(Control Tower, PHI Governance, Cost & Tokens, Lineage) live "
        "elsewhere and don't need a client picker."
    )
    _st.stop()
_NAVY = "#0a1a3e"
_GOLD = "#d4af37"

st.markdown(
    f"""
    <style>
      h1 {{color: {_NAVY}; border-bottom: 3px solid {_GOLD}; padding-bottom: .4rem;}}
      h2 {{color: {_NAVY}; margin-top: 1.5rem;}}
      .scope-pill   {{background:{_GOLD};color:{_NAVY};padding:.15rem .55rem;border-radius:3px;font-weight:700;font-size:.78rem}}
      .tier-client  {{background:#d1fae5;color:#047857;padding:.1rem .4rem;border-radius:3px;font-size:.75rem}}
      .tier-global  {{background:#dbeafe;color:#1e3a8a;padding:.1rem .4rem;border-radius:3px;font-size:.75rem}}
      .tier-default {{background:#e5e7eb;color:#4b5563;padding:.1rem .4rem;border-radius:3px;font-size:.75rem}}

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
# Source type options for the editor — None marker first ("pipeline-wide").
SOURCE_TYPE_OPTIONS: list[str | None] = [None, *KNOWN_SOURCE_TYPES]


def _src_label(s: str | None) -> str:
    return "(any) — pipeline-wide" if s is None else s


def _client_options(reg: PipelineControlPolicyRegistry) -> list[str]:
    """Real clients (excluding 'default') for the clone-to-client picker.
    Sourced from the policies that already exist + the standard 6 tenants."""
    seen: set[str] = set()
    for p in reg.list_all_active():
        if p.client_id != GLOBAL_CLIENT:
            seen.add(p.client_id)
    # Make sure the standard set is always offered.
    seen.update(["aetna", "caresource", "affinity", "coaccess", "dhmp", "hcsc"])
    return sorted(seen)


def _resolve_tier(policy: ThresholdPolicy, current_client: str) -> tuple[str, str]:
    """Classify which tier of the cascade produced ``policy``. Returns
    ``(label, css_class)`` for badge rendering."""
    if policy.is_default:
        return ("DEFAULT (hardcoded)", "tier-default")
    if policy.client_id == current_client:
        return (
            f"client / {policy.source_type or 'pipeline-wide'} v{policy.version}",
            "tier-client",
        )
    if policy.is_global:
        return (
            f"global * / {policy.source_type or 'pipeline-wide'} v{policy.version}",
            "tier-global",
        )
    return (
        f"{policy.client_id} / {policy.source_type or 'pipeline-wide'} v{policy.version}",
        "tier-default",
    )


# ============================================================================
# HEADER
# ============================================================================

st.markdown("# 🚦 Pipeline Control")
st.caption(
    f"Threshold policies + live state for **{client_id}**. "
    f"Cascade lookup: client→source > client→pipeline-wide > global→source > global→pipeline-wide > DEFAULT."
)


# ============================================================================
# 1. ACTIVE POLICIES REGISTERED — every LIVE policy for client + global *
# ============================================================================

st.markdown("## Active policies registered")
st.caption(
    f"Every LIVE policy for **{client_id}** and the **global `*`** tier. "
    f"This is what you've actually authored — the resolution grid below "
    f"shows which one wins per tuple after the 5-tier cascade."
)

with _warehouse(readonly=True) as wh:
    control = PipelineControl(wh)
    control.ensure()
    policy_reg = PipelineControlPolicyRegistry(wh)

    all_live = policy_reg.list_all_active()
    scoped = [p for p in all_live if p.client_id in (client_id, GLOBAL_CLIENT)]

    # Pre-compute the resolution grid in the same warehouse trip so we don't
    # pay another connection-open round-trip.
    grid_rows: list[dict[str, Any]] = []
    for pid in PIPELINE_IDS:
        runtime_state = control.current(pid, client_id=client_id)
        runtime_label = runtime_state.value if runtime_state else "—"
        for src in SOURCE_TYPE_OPTIONS:
            policy = policy_reg.get_or_default(client_id, pid, source_type=src)
            tier_label, _tier_cls = _resolve_tier(policy, client_id)
            grid_rows.append(
                {
                    "Pipeline": pid,
                    "Source": _src_label(src),
                    "Runtime state": runtime_label,
                    "Resolved policy": tier_label,
                    "Pause >": f"{policy.fail_rate_pause_pct}%",
                    "Abort >": f"{policy.fail_rate_abort_pct}%",
                    "Drift action": policy.schema_drift_action,
                }
            )

if not scoped:
    st.info(
        f"No LIVE policies registered for **{client_id}** or global. "
        f"Runtime cascade falls through to hard-coded DEFAULT for every "
        f"pipeline. Author your first policy in the editor below."
    )
else:
    # Headline metrics — quick visual sense of the policy footprint.
    tenant_count = sum(1 for p in scoped if p.client_id == client_id)
    global_count = sum(1 for p in scoped if p.client_id == GLOBAL_CLIENT)
    src_specific = sum(1 for p in scoped if p.source_type is not None)
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total LIVE", len(scoped))
    m2.metric(f"📍 {client_id}", tenant_count)
    m3.metric("🌐 Global (*)", global_count)
    m4.metric("Per-source-type", src_specific)

    # ----------------------------------------------------------------------
    # Native st.dataframe — same visual pattern as the "Resolved policies"
    # table below. Row selection turns Edit / Archive on in the toolbar
    # underneath; standard Streamlit pattern, no custom CSS.
    # ----------------------------------------------------------------------
    df_active = pd.DataFrame(
        [
            {
                "Scope": "🌐 GLOBAL (*)" if p.client_id == GLOBAL_CLIENT else f"📍 {p.client_id}",
                "Pipeline": p.pipeline_id,
                "Source": _src_label(p.source_type),
                "v": p.version,
                "Pause >": f"{p.fail_rate_pause_pct}%",
                "Abort >": f"{p.fail_rate_abort_pct}%",
                "Drift": p.schema_drift_action,
                "Created by": p.created_by,
                "Created at": (
                    p.created_at.strftime("%Y-%m-%d %H:%M")
                    if hasattr(p.created_at, "strftime")
                    else str(p.created_at)
                ),
            }
            for p in scoped
        ]
    )

    df_event = st.dataframe(
        df_active,
        use_container_width=True,
        hide_index=True,
        selection_mode="single-row",
        on_select="rerun",
        key="active_policies_df",
    )

    selected_rows = df_event.selection.rows if hasattr(df_event, "selection") else []
    if not selected_rows:
        st.caption("Select a row above to enable ✏️ **Edit** and 🗑️ **Archive**.")
    else:
        sel_policy = scoped[selected_rows[0]]
        st.caption(
            f"Selected: **{sel_policy.client_id}** · `{sel_policy.pipeline_id}` · "
            f"{_src_label(sel_policy.source_type)} · v{sel_policy.version}"
        )
        confirm_key = f"row_arch_confirm_{sel_policy.policy_id}"
        a_edit, a_arch, a_spacer = st.columns([1.2, 1.4, 4])

        # ---- Edit ----------------------------------------------------------
        if a_edit.button(
            "✏️ Edit selected",
            key=f"sel_edit_{sel_policy.policy_id}",
            use_container_width=True,
            help="Jump to editor below — scope/pipeline/source pre-selected.",
        ):
            st.session_state["pc_scope_choice"] = (
                "global" if sel_policy.client_id == GLOBAL_CLIENT else "client"
            )
            st.session_state["pc_pipeline_id"] = sel_policy.pipeline_id
            st.session_state["pc_source_type"] = sel_policy.source_type
            st.toast(
                f"Editor loaded with {sel_policy.pipeline_id} / "
                f"{_src_label(sel_policy.source_type)}. Scroll down ↓",
                icon="✏️",
            )
            st.rerun()

        # ---- Archive (two-click confirm) -----------------------------------
        if not st.session_state.get(confirm_key, False):
            if a_arch.button(
                "🗑️ Archive selected",
                key=f"sel_arch_{sel_policy.policy_id}",
                use_container_width=True,
                help="Soft-delete (status → ARCHIVED). Two-click confirm.",
            ):
                st.session_state[confirm_key] = True
                st.rerun()
        else:
            ay, an = a_arch.columns(2)
            if ay.button(
                "✅ Confirm",
                key=f"sel_arch_yes_{sel_policy.policy_id}",
                type="primary",
                use_container_width=True,
            ):
                try:
                    with _warehouse(readonly=False) as wh:
                        PipelineControlPolicyRegistry(wh).archive(
                            sel_policy.policy_id,
                            actor=st.session_state.get("user", "anonymous@local"),
                            reason="archived from active-policies list",
                        )
                    st.session_state[confirm_key] = False
                    st.toast(f"Archived {sel_policy.policy_id[:8]}…", icon="🗑️")
                    st.rerun()
                except Exception as e:
                    st.session_state[confirm_key] = False
                    st.error(f"Archive failed: {type(e).__name__}: {e}")
            if an.button(
                "✖️ Cancel",
                key=f"sel_arch_no_{sel_policy.policy_id}",
                use_container_width=True,
            ):
                st.session_state[confirm_key] = False
                st.rerun()


# ============================================================================
# 2. RESOLVED POLICIES GRID — what wins for every (pipeline × source) tuple
# ============================================================================

st.markdown("## Resolved policies — what's actually gating runtime")
st.caption(
    "For every (pipeline × source_type) tuple, the runtime calls "
    "`get_or_default(client, pipeline, source_type)` and the most specific "
    f"LIVE policy wins. Below shows which tier resolves first for **{client_id}**."
)

st.dataframe(pd.DataFrame(grid_rows), use_container_width=True, hide_index=True)


# ============================================================================
# 3. SCOPE PICKER — current client vs Global (*) tier
# ============================================================================

st.markdown("## Author / edit policy")

scope_l, scope_r = st.columns([1, 3])
with scope_l:
    scope_choice = st.radio(
        "Scope",
        options=["client", "global"],
        horizontal=True,
        format_func=lambda s: f"📍 {client_id}" if s == "client" else "🌐 Global (*)",
        help=(
            f"client = author a policy that applies only to **{client_id}**.\n"
            f"global = author a policy that every client inherits unless they have their own override."
        ),
        key="pc_scope_choice",
    )
target_client = client_id if scope_choice == "client" else GLOBAL_CLIENT
with scope_r:
    if scope_choice == "global":
        st.warning(
            "🌐 **Global scope.** Edits here apply to ALL clients that don't have their own "
            "per-client override. Use this for organization-wide defaults; individual tenants "
            "override by creating per-client policies."
        )
    else:
        st.info(
            f"📍 **Client scope: {client_id}.** Edits here override the global tier (and DEFAULT) "
            f"for this tenant only. Other tenants are unaffected."
        )


# ============================================================================
# 4. PIPELINE + SOURCE_TYPE PICKER + EDITOR
# ============================================================================

ed_l, ed_r = st.columns([1, 1])
with ed_l:
    pipeline_id = st.selectbox(
        "Pipeline",
        options=PIPELINE_IDS,
        help="Which medallion stage's threshold policy you're editing.",
        key="pc_pipeline_id",
    )
with ed_r:
    source_type_pick = st.selectbox(
        "Source type",
        options=SOURCE_TYPE_OPTIONS,
        format_func=_src_label,
        help=(
            "(any) — pipeline-wide policy that gates every source.\n"
            "CLAIMS / MEMBERSHIP / PROVIDER — per-source override (more specific = wins)."
        ),
        key="pc_source_type",
    )

with _warehouse(readonly=True) as wh:
    policy_reg = PipelineControlPolicyRegistry(wh)
    active = policy_reg.get_active_policy(target_client, pipeline_id, source_type=source_type_pick)
    versions = policy_reg.list_versions(target_client, pipeline_id, source_type=source_type_pick)
    latest_draft = next((v for v in versions if v.status is PolicyStatus.DRAFT), None)

# ---- Active policy (or "no policy registered") ----------------------------

st.markdown(
    f"### Active policy — `{target_client} / {pipeline_id} / {_src_label(source_type_pick)}`"
)

if active is None:
    cascade_winner = policy_reg.get_or_default(
        target_client, pipeline_id, source_type=source_type_pick
    )
    cascade_label, _ = _resolve_tier(cascade_winner, target_client)
    st.info(
        f"⚠️ No registered policy for this exact tuple. "
        f"Runtime falls back to: **{cascade_label}** "
        f"(pause > {cascade_winner.fail_rate_pause_pct}%, abort > {cascade_winner.fail_rate_abort_pct}%, "
        f"drift = {cascade_winner.schema_drift_action}). "
        f"Author below to override."
    )

    with st.expander("➕ Author first policy (pre-filled from cascade)", expanded=True):
        with st.form(
            key=f"create_{pipeline_id}_{source_type_pick or 'any'}", clear_on_submit=False
        ):
            c_a, c_b = st.columns(2)
            with c_a:
                pause_pct = st.number_input(
                    "Pause if fail % >",
                    min_value=0.0,
                    max_value=100.0,
                    value=float(cascade_winner.fail_rate_pause_pct),
                    step=1.0,
                    help="Pipeline pauses (Resume reversible) when fail rate exceeds this. 0.0 = pause on any failure.",
                )
                abort_pct = st.number_input(
                    "Abort if fail % >",
                    min_value=0.0,
                    max_value=100.0,
                    value=float(cascade_winner.fail_rate_abort_pct),
                    step=1.0,
                    help="Pipeline aborts (Force Resume only) when exceeded. Must be ≥ pause threshold.",
                )
            with c_b:
                drift_action = st.selectbox(
                    "Schema drift action (FATAL drift)",
                    options=[a.value for a in DriftAction],
                    index=[a.value for a in DriftAction].index(cascade_winner.schema_drift_action),
                )
                row_drop_pct = st.number_input(
                    "Row count drop % (reserved)",
                    min_value=0.0,
                    max_value=100.0,
                    value=float(cascade_winner.row_count_drop_pct),
                    step=1.0,
                    help="Phase 12.5 reserved — runtime check not yet wired.",
                )
            notes = st.text_input(
                "Notes (audit trail)",
                value=f"initial policy for {target_client}/{pipeline_id}/{source_type_pick or 'any'}",
            )
            mode_pick = st.radio(
                "Submit as:",
                options=[ApprovalMode.DRAFT, ApprovalMode.HITL, ApprovalMode.AUTO_APPROVE],
                index=1,
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
                    client_id=target_client,
                    pipeline_id=pipeline_id,
                    source_type=source_type_pick,
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
                            "Pipeline Control auto-approve"
                            if mode_pick is ApprovalMode.AUTO_APPROVE
                            else None
                        ),
                    )
                final_state = {
                    ApprovalMode.DRAFT: "DRAFT",
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

    # ---- Edit / fork action ------------------------------------------------

    st.markdown("### Edit thresholds")
    st.caption(
        f"🔒 **Editing v{active.version} (LIVE)** — by Phase-10 policy this fork "
        f"is locked to **HITL review**. Senior stewards cannot bypass review on production edits."
    )

    if latest_draft is None:
        if st.button(f"🔀 Fork v{active.version} to a new DRAFT for editing", type="primary"):
            try:
                with _warehouse(readonly=False) as wh:
                    reg = PipelineControlPolicyRegistry(wh)
                    new_id = reg.fork_for_edit(
                        active.policy_id,
                        actor=st.session_state.get("user", "anonymous@local"),
                    )
                st.success(
                    f"Forked v{active.version} → DRAFT v{active.version + 1} (`{new_id[:8]}…`). "
                    f"Reload to edit."
                )
                st.rerun()
            except Exception as e:
                st.error(f"Fork failed: {type(e).__name__}: {e}")
    else:
        with st.form(key=f"edit_{latest_draft.policy_id}", clear_on_submit=False):
            st.markdown(
                f"**Editing v{latest_draft.version} DRAFT** `{latest_draft.policy_id[:8]}…`"
            )
            c_a, c_b = st.columns(2)
            with c_a:
                e_pause = st.number_input(
                    "Pause if fail % >",
                    min_value=0.0,
                    max_value=100.0,
                    value=float(latest_draft.fail_rate_pause_pct),
                    step=1.0,
                )
                e_abort = st.number_input(
                    "Abort if fail % >",
                    min_value=0.0,
                    max_value=100.0,
                    value=float(latest_draft.fail_rate_abort_pct),
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
                    value=float(latest_draft.row_count_drop_pct),
                    step=1.0,
                )
            e_notes = st.text_input("Notes", value=latest_draft.notes or "")
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
            edit_submitted = st.form_submit_button("💾 Save / submit", type="primary")

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
                            latest_draft.policy_id, mode=ApprovalMode.HITL, actor=actor
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

    # ---- Cross-client clone (Phase 12.5) -----------------------------------

    st.markdown("### 📋 Clone this policy to another client")
    st.caption(
        "Duplicate this policy's threshold values into another client's tenant. "
        "Lands as a DRAFT in the target client — still goes through HITL review "
        "before activation. Use to standardise across tenants from one source of truth."
    )
    with _warehouse(readonly=True) as wh:
        clone_reg = PipelineControlPolicyRegistry(wh)
        candidates = [c for c in _client_options(clone_reg) if c != active.client_id]
    if not candidates:
        st.info("No other client tenants available to clone to.")
    else:
        clone_l, clone_r = st.columns([2, 1])
        with clone_l:
            clone_target = st.selectbox(
                "Target client",
                options=candidates,
                key=f"clone_tgt_{active.policy_id}",
            )
        with clone_r:
            st.write("")
            st.write("")
            if st.button("📋 Clone to selected client", key=f"clone_btn_{active.policy_id}"):
                try:
                    with _warehouse(readonly=False) as wh:
                        new_id = PipelineControlPolicyRegistry(wh).clone_to_client(
                            active.policy_id,
                            clone_target,
                            actor=st.session_state.get("user", "anonymous@local"),
                        )
                    st.success(
                        f"✅ Cloned to **{clone_target}** as DRAFT (`{new_id[:8]}…`). "
                        f"Switch the sidebar Client to **{clone_target}** to review and submit."
                    )
                except ValueError as e:
                    st.error(f"Clone refused: {e}")
                except Exception as e:
                    st.error(f"Clone failed: {type(e).__name__}: {e}")

    # ---- Archive policy (soft-delete) --------------------------------------

    st.markdown("### 🗑️ Archive this policy")
    st.caption(
        "Soft-delete: status flips to `ARCHIVED` (audit row preserved). "
        "Runtime cascade falls through to the next-tier policy or DEFAULT. "
        "Two-click confirm — accidental archives can still be reversed in SQL."
    )
    confirm_key = f"archive_confirm_{active.policy_id}"
    reason_key = f"archive_reason_{active.policy_id}"
    arch_l, arch_r = st.columns([3, 1])
    with arch_l:
        archive_reason = st.text_input(
            "Reason (audit trail)",
            value="superseded by newer policy",
            key=reason_key,
        )
    with arch_r:
        st.write("")
        st.write("")
        if not st.session_state.get(confirm_key, False):
            if st.button("🗑️ Archive…", key=f"archive_btn_{active.policy_id}"):
                st.session_state[confirm_key] = True
                st.rerun()
        else:
            cf_l, cf_r = st.columns(2)
            with cf_l:
                if st.button(
                    "✅ Confirm",
                    key=f"archive_yes_{active.policy_id}",
                    type="primary",
                ):
                    try:
                        with _warehouse(readonly=False) as wh:
                            PipelineControlPolicyRegistry(wh).archive(
                                active.policy_id,
                                actor=st.session_state.get("user", "anonymous@local"),
                                reason=archive_reason,
                            )
                        st.session_state[confirm_key] = False
                        st.success(
                            f"✅ Archived `{active.policy_id[:8]}…`. Cascade now "
                            f"resolves to the next-tier policy."
                        )
                        st.rerun()
                    except Exception as e:
                        st.session_state[confirm_key] = False
                        st.error(f"Archive failed: {type(e).__name__}: {e}")
            with cf_r:
                if st.button("✖️ Cancel", key=f"archive_no_{active.policy_id}"):
                    st.session_state[confirm_key] = False
                    st.rerun()


# ============================================================================
# 5. PENDING REVIEWS — current client + global tier
# ============================================================================

st.markdown("## Pending policy reviews")
st.caption(
    f"Every policy currently awaiting reviewer approval that affects **{client_id}** "
    f"(includes the global * tier). Reviewers approve / reject from this list."
)

with _warehouse(readonly=True) as wh:
    reg = PipelineControlPolicyRegistry(wh)
    pending = [
        p
        for p in reg.list_pending_reviews()
        if p.client_id == client_id or p.client_id == GLOBAL_CLIENT
    ]

if not pending:
    st.info("No policy reviews pending for this client (or global tier).")
else:
    for p in pending:
        with st.container(border=True):
            r_l, r_r = st.columns([3, 1])
            with r_l:
                tier = "🌐 GLOBAL" if p.is_global else f"📍 {p.client_id}"
                st.markdown(
                    f"{tier} · **{p.pipeline_id} / {p.source_type or '(any)'}** v{p.version} · "
                    f"`{p.policy_id[:8]}…` · "
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
                if br.button("❌ Reject", key=f"rej_{p.policy_id}", use_container_width=True):
                    try:
                        actor = st.session_state.get("user", "anonymous@local")
                        with _warehouse(readonly=False) as wh:
                            reg = PipelineControlPolicyRegistry(wh)
                            reg.reject(p.policy_id, actor=actor, notes=approve_notes)
                        st.warning(f"v{p.version} → REJECTED.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Reject failed: {e}")
