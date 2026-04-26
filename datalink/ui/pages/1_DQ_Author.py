"""DQ Author — edit expectation suites in a browser, no Python required.

One of two Phase-5.8 pages (the other is /dq-review). Here a DQ analyst:
  1. Picks a (client, suite_name) combination.
  2. Sees the LIVE version's expectations in an editable table.
  3. Edits rows (add / remove / modify / tag with dq_dimension + severity).
  4. Saves a DRAFT → visible in version history + editable again later.
  5. Submits for review → moves to PENDING_REVIEW → a reviewer picks it up
     on /dq-review, approves, and activates it (the new LIVE).

No file edits required. Version history shows every v1..vN with status,
author, timestamp. Works for multi-tenant — one client's edits never
bleed into another's LIVE.
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import pandas as pd
import streamlit as st

from datalink.quality.registry import (
    ApprovalMode,
    DqDimension,
    SuiteRegistry,
    SuiteStatus,
)
from datalink.ui._query import warehouse_ctx

# ============================================================================
# CONFIG + THEME (reused from control_tower.py)
# ============================================================================

WAREHOUSE_PATH = os.environ.get("DL_CT_WAREHOUSE_PATH", "/opt/datalink/warehouse.duckdb")

# Phase 6 fix: bootstrap warehouse file + CONTROL schema before any
# read-only connection attempt (fresh-boot fix).
from datalink.ui._bootstrap import ensure_warehouse_exists  # noqa: E402

ensure_warehouse_exists(WAREHOUSE_PATH)

st.set_page_config(
    page_title="DataLink — DQ Author",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Shared sidebar nav (defined in datalink/ui/_nav.py).
from datalink.ui._nav import render_sidebar  # noqa: E402

render_sidebar(active="DQ Author")

_NAVY = "#0a1a3e"
_GOLD = "#d4af37"

st.markdown(
    f"""
    <style>
      h1 {{color: {_NAVY}; border-bottom: 3px solid {_GOLD}; padding-bottom: .4rem;}}
      h2 {{color: {_NAVY}; margin-top: 1.5rem;}}
      .status-DRAFT {{background:#e0e7ff;color:#1e3a8a;padding:.15rem .5rem;border-radius:3px;font-weight:600}}
      .status-PENDING_REVIEW {{background:#fef3c7;color:#92400e;padding:.15rem .5rem;border-radius:3px;font-weight:600}}
      .status-APPROVED {{background:#d1fae5;color:#047857;padding:.15rem .5rem;border-radius:3px;font-weight:600}}
      .status-LIVE {{background:#{_GOLD.lstrip("#")};color:#{_NAVY.lstrip("#")};padding:.15rem .5rem;border-radius:3px;font-weight:700}}
      .status-ARCHIVED {{background:#e5e7eb;color:#4b5563;padding:.15rem .5rem;border-radius:3px;font-weight:600}}
      .status-REJECTED {{background:#fee2e2;color:#991b1b;padding:.15rem .5rem;border-radius:3px;font-weight:600}}
    </style>
    """,
    unsafe_allow_html=True,
)


# ============================================================================
# REGISTRY — opens a short-lived DB connection per request to avoid holding
# the write lock while dbt / ingest tasks are in flight.
# ============================================================================


@contextmanager
def _registry(*, readonly: bool = True) -> Iterator[SuiteRegistry]:
    """Yield a fresh SuiteRegistry bound to a short-lived warehouse adapter.

    Delegates connection lifecycle to ``datalink.ui._query.warehouse_ctx``
    so the backend (DuckDB / Snowflake / …) is config-driven. The
    short-lived pattern is still mandatory for DuckDB: Streamlit re-runs
    the script on every interaction, and DuckDB disallows concurrent
    connections of mixed read-only / read-write mode to the same file —
    so we open, use, close, per request.
    """
    with warehouse_ctx(readonly=readonly) as wh:
        yield SuiteRegistry(wh)  # type: ignore[arg-type]


def _df_to_expectations(df: pd.DataFrame) -> list[dict[str, Any]]:
    """Convert the edited DataFrame back to the JSON shape the registry stores."""
    out: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        exp_type = str(row.get("expectation_type", "")).strip()
        if not exp_type:
            continue
        kwargs_str = str(row.get("kwargs_json", "") or "{}").strip()
        try:
            kwargs = json.loads(kwargs_str) if kwargs_str else {}
        except json.JSONDecodeError as e:
            raise ValueError(
                f"Row with expectation_type={exp_type!r} has invalid kwargs_json: {e}"
            ) from e
        col = str(row.get("column", "") or "").strip()
        if col and "column" not in kwargs and "column_A" not in kwargs:
            kwargs["column"] = col
        out.append(
            {
                "expectation_type": exp_type,
                "kwargs": kwargs,
                "meta": {
                    "dq_dimension": str(row.get("dq_dimension", "") or "").strip(),
                    "severity": str(row.get("severity", "MEDIUM") or "MEDIUM").strip(),
                    "description": str(row.get("description", "") or "").strip(),
                },
            }
        )
    return out


# ============================================================================
# PAGE
# ============================================================================


st.markdown(
    f"<h1>🛡️ <span style='color:{_NAVY}'>DataLink</span> "
    f"<span style='color:{_GOLD}'>DQ Author</span></h1>",
    unsafe_allow_html=True,
)

st.markdown(
    '<p style="color:#4a5a7e">Edit expectation suites in the browser. Every save creates a new version; '
    "submit for review and a DQ reviewer approves before the suite goes LIVE and starts gating pipeline runs.</p>",
    unsafe_allow_html=True,
)


# ----------------------------------------------------------------------------
# CLIENT / SUITE PICKER
# ----------------------------------------------------------------------------

try:
    with _registry(readonly=True) as reg:
        clients = reg.list_clients()
except Exception as e:
    st.error(f"Can't reach the warehouse: {e}. Is `docker compose ps` showing everything healthy?")
    st.stop()

if not clients:
    st.warning(
        "No DQ suites exist yet. The baseline seeder runs on the first "
        "pipeline invocation — click ▶ Run Bronze on the main Control Tower "
        "page once, then refresh this page."
    )
    st.stop()

col_client, col_suite, col_new = st.columns([1, 1, 1])

with col_client:
    new_client_mode = st.toggle(
        "➕ New client",
        value=False,
        help="Add a fresh client_id — starts with no LIVE suite; "
        "runtime falls back to 'default' until you author + approve.",
    )
    if new_client_mode:
        client_id = st.text_input("Client ID", value="client_", max_chars=40).strip()
        if not client_id or client_id == "client_":
            st.stop()
    else:
        client_id = st.selectbox("Client", options=clients, index=0)

# Fetch suite-names + versions in a single short-lived read connection.
with _registry(readonly=True) as reg:
    suite_names = reg.list_suite_names(client_id) or reg.list_suite_names("default")

with col_suite:
    if not suite_names:
        st.error("No suites exist for this client. Contact your admin.")
        st.stop()
    suite_name = st.selectbox("Suite", options=suite_names, index=0)

with col_new:
    st.write("")  # spacer
    st.write("")
    clone_from_live = st.button(
        "📋 Clone LIVE → new DRAFT",
        use_container_width=True,
        help="Start a new DRAFT by copying the current LIVE's expectations. "
        "Edit, then submit for review.",
    )


# ----------------------------------------------------------------------------
# VERSION HISTORY
# ----------------------------------------------------------------------------

with _registry(readonly=True) as reg:
    versions = reg.list_versions(client_id, suite_name)
    if not versions:
        st.info(
            f"`{client_id}` has no suites for `{suite_name}` yet. "
            f"Falling back to `default`'s LIVE until you author one here."
        )
        versions = reg.list_versions("default", suite_name)

live = next((v for v in versions if v.status is SuiteStatus.LIVE), None)
latest_draft = next(
    (v for v in versions if v.status is SuiteStatus.DRAFT and v.client_id == client_id), None
)

# Phase 10.2 — clone uses fork_for_edit() so audit log records the
# source suite_id + version, and the new draft inherits source/source_type/
# fingerprint from the original (preserves provenance for baselines that
# get edited via the UI; old code reset source to SuiteSource.UI which
# erased the fact that this was originally a baseline-seeded suite).
if clone_from_live and live:
    with st.spinner("Cloning..."):
        try:
            with _registry(readonly=False) as reg:
                new_id = reg.fork_for_edit(
                    live.suite_id,
                    actor=st.session_state.get("user", "anonymous@local"),
                )
            st.success(
                f"Forked v{live.version} → new DRAFT v{live.version + 1} "
                f"(id={new_id[:8]}). Scroll down to edit."
            )
            st.session_state["active_draft_id"] = new_id
            st.rerun()
        except Exception as e:
            st.error(f"Fork failed: {e}")


# ----------------------------------------------------------------------------
# SHOW LIVE + LET USER EDIT ACTIVE DRAFT
# ----------------------------------------------------------------------------

st.markdown("## Live version")
if live is None:
    st.warning(
        "No LIVE suite for this (client, suite). Pipeline runs will "
        "fail-safe to the default client's baseline if one exists."
    )
else:
    left, right = st.columns([3, 1])
    with left:
        st.markdown(
            f"**v{live.version}** · "
            f"<span class='status-LIVE'>LIVE</span> · "
            f"by `{live.created_by}` · {len(live.expectations)} expectations · "
            f"activated {live.activated_at.strftime('%Y-%m-%d %H:%M') if live.activated_at else '—'}",
            unsafe_allow_html=True,
        )
    with right:
        st.metric("Dimensions", len(live.dq_dimensions))

    live_df = pd.DataFrame(
        [
            {
                "expectation_type": e.get("expectation_type", ""),
                "column": e.get("kwargs", {}).get("column", "—"),
                "dq_dimension": e.get("meta", {}).get("dq_dimension", ""),
                "severity": e.get("meta", {}).get("severity", ""),
                "description": e.get("meta", {}).get("description", ""),
            }
            for e in live.expectations
        ]
    )
    st.dataframe(live_df, use_container_width=True, hide_index=True)

    # Direct archive of a LIVE suite — for retiring rules without
    # replacing them via clone-edit-activate. Two-click confirm so
    # nobody nukes a critical baseline by accident.
    arc1, arc2 = st.columns([1, 4])
    with arc1:
        confirm_arc = st.toggle(
            "🗄️ Archive LIVE",
            value=False,
            key=f"arc_toggle_{live.suite_id}",
            help="Two-click confirm: ON enables the Archive button. "
            "Archiving a LIVE suite stops it from gating future "
            "checkpoint runs. Audit row preserved.",
        )
    with arc2:
        if confirm_arc and st.button(
            f"Archive v{live.version} now",
            key=f"arc_btn_{live.suite_id}",
            type="secondary",
        ):
            try:
                with _registry(readonly=False) as reg:
                    reg.archive(
                        live.suite_id,
                        actor=st.session_state.get("user", "anonymous@local"),
                        reason="manually archived from DQ Author (LIVE → ARCHIVED)",
                    )
                st.success(
                    f"Archived `{live.suite_name}` v{live.version}. "
                    f"This suite no longer gates pipeline runs."
                )
                st.rerun()
            except Exception as e:
                st.error(f"Archive failed: {e}")


st.markdown("## Edit DRAFT")

# Pick or create the active draft
active_id = st.session_state.get("active_draft_id")
if active_id:
    with _registry(readonly=True) as reg:
        active = reg.get_by_id(active_id)
else:
    active = latest_draft

if active is None:
    st.info(
        "No DRAFT yet for this (client, suite). Click "
        "'📋 Clone LIVE → new DRAFT' above to start editing."
    )
else:
    if active.status is not SuiteStatus.DRAFT:
        st.warning(
            f"Selected suite (v{active.version}) is in status "
            f"`{active.status.value}` — edits are only allowed on DRAFT."
        )
    else:
        st.markdown(
            f"Editing **v{active.version} DRAFT** · by `{active.created_by}` · "
            f"{len(active.expectations)} expectations · draft id `{active.suite_id[:8]}`"
        )

        # Build an editable DataFrame
        edit_df = pd.DataFrame(
            [
                {
                    "expectation_type": e.get("expectation_type", ""),
                    "column": e.get("kwargs", {}).get("column", ""),
                    "kwargs_json": json.dumps(e.get("kwargs", {}), ensure_ascii=False),
                    "dq_dimension": e.get("meta", {}).get("dq_dimension", ""),
                    "severity": e.get("meta", {}).get("severity", "MEDIUM"),
                    "description": e.get("meta", {}).get("description", ""),
                }
                for e in active.expectations
            ]
        )

        supported_types = [
            "expect_column_values_to_not_be_null",
            "expect_column_values_to_be_unique",
            "expect_column_values_to_match_regex",
            "expect_column_values_to_be_in_set",
            "expect_column_values_to_be_between",
            "expect_column_max_to_be_between",
            "expect_column_pair_values_a_to_be_greater_than_b",
            "expect_column_pair_values_to_be_equal",
            "expect_table_row_count_to_be_between",
            "expect_table_columns_to_match_ordered_list",
        ]

        edited = st.data_editor(
            edit_df,
            use_container_width=True,
            num_rows="dynamic",
            column_config={
                "expectation_type": st.column_config.SelectboxColumn(
                    "Expectation type",
                    options=supported_types,
                    required=True,
                    width="medium",
                ),
                "column": st.column_config.TextColumn(
                    "Column",
                    help="The column this expectation runs on (blank for table-level).",
                    width="small",
                ),
                "kwargs_json": st.column_config.TextColumn(
                    "Kwargs (JSON)",
                    help='Extra params, e.g. {"regex": "^\\\\d{10}$", "mostly": 0.98}.',
                    width="large",
                ),
                "dq_dimension": st.column_config.SelectboxColumn(
                    "DQ Dimension",
                    options=[d.value for d in DqDimension],
                    required=False,
                    width="small",
                ),
                "severity": st.column_config.SelectboxColumn(
                    "Severity",
                    options=["HIGH", "MEDIUM", "LOW"],
                    required=False,
                    width="small",
                ),
                "description": st.column_config.TextColumn(
                    "Description",
                    width="large",
                ),
            },
            key=f"editor_{active.suite_id}",
        )

        actor = st.text_input(
            "Your identity (for the audit trail)",
            value=st.session_state.get("user", "anonymous@local"),
            key=f"actor_{active.suite_id}",
        )

        # Phase 10.4 — approval-mode picker. Per Option C:
        # - When editing a suite that already has a LIVE version, the
        #   policy LOCKS the picker to HITL. Edits to production must
        #   go through review even if the user is a senior steward.
        # - For brand-new suites (no LIVE yet), the user picks freely:
        #   DRAFT (park), HITL (default — review queue), AUTO_APPROVE
        #   (skip review, go straight to LIVE — power-user opt-in).
        if live is not None:  # narrows live to SuiteVersion (mypy)
            st.caption(
                f"🔒 Editing v{live.version} (currently LIVE) — "
                f"by Phase-10 policy this draft must go through HITL review "
                f"regardless of mode picker. Changes to production-gating "
                f"suites cannot bypass two-eye approval."
            )
            mode_options = [ApprovalMode.DRAFT, ApprovalMode.HITL]
            mode_default_idx = 1  # HITL
        else:
            mode_options = [
                ApprovalMode.DRAFT,
                ApprovalMode.HITL,
                ApprovalMode.AUTO_APPROVE,
            ]
            mode_default_idx = 1  # HITL — Option C default for UI authoring

        approval_mode = st.radio(
            "On Submit, route as:",
            options=mode_options,
            index=mode_default_idx,
            horizontal=True,
            format_func=lambda m: {
                ApprovalMode.DRAFT: "📝 Save DRAFT (park)",
                ApprovalMode.HITL: "👁️ HITL (route to DQ Review)",
                ApprovalMode.AUTO_APPROVE: "⚡ AUTO_APPROVE (skip review, go LIVE)",
            }[m],
            key=f"mode_{active.suite_id}",
        )

        btn_save, btn_submit, btn_archive, _ = st.columns([1, 1, 1, 2])
        with btn_save:
            if st.button(
                "💾 Save Draft",
                use_container_width=True,
                type="secondary",
                help="Persists the editor state without changing status. "
                "Use this between edit sessions; click Submit when ready.",
            ):
                try:
                    new_exps = _df_to_expectations(edited)
                    dims = sorted(
                        {
                            e["meta"]["dq_dimension"]
                            for e in new_exps
                            if e.get("meta", {}).get("dq_dimension")
                        }
                    )
                    with _registry(readonly=False) as reg:
                        reg.update_draft(active.suite_id, new_exps, list(dims), actor=actor)
                    st.success(f"Saved — {len(new_exps)} expectations.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Save failed: {e}")
        with btn_submit:
            submit_label = {
                ApprovalMode.DRAFT: "💾 Save (no submit)",
                ApprovalMode.HITL: "📤 Submit for Review",
                ApprovalMode.AUTO_APPROVE: "⚡ Approve & Go LIVE",
            }[approval_mode]
            if st.button(submit_label, use_container_width=True, type="primary"):
                try:
                    # Save first, THEN submit — so the reviewer sees the latest edits.
                    new_exps = _df_to_expectations(edited)
                    dims = sorted(
                        {
                            e["meta"]["dq_dimension"]
                            for e in new_exps
                            if e.get("meta", {}).get("dq_dimension")
                        }
                    )
                    with _registry(readonly=False) as reg:
                        reg.update_draft(active.suite_id, new_exps, list(dims), actor=actor)
                        reg.submit_with_policy(
                            active.suite_id,
                            mode=approval_mode,
                            actor=actor,
                            approval_notes=(
                                "DQ Author auto-approve"
                                if approval_mode is ApprovalMode.AUTO_APPROVE
                                else None
                            ),
                        )
                    final_status = {
                        ApprovalMode.DRAFT: "DRAFT (still editable)",
                        ApprovalMode.HITL: "PENDING_REVIEW",
                        ApprovalMode.AUTO_APPROVE: "LIVE",
                    }[approval_mode]
                    next_stop = {
                        ApprovalMode.DRAFT: "this page (continue editing)",
                        ApprovalMode.HITL: "[DQ Review](/DQ_Review)",
                        ApprovalMode.AUTO_APPROVE: "[DQ Suite Registry](/DQ_Suite_Registry)",
                    }[approval_mode]
                    st.success(
                        f"Done — suite is now **{final_status}**. "
                        f"Next stop: {next_stop}. "
                        f"Draft id: `{active.suite_id[:8]}`."
                    )
                    if approval_mode is not ApprovalMode.DRAFT:
                        st.session_state.pop("active_draft_id", None)
                    st.rerun()
                except Exception as e:
                    st.error(f"Submit failed: {e}")
        with btn_archive:
            # Soft-delete the DRAFT — it stays in the audit table but is
            # filtered out of every checkpoint query and the active-draft
            # session pointer.
            if st.button(
                "🗄️ Archive",
                use_container_width=True,
                help="Soft-delete this DRAFT — moves it to ARCHIVED. Audit row preserved.",
            ):
                try:
                    with _registry(readonly=False) as reg:
                        reg.archive(
                            active.suite_id,
                            actor=actor,
                            reason="archived from DQ Author",
                        )
                    st.success(f"Archived draft `{active.suite_id[:8]}`.")
                    st.session_state.pop("active_draft_id", None)
                    st.rerun()
                except Exception as e:
                    st.error(f"Archive failed: {e}")


# ----------------------------------------------------------------------------
# VERSION HISTORY
# ----------------------------------------------------------------------------

st.markdown("## Version history")

if not versions:
    st.info("No versions yet.")
else:
    history_df = pd.DataFrame(
        [
            {
                "version": v.version,
                "status": v.status.value,
                "source": v.source.value,
                "expectations": len(v.expectations),
                "created_by": v.created_by,
                "created_at": v.created_at.strftime("%Y-%m-%d %H:%M") if v.created_at else "",
                "reviewed_by": v.reviewed_by or "—",
                "activated_at": v.activated_at.strftime("%Y-%m-%d %H:%M")
                if v.activated_at
                else "—",
            }
            for v in versions
        ]
    )
    st.dataframe(history_df, use_container_width=True, hide_index=True)
