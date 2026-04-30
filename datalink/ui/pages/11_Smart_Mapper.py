"""Smart Mapper — Phase 13 dedicated operator page.

The conversational AI mapper. Operator picks a source Bronze table,
describes the Gold target in plain English, the mapper proposes a
Silver dbt model SQL with a live sample preview. Operator can revise
("no, denial_reason should come from the latest non-null") and the
agent regenerates with full prior history. When happy, operator
clicks Submit / Approve through the Phase-10/12 HITL state machine;
on final Approve the deployer (13.8) writes the file to ``dbt/models/``.

Layout:

  * **Top** — source picker + target mode + Start/Continue session.
  * **Middle** — conversation pane with full chat history,
    sample-preview tabs, generated SQL pane.
  * **Bottom** — HITL controls (Save Draft / Submit / Approve / Reject /
    Deploy when APPROVED).

Sessions persist in ``CONTROL.mapping_sessions`` so reloads / cross-page
navigation survive. Multiple operators can collaborate on the same
session; every action audits to ``mapping_session_audit_log``.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import pandas as pd
import streamlit as st

from datalink.agents.llm_router import get_llm
from datalink.agents.mapper.optimization import TargetBackend, validate_push_sql
from datalink.agents.mapper.orchestrator import (
    propose_gold_view,
    propose_push_script,
    propose_silver_mapping,
)
from datalink.agents.mapper.session import (
    MappingSessionRegistry,
    SessionStatus,
    TargetMode,
)
from datalink.config.loader import load_settings
from datalink.quality.registry import ApprovalMode

WAREHOUSE_PATH = os.environ.get("DL_CT_WAREHOUSE_PATH", "/opt/datalink/warehouse.duckdb")

from datalink.ui._bootstrap import ensure_warehouse_exists  # noqa: E402

ensure_warehouse_exists(WAREHOUSE_PATH)

st.set_page_config(
    page_title="DataLink — Smart Mapper",
    page_icon="🧠",
    layout="wide",
    initial_sidebar_state="expanded",
)

from datalink.ui._nav import render_sidebar, require_client  # noqa: E402

render_sidebar(active="Smart Mapper")
client_id = require_client()

_NAVY = "#0a1a3e"
_GOLD = "#d4af37"

st.markdown(
    f"""
    <style>
      h1 {{color: {_NAVY}; border-bottom: 3px solid {_GOLD}; padding-bottom: .4rem;}}
      h2 {{color: {_NAVY}; margin-top: 1.5rem;}}
      .stat-DRAFT          {{background:#dbeafe;color:#1e40af;padding:.15rem .5rem;border-radius:3px;font-weight:600}}
      .stat-PENDING_REVIEW {{background:#fef3c7;color:#92400e;padding:.15rem .5rem;border-radius:3px;font-weight:600}}
      .stat-APPROVED       {{background:#d1fae5;color:#047857;padding:.15rem .5rem;border-radius:3px;font-weight:600}}
      .stat-DEPLOYED       {{background:#{_GOLD.lstrip("#")};color:#{_NAVY.lstrip("#")};padding:.15rem .5rem;border-radius:3px;font-weight:700}}
      .stat-REJECTED       {{background:#fee2e2;color:#991b1b;padding:.15rem .5rem;border-radius:3px;font-weight:600}}
      .stat-ARCHIVED       {{background:#e5e7eb;color:#4b5563;padding:.15rem .5rem;border-radius:3px;font-weight:600}}
      .turn-user {{background:#eff6ff;border-left:4px solid #3b82f6;padding:.6rem .8rem;border-radius:4px;margin:.4rem 0}}
      .turn-assistant {{background:#fef3c7;border-left:4px solid {_GOLD};padding:.6rem .8rem;border-radius:4px;margin:.4rem 0}}
    </style>
    """,
    unsafe_allow_html=True,
)


@contextmanager
def _warehouse(*, readonly: bool = True) -> Iterator[Any]:
    from datalink.ui._query import warehouse_ctx

    with warehouse_ctx(readonly=readonly) as wh:
        yield wh


def _html_escape(s: str) -> str:
    """Minimal HTML escape for chat-bubble rendering."""
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")


# ============================================================================
# HEADER
# ============================================================================

st.markdown("# 🧠 Smart Mapper")
st.caption(
    f"Conversational AI mapper for **{client_id}**. "
    f"Pick a Bronze source, describe the Gold target in plain English, "
    f"the mapper proposes a Silver dbt model with sample preview. "
    f"Revise via natural language; approve through HITL; deploy writes to `dbt/models/`."
)


# ============================================================================
# 1. SESSION SELECTION (start new OR continue existing)
# ============================================================================

with _warehouse(readonly=True) as wh:
    sess_reg = MappingSessionRegistry(wh)
    open_sessions = [
        s
        for s in sess_reg.list_sessions(client_id=client_id, limit=20)
        if s.status not in {SessionStatus.ARCHIVED, SessionStatus.REJECTED, SessionStatus.DEPLOYED}
    ]
    deployed_sessions = sess_reg.list_sessions(
        client_id=client_id, status=SessionStatus.DEPLOYED, limit=20
    )

st.markdown("## Session")

mode_col1, mode_col2 = st.columns([1, 2])
with mode_col1:
    is_new = st.toggle(
        "➕ New session",
        value=not bool(open_sessions),
        help=(
            "ON = start a new mapping conversation. "
            "OFF = pick from existing open sessions below."
        ),
    )

active_session_id: str | None = None

if is_new:
    new_col1, new_col2, new_col3 = st.columns([2, 1, 1])
    with new_col1:
        # Source table picker — list Bronze tables for this client.
        # Parameterize the LIKE pattern so the literal `%` doesn't collide
        # with Snowflake adapter's pyformat substitution (would fail with
        # "TypeError: not enough arguments for format string").
        bronze_schema = f"BRONZE_{client_id.upper()}" if client_id != "default" else "BRONZE"
        bronze_tables: list[str] = []
        list_error: str | None = None
        with _warehouse(readonly=True) as wh:
            try:
                rows = wh.query(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = $s AND table_name LIKE $p "
                    "ORDER BY table_name",
                    {"s": bronze_schema, "p": "RAW_%"},
                )
                # Snowflake returns UPPER keys; DuckDB returns lower.
                # Normalise so both backends work.
                bronze_tables = [
                    f"{bronze_schema}.{r.get('table_name') or r.get('TABLE_NAME')}" for r in rows
                ]
            except Exception as e:
                list_error = f"{type(e).__name__}: {e}"

        if list_error:
            st.error(
                f"Failed to list tables in `{bronze_schema}`: {list_error}. "
                f"Check Snowflake connection / role permissions."
            )
            source_table = ""
        elif not bronze_tables:
            st.warning(
                f"No `RAW_*` tables found in `{bronze_schema}`. "
                f"Run ▶ Run Bronze on Control Tower first to create source tables."
            )
            source_table = ""
        else:
            source_table = st.selectbox(
                "Source Bronze table",
                options=bronze_tables,
                help="The mapper will profile this table and use its columns + sample values as ground truth for the LLM.",
            )

    with new_col2:
        target_mode_choice = st.selectbox(
            "Target mode",
            options=[
                TargetMode.SILVER_ONLY,
                TargetMode.SILVER_AND_GOLD,
                TargetMode.FULL_STACK,
            ],
            format_func=lambda m: {
                TargetMode.SILVER_ONLY: "Silver only",
                TargetMode.SILVER_AND_GOLD: "Silver + Gold",
                TargetMode.FULL_STACK: "Full stack (Silver + Gold + On-Prem push)",
            }[m],
            help=(
                "Silver only — propose just the Silver dbt model.\n"
                "Silver + Gold — also propose the Gold view (13.6).\n"
                "Full stack — also generate optimised On-Prem push scripts (13.7)."
            ),
        )

    with new_col3:
        st.write("")
        st.write("")
        if st.button(
            "🚀 Start session", type="primary", use_container_width=True, disabled=not source_table
        ):
            try:
                with _warehouse(readonly=False) as wh:
                    new_id = MappingSessionRegistry(wh).create_session(
                        client_id=client_id,
                        source_qualified_table=source_table,
                        target_mode=target_mode_choice,
                        created_by=st.session_state.get("user", "anonymous@local"),
                    )
                st.session_state["mapper_active_session"] = new_id
                st.rerun()
            except Exception as e:
                st.error(f"Failed to start session: {type(e).__name__}: {e}")

else:
    if not open_sessions:
        st.info("No open sessions for this client. Toggle ➕ New session to start one.")
    else:
        opt_to_label = {
            s.session_id: (
                f"{s.source_qualified_table} · "
                f"{s.target_mode.value} · "
                f"{s.status.value} · "
                f"{s.created_at.strftime('%Y-%m-%d %H:%M')}"
            )
            for s in open_sessions
        }
        active_session_id = st.selectbox(
            "Continue session",
            options=list(opt_to_label),
            format_func=lambda s: opt_to_label[s],
            index=0,
        )
        st.session_state["mapper_active_session"] = active_session_id


# Resolve the active session id for the rest of the page.
active_session_id = st.session_state.get("mapper_active_session")
if not active_session_id:
    st.info(
        "Pick or start a session above. The mapper writes everything to "
        "`CONTROL.mapping_sessions` so reloads / cross-page nav survive."
    )
    st.stop()

# active_session_id is str | None at this point; the early-return above
# handles the None case via st.stop(), so for the rest of the page it's
# guaranteed non-None. Bind to a local str to make mypy happy.
assert active_session_id is not None
_session_id: str = active_session_id

with _warehouse(readonly=True) as wh:
    session = MappingSessionRegistry(wh).get_session(_session_id)

if session is None:
    st.error(f"Session `{_session_id[:8]}…` not found. Pick another above.")
    st.session_state.pop("mapper_active_session", None)
    st.stop()

# mypy can't infer that st.stop() halts execution — explicit assertion
# narrows ``session`` from ``MappingSession | None`` to ``MappingSession``
# for the rest of the file (a few hundred lines of attribute access).
assert session is not None


# ============================================================================
# 2. SESSION HEADER + STATUS
# ============================================================================

st.markdown("## Active session")

h1, h2, h3, h4 = st.columns([2, 1, 1, 1])
with h1:
    st.markdown(
        f"**`{session.source_qualified_table}`** · `{session.session_id[:8]}…` · "
        f"by `{session.created_by}`"
    )
    st.markdown(
        f"<span class='stat-{session.status.value}'>{session.status.value}</span> · "
        f"Target: **{session.target_mode.value}** · "
        f"Turns: **{session.turn_count}**",
        unsafe_allow_html=True,
    )
with h2:
    st.metric("Turns", session.turn_count)
with h3:
    st.metric("Has Silver", "Yes" if session.has_silver_proposal else "No")
with h4:
    if session.sample_preview is not None:
        st.metric("Sample rows", len(session.sample_preview))
    else:
        st.metric("Sample rows", "—")


# ============================================================================
# 3. CONVERSATION PANE
# ============================================================================

if session.conversation:
    st.markdown("### Conversation")
    for turn in session.conversation:
        cls = f"turn-{turn.role}"
        when = turn.ts.strftime("%H:%M:%S")
        meta = f" · {turn.tokens_used} tokens · {turn.duration_ms}ms" if turn.tokens_used else ""
        st.markdown(
            f"<div class='{cls}'>"
            f"<small><b>{turn.role.upper()}</b> · {when}{meta}</small><br>"
            f"<pre style='white-space:pre-wrap;font-family:inherit;margin:.4rem 0 0 0'>"
            f"{_html_escape(turn.text)}</pre>"
            f"</div>",
            unsafe_allow_html=True,
        )


# ============================================================================
# 4. PROPOSE / REVISE INPUT
# ============================================================================

if session.status is SessionStatus.DRAFT:
    # ----------------------------------------------------------------
    # 4a. STEP 1 — propose Silver (always required first)
    # ----------------------------------------------------------------
    st.markdown("### Step 1 — Propose Silver dbt model")
    silver_done = bool(session.silver_sql)
    silver_help = (
        "✅ Silver proposal exists — you can revise it below or move to Step 2 (Gold)."
        if silver_done
        else "Required first. The agent profiles the source, hits Claude, generates a Silver SCD2 dbt model + sample preview."
    )
    st.caption(silver_help)
    placeholder = (
        "Describe the Silver target in plain English. e.g.:\n"
        "  • A SCD2 Silver sat for member demographics keyed on member_id+plan_id\n"
        "  • Build a claims fact keyed on claim_id, with member_id FK, paid_amount, status"
        if session.turn_count == 0
        else "Revise. e.g. 'no, denial_reason should come from the latest non-null', 'drop subscriber_id', 'add soft-delete on FULL batches'"
    )
    user_prompt = st.text_area(
        "Your instruction",
        placeholder=placeholder,
        height=110,
        key=f"prompt_silver_{session.session_id}",
    )
    s_col1, s_col2, s_col3, _ = st.columns([1.2, 1, 1, 1.8])
    with s_col1:
        if st.button(
            "🤖 Propose / revise Silver",
            type="primary",
            use_container_width=True,
            disabled=not user_prompt.strip(),
        ):
            try:
                settings = load_settings()
                llm = get_llm(settings)
                with _warehouse(readonly=False) as wh:
                    reg = MappingSessionRegistry(wh)
                    proposal = propose_silver_mapping(
                        llm=llm,
                        warehouse=wh,
                        session_registry=reg,
                        session_id=session.session_id,
                        user_prompt=user_prompt.strip(),
                        actor=st.session_state.get("user", "anonymous@local"),
                    )
                st.success(
                    f"Proposed Silver `{proposal.silver_target_table}` "
                    f"(NK={proposal.natural_keys}, {len(proposal.scd2_change_cols)} change cols, "
                    f"{len(proposal.sample_rows)} sample rows). "
                    + (
                        f"⚠️ Sample run failed: {proposal.sample_error}"
                        if proposal.sample_error
                        else "Sample preview ran successfully."
                    )
                )
                st.rerun()
            except Exception as e:
                st.error(f"Silver mapper failed: {type(e).__name__}: {e}")
    with s_col2:
        if st.button(
            "🗑 Archive session", use_container_width=True, key=f"arc_{session.session_id}"
        ):
            try:
                with _warehouse(readonly=False) as wh:
                    MappingSessionRegistry(wh).archive(
                        session.session_id,
                        actor=st.session_state.get("user", "anonymous@local"),
                        reason="archived from Smart Mapper UI",
                    )
                st.session_state.pop("mapper_active_session", None)
                st.rerun()
            except Exception as e:
                st.error(f"Archive failed: {e}")

    # ----------------------------------------------------------------
    # 4b. STEP 2 — propose Gold (only when Silver exists + mode allows)
    # ----------------------------------------------------------------
    if session.target_mode is not TargetMode.SILVER_ONLY:
        st.markdown("### Step 2 — Propose Gold view")
        gold_done = bool(session.gold_sql)
        if not silver_done:
            st.caption(
                "🔒 Silver must be proposed first — Gold projects from the Silver SCD2 model."
            )
        elif gold_done:
            st.caption("✅ Gold proposal exists — revise below or move to Step 3 (On-Prem push).")
        else:
            st.caption(
                "Generates a Gold dbt view that filters Silver to active rows + projects "
                "operational columns. Revise via NL just like Silver."
            )
        gold_prompt = st.text_area(
            "Gold instruction",
            placeholder=(
                "e.g. 'Expose only active members with member_id, dob, gender, plan_id'. "
                "Or revise the existing Gold proposal."
            ),
            height=80,
            key=f"prompt_gold_{session.session_id}",
            disabled=not silver_done,
        )
        if st.button(
            "🧊 Propose / revise Gold",
            use_container_width=False,
            disabled=not (silver_done and gold_prompt.strip()),
            key=f"btn_gold_{session.session_id}",
        ):
            try:
                settings = load_settings()
                llm = get_llm(settings)
                with _warehouse(readonly=False) as wh:
                    reg = MappingSessionRegistry(wh)
                    g_proposal = propose_gold_view(
                        llm=llm,
                        warehouse=wh,
                        session_registry=reg,
                        session_id=session.session_id,
                        user_prompt=gold_prompt.strip(),
                        actor=st.session_state.get("user", "anonymous@local"),
                    )
                st.success(
                    f"Proposed Gold view `{g_proposal.gold_target_table}` "
                    + (
                        f"⚠️ Sample run failed: {g_proposal.sample_error}"
                        if g_proposal.sample_error
                        else "Sample preview ran successfully."
                    )
                )
                st.rerun()
            except Exception as e:
                st.error(f"Gold mapper failed: {type(e).__name__}: {e}")

    # ----------------------------------------------------------------
    # 4c. STEP 3 — propose On-Prem push (only when Gold exists + FULL_STACK)
    # ----------------------------------------------------------------
    if session.target_mode is TargetMode.FULL_STACK:
        st.markdown("### Step 3 — Propose On-Prem push script")
        push_done = bool(session.onprem_postgres_sql or session.onprem_mssql_sql)
        if not session.gold_sql:
            st.caption("🔒 Gold must be proposed first — push scripts read from the Gold view.")
        elif push_done:
            st.caption(
                "✅ At least one push script exists — re-run for the other backend, or move to Submit."
            )
        else:
            st.caption(
                "Generates an idempotent UPSERT script (Postgres `ON CONFLICT` / SQL Server "
                "`MERGE INTO`) keyed on the target's PK. Watermark-driven, batched, and "
                "scored against the Phase 13.4 optimisation rules."
            )

        # Per-backend form. Two side-by-side blocks.
        push_can_run = bool(session.gold_sql)
        push_l, push_r = st.columns(2)

        def _render_push_form(
            container: Any,
            backend_label: str,
            backend_value: str,
            existing_sql: str | None,
            *,
            default_target: str,
            key_prefix: str,
        ) -> None:
            # Re-narrow `session` inside this nested function. mypy does not
            # propagate the outer-scope `if session is None: st.stop()` guard
            # through closures, so we re-assert here for type safety.
            assert session is not None
            with container:
                st.markdown(f"**{backend_label}**")
                if existing_sql:
                    st.success(
                        f"Already proposed ({len(existing_sql)} chars). "
                        f"Submit a new prompt to overwrite."
                    )
                target_table = st.text_input(
                    "On-Prem target table (schema.table)",
                    value=default_target,
                    key=f"push_tgt_{key_prefix}_{session.session_id}",
                    help="The fully-qualified On-Prem table. e.g. `um.Member` for SQL Server.",
                )
                pks_csv = st.text_input(
                    "Primary key column(s) — comma-separated",
                    value="member_id",
                    key=f"push_pks_{key_prefix}_{session.session_id}",
                    help="Used as the UPSERT join key. Multi-column PKs allowed.",
                )
                cols_csv = st.text_input(
                    "Business columns to push — comma-separated",
                    value="member_id, dob, gender, plan_id",
                    key=f"push_cols_{key_prefix}_{session.session_id}",
                    help="Columns from the Gold view to ship. Include the PK columns.",
                )
                if st.button(
                    f"📤 Generate {backend_label} push",
                    use_container_width=True,
                    disabled=not (
                        push_can_run
                        and target_table.strip()
                        and pks_csv.strip()
                        and cols_csv.strip()
                    ),
                    key=f"btn_push_{key_prefix}_{session.session_id}",
                ):
                    try:
                        pks = [s.strip() for s in pks_csv.split(",") if s.strip()]
                        cols = [s.strip() for s in cols_csv.split(",") if s.strip()]
                        settings = load_settings()
                        llm = get_llm(settings)
                        with _warehouse(readonly=False) as wh:
                            reg = MappingSessionRegistry(wh)
                            p = propose_push_script(
                                llm=llm,
                                warehouse=wh,
                                session_registry=reg,
                                session_id=session.session_id,
                                backend=backend_value,
                                target_table=target_table.strip(),
                                primary_keys=pks,
                                column_list=cols,
                                actor=st.session_state.get("user", "anonymous@local"),
                            )
                        score_emoji = (
                            "🟢"
                            if p.optimization_score >= 0.9
                            else "🟡"
                            if p.optimization_score >= 0.7
                            else "🔴"
                        )
                        st.success(
                            f"{backend_label} push proposed — opt score {score_emoji} "
                            f"{p.optimization_score:.2f} "
                            + (
                                f"(failed: {', '.join(p.failed_rules)})"
                                if p.failed_rules
                                else "(all rules pass)"
                            )
                        )
                        st.rerun()
                    except Exception as e:
                        st.error(f"{backend_label} push failed: {type(e).__name__}: {e}")

        _render_push_form(
            push_l,
            "Postgres",
            "POSTGRES",
            session.onprem_postgres_sql,
            default_target="um.member",
            key_prefix="pg",
        )
        _render_push_form(
            push_r,
            "SQL Server",
            "SQLSERVER",
            session.onprem_mssql_sql,
            default_target="um.Member",
            key_prefix="ms",
        )
elif session.status is SessionStatus.PENDING_REVIEW:
    st.info(
        "🔒 Session is **PENDING_REVIEW** — conversation is frozen until "
        "a reviewer approves below or requests changes."
    )


# ============================================================================
# 5. ARTIFACT PANES
# ============================================================================

if session.has_silver_proposal:
    st.markdown("### Generated artifacts")

    tab_labels = ["Silver SQL", "Sample preview", "Source profile"]
    if session.gold_sql:
        tab_labels.append("Gold SQL")
    if session.onprem_postgres_sql or session.onprem_mssql_sql:
        tab_labels.append("On-Prem push")
    tabs = st.tabs(tab_labels)

    # Silver SQL
    with tabs[0]:
        st.code(session.silver_sql or "", language="sql")
        if session.silver_target_table:
            st.caption(
                f"Will deploy to: `dbt/models/silver/{session.silver_target_table}.sql` on Approve."
            )

    # Sample preview
    with tabs[1]:
        if session.sample_preview:
            df = pd.DataFrame(session.sample_preview)
            st.dataframe(df, use_container_width=True, hide_index=True)
            st.caption(
                f"{len(session.sample_preview)} rows from running the proposed SQL "
                f"with `LIMIT`. If this looks right, the proposal is structurally sound."
            )
        else:
            st.warning(
                "No sample preview. Either the proposed SQL failed to execute "
                "(check the assistant turn log above) or it returned 0 rows."
            )

    # Source profile
    with tabs[2]:
        if session.source_profile:
            cols_df = pd.DataFrame(
                [
                    {
                        "name": c["name"],
                        "logical_type": c["logical_type"],
                        "raw_type": c["raw_type"],
                        "null %": c["null_pct"],
                        "distinct": c["distinct_count"],
                        "semantic_hint": c.get("semantic_hint", ""),
                        "samples": ", ".join(repr(s) for s in (c.get("sample_values") or [])[:3]),
                    }
                    for c in session.source_profile.get("columns", [])
                ]
            )
            st.dataframe(cols_df, use_container_width=True, hide_index=True)
            st.caption(
                f"Total rows: **{session.source_profile.get('total_rows', '—')}** · "
                f"Profiled at: {session.source_profile.get('profiled_at', '—')}"
            )
        else:
            st.info("No profile snapshot — the first proposal call attaches it.")

    # Gold SQL (if present, 13.6)
    if session.gold_sql:
        with tabs[tab_labels.index("Gold SQL")]:
            st.code(session.gold_sql, language="sql")
            if session.gold_target_table:
                st.caption(
                    f"Will deploy to: `dbt/models/gold/{session.gold_target_table}.sql` on Approve."
                )

    # On-Prem push (if present, 13.7) with optimization scoring.
    if session.onprem_postgres_sql or session.onprem_mssql_sql:
        with tabs[tab_labels.index("On-Prem push")]:
            push_tabs = st.tabs(["Postgres", "SQL Server"])
            with push_tabs[0]:
                if session.onprem_postgres_sql:
                    report = validate_push_sql(
                        session.onprem_postgres_sql, backend=TargetBackend.POSTGRES
                    )
                    score_color = (
                        "🟢" if report.score >= 0.9 else "🟡" if report.score >= 0.7 else "🔴"
                    )
                    st.markdown(
                        f"**Optimization score:** {score_color} "
                        f"{report.passing}/{report.applicable} rules pass "
                        f"(score = {report.score:.2f})"
                    )
                    if report.failures:
                        st.warning("Rule failures (operator should ask the agent to revise):")
                        for r in report.failures:
                            st.markdown(f"- ❌ **{r.name}** — {r.detail}")
                    st.code(session.onprem_postgres_sql, language="sql")
                else:
                    st.info(
                        "No Postgres push script generated yet (target_mode=FULL_STACK required)."
                    )
            with push_tabs[1]:
                if session.onprem_mssql_sql:
                    report = validate_push_sql(
                        session.onprem_mssql_sql, backend=TargetBackend.SQLSERVER
                    )
                    score_color = (
                        "🟢" if report.score >= 0.9 else "🟡" if report.score >= 0.7 else "🔴"
                    )
                    st.markdown(
                        f"**Optimization score:** {score_color} "
                        f"{report.passing}/{report.applicable} rules pass "
                        f"(score = {report.score:.2f})"
                    )
                    if report.failures:
                        st.warning("Rule failures:")
                        for r in report.failures:
                            st.markdown(f"- ❌ **{r.name}** — {r.detail}")
                    st.code(session.onprem_mssql_sql, language="sql")
                else:
                    st.info("No SQL Server push script generated yet.")


# ============================================================================
# 6. HITL CONTROLS
# ============================================================================

if session.has_silver_proposal:
    st.markdown("### Submit / Approve")

    if session.status is SessionStatus.DRAFT:
        st.caption(
            "📝 In DRAFT — keep refining via the prompt box above, or submit when ready. "
            "Per Phase-10 policy edits to LIVE deployments will go through HITL automatically."
        )
        mode_pick = st.radio(
            "On submit, route as:",
            options=[ApprovalMode.DRAFT, ApprovalMode.HITL, ApprovalMode.AUTO_APPROVE],
            index=1,  # HITL default
            horizontal=True,
            format_func=lambda m: {
                ApprovalMode.DRAFT: "📝 Save DRAFT (continue editing)",
                ApprovalMode.HITL: "👁️ Submit for Review (HITL)",
                ApprovalMode.AUTO_APPROVE: "⚡ Auto-approve (skip review)",
            }[m],
            key=f"mode_{session.session_id}",
        )
        if st.button(
            "📤 Submit",
            type="primary",
            use_container_width=False,
        ):
            try:
                actor = st.session_state.get("user", "anonymous@local")
                with _warehouse(readonly=False) as wh:
                    reg = MappingSessionRegistry(wh)
                    reg.submit_with_policy(
                        session.session_id,
                        mode=mode_pick,
                        actor=actor,
                        approval_notes=(
                            "Smart Mapper auto-approve"
                            if mode_pick is ApprovalMode.AUTO_APPROVE
                            else None
                        ),
                    )
                st.success(f"Session is now **{mode_pick.value}**.")
                st.rerun()
            except Exception as e:
                st.error(f"Submit failed: {e}")

    elif session.status is SessionStatus.PENDING_REVIEW:
        rc1, rc2, rc3 = st.columns([1, 1, 1])
        review_notes = st.text_input(
            "Reviewer notes",
            value="LGTM",
            key=f"review_{session.session_id}",
        )
        with rc1:
            if st.button("✅ Approve", type="primary", use_container_width=True):
                try:
                    with _warehouse(readonly=False) as wh:
                        MappingSessionRegistry(wh).approve(
                            session.session_id,
                            actor=st.session_state.get("user", "anonymous@local"),
                            notes=review_notes,
                        )
                    st.success("Approved. Click **Deploy** below to write files to disk.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Approve failed: {e}")
        with rc2:
            if st.button("↩️ Request changes", use_container_width=True):
                try:
                    with _warehouse(readonly=False) as wh:
                        MappingSessionRegistry(wh).request_changes(
                            session.session_id,
                            actor=st.session_state.get("user", "anonymous@local"),
                            notes=review_notes,
                        )
                    st.info("Sent back to DRAFT for revision.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Request-changes failed: {e}")
        with rc3:
            if st.button("❌ Reject", use_container_width=True):
                try:
                    with _warehouse(readonly=False) as wh:
                        MappingSessionRegistry(wh).reject(
                            session.session_id,
                            actor=st.session_state.get("user", "anonymous@local"),
                            notes=review_notes,
                        )
                    st.warning("Rejected (terminal).")
                    st.rerun()
                except Exception as e:
                    st.error(f"Reject failed: {e}")

    elif session.status is SessionStatus.APPROVED:
        st.success(
            "✅ APPROVED — click **Deploy** to write the Silver / Gold / On-Prem "
            "files to disk and register them in `CONTROL.mapping_artifacts`."
        )
        if st.button("🚢 Deploy to disk", type="primary"):
            try:
                from datalink.agents.mapper.deployer import deploy_session

                with _warehouse(readonly=False) as wh:
                    artifact = deploy_session(
                        warehouse=wh,
                        session_id=session.session_id,
                        actor=st.session_state.get("user", "anonymous@local"),
                    )
                st.success(
                    f"🚢 Deployed: {len(artifact.files_written)} file(s) written.\n\n"
                    + "\n".join(f"  • `{p}`" for p in artifact.files_written)
                )
                st.rerun()
            except ImportError:
                st.error(
                    "Deployer not available yet — Phase 13.8 wires this up. "
                    "Approval still recorded; files can be written manually for now."
                )
            except Exception as e:
                st.error(f"Deploy failed: {type(e).__name__}: {e}")

    elif session.status is SessionStatus.DEPLOYED:
        st.success(
            f"🚢 DEPLOYED at "
            f"{session.deployed_at.strftime('%Y-%m-%d %H:%M') if session.deployed_at else '—'}. "
            f"Files are live in the dbt project — next dbt run picks them up."
        )


# ============================================================================
# 7. FOOTER — recent deployed sessions for reference
# ============================================================================

if deployed_sessions:
    with st.expander(f"🚢 Recently deployed ({len(deployed_sessions)})"):
        df = pd.DataFrame(
            [
                {
                    "session_id": s.session_id[:8],
                    "source": s.source_qualified_table,
                    "target_mode": s.target_mode.value,
                    "silver_target": s.silver_target_table or "—",
                    "deployed_at": (
                        s.deployed_at.strftime("%Y-%m-%d %H:%M") if s.deployed_at else "—"
                    ),
                    "by": s.reviewed_by or s.created_by,
                }
                for s in deployed_sessions
            ]
        )
        st.dataframe(df, use_container_width=True, hide_index=True)
