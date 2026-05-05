"""DQ AI Architect — chat-driven authoring of custom DQ checks.

Phase 8 rename: was "DQ AI Workshop". The page does much more than
"workshop" implied — real Anthropic Claude calls, RAG-grounded
retrieval of similar approved suites, live SQL preview against the
active warehouse, full SuiteRegistry state-machine handoff.
"Architect" reflects the senior-design role the operator + AI play
together when authoring new quality rules.


User types a natural-language ask in the chat box. The DqProposerAgent
returns a structured GX expectation + the SQL it implies + a sample run.
The user iterates ("make it stricter", "exclude denied claims") until
satisfied, then clicks Approve — the suite is persisted via SuiteRegistry
with ``source=AGENT`` and is picked up by every future checkpoint run for
the chosen client.

State machine — handled entirely by Streamlit session_state:

    [empty]
       │  user types prompt + clicks Propose
       ▼
    [proposal in review]
       │  user types "make it stricter" + Propose Again  → loops back
       │  user clicks Approve → calls accept_proposal    → [persisted]
       │  user clicks Reject  → drops proposal           → [empty]
       ▼
    [persisted]   (toast: suite_id, link to DQ Author for further edits)

Per-(client, table) chat scopes — switching the client or table starts a
fresh conversation so prompts can't leak across tenants.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import streamlit as st

# bootstrap path before datalink imports
ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

st.set_page_config(
    page_title="DQ AI Architect",
    page_icon="🧪",
    layout="wide",
    initial_sidebar_state="expanded",
)

from datalink.adapters._describe import list_columns_typed  # noqa: E402
from datalink.adapters.embeddings.router import get_embedder  # noqa: E402
from datalink.agents.llm_router import get_llm  # noqa: E402
from datalink.config.loader import load_settings  # noqa: E402
from datalink.memory import AgentMemoryStore  # noqa: E402
from datalink.quality.agent_authored import (  # noqa: E402
    Proposal,
    accept_proposal,
    propose_expectation,
)
from datalink.quality.registry import ApprovalMode, SuiteRegistry  # noqa: E402
from datalink.tenancy import Layer, schema_for  # noqa: E402
from datalink.ui._nav import render_sidebar, require_client  # noqa: E402
from datalink.ui._query import warehouse_ctx  # noqa: E402

render_sidebar(active="DQ AI Architect")

_NAVY = "#0a1a3e"
_GOLD = "#d4af37"
_AGENT_BLUE = "#2563eb"

st.markdown(
    f"""
    <style>
      h1 {{color: {_NAVY}; border-bottom: 3px solid {_GOLD}; padding-bottom: .4rem;}}
      .ws-hero {{background:linear-gradient(90deg,{_NAVY}11,{_GOLD}22);
                 padding:1rem 1.2rem;border-radius:8px;border-left:4px solid {_GOLD};
                 margin:.6rem 0 1.5rem 0}}
      .ws-prop {{background:#fff;border:1px solid #d4d4d8;border-radius:8px;
                 padding:1rem;margin:.5rem 0;box-shadow:0 1px 3px rgba(0,0,0,.04)}}
      .ws-prop-head {{font-weight:700;color:{_AGENT_BLUE};margin-bottom:.4rem}}
      .ws-pill {{display:inline-block;padding:.15rem .55rem;border-radius:12px;
                 font-size:.72rem;font-weight:700;letter-spacing:.04em;margin-right:.4rem}}
      .ws-pill-dim {{background:#dbeafe;color:#1e40af}}
      .ws-pill-sev-HIGH {{background:#fee2e2;color:#991b1b}}
      .ws-pill-sev-MEDIUM {{background:#fef3c7;color:#92400e}}
      .ws-pill-sev-LOW {{background:#dcfce7;color:#166534}}
      .ws-meta-row {{font-size:.85rem;color:#475569;margin-top:.4rem}}
    </style>
    """,
    unsafe_allow_html=True,
)


# ============================================================================
# Helpers
# ============================================================================


@contextmanager
def _registry(*, readonly: bool = True) -> Iterator[SuiteRegistry]:
    """Short-lived registry — DuckDB lock-friendly, transparent to caller."""
    with warehouse_ctx(readonly=readonly) as wh:
        yield SuiteRegistry(wh)  # type: ignore[arg-type]


def _resolve_default_table(client_id: str) -> tuple[str, str]:
    """Pick a sensible default (schema, table) for the chosen tenant —
    BRONZE_<CLIENT>.RAW_CLAIMS if it exists, otherwise the first
    bronze table we can introspect, else BRONZE.RAW_CLAIMS."""
    schema = schema_for(client_id, Layer.BRONZE)
    return schema.upper(), "RAW_CLAIMS"


def _backend_label() -> str:
    bk = os.environ.get("DL_ADAPTERS__WAREHOUSE__TYPE", "duckdb").lower()
    return "❄️ Snowflake" if bk == "snowflake" else "🦆 DuckDB"


def _pill(text: str, css: str) -> str:
    return f'<span class="ws-pill {css}">{text}</span>'


# ============================================================================
# Page header
# ============================================================================

st.markdown(
    f"<h1>🧪 <span style='color:{_GOLD}'>DQ AI Architect</span></h1>",
    unsafe_allow_html=True,
)
st.markdown(
    f"""
    <div class="ws-hero">
      <strong>Conversational DQ authoring.</strong> Describe the rule you want
      in plain English. The Proposer agent translates it into a Great
      Expectations check + the SQL it would run, executes a 5-row sample, and
      hands the proposal back to you for review. Iterate as many turns as you
      like, then click <em>Approve</em> to persist as a versioned suite that
      every future checkpoint will pick up automatically.
      <div class="ws-meta-row">Backend: <strong>{_backend_label()}</strong>
      &middot; Author: <code>workshop:{os.environ.get("USER", "operator")}</code></div>
    </div>
    """,
    unsafe_allow_html=True,
)

selected_client = require_client()
# Phase 16.6 — when no client picked (default = All clients), gate
# the per-client content with a friendly notice. Pages with a true
# all-clients view (Pipeline Architect) handle this differently.
if selected_client is None:
    import streamlit as _st

    _st.info(
        "🌐 **All-clients view.** Pick a client from the dropdown above "
        "to load this client-scoped page. Cross-tenant dashboards "
        "(Control Tower, PHI Governance, Cost & Tokens, Lineage) live "
        "elsewhere and don't need a client picker."
    )
    _st.stop()
# ============================================================================
# Top controls — Table picker + suite name + author
# ============================================================================

# Available bronze/silver/gold schemas / tables on the active warehouse —
# feeds the picker without hardcoding paths.
#
# IMPORTANT: ONLY business-data schemas (BRONZE_<CLIENT>, SILVER_silver_<CLIENT>,
# SILVER_gold_um_<CLIENT>) are eligible. Platform-metadata schemas (CONTROL,
# AGENT_MEMORY, etc.) are NEVER candidates for DQ rules — those are written
# by the platform itself and validating them is meta-validation. The earlier
# fallback that showed "all non-system tables" when no client schemas existed
# was a UX bug that surfaced CONTROL.* tables as fake DQ targets — removed
# 2026-04-30 per Jatin's directive ("CONTROL is sacred source of truth").
_BUSINESS_SCHEMA_PREFIXES = ("BRONZE_", "SILVER_", "GOLD_")
with warehouse_ctx(readonly=True) as wh_for_catalog:
    try:
        catalog_rows = wh_for_catalog.query(
            "SELECT table_schema, table_name FROM information_schema.tables "
            "WHERE table_schema NOT IN ('INFORMATION_SCHEMA','PG_CATALOG','PUBLIC') "
            "ORDER BY 1, 2"
        )
    except Exception as e:
        st.error(f"Could not enumerate tables: {e}")
        st.stop()

# Filter to BUSINESS-data schemas for the selected client.
# Schema must start with BRONZE_/SILVER_/GOLD_ AND contain the client_id.
client_upper = selected_client.upper()
client_tables = [
    f"{r['table_schema']}.{r['table_name']}"
    for r in catalog_rows
    if (
        str(r["table_schema"]).upper().startswith(_BUSINESS_SCHEMA_PREFIXES)
        and client_upper in str(r["table_schema"]).upper()
    )
]

if not client_tables:
    st.warning(
        f"⚠️ **No business-data tables available for `{selected_client}` yet.**\n\n"
        f"DQ AI Architect authors quality rules against the **Bronze / Silver / "
        f"Gold** medallion tables (e.g. `BRONZE_{client_upper}.RAW_CLAIMS`). "
        f"Those tables get created by the Airflow pipelines — they don't exist "
        f"until the pipelines have run for this tenant.\n\n"
        f"**Next steps to populate them:**\n"
        f"1. Open the **Control Tower** page.\n"
        f"2. Trigger the `bronze_ingest` DAG for client = `{selected_client}` "
        f"(loads CSVs from SFTP into `BRONZE_{client_upper}.RAW_CLAIMS / "
        f"RAW_MEMBERSHIP / RAW_PROVIDER`).\n"
        f"3. Trigger `silver_transform` (builds the Data Vault: hubs / sats / "
        f"links into `SILVER_silver_{client_upper}`).\n"
        f"4. Trigger `gold_um_push` (builds the operational Gold tables in "
        f"`SILVER_gold_um_{client_upper}` and pushes to Postgres + SQL Server).\n\n"
        f"Once at least Bronze has run, return here and the dropdown will list "
        f"the business tables you can author DQ rules against.\n\n"
        f"_Note: `CONTROL.*` platform-metadata tables are never DQ targets — "
        f"they are managed by the platform itself._"
    )
    st.stop()

default_idx = next(
    (
        i
        for i, t in enumerate(client_tables)
        if t.upper().endswith(".RAW_CLAIMS") and client_upper in t.upper()
    ),
    0,
)

c1, c2, c3 = st.columns([2, 2, 1])
with c1:
    qualified_table = st.selectbox(
        "Target table",
        options=client_tables,
        index=default_idx,
        help="The check you author will be evaluated against this table on every checkpoint run.",
    )
with c2:
    suite_name = st.text_input(
        "Suite name",
        value=f"custom_{qualified_table.split('.')[-1].lower()}",
        help="Authored suites land in CONTROL.dq_suites under this (client_id, suite_name) pair. "
        "Re-using a name creates a new VERSION; new names create a fresh suite.",
    )
with c3:
    # Phase 10.1 — radio now operates on ApprovalMode directly. Default
    # is HITL per Option C (agent-authored = creative content = review).
    approval_mode = st.radio(
        "On Approve, save as:",
        options=[ApprovalMode.DRAFT, ApprovalMode.HITL, ApprovalMode.AUTO_APPROVE],
        index=1,  # default = HITL (audit-friendly Option C)
        format_func=lambda m: {
            ApprovalMode.DRAFT: "📝 DRAFT (edit later)",
            ApprovalMode.HITL: "👁️ HITL (route to DQ Review)",
            ApprovalMode.AUTO_APPROVE: "⚡ AUTO_APPROVE (skip review, go LIVE)",
        }[m],
        help=(
            "DRAFT — saves it editable on the DQ Author page; you can "
            "refine and submit later.\n\n"
            "HITL — routes to the DQ Review queue for an independent "
            "reviewer. This is the audit-friendly default for "
            "agent-authored content (Option C).\n\n"
            "AUTO_APPROVE — skips peer review and activates immediately. "
            "Reserved for trusted ad-hoc rules where you are both author "
            "and approver. Logged with explicit auto-approval audit."
        ),
    )


# ============================================================================
# Reset chat scope when client/table changes
# ============================================================================

scope_key = f"workshop_scope::{selected_client}::{qualified_table}"
if st.session_state.get("workshop_active_scope") != scope_key:
    st.session_state["workshop_active_scope"] = scope_key
    st.session_state["workshop_history"] = []
    st.session_state["workshop_proposal"] = None
    st.session_state["workshop_last_prompt"] = ""


# ============================================================================
# Schema preview — feeds both the agent prompt + the user's mental model
# ============================================================================

with st.expander(f"📋 Schema for `{qualified_table}`", expanded=False):
    try:
        with warehouse_ctx(readonly=True) as wh_for_schema:
            cols_typed = list_columns_typed(wh_for_schema, qualified_table)
        st.dataframe(
            {"column": [c for c, _ in cols_typed], "type": [t for _, t in cols_typed]},
            hide_index=True,
            use_container_width=True,
        )
    except Exception as e:
        st.error(f"DESCRIBE failed: {e}")
        st.stop()

columns_for_agent = [{"name": c, "type": t} for c, t in cols_typed]


# ============================================================================
# Chat history (read-only display)
# ============================================================================

if st.session_state["workshop_history"]:
    st.markdown("##### 💬 Conversation")
    for turn in st.session_state["workshop_history"]:
        with st.chat_message(turn["role"]):
            st.markdown(turn["content"])


# ============================================================================
# Prompt input
# ============================================================================

prompt = st.text_area(
    "What do you want the check to enforce?",
    value="",
    height=110,
    placeholder=(
        "Examples:\n"
        "  • Every claim must have a 10-digit provider NPI (regex: ^[0-9]{10}$).\n"
        "  • billed_amount should never exceed $250k — flag potential fraud.\n"
        "  • claim_status can only be SUBMITTED, APPROVED, DENIED, or PENDED.\n"
        "  • prior_auth_ref must be set whenever cpt_code starts with 2744 (inpatient surgery)."
    ),
    key=f"prompt_input::{scope_key}",
)

# ---------------------------------------------------------------------------
# Phase 8 — AI controls: temperature + grounding (RAG)
# ---------------------------------------------------------------------------
with st.expander("⚙️ AI controls (temperature, grounding)", expanded=False):
    ctl_t, ctl_g, ctl_k = st.columns([2, 1, 1])
    with ctl_t:
        ai_temperature = st.slider(
            "Temperature",
            min_value=0.0,
            max_value=1.0,
            value=0.0,
            step=0.05,
            help=(
                "0.0 = deterministic (default — same prompt → same proposal). "
                "0.3-0.5 = mild variation. "
                "0.7+ = creative — useful when you want the agent to explore "
                "alternative phrasings or thresholds. Logged on the proposal."
            ),
        )
    with ctl_g:
        ai_grounding_on = st.toggle(
            "🔗 Grounded RAG",
            value=True,
            help=(
                "ON: agent sees the top-k most-similar approved suites for "
                "this client as EXAMPLES before drafting. Tracks team conventions "
                "(severity defaults, threshold style, dimension preferences). "
                "OFF: ungrounded — agent reasons from your prompt + the table "
                "schema only."
            ),
        )
    with ctl_k:
        ai_grounding_k = st.number_input(
            "Grounding k",
            min_value=1,
            max_value=10,
            value=3,
            step=1,
            help="Number of similar past suites to retrieve and inject as examples.",
            disabled=not ai_grounding_on,
        )

# Phase 16.10 — AI spend confirmation gate. Disabled by default to prevent
# accidental Propose clicks burning LLM tokens during demo / exploration.
_dq_ai_confirm = st.checkbox(
    "✅ I confirm AI spend (~$0.001 per propose)",
    value=False,
    key="dq_ai_confirm",
    help="Required to enable the Propose button. Calls Claude Haiku 4.5 to "
    "translate your natural-language ask into a GX expectation. "
    "Defaults OFF — tick to enable, click Propose, no tokens spent until then.",
)

b1, b2, b3 = st.columns([1, 1, 4])
propose_clicked = b1.button(
    "🧠 Propose",
    type="primary",
    use_container_width=True,
    disabled=(not prompt.strip()) or (not _dq_ai_confirm),
    help=None if _dq_ai_confirm else "🔒 Tick the AI-spend confirm box above to enable.",
)
clear_clicked = b2.button("🗑️ Clear chat", use_container_width=True)

if clear_clicked:
    st.session_state["workshop_history"] = []
    st.session_state["workshop_proposal"] = None
    st.session_state["workshop_last_prompt"] = ""
    st.rerun()


# ============================================================================
# Propose action — call the agent
# ============================================================================

if propose_clicked and prompt.strip():
    settings = load_settings()
    llm = get_llm(settings)

    # Append user turn first so it's visible while the spinner runs.
    st.session_state["workshop_history"].append({"role": "user", "content": prompt.strip()})
    st.session_state["workshop_last_prompt"] = prompt.strip()

    with st.spinner(f"{llm.__class__.__name__} thinking…"):
        try:
            # Build the memory store on demand — short-lived, just like the
            # warehouse handle. ensure_schema() is idempotent so it costs
            # essentially nothing on re-entry.
            memory_store = None
            if ai_grounding_on:
                try:
                    memory_store = AgentMemoryStore(embedder=get_embedder())
                    memory_store.ensure_schema()
                except Exception as mem_err:
                    st.info(
                        f"⚠ Grounding unavailable ({mem_err}). "
                        f"Proceeding without retrieval; the proposal still ships."
                    )
                    memory_store = None

            with warehouse_ctx(readonly=True) as wh_for_agent:
                # ``warehouse_ctx`` yields the project's _Warehouse facade;
                # ``propose_expectation`` annotates against the protocol
                # ``Warehouse``. They are duck-equivalent (query / execute);
                # cast for mypy's structural check.
                from typing import cast as _cast

                from datalink.adapters.protocols import Warehouse as _WarehouseProto

                proposal_out = propose_expectation(
                    llm=llm,
                    warehouse=_cast(_WarehouseProto, wh_for_agent),
                    client_id=selected_client,
                    qualified_table=qualified_table,
                    columns=columns_for_agent,
                    prompt=prompt.strip(),
                    history=st.session_state["workshop_history"][
                        :-1
                    ],  # exclude the just-added turn
                    temperature=float(ai_temperature),
                    memory=memory_store,
                    grounding_k=int(ai_grounding_k),
                    grounding_enabled=bool(ai_grounding_on),
                )
            st.session_state["workshop_proposal"] = proposal_out
            # Phase 16.5 (Wave 5) — persist proposal to CONTROL.agent_proposals
            # so cost telemetry + audit trail capture every DQ-Architect run.
            try:
                from datalink.proposals import store as p_store

                _toks = int(getattr(proposal_out, "tokens_used", 0) or 0)
                p_store.save_proposal(
                    agent_type="dq_proposer",
                    scope_type="dq_scope",
                    scope_key=f"{selected_client}:{qualified_table}",
                    payload={
                        "expectation_type": proposal_out.expectation_type,
                        "kwargs": getattr(proposal_out, "kwargs", {}),
                        "meta": getattr(proposal_out, "meta", {}),
                        "sample_sql": getattr(proposal_out, "sample_sql", ""),
                    },
                    agent_input={
                        "prompt": prompt.strip(),
                        "table": qualified_table,
                        "client_id": selected_client,
                    },
                    prompt_tokens=int(_toks * 0.8),
                    completion_tokens=_toks - int(_toks * 0.8),
                    model_id=str(getattr(proposal_out, "model_id", "claude-haiku-4.5")),
                    latency_ms=int(getattr(proposal_out, "duration_ms", 0) or 0),
                    created_by=f"ui:{selected_client}",
                )
            except Exception:
                pass  # store failure must not break the propose flow
            agent_summary = (
                f"**{proposal_out.expectation_type}** &mdash; "
                f"{proposal_out.meta.get('description', '')}"
            )
            st.session_state["workshop_history"].append(
                {"role": "assistant", "content": agent_summary}
            )
        except Exception as e:
            st.session_state["workshop_history"].append(
                {"role": "assistant", "content": f"❌ {type(e).__name__}: {e}"}
            )
            st.session_state["workshop_proposal"] = None
    st.rerun()


# ============================================================================
# Render the latest proposal (review surface)
# ============================================================================

proposal: Proposal | None = st.session_state.get("workshop_proposal")
if proposal is not None:
    sev = proposal.meta.get("severity", "MEDIUM")
    dim = proposal.meta.get("dq_dimension", "")
    known = proposal.meta.get("known_type", True)

    pills = (
        _pill(f"DIM: {dim}", "ws-pill-dim")
        + _pill(f"SEV: {sev}", f"ws-pill-sev-{sev}")
        + (_pill("⚠ uncommon expectation type", "ws-pill-sev-MEDIUM") if not known else "")
    )

    st.markdown('<div class="ws-prop">', unsafe_allow_html=True)
    st.markdown(
        f'<div class="ws-prop-head">🤖 Proposal — {proposal.expectation_type}</div>'
        f"<div>{pills}</div>"
        f"<div class='ws-meta-row'>{proposal.meta.get('description', '')}</div>",
        unsafe_allow_html=True,
    )

    pcol1, pcol2 = st.columns([1, 1])
    with pcol1:
        st.markdown("**kwargs**")
        st.json(proposal.kwargs, expanded=True)
    with pcol2:
        st.markdown("**rationale**")
        st.markdown(proposal.rationale)

    st.markdown("**SQL preview**")
    st.code(proposal.sql_preview, language="sql")

    st.markdown("**Sample run**")
    if proposal.sample_error:
        st.warning(f"SQL preview failed: {proposal.sample_error}")
    elif proposal.sample_rows:
        st.dataframe(proposal.sample_rows, hide_index=True, use_container_width=True)
    else:
        st.info(
            "Query returned 0 rows. Either the table is empty or the rule doesn't match anything."
        )

    st.markdown(
        f'<div class="ws-meta-row">Model: <code>{proposal.proposer_model}</code> &middot; '
        f"Tokens: <code>{proposal.tokens_used}</code> &middot; "
        f"Took: <code>{proposal.duration_ms} ms</code> &middot; "
        f"Temperature: <code>{proposal.temperature:.2f}</code>"
        + (
            f" &middot; Embeddings: <code>{proposal.embedding_model}</code>"
            if proposal.embedding_model
            else ""
        )
        + "</div>",
        unsafe_allow_html=True,
    )

    # Phase 8 — show what past suites the agent saw as EXAMPLES.
    if proposal.grounding:
        with st.expander(
            f"🔗 AI grounded its proposal on {len(proposal.grounding)} past suite(s)",
            expanded=False,
        ):
            st.caption(
                "Lower distance = more similar. Cosine distance: "
                "0.0 = identical, ~1.0 = orthogonal. The agent saw these "
                "as EXAMPLES OF APPROVED SUITES FOR THIS CLIENT to keep "
                "its proposal stylistically consistent with prior work."
            )
            for hit in proposal.grounding:
                st.markdown(
                    f"**{hit['suite_name']}** "
                    f"(`{hit.get('status', '?')}` · src=`{hit.get('source', '?')}` · "
                    f"distance=`{hit['distance']:.3f}`)"
                )
                # Render the source_text in a code block — easy to scan.
                st.code(hit.get("source_text", "")[:1500], language="text")
    else:
        st.caption(
            "_No grounding hits — either RAG was off, no past suites exist "
            "for this client, or the memory store wasn't reachable. "
            "Proposal is ungrounded._"
        )

    st.markdown("</div>", unsafe_allow_html=True)

    # Approve / Edit / Reject buttons. Approve label adapts to chosen mode.
    approve_label = {
        ApprovalMode.DRAFT: "💾 Save as DRAFT",
        ApprovalMode.HITL: "👁️ Submit for Review",
        ApprovalMode.AUTO_APPROVE: "⚡ Approve & Go LIVE",
    }[approval_mode]
    next_destination_hint = {
        ApprovalMode.DRAFT: "DQ Author (editable)",
        ApprovalMode.HITL: "DQ Review (reviewer queue)",
        ApprovalMode.AUTO_APPROVE: "DQ Suite Registry (LIVE)",
    }[approval_mode]

    a1, a2, a3, _ = st.columns([1.2, 1, 1, 1.8])
    if a1.button(
        approve_label,
        type="primary",
        use_container_width=True,
        help=(
            "Writes the proposal to CONTROL.dq_suites with source=AGENT and "
            f"walks the state machine per **{approval_mode.value}** policy. "
            f"Next stop after this click: {next_destination_hint}."
        ),
    ):
        try:
            with _registry(readonly=False) as reg:
                suite_id = accept_proposal(
                    registry=reg,
                    proposal=proposal,
                    client_id=selected_client,
                    suite_name=(
                        suite_name.strip() or f"custom_{qualified_table.split('.')[-1].lower()}"
                    ),
                    source_prompt=st.session_state["workshop_last_prompt"],
                    author=f"workshop:{os.environ.get('USER', 'operator')}",
                    mode=approval_mode,
                )
            st.success(
                f"Suite persisted — `{suite_id}` "
                f"(policy: **{approval_mode.value}**). "
                f"Find it on **{next_destination_hint}**."
            )
            st.session_state["workshop_proposal"] = None
            # Keep the chat history so the user can see what they did.
        except Exception as e:
            st.error(f"Persist failed: {type(e).__name__}: {e}")

    if a2.button("✏️ Edit & retry", use_container_width=True):
        st.info(
            "Type a follow-up in the prompt box ('make it stricter', "
            "'exclude denied claims', etc.) and click Propose again. "
            "The agent uses the prior turns as context."
        )

    if a3.button("🗑️ Reject", use_container_width=True):
        st.session_state["workshop_proposal"] = None
        st.session_state["workshop_history"].append(
            {"role": "user", "content": "_(rejected the previous proposal)_"}
        )
        st.rerun()


# ============================================================================
# Footer — recent suites for this client+table (read-only)
# ============================================================================

st.markdown("---")
st.markdown("##### 📚 Custom suites for this client")
st.caption(
    "All workshop-authored suites for the selected tenant. Pick any "
    "non-archived row in the dropdown below to inspect or archive it."
)
try:
    with _registry(readonly=True) as reg:
        names = [n for n in reg.list_suite_names(selected_client) if n.startswith("custom_")]
        rows: list[dict[str, Any]] = []
        all_versions: list[Any] = []  # keep SuiteVersion refs for actions
        for n in names:
            for v in reg.list_versions(selected_client, n):
                rows.append(
                    {
                        "suite_name": v.suite_name,
                        "version": v.version,
                        "status": v.status.value,
                        "source": v.source.value,
                        "expectations": len(v.expectations),
                        "created_at": (
                            v.created_at.isoformat()
                            if hasattr(v, "created_at") and v.created_at
                            else ""
                        ),
                        "suite_id": v.suite_id,
                    }
                )
                all_versions.append(v)
        if not rows:
            st.caption("No custom suites yet for this client. Author one above ↑")
        else:
            st.dataframe(
                [{k: v for k, v in r.items() if k != "suite_id"} for r in rows],
                hide_index=True,
                use_container_width=True,
            )

            # Per-suite action panel — pick any non-terminal suite + archive it.
            actionable = [v for v in all_versions if v.status.value not in ("ARCHIVED", "REJECTED")]
            if actionable:
                st.markdown("###### 🛠️ Manage a suite")
                opt_to_id = {
                    f"{v.suite_name} v{v.version} [{v.status.value}] — {v.suite_id[:8]}": v
                    for v in actionable
                }
                pick_label = st.selectbox(
                    "Pick suite",
                    options=list(opt_to_id.keys()),
                    label_visibility="collapsed",
                )
                picked = opt_to_id[pick_label]
                arc_col1, arc_col2 = st.columns([1, 4])
                with arc_col1:
                    arc_confirm = st.toggle(
                        "🗄️ Archive",
                        value=False,
                        key=f"ws_arc_toggle_{picked.suite_id}",
                        help="Two-click confirm: ON enables the Archive button.",
                    )
                with arc_col2:
                    if arc_confirm and st.button(
                        f"Archive '{picked.suite_name}' v{picked.version} now",
                        type="secondary",
                        key=f"ws_arc_btn_{picked.suite_id}",
                    ):
                        try:
                            with _registry(readonly=False) as reg_w:
                                reg_w.archive(
                                    picked.suite_id,
                                    actor=f"workshop:{os.environ.get('USER', 'operator')}",
                                    reason="archived from DQ AI Architect",
                                )
                            st.success(
                                f"Archived `{picked.suite_name}` v{picked.version}. "
                                f"It no longer gates pipeline runs (audit row preserved)."
                            )
                            st.rerun()
                        except Exception as e:
                            st.error(f"Archive failed: {e}")
            else:
                st.caption("All custom suites for this client are already in a terminal state.")
except Exception as e:
    st.caption(f"_(history unavailable: {e})_")
