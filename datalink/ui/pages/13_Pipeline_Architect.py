"""Pipeline Architect — Phase 15 — Gold-first multi-tenant pipeline orchestrator.

This is the new top-of-funnel for Author DQ. The Global Gold Catalog
(33 datasets, 943 fields) is the contract; the operator picks a dataset,
the agent proposes a fully-resolved Bronze→Silver→Gold pipeline anchored
to it, and one click deploys all 5 artifacts:

    1. Gold DDL              — datalink/pipeline/gold/ddl/<client>_<dataset>.sql
    2. Silver dbt model      — dbt/models/silver/<client>/<dataset>_clean.sql
    3. Gold dbt model        — dbt/models/gold/<client>/<dataset>.sql
    4. Airflow DAG           — dags/<client>_<dataset>_pipeline.py
    5. GX expectation suite  — CONTROL.dq_suites row (LIVE)

The agent uses real Anthropic Claude haiku-4.5 for the executive summary +
clone-vs-build narrative + override review. Deterministic builders generate
the structural artifacts so they're identical across runs.
"""

from __future__ import annotations

import json
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

# bootstrap path
ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

st.set_page_config(
    page_title="Pipeline Architect",
    page_icon="🏛",
    layout="wide",
    initial_sidebar_state="expanded",
)

from datalink.agents.llm_router import get_llm  # noqa: E402
from datalink.agents.pipeline_architect import (  # noqa: E402
    approve_and_deploy,
    list_client_instances,
    list_dataset_codes,
    persist_proposal,
    propose_pipeline,
)
from datalink.config.loader import load_settings  # noqa: E402
from datalink.quality.control import create_control_tables  # noqa: E402
from datalink.ui._nav import render_sidebar, require_client  # noqa: E402
from datalink.ui._query import warehouse_ctx  # noqa: E402

render_sidebar(active="Pipeline Architect")

_NAVY = "#0a1a3e"
_GOLD = "#d4af37"
_AGENT_BLUE = "#2563eb"
_GREEN = "#15803d"
_AMBER = "#b45309"
_RED = "#b91c1c"

st.markdown(
    f"""
    <style>
      h1 {{color: {_NAVY}; border-bottom: 3px solid {_GOLD}; padding-bottom: .4rem;}}
      h2 {{color: {_NAVY}; margin-top: 1.5rem;}}
      h3 {{color: {_NAVY};}}
      .arch-hero {{background:linear-gradient(90deg,{_NAVY}11,{_GOLD}22);
                   padding:1rem 1.2rem;border-radius:8px;border-left:4px solid {_GOLD};
                   margin:.6rem 0 1.5rem 0}}
      .arch-card {{background:#fff;border:1px solid #d4d4d8;border-radius:8px;
                   padding:1rem;margin:.5rem 0;box-shadow:0 1px 3px rgba(0,0,0,.04)}}
      .arch-pill {{display:inline-block;padding:.15rem .55rem;border-radius:12px;
                   font-size:.72rem;font-weight:700;letter-spacing:.04em;margin-right:.4rem}}
      .arch-pill-clone {{background:#dbeafe;color:#1e40af}}
      .arch-pill-build {{background:#dcfce7;color:#166534}}
      .arch-pill-live {{background:#dcfce7;color:#166534}}
      .arch-pill-pending {{background:#fef3c7;color:#92400e}}
      .arch-pill-archived {{background:#f3f4f6;color:#4b5563}}
      .arch-pill-fhir {{background:#dbeafe;color:#1e40af}}
      .arch-pill-x12 {{background:#fde68a;color:#78350f}}
      .arch-pill-flat {{background:#e0e7ff;color:#3730a3}}
      .arch-pill-ncpdp {{background:#fce7f3;color:#9d174d}}
      .arch-pill-api {{background:#d1fae5;color:#065f46}}
      .arch-pill-dev {{background:#fee2e2;color:#991b1b}}
      .arch-meta {{font-size:.85rem;color:#475569;margin-top:.4rem}}
      .arch-cite {{font-size:.78rem;color:{_AGENT_BLUE};font-style:italic;}}
      .arch-stat {{font-size:1.6rem;font-weight:700;color:{_NAVY};}}
      .arch-stat-label {{font-size:.78rem;color:#64748b;text-transform:uppercase;
                         letter-spacing:.05em;font-weight:600;}}
      .arch-route-row {{padding:.4rem .6rem;border-radius:4px;background:#f8fafc;
                        border-left:3px solid {_AGENT_BLUE};margin:.2rem 0;}}
      .arch-route-row.disabled {{border-left-color:{_RED};opacity:.6;}}
    </style>
    """,
    unsafe_allow_html=True,
)


@contextmanager
def _warehouse(*, readonly: bool = True) -> Iterator[Any]:
    with warehouse_ctx(readonly=readonly) as wh:
        yield wh


# ---------------------------------------------------------------------------
# HEADER
# ---------------------------------------------------------------------------

st.markdown("# 🏛 Pipeline Architect")
st.markdown(
    """
    <div class="arch-hero">
      <strong>Gold-first multi-tenant pipelines from the Global Gold Catalog.</strong>
      Pick a dataset, the agent proposes a fully-resolved
      Bronze→Silver→Gold pipeline anchored to the catalog. One click
      deploys: Gold DDL, Silver+Gold dbt models, Airflow DAG, GX
      expectation suite, OnPrem routing rules. CLONE from a peer client
      when one exists; BUILD from catalog when it's the first.
      <div class="arch-meta">
        Backend: <strong>Claude Haiku 4.5 + deterministic builders</strong>
        &middot; Catalog version 1: 33 datasets &middot; 943 fields &middot;
        61 routing rules. PHI-safe: only metadata reaches the LLM.
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)

selected_client = require_client()


# ---------------------------------------------------------------------------
# Bootstrap CONTROL tables (idempotent — no-op after first run)
# ---------------------------------------------------------------------------

with _warehouse(readonly=False) as wh:
    create_control_tables(wh)


# ---------------------------------------------------------------------------
# CATALOG STATUS PANEL — shows operator the catalog is loaded + browsable.
# ---------------------------------------------------------------------------

with _warehouse(readonly=True) as wh:
    catalog_datasets = list_dataset_codes(wh)
    all_instances = list_client_instances(wh)

st.markdown("## 📚 Global Gold Catalog")

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.markdown(
        f'<div class="arch-card"><div class="arch-stat">{len(catalog_datasets)}</div>'
        f'<div class="arch-stat-label">Datasets</div></div>',
        unsafe_allow_html=True,
    )
with col2:
    total_fields = sum(int(d.get("total_fields") or 0) for d in catalog_datasets)
    st.markdown(
        f'<div class="arch-card"><div class="arch-stat">{total_fields}</div>'
        f'<div class="arch-stat-label">Fields (across all datasets)</div></div>',
        unsafe_allow_html=True,
    )
with col3:
    live_count = sum(1 for i in all_instances if i.get("status") == "LIVE")
    st.markdown(
        f'<div class="arch-card"><div class="arch-stat">{live_count}</div>'
        f'<div class="arch-stat-label">LIVE pipeline instances</div></div>',
        unsafe_allow_html=True,
    )
with col4:
    distinct_clients = len({i.get("client_id") for i in all_instances if i.get("client_id")})
    st.markdown(
        f'<div class="arch-card"><div class="arch-stat">{distinct_clients}</div>'
        f'<div class="arch-stat-label">Onboarded clients</div></div>',
        unsafe_allow_html=True,
    )


with st.expander("📋 Browse catalog — 33 datasets, click to inspect", expanded=False):
    if not catalog_datasets:
        st.warning(
            "Catalog is empty. Run "
            "`docker exec datalink-control-tower python3 /opt/datalink/scripts/load_product_catalog.py` "
            "to seed it from `data/sample/product_catalog.xlsx`."
        )
    else:
        # Friendly DataFrame summary
        rows = []
        for d in catalog_datasets:
            used_by_raw = d.get("used_by") or "[]"
            try:
                used_by_list = (
                    json.loads(used_by_raw) if isinstance(used_by_raw, str) else used_by_raw
                )
            except json.JSONDecodeError:
                used_by_list = []
            rows.append(
                {
                    "Dataset": d.get("display_name"),
                    "Code": d.get("dataset_code"),
                    "Category": d.get("category"),
                    "Frequency": d.get("default_frequency"),
                    "Total fields": d.get("total_fields"),
                    "Required": d.get("required_fields"),
                    "Optional": d.get("optional_fields"),
                    "Used by": ", ".join(used_by_list) if used_by_list else "—",
                }
            )
        df = pd.DataFrame(rows)
        st.dataframe(df, use_container_width=True, hide_index=True)


# ---------------------------------------------------------------------------
# PROPOSAL FORM — pick dataset + bronze anchor + run the agent.
# ---------------------------------------------------------------------------

st.markdown(f"## 🛠 Propose a pipeline for `{selected_client}`")

if not catalog_datasets:
    st.stop()

# Dataset picker
dataset_options = sorted(catalog_datasets, key=lambda d: str(d.get("display_name") or ""))
ds_label_to_obj = {
    f"{d.get('display_name')}  ({d.get('total_fields')} fields, {d.get('category')})": d
    for d in dataset_options
}

col_ds, col_anchor, col_mode, col_sched = st.columns([3, 2, 1.5, 1.5])

with col_ds:
    selected_label = st.selectbox(
        "Dataset",
        options=list(ds_label_to_obj.keys()),
        index=0,
        help="Pick a dataset from the Global Gold Catalog. The agent anchors against this.",
    )
    selected_dataset = ds_label_to_obj[selected_label]
    selected_dataset_code = str(selected_dataset.get("dataset_code"))
    selected_dataset_display = str(selected_dataset.get("display_name"))

with col_anchor:
    bronze_anchor = st.selectbox(
        "Bronze anchor",
        options=["FLAT_FILE", "FHIR", "X12", "NCPDP", "API"],
        index=0,
        help="What shape does the client's incoming data take? Drives the Bronze landing pattern. "
        "Silver+Gold stay structurally identical regardless.",
    )

with col_mode:
    decision_mode = st.selectbox(
        "Decision",
        options=["AUTO", "CLONE", "BUILD"],
        index=0,
        help="AUTO: clone if a peer client has this dataset LIVE, otherwise build from catalog. "
        "Force CLONE/BUILD to override.",
    )

with col_sched:
    schedule_cron = st.text_input(
        "Schedule (cron)",
        value="0 4 * * *",
        help="Airflow schedule expression. Default: daily at 04:00 UTC.",
    )

# Peer instances panel — show what's already live for this dataset
peer_instances = [i for i in all_instances if i.get("dataset_code") == selected_dataset_code]
if peer_instances:
    st.markdown("##### 👥 Peer instances for this dataset")
    pdf_rows = []
    for inst in peer_instances:
        pdf_rows.append(
            {
                "Client": inst.get("client_id"),
                "Anchor": inst.get("bronze_anchor"),
                "Status": inst.get("status"),
                "Gold table": f"{inst.get('gold_schema')}.{inst.get('gold_table')}",
                "Schedule": inst.get("schedule_cron"),
                "Deviations": inst.get("deviation_count"),
                "Deployed": inst.get("deployed_at"),
            }
        )
    st.dataframe(pd.DataFrame(pdf_rows), use_container_width=True, hide_index=True)
else:
    st.info(
        f"No peer client has the **{selected_dataset_display}** dataset live yet. "
        f"This deployment will be the **first instance** — future clients can clone from it."
    )


# ---------------------------------------------------------------------------
# PROPOSAL ACTION — invoke agent, store proposal in session_state.
# ---------------------------------------------------------------------------

propose_btn, _ = st.columns([1, 5])
with propose_btn:
    propose_clicked = st.button(
        "🚀 Propose pipeline",
        type="primary",
        use_container_width=True,
    )

PROPOSAL_KEY = f"p15_proposal_{selected_client}_{selected_dataset_code}"

if propose_clicked:
    with (
        st.spinner(
            f"Agent proposing {selected_dataset_display} pipeline for {selected_client} "
            f"(anchor={bronze_anchor}, decision={decision_mode})..."
        ),
        _warehouse(readonly=False) as wh,
    ):
        settings = load_settings()
        llm = get_llm(settings)
        try:
            proposal = propose_pipeline(
                llm=llm,
                warehouse=wh,
                client_id=selected_client,
                dataset_code=selected_dataset_code,
                bronze_anchor=bronze_anchor,
                decision_mode=decision_mode,
                schedule_cron=schedule_cron,
                actor=f"ui:{selected_client}",
            )
            st.session_state[PROPOSAL_KEY] = proposal
            st.success(
                f"Proposal ready — decision={proposal['decision_mode']}, "
                f"{proposal['resolved_field_count']} resolved Gold columns, "
                f"{proposal['gx_suite']['expectation_count']} GX expectations, "
                f"{proposal['tokens_used']} LLM tokens, "
                f"{proposal['duration_ms']}ms."
            )
        except Exception as exc:
            st.error(f"Agent failed: {type(exc).__name__}: {exc}")
            st.exception(exc)


# ---------------------------------------------------------------------------
# PROPOSAL DISPLAY — render once an active proposal is in session_state.
# ---------------------------------------------------------------------------

active_proposal = st.session_state.get(PROPOSAL_KEY)
if not active_proposal:
    st.markdown("---")
    st.info("👆 Click **Propose pipeline** to generate a proposal.")
    st.stop()
proposal = active_proposal  # alias for the rest of the rendering logic

st.markdown("---")
st.markdown(f"## 📋 Proposal — {selected_dataset_display} for `{proposal['client_id']}`")

# Decision banner
decision_pill_class = (
    "arch-pill arch-pill-clone"
    if proposal["decision_mode"] == "CLONE"
    else "arch-pill arch-pill-build"
)
anchor_class = {
    "FHIR": "arch-pill-fhir",
    "X12": "arch-pill-x12",
    "FLAT_FILE": "arch-pill-flat",
    "NCPDP": "arch-pill-ncpdp",
    "API": "arch-pill-api",
}.get(str(proposal["bronze_anchor"]), "arch-pill-flat")

st.markdown(
    f"""
    <div class="arch-card">
      <span class="{decision_pill_class}">DECISION: {proposal["decision_mode"]}</span>
      <span class="arch-pill {anchor_class}">BRONZE: {proposal["bronze_anchor"]}</span>
      <span class="arch-pill arch-pill-pending">STATUS: NEW PROPOSAL</span>
      <div class="arch-meta">
        Cloned from: <code>{proposal.get("cloned_from_instance_id") or "—"}</code>
        &nbsp;·&nbsp; Schedule: <code>{proposal["schedule_cron"]}</code>
        &nbsp;·&nbsp; Catalog version: {proposal["catalog_version"]}
        &nbsp;·&nbsp; Proposer: {proposal["proposer_model"]}
        &nbsp;·&nbsp; Tokens: {proposal["tokens_used"]}
        &nbsp;·&nbsp; Latency: {proposal["duration_ms"]}ms
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)

# AI narrative
st.markdown("### 🤖 AI Architect narrative")
st.markdown(
    f"""
    <div class="arch-card">
      <h4>Executive summary</h4>
      <p>{proposal["executive_summary"]}</p>
      <h4>Clone vs Build recommendation</h4>
      <p>{proposal["clone_recommendation"]}</p>
      <h4>Override review</h4>
      <p>{proposal["override_review"]}</p>
      <h4>Audit-log rationale</h4>
      <p class="arch-cite">{proposal["rationale"]}</p>
    </div>
    """,
    unsafe_allow_html=True,
)

# Schema layout
st.markdown("### 🏗 Pipeline layout")
sl1, sl2, sl3 = st.columns(3)
sl1.markdown(
    f"""
    <div class="arch-card">
      <div class="arch-stat-label">Bronze ({proposal["bronze_anchor"]})</div>
      <div style="font-family:ui-monospace,Menlo,monospace;font-size:.95rem;color:{_NAVY};">
        {proposal["bronze_schema"]}<br>.{proposal["bronze_table"]}
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)
sl2.markdown(
    f"""
    <div class="arch-card">
      <div class="arch-stat-label">Silver (NORMALIZED)</div>
      <div style="font-family:ui-monospace,Menlo,monospace;font-size:.95rem;color:{_NAVY};">
        {proposal["silver_schema"]}<br>.{proposal["silver_table"]}
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)
sl3.markdown(
    f"""
    <div class="arch-card">
      <div class="arch-stat-label">Gold (UM_OPERATIONAL)</div>
      <div style="font-family:ui-monospace,Menlo,monospace;font-size:.95rem;color:{_NAVY};">
        {proposal["gold_schema"]}<br>.{proposal["gold_table"]}
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)

# Stats row
sm1, sm2, sm3, sm4 = st.columns(4)
sm1.metric("Resolved fields", proposal["resolved_field_count"])
sm2.metric("Catalog overrides", proposal["override_count"])
sm3.metric("GX expectations", proposal["gx_suite"]["expectation_count"])
sm4.metric("Routing entries", len(proposal["routing_plan"]))

# Routing plan
st.markdown("### 🛰 OnPrem routing plan")
if not proposal["routing_plan"]:
    st.info("No downstream products subscribe to this dataset.")
else:
    for r in proposal["routing_plan"]:
        disabled_cls = "disabled" if r["action"].startswith("DISABLED") else ""
        st.markdown(
            f"""
            <div class="arch-route-row {disabled_cls}">
              <strong>{r["downstream_product"]}</strong>
              &nbsp;→&nbsp; <code>{r["target_system"]}</code>
              &nbsp;<code>{r.get("target_uri") or ""}</code>
              &nbsp;·&nbsp; <span class="arch-meta">action={r["action"]}</span>
              {f'&nbsp;·&nbsp; <span class="arch-meta">{r["rationale"]}</span>' if r.get("rationale") else ""}
            </div>
            """,
            unsafe_allow_html=True,
        )

# Deviations (overrides) — if any
if proposal.get("deviations"):
    st.markdown("### 🔀 Catalog deviations (client overrides)")
    dev_rows = []
    for d in proposal["deviations"]:
        dev_rows.append(
            {
                "Column": d.get("gold_column_name"),
                "Kind": d.get("kind"),
                "Before": json.dumps(d.get("before"), default=str)[:80] if d.get("before") else "—",
                "After": json.dumps(d.get("after"), default=str)[:80] if d.get("after") else "—",
                "Rationale": d.get("rationale") or "—",
            }
        )
    st.dataframe(pd.DataFrame(dev_rows), use_container_width=True, hide_index=True)
else:
    st.markdown("### 🔀 Catalog deviations")
    st.success(
        "**No client-level overrides.** The pipeline is structurally identical to the global "
        "Gold catalog. Cross-client validations stay coherent."
    )


# ---------------------------------------------------------------------------
# Phase 15.7 — Gold-LIVE banner
# ---------------------------------------------------------------------------

gold_status = proposal.get("gold_schema_status")
silver_pattern = proposal.get("silver_pattern")
silver_models = proposal.get("silver_dbt_models") or {}
if gold_status == "LIVE":
    st.markdown(
        f"""
        <div style="background:#dcfce7;border:1px solid #86efac;
                    border-left:4px solid #15803d;padding:.85rem 1.1rem;
                    border-radius:8px;margin:.8rem 0 1.2rem 0;">
          <strong>✅ Gold-LIVE detected</strong> &nbsp;·&nbsp;
          Anchor: <code>{proposal.get('live_gold_anchor')}</code> &nbsp;·&nbsp;
          Gold table: <code>{proposal.get('live_gold_table_name')}</code> &nbsp;·&nbsp;
          {proposal.get('gold_columns_count', 0)} canonical columns &nbsp;·&nbsp;
          {proposal.get('bronze_to_gold_mapping_count', 0)} Bronze→Gold mappings.
          <br>
          <span style="font-size:.85rem;color:#166534;">
          Silver pattern: <strong>{silver_pattern}</strong>
          ({len(silver_models)} dbt files generated)
          {' &nbsp;·&nbsp; <em>(operator-flagged as overkill — review before deploy)</em>' if proposal.get('silver_pattern_is_overkill') else ''}
          </span>
        </div>
        """,
        unsafe_allow_html=True,
    )
elif gold_status == "MISSING_RECOMMEND_DESIGN":
    st.markdown(
        f"""
        <div style="background:#fef3c7;border:1px solid #fde68a;
                    border-left:4px solid {_AMBER};padding:.85rem 1.1rem;
                    border-radius:8px;margin:.8rem 0 1.2rem 0;">
          <strong>⚠️ No LIVE Gold schema for this dataset</strong> — pipeline is
          falling back to Phase-15 Bronze-as-Gold mode (1:1 cast). For the full
          DV2 Hub/Sat/Link Silver + canonical Gold, design Gold first.
          <div style="font-size:.85rem;color:#78350f;margin-top:.45rem;">
            <a href="/Data_Model_Designer" style="color:{_NAVY};font-weight:600;">→ Open Data Model Designer</a>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

# ---------------------------------------------------------------------------
# ARTIFACT PREVIEWS (DDL, dbt, DAG, GX) — collapsible.
# ---------------------------------------------------------------------------

st.markdown("### 📄 Artifact previews")

if proposal.get("bronze_ddl_overflow"):
    with st.expander("🥉 Bronze DDL (with `_variant_overflow` safety column)", expanded=False):
        st.code(str(proposal["bronze_ddl_overflow"]), language="sql")
        st.info(
            "The `_variant_overflow VARIANT` column captures any unexpected "
            "vendor-supplied columns as JSON. Silver and Gold transforms "
            "IGNORE it — overflow data NEVER propagates to operational "
            "databases without an explicit handshake. Pending review queue "
            "below."
        )

with st.expander("🥇 Gold DDL", expanded=False):
    st.code(str(proposal["gold_ddl"]), language="sql")

if silver_models:
    with st.expander(f"🥈 Silver DV2 dbt models ({len(silver_models)} files)", expanded=False):
        for filename, sql in silver_models.items():
            st.markdown(f"**`{filename}`**")
            st.code(sql, language="sql")
else:
    with st.expander("🥈 Silver dbt model (legacy 1:1 cast)", expanded=False):
        st.code(str(proposal.get("silver_dbt_sql") or ""), language="sql")

with st.expander("🥇 Gold dbt model", expanded=False):
    st.code(str(proposal["gold_dbt_sql"]), language="sql")

with st.expander("🪂 Airflow DAG", expanded=False):
    st.code(str(proposal["airflow_dag_py"]), language="python")

with st.expander("✅ GX expectation suite", expanded=False):
    suite = proposal["gx_suite"]
    st.markdown(
        f"**Suite name:** `{suite.get('suite_name')}`  \n"
        f"**Expectations:** {suite.get('expectation_count')}  \n"
        f"**Dimensions:** {', '.join(suite.get('dq_dimensions', []))}"
    )
    st.dataframe(
        pd.DataFrame(
            [
                {
                    "Type": e.get("expectation_type"),
                    "Column": e.get("column"),
                    "Dimension": e.get("dimension"),
                    "Severity": e.get("severity"),
                    "Args": json.dumps(e.get("kwargs", {}), default=str)[:60],
                    "Rationale": e.get("rationale", "")[:80],
                }
                for e in suite.get("expectations", [])
            ]
        ),
        use_container_width=True,
        hide_index=True,
    )


# ---------------------------------------------------------------------------
# DEPLOY ACTION — persist + approve_and_deploy in one click.
# ---------------------------------------------------------------------------

st.markdown("---")
st.markdown("### 🚢 Deploy")

c1, c2, c3 = st.columns([1, 1, 4])
with c1:
    deploy_clicked = st.button(
        "✅ Approve & Deploy",
        type="primary",
        use_container_width=True,
        help="Persists the proposal as PENDING_REVIEW, then immediately approves "
        "and emits all 5 artifacts (Gold DDL, Silver dbt, Gold dbt, Airflow DAG, "
        "GX suite). Status flips to LIVE.",
    )
with c2:
    save_draft_clicked = st.button(
        "💾 Save as DRAFT",
        use_container_width=True,
        help="Persist as PENDING_REVIEW only — does not deploy artifacts. "
        "Use when you want to come back to review later.",
    )

deploy_notes = st.text_area(
    "Deploy notes (audit trail)",
    placeholder="Optional — will be appended to the audit log.",
    height=70,
)

if save_draft_clicked:
    with _warehouse(readonly=False) as wh:
        try:
            instance_id = persist_proposal(
                warehouse=wh,
                proposal=proposal,
                actor=f"ui:{selected_client}",
                notes=deploy_notes,
            )
            st.success(f"Saved as PENDING_REVIEW. Instance id: `{instance_id}`")
            st.session_state.pop(PROPOSAL_KEY, None)
        except Exception as exc:
            st.error(f"Save failed: {type(exc).__name__}: {exc}")
            st.exception(exc)

if deploy_clicked:
    with (
        _warehouse(readonly=False) as wh,
        st.spinner("Persisting proposal + emitting 5 artifacts..."),
    ):
        try:
            instance_id = persist_proposal(
                warehouse=wh,
                proposal=proposal,
                actor=f"ui:{selected_client}",
                notes=deploy_notes,
            )
            deploy_result = approve_and_deploy(
                warehouse=wh,
                instance_id=instance_id,
                proposal=proposal,
                actor=f"ui:{selected_client}",
                notes=deploy_notes,
            )
            st.session_state.pop(PROPOSAL_KEY, None)
            st.success(
                f"🎉 LIVE — instance `{instance_id[:8]}…`. "
                f"GX suite `{deploy_result['gx_suite_id'][:8]}…` registered."
            )
            st.markdown("**Artifacts emitted:**")
            for k, v in deploy_result["artifact_paths"].items():
                st.markdown(f"- `{k}` → `{v}`")
        except Exception as exc:
            st.error(f"Deploy failed: {type(exc).__name__}: {exc}")
            st.exception(exc)


# ---------------------------------------------------------------------------
# Phase 15.7 — Overflow review panel
# ---------------------------------------------------------------------------

st.markdown("---")
st.markdown("## 🚨 Pending overflow columns (HITL handshake required)")

from datalink.agents.pipeline_architect import list_pending_overflow  # noqa: E402

with _warehouse(readonly=True) as wh:
    pending_overflow = list_pending_overflow(wh, client_id=selected_client)

if not pending_overflow:
    st.success(
        f"No pending overflow columns for **{selected_client}**. Every "
        "incoming column matches the agreed Bronze contract."
    )
else:
    st.warning(
        f"⚠️ **{len(pending_overflow)} unexpected column(s)** seen in "
        f"incoming Bronze batches that are NOT in the agreed contract. "
        f"Data is preserved in `_variant_overflow` but BLOCKED from "
        f"reaching downstream operational DBs until you handshake."
    )
    overflow_rows = []
    for o in pending_overflow:
        overflow_rows.append(
            {
                "Dataset": o.get("dataset_code"),
                "Unexpected column": o.get("column_name"),
                "Inferred type": o.get("inferred_logical_type"),
                "Occurrences": o.get("occurrence_count"),
                "First seen": o.get("first_seen_at"),
                "Last seen": o.get("last_seen_at"),
                "Status": o.get("status"),
                "Sample values": (o.get("sample_values") or "")[:80],
            }
        )
    st.dataframe(pd.DataFrame(overflow_rows), use_container_width=True, hide_index=True)
    st.caption(
        "To handshake an overflow column: open Data Model Designer → re-run "
        "the AI agent (or Manual edit) → add the column to Gold + Bronze contract → "
        "approve. The overflow row will auto-flip to RESOLVED on next batch."
    )

# ---------------------------------------------------------------------------
# DEPLOYED INSTANCES PANEL — operator monitoring at the bottom of the page.
# ---------------------------------------------------------------------------

st.markdown("---")
st.markdown(f"## 🗂 All instances for `{selected_client}`")

with _warehouse(readonly=True) as wh:
    client_instances = list_client_instances(wh, client_id=selected_client)

if not client_instances:
    st.info(
        f"No pipeline instances for **{selected_client}** yet. Use the form above to propose one."
    )
else:
    inst_rows = []
    for inst in client_instances:
        status = str(inst.get("status") or "")
        status_pill_class = {
            "LIVE": "arch-pill-live",
            "PENDING_REVIEW": "arch-pill-pending",
            "ARCHIVED": "arch-pill-archived",
            "PAUSED": "arch-pill-pending",
        }.get(status, "arch-pill-pending")
        inst_rows.append(
            {
                "Dataset": inst.get("dataset_code"),
                "Status": status,
                "Anchor": inst.get("bronze_anchor"),
                "Gold table": f"{inst.get('gold_schema')}.{inst.get('gold_table')}",
                "Schedule": inst.get("schedule_cron"),
                "Deviations": inst.get("deviation_count"),
                "DAG": inst.get("dag_uri") or "—",
                "Created": inst.get("created_at"),
                "Deployed": inst.get("deployed_at"),
                "Instance ID": str(inst.get("instance_id"))[:8] + "…",
            }
        )
    st.dataframe(pd.DataFrame(inst_rows), use_container_width=True, hide_index=True)
