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
    PipelinePrerequisitesError,
    approve_and_deploy,
    check_pipeline_prerequisites,
    list_client_instances,
    list_dataset_codes,
    persist_proposal,
    propose_pipeline,
)
from datalink.config.loader import load_settings  # noqa: E402
from datalink.proposals import store as proposal_store  # noqa: E402  Phase 16.1
from datalink.proposals.store import ProposalStatus  # noqa: E402
from datalink.quality.control import CONTROL_SCHEMA, create_control_tables  # noqa: E402
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

      /* Phase 16.1 — saved-proposal banner */
      .saved-prop-card {{background:linear-gradient(90deg,#fffbeb,#fef3c7);
                         border:1px solid #fbbf24;border-left:5px solid #d97706;
                         border-radius:8px;padding:1rem 1.2rem;margin:.8rem 0 1.2rem 0;
                         box-shadow:0 1px 4px rgba(0,0,0,.06);}}
      .saved-prop-card.approved {{background:linear-gradient(90deg,#f0fdf4,#dcfce7);
                                  border-color:#86efac;border-left-color:#15803d;}}
      .saved-prop-card .saved-prop-headline {{font-size:1rem;font-weight:700;
                                               color:#0a1a3e;margin-bottom:.25rem;}}
      .saved-prop-card .saved-prop-meta {{font-size:.85rem;color:#475569;}}
      .saved-prop-card code {{background:#fff;padding:1px 4px;border-radius:3px;
                              font-size:.8rem;}}
      .tag-pill {{display:inline-block;padding:.1rem .5rem;border-radius:10px;
                  font-size:.7rem;font-weight:600;color:#fff;
                  margin-right:.25rem;margin-bottom:.15rem;}}
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

# Phase 17.1 — special-case the GLOBAL_CORP pseudo-client. This is where AI
# spends tokens ONCE per dataset to build the canonical pipeline template.
# All real clients clone this — never spend tokens on AI again.
# Legacy alias ``__global__`` kept temporarily for in-flight session links.
GLOBAL_CLIENT = "GLOBAL_CORP"
GLOBAL_CLIENT_LEGACY = "__global__"
_is_global_build_mode = selected_client in (GLOBAL_CLIENT, GLOBAL_CLIENT_LEGACY)

# Phase 16.6 — when no client is picked (default), render an "all clients"
# overview and stop. Deploy / propose forms only render after a real client
# is chosen from the dropdown.
if selected_client is None:
    with _warehouse(readonly=True) as wh:
        all_instances = list_client_instances(wh)
        catalog_datasets = list_dataset_codes(wh)

    # Global pipelines (templates) — surface them prominently
    global_pipelines: list[dict[str, Any]] = []
    try:
        from datalink.templates import store as _ts

        global_pipelines = _ts.list_globals()
    except Exception:
        global_pipelines = []

    st.markdown("### 🌍 Build a GLOBAL pipeline (template)")
    st.caption(
        "**Token-efficient design pattern.** AI runs ONCE per dataset to build "
        "a canonical pipeline. All real clients clone it instantly — zero "
        "LLM cost. Use this for any new dataset before onboarding clients."
    )
    gcol1, gcol2 = st.columns([1, 4])
    with gcol1:
        # st.page_link rejects query-string URLs on internal paths. Use a
        # plain anchor so we can pass ?client=GLOBAL_CORP deep-link.
        st.markdown(
            '<a href="/Pipeline_Architect?client=GLOBAL_CORP" target="_self" '
            'style="display:inline-block;background:#0a1a3e;color:#fff;'
            "padding:.6rem 1.2rem;border-radius:8px;text-decoration:none;"
            'font-weight:700;width:100%;text-align:center;">'
            "🌍 Open Global Builder →</a>",
            unsafe_allow_html=True,
        )
    with gcol2:
        if global_pipelines:
            st.success(
                f"📦 **{len(global_pipelines)} global pipeline(s) published**: "
                + ", ".join(
                    f"`{g.get('dataset_code')}` v{g.get('pipeline_version', '?')}"
                    for g in global_pipelines
                    if g.get("pipeline_version")
                )
                + ". Real clients can clone these in one click."
            )
        else:
            st.info(
                "No global pipeline templates published yet. **Build one** "
                "with the button to the left, then onboard real clients via clone."
            )

    st.markdown("---")
    st.info(
        "🌐 **All-clients view.** Pick a real client from the dropdown above "
        "(or the URL `?client=`) to **propose / deploy / clone** a pipeline "
        "for that tenant. The table below shows every pipeline instance "
        "across the platform."
    )
    cols = st.columns(3)
    with cols[0]:
        st.metric("Bronze datasets", len(catalog_datasets))
    with cols[1]:
        st.metric("LIVE pipelines", sum(1 for i in all_instances if i.get("status") == "LIVE"))
    with cols[2]:
        st.metric(
            "Onboarded clients",
            len({i.get("client_id") for i in all_instances if i.get("client_id")}),
        )

    st.markdown("### 🏛 All pipelines (every client, every dataset)")
    if all_instances:
        df = pd.DataFrame(
            [
                {
                    "Client": i.get("client_id"),
                    "Dataset": i.get("dataset_code"),
                    "Status": i.get("status"),
                    "Anchor": i.get("bronze_anchor"),
                    "Gold table": f"{i.get('gold_schema')}.{i.get('gold_table')}",
                    "Schedule": i.get("schedule_cron"),
                    "Deployed": str(i.get("deployed_at") or "—")[:19],
                }
                for i in all_instances
            ]
        )
        st.dataframe(df, use_container_width=True, hide_index=True)
    else:
        st.caption("No pipelines deployed yet platform-wide.")
    st.stop()


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


# ═════════════════════════════════════════════════════════════════════════════
# Phase 16.10 — Published Global Templates grid + Clone action
#
# This is the "shopping aisle" for new clients: every dataset that has been
# promoted to global (Silver+Gold+Pipeline LIVE under scope_owner='GLOBAL_CORP')
# shows up here with a one-click [📦 Clone to Client] action. Zero LLM tokens.
# ═════════════════════════════════════════════════════════════════════════════
from datalink.templates import store as _tpl_store_grid  # noqa: E402

st.markdown("### 🌍 Published Global Templates")
st.caption(
    "Canonical, AI-authored once, cloned per client at zero LLM cost. "
    "Each row is a fully published template (Silver schema · Gold schema · "
    "Pipeline). Pick a row and click **Clone to Client** to instantiate "
    "for a specific tenant."
)

try:
    _published_globals = _tpl_store_grid.list_globals()
except Exception as _exc:
    _published_globals = []
    st.warning(f"Could not load global templates: {_exc}")

if not _published_globals:
    st.info(
        "📭 No global templates published yet. **How to publish one:**\n\n"
        "1. Switch the client picker (top-left of page) to "
        "`🌍 GLOBAL_CORP  (build template — AI runs ONCE per dataset)`\n"
        "2. Pick a dataset and run the AI propose flow once (Silver → Gold "
        "→ Pipeline)\n"
        "3. Approve & Deploy — the artifacts auto-publish to global\n\n"
        "Future clients will then see a `📦 Clone from Global` option here "
        "and skip the LLM entirely."
    )
else:
    # Count clones from client_pipeline_instances.cloned_from_template_id
    with _warehouse(readonly=True) as _wh_clones:
        try:
            _clone_rows = list(
                _wh_clones.query(
                    f"SELECT cloned_from_template_id AS tid, COUNT(*) AS c "
                    f"FROM {CONTROL_SCHEMA}.client_pipeline_instances "
                    f"WHERE cloned_from_template_id IS NOT NULL "
                    f"GROUP BY cloned_from_template_id"
                )
            )
            _clone_count_by_tid = {str(r["tid"]): int(r["c"]) for r in _clone_rows}
        except Exception:
            _clone_count_by_tid = {}

    grid_rows = []
    for g in _published_globals:
        tid = str(g.get("pipeline_template_id") or "")
        grid_rows.append(
            {
                "Dataset": g.get("dataset_code"),
                "Silver v": f"v{g.get('silver_version')}" if g.get("silver_version") else "—",
                "Gold v": f"v{g.get('gold_version')}" if g.get("gold_version") else "—",
                "Pipeline v": f"v{g.get('pipeline_version')}" if g.get("pipeline_version") else "—",
                "Anchor": g.get("bronze_anchor") or "—",
                "Schedule": g.get("schedule_cron") or "—",
                "🔁 Clones": _clone_count_by_tid.get(tid, 0),
                "Template ID": (tid[:8] + "…") if tid else "—",
            }
        )
    st.dataframe(pd.DataFrame(grid_rows), use_container_width=True, hide_index=True)

    # Clone-to-client picker
    _clonable = [g for g in _published_globals if g.get("pipeline_template_id")]
    if _clonable:
        st.markdown("##### 📦 Clone a global template to a client")
        cc1, cc2, cc3 = st.columns([2, 2, 1])
        with cc1:
            _clone_dataset = st.selectbox(
                "Dataset",
                options=[str(g.get("dataset_code")) for g in _clonable],
                key="global_clone_dataset_picker",
                help="Pick the published global template to clone.",
            )
        with cc2:
            _clone_target_client = st.text_input(
                "Target client",
                placeholder=selected_client if selected_client and selected_client not in (GLOBAL_CLIENT, GLOBAL_CLIENT_LEGACY) else "aetna",
                key="global_clone_target_client",
                help="Lower-case client_id. Bronze/Silver/Gold schemas auto-create.",
            )
        with cc3:
            st.write("")  # vertical alignment
            _clone_now = st.button(
                "📦 Clone now",
                type="primary",
                use_container_width=True,
                disabled=not (_clone_target_client and _clone_target_client.strip()),
            )
        if _clone_now and _clone_target_client.strip():
            try:
                with _warehouse(readonly=False) as _wh_clone:
                    result = _tpl_store_grid.clone_to_client(
                        warehouse=_wh_clone,
                        dataset_code=_clone_dataset,
                        target_client_id=_clone_target_client.strip().lower(),
                        actor=f"ui:pipeline_architect:{_clone_target_client.strip().lower()}",
                    )
                st.success(
                    f"✅ Cloned `{_clone_dataset}` global → "
                    f"`{_clone_target_client.strip().lower()}`. "
                    f"New instance: `{str(result.get('new_instance_id', ''))[:8]}…`. "
                    f"Status: DRAFT — approve & deploy below."
                )
                st.rerun()
            except Exception as _exc:
                st.error(f"Clone failed: {type(_exc).__name__}: {_exc}")
    else:
        st.caption(
            "_(No clonable templates yet — all rows above are missing the pipeline component. "
            "Deploy + publish a pipeline against `GLOBAL_CORP` to make a row clonable.)_"
        )


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

# ═════════════════════════════════════════════════════════════════════════════
# Phase 16.7 — GLOBAL TEMPLATE PRIORITY PICKER
#
# Architecture: every dataset has a CANONICAL global template (Bronze + Silver
# + Gold + DAG + GX). Operators clone from global by default — zero LLM tokens,
# 70-80% of pipelines are identical across clients. AI proposal is opt-in
# (only when the operator explicitly selects "AI Construct" because they need
# a brand-new pipeline that diverges from global).
# ═════════════════════════════════════════════════════════════════════════════

from datalink.templates import store as template_store  # noqa: E402

_global_status = template_store.has_global(selected_dataset_code)
_has_global_pipeline = _global_status.get("pipeline", False)
_has_global_silver = _global_status.get("silver", False)
_has_global_gold = _global_status.get("gold", False)

st.markdown("---")
st.markdown("### 🧬 Choose source — **Global template is the priority path**")

# Source picker: clone-global is default + first option when global exists.
SOURCE_CLONE = "📦 Clone from Global (no LLM tokens)"
SOURCE_AI = "🤖 AI Construct (LLM, ~$0.002 per propose)"
SOURCE_MANUAL = "✏️ Manual / Import (copy a spec)"

source_options = []
if _has_global_pipeline:
    source_options.append(SOURCE_CLONE)
source_options.append(SOURCE_AI)
source_options.append(SOURCE_MANUAL)

source_choice = st.radio(
    "Source",
    options=source_options,
    index=0,  # default = first option (clone if available, AI otherwise)
    help=(
        "Clone-from-global is FREE — instantiates the canonical pipeline "
        "for this client. AI Construct spends LLM tokens to design a new "
        "version. Manual lets you paste a spec or DDL."
    ),
)


# Show global template status pills.
# Phase 16.9 — wording clarified so it doesn't conflict with the Upstream
# Readiness panel below. This panel is about the GLOBAL TEMPLATE
# (canonical clone source); the Upstream Readiness panel is about whether
# Silver/Gold SCHEMAS exist for the dataset (which they may, regardless
# of global-template status).
def _pill(label: str, ok: bool) -> str:
    cls = "arch-pill-live" if ok else "arch-pill-dev"
    return f'<span class="arch-pill {cls}">{label}</span>'


# Three components of a "complete global template": Silver schema + Gold
# schema + Pipeline blob. Silver+Gold may already be published (any LIVE
# Silver/Gold authored in DMD defaults to scope_owner='GLOBAL_CORP'); the
# pipeline part requires deploying once + publishing to global.
all_three_published = _has_global_silver and _has_global_gold and _has_global_pipeline
some_published = _has_global_silver or _has_global_gold or _has_global_pipeline

if all_three_published:
    bg, border, header_icon = "#dcfce7", "#15803d", "✅"
    headline = "Global template COMPLETE for"
    subtext = (
        "All three components published. Real clients see "
        "<strong>📦 Clone from Global</strong> as their default — "
        "zero LLM cost to onboard."
    )
elif some_published:
    bg, border, header_icon = "#fef3c7", "#b45309", "🟡"
    headline = "Global template PARTIAL for"
    missing = []
    if not _has_global_silver:
        missing.append("Silver schema")
    if not _has_global_gold:
        missing.append("Gold schema")
    if not _has_global_pipeline:
        missing.append("Pipeline (deploy + publish)")
    subtext = (
        f"Still needed: <strong>{', '.join(missing)}</strong>. "
        f"Switch to <strong>🌍 Global Builder</strong> mode "
        f"(URL <code>?client=GLOBAL_CORP</code>) to finish the template "
        f"with one AI deploy."
    )
else:
    bg, border, header_icon = "#fee2e2", "#991b1b", "🚫"
    headline = "No global template yet for"
    subtext = (
        "Author Silver+Gold (cross-tenant designs) in <strong>Data Model "
        "Designer</strong>, then run the AI pipeline build ONCE in "
        "<strong>🌍 Global Builder</strong> (<code>?client=GLOBAL_CORP</code>). "
        "After that, all real clients clone instantly."
    )

st.markdown(
    f"""
    <div style="background:{bg};border:1px solid {border};
                border-left:5px solid {border};
                border-radius:8px;padding:.8rem 1.2rem;margin:.5rem 0 1rem 0;">
      <div style="font-size:.95rem;font-weight:700;color:#0a1a3e;margin-bottom:.4rem;">
        {header_icon} {headline} <code>{selected_dataset_code}</code>
      </div>
      <div>
        {_pill("Silver schema", _has_global_silver)}
        {_pill("Gold schema", _has_global_gold)}
        {_pill("Pipeline template", _has_global_pipeline)}
      </div>
      <div style="margin-top:.5rem;font-size:.85rem;color:#475569;">
        {subtext}
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)

if source_choice == SOURCE_CLONE and _has_global_pipeline:
    g = template_store.get_global(selected_dataset_code)
    pipe = g.get("pipeline", {})
    st.markdown(
        f"**Will clone from:** `{pipe.get('template_id', '?')}` "
        f"(v{pipe.get('version', '?')}) · "
        f"{g.get('artifact_blob_count', 0)} stored artifact blobs · "
        f"Silver v{(g.get('silver') or {}).get('version', '?')} · "
        f"Gold v{(g.get('gold') or {}).get('version', '?')}"
    )
    if st.button(
        f"📦 Clone Global → `{selected_client}` (instantiate, no LLM call)",
        type="primary",
        use_container_width=False,
    ):
        try:
            with st.spinner("Cloning global template..."):
                result = template_store.clone_to_client(
                    dataset_code=selected_dataset_code,
                    target_client_id=selected_client,
                    actor=f"ui:{selected_client}",
                    notes="Cloned via Pipeline Architect's Global priority path",
                )
            st.success(
                f"📦 **Cloned successfully** — DRAFT instance "
                f"`{result['pipeline_instance_id'][:8]}…` created. "
                f"{len(result['artifacts_written'])} artifact files written. "
                f"Use **Approve & Deploy** below to materialize physical Snowflake tables."
            )
            with st.expander("Cloned artifacts"):
                for f in result["artifacts_written"]:
                    st.code(f, language="text")
            st.info(
                "💡 **No LLM tokens spent.** All artifacts came from the "
                "canonical global template — edit them in `dbt/models/...` "
                "or per-client overrides before deploy if needed."
            )
        except Exception as exc:
            st.error(f"Clone failed: {type(exc).__name__}: {exc}")

elif source_choice == SOURCE_MANUAL:
    st.info(
        "✏️ **Manual / Import mode.** Paste a Bronze/Silver/Gold spec into "
        "Data Model Designer's Import tab, then return here to deploy. "
        "Or: edit the cloned global artifacts after a Clone action."
    )
    st.markdown(
        f'<a href="/Data_Model_Designer?client={selected_client}&dataset='
        f'{selected_dataset_code}" target="_self">'
        f"🥇 → Open Data Model Designer (Import / Manual)</a>",
        unsafe_allow_html=True,
    )

# When source_choice == SOURCE_AI, the legacy Propose form below is the path —
# user clicks Propose pipeline button as before (LLM call). Otherwise the form
# is hidden via the gate.
_use_ai_propose = source_choice == SOURCE_AI

st.markdown("---")

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
    if _is_global_build_mode:
        st.info(
            f"🌍 **Global Builder mode** — AI will build the canonical "
            f"`{selected_dataset_display}` pipeline ONCE. Schemas: "
            f"`BRONZE_GLOBAL` / `SILVER_GLOBAL` / `GOLD_GLOBAL`. "
            f"DAG deploys **PAUSED** (no data ever flows here). After deploy, "
            f"the pipeline auto-publishes to global so real clients can clone."
        )
    else:
        st.info(
            f"No peer client has the **{selected_dataset_display}** dataset live yet. "
            f"This deployment will be the **first instance** — future clients can clone from it."
        )


# ---------------------------------------------------------------------------
# Phase 16.1 (Item 1.C + 1.D) — UPSTREAM PREREQUISITES GATE
#
# Pipeline Architect is a CONSUMER of Data Model Designer's LIVE Silver and
# Gold schemas. It REFUSES to propose unless both upstream schemas are LIVE
# in the registry (or the operator opts into "Silver-stop" mode).
#
# Bronze catalog is the source for Data Model Designer, NOT for us.
# ---------------------------------------------------------------------------

st.markdown("---")
st.markdown("### 📋 Upstream readiness")

with _warehouse(readonly=True) as wh:
    try:
        prereqs = check_pipeline_prerequisites(wh, selected_dataset_code)
    except Exception as exc:
        st.error(f"Could not check prerequisites: {exc}")
        prereqs = {
            "ready_for_propose": False,
            "ready_for_silver_stop": False,
            "silver_live": False,
            "gold_live": False,
            "missing": ["silver", "gold"],
            "next_steps": [str(exc)],
        }


def _ready_pill(label: str, ok: bool, version: int | None = None) -> str:
    if ok:
        v = f" v{version}" if version else ""
        return f'<span class="arch-pill arch-pill-live">{label}-LIVE{v}</span>'
    return f'<span class="arch-pill arch-pill-dev">{label}-MISSING</span>'


# Render the readiness card. Fix the colors per state so it's unmistakeable.
_silver_ok = bool(prereqs["silver_live"])
_gold_ok = bool(prereqs["gold_live"])
if _silver_ok and _gold_ok:
    _state_color = "#dcfce7"  # emerald wash
    _state_border = "#15803d"
    _state_icon = "✅"
    _state_text = (
        "<strong>Both schemas LIVE</strong> for "
        f"<code>{selected_dataset_display}</code>. Pipeline Architect can "
        "consume them — Propose enabled."
    )
elif _silver_ok and not _gold_ok:
    _state_color = "#fef3c7"  # amber wash
    _state_border = "#b45309"
    _state_icon = "⚠️"
    _state_text = (
        f"<strong>Silver LIVE, Gold MISSING</strong> for <code>{selected_dataset_display}</code>. "
        "Author Gold in Data Model Designer for full Bronze→Silver→Gold, OR "
        "tick <em>Silver-stop pipeline</em> below to deploy without Gold "
        "materialization."
    )
elif not _silver_ok and _gold_ok:
    _state_color = "#fee2e2"  # red wash
    _state_border = "#991b1b"
    _state_icon = "🚫"
    _state_text = (
        f"<strong>Gold LIVE but Silver MISSING</strong> for <code>{selected_dataset_display}</code>. "
        "Gold without Silver isn't supported — author Silver in Data Model "
        "Designer first. Pipeline Architect is REFUSING to propose."
    )
else:
    _state_color = "#fee2e2"  # red wash
    _state_border = "#991b1b"
    _state_icon = "🚫"
    _state_text = (
        f"<strong>Neither Silver nor Gold is LIVE</strong> for "
        f"<code>{selected_dataset_display}</code>. Open Data Model Designer "
        "to author both schemas first. Pipeline Architect is REFUSING to "
        "propose."
    )

_silver_v = prereqs.get("silver_version")
_gold_v = prereqs.get("gold_version")
st.markdown(
    f"""
    <div style="background:{_state_color};border:1px solid {_state_border};
                border-left:5px solid {_state_border};border-radius:8px;
                padding:1rem 1.2rem;margin:.5rem 0 1rem 0;">
      <div style="font-size:1rem;font-weight:700;color:#0a1a3e;margin-bottom:.4rem;">
        {_state_icon} Upstream readiness for <code>{selected_dataset_code}</code>
      </div>
      <div style="margin-bottom:.5rem;">
        {_ready_pill("Silver", _silver_ok, _silver_v)}
        {_ready_pill("Gold", _gold_ok, _gold_v)}
        {f'<span class="arch-pill arch-pill-flat">Pattern: {prereqs["silver_pattern"]}</span>' if prereqs.get("silver_pattern") else ""}
      </div>
      <div style="font-size:.9rem;color:#1f2937;">{_state_text}</div>
    </div>
    """,
    unsafe_allow_html=True,
)

# Next steps + deep-link when something's missing.
# Phase 16.1 (Jatin's UX feedback): make the deep-link the *first* and most
# prominent action — operator shouldn't have to scan side panel + click.
if prereqs["missing"]:
    _dmd_url = f"/Data_Model_Designer?client={selected_client}&dataset={selected_dataset_code}"
    # Big call-to-action card with a direct link button + concrete step list.
    st.markdown(
        f"""
        <div style="background:#1e3a8a;border-radius:8px;padding:1rem 1.2rem;
                    margin:.5rem 0;color:#fff;display:flex;
                    flex-direction:column;gap:.5rem;">
          <div style="font-size:1.05rem;font-weight:700;">
            🥇 Author the missing schema(s) in Data Model Designer
          </div>
          <div style="font-size:.88rem;color:#dbeafe;">
            One click takes you there with <code>{selected_client}</code> /
            <code>{selected_dataset_code}</code> already selected.
          </div>
          <a href="{_dmd_url}" target="_self" style="display:inline-block;
                background:#fbbf24;color:#0a1a3e;font-weight:700;
                padding:.5rem 1rem;border-radius:6px;text-decoration:none;
                width:fit-content;margin-top:.25rem;">
            🚀 Open Data Model Designer →
          </a>
        </div>
        """,
        unsafe_allow_html=True,
    )
    with st.expander("📝 Step-by-step next-steps", expanded=False):
        for step in prereqs["next_steps"]:
            st.markdown(f"- {step}")

# Silver-stop opt-in checkbox — ONLY shown when prerequisites would refuse
# the propose AND Silver is LIVE (Gold missing). For other missing-states
# Silver-stop isn't a meaningful workaround.
allow_silver_stop = False
if not prereqs["ready_for_propose"] and prereqs["ready_for_silver_stop"]:
    allow_silver_stop = st.checkbox(
        "🟡 Deploy a Silver-stop pipeline (Bronze→Silver only, no Gold)",
        value=False,
        help="Override: emit a pipeline that materializes Silver but stops "
        "before Gold. Use when business consumers can wait on Gold but "
        "Silver-cleansed data is needed downstream now.",
    )


# ---------------------------------------------------------------------------
# PROPOSAL ACTION — Phase 16.1 saved-proposal pattern.
#
#   1. On page load, look up any saved proposal for this (client, dataset)
#      scope. If found, show a banner with action buttons.
#   2. "Use Saved" → load payload from CONTROL.agent_proposals, no LLM call.
#   3. "Re-propose" → confirmation modal showing token cost, then LLM call.
#   4. "Discard"   → mark REJECTED, fall through to fresh-propose UI.
#   5. Approval still flows through approve_and_deploy below; we ALSO call
#      proposal_store.approve() to link the proposal to its instance_id.
# ---------------------------------------------------------------------------

PROPOSAL_KEY = f"p15_proposal_{selected_client}_{selected_dataset_code}"
PROPOSAL_ID_KEY = f"p15_proposal_id_{selected_client}_{selected_dataset_code}"
SCOPE_TYPE = "client_dataset"
SCOPE_KEY = f"{selected_client}:{selected_dataset_code}"


def _save_proposal_to_store(proposal_dict: dict, *, parent_proposal_id: str | None = None) -> str:
    """Persist a freshly-generated proposal into CONTROL.agent_proposals.

    Returns the proposal_id. The agent today exposes only a flat
    ``tokens_used`` total — we estimate an 80/20 input/output split for
    cost calculation. When the agent is upgraded to expose split tokens,
    swap the split here.
    """
    total = int(proposal_dict.get("tokens_used", 0))
    # Anthropic typical ratio for our prompts ≈ 80% input / 20% output.
    # TODO: have propose_pipeline return split tokens directly.
    prompt_t = int(total * 0.80)
    completion_t = total - prompt_t
    saved = proposal_store.save_proposal(
        agent_type="pipeline_architect",
        scope_type=SCOPE_TYPE,
        scope_key=SCOPE_KEY,
        payload=proposal_dict,
        agent_input={
            "client_id": selected_client,
            "dataset_code": selected_dataset_code,
            "bronze_anchor": bronze_anchor,
            "decision_mode": decision_mode,
            "schedule_cron": schedule_cron,
        },
        prompt_tokens=prompt_t,
        completion_tokens=completion_t,
        model_id=str(proposal_dict.get("proposer_model", "claude-haiku-4.5")),
        latency_ms=int(proposal_dict.get("duration_ms", 0)),
        created_by=f"ui:{selected_client}",
    )
    return saved.proposal_id


def _run_propose() -> None:
    """Invoke the LLM, save to store + session_state. Shared by Propose
    and Re-propose actions.

    On success: stashes a "just proposed" message in session_state and
    triggers ``st.rerun()`` so the saved-proposal banner + history table
    refresh automatically (no manual hard-reload needed).
    """
    with (
        st.spinner(
            f"Agent proposing {selected_dataset_display} pipeline for "
            f"{selected_client} (anchor={bronze_anchor}, decision={decision_mode})..."
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
                allow_silver_stop=allow_silver_stop,
            )
            st.session_state[PROPOSAL_KEY] = proposal
            # Phase 16.1 — persist to CONTROL.agent_proposals so
            # re-clicks don't burn tokens.
            try:
                proposal_id = _save_proposal_to_store(proposal)
                st.session_state[PROPOSAL_ID_KEY] = proposal_id
            except Exception as save_exc:
                # Saving to the store is best-effort. If it fails, the
                # propose still succeeded — log and continue. The user
                # can still deploy from session_state.
                st.warning(
                    f"⚠️ Saved-proposal store unavailable: {save_exc}. "
                    "Proposal is still usable in this session."
                )
            # Stash success message — st.success doesn't survive rerun, but
            # session_state does. We pick it up after rerun and show as toast.
            st.session_state[f"_propose_toast_{SCOPE_KEY}"] = (
                f"Proposal ready — decision={proposal['decision_mode']}, "
                f"{proposal['resolved_field_count']} Gold cols, "
                f"{proposal['gx_suite']['expectation_count']} GX expectations, "
                f"{proposal['tokens_used']} tokens, "
                f"{proposal['duration_ms']}ms."
            )
        except PipelinePrerequisitesError as exc:
            # Caught at the bridge — translate into the readiness UI.
            st.error(
                f"🚫 Cannot propose pipeline for `{exc.dataset_code}`: "
                f"upstream Silver/Gold not LIVE (missing: {exc.missing}). "
                f"See **Upstream readiness** card above for next steps."
            )
            return
        except Exception as exc:
            st.error(f"Agent failed: {type(exc).__name__}: {exc}")
            st.exception(exc)
            return  # don't rerun on failure — let the user see the error
    # Trigger immediate rerun so the saved-proposal banner + history pick up
    # the new row. Outside the spinner so the spinner closes first.
    st.rerun()


# Phase 16.1 — pick up any "just proposed" toast stashed by _run_propose()
# before its st.rerun(). Showing here (top of page, post-rerun) means the user
# sees the success ack without needing a manual refresh.
_toast_key = f"_propose_toast_{SCOPE_KEY}"
if _toast_key in st.session_state:
    st.toast(st.session_state.pop(_toast_key), icon="✅")

# Phase 16.1 — Saved-proposal banner. Renders BEFORE the Propose button.
# Uses `find_active` which returns the latest DRAFT or APPROVED for the scope.
try:
    saved_active = proposal_store.find_active(scope_type=SCOPE_TYPE, scope_key=SCOPE_KEY)
except Exception:
    saved_active = None  # fall back to unbanner-ed flow if store unavailable

if saved_active is not None:
    is_approved = saved_active.status == ProposalStatus.APPROVED
    age_str = saved_active.created_at.strftime("%Y-%m-%d %H:%M UTC")
    tag_html = "".join(
        f'<span class="tag-pill" style="background:#475569">{t}</span>' for t in saved_active.tags
    )
    headline_label = "✅ Approved proposal" if is_approved else "📦 Saved proposal"
    extra_meta = ""
    if is_approved and saved_active.linked_artifact_id:
        extra_meta = (
            f"<br>Linked: <code>{saved_active.linked_artifact_type}:"
            f"{saved_active.linked_artifact_id[:8]}…</code>"
        )
    st.markdown(
        f"""
        <div class="saved-prop-card {"approved" if is_approved else ""}">
          <div class="saved-prop-headline">
            {headline_label} — v{saved_active.version}
            <span style="font-size:.78rem;font-weight:500;color:#64748b;margin-left:.5rem;">
              {age_str} · {saved_active.total_tokens:,} tokens · ${saved_active.estimated_cost_usd:.4f}
              · by {saved_active.created_by}
              · status: {saved_active.status.value}
            </span>
          </div>
          <div class="saved-prop-meta">
            Decision = <code>{saved_active.proposal_payload.get("decision_mode", "?")}</code>
            &nbsp;·&nbsp;
            {saved_active.proposal_payload.get("resolved_field_count", "?")} Gold cols
            &nbsp;·&nbsp;
            {saved_active.proposal_payload.get("gx_suite", {}).get("expectation_count", "?")} GX expectations
            {extra_meta}
            {("<br>Tags: " + tag_html) if tag_html else ""}
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    bcols = st.columns([1.4, 1.6, 1, 1.4, 4])
    with bcols[0]:
        use_saved_clicked = st.button(
            "📋 Use Saved",
            type="primary",
            use_container_width=True,
            help="Load the saved proposal — no LLM call, no tokens spent.",
        )
    with bcols[1]:
        # Cost preview for re-propose. Use the LAST proposal's cost as the
        # estimate (variation between runs is small).
        last_cost = saved_active.estimated_cost_usd
        last_tokens = saved_active.total_tokens
        # Phase 16.10 — Re-propose AI spend gate. Default OFF.
        _ai_confirm_repropose = st.checkbox(
            f"✅ Confirm spend ~${last_cost:.4f}",
            value=False,
            key=f"ai_confirm_repropose_{SCOPE_KEY}_{saved_active.proposal_id}",
            help="Required to enable Re-propose. Defaults OFF.",
        )
        repropose_clicked = st.button(
            f"🔄 Re-propose (~{last_tokens:,} tokens, ~${last_cost:.4f})",
            use_container_width=True,
            disabled=not _ai_confirm_repropose,
            help=("Spend ~tokens to regenerate. Auto-archives this saved version (unless pinned)."
                  if _ai_confirm_repropose
                  else "🔒 Tick the confirm box above to enable Re-propose."),
        )
    with bcols[2]:
        discard_clicked = st.button(
            "🗑 Discard",
            use_container_width=True,
            help="Mark this saved proposal REJECTED. Falls back to a fresh propose flow.",
        )
    with bcols[3]:
        if saved_active.pinned:
            unpin_clicked = st.button(
                "📌 Unpin",
                use_container_width=True,
                help="Allow this proposal to be auto-archived on next re-propose.",
            )
            pin_clicked = False
        else:
            pin_clicked = st.button(
                "📌 Pin",
                use_container_width=True,
                help="Protect this version from auto-archive. Useful for "
                "marking a 'golden reference' template.",
            )
            unpin_clicked = False

    # Action handlers
    if use_saved_clicked:
        st.session_state[PROPOSAL_KEY] = saved_active.proposal_payload
        st.session_state[PROPOSAL_ID_KEY] = saved_active.proposal_id
        st.toast("📋 Loaded saved proposal — no LLM call.", icon="✅")
        st.rerun()
    if repropose_clicked:
        # Confirm modal — Streamlit doesn't have native modals, use a
        # session_state flag to do a two-click confirmation.
        confirm_key = f"confirm_repropose_{SCOPE_KEY}"
        if st.session_state.get(confirm_key):
            st.session_state[confirm_key] = False
            _run_propose()
            st.rerun()
        else:
            st.session_state[confirm_key] = True
            st.warning(
                f"⚠️ Re-proposing will spend ~{last_tokens:,} tokens "
                f"(~${last_cost:.4f}) and **auto-archive saved v{saved_active.version}**. "
                f"Click 🔄 Re-propose again to confirm."
            )
            st.stop()
    if discard_clicked:
        try:
            proposal_store.reject(
                saved_active.proposal_id,
                rejected_by=f"ui:{selected_client}",
                notes="Discarded from Pipeline Architect UI",
            )
            st.session_state.pop(PROPOSAL_KEY, None)
            st.session_state.pop(PROPOSAL_ID_KEY, None)
            st.toast("🗑 Proposal discarded.", icon="✅")
        except Exception as exc:
            st.error(f"Discard failed: {exc}")
        st.rerun()
    if pin_clicked:
        try:
            proposal_store.pin(saved_active.proposal_id)
            st.toast("📌 Pinned — protected from auto-archive.", icon="✅")
        except Exception as exc:
            st.error(f"Pin failed: {exc}")
        st.rerun()
    if unpin_clicked:
        try:
            proposal_store.unpin(saved_active.proposal_id)
            st.toast("📌 Unpinned.", icon="✅")
        except Exception as exc:
            st.error(f"Unpin failed: {exc}")
        st.rerun()


# Standard Propose button — only shown when no saved active proposal exists,
# OR when the user wants to start fresh after a Discard.
# Phase 16.1 (1.C): also gated by upstream prereqs.
# Phase 16.7: also gated by source_choice — only shown when "AI Construct"
# is the chosen source (the priority path is Clone-from-Global, above).
if not _use_ai_propose:
    st.info(
        f"🤖 **AI Construct mode is OFF** — current source is "
        f"`{source_choice}`. Switch the source picker above to "
        f"`{SOURCE_AI}` if you need a fresh AI proposal "
        f"(spends LLM tokens)."
    )
    st.stop()
_can_propose = prereqs["ready_for_propose"] or (
    prereqs["ready_for_silver_stop"] and allow_silver_stop
)
_button_help = (
    "Generate a fresh proposal."
    if _can_propose and saved_active is None
    else "An approved proposal exists. Use 🔄 Re-propose above to regenerate."
    if saved_active is not None and saved_active.status == ProposalStatus.APPROVED
    else f"Click to generate a brand new proposal (will create v{saved_active.version + 1})."
    if _can_propose and saved_active is not None
    else "🚫 Upstream Silver/Gold not LIVE — see Upstream readiness card above. "
    "Author the missing schemas in Data Model Designer first."
)

# Phase 16.10 — AI spend confirmation gate. AI buttons MUST be disabled by
# default until the operator explicitly ticks the confirm box. Prevents
# accidental clicks burning ~$0.002 per propose × N re-clicks while
# demoing or exploring the UI.
_ai_confirm_propose = st.checkbox(
    "✅ I confirm AI spend (~$0.002 per propose)",
    value=False,
    key=f"ai_confirm_propose_{SCOPE_KEY}",
    help="Required to enable the Propose button. AI Construct calls Claude "
    "Haiku 4.5 — small but real cost. Tick to enable, click Propose, "
    "then this auto-resets on next page load.",
)

propose_btn, _ = st.columns([1, 5])
with propose_btn:
    propose_clicked = st.button(
        "🚀 Propose pipeline",
        type="primary" if (saved_active is None and _can_propose and _ai_confirm_propose) else "secondary",
        use_container_width=True,
        disabled=(
            not _can_propose
            or not _ai_confirm_propose
            or (saved_active is not None and saved_active.status == ProposalStatus.APPROVED)
        ),
        help=_button_help if _ai_confirm_propose else "🔒 Tick the AI-spend confirm box above to enable.",
    )

if propose_clicked:
    _run_propose()


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
          Anchor: <code>{proposal.get("live_gold_anchor")}</code> &nbsp;·&nbsp;
          Gold table: <code>{proposal.get("live_gold_table_name")}</code> &nbsp;·&nbsp;
          {proposal.get("gold_columns_count", 0)} canonical columns &nbsp;·&nbsp;
          {proposal.get("bronze_to_gold_mapping_count", 0)} Bronze→Gold mappings.
          <br>
          <span style="font-size:.85rem;color:#166534;">
          Silver pattern: <strong>{silver_pattern}</strong>
          ({len(silver_models)} dbt files generated)
          {" &nbsp;·&nbsp; <em>(operator-flagged as overkill — review before deploy)</em>" if proposal.get("silver_pattern_is_overkill") else ""}
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
# Phase 16.1: deploy buttons grey out when proposal is already APPROVED to
# prevent double-deploys + token waste from accidental re-clicks.
# ---------------------------------------------------------------------------

st.markdown("---")

# Check whether the in-session proposal is already approved (= already deployed)
_pid_in_session = st.session_state.get(PROPOSAL_ID_KEY)
_already_approved = False
_linked_instance_id: str | None = None
if _pid_in_session:
    try:
        _saved_p = proposal_store.get(_pid_in_session)
        if _saved_p and _saved_p.status == ProposalStatus.APPROVED:
            _already_approved = True
            _linked_instance_id = _saved_p.linked_artifact_id
    except Exception:
        pass  # graceful fallback — keep deploy enabled

if _already_approved:
    st.markdown("### ✅ Already Deployed")
    st.success(
        f"This proposal is already approved and deployed as instance "
        f"`{(_linked_instance_id or '?')[:8]}…`. To redeploy, use 🔄 Re-propose "
        f"at the top of the page (will create a new version)."
    )
    st.markdown("**Deploy buttons disabled** to prevent accidental re-deploy / token waste.")
    deploy_clicked = False
    save_draft_clicked = False
    deploy_notes = ""
else:
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
            # Phase 16.1 — link the saved proposal to the deployed instance
            # so future page loads show "Approved → instance X" banner.
            saved_pid = st.session_state.get(PROPOSAL_ID_KEY)
            if saved_pid:
                try:
                    proposal_store.approve(
                        saved_pid,
                        approved_by=f"ui:{selected_client}",
                        linked_artifact_type="pipeline_instance",
                        linked_artifact_id=instance_id,
                        notes=deploy_notes or None,
                    )
                except Exception as link_exc:
                    # Linking is bookkeeping — deploy already succeeded.
                    st.warning(
                        f"⚠️ Couldn't link saved proposal to instance "
                        f"({link_exc}). Deploy succeeded; manual approve "
                        f"of proposal {saved_pid[:8]}… needed to re-link."
                    )
            st.session_state.pop(PROPOSAL_KEY, None)
            st.session_state.pop(PROPOSAL_ID_KEY, None)
            st.success(
                f"🎉 LIVE — instance `{instance_id[:8]}…`. "
                f"GX suite `{deploy_result['gx_suite_id'][:8]}…` registered."
            )
            st.markdown("**Artifacts emitted:**")
            for k, v in deploy_result["artifact_paths"].items():
                st.markdown(f"- `{k}` → `{v}`")

            # Phase 16.9 — when the deploy is for the GLOBAL pseudo-client,
            # automatically publish the pipeline as the canonical template +
            # pause the DAG (the global is a TEMPLATE — never runs data).
            if _is_global_build_mode:
                try:
                    with st.spinner("🌍 Auto-publishing as Global template..."):
                        new_template_id = template_store.publish_pipeline_to_global(
                            instance_id,
                            by="ui:global-builder",
                            notes=(
                                "Auto-published — Global Builder mode. "
                                "Pipeline serves as canonical template only; "
                                "real clients clone from this."
                            ),
                        )
                    # Pause the global DAG immediately (it's a template)
                    try:
                        from datalink.orchestration import airflow_ops

                        global_dag_id = (
                            f"{GLOBAL_CLIENT.lower()}_{selected_dataset_code.lower()}_pipeline"
                        )
                        airflow_ops.pause_dag(global_dag_id)
                    except Exception:
                        pass  # Airflow may not have picked up DAG yet — pause on next scan
                    st.success(
                        f"🌍 **Global pipeline template published** as "
                        f"`{new_template_id}`. DAG paused (template only — "
                        f"no data flows through it). Real clients now see "
                        f"**📦 Clone from Global** as their default Pipeline "
                        f"Architect option."
                    )
                    st.balloons()
                except Exception as auto_exc:
                    st.warning(
                        f"⚠️ Auto-publish to Global failed: {auto_exc}. "
                        f"Use the **🌐 Promote as Global** button below to "
                        f"publish manually."
                    )
        except Exception as exc:
            st.error(f"Deploy failed: {type(exc).__name__}: {exc}")
            st.exception(exc)


# ---------------------------------------------------------------------------
# Phase 16.1 (Wave 1 Item 3) — Run Pipeline panel
#
# Once a pipeline instance is LIVE, the operator needs three buttons to
# actually exercise it end-to-end without leaving Streamlit:
#
#   1. 🧪 Generate sample data (PSV) — creates ~100-row deterministic
#      synthetic file at data/generated/<dataset>_bronze_sample.psv
#   2. ▶️ Trigger DAG now            — calls Airflow REST API
#   3. 👁 View latest run + tasks   — pulls run state, links to logs
#
# All three operate via Streamlit / Airflow REST. NO terminal.
# ---------------------------------------------------------------------------

st.markdown("---")

# Phase 16.9 — in Global Builder mode, the pipeline is a TEMPLATE: it never
# runs data. Hide the Run-pipeline panel entirely.
if _is_global_build_mode:
    st.info(
        "🌍 **Global Builder mode** — this pipeline is a TEMPLATE only. "
        "It never ingests data. Real clients **clone** this template and "
        "trigger their own DAG runs from their Pipeline Architect view."
    )
    st.markdown(
        "**Next step**: switch to a real client (URL `?client=<name>`) "
        "to clone this global pipeline and run it for that tenant."
    )
    st.stop()

st.markdown("### 🚀 Run pipeline")

# Find the LIVE instance for this (client, dataset).
with _warehouse(readonly=True) as wh:
    _client_instances_for_run = list_client_instances(wh, client_id=selected_client)
    _live_instances = [
        i
        for i in _client_instances_for_run or []
        if i.get("dataset_code") == selected_dataset_code and i.get("status") == "LIVE"
    ]
_live_instance = _live_instances[0] if _live_instances else None
_dag_id = (
    f"{selected_client.lower()}_{selected_dataset_code.lower()}_pipeline"
    if _live_instance
    else None
)

if not _live_instance:
    st.info(
        f"Deploy a pipeline instance for `{selected_client}` / "
        f"`{selected_dataset_code}` (above) to enable run controls."
    )
else:
    st.caption(
        f"Pipeline LIVE: instance `{str(_live_instance.get('instance_id'))[:8]}…` "
        f"· DAG `{_dag_id}` · "
        f"Bronze: `{_live_instance.get('bronze_schema')}.{_live_instance.get('bronze_table')}` "
        f"· Silver: `{_live_instance.get('silver_schema')}.{_live_instance.get('silver_table')}` "
        f"· Gold: `{_live_instance.get('gold_schema')}.{_live_instance.get('gold_table')}`"
    )

    rcol1, rcol2, rcol3, rcol4 = st.columns([1.4, 1.4, 1.4, 4])

    with rcol1:
        gen_clicked = st.button(
            "🧪 Generate sample",
            use_container_width=True,
            help=f"Synthesize ~100 rows of realistic {selected_dataset_code} data "
            f"and drop it where the bronze_land_task expects it. Idempotent.",
        )
    with rcol2:
        trigger_clicked = st.button(
            "▶️ Trigger DAG now",
            type="primary",
            use_container_width=True,
            help="Manually trigger the Airflow DAG. The bronze_land task picks "
            "up the sample PSV, COPY INTO Bronze, dbt to Silver/Gold.",
        )
    with rcol3:
        # Page link to Run Monitor (built in Item 4)
        st.page_link(
            "pages/15_Run_Monitor.py",
            label="👁 Open Run Monitor",
            icon="📺",
            use_container_width=True,
        )

    if gen_clicked:
        with st.spinner(f"Generating sample data for {selected_dataset_code}..."):
            try:
                # Lazy-import the generator to avoid eager dependency on the script
                # module at page load.
                import subprocess
                from pathlib import Path

                # Use the in-container path for control_tower (the page runs
                # inside that container).
                script = Path("/opt/datalink/scripts/generate_membership_bronze_sample.py")
                if not script.exists():
                    # Fallback for non-Aetna-Membership case — generic generator
                    # would go here. For now we only have the Membership generator.
                    st.error(
                        f"No sample generator wired for `{selected_dataset_code}` yet. "
                        f"Wave 4 will add a generic generator. Today this only works "
                        f"for membership."
                    )
                else:
                    result = subprocess.run(
                        ["/usr/local/bin/python", str(script)],
                        capture_output=True,
                        text=True,
                        timeout=60,
                        cwd="/opt/datalink",
                    )
                    if result.returncode == 0:
                        last_line = (result.stdout.strip().splitlines() or ["OK"])[-1]
                        st.success(f"🧪 Sample generated: {last_line}")
                    else:
                        st.error(f"Sample generation failed:\n{result.stderr[:500]}")
            except Exception as exc:
                st.error(f"Sample generation error: {exc}")

    if trigger_clicked:
        from datalink.orchestration import airflow_ops

        with st.spinner(f"Triggering DAG {_dag_id}..."):
            try:
                # Make sure DAG is unpaused first (no-op if already)
                airflow_ops.unpause_dag(_dag_id)
                run_meta = airflow_ops.trigger_dag(
                    _dag_id,
                    conf={
                        "triggered_by_ui": "pipeline_architect",
                        "client_id": selected_client,
                        "dataset_code": selected_dataset_code,
                    },
                )
                st.success(
                    f"▶️ DAG triggered. Run ID: `{run_meta.get('dag_run_id', '?')}`. "
                    f"State: `{run_meta.get('state', 'queued')}`. "
                    f"Use **👁 Open Run Monitor** to watch it progress."
                )
            except Exception as exc:
                if "is_dag_known" in str(exc) or "404" in str(exc):
                    st.error(
                        f"DAG `{_dag_id}` not yet picked up by the Airflow scheduler. "
                        f"This is expected immediately after deploy — the scheduler "
                        f"scans the dags/ folder every 30s. Wait a moment and retry."
                    )
                else:
                    st.error(f"Trigger failed: {type(exc).__name__}: {exc}")


# ---------------------------------------------------------------------------
# Phase 16.7 (final piece) — PUBLISH PIPELINE TO GLOBAL
#
# Once a pipeline has been deployed AND its DAG has run successfully
# end-to-end at least once, the operator can promote this pipeline as the
# canonical GLOBAL template for the dataset. This:
#
#   1. Inserts a row into CONTROL.global_pipeline_templates (LIVE)
#   2. Captures every artifact file (Bronze DDL, Silver dbt files,
#      Gold dbt, Airflow DAG) into CONTROL.global_artifact_blobs
#   3. Marks the underlying Silver+Gold designs as scope_owner='GLOBAL_CORP'
#      (handled separately in Data Model Designer's Publish to Global,
#      but we trigger it here too for one-click promotion)
#
# After this, every other client onboarding for this dataset starts with
# 📦 Clone from Global — zero LLM tokens.
# ---------------------------------------------------------------------------

if _live_instance is not None:
    st.markdown("---")
    st.markdown("### 🌐 Promote this pipeline as the canonical Global template")

    # Read current global status
    _global_now = template_store.has_global(selected_dataset_code)
    _g_pipe_now = _global_now.get("pipeline", False)

    if _g_pipe_now:
        # A global already exists — show "Re-publish (creates v2)" affordance
        _g = template_store.get_global(selected_dataset_code)
        _g_pipe = _g.get("pipeline") or {}
        st.success(
            f"📦 **Global template already published** for "
            f"`{selected_dataset_code}` — "
            f"version `v{_g_pipe.get('version', '?')}` "
            f"(template_id `{str(_g_pipe.get('template_id', ''))[:40]}…`). "
            f"Future clients clone from it via Pipeline Architect's "
            f"**📦 Clone from Global** source picker."
        )
        if st.button(
            "🔁 Re-publish (creates v2 — supersedes current global)",
            help="Promotes THIS pipeline as the new canonical version. The "
            "prior global gets DEPRECATED. Use after intentional design "
            "changes you want fanned out to all future clones.",
        ):
            try:
                with st.spinner("Snapshotting + capturing artifact blobs..."):
                    new_template_id = template_store.publish_pipeline_to_global(
                        str(_live_instance["instance_id"]),
                        by=f"ui:{selected_client}",
                        notes=f"Re-published from {selected_client}'s instance.",
                    )
                st.success(
                    f"📦 Re-published as `{new_template_id}`. "
                    f"Existing clones are now consuming a deprecated version — "
                    f"the **Version Browser** will show migration plans."
                )
                st.rerun()
            except Exception as exc:
                st.error(f"Re-publish failed: {type(exc).__name__}: {exc}")
    else:
        # No global yet — primary publish action
        st.info(
            f"⚠️ **No global template yet for `{selected_dataset_code}`.** "
            f"Publish this deployed pipeline as the canonical version so "
            f"future clients can **clone in one click** instead of "
            f"re-running the AI agent. **Saves ~$0.002 + ~5s per future client.**"
        )

        with st.expander("What gets captured by 'Publish to Global'?"):
            st.markdown(f"""
- **Pipeline metadata** — anchor (`{_live_instance.get("bronze_anchor")}`),
  schedule (`{_live_instance.get("schedule_cron")}`), routing rules
- **Bronze DDL** — `datalink/pipeline/bronze/ddl/{selected_client.lower()}_{selected_dataset_code}.sql`
- **Gold DDL** — `datalink/pipeline/gold/ddl/{selected_client.lower()}_{selected_dataset_code}.sql`
- **Silver dbt models** (Hub/Sat/Link files) — entire `dbt/models/silver/{selected_client.lower()}/{selected_dataset_code}/` directory
- **Gold dbt model** — `dbt/models/gold/{selected_client.lower()}/{selected_dataset_code}.sql`
- **Airflow DAG** — `dags/{selected_client.lower()}_{selected_dataset_code}_pipeline.py`

All blobs are stored in `CONTROL.global_artifact_blobs`. When a future
client clones, the blobs are written out as that client's artifacts —
**zero LLM call, instant materialisation**.
""")

        if st.button(
            "🌐 Publish this pipeline as Global template",
            type="primary",
            help=f"Snapshot {selected_client}'s {selected_dataset_code} "
            f"pipeline as the canonical version. Other clients will "
            f"clone from this with no LLM cost.",
        ):
            try:
                with st.spinner("Snapshotting pipeline + capturing artifact blobs..."):
                    template_id = template_store.publish_pipeline_to_global(
                        str(_live_instance["instance_id"]),
                        by=f"ui:{selected_client}",
                        notes=(
                            f"Initial global publish from {selected_client}'s "
                            f"deployed {selected_dataset_code} pipeline."
                        ),
                    )
                st.success(
                    f"🎉 **Published as Global template** "
                    f"`{template_id}`. "
                    f"Future clients see **📦 Clone from Global** as their "
                    f"default Pipeline Architect source. **Zero LLM tokens** "
                    f"to onboard them now."
                )
                st.balloons()
                st.rerun()
            except Exception as exc:
                st.error(f"Publish failed: {type(exc).__name__}: {exc}")


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


# ---------------------------------------------------------------------------
# Phase 16.1 — Proposal History + Tag editor
#
# History tab: every proposal version for this (client, dataset) scope, newest
# first, with status / cost / linked-artifact columns. Click any row to view
# the full payload + restore that version.
#
# Tag editor: visible when an active proposal exists. Multi-select against
# the catalog, with role enforcement (today permissive — Wave 3 enforces).
# ---------------------------------------------------------------------------

st.markdown("---")
st.markdown("## 📜 Proposal History — full audit trail")
st.caption(
    "Every AI proposal for this (client, dataset) is saved here. No-LLM "
    "browsing, restore-to-this-version, tag governance, full lineage. "
    "Pin a version to protect it from auto-archive on re-propose."
)

try:
    history = proposal_store.history(scope_type=SCOPE_TYPE, scope_key=SCOPE_KEY, limit=50)
except Exception as exc:
    history = []
    st.warning(f"Could not load proposal history: {exc}")

if not history:
    st.info(
        f"No proposals saved yet for `{SCOPE_KEY}`. Click 🚀 Propose pipeline above to create v1."
    )
else:
    # Render as a dataframe of summaries first; rich detail in expanders below.
    hist_rows = []
    for h in history:
        link = (
            f"{h.linked_artifact_type or ''}:{(h.linked_artifact_id or '')[:8]}…"
            if h.linked_artifact_id
            else "—"
        )
        pin_marker = "📌" if h.pinned else ""
        hist_rows.append(
            {
                "v": f"v{h.version}",
                "Status": h.status.value,
                "Pinned": pin_marker,
                "Tokens": f"{h.total_tokens:,}",
                "Cost": f"${h.estimated_cost_usd:.4f}",
                "Latency": f"{h.latency_ms} ms",
                "Tags": ", ".join(h.tags) if h.tags else "—",
                "By": h.created_by,
                "Created": h.created_at.strftime("%Y-%m-%d %H:%M"),
                "Linked": link,
                "ID": h.proposal_id[:8] + "…",
            }
        )
    st.dataframe(pd.DataFrame(hist_rows), use_container_width=True, hide_index=True)

    # Aggregate cost summary — groundwork for the Wave 4 #2 cost dashboard
    total_spent = sum(h.estimated_cost_usd for h in history)
    total_tokens = sum(h.total_tokens for h in history)
    st.caption(
        f"💰 **Cumulative spend** for this scope: ${total_spent:.4f} across "
        f"{total_tokens:,} tokens over {len(history)} proposals."
    )

    # Per-proposal expander — payload preview + restore + manage tags
    st.markdown("### 🔍 Inspect / Restore / Tag")
    sel_options = [
        f"v{h.version}  [{h.status.value}]  ({h.created_at.strftime('%Y-%m-%d %H:%M')})  "
        f"— {h.proposal_id[:8]}"
        for h in history
    ]
    sel_idx = st.selectbox(
        "Pick a version to inspect",
        options=range(len(sel_options)),
        format_func=lambda i: sel_options[i],
        key=f"hist_select_{SCOPE_KEY}",
    )
    selected_h = history[sel_idx]

    insp_cols = st.columns([1, 1, 1, 1, 4])
    with insp_cols[0]:
        if st.button(
            "📋 Restore as Active",
            key=f"restore_{selected_h.proposal_id}",
            help="Loads this version into the proposal display above. "
            "If the current active proposal is APPROVED, this "
            "creates a new DRAFT version superseding it.",
            disabled=(
                selected_h.status == ProposalStatus.APPROVED
                and saved_active is not None
                and saved_active.proposal_id == selected_h.proposal_id
            ),
        ):
            # Load into session_state — the proposal_id is the historical one,
            # but we mark this as "use saved" semantically.
            st.session_state[PROPOSAL_KEY] = selected_h.proposal_payload
            st.session_state[PROPOSAL_ID_KEY] = selected_h.proposal_id
            st.toast(f"📋 Restored v{selected_h.version} as active.", icon="✅")
            st.rerun()
    with insp_cols[1]:
        if selected_h.pinned:
            if st.button(
                "📌 Unpin", key=f"hunpin_{selected_h.proposal_id}", use_container_width=True
            ):
                proposal_store.unpin(selected_h.proposal_id)
                st.toast("📌 Unpinned.", icon="✅")
                st.rerun()
        else:
            if st.button(
                "📌 Pin",
                key=f"hpin_{selected_h.proposal_id}",
                use_container_width=True,
                help="Protect from auto-archive. Use for golden-reference templates.",
            ):
                proposal_store.pin(selected_h.proposal_id)
                st.toast("📌 Pinned.", icon="✅")
                st.rerun()
    with insp_cols[2]:
        if st.button(
            "🗑 Archive",
            key=f"harch_{selected_h.proposal_id}",
            use_container_width=True,
            disabled=(selected_h.status == ProposalStatus.ARCHIVED),
            help="Manually archive this version. Reversible — re-propose "
            "or restore creates a new version.",
        ):
            proposal_store.archive(selected_h.proposal_id, archived_by=f"ui:{selected_client}")
            st.toast("🗑 Archived.", icon="✅")
            st.rerun()

    # Payload preview
    with st.expander(f"Payload for v{selected_h.version} (read-only JSON)"):
        st.json(selected_h.proposal_payload, expanded=False)

    # Tag editor — multiselect against catalog, scoped to selected version.
    with st.expander(f"🏷️  Edit tags on v{selected_h.version}", expanded=False):
        try:
            catalog = proposal_store.all_tags()
        except Exception as exc:
            catalog = []
            st.warning(f"Tag catalog unavailable: {exc}")

        if catalog:
            current = set(selected_h.tags)
            tag_names = sorted(t["tag_name"] for t in catalog)
            tag_lookup = {t["tag_name"]: t for t in catalog}

            # Group by category for readability
            categories = sorted({t["tag_category"] for t in catalog})
            cat_pick = st.selectbox(
                "Filter by category",
                options=["(all)", *categories],
                key=f"tagcat_{selected_h.proposal_id}",
            )
            visible_tags = [
                t
                for t in tag_names
                if cat_pick == "(all)" or tag_lookup[t]["tag_category"] == cat_pick
            ]

            new_set = st.multiselect(
                "Tags applied to this proposal",
                options=visible_tags,
                default=[t for t in visible_tags if t in current],
                key=f"tagsel_{selected_h.proposal_id}_{cat_pick}",
                help="Some tags require a role (e.g. `certified-prod` "
                "requires data-steward). Wave 3 will enforce; today "
                "every actor passes.",
            )
            apply_cols = st.columns([1, 1, 4])
            with apply_cols[0]:
                if st.button(
                    "💾 Apply tag changes", key=f"tagapply_{selected_h.proposal_id}", type="primary"
                ):
                    # Diff against current
                    add_these = set(new_set) - current
                    remove_these = current - set(new_set)
                    # Limit removals to the visible category to avoid wiping
                    # tags from other categories that weren't shown
                    if cat_pick != "(all)":
                        cat_tags = {t["tag_name"] for t in catalog if t["tag_category"] == cat_pick}
                        remove_these = remove_these & cat_tags
                    actor = f"ui:{selected_client}"
                    errors: list[str] = []
                    for tn in add_these:
                        try:
                            proposal_store.add_tag(
                                proposal_id=selected_h.proposal_id,
                                tag_name=tn,
                                assigned_by=actor,
                            )
                        except Exception as exc:
                            errors.append(f"add {tn}: {exc}")
                    for tn in remove_these:
                        try:
                            proposal_store.remove_tag(
                                proposal_id=selected_h.proposal_id,
                                tag_name=tn,
                                removed_by=actor,
                            )
                        except Exception as exc:
                            errors.append(f"remove {tn}: {exc}")
                    if errors:
                        st.error("Some changes failed:\n" + "\n".join(errors))
                    else:
                        st.toast(
                            f"🏷️ Applied: +{len(add_these)} / -{len(remove_these)} tags.",
                            icon="✅",
                        )
                    st.rerun()
