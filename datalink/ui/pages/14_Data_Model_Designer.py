"""Data Model Designer — Phase 15.8 — Independent Bronze / Silver / Gold authoring.

Top-of-funnel for the medallion architecture. Three layers, three independent
lifecycles, three authoring modes per designable layer:

    🥉 Bronze  =  read-only (loaded from product catalog)
    🥈 Silver  =  designable: AI / Manual / Upload  (Hub/Sat/Link or NORMALIZED)
    🥇 Gold    =  designable: AI / Manual / Upload  (canonical consumption model)

Each layer has its own DRAFT → PENDING_REVIEW → LIVE → ARCHIVED lifecycle.
Operator can ship "Silver-stop" pipelines (clean+integrate, hold consumption)
or full Bronze→Silver→Gold. Pipeline Architect picks up whatever is LIVE.
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
    page_title="Data Model Designer",
    page_icon="🥇",
    layout="wide",
    initial_sidebar_state="expanded",
)

from datalink.adapters.embeddings.router import get_embedder  # noqa: E402
from datalink.agents.gold_schema_designer import (  # noqa: E402
    GoldAnchor,
    approve_gold_schema,
    list_gold_datasets,
    propose_gold_ai,
    propose_gold_import,
    propose_gold_manual,
)
from datalink.agents.gold_schema_designer import (  # noqa: E402
    persist_proposal as persist_gold_proposal,
)
from datalink.agents.llm_router import get_llm  # noqa: E402
from datalink.agents.silver_schema_designer import (  # noqa: E402
    approve_silver_schema,
    get_silver_schema,
    list_silver_datasets,
    persist_silver_proposal,
    propose_silver_ai,
    propose_silver_import,
)
from datalink.config.loader import load_settings  # noqa: E402
from datalink.memory import AgentMemoryStore  # noqa: E402
from datalink.quality.control import CONTROL_SCHEMA, create_control_tables  # noqa: E402
from datalink.ui._nav import render_sidebar, require_client  # noqa: E402
from datalink.ui._query import warehouse_ctx  # noqa: E402

render_sidebar(active="Data Model Designer")

_NAVY = "#0a1a3e"
_GOLD = "#d4af37"
_AGENT_BLUE = "#2563eb"
_GREEN = "#15803d"
_AMBER = "#b45309"
_RED = "#b91c1c"
_GREY = "#6b7280"

st.markdown(
    f"""
    <style>
      h1 {{color: {_NAVY}; border-bottom: 3px solid {_GOLD}; padding-bottom: .4rem;}}
      h2 {{color: {_NAVY}; margin-top: 1.5rem;}}
      h3 {{color: {_NAVY};}}
      .gd-hero {{background:linear-gradient(90deg,{_NAVY}11,{_GOLD}33);
                 padding:1rem 1.2rem;border-radius:8px;border-left:4px solid {_GOLD};
                 margin:.6rem 0 1.5rem 0}}
      .gd-card {{background:#fff;border:1px solid #d4d4d8;border-radius:8px;
                 padding:1rem;margin:.5rem 0;box-shadow:0 1px 3px rgba(0,0,0,.04)}}
      .gd-pill {{display:inline-block;padding:.15rem .55rem;border-radius:12px;
                 font-size:.72rem;font-weight:700;letter-spacing:.04em;margin-right:.4rem}}
      .gd-pill-live {{background:#dcfce7;color:#166534}}
      .gd-pill-pending {{background:#fef3c7;color:#92400e}}
      .gd-pill-draft {{background:#dbeafe;color:#1e40af}}
      .gd-pill-archived {{background:#f3f4f6;color:#4b5563}}
      .gd-pill-none {{background:#f3f4f6;color:#9ca3af}}
      .gd-pill-ai {{background:#e0e7ff;color:#3730a3}}
      .gd-pill-manual {{background:#fef3c7;color:#92400e}}
      .gd-pill-import {{background:#d1fae5;color:#065f46}}
      .gd-stat {{font-size:1.7rem;font-weight:700;color:{_NAVY};margin:0;}}
      .gd-stat-label {{font-size:.78rem;color:#64748b;text-transform:uppercase;
                       letter-spacing:.05em;font-weight:600;margin-top:.2rem;}}
      .gd-stat-emoji {{font-size:1.1rem;}}
      .gd-meta {{font-size:.85rem;color:#475569;margin-top:.4rem}}
      .gd-layer-tab {{display:inline-block;padding:.35rem .9rem;margin:.2rem;
                      border-radius:6px;font-weight:600;font-size:.92rem;}}
      .gd-table-status {{font-family:ui-monospace,Menlo,monospace;font-size:.85rem;}}
    </style>
    """,
    unsafe_allow_html=True,
)


@contextmanager
def _warehouse(*, readonly: bool = True) -> Iterator[Any]:
    with warehouse_ctx(readonly=readonly) as wh:
        yield wh


# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------

st.markdown("# 🥇 Data Model Designer")
st.markdown(
    """
    <div class="gd-hero">
      <strong>Independent Bronze · Silver · Gold authoring with three modes per layer.</strong>
      Bronze comes from the product catalog (vendor mapping spec). Silver
      and Gold each have their own DRAFT → PENDING_REVIEW → LIVE → ARCHIVED
      lifecycle and three authoring modes: AI Construct, Manual Author,
      Upload. Operator can ship Silver-stop pipelines (clean+integrate,
      hold consumption) or full medallion. Once LIVE every client clones
      from the global model on Pipeline Architect deploy.
      <div class="gd-meta">
        Backend: <strong>Claude Haiku 4.5 + Voyage 3.5-lite RAG</strong>
        &middot; Bronze catalog: 33 datasets · 943 fields · CATALOG_ANCHOR by default
        &middot; PHI-safe: only metadata reaches the LLM.
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)

require_client()


# ---------------------------------------------------------------------------
# Bootstrap CONTROL tables
# ---------------------------------------------------------------------------

with _warehouse(readonly=False) as wh:
    create_control_tables(wh)


# ---------------------------------------------------------------------------
# Cached lookups
# ---------------------------------------------------------------------------


@st.cache_data(ttl=20)  # type: ignore
def _list_bronze_datasets() -> list[dict[str, Any]]:
    with warehouse_ctx(readonly=True) as wh:
        return list(
            wh.query(
                f"SELECT dataset_code, display_name, category, default_frequency, "
                f"       total_fields, default_anchor, used_by "
                f"  FROM {CONTROL_SCHEMA}.global_bronze_catalog_datasets "
                f" WHERE is_active = TRUE ORDER BY display_name"
            )
        )


@st.cache_data(ttl=20)  # type: ignore
def _bronze_fields_for(dataset_code: str) -> list[dict[str, Any]]:
    with warehouse_ctx(readonly=True) as wh:
        return list(
            wh.query(
                f"SELECT field_order, bronze_column_name, requirement, logical_type, "
                f"       is_business_key, is_pii, is_phi, description "
                f"  FROM {CONTROL_SCHEMA}.global_bronze_catalog_fields "
                f" WHERE dataset_code = $ds ORDER BY field_order",
                {"ds": dataset_code},
            )
        )


bronze_datasets = _list_bronze_datasets()
with _warehouse(readonly=True) as wh:
    all_silver = list_silver_datasets(wh)
    all_gold = list_gold_datasets(wh)


# ---------------------------------------------------------------------------
# Medallion Registry Status — 4 tiles + drill-down expander
# ---------------------------------------------------------------------------

st.markdown("## 🏗 Medallion Registry Status")

bronze_count = len(bronze_datasets)
silver_live_count = sum(1 for s in all_silver if s.get("status") == "LIVE")
silver_draft_count = sum(1 for s in all_silver if s.get("status") in ("DRAFT", "PENDING_REVIEW"))
gold_live_count = sum(1 for g in all_gold if g.get("status") == "LIVE")
gold_draft_count = sum(1 for g in all_gold if g.get("status") in ("DRAFT", "PENDING_REVIEW"))

c1, c2, c3, c4 = st.columns(4)
c1.markdown(
    f'<div class="gd-card"><div class="gd-stat-emoji">🥉</div>'
    f'<div class="gd-stat">{bronze_count}</div>'
    f'<div class="gd-stat-label">Bronze datasets available</div></div>',
    unsafe_allow_html=True,
)
silver_draft_html = (
    f"<small style='color:{_AMBER};font-size:.7em'> +{silver_draft_count} draft</small>"
    if silver_draft_count
    else ""
)
gold_draft_html = (
    f"<small style='color:{_AMBER};font-size:.7em'> +{gold_draft_count} draft</small>"
    if gold_draft_count
    else ""
)
c2.markdown(
    f'<div class="gd-card"><div class="gd-stat-emoji">🥈</div>'
    f'<div class="gd-stat">{silver_live_count}{silver_draft_html}</div>'
    f'<div class="gd-stat-label">Silver schemas LIVE</div></div>',
    unsafe_allow_html=True,
)
c3.markdown(
    f'<div class="gd-card"><div class="gd-stat-emoji">🥇</div>'
    f'<div class="gd-stat">{gold_live_count}{gold_draft_html}</div>'
    f'<div class="gd-stat-label">Gold schemas LIVE</div></div>',
    unsafe_allow_html=True,
)
distinct_clients = 0
with _warehouse(readonly=True) as wh:
    rows = list(
        wh.query(
            f"SELECT COUNT(DISTINCT client_id) AS c FROM {CONTROL_SCHEMA}.client_pipeline_instances "
            f"WHERE status = 'LIVE'"
        )
    )
    if rows:
        distinct_clients = int(rows[0]["c"] or 0)
c4.markdown(
    f'<div class="gd-card"><div class="gd-stat-emoji">🤝</div>'
    f'<div class="gd-stat">{distinct_clients}</div>'
    f'<div class="gd-stat-label">Onboarded clients (LIVE)</div></div>',
    unsafe_allow_html=True,
)


# Drill-down expander — unified colored table across all 33 datasets
def _readiness_row(ds: dict[str, Any]) -> dict[str, Any]:
    code = ds["dataset_code"]
    silver = next((s for s in all_silver if s["dataset_code"] == code), None)
    gold = next((g for g in all_gold if g["dataset_code"] == code), None)
    used_by_raw = ds.get("used_by") or "[]"
    try:
        used_by_list = json.loads(used_by_raw) if isinstance(used_by_raw, str) else used_by_raw
    except json.JSONDecodeError:
        used_by_list = []
    return {
        "Dataset": ds["display_name"],
        "Code": code,
        "Category": ds.get("category") or "—",
        "Bronze": "✓ LIVE",
        "Silver": (
            f"✓ LIVE ({silver['silver_pattern']} v{silver['version']})"
            if silver and silver.get("status") == "LIVE"
            else "◌ DRAFT"
            if silver and silver.get("status") in ("DRAFT", "PENDING_REVIEW")
            else "— none"
        ),
        "Gold": (
            f"✓ LIVE (v{gold['version']})"
            if gold and gold.get("status") == "LIVE"
            else "◌ DRAFT"
            if gold and gold.get("status") in ("DRAFT", "PENDING_REVIEW")
            else "— none"
        ),
        "Bronze fields": ds["total_fields"],
        "Used by": ", ".join(used_by_list) if used_by_list else "—",
    }


def _color_status(val: str) -> str:
    """Streamlit Pandas Styler — color cells by readiness."""
    if isinstance(val, str):
        if val.startswith("✓ LIVE"):
            return f"background-color:#dcfce7;color:{_GREEN};font-weight:600"
        if val.startswith("◌ DRAFT"):
            return f"background-color:#fef3c7;color:{_AMBER};font-weight:600"
        if val.startswith("— none"):
            return f"background-color:#f3f4f6;color:{_GREY}"
    return ""


with st.expander(
    f"📊 Drill into all {bronze_count} datasets — Bronze / Silver / Gold readiness",
    expanded=False,
):
    if not bronze_datasets:
        st.warning(
            "Bronze catalog is empty. Run " "`python3 scripts/load_product_catalog.py` to seed it."
        )
    else:
        rows = [_readiness_row(d) for d in bronze_datasets]
        df = pd.DataFrame(rows)
        styled = df.style.map(_color_status, subset=["Bronze", "Silver", "Gold"])
        st.dataframe(styled, use_container_width=True, hide_index=True, height=520)


# ---------------------------------------------------------------------------
# Dataset + anchor pickers
# ---------------------------------------------------------------------------

st.markdown("## 🎯 Pick a dataset")
ds_label_to_obj = {
    f"{d['display_name']}  ({d['total_fields']} fields, {d['category']})": d
    for d in bronze_datasets
}
col_ds, col_anchor = st.columns([3, 2])
with col_ds:
    selected_label = st.selectbox(
        "Dataset",
        options=list(ds_label_to_obj.keys()),
        index=0 if bronze_datasets else None,
    )
    selected_dataset = ds_label_to_obj[selected_label] if selected_label else None
    selected_dataset_code = str(selected_dataset["dataset_code"]) if selected_dataset else ""
    selected_dataset_display = str(selected_dataset["display_name"]) if selected_dataset else ""
with col_anchor:
    anchor_options = [a.value for a in GoldAnchor]
    default_anchor = (
        str(selected_dataset.get("default_anchor") or "CATALOG_ANCHOR")
        if selected_dataset
        else "CATALOG_ANCHOR"
    )
    default_idx = anchor_options.index(default_anchor) if default_anchor in anchor_options else 0
    anchor = st.selectbox(
        "Anchor",
        options=anchor_options,
        index=default_idx,
        help="CATALOG_ANCHOR = vendor mapping spec is the truth. Specialized anchors "
        "(FHIR/X12/etc.) override when applicable.",
    )

if not selected_dataset:
    st.stop()


# ---------------------------------------------------------------------------
# Layer navigation — radio-as-tabs
# ---------------------------------------------------------------------------

st.markdown("---")
st.markdown(f"## Designing **{selected_dataset_display}**")

layer = st.radio(
    "Layer",
    options=["🥉 Bronze (read-only)", "🥈 Silver (designable)", "🥇 Gold (designable)"],
    horizontal=True,
    label_visibility="collapsed",
)

# Find existing LIVE schemas for this dataset
live_silver = next(
    (
        s
        for s in all_silver
        if s["dataset_code"] == selected_dataset_code and s.get("status") == "LIVE"
    ),
    None,
)
live_gold = next(
    (
        g
        for g in all_gold
        if g["dataset_code"] == selected_dataset_code and g.get("status") == "LIVE"
    ),
    None,
)


# ===========================================================================
# 🥉 BRONZE TAB — read-only
# ===========================================================================

if layer.startswith("🥉"):
    st.markdown("### 🥉 Bronze (read-only — from vendor mapping spec)")
    st.info(
        "Bronze fields are loaded from the product catalog (`global_bronze_catalog_*`). "
        "They reflect what the client agreed to send. To author a one-off Bronze "
        "contract for a vendor file outside the catalog, use the "
        "[Data Contract Architect](/Data_Contract_Architect)."
    )
    fields = _bronze_fields_for(selected_dataset_code)
    if not fields:
        st.warning("No Bronze fields found.")
    else:
        rows = []
        for f in fields:
            bk = "🔑" if f.get("is_business_key") else ""
            pii = "🔒" if f.get("is_pii") else ""
            phi = "🩺" if f.get("is_phi") else ""
            rows.append(
                {
                    "#": f["field_order"],
                    "Bronze column": f["bronze_column_name"],
                    "Type": f["logical_type"],
                    "Req?": f["requirement"],
                    "BK": bk,
                    "PII": pii,
                    "PHI": phi,
                    "Description": (f.get("description") or "")[:80],
                }
            )
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)


# ===========================================================================
# 🥈 SILVER TAB — designable, 3 modes, version history
# ===========================================================================

elif layer.startswith("🥈"):
    st.markdown("### 🥈 Silver (Hub/Sat/Link by default; NORMALIZED for trivial datasets)")

    # Show LIVE state
    if live_silver:
        st.success(
            f"✅ **LIVE Silver** v{live_silver['version']} · "
            f"pattern=`{live_silver['silver_pattern']}` · "
            f"anchor=`{live_silver['silver_anchor']}` · "
            f"source=`{live_silver['source']}` · "
            f"approved by `{live_silver['approved_by']}`"
        )
        with _warehouse(readonly=True) as wh:
            schema = get_silver_schema(wh, live_silver["silver_dataset_id"])
        # Render tables list
        with st.expander("📋 Silver tables in this LIVE schema", expanded=True):
            tables_rows = []
            for t in schema["tables"]:
                bks = json.loads(t.get("business_keys_json") or "[]") or []
                tables_rows.append(
                    {
                        "Order": t["table_order"],
                        "Table": t["table_name"],
                        "Kind": t["table_kind"],
                        "Business keys": ", ".join(bks) if bks else "—",
                        "Description": (t.get("description") or "")[:80],
                    }
                )
            st.dataframe(pd.DataFrame(tables_rows), use_container_width=True, hide_index=True)
        with st.expander(f"🧬 Silver columns ({len(schema['columns'])})", expanded=False):
            col_rows = []
            for c in schema["columns"]:
                tag = ""
                if c.get("is_hash_key"):
                    tag = "🔑h"
                elif c.get("is_business_key"):
                    tag = "🔑"
                elif c.get("is_hash_diff"):
                    tag = "📐d"
                col_rows.append(
                    {
                        "#": c["column_order"],
                        "Column": c["column_name"],
                        "Type": c["logical_type"],
                        "Null?": "✓" if c.get("nullable") else "—",
                        "Tag": tag,
                        "PII": "🔒" if c.get("is_pii") else "",
                        "PHI": "🩺" if c.get("is_phi") else "",
                    }
                )
            st.dataframe(pd.DataFrame(col_rows), use_container_width=True, hide_index=True)
        with st.expander(
            f"🔁 Bronze → Silver mappings ({len(schema['mappings'])})", expanded=False
        ):
            map_rows = []
            for m in schema["mappings"]:
                srcs = m.get("bronze_source_columns") or "[]"
                if isinstance(srcs, str):
                    try:
                        srcs = json.loads(srcs)
                    except json.JSONDecodeError:
                        srcs = []
                map_rows.append(
                    {
                        "Silver table": m["silver_table_name"],
                        "Silver col": m["silver_column_name"],
                        "Kind": m["transform_kind"],
                        "Bronze sources": ", ".join(srcs)[:60],
                        "Transform SQL": (m["transform_sql"] or "")[:80],
                    }
                )
            st.dataframe(pd.DataFrame(map_rows), use_container_width=True, hide_index=True)
    else:
        st.info(f"No LIVE Silver schema for **{selected_dataset_display}** yet.")

    # Version history
    silver_versions = [s for s in all_silver if s["dataset_code"] == selected_dataset_code]
    if silver_versions:
        with st.expander(f"📜 Silver version history ({len(silver_versions)})", expanded=False):
            ver_rows = []
            for s in silver_versions:
                ver_rows.append(
                    {
                        "Version": s["version"],
                        "Status": s["status"],
                        "Pattern": s["silver_pattern"],
                        "Anchor": s["silver_anchor"],
                        "Source": s["source"],
                        "Created": s["created_at"],
                        "Approved": s["approved_at"] or "—",
                        "Archived": s["archived_at"] or "—",
                        "Silver ID": str(s["silver_dataset_id"])[:8] + "…",
                    }
                )
            st.dataframe(pd.DataFrame(ver_rows), use_container_width=True, hide_index=True)
            st.caption(
                "💡 Revert to an archived version: use `revert_silver_schema(archived_silver_dataset_id, ...)` "
                "from the bridge — UI button coming in next iteration."
            )

    # Author mode tabs
    st.markdown("#### Design a new Silver schema (or new version)")
    SILVER_PROPOSAL_KEY = f"silver_proposal_{selected_dataset_code}"
    pattern_default = live_silver["silver_pattern"] if live_silver else "HUB_SAT_LINK"
    silver_pattern_choice = st.radio(
        "Silver pattern",
        options=["HUB_SAT_LINK (DV2 default)", "NORMALIZED (overkill fallback)"],
        index=0 if pattern_default == "HUB_SAT_LINK" else 1,
        horizontal=True,
        help="HUB_SAT_LINK is the DV2 integration pattern (default). NORMALIZED "
        "is a single-table fallback for trivially small datasets.",
    )
    silver_pattern = "HUB_SAT_LINK" if silver_pattern_choice.startswith("HUB") else "NORMALIZED"

    silver_tab_ai, silver_tab_manual, silver_tab_import = st.tabs(
        ["🤖 AI Construct", "✏️ Manual Author", "📥 Import"]
    )

    with silver_tab_ai:
        _ds_for_msg = selected_dataset or {}
        st.markdown(
            f"Agent designs Silver `{silver_pattern}` from the Bronze catalog "
            f"({_ds_for_msg.get('total_fields', '?')} fields), grounded against the "
            f"`{anchor}` corpus when applicable."
        )
        if st.button("🚀 Propose Silver schema (AI)", type="primary", key="silver_ai_propose"):
            with (
                st.spinner(
                    f"Agent designing Silver `{silver_pattern}` for {selected_dataset_display} ({anchor})..."
                ),
                _warehouse(readonly=False) as wh,
            ):
                settings = load_settings()
                llm = get_llm(settings)
                memory: AgentMemoryStore | None = None
                try:
                    memory = AgentMemoryStore(embedder=get_embedder())
                except Exception:
                    memory = None
                try:
                    proposal = propose_silver_ai(
                        llm=llm,
                        warehouse=wh,
                        memory=memory,
                        dataset_code=selected_dataset_code,
                        silver_anchor=anchor,
                        silver_pattern=silver_pattern,
                        actor=f"ui:{st.session_state.get('client_id', 'operator')}",
                    )
                    st.session_state[SILVER_PROPOSAL_KEY] = proposal
                    st.success(
                        f"Proposal ready — {len(proposal['silver_tables'])} tables, "
                        f"{len(proposal.get('bronze_to_silver_mappings', []))} mappings, "
                        f"{proposal['tokens_used']} tokens, {proposal['duration_ms']}ms."
                    )
                except Exception as exc:
                    st.error(f"Agent failed: {type(exc).__name__}: {exc}")
                    st.exception(exc)

    with silver_tab_manual:
        st.markdown(
            "Hand-author the Silver schema. Pro tip: run **AI Construct** first to "
            "get a starting shape, then come back to Manual to refine — that's "
            "faster than authoring from scratch. Manual-from-scratch coming in "
            "next iteration with full Hub/Sat editor."
        )
        st.info(
            "Manual mode for Silver is currently a stub. Use AI Construct or "
            "Import for now; full inline manual authoring is on the roadmap."
        )

    with silver_tab_import:
        st.markdown("Paste an existing Silver schema in one of the supported formats.")
        silver_import_format = st.selectbox(
            "Format",
            options=["DDL_SQL", "DBT_YAML", "DBT_PROJECT", "JSON_SCHEMA"],
            help="DDL_SQL: single CREATE TABLE → NORMALIZED. "
            "DBT_YAML: single dbt model → NORMALIZED. "
            "DBT_PROJECT: multiple models with HUB_/SAT_/LINK_ naming → HUB_SAT_LINK. "
            "JSON_SCHEMA: → NORMALIZED.",
        )
        silver_import_text = st.text_area(
            "Paste schema definition", height=240, key="silver_import_text"
        )
        if st.button("📥 Parse + capture", key="silver_import_propose"):
            if not silver_import_text.strip():
                st.warning("Paste a schema first.")
            else:
                try:
                    proposal = propose_silver_import(
                        dataset_code=selected_dataset_code,
                        import_format=silver_import_format,
                        text=silver_import_text,
                        silver_anchor=anchor,
                    )
                    st.session_state[SILVER_PROPOSAL_KEY] = proposal
                    st.success(f"Parsed — {len(proposal['silver_tables'])} table(s).")
                except Exception as exc:
                    st.error(f"Import failed: {type(exc).__name__}: {exc}")

    # Show Silver proposal if any
    silver_proposal = st.session_state.get(SILVER_PROPOSAL_KEY)
    if silver_proposal:
        st.markdown("---")
        st.markdown("#### 📋 Silver proposal — review")
        st.markdown(
            f"""
            <div class="gd-card">
              <span class="gd-pill gd-pill-{str(silver_proposal.get('designer_mode', '')).split('_')[0].lower()}">
                {silver_proposal.get('designer_mode')}
              </span>
              <span class="gd-pill gd-pill-draft">DRAFT (NEW)</span>
              <span class="gd-pill" style="background:#fde68a;color:#78350f;">
                PATTERN: {silver_proposal.get('silver_pattern')}
              </span>
              <span class="gd-pill" style="background:#dbeafe;color:#1e40af;">
                ANCHOR: {silver_proposal.get('silver_anchor')}
              </span>
              <div class="gd-meta">
                Tokens: {silver_proposal.get('tokens_used', 0)} ·
                Latency: {silver_proposal.get('duration_ms', 0)}ms ·
                Tables: {len(silver_proposal['silver_tables'])} ·
                Mappings: {len(silver_proposal.get('bronze_to_silver_mappings', []))}
              </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        with st.expander("Tables in proposal", expanded=True):
            t_rows = []
            for t in silver_proposal["silver_tables"]:
                t_rows.append(
                    {
                        "Table": t["table_name"],
                        "Kind": t["table_kind"],
                        "Columns": len(t.get("columns") or []),
                        "BKs": ", ".join(t.get("business_keys") or []),
                        "Parent Hub": t.get("parent_hub_name") or "—",
                    }
                )
            st.dataframe(pd.DataFrame(t_rows), use_container_width=True, hide_index=True)

        st.markdown(f"**Rationale:** {silver_proposal.get('rationale', '')}")

        s_a, s_b, _ = st.columns([1, 1, 4])
        with s_a:
            silver_save_draft = st.button(
                "💾 Save as DRAFT", key="silver_save_draft", use_container_width=True
            )
        with s_b:
            silver_approve = st.button(
                "✅ Save & Approve → LIVE",
                type="primary",
                key="silver_approve",
                use_container_width=True,
            )

        if silver_save_draft:
            with _warehouse(readonly=False) as wh:
                try:
                    sid = persist_silver_proposal(
                        warehouse=wh,
                        proposal=silver_proposal,
                        actor=f"ui:{st.session_state.get('client_id', 'operator')}",
                        auto_submit=False,
                    )
                    st.success(f"Saved as DRAFT. silver_dataset_id=`{sid}`")
                    st.session_state.pop(SILVER_PROPOSAL_KEY, None)
                except Exception as exc:
                    st.error(f"Save failed: {exc}")

        if silver_approve:
            with _warehouse(readonly=False) as wh:
                try:
                    sid = persist_silver_proposal(
                        warehouse=wh,
                        proposal=silver_proposal,
                        actor=f"ui:{st.session_state.get('client_id', 'operator')}",
                        auto_submit=True,
                    )
                    approve_silver_schema(
                        warehouse=wh,
                        silver_dataset_id=sid,
                        actor=f"ui:{st.session_state.get('client_id', 'operator')}",
                    )
                    st.success(
                        f"🎉 LIVE — Silver schema for `{selected_dataset_code}` is now LIVE."
                    )
                    st.session_state.pop(SILVER_PROPOSAL_KEY, None)
                except Exception as exc:
                    st.error(f"Approve failed: {exc}")
                    st.exception(exc)


# ===========================================================================
# 🥇 GOLD TAB — designable, 3 modes (existing flow, gated on Silver-LIVE)
# ===========================================================================

elif layer.startswith("🥇"):
    st.markdown("### 🥇 Gold (canonical consumption model)")

    # Gate: Gold requires Silver-LIVE for approval
    if not live_silver:
        st.warning(
            f"⚠️ **Gold approval requires LIVE Silver first.** No LIVE Silver "
            f"schema exists for `{selected_dataset_code}` yet. "
            f"Switch to the 🥈 Silver tab to design + approve Silver, then come back."
        )
        st.stop()

    # Show LIVE Gold
    if live_gold:
        st.success(
            f"✅ **LIVE Gold** v{live_gold['version']} · "
            f"anchor=`{live_gold['gold_anchor']}` · "
            f"source=`{live_gold['source']}` · "
            f"table=`{live_gold['gold_table_name']}` · "
            f"approved by `{live_gold['approved_by']}`"
        )
        # Show Gold columns
        with _warehouse(readonly=True) as wh:
            from datalink.agents.gold_schema_designer import get_gold_schema as gget

            gschema = gget(wh, live_gold["gold_dataset_id"])
        with st.expander(f"🥇 Gold columns ({len(gschema['columns'])})", expanded=True):
            col_rows = []
            for c in gschema["columns"]:
                col_rows.append(
                    {
                        "#": c["column_order"],
                        "Column": c["gold_column_name"],
                        "Type": c["logical_type"],
                        "Null?": "✓" if c.get("nullable") else "—",
                        "BK": "🔑" if c.get("is_business_key") else "",
                        "PII": "🔒" if c.get("is_pii") else "",
                        "PHI": "🩺" if c.get("is_phi") else "",
                        "Anchor cite": (c.get("anchor_reference") or "")[:50],
                    }
                )
            st.dataframe(pd.DataFrame(col_rows), use_container_width=True, hide_index=True)
    else:
        st.info(f"No LIVE Gold schema for **{selected_dataset_display}** yet.")

    # Gold version history
    gold_versions = [g for g in all_gold if g["dataset_code"] == selected_dataset_code]
    if gold_versions:
        with st.expander(f"📜 Gold version history ({len(gold_versions)})", expanded=False):
            ver_rows = []
            for g in gold_versions:
                ver_rows.append(
                    {
                        "Version": g["version"],
                        "Status": g["status"],
                        "Anchor": g["gold_anchor"],
                        "Source": g["source"],
                        "Created": g["created_at"],
                        "Approved": g["approved_at"] or "—",
                        "Archived": g["archived_at"] or "—",
                        "Gold ID": str(g["gold_dataset_id"])[:8] + "…",
                    }
                )
            st.dataframe(pd.DataFrame(ver_rows), use_container_width=True, hide_index=True)

    # Gold authoring — keep existing 3-mode flow
    st.markdown("#### Design a new Gold schema (or new version)")
    GOLD_PROPOSAL_KEY = f"gold_proposal_{selected_dataset_code}"

    gold_tab_ai, gold_tab_manual, gold_tab_import = st.tabs(
        ["🤖 AI Construct", "✏️ Manual Author", "📥 Import"]
    )
    with gold_tab_ai:
        st.markdown(
            f"Agent designs canonical Gold schema for {selected_dataset_display} "
            f"anchored against `{anchor}`."
        )
        if st.button("🚀 Propose Gold (AI)", type="primary", key="gold_ai_propose"):
            with (
                st.spinner(f"Agent constructing Gold for {selected_dataset_display}..."),
                _warehouse(readonly=False) as wh,
            ):
                settings = load_settings()
                llm = get_llm(settings)
                gold_memory: AgentMemoryStore | None = None
                try:
                    gold_memory = AgentMemoryStore(embedder=get_embedder())
                except Exception:
                    gold_memory = None
                try:
                    proposal = propose_gold_ai(
                        llm=llm,
                        warehouse=wh,
                        memory=gold_memory,
                        dataset_code=selected_dataset_code,
                        gold_anchor=anchor,
                        actor=f"ui:{st.session_state.get('client_id', 'operator')}",
                    )
                    st.session_state[GOLD_PROPOSAL_KEY] = proposal
                    st.success(
                        f"Proposal ready — {len(proposal['proposed_columns'])} cols, "
                        f"{len(proposal['bronze_to_gold_mappings'])} mappings, "
                        f"{proposal['tokens_used']} tokens."
                    )
                except Exception as exc:
                    st.error(f"Agent failed: {exc}")
                    st.exception(exc)
    with gold_tab_manual:
        st.markdown(
            "Hand-author Gold columns in the grid below. Pro tip: run AI Construct "
            "first to get a starting shape."
        )
        manual_table_name = st.text_input(
            "Gold table name",
            value=selected_dataset_code,
            key="gold_manual_table_name",
        )
        if "gold_manual_cols" not in st.session_state:
            st.session_state["gold_manual_cols"] = pd.DataFrame(
                [
                    {
                        "gold_column_name": "",
                        "logical_type": "TEXT",
                        "nullable": True,
                        "is_business_key": False,
                        "is_pii": False,
                        "is_phi": False,
                        "description": "",
                        "anchor_reference": "",
                        "rationale": "",
                    }
                ]
            )
        cols_df = st.data_editor(
            st.session_state["gold_manual_cols"],
            num_rows="dynamic",
            use_container_width=True,
            column_config={
                "logical_type": st.column_config.SelectboxColumn(
                    options=["TEXT", "INTEGER", "DECIMAL", "DATE", "TIMESTAMP", "BOOLEAN"],
                ),
            },
            key="gold_manual_cols_editor",
        )
        st.session_state["gold_manual_cols"] = cols_df
        if st.button("📋 Capture as proposal", key="gold_manual_propose"):
            clean_cols = []
            for _, row in cols_df.iterrows():
                name = str(row.get("gold_column_name") or "").strip()
                if not name:
                    continue
                clean_cols.append(
                    {
                        "gold_column_name": name,
                        "logical_type": str(row.get("logical_type") or "TEXT"),
                        "nullable": bool(row.get("nullable", True)),
                        "is_business_key": bool(row.get("is_business_key", False)),
                        "is_pii": bool(row.get("is_pii", False)),
                        "is_phi": bool(row.get("is_phi", False)),
                        "description": str(row.get("description") or ""),
                        "anchor_reference": str(row.get("anchor_reference") or "") or None,
                        "rationale": str(row.get("rationale") or "Manually authored."),
                    }
                )
            if not clean_cols:
                st.warning("Add at least one column.")
            else:
                proposal = propose_gold_manual(
                    dataset_code=selected_dataset_code,
                    gold_table_name=manual_table_name,
                    columns=clean_cols,
                    mappings=[],
                    gold_anchor=anchor,
                )
                st.session_state[GOLD_PROPOSAL_KEY] = proposal
                st.success(f"Captured manual proposal — {len(clean_cols)} columns.")
    with gold_tab_import:
        gold_import_format = st.selectbox(
            "Format",
            options=[
                "DDL_SQL",
                "DBT_YAML",
                "FHIR_PROFILE_JSON",
                "JSON_SCHEMA",
                "SNOWFLAKE_DESCRIBE",
            ],
            key="gold_import_format",
        )
        gold_import_text = st.text_area(
            "Paste schema definition", height=240, key="gold_import_text"
        )
        if st.button("📥 Parse + capture", key="gold_import_propose"):
            if not gold_import_text.strip():
                st.warning("Paste a schema first.")
            else:
                try:
                    proposal = propose_gold_import(
                        dataset_code=selected_dataset_code,
                        import_format=gold_import_format,
                        text=gold_import_text,
                        gold_anchor=anchor,
                    )
                    st.session_state[GOLD_PROPOSAL_KEY] = proposal
                    st.success(f"Parsed — {len(proposal['proposed_columns'])} columns.")
                except Exception as exc:
                    st.error(f"Import failed: {exc}")

    # Render Gold proposal if any
    gold_proposal = st.session_state.get(GOLD_PROPOSAL_KEY)
    if gold_proposal:
        st.markdown("---")
        st.markdown("#### 📋 Gold proposal — review")
        st.markdown(f"**Rationale:** {gold_proposal.get('rationale', '')}")
        with st.expander(f"Gold columns ({len(gold_proposal['proposed_columns'])})", expanded=True):
            col_rows = []
            for c in gold_proposal["proposed_columns"]:
                col_rows.append(
                    {
                        "Column": c["gold_column_name"],
                        "Type": c["logical_type"],
                        "Null?": "✓" if c["nullable"] else "—",
                        "BK": "🔑" if c.get("is_business_key") else "",
                        "PII": "🔒" if c.get("is_pii") else "",
                        "PHI": "🩺" if c.get("is_phi") else "",
                        "Anchor": (c.get("anchor_reference") or "")[:60],
                    }
                )
            st.dataframe(pd.DataFrame(col_rows), use_container_width=True, hide_index=True)
        with st.expander(
            f"Bronze→Gold mappings ({len(gold_proposal.get('bronze_to_gold_mappings', []))})",
            expanded=False,
        ):
            map_rows = []
            for m in gold_proposal.get("bronze_to_gold_mappings", []):
                srcs = ", ".join(m.get("bronze_source_columns", []))
                map_rows.append(
                    {
                        "Gold col": m["gold_column_name"],
                        "Kind": m["transform_kind"],
                        "Bronze sources": srcs,
                        "Transform SQL": m["transform_sql"][:80],
                    }
                )
            st.dataframe(pd.DataFrame(map_rows), use_container_width=True, hide_index=True)

        ga, gb, _ = st.columns([1, 1, 4])
        with ga:
            gold_save_draft = st.button(
                "💾 Save as DRAFT", key="gold_save_draft", use_container_width=True
            )
        with gb:
            gold_approve = st.button(
                "✅ Save & Approve → LIVE",
                type="primary",
                key="gold_approve",
                use_container_width=True,
            )
        if gold_save_draft:
            with _warehouse(readonly=False) as wh:
                try:
                    gid = persist_gold_proposal(
                        warehouse=wh,
                        proposal=gold_proposal,
                        actor=f"ui:{st.session_state.get('client_id', 'operator')}",
                        auto_submit=False,
                    )
                    st.success(f"Saved as DRAFT. gold_dataset_id=`{gid}`")
                    st.session_state.pop(GOLD_PROPOSAL_KEY, None)
                except Exception as exc:
                    st.error(f"Save failed: {exc}")
        if gold_approve:
            with _warehouse(readonly=False) as wh:
                try:
                    gid = persist_gold_proposal(
                        warehouse=wh,
                        proposal=gold_proposal,
                        actor=f"ui:{st.session_state.get('client_id', 'operator')}",
                        auto_submit=True,
                    )
                    approve_gold_schema(
                        warehouse=wh,
                        gold_dataset_id=gid,
                        actor=f"ui:{st.session_state.get('client_id', 'operator')}",
                    )
                    st.success(f"🎉 LIVE — Gold schema for `{selected_dataset_code}` is now LIVE.")
                    st.session_state.pop(GOLD_PROPOSAL_KEY, None)
                except Exception as exc:
                    st.error(f"Approve failed: {exc}")
                    st.exception(exc)
