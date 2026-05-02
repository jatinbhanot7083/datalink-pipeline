"""Gold Schema Designer — Phase 15.6 — top-of-funnel for the medallion architecture.

The canonical Gold (consumption-layer) schema for each of the 33 datasets is
designed here. Three modes converge on the same global Gold registry:

    1. AI Construct  — agent proposes Gold from Bronze + chosen anchor + RAG
    2. Manual Author — operator hand-builds via column grid
    3. Import        — paste DDL / YAML / FHIR profile / JSON Schema / DESCRIBE

Once a Gold schema is LIVE for a dataset, every client clones from it on
deploy via the Pipeline Architect; client-level overrides allowed.
"""

from __future__ import annotations

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
    page_title="Gold Schema Designer",
    page_icon="🥇",
    layout="wide",
    initial_sidebar_state="expanded",
)

from datalink.adapters.embeddings.router import get_embedder  # noqa: E402
from datalink.agents.gold_schema_designer import (  # noqa: E402
    GoldAnchor,
    approve_gold_schema,
    list_gold_datasets,
    persist_proposal,
    propose_gold_ai,
    propose_gold_import,
    propose_gold_manual,
)
from datalink.agents.llm_router import get_llm  # noqa: E402
from datalink.config.loader import load_settings  # noqa: E402
from datalink.memory import AgentMemoryStore  # noqa: E402
from datalink.quality.control import CONTROL_SCHEMA, create_control_tables  # noqa: E402
from datalink.ui._nav import render_sidebar, require_client  # noqa: E402
from datalink.ui._query import warehouse_ctx  # noqa: E402

render_sidebar(active="Gold Schema Designer")

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
      .gd-pill-ai {{background:#e0e7ff;color:#3730a3}}
      .gd-pill-manual {{background:#fef3c7;color:#92400e}}
      .gd-pill-import {{background:#d1fae5;color:#065f46}}
      .gd-stat {{font-size:1.6rem;font-weight:700;color:{_NAVY};}}
      .gd-stat-label {{font-size:.78rem;color:#64748b;text-transform:uppercase;
                       letter-spacing:.05em;font-weight:600;}}
      .gd-meta {{font-size:.85rem;color:#475569;margin-top:.4rem}}
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

st.markdown("# 🥇 Gold Schema Designer")
st.markdown(
    """
    <div class="gd-hero">
      <strong>The canonical Gold model for every dataset, designed once.</strong>
      Pick a dataset; choose an anchor (CATALOG / FHIR R4 / X12 / NCPDP /
      CMS / HEDIS); let the AI construct, hand-author, or import. Once LIVE
      every client clones it on deploy via the Pipeline Architect — with
      optional per-client overrides.
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
# Bootstrap CONTROL tables (idempotent)
# ---------------------------------------------------------------------------

with _warehouse(readonly=False) as wh:
    create_control_tables(wh)


# ---------------------------------------------------------------------------
# Status panel + dataset picker
# ---------------------------------------------------------------------------


@st.cache_data(ttl=30)  # type: ignore[misc]
def _list_bronze_datasets() -> list[dict[str, Any]]:
    """Return the 33 Bronze datasets — one row each."""
    with warehouse_ctx(readonly=True) as wh:
        return list(
            wh.query(
                f"SELECT dataset_code, display_name, category, default_frequency, "
                f"       total_fields, default_anchor "
                f"  FROM {CONTROL_SCHEMA}.global_bronze_catalog_datasets "
                f" WHERE is_active = TRUE ORDER BY display_name"
            )
        )


bronze_datasets = _list_bronze_datasets()
with _warehouse(readonly=True) as wh:
    all_gold = list_gold_datasets(wh)

# Stats row
st.markdown("## 📊 Gold registry status")
c1, c2, c3, c4 = st.columns(4)
c1.markdown(
    f'<div class="gd-card"><div class="gd-stat">{len(bronze_datasets)}</div>'
    f'<div class="gd-stat-label">Bronze datasets available</div></div>',
    unsafe_allow_html=True,
)
live_count = sum(1 for g in all_gold if g.get("status") == "LIVE")
c2.markdown(
    f'<div class="gd-card"><div class="gd-stat">{live_count}</div>'
    f'<div class="gd-stat-label">LIVE Gold schemas</div></div>',
    unsafe_allow_html=True,
)
draft_count = sum(1 for g in all_gold if g.get("status") in ("DRAFT", "PENDING_REVIEW"))
c3.markdown(
    f'<div class="gd-card"><div class="gd-stat">{draft_count}</div>'
    f'<div class="gd-stat-label">In progress (DRAFT / PENDING)</div></div>',
    unsafe_allow_html=True,
)
ai_count = sum(1 for g in all_gold if g.get("source") == "AI_CONSTRUCT")
c4.markdown(
    f'<div class="gd-card"><div class="gd-stat">{ai_count}</div>'
    f'<div class="gd-stat-label">AI-constructed (so far)</div></div>',
    unsafe_allow_html=True,
)


# ---------------------------------------------------------------------------
# Dataset + anchor pickers
# ---------------------------------------------------------------------------

st.markdown("## 🎯 Pick a dataset")

if not bronze_datasets:
    st.warning(
        "Bronze catalog is empty. Run "
        "`docker exec datalink-control-tower python3 /opt/datalink/scripts/load_product_catalog.py` "
        "to seed it."
    )
    st.stop()

ds_label_to_obj = {
    f"{d['display_name']}  ({d['total_fields']} fields, {d['category']})": d
    for d in bronze_datasets
}
col_ds, col_anchor = st.columns([3, 2])
with col_ds:
    selected_label = st.selectbox(
        "Dataset",
        options=list(ds_label_to_obj.keys()),
        index=0,
        help="Pick the Bronze dataset to design a Gold schema for.",
    )
    selected_dataset = ds_label_to_obj[selected_label]
    selected_dataset_code = str(selected_dataset["dataset_code"])
    selected_dataset_display = str(selected_dataset["display_name"])
with col_anchor:
    anchor_options = [a.value for a in GoldAnchor]
    default_anchor = str(selected_dataset.get("default_anchor") or "CATALOG_ANCHOR")
    default_idx = anchor_options.index(default_anchor) if default_anchor in anchor_options else 0
    gold_anchor = st.selectbox(
        "Gold anchor",
        options=anchor_options,
        index=default_idx,
        help="CATALOG_ANCHOR = vendor mapping spec is the truth. Specialized anchors "
        "(FHIR/X12/etc.) override when applicable.",
    )

# Show prior LIVE Gold for this dataset, if any
prior_live = [
    g
    for g in all_gold
    if g.get("dataset_code") == selected_dataset_code and g.get("status") == "LIVE"
]
if prior_live:
    g = prior_live[0]
    st.markdown(
        f"""
        <div class="gd-card">
          <span class="gd-pill gd-pill-live">LIVE</span>
          <span class="gd-pill gd-pill-{str(g.get('source', '')).split('_')[0].lower()}">{g.get('source')}</span>
          A LIVE Gold schema already exists for <strong>{selected_dataset_display}</strong>:
          <code>{g['gold_table_name']}</code> v{g['version']}, anchor=<code>{g['gold_anchor']}</code>.
          Approving a new design archives this one.
        </div>
        """,
        unsafe_allow_html=True,
    )
else:
    st.info(
        f"No Gold schema exists yet for **{selected_dataset_display}**. "
        f"Pick a mode below to design one."
    )


# ---------------------------------------------------------------------------
# 3-mode tab interface
# ---------------------------------------------------------------------------

tab_ai, tab_manual, tab_import = st.tabs(["🤖 AI Construct", "✏️ Manual Author", "📥 Import"])

PROPOSAL_KEY = f"gd_proposal_{selected_dataset_code}"

# === Tab 1: AI Construct =====================================================
with tab_ai:
    st.markdown("### 🤖 AI Construct")
    st.markdown(
        f"Agent reads the {selected_dataset_display} Bronze catalog "
        f"({selected_dataset['total_fields']} fields), pulls RAG chunks from the "
        f"{gold_anchor} corpus, and proposes a canonical Gold schema."
    )
    if st.button("🚀 Propose Gold schema (AI)", type="primary", key="ai_propose"):
        with (
            st.spinner(
                f"Agent constructing Gold schema for {selected_dataset_display} ({gold_anchor})..."
            ),
            _warehouse(readonly=False) as wh,
        ):
            settings = load_settings()
            llm = get_llm(settings)
            memory: AgentMemoryStore | None = None
            try:
                embedder = get_embedder()
                memory = AgentMemoryStore(embedder=embedder)
            except Exception as e:
                st.warning(f"RAG unavailable; agent will run without grounding. ({e})")
                memory = None

            try:
                proposal = propose_gold_ai(
                    llm=llm,
                    warehouse=wh,
                    memory=memory,
                    dataset_code=selected_dataset_code,
                    gold_anchor=gold_anchor,
                    actor=f"ui:{st.session_state.get('client_id', 'operator')}",
                )
                st.session_state[PROPOSAL_KEY] = proposal
                st.success(
                    f"Proposal ready — {len(proposal['proposed_columns'])} Gold columns, "
                    f"{len(proposal['bronze_to_gold_mappings'])} mappings, "
                    f"{proposal['tokens_used']} tokens, {proposal['duration_ms']}ms."
                )
            except Exception as exc:
                st.error(f"Agent failed: {type(exc).__name__}: {exc}")
                st.exception(exc)

# === Tab 2: Manual Author ====================================================
with tab_manual:
    st.markdown("### ✏️ Manual Author")
    st.markdown(
        f"Hand-author the Gold column list for {selected_dataset_display}. "
        "Add rows for each canonical column. Mappings (Bronze→Gold transform "
        "SQL) can be filled in here too."
    )
    default_table_name = selected_dataset_code
    manual_table_name = st.text_input(
        "Gold table name",
        value=default_table_name,
        help="snake_case singular noun, e.g. `member`, `claim`.",
    )

    # Column grid
    if "gd_manual_cols" not in st.session_state:
        st.session_state["gd_manual_cols"] = pd.DataFrame(
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
        st.session_state["gd_manual_cols"],
        num_rows="dynamic",
        use_container_width=True,
        column_config={
            "logical_type": st.column_config.SelectboxColumn(
                options=["TEXT", "INTEGER", "DECIMAL", "DATE", "TIMESTAMP", "BOOLEAN"],
            ),
        },
        key="manual_cols_editor",
    )
    st.session_state["gd_manual_cols"] = cols_df

    if st.button("📋 Capture as proposal", key="manual_propose"):
        # Drop empty rows
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
            try:
                proposal = propose_gold_manual(
                    dataset_code=selected_dataset_code,
                    gold_table_name=manual_table_name,
                    columns=clean_cols,
                    mappings=[],  # operator can wire later
                    gold_anchor=gold_anchor,
                    rationale="Operator hand-authored via Manual mode.",
                )
                st.session_state[PROPOSAL_KEY] = proposal
                st.success(f"Captured manual proposal — {len(clean_cols)} columns.")
            except Exception as exc:
                st.error(f"Capture failed: {exc}")

# === Tab 3: Import ===========================================================
with tab_import:
    st.markdown("### 📥 Import")
    st.markdown(
        "Paste an existing schema definition. The parser converts it into "
        "the canonical proposal shape; review the resulting columns + add "
        "Bronze→Gold mappings on the next page."
    )
    import_format = st.selectbox(
        "Format",
        options=["DDL_SQL", "DBT_YAML", "FHIR_PROFILE_JSON", "JSON_SCHEMA", "SNOWFLAKE_DESCRIBE"],
        help="DDL_SQL: paste a CREATE TABLE statement. "
        "DBT_YAML: paste a dbt schema.yml model entry. "
        "FHIR_PROFILE_JSON: paste a FHIR R4 StructureDefinition. "
        "JSON_SCHEMA: paste a JSON Schema. "
        "SNOWFLAKE_DESCRIBE: paste DESCRIBE TABLE output.",
    )
    import_text = st.text_area(
        "Paste schema definition",
        height=260,
        placeholder="Paste your schema here...",
    )
    if st.button("📥 Parse + capture as proposal", key="import_propose"):
        if not import_text.strip():
            st.warning("Paste a schema first.")
        else:
            try:
                proposal = propose_gold_import(
                    dataset_code=selected_dataset_code,
                    import_format=import_format,
                    text=import_text,
                    gold_anchor=gold_anchor,
                )
                st.session_state[PROPOSAL_KEY] = proposal
                st.success(
                    f"Parsed — {len(proposal['proposed_columns'])} columns. "
                    f"Note: Bronze→Gold mappings are empty; wire them via Manual edits or re-run AI Construct."
                )
            except Exception as exc:
                st.error(f"Import failed: {type(exc).__name__}: {exc}")


# ---------------------------------------------------------------------------
# Proposal display + persist + approve
# ---------------------------------------------------------------------------

proposal = st.session_state.get(PROPOSAL_KEY)
if proposal:
    st.markdown("---")
    st.markdown(f"## 📋 Gold proposal — `{proposal['proposed_gold_table_name']}`")
    mode_pill_class = {
        "AI_CONSTRUCT": "gd-pill-ai",
        "MANUAL_AUTHOR": "gd-pill-manual",
        "IMPORT": "gd-pill-import",
    }.get(str(proposal.get("designer_mode", "")), "gd-pill-ai")
    st.markdown(
        f"""
        <div class="gd-card">
          <span class="gd-pill {mode_pill_class}">{proposal.get('designer_mode')}</span>
          <span class="gd-pill gd-pill-draft">DRAFT (NEW)</span>
          <span class="gd-pill" style="background:#fde68a;color:#78350f;">ANCHOR: {proposal.get('gold_anchor')}</span>
          <div class="gd-meta">
            Tokens: {proposal.get('tokens_used', 0)}
            &nbsp;·&nbsp; Latency: {proposal.get('duration_ms', 0)}ms
            &nbsp;·&nbsp; Model: {proposal.get('proposer_model') or '—'}
            &nbsp;·&nbsp; Columns: {len(proposal['proposed_columns'])}
            &nbsp;·&nbsp; Mappings: {len(proposal.get('bronze_to_gold_mappings', []))}
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # Rationale + Silver pattern recommendation
    rec = proposal.get("silver_pattern_recommendation") or {}
    rec_color = "#fef3c7" if rec.get("is_overkill_flag") else "#dcfce7"
    rec_text_color = "#92400e" if rec.get("is_overkill_flag") else "#166534"
    st.markdown(
        f"""
        <div class="gd-card">
          <h4>Rationale</h4>
          <p>{proposal.get('rationale', '')}</p>
          <h4>Silver pattern recommendation</h4>
          <div style="background:{rec_color};color:{rec_text_color};padding:.5rem .75rem;border-radius:6px;">
            <strong>Pattern:</strong> {rec.get('recommended_pattern', '—')}
            &nbsp;·&nbsp; <strong>Overkill flag:</strong> {rec.get('is_overkill_flag', False)}
            <br><br>
            {rec.get('reasoning', '')}
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # Column display
    st.markdown("### 🥇 Proposed Gold columns")
    col_rows = []
    for c in proposal["proposed_columns"]:
        col_rows.append(
            {
                "Column": c["gold_column_name"],
                "Type": c["logical_type"],
                "Null?": "✓" if c["nullable"] else "—",
                "BK": "🔑" if c.get("is_business_key") else "",
                "PII": "🔒" if c.get("is_pii") else "",
                "PHI": "🩺" if c.get("is_phi") else "",
                "Anchor cite": (c.get("anchor_reference") or "")[:60],
                "Description": (c.get("description") or "")[:80],
            }
        )
    st.dataframe(pd.DataFrame(col_rows), use_container_width=True, hide_index=True)

    # Mappings display
    if proposal.get("bronze_to_gold_mappings"):
        st.markdown("### 🔁 Bronze → Gold mappings")
        map_rows = []
        for m in proposal["bronze_to_gold_mappings"]:
            srcs = ", ".join(m.get("bronze_source_columns", []))
            map_rows.append(
                {
                    "Gold column": m["gold_column_name"],
                    "Kind": m["transform_kind"],
                    "Bronze sources": srcs,
                    "Transform SQL": m["transform_sql"][:100],
                    "Confidence": f"{m.get('confidence', 0.9):.2f}",
                }
            )
        st.dataframe(pd.DataFrame(map_rows), use_container_width=True, hide_index=True)
    else:
        st.markdown("### 🔁 Bronze → Gold mappings")
        st.info(
            "No mappings yet — Manual / Import modes start without them. "
            "Add them on a future iteration or re-run AI mode to generate them."
        )

    # Persist + Approve buttons
    st.markdown("---")
    st.markdown("### 🚢 Persist / Approve")
    a, b, c = st.columns([1, 1, 4])
    with a:
        save_draft = st.button("💾 Save as DRAFT", key="save_draft", use_container_width=True)
    with b:
        save_and_approve = st.button(
            "✅ Save & Approve → LIVE",
            type="primary",
            key="save_approve",
            use_container_width=True,
            help="Persist as PENDING_REVIEW then immediately approve. Archives any prior LIVE for this dataset.",
        )
    notes = st.text_area("Notes (audit trail)", placeholder="Optional", height=70)

    if save_draft:
        with _warehouse(readonly=False) as wh:
            try:
                gid = persist_proposal(
                    warehouse=wh,
                    proposal=proposal,
                    actor=f"ui:{st.session_state.get('client_id', 'operator')}",
                    notes=notes,
                    auto_submit=False,
                )
                st.success(f"Saved as DRAFT. gold_dataset_id=`{gid}`")
                st.session_state.pop(PROPOSAL_KEY, None)
            except Exception as exc:
                st.error(f"Save failed: {exc}")
                st.exception(exc)

    if save_and_approve:
        with _warehouse(readonly=False) as wh:
            try:
                gid = persist_proposal(
                    warehouse=wh,
                    proposal=proposal,
                    actor=f"ui:{st.session_state.get('client_id', 'operator')}",
                    notes=notes,
                    auto_submit=True,
                )
                result = approve_gold_schema(
                    warehouse=wh,
                    gold_dataset_id=gid,
                    actor=f"ui:{st.session_state.get('client_id', 'operator')}",
                    notes=notes,
                )
                st.success(
                    f"🎉 LIVE — `{proposal['proposed_gold_table_name']}` for "
                    f"`{result['dataset_code']}` is now the canonical Gold schema."
                )
                st.session_state.pop(PROPOSAL_KEY, None)
            except Exception as exc:
                st.error(f"Approve failed: {exc}")
                st.exception(exc)
else:
    st.markdown("---")
    st.info("👆 Pick a mode above and run it to see a proposal here.")


# ---------------------------------------------------------------------------
# Registry panel — every Gold schema ever registered
# ---------------------------------------------------------------------------

st.markdown("---")
st.markdown("## 🗂 Gold registry")
with _warehouse(readonly=True) as wh:
    all_rows = list_gold_datasets(wh)

if not all_rows:
    st.info("No Gold schemas registered yet.")
else:
    reg_rows = []
    for g in all_rows:
        reg_rows.append(
            {
                "Dataset": g["dataset_code"],
                "Gold table": g["gold_table_name"],
                "Status": g["status"],
                "Version": g["version"],
                "Anchor": g["gold_anchor"],
                "Source": g["source"],
                "Tokens": g.get("ai_token_count") or 0,
                "Created": g["created_at"],
                "Approved": g["approved_at"] or "—",
                "Gold ID": str(g["gold_dataset_id"])[:8] + "…",
            }
        )
    st.dataframe(pd.DataFrame(reg_rows), use_container_width=True, hide_index=True)
