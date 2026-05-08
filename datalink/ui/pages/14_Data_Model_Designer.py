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
      /* Phase 16.7 — global-template pills */
      .dmd-pill {{display:inline-block;padding:.15rem .55rem;border-radius:12px;
                  font-size:.72rem;font-weight:700;letter-spacing:.04em;margin-right:.4rem}}
      .dmd-pill-live {{background:#dcfce7;color:#166534}}
      .dmd-pill-dev {{background:#fef3c7;color:#92400e}}
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

# Phase 17.4 UX: capture return value so the dashboard panel below can
# filter subscriptions by selected client (None = show all clients).
selected_client_filter = require_client()


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


@st.cache_data(ttl=20)  # type: ignore
def _cached_silver_rows() -> list[dict[str, Any]]:
    with warehouse_ctx(readonly=True) as wh:
        return list_silver_datasets(wh)


@st.cache_data(ttl=20)  # type: ignore
def _cached_gold_rows() -> list[dict[str, Any]]:
    with warehouse_ctx(readonly=True) as wh:
        return list_gold_datasets(wh)


@st.cache_data(ttl=20)  # type: ignore
def _cached_distinct_clients() -> int:
    with warehouse_ctx(readonly=True) as wh:
        rows = list(
            wh.query(
                f"SELECT COUNT(DISTINCT client_id) AS c FROM "
                f"{CONTROL_SCHEMA}.client_pipeline_instances WHERE status = 'LIVE'"
            )
        )
        return int(rows[0]["c"] or 0) if rows else 0


@st.cache_data(ttl=20)  # type: ignore
def _cached_silver_schema(silver_dataset_id: str) -> dict[str, Any]:
    with warehouse_ctx(readonly=True) as wh:
        return get_silver_schema(wh, silver_dataset_id)


@st.cache_data(ttl=20)  # type: ignore
def _cached_gold_schema(gold_dataset_id: str) -> dict[str, Any]:
    from datalink.agents.gold_schema_designer import get_gold_schema as _gget

    with warehouse_ctx(readonly=True) as wh:
        return _gget(wh, gold_dataset_id)


bronze_datasets = _list_bronze_datasets()
all_silver = _cached_silver_rows()
all_gold = _cached_gold_rows()


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
    expanded=True,
):
    if not bronze_datasets:
        st.warning(
            "Bronze catalog is empty. Run `python3 scripts/load_product_catalog.py` to seed it."
        )
    else:
        rows = [_readiness_row(d) for d in bronze_datasets]
        df = pd.DataFrame(rows)
        styled = df.style.map(_color_status, subset=["Bronze", "Silver", "Gold"])
        st.dataframe(styled, use_container_width=True, hide_index=True, height=520)


# ═════════════════════════════════════════════════════════════════════════════
# Phase 17.5 UX — Schema Inventory (unified CRUD grid for everything authored)
#
# One grid. Every Silver + Gold schema, every status, every client/scope.
# Click a row → inline action panel below shows the right buttons for that
# row's status. Operators stop hunting for what they created.
# ═════════════════════════════════════════════════════════════════════════════

st.markdown("## 📋 Schema Inventory")
st.caption(
    "Every authored Silver + Gold schema across all clients and statuses. "
    "Click a row to act on it — promote / archive / view / edit. "
    "Filters narrow the view; defaults show everything."
)


# Phase 17.5.1 — wrap the entire inventory section in @st.fragment so a
# row-click only re-runs THIS section (not the whole page). The medallion
# tiles, drill-down readiness table, and Gold Versioning Dashboard all
# render once on page load and don't get re-fired on every click.
#
# Inventory query is cached for 8s — repeated reads on quick re-clicks
# hit the cache, not Snowflake.
@st.cache_data(ttl=8, show_spinner=False)
def _inventory_fetch_rows() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Read Silver + Gold inventory in two queries. Returns (silver, gold)."""
    with _warehouse(readonly=True) as wh:
        try:
            silver = list(
                wh.query(
                    f"""
                    SELECT silver_dataset_id   AS id,
                           'Silver'             AS layer,
                           dataset_code,
                           version,
                           silver_pattern       AS spec,
                           status,
                           scope_owner,
                           created_by,
                           created_at,
                           approved_by,
                           approved_at,
                           archived_at
                      FROM {CONTROL_SCHEMA}.global_silver_schema_datasets
                    """
                )
            )
        except Exception:
            silver = []
        try:
            gold = list(
                wh.query(
                    f"""
                    SELECT gold_dataset_id     AS id,
                           'Gold'               AS layer,
                           dataset_code,
                           version,
                           gold_anchor          AS spec,
                           status,
                           scope_owner,
                           created_by,
                           created_at,
                           approved_by,
                           approved_at,
                           archived_at
                      FROM {CONTROL_SCHEMA}.global_gold_schema_datasets
                    """
                )
            )
        except Exception:
            gold = []
    return silver, gold


@st.fragment
def _render_schema_inventory() -> None:
    """Schema inventory grid + action panel. Self-contained — st.fragment
    isolates re-runs to this function so row-click selections don't drag
    the whole page through another Snowflake round-trip."""
    # Filter row — All-by-default per user spec.
    _inv_f1, _inv_f2, _inv_f3, _inv_f4 = st.columns([1, 1, 1, 2])
    with _inv_f1:
        _inv_layer_filter = st.selectbox(
            "Layer", ["All", "Silver", "Gold"], key="inv_layer_filter"
        )
    with _inv_f2:
        _inv_status_filter = st.selectbox(
            "Status",
            ["All", "DRAFT", "PENDING_REVIEW", "APPROVED", "LIVE", "ARCHIVED", "REJECTED"],
            key="inv_status_filter",
        )
    with _inv_f3:
        _inv_scope_filter = st.selectbox(
            "Scope",
            ["All", "GLOBAL_CORP", "Client-scoped only"],
            key="inv_scope_filter",
        )
    with _inv_f4:
        st.caption(
            "**Why this is here:** so you never lose track of a draft you "
            "authored. Every row, every status, one grid. Row clicks re-run "
            "ONLY this section — page is otherwise frozen for performance."
        )

    _silver_inv, _gold_inv = _inventory_fetch_rows()
    _all_inv_rows = list(_silver_inv) + list(_gold_inv)
    _all_inv_rows.sort(key=lambda r: str(r.get("created_at") or ""), reverse=True)
    # Apply filters in-memory (cheap — no Snowflake round-trip).
    def _row_passes(r: dict[str, Any]) -> bool:
        if _inv_layer_filter != "All" and r.get("layer") != _inv_layer_filter:
            return False
        if _inv_status_filter != "All" and str(r.get("status")) != _inv_status_filter:
            return False
        if _inv_scope_filter == "GLOBAL_CORP" and str(r.get("scope_owner")) not in (
            "GLOBAL_CORP",
            "__global__",
        ):
            return False
        return not (
            _inv_scope_filter == "Client-scoped only"
            and str(r.get("scope_owner")) in ("GLOBAL_CORP", "__global__")
        )

    _filtered_inv = [r for r in _all_inv_rows if _row_passes(r)]

    if not _filtered_inv:
        st.info(
            "📭 No schemas match the current filters. Author one in the layer-specific "
            "sections below (Silver / Gold tabs) — it'll appear here automatically."
        )
        return

    # Build display table (id column hidden — kept aside for action panel).
    def _status_pill(s: str | None) -> str:
        return {
            "DRAFT": "📝 DRAFT",
            "PENDING_REVIEW": "🟡 PENDING_REVIEW",
            "APPROVED": "✅ APPROVED",
            "LIVE": "🟢 LIVE",
            "ARCHIVED": "🗄️ ARCHIVED",
            "REJECTED": "🔴 REJECTED",
        }.get(str(s or ""), str(s or "?"))

    _inv_view = []
    for r in _filtered_inv:
        _inv_view.append(
            {
                "Layer": r.get("layer"),
                "Dataset": r.get("dataset_code"),
                "v": f"v{r.get('version')}",
                "Status": _status_pill(r.get("status")),
                "Spec": str(r.get("spec") or "—"),
                "Scope": str(r.get("scope_owner") or "—"),
                "Created by": str(r.get("created_by") or "—"),
                "Created": str(r.get("created_at") or "")[:19],
                "Approved": (
                    str(r.get("approved_at") or "—")[:19]
                    if r.get("approved_at")
                    else "—"
                ),
            }
        )

    _inv_df = pd.DataFrame(_inv_view)
    _selection = st.dataframe(
        _inv_df,
        use_container_width=True,
        hide_index=True,
        on_select="rerun",  # confined to fragment via @st.fragment decorator
        selection_mode="single-row",
        height=320,
        key="schema_inventory_grid",
    )

    _sel_idx_list = _selection.selection.get("rows", []) if _selection else []
    if not _sel_idx_list:
        st.caption("_(Click any row above to see action buttons.)_")
        return

    _sel_idx = _sel_idx_list[0]
    _sel = _filtered_inv[_sel_idx]

    st.markdown(
        f"### Actions for **{_sel['layer']} · {_sel['dataset_code']} · "
        f"v{_sel['version']}** — current status: {_status_pill(_sel.get('status'))}"
    )

    _next_status = {
        "DRAFT": "PENDING_REVIEW",
        "PENDING_REVIEW": "APPROVED",
        "APPROVED": "LIVE",
    }.get(str(_sel.get("status")))

    _ac1, _ac2, _ac3, _ac4, _ac5 = st.columns([1.2, 1.4, 1, 1, 1.2])

    with _ac1:
        _view_clicked = st.button(
            "👁️  View columns",
            use_container_width=True,
            key="inv_act_view",
            help="Show all columns + types + PII/PHI flags for this version.",
        )

    with _ac2:
        if _next_status:
            _promote_clicked = st.button(
                f"⬆️  Promote → {_next_status}",
                use_container_width=True,
                type="primary",
                key="inv_act_promote",
                help=f"Advance from {_sel.get('status')} to {_next_status}.",
            )
        else:
            _promote_clicked = False
            st.button(
                "⬆️  Promote",
                use_container_width=True,
                disabled=True,
                help=f"Cannot promote from {_sel.get('status')} (terminal state).",
                key="inv_act_promote_disabled",
            )

    with _ac3:
        if str(_sel.get("status")) != "ARCHIVED":
            _archive_clicked = st.button(
                "🗄️  Archive",
                use_container_width=True,
                key="inv_act_archive",
                help="Mark as ARCHIVED. Audit row preserved. Soft delete.",
            )
        else:
            _archive_clicked = False
            st.button(
                "🗄️  Archive",
                use_container_width=True,
                disabled=True,
                key="inv_act_archive_disabled",
            )

    with _ac4:
        _layer_param = "silver" if _sel["layer"] == "Silver" else "gold"
        _edit_href = (
            f"/Data_Model_Designer?dataset={_sel['dataset_code']}"
            f"&layer={_layer_param}"
        )
        st.markdown(
            f'<a href="{_edit_href}" target="_self" '
            f'style="display:block;background:#1d4ed8;color:#fff;'
            f"padding:.45rem .5rem;border-radius:6px;text-decoration:none;"
            f'text-align:center;font-weight:600;font-size:.88rem;">'
            f"✏️ Edit</a>",
            unsafe_allow_html=True,
        )

    with _ac5:
        # Clone is enabled for any non-terminal source row.
        # Industry pattern: clone-from-template skips LLM cost; this is THE
        # primary way new clients get a Silver/Gold without re-authoring.
        _clone_eligible = str(_sel.get("status")) not in ("ARCHIVED", "REJECTED")
        _clone_clicked = st.button(
            "📦  Clone to client",
            use_container_width=True,
            disabled=not _clone_eligible,
            key="inv_act_clone",
            help=(
                "Create a NEW DRAFT row scoped to a target client, "
                "copying all columns/tables/mappings. Zero LLM cost."
                if _clone_eligible
                else f"Cannot clone from {_sel.get('status')}."
            ),
        )

    # ── Action handlers ─────────────────────────────────────────────────
    if _promote_clicked and _next_status:
        _table = (
            "global_silver_schema_datasets"
            if _sel["layer"] == "Silver"
            else "global_gold_schema_datasets"
        )
        _id_col = (
            "silver_dataset_id" if _sel["layer"] == "Silver" else "gold_dataset_id"
        )
        _set_clauses = [f"status = '{_next_status}'"]
        if _next_status == "APPROVED":
            _set_clauses.append("approved_at = CURRENT_TIMESTAMP()")
            _set_clauses.append("approved_by = 'ui:inventory'")
        try:
            with _warehouse(readonly=False) as _wh_op:
                _wh_op.execute(
                    f"UPDATE {CONTROL_SCHEMA}.{_table} "
                    f"SET {', '.join(_set_clauses)} "
                    f"WHERE {_id_col} = $id",
                    {"id": str(_sel["id"])},
                )
            _inventory_fetch_rows.clear()  # bust cache so next read sees new status
            st.toast(f"✅ Promoted to {_next_status}", icon="🚀")
            st.rerun(scope="fragment")
        except Exception as _exc:
            st.error(f"Promotion failed: {_exc}")

    if _archive_clicked:
        _table = (
            "global_silver_schema_datasets"
            if _sel["layer"] == "Silver"
            else "global_gold_schema_datasets"
        )
        _id_col = (
            "silver_dataset_id" if _sel["layer"] == "Silver" else "gold_dataset_id"
        )
        try:
            with _warehouse(readonly=False) as _wh_op:
                _wh_op.execute(
                    f"UPDATE {CONTROL_SCHEMA}.{_table} "
                    f"SET status = 'ARCHIVED', "
                    f"    archived_at = CURRENT_TIMESTAMP() "
                    f"WHERE {_id_col} = $id",
                    {"id": str(_sel["id"])},
                )
            _inventory_fetch_rows.clear()
            st.toast(f"🗄️ Archived {_sel['layer']} v{_sel['version']}", icon="✅")
            st.rerun(scope="fragment")
        except Exception as _exc:
            st.error(f"Archive failed: {_exc}")

    if _view_clicked:
        try:
            with _warehouse(readonly=True) as _wh_cols:
                if _sel["layer"] == "Silver":
                    _col_rows = list(
                        _wh_cols.query(
                            f"SELECT column_order, gold_column_name AS column_name, "
                            f"       logical_type, nullable, is_business_key, "
                            f"       is_pii, is_phi, description "
                            f"FROM {CONTROL_SCHEMA}.global_silver_schema_columns "
                            f"WHERE silver_dataset_id = $id "
                            f"ORDER BY column_order",
                            {"id": str(_sel["id"])},
                        )
                    )
                else:
                    _col_rows = list(
                        _wh_cols.query(
                            f"SELECT column_order, gold_column_name AS column_name, "
                            f"       logical_type, nullable, is_business_key, "
                            f"       is_pii, is_phi, description "
                            f"FROM {CONTROL_SCHEMA}.global_gold_schema_fields "
                            f"WHERE gold_dataset_id = $id "
                            f"ORDER BY column_order",
                            {"id": str(_sel["id"])},
                        )
                    )
            if _col_rows:
                _col_view = [
                    {
                        "#": c.get("column_order"),
                        "Column": c.get("column_name"),
                        "Type": c.get("logical_type"),
                        "Null?": "✓" if c.get("nullable") else "—",
                        "BK": "🔑" if c.get("is_business_key") else "",
                        "PII": "🔒" if c.get("is_pii") else "",
                        "PHI": "🩺" if c.get("is_phi") else "",
                        "Description": (c.get("description") or "")[:80],
                    }
                    for c in _col_rows
                ]
                st.markdown(f"##### 📋 Columns ({len(_col_rows)})")
                st.dataframe(
                    pd.DataFrame(_col_view),
                    use_container_width=True,
                    hide_index=True,
                )
            else:
                st.info("No columns registered for this version.")
        except Exception as _exc:
            st.error(f"Could not load columns: {_exc}")

    # ── Clone-to-client handler ─────────────────────────────────────────
    # When user clicks 📦 Clone, stash that intent in session_state so the
    # form persists across the toast-driven fragment rerun.
    _clone_form_key = f"__inv_clone_form_open_{_sel['id']}"
    if _clone_clicked:
        st.session_state[_clone_form_key] = True

    if st.session_state.get(_clone_form_key):
        st.markdown("---")
        st.markdown(
            f"##### 📦 Clone {_sel['layer']} `{_sel['dataset_code']}` "
            f"v{_sel['version']} → target client"
        )
        with st.form(key=f"clone_form_{_sel['id']}"):
            _cf1, _cf2 = st.columns([2, 1])
            with _cf1:
                _target_client = st.text_input(
                    "Target client_id",
                    placeholder="e.g. aetna, bcbs, humana",
                    help="Lowercase, no spaces. Will become scope_owner on "
                    "the new DRAFT row.",
                )
            with _cf2:
                _confirm_clone = st.form_submit_button(
                    "📦 Clone now",
                    type="primary",
                    use_container_width=True,
                )
            _cancel_clone = st.form_submit_button(
                "Cancel",
                use_container_width=True,
            )

        if _cancel_clone:
            st.session_state.pop(_clone_form_key, None)
            st.rerun(scope="fragment")

        if _confirm_clone:
            _t = (_target_client or "").strip().lower()
            if not _t:
                st.error("Target client_id is required.")
            elif _t in ("global_corp", "__global__"):
                st.error("Target cannot be GLOBAL_CORP — pick a real client.")
            else:
                # Layer-specific clone: copies header row + all child rows
                # under fresh UUIDs so the new client gets an independent
                # DRAFT to edit.
                try:
                    import uuid

                    with _warehouse(readonly=False) as _wh_clone:
                        if _sel["layer"] == "Silver":
                            new_silver_id = str(uuid.uuid4())
                            # 1. Header row — start at v1 under target client.
                            _wh_clone.execute(
                                f"INSERT INTO {CONTROL_SCHEMA}.global_silver_schema_datasets "
                                f"(silver_dataset_id, dataset_code, silver_pattern, "
                                f" version, status, silver_anchor, source, scope_owner, "
                                f" forked_from_global_version, "
                                f" notes, created_by, created_at) "
                                f"SELECT $newid, dataset_code, silver_pattern, "
                                f"       1, 'DRAFT', silver_anchor, 'CLONE', $owner, "
                                f"       version, "
                                f"       'Cloned from ' || scope_owner || ' v' || version || "
                                f"           ' on ' || CAST(CURRENT_TIMESTAMP() AS VARCHAR), "
                                f"       $by, CURRENT_TIMESTAMP() "
                                f"FROM {CONTROL_SCHEMA}.global_silver_schema_datasets "
                                f"WHERE silver_dataset_id = $srcid",
                                {
                                    "newid": new_silver_id,
                                    "owner": _t,
                                    "by": "ui:inventory:clone",
                                    "srcid": str(_sel["id"]),
                                },
                            )

                            # 2. Tables — remap silver_table_id → new UUID
                            _src_tables = list(
                                _wh_clone.query(
                                    f"SELECT * FROM {CONTROL_SCHEMA}.global_silver_schema_tables "
                                    f"WHERE silver_dataset_id = $sid",
                                    {"sid": str(_sel["id"])},
                                )
                            )
                            _table_remap: dict[str, str] = {}
                            for t in _src_tables:
                                old_tid = str(
                                    t.get("silver_table_id") or t.get("SILVER_TABLE_ID")
                                )
                                new_tid = str(uuid.uuid4())
                                _table_remap[old_tid] = new_tid
                                _wh_clone.execute(
                                    f"INSERT INTO {CONTROL_SCHEMA}.global_silver_schema_tables "
                                    f"(silver_table_id, silver_dataset_id, dataset_code, "
                                    f" table_name, table_kind, parent_silver_table_id, "
                                    f" business_keys_json, linked_hub_ids_json, table_order, "
                                    f" description, created_at) "
                                    f"VALUES ($tid, $sid, $ds, $tn, $tk, $ptid, "
                                    f"        $bk, $lh, $ord, $desc, CURRENT_TIMESTAMP())",
                                    {
                                        "tid": new_tid,
                                        "sid": new_silver_id,
                                        "ds": t.get("dataset_code") or t.get("DATASET_CODE"),
                                        "tn": t.get("table_name") or t.get("TABLE_NAME"),
                                        "tk": t.get("table_kind") or t.get("TABLE_KIND"),
                                        "ptid": t.get("parent_silver_table_id")
                                        or t.get("PARENT_SILVER_TABLE_ID"),
                                        "bk": t.get("business_keys_json")
                                        or t.get("BUSINESS_KEYS_JSON"),
                                        "lh": t.get("linked_hub_ids_json")
                                        or t.get("LINKED_HUB_IDS_JSON"),
                                        "ord": t.get("table_order") or t.get("TABLE_ORDER"),
                                        "desc": t.get("description") or t.get("DESCRIPTION"),
                                    },
                                )

                            # 3. Columns — remap silver_column_id, point to new silver_table_id
                            _src_cols = list(
                                _wh_clone.query(
                                    f"SELECT * FROM {CONTROL_SCHEMA}.global_silver_schema_columns "
                                    f"WHERE silver_dataset_id = $sid",
                                    {"sid": str(_sel["id"])},
                                )
                            )
                            _col_remap: dict[str, str] = {}
                            for c in _src_cols:
                                old_cid = str(
                                    c.get("silver_column_id") or c.get("SILVER_COLUMN_ID")
                                )
                                new_cid = str(uuid.uuid4())
                                _col_remap[old_cid] = new_cid
                                old_tid = str(
                                    c.get("silver_table_id") or c.get("SILVER_TABLE_ID")
                                )
                                _wh_clone.execute(
                                    f"INSERT INTO {CONTROL_SCHEMA}.global_silver_schema_columns "
                                    f"(silver_column_id, silver_table_id, silver_dataset_id, "
                                    f" column_order, column_name, logical_type, nullable, "
                                    f" is_business_key, is_hash_key, is_hash_diff, is_pii, "
                                    f" is_phi, description, registered_at) "
                                    f"VALUES ($cid, $tid, $sid, $ord, $name, $type, $null, "
                                    f"        $bk, $hk, $hd, $pii, $phi, $desc, "
                                    f"        CURRENT_TIMESTAMP())",
                                    {
                                        "cid": new_cid,
                                        "tid": _table_remap.get(old_tid, old_tid),
                                        "sid": new_silver_id,
                                        "ord": c.get("column_order") or c.get("COLUMN_ORDER"),
                                        "name": c.get("column_name") or c.get("COLUMN_NAME"),
                                        "type": c.get("logical_type") or c.get("LOGICAL_TYPE"),
                                        "null": c.get("nullable") or c.get("NULLABLE"),
                                        "bk": c.get("is_business_key")
                                        or c.get("IS_BUSINESS_KEY"),
                                        "hk": c.get("is_hash_key") or c.get("IS_HASH_KEY"),
                                        "hd": c.get("is_hash_diff") or c.get("IS_HASH_DIFF"),
                                        "pii": c.get("is_pii") or c.get("IS_PII"),
                                        "phi": c.get("is_phi") or c.get("IS_PHI"),
                                        "desc": c.get("description") or c.get("DESCRIPTION"),
                                    },
                                )

                            # 4. Bronze→Silver mappings — point to new silver_column_id
                            _src_maps = list(
                                _wh_clone.query(
                                    f"SELECT * FROM {CONTROL_SCHEMA}.bronze_to_silver_mappings "
                                    f"WHERE silver_dataset_id = $sid",
                                    {"sid": str(_sel["id"])},
                                )
                            )
                            for m in _src_maps:
                                old_mid_col = str(
                                    m.get("silver_column_id") or m.get("SILVER_COLUMN_ID")
                                )
                                _wh_clone.execute(
                                    f"INSERT INTO {CONTROL_SCHEMA}.bronze_to_silver_mappings "
                                    f"(mapping_id, silver_column_id, silver_dataset_id, "
                                    f" silver_table_name, silver_column_name, "
                                    f" bronze_source_columns, transform_kind, transform_sql, "
                                    f" rationale, confidence, created_by, created_at) "
                                    f"VALUES ($mid, $cid, $sid, $tn, $cn, $bsc, $tk, $sql, "
                                    f"        $rat, $conf, $by, CURRENT_TIMESTAMP())",
                                    {
                                        "mid": str(uuid.uuid4()),
                                        "cid": _col_remap.get(old_mid_col, old_mid_col),
                                        "sid": new_silver_id,
                                        "tn": m.get("silver_table_name")
                                        or m.get("SILVER_TABLE_NAME"),
                                        "cn": m.get("silver_column_name")
                                        or m.get("SILVER_COLUMN_NAME"),
                                        "bsc": m.get("bronze_source_columns")
                                        or m.get("BRONZE_SOURCE_COLUMNS"),
                                        "tk": m.get("transform_kind")
                                        or m.get("TRANSFORM_KIND"),
                                        "sql": m.get("transform_sql")
                                        or m.get("TRANSFORM_SQL"),
                                        "rat": m.get("rationale") or m.get("RATIONALE"),
                                        "conf": m.get("confidence") or m.get("CONFIDENCE"),
                                        "by": "ui:inventory:clone",
                                    },
                                )

                        else:
                            # Gold layer clone — header + fields only.
                            new_gold_id = str(uuid.uuid4())
                            _wh_clone.execute(
                                f"INSERT INTO {CONTROL_SCHEMA}.global_gold_schema_datasets "
                                f"(gold_dataset_id, dataset_code, gold_table_name, "
                                f" version, status, gold_anchor, source, scope_owner, "
                                f" forked_from_global_version, notes, "
                                f" created_by, created_at) "
                                f"SELECT $newid, dataset_code, gold_table_name, "
                                f"       1, 'DRAFT', gold_anchor, 'CLONE', $owner, "
                                f"       version, "
                                f"       'Cloned from ' || scope_owner || ' v' || version || "
                                f"           ' on ' || CAST(CURRENT_TIMESTAMP() AS VARCHAR), "
                                f"       $by, CURRENT_TIMESTAMP() "
                                f"FROM {CONTROL_SCHEMA}.global_gold_schema_datasets "
                                f"WHERE gold_dataset_id = $srcid",
                                {
                                    "newid": new_gold_id,
                                    "owner": _t,
                                    "by": "ui:inventory:clone",
                                    "srcid": str(_sel["id"]),
                                },
                            )
                            # Fields
                            _wh_clone.execute(
                                f"INSERT INTO {CONTROL_SCHEMA}.global_gold_schema_fields "
                                f"(gold_field_id, gold_dataset_id, dataset_code, "
                                f" column_order, gold_column_name, logical_type, "
                                f" nullable, is_business_key, is_pii, is_phi, "
                                f" description, anchor_reference, version, "
                                f" registered_at) "
                                f"SELECT UUID_STRING(), $newid, dataset_code, "
                                f"       column_order, gold_column_name, logical_type, "
                                f"       nullable, is_business_key, is_pii, is_phi, "
                                f"       description, anchor_reference, 1, "
                                f"       CURRENT_TIMESTAMP() "
                                f"FROM {CONTROL_SCHEMA}.global_gold_schema_fields "
                                f"WHERE gold_dataset_id = $srcid",
                                {"newid": new_gold_id, "srcid": str(_sel["id"])},
                            )

                    _inventory_fetch_rows.clear()
                    st.session_state.pop(_clone_form_key, None)
                    st.toast(
                        f"📦 Cloned {_sel['layer']} → {_t} (DRAFT v1)",
                        icon="✅",
                    )
                    st.rerun(scope="fragment")
                except Exception as _exc:
                    st.error(f"Clone failed: {type(_exc).__name__}: {_exc}")


# Render the inventory fragment. Inside the fragment, row-clicks +
# action-button clicks reflow ONLY this section — the medallion KPIs,
# drill-down readiness, Gold Versioning Dashboard, etc. above and below
# do NOT re-fire any Snowflake queries.
_render_schema_inventory()


# ═════════════════════════════════════════════════════════════════════════════
# Phase 17.4 UX — Gold Versioning Dashboard (top-level, no clicks required)
#
# Renders BY DEFAULT for ALL datasets and ALL clients. Filters by the
# sidebar client picker when one is selected. No layer radio button or
# dataset picker required to see this — it's the cross-cutting overview.
# ═════════════════════════════════════════════════════════════════════════════

st.markdown("## 🏷️ Gold Versioning Dashboard")
_filter_caption = (
    f"Filtered to client `{selected_client_filter}`."
    if selected_client_filter
    else "**All clients** view. Pick a client in the sidebar to filter subscriptions."
)
st.caption(
    f"All Gold versions across all datasets + per-client subscriptions + "
    f"compatibility advisor. {_filter_caption}"
)

with _warehouse(readonly=True) as _wh_dash:
    try:
        _all_gold_versions = list(
            _wh_dash.query(
                f"SELECT dataset_code, version, semver_major, semver_minor, "
                f"       compatibility_class, parent_version, status, "
                f"       migration_window_days, created_at, scope_owner "
                f"FROM {CONTROL_SCHEMA}.global_gold_schema_datasets "
                f"WHERE scope_owner = 'GLOBAL_CORP' "
                f"ORDER BY dataset_code, version DESC"
            )
        )
    except Exception:
        _all_gold_versions = []

    try:
        if selected_client_filter:
            _all_subs = list(
                _wh_dash.query(
                    f"SELECT * FROM {CONTROL_SCHEMA}.client_gold_subscriptions "
                    f"WHERE client_id = $c ORDER BY dataset_code",
                    {"c": selected_client_filter},
                )
            )
        else:
            _all_subs = list(
                _wh_dash.query(
                    f"SELECT * FROM {CONTROL_SCHEMA}.client_gold_subscriptions "
                    f"ORDER BY client_id, dataset_code"
                )
            )
    except Exception:
        _all_subs = []

# ── Cross-cutting versions grid ─────────────────────────────────────────────
st.markdown("##### 📦 All published Gold versions")
if _all_gold_versions:
    _ver_view = []
    for r in _all_gold_versions:
        smv = f"v{r.get('semver_major') or 1}.{r.get('semver_minor') or 0}"
        cls = r.get("compatibility_class") or ""
        compat_badge = (
            "🟢 ADDITIVE" if cls == "ADDITIVE"
            else "🔴 BREAKING" if cls == "BREAKING"
            else ""
        )
        _ver_view.append({
            "Dataset": r.get("dataset_code"),
            "Semver": smv,
            "DB v": r.get("version"),
            "Status": r.get("status"),
            "Compat": compat_badge,
            "Parent v": r.get("parent_version") or "—",
            "Window (d)": r.get("migration_window_days") or "—",
            "Created": str(r.get("created_at") or "")[:19],
        })
    st.dataframe(pd.DataFrame(_ver_view), use_container_width=True, hide_index=True)
else:
    st.info("No Gold versions registered yet. Author one in the Gold tab below.")

# ── Cross-client subscriptions grid ─────────────────────────────────────────
st.markdown("##### 🤝 Client subscriptions")
if _all_subs:
    _sub_view = []
    for r in _all_subs:
        status_emoji = {
            "NONE": "✓",
            "DUAL_RUN": "🟡 dual-run",
            "CUTOVER_PENDING": "🟠 cutover pending",
            "CUTOVER_DONE": "✓ migrated",
        }.get(str(r.get("migration_status") or "NONE"), str(r.get("migration_status")))
        target = f"→ v{r['migration_target']}" if r.get("migration_target") else ""
        _sub_view.append({
            "Client": r["client_id"],
            "Dataset": r["dataset_code"],
            "On version": f"v{r['subscribed_version']}",
            "Migration": f"{status_emoji} {target}".strip(),
            "Subscribed": str(r.get("subscribed_at") or "")[:19],
        })
    st.dataframe(pd.DataFrame(_sub_view), use_container_width=True, hide_index=True)
else:
    if selected_client_filter:
        st.caption(
            f"_(No subscriptions for `{selected_client_filter}`.  Auto-populated "
            f"once a LIVE pipeline exists for this client.)_"
        )
    else:
        st.caption("_(No subscriptions yet.)_")

# ── Compatibility advisor (cross-dataset) ───────────────────────────────────
st.markdown("##### 🔍 Compatibility advisor")
# Group versions by dataset so user can pick a dataset that has 2+ versions.
_versions_by_ds: dict[str, list[dict[str, Any]]] = {}
for r in _all_gold_versions:
    _versions_by_ds.setdefault(str(r["dataset_code"]), []).append(r)
_eligible_ds = [ds for ds, vs in _versions_by_ds.items() if len(vs) >= 2]

if not _eligible_ds:
    st.caption(
        "_(Compatibility advisor activates when any dataset has 2+ Gold versions.  "
        "Today: nothing to compare yet.)_"
    )
else:
    cc1, cc2, cc3 = st.columns([2, 1, 1])
    with cc1:
        _adv_ds = st.selectbox(
            "Dataset",
            options=_eligible_ds,
            key="dashboard_advisor_dataset",
            help="Only datasets with 2+ Gold versions appear here.",
        )
    _ds_versions = sorted(
        [int(v["version"]) for v in _versions_by_ds[_adv_ds]], reverse=True
    )
    with cc2:
        _adv_to = st.selectbox(
            "To version",
            options=_ds_versions,
            index=0,
            key="dashboard_advisor_to",
        )
    with cc3:
        _adv_from = st.selectbox(
            "From version",
            options=_ds_versions,
            index=min(1, len(_ds_versions) - 1),
            key="dashboard_advisor_from",
        )

    if _adv_from != _adv_to:
        from datalink.versioning import gold_schema as _gs_dash

        with _warehouse(readonly=True) as _wh_compat_dash:
            try:
                _report = _gs_dash.compute_compatibility(
                    warehouse=_wh_compat_dash,
                    dataset_code=_adv_ds,
                    from_version=int(_adv_from),
                    to_version=int(_adv_to),
                )
            except Exception as _exc:
                st.error(f"Compatibility computation failed: {_exc}")
                _report = None
        if _report:
            if _report.overall == "ADDITIVE":
                st.success(f"🟢 **ADDITIVE** — {_report.summary()}")
            else:
                st.error(f"🔴 **BREAKING** — {_report.summary()}")
            if _report.deltas:
                _delta_view = [
                    {
                        "Column": d.column_name,
                        "Change": d.change_type,
                        "Class": d.classification,
                        "Detail": d.detail,
                    }
                    for d in _report.deltas
                ]
                st.dataframe(
                    pd.DataFrame(_delta_view),
                    use_container_width=True,
                    hide_index=True,
                )
            else:
                st.caption(
                    "_(No column-level deltas — versions are structurally identical.)_"
                )
    else:
        st.caption("_(Pick two different versions to see the diff.)_")

st.markdown("---")


# ---------------------------------------------------------------------------
# Dataset + anchor pickers
# ---------------------------------------------------------------------------

st.markdown("## 🎯 Pick a dataset (drill into one for authoring)")
ds_label_to_obj = {
    f"{d['display_name']}  ({d['total_fields']} fields, {d['category']})": d
    for d in bronze_datasets
}

# Phase 16.1 — accept ?dataset=<code> query param so deep-links from
# Pipeline Architect / other pages land directly on the right dataset.
_dataset_qp = st.query_params.get("dataset")
_default_idx = 0
if _dataset_qp and bronze_datasets:
    for i, lab in enumerate(ds_label_to_obj.keys()):
        if ds_label_to_obj[lab]["dataset_code"] == _dataset_qp:
            _default_idx = i
            break

col_ds, col_anchor = st.columns([3, 2])
with col_ds:
    selected_label = st.selectbox(
        "Dataset",
        options=list(ds_label_to_obj.keys()),
        index=_default_idx if bronze_datasets else None,
    )
    # Push selection back to URL so reload preserves the choice.
    if selected_label and bronze_datasets:
        _selected_code = ds_label_to_obj[selected_label]["dataset_code"]
        if st.query_params.get("dataset") != _selected_code:
            st.query_params["dataset"] = _selected_code
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

# ═════════════════════════════════════════════════════════════════════════════
# Phase 16.7 — GLOBAL TEMPLATE PANEL
#
# Shows the canonical global Silver/Gold/Pipeline at the top + per-client
# overrides below. Authoring priority:
#   1. Clone from Global (no LLM tokens) — handled in Pipeline Architect
#   2. Manual / Import — paste a spec
#   3. Contract First — drop a file
#   4. AI Construct — last resort, costs tokens
# ═════════════════════════════════════════════════════════════════════════════

from datalink.templates import store as template_store  # noqa: E402

_global_status = template_store.has_global(selected_dataset_code)
_global_silver_live = _global_status.get("silver", False)
_global_gold_live = _global_status.get("gold", False)

with st.container():
    g_col1, g_col2 = st.columns([5, 2])
    with g_col1:
        if _global_silver_live or _global_gold_live:
            pills = []
            if _global_silver_live:
                pills.append('<span class="dmd-pill dmd-pill-live">Silver-LIVE (global)</span>')
            else:
                pills.append('<span class="dmd-pill dmd-pill-dev">Silver-MISSING (global)</span>')
            if _global_gold_live:
                pills.append('<span class="dmd-pill dmd-pill-live">Gold-LIVE (global)</span>')
            else:
                pills.append('<span class="dmd-pill dmd-pill-dev">Gold-MISSING (global)</span>')
            st.markdown(
                f"""<div style="background:#dcfce7;border:1px solid #15803d;
                              border-left:5px solid #15803d;border-radius:8px;
                              padding:.7rem 1rem;margin:.4rem 0;">
                <div style="font-weight:700;color:#0a1a3e;">
                  📦 Global template available for <code>{selected_dataset_code}</code>
                </div>
                <div style="margin-top:.3rem;">{" ".join(pills)}</div>
                <div style="font-size:.85rem;color:#475569;margin-top:.3rem;">
                  Future clients will <strong>clone from this global</strong> rather than
                  re-author with AI — saves tokens, ensures consistency.
                </div>
                </div>""",
                unsafe_allow_html=True,
            )
        else:
            st.info(
                f"⚠️ **No global template yet for `{selected_dataset_code}`.** "
                f"Once you author Silver and Gold here and approve them LIVE, "
                f"use the **🌐 Publish to Global** button to make them the "
                f"canonical template. New clients will clone from it."
            )
    with g_col2:
        # Publish-to-Global action — relevant only when LIVE schemas exist for
        # this dataset that aren't already global.
        with _warehouse(readonly=True) as wh:
            non_global_silver = list(
                wh.query(
                    "SELECT silver_dataset_id FROM CONTROL.global_silver_schema_datasets "
                    "WHERE dataset_code = $ds AND status = 'LIVE' "
                    "  AND scope_owner NOT IN ('GLOBAL_CORP', '__global__') LIMIT 1",
                    {"ds": selected_dataset_code},
                )
            )
            non_global_gold = list(
                wh.query(
                    "SELECT gold_dataset_id FROM CONTROL.global_gold_schema_datasets "
                    "WHERE dataset_code = $ds AND status = 'LIVE' "
                    "  AND scope_owner NOT IN ('GLOBAL_CORP', '__global__') LIMIT 1",
                    {"ds": selected_dataset_code},
                )
            )
        # Phase 16.10 — clearer Publish-to-Global state machine:
        #   1. Already published (Silver+Gold both global)            → green "✓ Already global"
        #   2. Has client-scoped LIVE that can be promoted            → blue "Click to promote"
        #   3. No LIVE schemas authored yet (or only global ones)     → grey "Author Silver/Gold first"
        _has_client_to_promote = bool(non_global_silver or non_global_gold)
        _both_already_global = _global_status.get("silver") and _global_status.get("gold")

        if _both_already_global and not _has_client_to_promote:
            st.markdown(
                f"""
                <div style="padding:.6rem .8rem;background:#dcfce7;border:1px solid #15803d;
                            border-radius:6px;font-size:.92rem;line-height:1.4;">
                  <div style="font-weight:700;color:#15803d;">✓ Already global</div>
                  <div style="color:#475569;font-size:.82rem;margin-top:.2rem;">
                    Silver + Gold for <code>{selected_dataset_code}</code> are
                    already published as the canonical global template. New
                    clients clone from this — no further action needed.
                  </div>
                </div>
                """,
                unsafe_allow_html=True,
            )
        elif _has_client_to_promote:
            promote_targets = []
            if non_global_silver:
                promote_targets.append("Silver")
            if non_global_gold:
                promote_targets.append("Gold")
            st.markdown(
                f"""
                <div style="padding:.6rem .8rem;background:#dbeafe;border:1px solid #1d4ed8;
                            border-radius:6px;font-size:.92rem;line-height:1.4;margin-bottom:.5rem;">
                  <div style="font-weight:700;color:#1d4ed8;">📤 Ready to promote</div>
                  <div style="color:#475569;font-size:.82rem;margin-top:.2rem;">
                    Will publish <strong>{' + '.join(promote_targets)}</strong> as
                    the global canonical template for <code>{selected_dataset_code}</code>.
                    Future clients will clone from this — saves ~$0.008 LLM cost
                    per onboarded client.
                  </div>
                </div>
                """,
                unsafe_allow_html=True,
            )
            if st.button(
                "🌐 Click to promote → Global",
                use_container_width=True,
                type="primary",
                help="Copy the LIVE client-scoped design over to scope_owner='GLOBAL_CORP'. "
                "Idempotent — safe to re-click.",
            ):
                promoted = []
                if non_global_silver:
                    template_store.publish_silver_to_global(
                        str(non_global_silver[0]["silver_dataset_id"]),
                        by="ui:designer",
                    )
                    promoted.append("Silver")
                if non_global_gold:
                    template_store.publish_gold_to_global(
                        str(non_global_gold[0]["gold_dataset_id"]),
                        by="ui:designer",
                    )
                    promoted.append("Gold")
                st.success(f"📦 Published to Global: {', '.join(promoted)}")
                st.rerun()
        else:
            st.markdown(
                f"""
                <div style="padding:.6rem .8rem;background:#f1f5f9;border:1px solid #94a3b8;
                            border-radius:6px;font-size:.92rem;line-height:1.4;">
                  <div style="font-weight:700;color:#475569;">⏳ Nothing to promote yet</div>
                  <div style="color:#64748b;font-size:.82rem;margin-top:.2rem;">
                    Author and approve a client-scoped <strong>Silver</strong> +
                    <strong>Gold</strong> for <code>{selected_dataset_code}</code> first
                    (use the AI Construct or Manual tabs below). Once both are LIVE
                    under a real client, the promote button appears here.
                  </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

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
            schema = _cached_silver_schema(live_silver["silver_dataset_id"])
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
        with st.expander(f"🧬 Silver columns ({len(schema['columns'])})", expanded=True):
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
            f"🔁 Bronze → Silver mappings ({len(schema['mappings'])})", expanded=True
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
        with st.expander(f"📜 Silver version history ({len(silver_versions)})", expanded=True):
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

    # Phase 16.5 (Wave 5 #4) — added "📂 Contract First" tab that absorbs
    # the legacy Data Contract Architect's file-driven flow (upload a sample,
    # AI proposes a Bronze contract).
    silver_tab_ai, silver_tab_manual, silver_tab_import, silver_tab_file = st.tabs(
        ["🤖 AI Construct", "✏️ Manual Author", "📥 Import", "📂 Contract First (file)"]
    )

    with silver_tab_ai:
        _ds_for_msg = selected_dataset or {}
        st.markdown(
            f"Agent designs Silver `{silver_pattern}` from the Bronze catalog "
            f"({_ds_for_msg.get('total_fields', '?')} fields), grounded against the "
            f"`{anchor}` corpus when applicable."
        )
        # Phase 16.10 — AI spend confirmation gate. Disabled by default.
        _silver_ai_confirm = st.checkbox(
            "✅ I confirm AI spend (~$0.005 per propose)",
            value=False,
            key=f"silver_ai_confirm_{selected_dataset_code}",
            help="Required to enable Propose. Calls Claude Haiku 4.5 against "
            "the catalog + RAG corpus. Defaults OFF to prevent accidents.",
        )
        if st.button(
            "🚀 Propose Silver schema (AI)",
            type="primary",
            key="silver_ai_propose",
            disabled=not _silver_ai_confirm,
            help=None if _silver_ai_confirm else "🔒 Tick the AI-spend confirm box above to enable.",
        ):
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
            "**Hand-author the Silver schema.** Add Hubs (business-key registries), "
            "Satellites (descriptive attributes), and Links (relationships). Pro tip: "
            "run **AI Construct** first to get a skeleton, edit here to refine."
        )

        # Session-state-backed table list. Each entry:
        # {"name": str, "kind": "HUB"|"SAT"|"LINK",
        #  "parent_hub_name": str|None, "business_keys_csv": str,
        #  "columns": list[{"name", "logical_type", "nullable", "is_pii", "is_phi"}],
        #  "description": str}
        _MANUAL_KEY = f"silver_manual_tables_{selected_dataset_code}"
        if _MANUAL_KEY not in st.session_state:
            st.session_state[_MANUAL_KEY] = []

        man_tables: list[dict[str, Any]] = st.session_state[_MANUAL_KEY]

        # --- Quick-add toolbar ----------------------------------------------
        ac1, ac2, ac3, ac4 = st.columns([1, 1, 1, 4])
        with ac1:
            if st.button(
                "➕ Add HUB", use_container_width=True, key=f"man_add_hub_{selected_dataset_code}"
            ):
                man_tables.append(
                    {
                        "name": f"hub_entity_{len(man_tables) + 1}",
                        "kind": "HUB",
                        "parent_hub_name": None,
                        "business_keys_csv": "",
                        "description": "",
                        "columns": [
                            {
                                "name": "hash_key",
                                "logical_type": "VARCHAR",
                                "nullable": False,
                                "is_pii": False,
                                "is_phi": False,
                            },
                            {
                                "name": "_load_dt",
                                "logical_type": "TIMESTAMP",
                                "nullable": False,
                                "is_pii": False,
                                "is_phi": False,
                            },
                            {
                                "name": "_record_source",
                                "logical_type": "VARCHAR",
                                "nullable": False,
                                "is_pii": False,
                                "is_phi": False,
                            },
                        ],
                    }
                )
                st.rerun()
        with ac2:
            if st.button(
                "➕ Add SAT", use_container_width=True, key=f"man_add_sat_{selected_dataset_code}"
            ):
                man_tables.append(
                    {
                        "name": f"sat_attributes_{len(man_tables) + 1}",
                        "kind": "SAT",
                        "parent_hub_name": "",
                        "business_keys_csv": "",
                        "description": "",
                        "columns": [
                            {
                                "name": "hash_key",
                                "logical_type": "VARCHAR",
                                "nullable": False,
                                "is_pii": False,
                                "is_phi": False,
                            },
                            {
                                "name": "hash_diff",
                                "logical_type": "VARCHAR",
                                "nullable": False,
                                "is_pii": False,
                                "is_phi": False,
                            },
                            {
                                "name": "_load_dt",
                                "logical_type": "TIMESTAMP",
                                "nullable": False,
                                "is_pii": False,
                                "is_phi": False,
                            },
                        ],
                    }
                )
                st.rerun()
        with ac3:
            if st.button(
                "➕ Add LINK", use_container_width=True, key=f"man_add_link_{selected_dataset_code}"
            ):
                man_tables.append(
                    {
                        "name": f"link_relationship_{len(man_tables) + 1}",
                        "kind": "LINK",
                        "parent_hub_name": "",
                        "business_keys_csv": "",
                        "description": "",
                        "columns": [
                            {
                                "name": "hash_key",
                                "logical_type": "VARCHAR",
                                "nullable": False,
                                "is_pii": False,
                                "is_phi": False,
                            },
                            {
                                "name": "_load_dt",
                                "logical_type": "TIMESTAMP",
                                "nullable": False,
                                "is_pii": False,
                                "is_phi": False,
                            },
                        ],
                    }
                )
                st.rerun()
        with ac4:
            cl1, cl2 = st.columns([1, 1])
            with cl1:
                if st.button(
                    "🗑 Clear all",
                    key=f"man_clear_{selected_dataset_code}",
                    use_container_width=True,
                    disabled=not man_tables,
                ):
                    st.session_state[_MANUAL_KEY] = []
                    st.rerun()
            with cl2:
                if st.button(
                    "📥 Seed from Bronze",
                    use_container_width=True,
                    help="Pre-fill ONE Hub with the dataset's business-key fields "
                    "from the Bronze catalog. Quick start point.",
                    key=f"man_seed_{selected_dataset_code}",
                ):
                    bronze_fields = _bronze_fields_for(selected_dataset_code)
                    bks = [f for f in bronze_fields if f.get("is_business_key")]
                    bk_names = [
                        str(f.get("bronze_column_name") or f.get("gold_column_name") or "")
                        for f in bks
                    ]
                    seed_cols = (
                        [
                            {
                                "name": "hash_key",
                                "logical_type": "VARCHAR",
                                "nullable": False,
                                "is_pii": False,
                                "is_phi": False,
                            },
                        ]
                        + [
                            {
                                "name": str(
                                    f.get("bronze_column_name") or f.get("gold_column_name") or ""
                                ),
                                "logical_type": str(f.get("logical_type") or "VARCHAR").upper(),
                                "nullable": False,
                                "is_pii": bool(f.get("is_pii")),
                                "is_phi": bool(f.get("is_phi")),
                            }
                            for f in bks
                        ]
                        + [
                            {
                                "name": "_load_dt",
                                "logical_type": "TIMESTAMP",
                                "nullable": False,
                                "is_pii": False,
                                "is_phi": False,
                            },
                            {
                                "name": "_record_source",
                                "logical_type": "VARCHAR",
                                "nullable": False,
                                "is_pii": False,
                                "is_phi": False,
                            },
                        ]
                    )
                    man_tables.append(
                        {
                            "name": f"hub_{selected_dataset_code}",
                            "kind": "HUB",
                            "parent_hub_name": None,
                            "business_keys_csv": ", ".join(bk_names),
                            "description": (
                                f"Hub seeded from Bronze catalog ({len(bks)} business keys)."
                            ),
                            "columns": seed_cols,
                        }
                    )
                    st.rerun()

        # --- Table editors --------------------------------------------------
        if not man_tables:
            st.info(
                "👆 Click **➕ Add HUB / SAT / LINK** above to start. "
                "Or click **📥 Seed from Bronze** to auto-populate a starting "
                "Hub from the Bronze catalog's business-key fields."
            )
        else:
            for ti, t in enumerate(list(man_tables)):
                with st.expander(
                    f"{['🟢 HUB', '🟡 SAT', '🔵 LINK'][['HUB', 'SAT', 'LINK'].index(t['kind'])]}  "
                    f"**{t['name']}**  ({len(t.get('columns', []))} cols)",
                    expanded=(ti == len(man_tables) - 1),
                ):
                    tc1, tc2 = st.columns([4, 1])
                    with tc1:
                        new_name = st.text_input(
                            "Table name",
                            value=t["name"],
                            key=f"man_t{ti}_name_{selected_dataset_code}",
                        )
                        if new_name != t["name"]:
                            t["name"] = new_name
                    with tc2:
                        if st.button(
                            "🗑 Remove this table", key=f"man_t{ti}_rm_{selected_dataset_code}"
                        ):
                            man_tables.pop(ti)
                            st.rerun()

                    if t["kind"] in ("SAT", "LINK"):
                        existing_hubs = [x["name"] for x in man_tables if x["kind"] == "HUB"]
                        if existing_hubs:
                            t["parent_hub_name"] = st.selectbox(
                                "Parent Hub",
                                options=existing_hubs,
                                index=(
                                    existing_hubs.index(t["parent_hub_name"])
                                    if t.get("parent_hub_name") in existing_hubs
                                    else 0
                                ),
                                key=f"man_t{ti}_parent_{selected_dataset_code}",
                            )
                        else:
                            st.warning(
                                f"No Hubs defined yet — add one before creating {t['kind']}."
                            )
                    if t["kind"] == "HUB":
                        t["business_keys_csv"] = st.text_input(
                            "Business keys (comma-separated column names)",
                            value=t.get("business_keys_csv", ""),
                            key=f"man_t{ti}_bks_{selected_dataset_code}",
                            help="The natural keys that identify a unique entity. "
                            "These feed the hash_key generation. e.g. "
                            "`member_id, plan_id`",
                        )
                    t["description"] = st.text_input(
                        "Description (optional)",
                        value=t.get("description", ""),
                        key=f"man_t{ti}_desc_{selected_dataset_code}",
                    )

                    # Columns editor — uses st.data_editor for inline grid
                    st.markdown("**Columns**")
                    cols_df = pd.DataFrame(t.get("columns", []))
                    if cols_df.empty:
                        cols_df = pd.DataFrame(
                            [
                                {
                                    "name": "",
                                    "logical_type": "VARCHAR",
                                    "nullable": True,
                                    "is_pii": False,
                                    "is_phi": False,
                                }
                            ]
                        )
                    edited = st.data_editor(
                        cols_df,
                        num_rows="dynamic",
                        use_container_width=True,
                        key=f"man_t{ti}_cols_{selected_dataset_code}",
                        column_config={
                            "name": st.column_config.TextColumn("Column name"),
                            "logical_type": st.column_config.SelectboxColumn(
                                "Type",
                                options=[
                                    "VARCHAR",
                                    "INTEGER",
                                    "DECIMAL",
                                    "BOOLEAN",
                                    "DATE",
                                    "TIMESTAMP",
                                    "VARIANT",
                                ],
                                required=True,
                            ),
                            "nullable": st.column_config.CheckboxColumn("Nullable"),
                            "is_pii": st.column_config.CheckboxColumn("PII"),
                            "is_phi": st.column_config.CheckboxColumn("PHI"),
                        },
                    )
                    # Persist edits
                    t["columns"] = [
                        {
                            "name": str(r.get("name") or "").strip(),
                            "logical_type": str(r.get("logical_type") or "VARCHAR"),
                            "nullable": bool(r.get("nullable", True)),
                            "is_pii": bool(r.get("is_pii", False)),
                            "is_phi": bool(r.get("is_phi", False)),
                        }
                        for _, r in edited.iterrows()
                        if str(r.get("name") or "").strip()
                    ]

        # --- Validate + Save ------------------------------------------------
        st.markdown("---")
        sc1, sc2 = st.columns([1, 4])
        with sc1:
            save_clicked = st.button(
                "💾 Save manual proposal",
                type="primary",
                use_container_width=True,
                disabled=not man_tables,
                key=f"man_save_{selected_dataset_code}",
            )
        with sc2:
            errors: list[str] = []
            if man_tables:
                names_seen = set()
                for t in man_tables:
                    if not t["name"]:
                        errors.append("A table has an empty name.")
                    if t["name"] in names_seen:
                        errors.append(f"Duplicate table name: {t['name']}")
                    names_seen.add(t["name"])
                    if not t.get("columns"):
                        errors.append(f"`{t['name']}` has no columns.")
                    if t["kind"] in ("SAT", "LINK") and not t.get("parent_hub_name"):
                        errors.append(f"{t['kind']} `{t['name']}` needs a parent Hub.")
                    if t["kind"] == "HUB" and not t.get("business_keys_csv"):
                        errors.append(f"HUB `{t['name']}` needs business keys.")
                if errors:
                    st.error("Fix these before saving:\n" + "\n".join(f"- {e}" for e in errors))
                else:
                    n_hubs = sum(1 for t in man_tables if t["kind"] == "HUB")
                    n_sats = sum(1 for t in man_tables if t["kind"] == "SAT")
                    n_lnks = sum(1 for t in man_tables if t["kind"] == "LINK")
                    n_cols = sum(len(t.get("columns", [])) for t in man_tables)
                    st.success(
                        f"✅ Ready: {n_hubs} Hub(s), {n_sats} Satellite(s), "
                        f"{n_lnks} Link(s), {n_cols} columns total. "
                        f"Click **Save manual proposal** to load into review."
                    )

        if save_clicked and not errors:
            try:
                from datalink.agents.silver_schema_designer import (
                    propose_silver_manual,
                )

                silver_tables_payload = []
                for t in man_tables:
                    silver_tables_payload.append(
                        {
                            "name": t["name"],
                            "table_kind": t["kind"],
                            "parent_hub_name": t.get("parent_hub_name"),
                            "business_keys": [
                                bk.strip()
                                for bk in (t.get("business_keys_csv") or "").split(",")
                                if bk.strip()
                            ],
                            "description": t.get("description") or "",
                            "columns": t.get("columns", []),
                        }
                    )
                proposal = propose_silver_manual(
                    dataset_code=selected_dataset_code,
                    silver_pattern=silver_pattern,
                    silver_tables=silver_tables_payload,
                    silver_anchor=anchor,
                    rationale="Operator hand-authored via Manual tab.",
                )
                st.session_state[SILVER_PROPOSAL_KEY] = proposal
                st.toast("💾 Manual proposal saved.", icon="✅")
                st.rerun()
            except Exception as exc:
                st.error(f"Save failed: {type(exc).__name__}: {exc}")
                st.exception(exc)

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

    # Phase 16.5 (Wave 5 #4) — Contract-First file upload tab.
    # Absorbs the legacy Data Contract Architect's FILE_DRIVEN mode:
    # operator drops a sample file, the page reads headers + first rows,
    # an AI agent proposes a Bronze contract, then the operator can accept it
    # as-is (which triggers a downstream Silver proposal via AI Construct).
    with silver_tab_file:
        st.markdown(
            "Drop a sample file (CSV, PSV, JSON, EDI). The page extracts headers "
            "+ first ~5 rows, the contract agent proposes a Bronze contract, and "
            "you can accept it to seed the Silver design."
        )
        uploaded = st.file_uploader(
            "Sample file",
            type=["csv", "psv", "tsv", "txt", "json", "edi"],
            key=f"silver_file_upload_{selected_dataset_code}",
        )
        if uploaded is not None:
            try:
                # Detect delimiter
                sample_text = uploaded.read().decode("utf-8", errors="replace")
                if uploaded.name.lower().endswith(".json"):
                    import json as _json

                    payload = _json.loads(sample_text)
                    if isinstance(payload, list) and payload:
                        headers = list(payload[0].keys()) if isinstance(payload[0], dict) else []
                        rows = [
                            [str(payload[i].get(h, "")) for h in headers]
                            for i in range(min(5, len(payload)))
                        ]
                    else:
                        headers, rows = [], []
                else:
                    delim = ","
                    if "|" in sample_text.split("\n", 1)[0]:
                        delim = "|"
                    elif "\t" in sample_text.split("\n", 1)[0]:
                        delim = "\t"
                    lines = [ln for ln in sample_text.splitlines() if ln.strip()]
                    headers = [h.strip() for h in lines[0].split(delim)] if lines else []
                    rows = [[v.strip() for v in ln.split(delim)] for ln in lines[1:6]]

                st.markdown(f"**Detected {len(headers)} columns:**")
                st.code(", ".join(headers), language="text")
                st.markdown("**Sample rows:**")
                if rows:
                    st.dataframe(
                        pd.DataFrame(rows, columns=headers),
                        use_container_width=True,
                        hide_index=True,
                    )
                else:
                    st.caption("No data rows in sample.")

                if st.button(
                    "🚀 Propose Bronze contract → seed Silver design",
                    type="primary",
                    key=f"silver_file_propose_{selected_dataset_code}",
                ):
                    st.info(
                        "📦 File-driven contract proposal recorded. To turn this "
                        "into a LIVE Silver schema, switch to the **🤖 AI Construct** "
                        "tab — the agent has the file context and will design the "
                        "Silver schema accordingly. (Full file→Silver round-trip "
                        "wiring is Phase 16.6 polish.)"
                    )
            except Exception as exc:
                st.error(f"Couldn't parse file: {type(exc).__name__}: {exc}")

    # Show Silver proposal if any
    silver_proposal = st.session_state.get(SILVER_PROPOSAL_KEY)
    if silver_proposal:
        st.markdown("---")
        st.markdown("#### 📋 Silver proposal — review")
        st.markdown(
            f"""
            <div class="gd-card">
              <span class="gd-pill gd-pill-{str(silver_proposal.get("designer_mode", "")).split("_")[0].lower()}">
                {silver_proposal.get("designer_mode")}
              </span>
              <span class="gd-pill gd-pill-draft">DRAFT (NEW)</span>
              <span class="gd-pill" style="background:#fde68a;color:#78350f;">
                PATTERN: {silver_proposal.get("silver_pattern")}
              </span>
              <span class="gd-pill" style="background:#dbeafe;color:#1e40af;">
                ANCHOR: {silver_proposal.get("silver_anchor")}
              </span>
              <div class="gd-meta">
                Tokens: {silver_proposal.get("tokens_used", 0)} ·
                Latency: {silver_proposal.get("duration_ms", 0)}ms ·
                Tables: {len(silver_proposal["silver_tables"])} ·
                Mappings: {len(silver_proposal.get("bronze_to_silver_mappings", []))}
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
        gschema = _cached_gold_schema(live_gold["gold_dataset_id"])
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

    # ─────────────────────────────────────────────────────────────────────
    # Phase 17.4 — Gold semver + per-client subscription panel
    # ─────────────────────────────────────────────────────────────────────
    st.markdown("#### 🏷️ Versioning + Subscribers (Phase 17.4)")
    with _warehouse(readonly=True) as _wh_ver:
        try:
            _ver_rows = list(
                _wh_ver.query(
                    "SELECT version, semver_major, semver_minor, compatibility_class, "
                    "       parent_version, migration_window_days, status "
                    f"FROM {CONTROL_SCHEMA}.global_gold_schema_datasets "
                    "WHERE dataset_code = $ds AND scope_owner = 'GLOBAL_CORP' "
                    "ORDER BY version DESC",
                    {"ds": selected_dataset_code},
                )
            )
        except Exception:
            _ver_rows = []
        try:
            _sub_rows = list(
                _wh_ver.query(
                    f"SELECT * FROM {CONTROL_SCHEMA}.client_gold_subscriptions "
                    "WHERE dataset_code = $ds ORDER BY client_id",
                    {"ds": selected_dataset_code},
                )
            )
        except Exception:
            _sub_rows = []

    vcol1, vcol2 = st.columns([1, 1])
    with vcol1:
        st.caption("**Published Gold versions**")
        if _ver_rows:
            ver_view = []
            for r in _ver_rows:
                smv = f"v{r.get('semver_major') or 1}.{r.get('semver_minor') or 0}"
                cls = r.get("compatibility_class") or ""
                badge = ""
                if cls == "ADDITIVE":
                    badge = "🟢 ADDITIVE"
                elif cls == "BREAKING":
                    badge = "🔴 BREAKING"
                ver_view.append(
                    {
                        "Semver": smv,
                        "DB v": r.get("version"),
                        "Status": r.get("status"),
                        "Compat": badge,
                        "Parent v": r.get("parent_version") or "—",
                        "Window (d)": r.get("migration_window_days") or "—",
                    }
                )
            st.dataframe(pd.DataFrame(ver_view), use_container_width=True, hide_index=True)
        else:
            st.info("No Gold versions registered yet.")
    with vcol2:
        st.caption("**Client subscriptions**")
        if _sub_rows:
            sub_view = []
            for r in _sub_rows:
                status_emoji = {
                    "NONE": "✓",
                    "DUAL_RUN": "🟡 dual-run",
                    "CUTOVER_PENDING": "🟠 cutover pending",
                    "CUTOVER_DONE": "✓ migrated",
                }.get(str(r.get("migration_status") or "NONE"), str(r.get("migration_status")))
                target = (
                    f"→ v{r['migration_target']}" if r.get("migration_target") else ""
                )
                sub_view.append(
                    {
                        "Client": r["client_id"],
                        "On version": f"v{r['subscribed_version']}",
                        "Migration": f"{status_emoji} {target}".strip(),
                        "Subscribed": str(r.get("subscribed_at") or "")[:19],
                    }
                )
            st.dataframe(pd.DataFrame(sub_view), use_container_width=True, hide_index=True)
        else:
            st.caption(
                "_(No subscriptions yet — they auto-populate when LIVE pipelines exist.)_"
            )

    # Compatibility advisor: pick two versions, show ADDITIVE/BREAKING report
    if len(_ver_rows) >= 2:
        with st.expander("🔍 Compatibility advisor (compare two Gold versions)", expanded=True):
            adv_c1, adv_c2 = st.columns(2)
            ver_options = [r["version"] for r in _ver_rows]
            with adv_c1:
                from_v = st.selectbox("From version", options=ver_options, key=f"compat_from_{selected_dataset_code}")
            with adv_c2:
                to_v = st.selectbox("To version", options=ver_options, index=0, key=f"compat_to_{selected_dataset_code}")
            if from_v != to_v:
                from datalink.versioning import gold_schema as _gs
                with _warehouse(readonly=True) as _wh_compat:
                    try:
                        report = _gs.compute_compatibility(
                            warehouse=_wh_compat,
                            dataset_code=selected_dataset_code,
                            from_version=int(from_v),
                            to_version=int(to_v),
                        )
                    except Exception as _exc:
                        st.error(f"Compatibility computation failed: {_exc}")
                        report = None
                if report:
                    if report.overall == "ADDITIVE":
                        st.success(f"🟢 **ADDITIVE** — {report.summary()}")
                    else:
                        st.error(f"🔴 **BREAKING** — {report.summary()}")
                    if report.deltas:
                        delta_view = [
                            {
                                "Column": d.column_name,
                                "Change": d.change_type,
                                "Class": d.classification,
                                "Detail": d.detail,
                            }
                            for d in report.deltas
                        ]
                        st.dataframe(
                            pd.DataFrame(delta_view), use_container_width=True, hide_index=True
                        )
                    else:
                        st.caption("(No column-level deltas — versions are structurally identical.)")

    # Legacy Gold version history (Phase 15.x — kept for audit)
    gold_versions = [g for g in all_gold if g["dataset_code"] == selected_dataset_code]
    if gold_versions:
        with st.expander(f"📜 Gold version history ({len(gold_versions)})", expanded=True):
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

    gold_tab_ai, gold_tab_manual, gold_tab_import, gold_tab_file = st.tabs(
        ["🤖 AI Construct", "✏️ Manual Author", "📥 Import", "📂 Contract First (file)"]
    )
    with gold_tab_ai:
        st.markdown(
            f"Agent designs canonical Gold schema for {selected_dataset_display} "
            f"anchored against `{anchor}`."
        )
        # Phase 16.10 — AI spend confirmation gate. Disabled by default.
        _gold_ai_confirm = st.checkbox(
            "✅ I confirm AI spend (~$0.003 per propose)",
            value=False,
            key=f"gold_ai_confirm_{selected_dataset_code}",
            help="Required to enable Propose. Calls Claude Haiku 4.5 against "
            "the catalog + RAG corpus. Defaults OFF to prevent accidents.",
        )
        if st.button(
            "🚀 Propose Gold (AI)",
            type="primary",
            key="gold_ai_propose",
            disabled=not _gold_ai_confirm,
            help=None if _gold_ai_confirm else "🔒 Tick the AI-spend confirm box above to enable.",
        ):
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

    # Phase 16.5 (Wave 5 #4) — Contract-First file upload for Gold.
    with gold_tab_file:
        st.info(
            "📂 **Contract-First file upload is most useful for Silver design.** "
            "For Gold, use **🤖 AI Construct** (which runs against the LIVE Silver "
            "design) or **📥 Import** (paste a Gold spec directly). If you must "
            "upload a file for Gold:"
        )
        gold_uploaded = st.file_uploader(
            "Sample file (Gold-shape)",
            type=["csv", "psv", "tsv", "txt", "json"],
            key=f"gold_file_upload_{selected_dataset_code}",
        )
        if gold_uploaded is not None:
            try:
                content = gold_uploaded.read().decode("utf-8", errors="replace")
                first_line = content.split("\n", 1)[0] if content else ""
                delim = "|" if "|" in first_line else "\t" if "\t" in first_line else ","
                headers = [h.strip() for h in first_line.split(delim)] if first_line else []
                st.markdown(f"**{len(headers)} headers detected:**")
                st.code(", ".join(headers))
                st.caption(
                    "Use these headers as a reference when authoring via Manual "
                    "or Import tabs above. Direct-to-Gold proposal from a file "
                    "is forthcoming (Phase 16.6)."
                )
            except Exception as exc:
                st.error(f"Couldn't read file: {exc}")

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
            expanded=True,
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
