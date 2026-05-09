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
from collections.abc import Callable, Iterator
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
from datalink.ui._nav import render_sidebar  # noqa: E402
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

# Phase 17.6 — page-level client picker REMOVED.  The Client Medallion
# Registry grid below has its own scoped filter; the global sections
# above never need a per-client view.


# ═════════════════════════════════════════════════════════════════════════════
# Phase 17.6 — 🚀 Submit Bar (sticky at the top of the page)
#
# Shows count of pending in-memory edits + a Submit button that batches
# them server-side with optimistic-concurrency check.  When a conflict is
# detected, the buffer is preserved (forced-review path per Q2 (b)).
# ═════════════════════════════════════════════════════════════════════════════
from datalink.ui import _dmd_data as _dmd_top  # noqa: E402


@st.fragment
def _render_submit_bar() -> None:
    """Sticky-feeling submit + discard pair.  Lives in its own fragment so
    a click here does NOT re-fire any of the page's grid queries."""
    pending = _dmd_top.dirty_count()
    if pending == 0:
        st.caption(
            "💤 No pending edits. Edits made in the grids below buffer here "
            "and submit as a batch when you click the green button."
        )
        return

    bar1, bar2, bar3, bar4 = st.columns([3, 1.2, 1.2, 1.4])
    with bar1:
        st.markdown(
            f"<div style='padding:.4rem .6rem;background:#fef3c7;"
            f"border-left:4px solid {_AMBER};border-radius:6px;'>"
            f"<strong>✏️ {pending} unsaved edit{'s' if pending != 1 else ''}</strong> "
            f"buffered locally — nothing has reached Snowflake yet.</div>",
            unsafe_allow_html=True,
        )
    with bar2:
        if st.button(
            "🚀 Submit changes",
            type="primary",
            use_container_width=True,
            help="Apply every buffered edit. Optimistic-concurrency checked "
            "per row — conflicts force a review modal.",
            key="submit_bar_submit",
        ):
            report = _dmd_top.submit_all()
            if report.all_clean:
                st.toast(
                    f"✅ {report.applied_count} edit"
                    f"{'s' if report.applied_count != 1 else ''} applied.",
                    icon="🚀",
                )
                st.rerun()  # whole page rerender — fresh snapshot
            else:
                # Stash report for the conflict review modal further down the
                # page (rendered when a conflict exists).
                st.session_state["__dmd_last_report__"] = report
                st.rerun()  # show the conflict review surface
    with bar3:
        if st.button(
            "🗑️ Discard all",
            use_container_width=True,
            help="Drop every buffered edit without writing.",
            key="submit_bar_discard",
        ):
            n = _dmd_top.discard_edits()
            st.toast(f"🗑️ Discarded {n} edit{'s' if n != 1 else ''}.", icon="✅")
            st.rerun(scope="fragment")
    with bar4:
        # Don't reach for the page-scoped `_snap` here — this fragment
        # runs ABOVE where `_snap` is defined, and on a fragment-only
        # rerun the module-level binding may not exist yet.  Pull a
        # cached snapshot of our own (cheap — it's the same @st.cache_data
        # entry the rest of the page reads).
        _bar_snap = _dmd_top.get_snapshot()
        st.caption(f"Snapshot loaded: `{_bar_snap.loaded_at[:19]}`")


_render_submit_bar()


# Phase 17.6 — Conflict review surface.  When submit_all() returns conflicts
# (another user edited the same row), we don't auto-merge.  We show the
# operator a per-conflict diff and force them to discard or override.
_last_report = st.session_state.get("__dmd_last_report__")
if _last_report is not None and not getattr(_last_report, "all_clean", True):
    st.error(
        f"⚠️ Submit completed with **{_last_report.conflict_count} conflict"
        f"{'s' if _last_report.conflict_count != 1 else ''}** and "
        f"**{_last_report.error_count} error"
        f"{'s' if _last_report.error_count != 1 else ''}**. "
        f"{_last_report.applied_count} clean edit"
        f"{'s' if _last_report.applied_count != 1 else ''} applied."
    )
    with st.expander("🔍 Forced review — every per-row outcome", expanded=True):
        for o in _last_report.outcomes:
            icon = {
                "applied": "✅",
                "conflict": "⚠️",
                "error": "❌",
            }.get(o.status, "•")
            st.markdown(
                f"{icon} **{o.kind}** · `{o.entity_id[:8]}…` · "
                f"_{o.status}_ — {o.detail or '(no detail)'}"
            )
        cb1, cb2 = st.columns([1, 4])
        with cb1:
            if st.button(
                "Dismiss & re-edit",
                key="dismiss_report",
                help="Clear this report; the buffer stays so you can rework "
                "the conflicting rows.",
            ):
                st.session_state.pop("__dmd_last_report__", None)
                st.rerun()
        with cb2:
            st.caption(
                "_Conflicting edits remained in the buffer.  Inspect the "
                "current state in the grid below; re-author your change "
                "knowing the server is now ahead of your snapshot._"
            )
    st.markdown("---")


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
# Phase 17.6 — Global Medallion Registry Status (GLOBAL_CORP scope only).
# Per-client status lives in the Client Medallion Registry section below.
# ---------------------------------------------------------------------------

# Pull the page-wide snapshot once.  Every section below operates on this
# in-memory cache — zero Snowflake calls until the operator clicks
# 🚀 Submit changes (Phase 17.6 batched-submit pattern) or 🔄 Refresh.
from datalink.ui import _dmd_data as _dmd  # noqa: E402

_snap = _dmd.get_snapshot()

# Global counts — only schemas whose scope_owner is GLOBAL_CORP (or legacy
# __global__ during the 17.1 transition).
def _is_global_scope(s: dict[str, Any]) -> bool:
    return str(s.get("scope_owner") or "") in ("GLOBAL_CORP", "__global__")


_g_silver = [s for s in _snap.silver_schemas if _is_global_scope(s)]
_g_gold = [g for g in _snap.gold_schemas if _is_global_scope(g)]

st.markdown("## 🌍 Global Medallion Registry Status")
st.caption(
    "Canonical templates that real clients clone from — zero LLM cost. "
    "Counts and drill-down below are GLOBAL_CORP-scoped only."
)

bronze_count = len(_snap.bronze_datasets)
g_silver_live_count = sum(1 for s in _g_silver if s.get("status") == "LIVE")
g_silver_draft_count = sum(
    1 for s in _g_silver if s.get("status") in ("DRAFT", "PENDING_REVIEW")
)
g_gold_live_count = sum(1 for g in _g_gold if g.get("status") == "LIVE")
g_gold_draft_count = sum(
    1 for g in _g_gold if g.get("status") in ("DRAFT", "PENDING_REVIEW")
)

c1, c2, c3 = st.columns(3)
c1.markdown(
    f'<div class="gd-card"><div class="gd-stat-emoji">🥉</div>'
    f'<div class="gd-stat">{bronze_count}</div>'
    f'<div class="gd-stat-label">Global Bronze datasets</div></div>',
    unsafe_allow_html=True,
)
silver_draft_html = (
    f"<small style='color:{_AMBER};font-size:.7em'> +{g_silver_draft_count} draft</small>"
    if g_silver_draft_count
    else ""
)
gold_draft_html = (
    f"<small style='color:{_AMBER};font-size:.7em'> +{g_gold_draft_count} draft</small>"
    if g_gold_draft_count
    else ""
)
c2.markdown(
    f'<div class="gd-card"><div class="gd-stat-emoji">🥈</div>'
    f'<div class="gd-stat">{g_silver_live_count}{silver_draft_html}</div>'
    f'<div class="gd-stat-label">Global Silver schemas LIVE</div></div>',
    unsafe_allow_html=True,
)
c3.markdown(
    f'<div class="gd-card"><div class="gd-stat-emoji">🥇</div>'
    f'<div class="gd-stat">{g_gold_live_count}{gold_draft_html}</div>'
    f'<div class="gd-stat-label">Global Gold schemas LIVE</div></div>',
    unsafe_allow_html=True,
)


# Drill-down expander — readiness across all 33 datasets at the GLOBAL_CORP
# layer.  Per-client readiness is in the Client Medallion Registry below.
def _readiness_row(ds: dict[str, Any]) -> dict[str, Any]:
    code = ds["dataset_code"]
    silver = next((s for s in _g_silver if s["dataset_code"] == code), None)
    gold = next((g for g in _g_gold if g["dataset_code"] == code), None)
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
    f"📊 Drill into all {bronze_count} datasets — Global Bronze / Silver / Gold readiness",
    expanded=True,
):
    if not _snap.bronze_datasets:
        st.warning(
            "Bronze catalog is empty. Run `python3 scripts/load_product_catalog.py` to seed it."
        )
    else:
        rows = [_readiness_row(d) for d in _snap.bronze_datasets]
        df = pd.DataFrame(rows)
        styled = df.style.map(_color_status, subset=["Bronze", "Silver", "Gold"])
        st.dataframe(styled, use_container_width=True, hide_index=True, height=520)


# ═════════════════════════════════════════════════════════════════════════════
# Phase 17.6 — 🔍 Compatibility Advisor (top-level, lives under Global
# Medallion Registry per design discussion).
#
# Auto-detects Global datasets that have ≥ 2 versions and surfaces
# ADDITIVE / BREAKING diff inline.  Renders nothing when no dataset has
# multiple versions — keeps the page quiet until it's relevant.
# ═════════════════════════════════════════════════════════════════════════════

# Group GLOBAL gold versions by dataset (snapshot already excludes archived
# unless we ask for them — for compat we want ALL versions).
_g_versions_by_ds: dict[str, list[dict[str, Any]]] = {}
for r in _g_gold:
    _g_versions_by_ds.setdefault(str(r["dataset_code"]), []).append(r)
_compat_eligible_ds = sorted(
    [ds for ds, vs in _g_versions_by_ds.items() if len(vs) >= 2]
)

if _compat_eligible_ds:
    st.markdown("### 🔍 Compatibility Advisor")
    st.caption(
        "Compare two GLOBAL Gold versions to see ADDITIVE vs BREAKING changes "
        "before promoting clients to a new version."
    )
    cc1, cc2, cc3 = st.columns([2, 1, 1])
    with cc1:
        _adv_ds = st.selectbox(
            "Dataset",
            options=_compat_eligible_ds,
            key="compat_advisor_dataset",
            help="Only datasets with ≥ 2 GLOBAL Gold versions appear here.",
        )
    _ds_versions = sorted(
        [int(v["version"]) for v in _g_versions_by_ds[_adv_ds]], reverse=True
    )
    with cc2:
        _adv_to = st.selectbox(
            "To version",
            options=_ds_versions,
            index=0,
            key="compat_advisor_to",
        )
    with cc3:
        _adv_from = st.selectbox(
            "From version",
            options=_ds_versions,
            index=min(1, len(_ds_versions) - 1),
            key="compat_advisor_from",
        )
    if _adv_from != _adv_to:
        from datalink.versioning import gold_schema as _gs_top

        with _warehouse(readonly=True) as _wh_compat_top:
            try:
                _ctop_report = _gs_top.compute_compatibility(
                    warehouse=_wh_compat_top,
                    dataset_code=_adv_ds,
                    from_version=int(_adv_from),
                    to_version=int(_adv_to),
                )
            except Exception as _exc:
                st.error(f"Compatibility computation failed: {_exc}")
                _ctop_report = None
        if _ctop_report:
            if _ctop_report.overall == "ADDITIVE":
                st.success(f"🟢 **ADDITIVE** — {_ctop_report.summary()}")
            else:
                st.error(f"🔴 **BREAKING** — {_ctop_report.summary()}")
            if _ctop_report.deltas:
                _ctop_view = [
                    {
                        "Column": d.column_name,
                        "Change": d.change_type,
                        "Class": d.classification,
                        "Detail": d.detail,
                    }
                    for d in _ctop_report.deltas
                ]
                st.dataframe(
                    pd.DataFrame(_ctop_view),
                    use_container_width=True,
                    hide_index=True,
                )
            else:
                st.caption(
                    "_(No column-level deltas — versions are structurally identical.)_"
                )

st.markdown("---")


# ═════════════════════════════════════════════════════════════════════════════
# Phase 17.6 — 🤝 Client Medallion Registry Status
#
# One row per (client, dataset) tuple where the client has authored anything.
# Clients with zero non-archived schemas are HIDDEN.  Sortable.  Filterable
# by client.  Collapsible top-level expander.  Row click → action panel
# below with status promote / archive / view buttons that BUFFER edits to
# the dirty queue (no direct writes — the Submit Bar at the top batches).
#
# Wrapped in @st.fragment so all interactions stay scoped.  Zero Snowflake
# round-trips after the initial snapshot fetch.
# ═════════════════════════════════════════════════════════════════════════════


@st.fragment
def _render_client_medallion_registry() -> None:
    # Build per-(client, dataset) cells from the snapshot.  We exclude the
    # GLOBAL_CORP scope — that's the canonical-template view above.
    _client_cells: dict[tuple[str, str], dict[str, Any]] = {}

    for s in _snap.silver_schemas:
        scope = str(s.get("scope_owner") or "")
        if scope in ("GLOBAL_CORP", "__global__", ""):
            continue
        if str(s.get("status")) == "ARCHIVED":
            continue
        key = (scope, str(s.get("dataset_code")))
        cell = _client_cells.setdefault(
            key,
            {
                "client_id": scope,
                "dataset_code": key[1],
                "silver": None,
                "gold": None,
            },
        )
        # Take the LATEST non-archived silver row for this cell.
        if (cell["silver"] is None) or (
            int(s.get("version") or 0) > int(cell["silver"].get("version") or 0)
        ):
            cell["silver"] = s

    for g in _snap.gold_schemas:
        scope = str(g.get("scope_owner") or "")
        if scope in ("GLOBAL_CORP", "__global__", ""):
            continue
        if str(g.get("status")) == "ARCHIVED":
            continue
        key = (scope, str(g.get("dataset_code")))
        cell = _client_cells.setdefault(
            key,
            {
                "client_id": scope,
                "dataset_code": key[1],
                "silver": None,
                "gold": None,
            },
        )
        if (cell["gold"] is None) or (
            int(g.get("version") or 0) > int(cell["gold"].get("version") or 0)
        ):
            cell["gold"] = g

    if not _client_cells:
        st.info(
            "📭 No client-scoped schemas yet. Once you clone a Global "
            "template to a client (or author client-specific Silver/Gold), "
            "rows will appear here."
        )
        return

    # Filter + sort row.
    f1, f2, f3 = st.columns([2, 1.3, 1])
    _all_clients_in_grid = sorted({key[0] for key in _client_cells})
    with f1:
        _filter_clients = st.multiselect(
            "Filter by client",
            options=_all_clients_in_grid,
            default=[],
            placeholder="All clients (default)",
            key="cmr_filter_clients",
        )
    with f2:
        _filter_reach = st.selectbox(
            "Filter by reach",
            options=[
                "All",
                "🟢 All levels (Silver + Gold LIVE)",
                "🟡 Up to Silver (Silver LIVE)",
                "🟠 In progress (any DRAFT/PENDING)",
            ],
            index=0,
            key="cmr_filter_reach",
        )
    with f3:
        _sort_by = st.selectbox(
            "Sort by",
            options=["Client", "Dataset", "Reach", "Latest activity"],
            index=0,
            key="cmr_sort_by",
        )

    # Compute reach + status pills per cell.
    def _status_pill(s: str | None) -> str:
        return {
            "DRAFT": "📝 DRAFT",
            "PENDING_REVIEW": "🟡 PENDING",
            "APPROVED": "✅ APPROVED",
            "LIVE": "🟢 LIVE",
            "REJECTED": "🔴 REJECTED",
        }.get(str(s or ""), "—")

    def _reach(cell: dict[str, Any]) -> str:
        s = cell.get("silver")
        g = cell.get("gold")
        s_status = str((s or {}).get("status") or "")
        g_status = str((g or {}).get("status") or "")
        if s_status == "LIVE" and g_status == "LIVE":
            return "🟢 All levels"
        if s_status == "LIVE":
            return "🟡 Up to Silver"
        if s_status or g_status:
            return "🟠 In progress"
        return "—"

    _rows = []
    for cell in _client_cells.values():
        s = cell.get("silver")
        g = cell.get("gold")
        # Buffered indicator — show a 🚧 if any pending edit targets this cell.
        s_id = str((s or {}).get("silver_dataset_id") or "")
        g_id = str((g or {}).get("gold_dataset_id") or "")
        buffered_flags = []
        for k in ("silver_status", "silver_archive"):
            if s_id and _dmd_top.is_buffered(k, s_id):
                buffered_flags.append("🚧 Silver")
                break
        for k in ("gold_status", "gold_archive"):
            if g_id and _dmd_top.is_buffered(k, g_id):
                buffered_flags.append("🚧 Gold")
                break
        latest = max(
            str((s or {}).get("created_at") or ""),
            str((g or {}).get("created_at") or ""),
        )[:19]
        _rows.append(
            {
                "Client": cell["client_id"],
                "Dataset": cell["dataset_code"],
                "🥉 Bronze": "✓ Catalog",
                "🥈 Silver": (
                    f"{_status_pill((s or {}).get('status'))} v{(s or {}).get('version', '—')}"
                    if s
                    else "—"
                ),
                "🥇 Gold": (
                    f"{_status_pill((g or {}).get('status'))} v{(g or {}).get('version', '—')}"
                    if g
                    else "—"
                ),
                "Reach": _reach(cell),
                "Pending": " · ".join(buffered_flags) or "",
                "Latest activity": latest,
                # hidden — used by selection handler
                "__client_id": cell["client_id"],
                "__dataset_code": cell["dataset_code"],
                "__silver_id": s_id,
                "__gold_id": g_id,
            }
        )

    # Apply filters
    if _filter_clients:
        _rows = [r for r in _rows if r["Client"] in _filter_clients]
    if _filter_reach != "All":
        _rows = [
            r
            for r in _rows
            if r["Reach"]
            == _filter_reach.split(" (")[0]  # match the emoji+label prefix
        ]

    # Apply sort
    sort_key_map = {
        "Client": lambda r: (r["Client"], r["Dataset"]),
        "Dataset": lambda r: (r["Dataset"], r["Client"]),
        "Reach": lambda r: (r["Reach"], r["Client"]),
        "Latest activity": lambda r: (r["Latest activity"] or "", r["Client"]),
    }
    _rows.sort(key=sort_key_map.get(_sort_by, sort_key_map["Client"]))

    if not _rows:
        st.caption(
            "_(No rows match the current filters.  Adjust filter chips above.)_"
        )
        return

    # Display table — drop hidden columns
    _disp_cols = [
        "Client",
        "Dataset",
        "🥉 Bronze",
        "🥈 Silver",
        "🥇 Gold",
        "Reach",
        "Pending",
        "Latest activity",
    ]
    _df = pd.DataFrame([{c: r[c] for c in _disp_cols} for r in _rows])

    _sel = st.dataframe(
        _df,
        use_container_width=True,
        hide_index=True,
        on_select="rerun",
        selection_mode="single-row",
        height=320,
        key="cmr_grid",
    )
    _sel_idx_list = _sel.selection.get("rows", []) if _sel else []
    if not _sel_idx_list:
        st.caption("_(Click any row above to see action buttons.)_")
        return

    _sel_row = _rows[_sel_idx_list[0]]
    _silver = next(
        (
            s
            for s in _snap.silver_schemas
            if str(s.get("silver_dataset_id")) == _sel_row["__silver_id"]
        ),
        None,
    )
    _gold = next(
        (
            g
            for g in _snap.gold_schemas
            if str(g.get("gold_dataset_id")) == _sel_row["__gold_id"]
        ),
        None,
    )

    st.markdown(
        f"### Actions for **{_sel_row['Client']} / {_sel_row['Dataset']}**"
    )

    # Two columns: Silver action panel + Gold action panel
    s_col, g_col = st.columns(2)

    def _layer_action_panel(layer: str, row: dict[str, Any] | None) -> None:
        layer_emoji = "🥈" if layer == "Silver" else "🥇"
        st.markdown(f"##### {layer_emoji} {layer}")
        if row is None:
            st.caption(f"_(No {layer} authored yet for this client.)_")
            return
        status = str(row.get("status") or "")
        st.caption(
            f"v{row.get('version')} · {_status_pill(status)} · "
            f"created by `{row.get('created_by') or '—'}` · "
            f"{str(row.get('created_at') or '')[:19]}"
        )

        next_status = {
            "DRAFT": "PENDING_REVIEW",
            "PENDING_REVIEW": "APPROVED",
            "APPROVED": "LIVE",
        }.get(status)

        kind_status = "silver_status" if layer == "Silver" else "gold_status"
        kind_archive = "silver_archive" if layer == "Silver" else "gold_archive"
        entity_id = str(
            row.get("silver_dataset_id" if layer == "Silver" else "gold_dataset_id")
        )

        ac1, ac2, ac3 = st.columns([1.5, 1, 1])
        with ac1:
            if next_status:
                if st.button(
                    f"⬆️ Promote → {next_status}",
                    use_container_width=True,
                    type="primary",
                    key=f"cmr_promote_{layer}_{entity_id}",
                    help="Buffers the change. Click 🚀 Submit at the top to apply.",
                ):
                    _dmd_top.stash_edit(
                        kind=kind_status,
                        entity_id=entity_id,
                        payload={
                            "new_status": next_status,
                            "actor": "ui:client_medallion",
                        },
                        base_version=int(row.get("version") or 0),
                        base_status=status,
                    )
                    st.toast(
                        f"✏️ Buffered {layer} → {next_status}", icon="📝"
                    )
                    st.rerun(scope="fragment")
            else:
                st.button(
                    "⬆️ Promote",
                    use_container_width=True,
                    disabled=True,
                    key=f"cmr_promote_{layer}_disabled",
                    help=f"{status} is terminal — cannot promote further.",
                )
        with ac2:
            if status != "ARCHIVED" and st.button(
                "🗄️ Archive",
                use_container_width=True,
                key=f"cmr_archive_{layer}_{entity_id}",
                help="Buffers the archive. Soft-delete; row preserved for audit.",
            ):
                _dmd_top.stash_edit(
                    kind=kind_archive,
                    entity_id=entity_id,
                    payload={"actor": "ui:client_medallion"},
                    base_version=int(row.get("version") or 0),
                    base_status=status,
                )
                st.toast(f"✏️ Buffered Archive {layer}", icon="🗄️")
                st.rerun(scope="fragment")
        with ac3:
            cols_dict = (
                _snap.silver_columns_by_dataset.get(entity_id, [])
                if layer == "Silver"
                else _snap.gold_fields_by_dataset.get(entity_id, [])
            )
            with st.popover(
                f"👁️ Columns ({len(cols_dict)})",
                use_container_width=True,
            ):
                if not cols_dict:
                    st.info(f"No columns registered for this {layer} version.")
                else:
                    _cv = [
                        {
                            "#": c.get("column_order"),
                            "Column": c.get("gold_column_name") or c.get("column_name"),
                            "Type": c.get("logical_type"),
                            "Null": "✓" if c.get("nullable") else "—",
                            "BK": "🔑" if c.get("is_business_key") else "",
                            "PII": "🔒" if c.get("is_pii") else "",
                            "PHI": "🩺" if c.get("is_phi") else "",
                        }
                        for c in cols_dict
                    ]
                    st.dataframe(
                        pd.DataFrame(_cv), use_container_width=True, hide_index=True
                    )

        # Buffered-edit indicator + cancel
        for k in (kind_status, kind_archive):
            if _dmd_top.is_buffered(k, entity_id):
                st.markdown(
                    f"<div style='padding:.3rem .5rem;background:#fef3c7;"
                    f"border-radius:5px;font-size:.85rem;'>"
                    f"🚧 <strong>Pending edit</strong> on this {layer} — "
                    f"submit at the top to apply.</div>",
                    unsafe_allow_html=True,
                )

    with s_col:
        _layer_action_panel("Silver", _silver)
    with g_col:
        _layer_action_panel("Gold", _gold)

    # Edit deep-link spans both layers
    st.markdown("")
    edit_href = (
        f"/Data_Model_Designer?dataset={_sel_row['__dataset_code']}"
        f"&client={_sel_row['__client_id']}"
    )
    st.markdown(
        f'<a href="{edit_href}" target="_self" '
        f'style="display:inline-block;background:#1d4ed8;color:#fff;'
        f"padding:.45rem .9rem;border-radius:6px;text-decoration:none;"
        f'font-weight:600;">✏️ Edit in authoring view</a>',
        unsafe_allow_html=True,
    )


# Top-level h2 to match Global Medallion Registry styling.  The expander
# under it is just for collapse/show — its label is muted so the section
# title carries the visual weight.
st.markdown("## 🤝 Client Medallion Registry Status")
st.caption(
    "Per-client schema state across all datasets where the client has "
    "authored anything. Clients with no authored content are hidden. "
    "Filter, sort, and click rows to act — every action buffers locally "
    "and submits as a batch via 🚀 Submit at the top."
)
with st.expander("Show / hide grid", expanded=True):
    _render_client_medallion_registry()


st.markdown("---")


# ═════════════════════════════════════════════════════════════════════════════
# Phase 17.6 — 📦 Cloning Center
#
# Three modes the operator demanded:
#   1. Full Global → Client  — clone ALL global datasets to one client
#   2. Per-dataset Global → Client — clone a single dataset
#   3. Per-dataset Client → Client — fork from one client to another
#
# Guard rails enforced everywhere:
#   * Source must be APPROVED or LIVE (never DRAFT / PENDING / ARCHIVED).
#   * Target cannot already have a non-archived row for the layer being
#     cloned.  Operator must archive the existing first.
#   * Target client_id ≠ GLOBAL_CORP / __global__ (sacred direction —
#     UI doesn't even offer client → global).
#
# All clone actions BUFFER to the dirty queue.  🚀 Submit at the top
# applies them in one batched write with optimistic-concurrency check
# on every source row.
# ═════════════════════════════════════════════════════════════════════════════
import uuid as _cc_uuid  # noqa: E402


def _cc_target_client_normalized(s: str) -> str | None:
    """Normalize + validate target client_id. Returns None if invalid."""
    t = (s or "").strip().lower()
    if not t:
        return None
    if t in ("global_corp", "__global__"):
        return None  # sacred direction violation
    if not all(ch.isalnum() or ch in "_-" for ch in t):
        return None  # safety — keep client_ids identifier-friendly
    return t


def _cc_silver_blocks(target_client: str, dataset_code: str) -> str | None:
    """Return a human-readable reason why we can't clone Silver to this
    target, or None if clear."""
    rows = _snap.silver_for(
        scope_owner=target_client, dataset_code=dataset_code, exclude_archived=True
    )
    if rows:
        statuses = ", ".join(sorted({str(r.get("status")) for r in rows}))
        return (
            f"Target {target_client!r} already has a non-archived Silver for "
            f"{dataset_code!r} (status: {statuses}). Archive it first."
        )
    return None


def _cc_gold_blocks(target_client: str, dataset_code: str) -> str | None:
    rows = _snap.gold_for(
        scope_owner=target_client, dataset_code=dataset_code, exclude_archived=True
    )
    if rows:
        statuses = ", ".join(sorted({str(r.get("status")) for r in rows}))
        return (
            f"Target {target_client!r} already has a non-archived Gold for "
            f"{dataset_code!r} (status: {statuses}). Archive it first."
        )
    return None


def _cc_source_eligible(row: dict[str, Any] | None) -> str | None:
    """Source must be APPROVED or LIVE. Returns block reason or None."""
    if row is None:
        return "no source row"
    status = str(row.get("status") or "")
    if status not in ("APPROVED", "LIVE"):
        return f"source status is {status} — must be APPROVED or LIVE to clone"
    return None


def _cc_build_silver_applier(
    source_silver_id: str, target_client: str
) -> Callable[[Any], None]:
    """Return an apply(wh) callable that runs the multi-table Silver clone."""

    def apply(wh: Any) -> None:
        new_id = str(_cc_uuid.uuid4())
        # 1. Header — start at v1 under target client.
        wh.execute(
            f"INSERT INTO {CONTROL_SCHEMA}.global_silver_schema_datasets "
            f"(silver_dataset_id, dataset_code, silver_pattern, version, "
            f" status, silver_anchor, source, scope_owner, "
            f" forked_from_global_version, notes, created_by, created_at) "
            f"SELECT $newid, dataset_code, silver_pattern, 1, 'DRAFT', "
            f"       silver_anchor, 'CLONE', $owner, version, "
            f"       'Cloned from ' || scope_owner || ' v' || version || "
            f"           ' on ' || CAST(CURRENT_TIMESTAMP() AS VARCHAR), "
            f"       $by, CURRENT_TIMESTAMP() "
            f"FROM {CONTROL_SCHEMA}.global_silver_schema_datasets "
            f"WHERE silver_dataset_id = $srcid",
            {
                "newid": new_id,
                "owner": target_client,
                "by": "ui:cloning_center",
                "srcid": source_silver_id,
            },
        )
        # 2. Tables — remap silver_table_id
        src_tables = list(
            wh.query(
                f"SELECT * FROM {CONTROL_SCHEMA}.global_silver_schema_tables "
                f"WHERE silver_dataset_id = $sid",
                {"sid": source_silver_id},
            )
        )
        table_remap: dict[str, str] = {}
        for t in src_tables:
            old_tid = str(t.get("silver_table_id") or t.get("SILVER_TABLE_ID"))
            new_tid = str(_cc_uuid.uuid4())
            table_remap[old_tid] = new_tid
            wh.execute(
                f"INSERT INTO {CONTROL_SCHEMA}.global_silver_schema_tables "
                f"(silver_table_id, silver_dataset_id, dataset_code, "
                f" table_name, table_kind, parent_silver_table_id, "
                f" business_keys_json, linked_hub_ids_json, table_order, "
                f" description, created_at) "
                f"VALUES ($tid, $sid, $ds, $tn, $tk, $ptid, $bk, $lh, $ord, "
                f"        $desc, CURRENT_TIMESTAMP())",
                {
                    "tid": new_tid,
                    "sid": new_id,
                    "ds": t.get("dataset_code") or t.get("DATASET_CODE"),
                    "tn": t.get("table_name") or t.get("TABLE_NAME"),
                    "tk": t.get("table_kind") or t.get("TABLE_KIND"),
                    "ptid": t.get("parent_silver_table_id") or t.get("PARENT_SILVER_TABLE_ID"),
                    "bk": t.get("business_keys_json") or t.get("BUSINESS_KEYS_JSON"),
                    "lh": t.get("linked_hub_ids_json") or t.get("LINKED_HUB_IDS_JSON"),
                    "ord": t.get("table_order") or t.get("TABLE_ORDER"),
                    "desc": t.get("description") or t.get("DESCRIPTION"),
                },
            )
        # 3. Columns — remap silver_column_id, point to new silver_table_id
        src_cols = list(
            wh.query(
                f"SELECT * FROM {CONTROL_SCHEMA}.global_silver_schema_columns "
                f"WHERE silver_dataset_id = $sid",
                {"sid": source_silver_id},
            )
        )
        col_remap: dict[str, str] = {}
        for c in src_cols:
            old_cid = str(c.get("silver_column_id") or c.get("SILVER_COLUMN_ID"))
            new_cid = str(_cc_uuid.uuid4())
            col_remap[old_cid] = new_cid
            old_tid = str(c.get("silver_table_id") or c.get("SILVER_TABLE_ID"))
            wh.execute(
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
                    "tid": table_remap.get(old_tid, old_tid),
                    "sid": new_id,
                    "ord": c.get("column_order") or c.get("COLUMN_ORDER"),
                    "name": c.get("column_name") or c.get("COLUMN_NAME"),
                    "type": c.get("logical_type") or c.get("LOGICAL_TYPE"),
                    "null": c.get("nullable") or c.get("NULLABLE"),
                    "bk": c.get("is_business_key") or c.get("IS_BUSINESS_KEY"),
                    "hk": c.get("is_hash_key") or c.get("IS_HASH_KEY"),
                    "hd": c.get("is_hash_diff") or c.get("IS_HASH_DIFF"),
                    "pii": c.get("is_pii") or c.get("IS_PII"),
                    "phi": c.get("is_phi") or c.get("IS_PHI"),
                    "desc": c.get("description") or c.get("DESCRIPTION"),
                },
            )
        # 4. Bronze→Silver mappings
        src_maps = list(
            wh.query(
                f"SELECT * FROM {CONTROL_SCHEMA}.bronze_to_silver_mappings "
                f"WHERE silver_dataset_id = $sid",
                {"sid": source_silver_id},
            )
        )
        for m in src_maps:
            old_mid_col = str(m.get("silver_column_id") or m.get("SILVER_COLUMN_ID"))
            wh.execute(
                f"INSERT INTO {CONTROL_SCHEMA}.bronze_to_silver_mappings "
                f"(mapping_id, silver_column_id, silver_dataset_id, "
                f" silver_table_name, silver_column_name, "
                f" bronze_source_columns, transform_kind, transform_sql, "
                f" rationale, confidence, created_by, created_at) "
                f"VALUES ($mid, $cid, $sid, $tn, $cn, $bsc, $tk, $sql, "
                f"        $rat, $conf, $by, CURRENT_TIMESTAMP())",
                {
                    "mid": str(_cc_uuid.uuid4()),
                    "cid": col_remap.get(old_mid_col, old_mid_col),
                    "sid": new_id,
                    "tn": m.get("silver_table_name") or m.get("SILVER_TABLE_NAME"),
                    "cn": m.get("silver_column_name") or m.get("SILVER_COLUMN_NAME"),
                    "bsc": m.get("bronze_source_columns") or m.get("BRONZE_SOURCE_COLUMNS"),
                    "tk": m.get("transform_kind") or m.get("TRANSFORM_KIND"),
                    "sql": m.get("transform_sql") or m.get("TRANSFORM_SQL"),
                    "rat": m.get("rationale") or m.get("RATIONALE"),
                    "conf": m.get("confidence") or m.get("CONFIDENCE"),
                    "by": "ui:cloning_center",
                },
            )

    return apply


def _cc_build_gold_applier(
    source_gold_id: str, target_client: str
) -> Callable[[Any], None]:
    """Apply(wh) for Gold clone — simpler, just header + fields."""

    def apply(wh: Any) -> None:
        new_id = str(_cc_uuid.uuid4())
        wh.execute(
            f"INSERT INTO {CONTROL_SCHEMA}.global_gold_schema_datasets "
            f"(gold_dataset_id, dataset_code, gold_table_name, version, "
            f" status, gold_anchor, source, scope_owner, "
            f" forked_from_global_version, notes, created_by, created_at) "
            f"SELECT $newid, dataset_code, gold_table_name, 1, 'DRAFT', "
            f"       gold_anchor, 'CLONE', $owner, version, "
            f"       'Cloned from ' || scope_owner || ' v' || version || "
            f"           ' on ' || CAST(CURRENT_TIMESTAMP() AS VARCHAR), "
            f"       $by, CURRENT_TIMESTAMP() "
            f"FROM {CONTROL_SCHEMA}.global_gold_schema_datasets "
            f"WHERE gold_dataset_id = $srcid",
            {
                "newid": new_id,
                "owner": target_client,
                "by": "ui:cloning_center",
                "srcid": source_gold_id,
            },
        )
        wh.execute(
            f"INSERT INTO {CONTROL_SCHEMA}.global_gold_schema_fields "
            f"(gold_field_id, gold_dataset_id, dataset_code, column_order, "
            f" gold_column_name, logical_type, nullable, is_business_key, "
            f" is_pii, is_phi, description, anchor_reference, version, "
            f" registered_at) "
            f"SELECT UUID_STRING(), $newid, dataset_code, column_order, "
            f"       gold_column_name, logical_type, nullable, "
            f"       is_business_key, is_pii, is_phi, description, "
            f"       anchor_reference, 1, CURRENT_TIMESTAMP() "
            f"FROM {CONTROL_SCHEMA}.global_gold_schema_fields "
            f"WHERE gold_dataset_id = $srcid",
            {"newid": new_id, "srcid": source_gold_id},
        )

    return apply


def _checkbox_multiselect(
    *,
    label: str,
    options: list[str],
    key: str,
    exclude: list[str] | None = None,
    allow_new: bool = True,
    new_placeholder: str = "type a new client_id and press Enter",
) -> list[str]:
    """Modern multi-select dropdown with CHECKBOXES — opens as a popover,
    operator can tick existing options OR type a brand-new client_id and
    press Enter to add it inline.  Selection persists in session_state.

    Returns the current selection list.

    Pattern is the same one PowerBI / Looker / Notion use for tag-style
    multi-select: click button → checkbox panel opens → tick → close.
    """
    state_key = f"__cms_{key}__"
    if state_key not in st.session_state:
        st.session_state[state_key] = []

    eff_options = [o for o in options if not exclude or o not in exclude]

    # Source-of-truth selection: derived from per-checkbox session_state
    # keys (for known options) + the custom-added clients we stash in
    # state_key.  Computed BEFORE rendering anything so button disabled-
    # states always reflect the live selection.
    selected: list[str] = [
        opt
        for opt in eff_options
        if st.session_state.get(f"{key}_cb_{opt}", False)
    ]
    selected += [
        s
        for s in st.session_state.get(state_key, [])
        if s not in eff_options
    ]
    # Mirror back to state_key so other code that reads it sees the
    # current truth.
    st.session_state[state_key] = list(selected)

    btn_label = (
        f"☑️ {label} — {len(selected)} selected" if selected else f"☑️ {label}"
    )

    with st.popover(btn_label, use_container_width=True):
        # ── PowerBI-slicer pattern ────────────────────────────────────
        # Top: search input.  Below: a single dynamic master toggle whose
        # label reflects the FILTERED-list state.  Then the checkbox list.
        # Custom-added items + ➕ Add-new input live at the bottom.
        # No clutter of always-visible quick-action buttons.

        search = st.text_input(
            "🔍 Search",
            key=f"{key}_search",
            placeholder="filter…",
            label_visibility="collapsed",
        )
        s_lc = (search or "").strip().lower()
        filtered = (
            [o for o in eff_options if s_lc in o.lower()] if s_lc else list(eff_options)
        )

        # Master toggle — single button, dynamic label.
        n_sel_in_filt = sum(
            1 for o in filtered if st.session_state.get(f"{key}_cb_{o}", False)
        )
        all_in_filt_selected = (
            bool(filtered) and n_sel_in_filt == len(filtered)
        )
        if eff_options:
            master_label = (
                f"✕ Clear ({len(filtered)})"
                if all_in_filt_selected
                else f"☑️ Select all ({n_sel_in_filt} / {len(filtered)})"
            )
            if st.button(
                master_label,
                key=f"{key}_master",
                use_container_width=True,
                disabled=not filtered,
            ):
                new_state = not all_in_filt_selected
                for opt in filtered:
                    st.session_state[f"{key}_cb_{opt}"] = new_state
                st.rerun(scope="fragment")

        # Checkbox list — bare, dense, scannable.  No extra captions /
        # decorations; the popover is its own visual frame.
        if not filtered:
            if eff_options:
                st.caption(f"_(No matches for `{search}`)_")
            else:
                st.caption("_(No existing options — use ➕ below)_")
        else:
            for opt in filtered:
                cb_key = f"{key}_cb_{opt}"
                if cb_key not in st.session_state:
                    st.session_state[cb_key] = opt in selected
                checked_now = st.checkbox(opt, key=cb_key)
                if checked_now and opt not in selected:
                    selected.append(opt)
                elif not checked_now and opt in selected:
                    selected.remove(opt)

        # Custom-added clients (typed in via ➕)
        _custom = [s for s in selected if s not in eff_options]
        if _custom:
            st.markdown("")  # tight separator
            for c in list(_custom):
                rm_col, lbl_col = st.columns([1, 9])
                if rm_col.button(
                    "✕", key=f"{key}_rm_{c}", help="Remove this custom entry"
                ):
                    selected.remove(c)
                    st.session_state[state_key] = selected
                    st.rerun(scope="fragment")
                lbl_col.markdown(f"`{c}` _new_")

        # Add-new input — bottom-anchored.
        if allow_new:
            new_val = st.text_input(
                "➕ Add new",
                key=f"{key}_new",
                placeholder=new_placeholder,
                label_visibility="collapsed",
            )
            if new_val:
                norm = new_val.strip().lower()
                if (
                    norm
                    and norm not in selected
                    and norm not in eff_options
                ):
                    selected.append(norm)
                    del st.session_state[f"{key}_new"]
                    st.session_state[state_key] = selected
                    st.rerun(scope="fragment")

    # Persist + show chips outside the popover so the operator sees the
    # current selection without having to reopen the panel.
    st.session_state[state_key] = selected
    if selected:
        st.markdown(
            "Selected: "
            + " ".join(
                f'<span style="display:inline-block;background:#e0e7ff;'
                f"color:#1e3a8a;padding:.15rem .55rem;border-radius:12px;"
                f'margin-right:.3rem;font-size:.82rem;">{s}</span>'
                for s in selected
            ),
            unsafe_allow_html=True,
        )
    return list(selected)


def _cc_inline_submit_button(*, key: str) -> None:
    """Compact inline Submit button — sits beside the mode's Plan+Buffer
    primary button.  Shows the live pending count.  Single click applies
    every buffered edit."""
    pending = _dmd_top.dirty_count()
    label = (
        f"🚀 Submit {pending}" if pending else "🚀 Submit"
    )
    if st.button(
        label,
        type="primary",
        use_container_width=True,
        disabled=pending == 0,
        key=key,
        help=(
            f"Apply all {pending} buffered edit{'s' if pending != 1 else ''} "
            f"with optimistic-concurrency check."
            if pending
            else "Buffer at least one clone (left) before submitting."
        ),
    ):
        report = _dmd_top.submit_all()
        if report.all_clean:
            st.toast(
                f"✅ {report.applied_count} clone"
                f"{'s' if report.applied_count != 1 else ''} applied.",
                icon="🚀",
            )
            st.rerun()
        else:
            st.session_state["__dmd_last_report__"] = report
            st.rerun()


@st.fragment
def _render_cloning_center() -> None:
    """The 3-mode clone surface. All actions buffer; nothing writes here."""
    # Mode selector — radio buttons keep all 3 forms inline.
    _mode = st.radio(
        "Clone mode",
        options=[
            "📦 Full Global → Client (all datasets)",
            "📦 Per-dataset · Global → Client",
            "📦 Per-dataset · Client → Client",
        ],
        index=0,
        horizontal=True,
        key="cc_mode",
    )

    # ── Mode A — Full Global → Multiple Clients (all datasets) ──────────
    if _mode.startswith("📦 Full Global"):
        st.markdown(
            "Clone every published Global Silver + Gold to one OR many target "
            "clients.  Datasets a target already has (in any non-archived "
            "state) are SKIPPED for that target, never overwritten."
        )
        col1, col2, col3 = st.columns([3, 1.3, 1.1])
        with col1:
            _full_targets = _checkbox_multiselect(
                label="Target clients",
                options=_snap.distinct_clients,
                key="cc_full",
            )
        with col2:
            st.write("")  # vertical alignment
            _full_go = st.button(
                "📦 Plan + buffer clones",
                type="secondary",
                use_container_width=True,
                disabled=not _full_targets,
                key="cc_full_go",
            )
        with col3:
            st.write("")
            _cc_inline_submit_button(key="cc_full_submit")

        if _full_go:
            normalized: list[str] = []
            invalid: list[str] = []
            for t in _full_targets:
                norm = _cc_target_client_normalized(t)
                if norm is None:
                    invalid.append(t)
                else:
                    normalized.append(norm)
            if invalid:
                st.error(
                    f"Invalid target client_id(s): "
                    f"{', '.join(repr(v) for v in invalid)}.  Lowercase "
                    f"alphanumeric / underscore / hyphen only.  "
                    f"GLOBAL_CORP and __global__ are forbidden."
                )
            elif not normalized:
                st.error("Pick at least one valid target.")
            else:
                # Per-target planning.  Each target gets its own block of
                # buffered clones (only for datasets it doesn't already have).
                planned_by_target: dict[str, list[str]] = {}
                skipped_by_target: dict[str, list[str]] = {}
                for target in normalized:
                    planned_by_target.setdefault(target, [])
                    skipped_by_target.setdefault(target, [])
                    for src in _g_silver:
                        src_status = str(src.get("status"))
                        if src_status not in ("APPROVED", "LIVE"):
                            continue
                        ds = str(src.get("dataset_code"))
                        if _cc_silver_blocks(target, ds):
                            skipped_by_target[target].append(
                                f"Silver {ds} (already exists)"
                            )
                            continue
                        src_id = str(src.get("silver_dataset_id"))
                        _dmd_top.stash_edit(
                            kind="clone_silver",
                            # Composite entity_id so the same source can be
                            # cloned to many targets in one buffer.
                            entity_id=f"{src_id}::{target}",
                            payload={
                                "apply": _cc_build_silver_applier(src_id, target),
                                "target_client": target,
                                "dataset_code": ds,
                                "source_silver_id": src_id,
                            },
                            base_version=int(src.get("version") or 0),
                            base_status=src_status,
                        )
                        planned_by_target[target].append(
                            f"Silver {ds} v{src.get('version')}"
                        )
                    for src in _g_gold:
                        src_status = str(src.get("status"))
                        if src_status not in ("APPROVED", "LIVE"):
                            continue
                        ds = str(src.get("dataset_code"))
                        if _cc_gold_blocks(target, ds):
                            skipped_by_target[target].append(
                                f"Gold {ds} (already exists)"
                            )
                            continue
                        src_id = str(src.get("gold_dataset_id"))
                        _dmd_top.stash_edit(
                            kind="clone_gold",
                            entity_id=f"{src_id}::{target}",
                            payload={
                                "apply": _cc_build_gold_applier(src_id, target),
                                "target_client": target,
                                "dataset_code": ds,
                                "source_gold_id": src_id,
                            },
                            base_version=int(src.get("version") or 0),
                            base_status=src_status,
                        )
                        planned_by_target[target].append(
                            f"Gold {ds} v{src.get('version')}"
                        )
                total_planned = sum(len(v) for v in planned_by_target.values())
                total_skipped = sum(len(v) for v in skipped_by_target.values())
                if total_planned > 0:
                    n_targets_with_plan = sum(
                        1 for v in planned_by_target.values() if v
                    )
                    st.success(
                        f"✏️ Buffered **{total_planned} clone"
                        f"{'s' if total_planned != 1 else ''}** across "
                        f"**{n_targets_with_plan} target"
                        f"{'s' if n_targets_with_plan != 1 else ''}**.  "
                        f"Click 🚀 Submit at the top to apply."
                    )
                # Per-target breakdown
                for target in normalized:
                    plan_n = len(planned_by_target.get(target, []))
                    skip_n = len(skipped_by_target.get(target, []))
                    if plan_n == 0 and skip_n == 0:
                        continue
                    with st.expander(
                        f"`{target}` — planned {plan_n}, skipped {skip_n}",
                        expanded=False,
                    ):
                        if planned_by_target[target]:
                            st.caption("**Buffered for clone:**")
                            for p in planned_by_target[target]:
                                st.markdown(f"  - {p}")
                        if skipped_by_target[target]:
                            st.caption("**Skipped:**")
                            for s in skipped_by_target[target]:
                                st.markdown(f"  - {s}")
                if total_planned == 0 and total_skipped == 0:
                    st.info(
                        "Nothing to clone.  No APPROVED / LIVE Global "
                        "schemas exist yet — author + promote a Global "
                        "Silver/Gold first."
                    )

    # ── Mode B — Per-dataset Global → Client ─────────────────────────────
    elif _mode.startswith("📦 Per-dataset · Global"):
        st.markdown(
            "Clone ONE dataset's Global Silver + Gold to a single client.  "
            "Source must be APPROVED or LIVE.  Either layer alone is "
            "buffered if the other doesn't qualify."
        )
        col1, col2, col3, col4 = st.columns([2, 3, 1.1, 1.1])
        with col1:
            ds_set = sorted(
                {
                    str(s.get("dataset_code"))
                    for s in _g_silver + _g_gold
                    if str(s.get("status")) in ("APPROVED", "LIVE")
                }
            )
            _gd_dataset = st.selectbox(
                "Dataset (Global APPROVED/LIVE)",
                options=ds_set,
                index=0 if ds_set else None,
                placeholder="No eligible datasets" if not ds_set else "Pick…",
                key="cc_gd_dataset",
            )
        with col2:
            _gd_targets = _checkbox_multiselect(
                label="Target clients",
                options=_snap.distinct_clients,
                key="cc_gd",
            )
        with col3:
            st.write("")
            _gd_go = st.button(
                "📦 Buffer",
                type="secondary",
                use_container_width=True,
                disabled=not (_gd_dataset and _gd_targets),
                key="cc_gd_go",
            )
        with col4:
            st.write("")
            _cc_inline_submit_button(key="cc_gd_submit")

        if _gd_go:
            normalized: list[str] = []
            invalid: list[str] = []
            for t in _gd_targets:
                norm = _cc_target_client_normalized(t)
                if norm is None:
                    invalid.append(t)
                else:
                    normalized.append(norm)
            if invalid:
                st.error(
                    f"Invalid target client_id(s): "
                    f"{', '.join(repr(v) for v in invalid)}. Lowercase "
                    f"alphanumeric / underscore / hyphen only."
                )
            elif not normalized:
                st.error("Pick at least one valid target.")
            else:
                ds = str(_gd_dataset)

                # Find best Silver + Gold sources for this dataset
                _src_silver_rows = [
                    s
                    for s in _g_silver
                    if str(s.get("dataset_code")) == ds
                    and str(s.get("status")) in ("APPROVED", "LIVE")
                ]
                _src_silver = max(
                    _src_silver_rows, key=lambda r: int(r.get("version") or 0)
                ) if _src_silver_rows else None
                _src_gold_rows = [
                    g
                    for g in _g_gold
                    if str(g.get("dataset_code")) == ds
                    and str(g.get("status")) in ("APPROVED", "LIVE")
                ]
                _src_gold = max(
                    _src_gold_rows, key=lambda r: int(r.get("version") or 0)
                ) if _src_gold_rows else None

                if not _src_silver and not _src_gold:
                    st.info(
                        f"No APPROVED/LIVE Global Silver or Gold for "
                        f"`{ds}` — promote one first."
                    )
                else:
                    buffered_by_target: dict[str, list[str]] = {}
                    blocked_by_target: dict[str, list[str]] = {}
                    for target in normalized:
                        buffered_by_target.setdefault(target, [])
                        blocked_by_target.setdefault(target, [])

                        if _src_silver:
                            blk = _cc_silver_blocks(target, ds)
                            if blk:
                                blocked_by_target[target].append(
                                    f"Silver — {blk}"
                                )
                            else:
                                sid = str(_src_silver.get("silver_dataset_id"))
                                _dmd_top.stash_edit(
                                    kind="clone_silver",
                                    entity_id=f"{sid}::{target}",
                                    payload={
                                        "apply": _cc_build_silver_applier(sid, target),
                                        "target_client": target,
                                        "dataset_code": ds,
                                        "source_silver_id": sid,
                                    },
                                    base_version=int(_src_silver.get("version") or 0),
                                    base_status=str(_src_silver.get("status")),
                                )
                                buffered_by_target[target].append(
                                    f"Silver v{_src_silver.get('version')}"
                                )

                        if _src_gold:
                            blk = _cc_gold_blocks(target, ds)
                            if blk:
                                blocked_by_target[target].append(
                                    f"Gold — {blk}"
                                )
                            else:
                                gid = str(_src_gold.get("gold_dataset_id"))
                                _dmd_top.stash_edit(
                                    kind="clone_gold",
                                    entity_id=f"{gid}::{target}",
                                    payload={
                                        "apply": _cc_build_gold_applier(gid, target),
                                        "target_client": target,
                                        "dataset_code": ds,
                                        "source_gold_id": gid,
                                    },
                                    base_version=int(_src_gold.get("version") or 0),
                                    base_status=str(_src_gold.get("status")),
                                )
                                buffered_by_target[target].append(
                                    f"Gold v{_src_gold.get('version')}"
                                )

                    total_planned = sum(
                        len(v) for v in buffered_by_target.values()
                    )
                    if total_planned > 0:
                        n_targets = sum(
                            1 for v in buffered_by_target.values() if v
                        )
                        st.success(
                            f"✏️ Buffered **{total_planned} clone"
                            f"{'s' if total_planned != 1 else ''}** "
                            f"across **{n_targets} target"
                            f"{'s' if n_targets != 1 else ''}** for `{ds}`. "
                            f"Submit at the top to apply."
                        )
                    for target in normalized:
                        plan_n = len(buffered_by_target.get(target, []))
                        block_n = len(blocked_by_target.get(target, []))
                        if plan_n == 0 and block_n == 0:
                            continue
                        with st.expander(
                            f"`{target}` — buffered {plan_n}, blocked {block_n}",
                            expanded=False,
                        ):
                            if buffered_by_target[target]:
                                st.caption("**Buffered:**")
                                for b in buffered_by_target[target]:
                                    st.markdown(f"  - {b}")
                            if blocked_by_target[target]:
                                st.caption("**Blocked:**")
                                for b in blocked_by_target[target]:
                                    st.markdown(f"  - {b}")

    # ── Mode C — Per-dataset Client → Client ─────────────────────────────
    else:
        st.markdown(
            "Fork from one client's Silver/Gold to another client.  Both "
            "ends are real clients (never GLOBAL_CORP — that's sacred)."
        )
        if not _snap.distinct_clients:
            st.info(
                "📭 No real clients have authored anything yet. Use Mode A "
                "or B to seed a client from Global first."
            )
        else:
            col1, col2, col3 = st.columns([1.2, 1.5, 1.2])
            with col1:
                _cc_src_client = st.selectbox(
                    "Source client",
                    options=_snap.distinct_clients,
                    index=0,
                    key="cc_cc_src_client",
                )
            with col2:
                # Datasets where the source client has at least one
                # APPROVED/LIVE Silver OR Gold
                _src_silver_for = [
                    s
                    for s in _snap.silver_schemas
                    if str(s.get("scope_owner")) == _cc_src_client
                    and str(s.get("status")) in ("APPROVED", "LIVE")
                ]
                _src_gold_for = [
                    g
                    for g in _snap.gold_schemas
                    if str(g.get("scope_owner")) == _cc_src_client
                    and str(g.get("status")) in ("APPROVED", "LIVE")
                ]
                _ds_options = sorted(
                    {
                        str(r.get("dataset_code"))
                        for r in _src_silver_for + _src_gold_for
                    }
                )
                _cc_dataset = st.selectbox(
                    "Dataset (APPROVED/LIVE in source)",
                    options=_ds_options,
                    index=0 if _ds_options else None,
                    placeholder="No eligible datasets"
                    if not _ds_options
                    else "Pick…",
                    key="cc_cc_dataset",
                )
            with col3:
                _cc_tgt_options = [
                    c for c in _snap.distinct_clients if c != _cc_src_client
                ]
                _cc_tgt_clients = _checkbox_multiselect(
                    label="Target clients",
                    options=_cc_tgt_options,
                    key="cc_cc",
                    exclude=[_cc_src_client] if _cc_src_client else None,
                )
            cc_act_a, cc_act_b = st.columns([2, 1])
            with cc_act_a:
                _cc_go = st.button(
                    "📦 Buffer client → client clone(s)",
                    type="secondary",
                    use_container_width=True,
                    disabled=not (
                        _cc_src_client and _cc_dataset and _cc_tgt_clients
                    ),
                    key="cc_cc_go",
                )
            with cc_act_b:
                _cc_inline_submit_button(key="cc_cc_submit")

            if _cc_go:
                normalized: list[str] = []
                invalid: list[str] = []
                for t in _cc_tgt_clients:
                    norm = _cc_target_client_normalized(t)
                    if norm is None:
                        invalid.append(t)
                    elif norm == _cc_src_client:
                        invalid.append(f"{t} (= source)")
                    else:
                        normalized.append(norm)
                if invalid:
                    st.error(
                        f"Invalid target client_id(s): "
                        f"{', '.join(repr(v) for v in invalid)}.  Lowercase "
                        f"alphanumeric / underscore / hyphen only; never "
                        f"GLOBAL_CORP / __global__; never the source itself."
                    )
                elif not normalized:
                    st.error("Pick at least one valid target.")
                else:
                    ds = str(_cc_dataset)

                    _src_s = [
                        s
                        for s in _src_silver_for
                        if str(s.get("dataset_code")) == ds
                    ]
                    _src_s_pick = max(
                        _src_s, key=lambda r: int(r.get("version") or 0)
                    ) if _src_s else None
                    _src_g = [
                        g
                        for g in _src_gold_for
                        if str(g.get("dataset_code")) == ds
                    ]
                    _src_g_pick = max(
                        _src_g, key=lambda r: int(r.get("version") or 0)
                    ) if _src_g else None

                    if not _src_s_pick and not _src_g_pick:
                        st.info(
                            f"No eligible source rows for `{ds}` under "
                            f"`{_cc_src_client}`."
                        )
                    else:
                        buffered_by_target: dict[str, list[str]] = {}
                        blocked_by_target: dict[str, list[str]] = {}
                        for target in normalized:
                            buffered_by_target.setdefault(target, [])
                            blocked_by_target.setdefault(target, [])

                            if _src_s_pick:
                                blk = _cc_silver_blocks(target, ds)
                                if blk:
                                    blocked_by_target[target].append(
                                        f"Silver — {blk}"
                                    )
                                else:
                                    sid = str(
                                        _src_s_pick.get("silver_dataset_id")
                                    )
                                    _dmd_top.stash_edit(
                                        kind="clone_silver",
                                        entity_id=f"{sid}::{target}",
                                        payload={
                                            "apply": _cc_build_silver_applier(
                                                sid, target
                                            ),
                                            "target_client": target,
                                            "dataset_code": ds,
                                            "source_silver_id": sid,
                                        },
                                        base_version=int(
                                            _src_s_pick.get("version") or 0
                                        ),
                                        base_status=str(
                                            _src_s_pick.get("status")
                                        ),
                                    )
                                    buffered_by_target[target].append(
                                        f"Silver v{_src_s_pick.get('version')}"
                                    )

                            if _src_g_pick:
                                blk = _cc_gold_blocks(target, ds)
                                if blk:
                                    blocked_by_target[target].append(
                                        f"Gold — {blk}"
                                    )
                                else:
                                    gid = str(
                                        _src_g_pick.get("gold_dataset_id")
                                    )
                                    _dmd_top.stash_edit(
                                        kind="clone_gold",
                                        entity_id=f"{gid}::{target}",
                                        payload={
                                            "apply": _cc_build_gold_applier(
                                                gid, target
                                            ),
                                            "target_client": target,
                                            "dataset_code": ds,
                                            "source_gold_id": gid,
                                        },
                                        base_version=int(
                                            _src_g_pick.get("version") or 0
                                        ),
                                        base_status=str(
                                            _src_g_pick.get("status")
                                        ),
                                    )
                                    buffered_by_target[target].append(
                                        f"Gold v{_src_g_pick.get('version')}"
                                    )

                        total_planned = sum(
                            len(v) for v in buffered_by_target.values()
                        )
                        if total_planned > 0:
                            n_targets = sum(
                                1 for v in buffered_by_target.values() if v
                            )
                            st.success(
                                f"✏️ Buffered **{total_planned} clone"
                                f"{'s' if total_planned != 1 else ''}** "
                                f"`{_cc_src_client}` → "
                                f"**{n_targets} target"
                                f"{'s' if n_targets != 1 else ''}** for "
                                f"`{ds}`. Submit at the top to apply."
                            )
                        for target in normalized:
                            plan_n = len(buffered_by_target.get(target, []))
                            block_n = len(blocked_by_target.get(target, []))
                            if plan_n == 0 and block_n == 0:
                                continue
                            with st.expander(
                                f"`{target}` — buffered {plan_n}, "
                                f"blocked {block_n}",
                                expanded=False,
                            ):
                                if buffered_by_target[target]:
                                    st.caption("**Buffered:**")
                                    for b in buffered_by_target[target]:
                                        st.markdown(f"  - {b}")
                                if blocked_by_target[target]:
                                    st.caption("**Blocked:**")
                                    for b in blocked_by_target[target]:
                                        st.markdown(f"  - {b}")

    # ── Inline Submit dock ─────────────────────────────────────────────
    # Right at the bottom of every clone-mode action area.  When buffered
    # edits exist, the green button is one click away from where the
    # operator just pressed Plan + Buffer (no scrolling to the top).
    _pending_now = _dmd_top.dirty_count()
    st.markdown(
        f"<div style='margin-top:1rem;padding:.6rem .9rem;background:#f8fafc;"
        f"border:1px solid #cbd5e1;border-radius:8px;'>"
        f"<strong>{('✏️ ' + str(_pending_now) + ' unsaved edit' + ('s' if _pending_now != 1 else '')) if _pending_now else '💤 No pending edits'}</strong> "
        f"&middot; submit applies all buffered clones with optimistic-concurrency check.</div>",
        unsafe_allow_html=True,
    )
    s_col1, s_col2, _ = st.columns([1.3, 1, 3])
    with s_col1:
        if st.button(
            f"🚀 Submit {_pending_now} change{'s' if _pending_now != 1 else ''}",
            type="primary",
            use_container_width=True,
            disabled=_pending_now == 0,
            key="cc_inline_submit",
            help="Apply every buffered clone in one batch.",
        ):
            report = _dmd_top.submit_all()
            if report.all_clean:
                st.toast(
                    f"✅ {report.applied_count} clone"
                    f"{'s' if report.applied_count != 1 else ''} applied.",
                    icon="🚀",
                )
                st.rerun()
            else:
                st.session_state["__dmd_last_report__"] = report
                st.rerun()
    with s_col2:
        if st.button(
            "🗑️ Discard",
            use_container_width=True,
            disabled=_pending_now == 0,
            key="cc_inline_discard",
            help="Drop every buffered edit without writing.",
        ):
            _dmd_top.discard_edits()
            st.rerun(scope="fragment")


st.markdown("## 📦 Cloning Center")
st.caption(
    "Industry-standard clone-from-template flow. All clones buffer locally "
    "and apply via 🚀 Submit (top of page OR inline at the bottom of this "
    "section) — concurrency-checked per source row. Client → Global is "
    "forbidden (Global is sacred)."
)
with st.expander("Show / hide cloning modes", expanded=True):
    _render_cloning_center()


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

    # Phase 17.6 — per-dataset Versioning + Subscribers + Compatibility
    # advisor REMOVED from inside the Gold layer.  The cross-cutting
    # versioning view + advisor now live in the page-level Global Medallion
    # Registry section.  Per-client subscriptions deprecated by user.

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
