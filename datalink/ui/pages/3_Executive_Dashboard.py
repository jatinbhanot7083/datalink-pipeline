"""Executive Dashboard — Phase 6.

VP-level visual rollup of pipeline health, DQ pass rates, and throughput
across every tenant. Designed for "how is DataLink doing?" at-a-glance
answers, with drill-down filters per client / source / dimension.

What it shows (all driven by CONTROL.* tables — zero hard-coded numbers):
  - KPI strip:  clients | runs | overall DQ pass rate | rows processed today
  - Plotly row of charts:
       1. Bar: rows processed per client (last 30 days)
       2. Pie: DQ pass-rate by dimension (6 dimensions from DataQuality_Metrics.docx)
       3. Bar: checkpoint failure count by suite_name (top 10)
       4. Line: pipeline runs over time (with fail/pass split)
  - Tenant filter in sidebar. Every chart re-queries scoped to selection.

Data source:
  Everything lives in CONTROL schema already. No joins outside /CONTROL
  because that's the cross-tenant surface (per datalink.tenancy contract).

Fail-safe behavior:
  If CONTROL.* is empty (fresh DB), the page renders a friendly empty-state
  card telling the operator to trigger the first DAG run rather than
  erroring out. This is deliberate — a new tenant's first boot shouldn't
  look broken.
"""

from __future__ import annotations

import os
from typing import Any

import pandas as pd
import plotly.express as px
import streamlit as st

# ============================================================================
# CONFIG + THEME
# ============================================================================

WAREHOUSE_PATH = os.environ.get("DL_CT_WAREHOUSE_PATH", "/opt/datalink/warehouse.duckdb")

# Phase 6 fix: ensure the DuckDB file + CONTROL schema exist before we try
# to open it read-only. On a fresh `docker compose up`, warehouse.duckdb
# doesn't exist until the first DAG runs — without this, every Streamlit
# page stack-traces with 'Cannot open database in read-only mode'.
from datalink.ui._bootstrap import ensure_warehouse_exists  # noqa: E402

# Phase 7 Day 2: warehouse access centralised in datalink.ui._query.
from datalink.ui._query import query_silent as _wh_query_silent  # noqa: E402

ensure_warehouse_exists(WAREHOUSE_PATH)

st.set_page_config(
    page_title="DataLink — Executive Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Shared sidebar nav (defined in datalink/ui/_nav.py).
from datalink.ui._nav import render_sidebar  # noqa: E402

render_sidebar(active="Executive Dashboard")

_NAVY = "#0a1a3e"
_GOLD = "#d4af37"
_GREEN = "#047857"
_RED = "#b91c1c"
_AMBER = "#d97706"

# DataLink brand palette for Plotly.
_BRAND = [_NAVY, _GOLD, _GREEN, _RED, _AMBER, "#64748b", "#6366f1"]

st.markdown(
    f"""
    <style>
      h1 {{color: {_NAVY}; border-bottom: 3px solid {_GOLD}; padding-bottom: .4rem;}}
      h2 {{color: {_NAVY}; margin-top: 1.5rem;}}
      .kpi-card {{
          background:#fff; border-left:6px solid {_GOLD}; padding:1rem 1.2rem;
          border-radius:6px; box-shadow:0 1px 2px rgba(0,0,0,.04);
      }}
      .kpi-label {{color:#475569;font-size:.85rem;text-transform:uppercase;letter-spacing:.08em;}}
      .kpi-value {{color:{_NAVY};font-size:2rem;font-weight:700;margin-top:.15rem;}}
      .empty-state {{
          background:#f8fafc; border:2px dashed #cbd5e1; padding:2rem;
          text-align:center; border-radius:8px; color:#64748b;
      }}
    </style>
    """,
    unsafe_allow_html=True,
)


# ============================================================================
# DATA ACCESS — short-lived DuckDB handles (matches 2_DQ_Review.py pattern)
# ============================================================================


def _try_query(sql: str, params: tuple[Any, ...] = ()) -> pd.DataFrame:
    """Run a query; return empty DataFrame on any failure (CONTROL not yet
    bootstrapped, column missing, etc.). Keeps the dashboard "demoable" on
    a fresh install. Delegates to datalink.ui._query so the backend is
    config-driven (DuckDB or Snowflake).

    Params pass through as a list so the SQL's ``?`` positional
    placeholders bind correctly under DuckDB.
    """
    return _wh_query_silent(sql, list(params) if params else None)


# ============================================================================
# SIDEBAR FILTERS
# ============================================================================

# Client comes from the global sidebar selector; halt if not chosen.
from datalink.ui._nav import require_client  # noqa: E402

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
st.sidebar.header("Filters")
days = st.sidebar.slider("Window (days)", 1, 90, 30)

# Client filter is always applied (guaranteed a real tenant).
client_filter_sql = "AND client_id = ?"
client_params: tuple[Any, ...] = (selected_client,)


# ============================================================================
# HEADER
# ============================================================================

st.title("📊 Executive Dashboard")
st.caption(
    f"Cross-tenant pipeline health — window: last **{days} days** · "
    f"client filter: **{selected_client}**"
)


# ============================================================================
# KPI STRIP
# ============================================================================


def _kpi(label: str, value: str) -> str:
    return (
        f'<div class="kpi-card"><div class="kpi-label">{label}</div>'
        f'<div class="kpi-value">{value}</div></div>'
    )


def _render_kpis() -> None:
    clients_count_df = _try_query("SELECT COUNT(DISTINCT client_id) AS n FROM CONTROL.dq_suites")
    total_clients = int(clients_count_df["n"].iloc[0]) if not clients_count_df.empty else 0

    # Phase 6: every KPI honors the sidebar client filter now that
    # client_id is persisted on pipeline_checkpoints + gx_validation_results.
    runs_df = _try_query(
        f"""
        SELECT COUNT(DISTINCT run_id) AS n FROM (
          SELECT run_id FROM CONTROL.pipeline_task_progress
            WHERE completed_at >= CURRENT_TIMESTAMP - INTERVAL '{days} days'
          UNION ALL
          SELECT run_id FROM CONTROL.pipeline_checkpoints
            WHERE completed_at >= CURRENT_TIMESTAMP - INTERVAL '{days} days'
            {client_filter_sql}
        )
        """,
        client_params,
    )
    total_runs = int(runs_df["n"].iloc[0]) if not runs_df.empty else 0

    dq_df = _try_query(
        f"""
        SELECT
          COUNT(*) AS n_all,
          SUM(CASE WHEN success THEN 1 ELSE 0 END) AS n_pass
        FROM CONTROL.gx_validation_results
        WHERE ts >= CURRENT_TIMESTAMP - INTERVAL '{days} days'
          {client_filter_sql}
        """,
        client_params,
    )
    if not dq_df.empty and dq_df["n_all"].iloc[0]:
        dq_pass_pct = 100.0 * float(dq_df["n_pass"].iloc[0]) / float(dq_df["n_all"].iloc[0])
    else:
        dq_pass_pct = 0.0

    rows_df = _try_query(
        f"""
        SELECT COALESCE(SUM(row_count), 0) AS rows_total
        FROM CONTROL.pipeline_checkpoints
        WHERE completed_at >= CURRENT_TIMESTAMP - INTERVAL '{days} days'
          {client_filter_sql}
        """,
        client_params,
    )
    rows_total = int(rows_df["rows_total"].iloc[0]) if not rows_df.empty else 0

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.markdown(_kpi("Clients", f"{total_clients:,}"), unsafe_allow_html=True)
    with col2:
        st.markdown(_kpi("Pipeline runs", f"{total_runs:,}"), unsafe_allow_html=True)
    with col3:
        st.markdown(_kpi("DQ pass rate", f"{dq_pass_pct:.1f}%"), unsafe_allow_html=True)
    with col4:
        st.markdown(_kpi("Rows processed", f"{rows_total:,}"), unsafe_allow_html=True)


_render_kpis()

st.markdown("---")


# ============================================================================
# CHART ROW 1 — rows-per-client + DQ by dimension
# ============================================================================

col_left, col_right = st.columns(2)

with col_left:
    st.subheader("Rows processed per checkpoint")
    # Phase 6: pipeline_checkpoints now has client_id + source_type so the
    # sidebar filter genuinely narrows results. Shows per-checkpoint row
    # counts scoped to the selected client (or <all>).
    rpc_sql = f"""
        SELECT checkpoint_name,
               SUM(row_count) AS rows_total
        FROM CONTROL.pipeline_checkpoints
        WHERE completed_at >= CURRENT_TIMESTAMP - INTERVAL '{days} days'
          {client_filter_sql}
        GROUP BY checkpoint_name
        ORDER BY rows_total DESC
        LIMIT 20
    """
    rpc_df = _try_query(rpc_sql, client_params)
    if rpc_df.empty:
        st.markdown(
            '<div class="empty-state">No rows processed yet in this window.<br>'
            "Trigger a DAG run from the Control Tower home page, then refresh.</div>",
            unsafe_allow_html=True,
        )
    else:
        fig = px.bar(
            rpc_df,
            x="checkpoint_name",
            y="rows_total",
            color_discrete_sequence=[_NAVY],
            labels={"checkpoint_name": "Checkpoint", "rows_total": "Rows"},
        )
        fig.update_layout(height=340, showlegend=False, margin=dict(t=10, b=40))
        st.plotly_chart(fig, use_container_width=True)


with col_right:
    st.subheader("DQ pass rate by dimension")
    # Phase 6: gx_validation_results.dq_dimension is populated at write time
    # by checkpoint._record_results using datalink.quality.dimensions.
    # No more JSON extraction — direct column read.
    dim_sql = f"""
        SELECT
          COALESCE(NULLIF(TRIM(dq_dimension), ''), 'Unclassified') AS dimension,
          COUNT(*) AS n_all,
          SUM(CASE WHEN success THEN 1 ELSE 0 END) AS n_pass
        FROM CONTROL.gx_validation_results
        WHERE ts >= CURRENT_TIMESTAMP - INTERVAL '{days} days'
          {client_filter_sql}
        GROUP BY dimension
        ORDER BY n_all DESC
    """
    dim_df = _try_query(dim_sql, client_params)
    if dim_df.empty or dim_df["n_all"].sum() == 0:
        st.markdown(
            '<div class="empty-state">No GX validations in window.<br>'
            "DQ breakdown populates after the first checkpoint runs.</div>",
            unsafe_allow_html=True,
        )
    else:
        dim_df["pass_pct"] = 100.0 * dim_df["n_pass"] / dim_df["n_all"]
        fig = px.pie(
            dim_df,
            names="dimension",
            values="n_all",
            color_discrete_sequence=_BRAND,
            hole=0.45,
        )
        fig.update_traces(textinfo="label+percent")
        fig.update_layout(height=340, margin=dict(t=10, b=10))
        st.plotly_chart(fig, use_container_width=True)


# ============================================================================
# CHART ROW 2 — top failing suites + runs over time
# ============================================================================

col_a, col_b = st.columns(2)

with col_a:
    st.subheader("Top failing checkpoints")
    fail_sql = f"""
        SELECT checkpoint_name, COUNT(*) AS fails
        FROM CONTROL.pipeline_checkpoints
        WHERE status IN ('BREACH', 'BREACHED', 'FAILED')
          AND completed_at >= CURRENT_TIMESTAMP - INTERVAL '{days} days'
          {client_filter_sql}
        GROUP BY checkpoint_name
        ORDER BY fails DESC
        LIMIT 10
    """
    fail_df = _try_query(fail_sql, client_params)
    if fail_df.empty:
        st.markdown(
            '<div class="empty-state">Zero BREACH events — pipelines are all green.</div>',
            unsafe_allow_html=True,
        )
    else:
        fig = px.bar(
            fail_df,
            x="fails",
            y="checkpoint_name",
            orientation="h",
            color_discrete_sequence=[_RED],
            labels={"fails": "Failures", "checkpoint_name": "Checkpoint"},
        )
        fig.update_layout(height=340, showlegend=False, margin=dict(t=10, b=40, l=140))
        st.plotly_chart(fig, use_container_width=True)

with col_b:
    st.subheader("Pipeline run history")
    # UNION both orchestration paths so Airflow + local_sequential runs
    # both count. pipeline_checkpoints uses status values 'PASSED' /
    # 'BREACH' / 'SKIPPED'; pipeline_task_progress uses 'SUCCESS' /
    # 'FAILED'. Normalize both into pass/fail so the line chart is sane.
    runs_sql = f"""
        WITH all_runs AS (
          SELECT completed_at,
                 CASE WHEN status IN ('PASSED','SUCCESS') THEN 1 ELSE 0 END AS ok,
                 CASE WHEN status IN ('BREACH','BREACHED','FAILED') THEN 1 ELSE 0 END AS bad
            FROM CONTROL.pipeline_checkpoints
           WHERE completed_at >= CURRENT_TIMESTAMP - INTERVAL '{days} days'
             {client_filter_sql}
          UNION ALL
          SELECT completed_at,
                 CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END AS ok,
                 CASE WHEN status = 'FAILED'  THEN 1 ELSE 0 END AS bad
            FROM CONTROL.pipeline_task_progress
           WHERE completed_at >= CURRENT_TIMESTAMP - INTERVAL '{days} days'
        )
        SELECT DATE_TRUNC('day', completed_at) AS d,
               SUM(ok)  AS succeeded,
               SUM(bad) AS failed
          FROM all_runs
         GROUP BY d
         ORDER BY d
    """
    runs_df = _try_query(runs_sql, client_params)
    if runs_df.empty:
        st.markdown(
            '<div class="empty-state">No run history in this window.</div>',
            unsafe_allow_html=True,
        )
    else:
        long_df = runs_df.melt(
            id_vars=["d"], value_vars=["succeeded", "failed"], var_name="kind", value_name="count"
        )
        fig = px.line(
            long_df,
            x="d",
            y="count",
            color="kind",
            color_discrete_map={"succeeded": _GREEN, "failed": _RED},
            labels={"d": "Date", "count": "Tasks", "kind": ""},
            markers=True,
        )
        fig.update_layout(height=340, margin=dict(t=10, b=40))
        st.plotly_chart(fig, use_container_width=True)


# ============================================================================
# FOOTER — per-source dq pass matrix (if data exists)
# ============================================================================

st.markdown("---")
st.subheader("Per-client DQ matrix")

matrix_sql = f"""
    SELECT client_id,
           COUNT(*) AS suite_count,
           SUM(CASE WHEN status = 'LIVE' THEN 1 ELSE 0 END) AS live_count,
           SUM(CASE WHEN status = 'PENDING_REVIEW' THEN 1 ELSE 0 END) AS pending_count
    FROM CONTROL.dq_suites
    WHERE 1=1 {client_filter_sql}
    GROUP BY client_id
    ORDER BY client_id
"""
matrix_df = _try_query(matrix_sql, client_params)
if matrix_df.empty:
    st.markdown(
        '<div class="empty-state">No DQ suites registered yet.<br>'
        "Seed baselines run automatically on first DAG run.</div>",
        unsafe_allow_html=True,
    )
else:
    st.dataframe(matrix_df, use_container_width=True, hide_index=True)

st.caption(
    f"Last refreshed: {pd.Timestamp.utcnow().strftime('%Y-%m-%d %H:%M UTC')}  ·  "
    f"Source: `{WAREHOUSE_PATH}`"
)
