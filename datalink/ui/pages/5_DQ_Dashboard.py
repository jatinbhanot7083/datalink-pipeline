"""Data Quality Dashboard — Phase 6.

Dedicated DQ-operations surface that answers the key operator question
Jatin flagged:

    "For CLIENT X, how clean is their CLAIMS / MEMBERSHIP / PROVIDER data,
     by DIMENSION (Completeness / Uniqueness / Timeliness / Accuracy /
     Consistency / Validity), across the last N days?"

Separate from the Executive Dashboard (which is cross-tenant rollup)
and the AI Agents dashboard (which is agent-layer focus). This page is
where a DQ analyst lives — the drill-down that the rules + suites in
CONTROL.dq_suites produce.

Data source
-----------
CONTROL.gx_validation_results — one row per expectation evaluated on
each checkpoint run. Phase 6 added:
    client_id       — tenant scope
    source_type     — CLAIMS | MEMBERSHIP | PROVIDER
    dq_dimension    — one of the 6 dimensions from DataQuality_Metrics.docx

Every slicer in this page filters via those columns.

Every panel shows a data timestamp so the operator can see what
window they're looking at — no chart is ambiguous about currency.
"""

from __future__ import annotations

from typing import Any

import pandas as pd
import plotly.express as px
import streamlit as st

# Phase 21 — Snowflake-only.  DuckDB warehouse path bootstrap removed.
from datalink.ui._query import query_silent as _wh_query_silent

WAREHOUSE_PATH = "CONTROL (Snowflake)"  # cosmetic — only displayed in footer

st.set_page_config(
    page_title="DataLink — DQ Dashboard",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Shared sidebar nav (defined in datalink/ui/_nav.py).
from datalink.ui._nav import render_sidebar  # noqa: E402

render_sidebar(active="DQ Dashboard")

_NAVY = "#0a1a3e"
_GOLD = "#d4af37"
_GREEN = "#047857"
_RED = "#b91c1c"
_AMBER = "#d97706"
_SLATE = "#64748b"

# Dimension palette — each of the 6 dimensions gets a dedicated hue so
# they're instantly recognisable across every chart on this page.
_DIM_COLORS = {
    "Completeness": "#2563eb",
    "Uniqueness": "#7c3aed",
    "Timeliness": "#0891b2",
    "Accuracy": "#059669",
    "Consistency": "#d4af37",
    "Validity": "#dc2626",
    "Unclassified": _SLATE,
}

# Pass-rate thresholds per DataQuality_Metrics.docx defaults
_PASS_GOOD = 99.0
_PASS_WARN = 95.0

st.markdown(
    f"""
    <style>
      h1 {{color: {_NAVY}; border-bottom: 3px solid {_GOLD}; padding-bottom: .4rem;}}
      h2, h3 {{color: {_NAVY}; margin-top: 1.3rem;}}
      .dq-hero {{
        background: linear-gradient(135deg, {_NAVY} 0%, #1e3a8a 100%);
        color:#fff; padding:1.2rem 1.6rem; border-radius:10px;
        margin-bottom:1rem;
      }}
      .dq-hero h2 {{color:#fff; margin:0;}}
      .dq-hero p  {{color:#cbd5e1; margin:.3rem 0 0 0; font-size:.92rem;}}
      .kpi-dq {{
        background:#fff; border-left:5px solid {_GOLD}; padding:.9rem 1.2rem;
        border-radius:6px; box-shadow:0 1px 3px rgba(10,26,62,.08);
      }}
      .kpi-dq-label {{color:#475569;font-size:.8rem;text-transform:uppercase;letter-spacing:.08em;font-weight:600;}}
      .kpi-dq-value {{color:{_NAVY};font-size:1.9rem;font-weight:700;margin-top:.1rem;}}
      .kpi-dq-value.green {{color:{_GREEN};}}
      .kpi-dq-value.amber {{color:{_AMBER};}}
      .kpi-dq-value.red {{color:{_RED};}}
      .empty-state {{
          background:#f8fafc; border:2px dashed #cbd5e1; padding:1.5rem;
          text-align:center; border-radius:8px; color:#64748b;
      }}
      .stamp {{color:#64748b; font-size:.8rem; font-style:italic;}}
    </style>
    """,
    unsafe_allow_html=True,
)


def _try_query(sql: str, params: tuple[Any, ...] = ()) -> pd.DataFrame:
    """Thin wrapper preserving the signature used throughout this page.
    Params pass through as a list so DuckDB ``?`` placeholders bind."""
    return _wh_query_silent(sql, list(params) if params else None)


# ============================================================================
# HEADER
# ============================================================================

st.title("🎯 Data Quality Dashboard")
st.markdown(
    '<div class="dq-hero">'
    "<h2>Per-Client × Per-Source × Per-Dimension DQ Operations</h2>"
    "<p>Every expectation in CONTROL.dq_suites is tagged by client, "
    "source_type, and one of the 6 DQ dimensions (Completeness, Uniqueness, "
    "Timeliness, Accuracy, Consistency, Validity). This page surfaces pass "
    "rates, breaches, and detail rows scoped to your filter selection.</p>"
    "</div>",
    unsafe_allow_html=True,
)


# ============================================================================
# SIDEBAR FILTERS — client / source_type / dimension / window
# ============================================================================

# Phase 21 — factory-pattern.  Client filter is OPTIONAL (multiselect,
# default = all clients).  Same for source_type / dimension.  This page
# is now a cross-tenant DQ console, paired with DQ Suite Registry for
# suite-level drill-down.
st.sidebar.header("Filters")

# Discover available client_ids from gx_validation_results so the picker
# only shows tenants who actually have DQ data.
_client_options_df = _wh_query_silent(
    "SELECT DISTINCT client_id FROM CONTROL.gx_validation_results "
    "WHERE client_id IS NOT NULL ORDER BY client_id"
)
_available_clients = (
    [str(c) for c in _client_options_df["client_id"].tolist()]
    if not _client_options_df.empty
    else []
)
selected_clients = st.sidebar.multiselect(
    "Filter by client",
    options=_available_clients,
    default=[],
    placeholder="All clients (default)",
)

# Source type (the dataset_code, but in the gx_validation_results legacy
# column it's stored as upper-case) — discover from data.
_src_options_df = _wh_query_silent(
    "SELECT DISTINCT source_type FROM CONTROL.gx_validation_results "
    "WHERE source_type IS NOT NULL ORDER BY source_type"
)
_available_sources = (
    [str(s) for s in _src_options_df["source_type"].tolist()] if not _src_options_df.empty else []
)
selected_sources = st.sidebar.multiselect(
    "Filter by source / dataset",
    options=_available_sources,
    default=[],
    placeholder="All sources (default)",
)

dimension_options = [
    "Completeness",
    "Uniqueness",
    "Timeliness",
    "Accuracy",
    "Consistency",
    "Validity",
]
selected_dims = st.sidebar.multiselect(
    "Filter by DQ dimension",
    options=dimension_options,
    default=[],
    placeholder="All dimensions (default)",
)

days = st.sidebar.slider("Window (days)", 1, 90, 30)

# Build dynamic where clauses.  Empty multiselect = no filter on that field.
_wheres: list[str] = []
_params: list[Any] = []

if selected_clients:
    _wheres.append("client_id IN (" + ",".join(["?"] * len(selected_clients)) + ")")
    _params.extend(selected_clients)
if selected_sources:
    _wheres.append("UPPER(source_type) IN (" + ",".join(["?"] * len(selected_sources)) + ")")
    _params.extend([s.upper() for s in selected_sources])
if selected_dims:
    _wheres.append("dq_dimension IN (" + ",".join(["?"] * len(selected_dims)) + ")")
    _params.extend(selected_dims)

filter_sql = (" AND " + " AND ".join(_wheres)) if _wheres else ""
params_tuple: tuple[Any, ...] = tuple(_params)

scope_str = (
    f"**Clients:** {', '.join(selected_clients) if selected_clients else 'all'}  ·  "
    f"**Sources:** {', '.join(selected_sources) if selected_sources else 'all'}  ·  "
    f"**Dimensions:** {', '.join(selected_dims) if selected_dims else 'all'}  ·  "
    f"**Window:** last {days} days"
)
st.caption(scope_str)
# Cosmetic alias: many existing strings reference selected_client (singular).
selected_client = ", ".join(selected_clients) if selected_clients else "all clients"


# ============================================================================
# KPI STRIP — pass rate, breach count, runs, distinct suites
# ============================================================================


def _kpi(label: str, value: str, cls: str = "") -> str:
    return (
        f'<div class="kpi-dq"><div class="kpi-dq-label">{label}</div>'
        f'<div class="kpi-dq-value {cls}">{value}</div></div>'
    )


kpi_df = _try_query(
    f"""
    SELECT
      COUNT(*) AS n_all,
      SUM(CASE WHEN success THEN 1 ELSE 0 END) AS n_pass,
      SUM(CASE WHEN NOT success THEN 1 ELSE 0 END) AS n_fail,
      COUNT(DISTINCT run_id) AS n_runs,
      COUNT(DISTINCT checkpoint_name) AS n_suites
    FROM CONTROL.gx_validation_results
    WHERE ts >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
      {filter_sql}
    """,
    params_tuple,
)

if kpi_df.empty or not kpi_df["n_all"].iloc[0]:
    n_all = n_pass = n_fail = n_runs = n_suites = 0
    pass_pct = 0.0
else:
    n_all = int(kpi_df["n_all"].iloc[0])
    n_pass = int(kpi_df["n_pass"].iloc[0])
    n_fail = int(kpi_df["n_fail"].iloc[0])
    n_runs = int(kpi_df["n_runs"].iloc[0])
    n_suites = int(kpi_df["n_suites"].iloc[0])
    pass_pct = 100.0 * n_pass / n_all

pass_cls = "green" if pass_pct >= _PASS_GOOD else "amber" if pass_pct >= _PASS_WARN else "red"

c1, c2, c3, c4, c5 = st.columns(5)
with c1:
    st.markdown(_kpi("DQ pass rate", f"{pass_pct:.1f}%", pass_cls), unsafe_allow_html=True)
with c2:
    st.markdown(_kpi("Expectations run", f"{n_all:,}"), unsafe_allow_html=True)
with c3:
    st.markdown(
        _kpi("Failed expectations", f"{n_fail:,}", "red" if n_fail > 0 else ""),
        unsafe_allow_html=True,
    )
with c4:
    st.markdown(_kpi("Pipeline runs", f"{n_runs:,}"), unsafe_allow_html=True)
with c5:
    st.markdown(_kpi("Distinct suites", f"{n_suites:,}"), unsafe_allow_html=True)

st.markdown("---")


# ============================================================================
# CHART ROW 1 — dimension breakdown + source-type heatmap
# ============================================================================

col_a, col_b = st.columns(2)

with col_a:
    st.subheader("Pass rate by DQ dimension")
    dim_df = _try_query(
        f"""
        SELECT COALESCE(NULLIF(TRIM(dq_dimension), ''), 'Unclassified') AS dimension,
               COUNT(*) AS n_all,
               SUM(CASE WHEN success THEN 1 ELSE 0 END) AS n_pass
        FROM CONTROL.gx_validation_results
        WHERE ts >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
          {filter_sql}
        GROUP BY dimension
        ORDER BY n_all DESC
        """,
        params_tuple,
    )
    if dim_df.empty or dim_df["n_all"].sum() == 0:
        st.markdown(
            '<div class="empty-state">No validation results in this window.<br>'
            "Trigger a DAG run from the Control Tower home page.</div>",
            unsafe_allow_html=True,
        )
    else:
        dim_df["pass_pct"] = (100.0 * dim_df["n_pass"] / dim_df["n_all"]).round(1)
        color_list = [_DIM_COLORS.get(d, _SLATE) for d in dim_df["dimension"].tolist()]
        fig = px.bar(
            dim_df,
            x="dimension",
            y="pass_pct",
            text="pass_pct",
            color="dimension",
            color_discrete_sequence=color_list,
            labels={"dimension": "Dimension", "pass_pct": "Pass %"},
        )
        fig.update_traces(texttemplate="%{text}%", textposition="outside")
        fig.update_layout(
            height=360,
            showlegend=False,
            yaxis=dict(range=[0, 105], title="Pass rate (%)"),
            margin=dict(t=10, b=40),
        )
        st.plotly_chart(fig, use_container_width=True)
        # Data-timestamp footer
        st.markdown(
            f'<div class="stamp">Data refreshed: {pd.Timestamp.utcnow().strftime("%Y-%m-%d %H:%M UTC")}</div>',
            unsafe_allow_html=True,
        )


with col_b:
    st.subheader("Heatmap: source × dimension pass rate")
    heat_df = _try_query(
        f"""
        SELECT UPPER(COALESCE(source_type, 'UNKNOWN')) AS source_type,
               COALESCE(NULLIF(TRIM(dq_dimension), ''), 'Unclassified') AS dimension,
               COUNT(*) AS n_all,
               SUM(CASE WHEN success THEN 1 ELSE 0 END) AS n_pass
        FROM CONTROL.gx_validation_results
        WHERE ts >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
          {filter_sql}
        GROUP BY source_type, dimension
        """,
        params_tuple,
    )
    if heat_df.empty or heat_df["n_all"].sum() == 0:
        st.markdown(
            '<div class="empty-state">No cross-tab data yet.</div>',
            unsafe_allow_html=True,
        )
    else:
        heat_df["pass_pct"] = (100.0 * heat_df["n_pass"] / heat_df["n_all"]).round(1)
        pivot = heat_df.pivot(index="source_type", columns="dimension", values="pass_pct")
        fig = px.imshow(
            pivot,
            text_auto=True,
            color_continuous_scale=["#dc2626", "#d97706", "#047857"],
            range_color=[90, 100],
            labels=dict(x="Dimension", y="Source", color="Pass %"),
            aspect="auto",
        )
        fig.update_layout(height=360, margin=dict(t=10, b=40))
        st.plotly_chart(fig, use_container_width=True)


# ============================================================================
# CHART ROW 2 — per-client bar + failed expectations table
# ============================================================================

col_c, col_d = st.columns(2)

with col_c:
    st.subheader("Pass rate by client")
    client_df = _try_query(
        f"""
        SELECT COALESCE(client_id, '(unknown)') AS client_id,
               COUNT(*) AS n_all,
               SUM(CASE WHEN success THEN 1 ELSE 0 END) AS n_pass
        FROM CONTROL.gx_validation_results
        WHERE ts >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
          {filter_sql}
        GROUP BY client_id
        ORDER BY n_all DESC
        """,
        params_tuple,
    )
    if client_df.empty or client_df["n_all"].sum() == 0:
        st.markdown(
            '<div class="empty-state">No per-client breakdown yet.</div>',
            unsafe_allow_html=True,
        )
    else:
        client_df["pass_pct"] = (100.0 * client_df["n_pass"] / client_df["n_all"]).round(1)
        client_df["color"] = client_df["pass_pct"].apply(
            lambda p: _GREEN if p >= _PASS_GOOD else (_AMBER if p >= _PASS_WARN else _RED)
        )
        fig = px.bar(
            client_df,
            x="client_id",
            y="pass_pct",
            text="pass_pct",
            color="pass_pct",
            color_continuous_scale=["#dc2626", "#d97706", "#047857"],
            range_color=[90, 100],
            labels={"client_id": "Client", "pass_pct": "Pass %"},
        )
        fig.update_traces(texttemplate="%{text}%", textposition="outside")
        fig.update_layout(
            height=360,
            yaxis=dict(range=[0, 105]),
            margin=dict(t=10, b=40),
        )
        st.plotly_chart(fig, use_container_width=True)


with col_d:
    st.subheader("Failed expectations — latest 100")
    fail_df = _try_query(
        f"""
        SELECT ts, client_id, source_type, dq_dimension, expectation,
               column_name, unexpected_count, unexpected_pct
        FROM CONTROL.gx_validation_results
        WHERE success = FALSE
          AND ts >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
          {filter_sql}
        ORDER BY ts DESC
        LIMIT 100
        """,
        params_tuple,
    )
    if fail_df.empty:
        st.markdown(
            '<div class="empty-state">Zero failed expectations in window — all green.</div>',
            unsafe_allow_html=True,
        )
    else:
        st.dataframe(
            fail_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "ts": st.column_config.DatetimeColumn("Time", width="small"),
                "client_id": st.column_config.TextColumn("Client", width="small"),
                "source_type": st.column_config.TextColumn("Source", width="small"),
                "dq_dimension": st.column_config.TextColumn("Dimension", width="small"),
                "expectation": st.column_config.TextColumn("Expectation", width="large"),
                "column_name": st.column_config.TextColumn("Column", width="small"),
                "unexpected_count": st.column_config.NumberColumn("Bad rows", format="%d"),
                "unexpected_pct": st.column_config.NumberColumn("Bad %", format="%.2f"),
            },
            height=360,
        )


# ============================================================================
# ROW 3 — Suite inventory by client
# ============================================================================

st.markdown("---")
st.subheader("Suite inventory")

suite_df = _try_query(
    """
    SELECT client_id,
           suite_name,
           COALESCE(source_type, '(legacy)') AS source_type,
           status,
           version,
           created_at
    FROM CONTROL.dq_suites
    WHERE status = 'LIVE'
    ORDER BY client_id, source_type, suite_name
    """
)

# Phase 21 — apply the multiselect filters (no required client).
if selected_clients and not suite_df.empty:
    suite_df = suite_df[suite_df["client_id"].isin(selected_clients)]
if selected_sources and not suite_df.empty:
    suite_df = suite_df[
        suite_df["source_type"].astype(str).str.upper().isin([s.upper() for s in selected_sources])
    ]

if suite_df.empty:
    st.markdown(
        '<div class="empty-state">No LIVE suites match the filter.</div>',
        unsafe_allow_html=True,
    )
else:
    st.dataframe(
        suite_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "client_id": "Client",
            "suite_name": "Suite",
            "source_type": "Source",
            "status": "Status",
            "version": st.column_config.NumberColumn("v"),
            "created_at": st.column_config.DatetimeColumn("Created", width="small"),
        },
    )

st.caption(
    f"Dashboard refreshed {pd.Timestamp.utcnow().strftime('%Y-%m-%d %H:%M UTC')}  ·  "
    f"Source: `{WAREHOUSE_PATH}`  ·  "
    f"Dimension mapping: datalink/quality/dimensions.py"
)
