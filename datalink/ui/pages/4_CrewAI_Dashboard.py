"""CrewAI Dashboard — Phase 6.

Dedicated surface for the agentic layer — ProfilerAgent,
ExpectationAuthorAgent, ReviewerAgent (pre-val), RootCauseAgent,
RemediationAgent, ReportingAgent (post-val). Built per Jatin's
AI-Native directive: 'we really want to project our solution as AI
Native. Currently, it doesn't seem to get its dues.'

What it shows (all from CONTROL.agent_reasoning_log):
  - Hero strip: invocations | tokens spent | PHI redactions caught | avg latency
  - Timeline: agent invocations over time, colored by crew
  - Bar: invocations per agent (Pre-Val vs Post-Val)
  - Pie: token spend by agent
  - Row-detail explorer: pick an invocation, see input/output previews

PHI-safety:
  agent_reasoning_log stores *previews* (truncated input + output) that
  PhiRedactionLayer.assert_clean already sanitized at agent->LLM boundary.
  This page never de-references raw PHI — it shows only what the guard
  already let through.

Fail-safe:
  Empty DB -> friendly empty-state cards telling the operator to trigger
  a DAG run. The dashboard never errors on a fresh install.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st

# ============================================================================
# CONFIG + THEME
# ============================================================================

WAREHOUSE_PATH = os.environ.get("DL_CT_WAREHOUSE_PATH", "/opt/datalink/warehouse.duckdb")

st.set_page_config(
    page_title="DataLink — CrewAI Dashboard",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded",
)

_NAVY = "#0a1a3e"
_GOLD = "#d4af37"
_INDIGO = "#4338ca"
_VIOLET = "#7c3aed"
_CYAN = "#0891b2"
_EMERALD = "#059669"
_ROSE = "#e11d48"

# AI-themed palette — cooler, tech-forward (contrasts Executive Dashboard's navy).
_AI_PALETTE = [_INDIGO, _VIOLET, _CYAN, _EMERALD, _ROSE, _GOLD]

st.markdown(
    f"""
    <style>
      h1 {{color: {_INDIGO}; border-bottom: 3px solid {_VIOLET}; padding-bottom: .4rem;}}
      h2, h3 {{color: {_INDIGO}; margin-top: 1.3rem;}}
      .ai-hero {{
        background: linear-gradient(135deg, {_INDIGO} 0%, {_VIOLET} 100%);
        color:#fff; padding:1.4rem 1.8rem; border-radius:12px;
        margin-bottom:1.2rem;
      }}
      .ai-hero h2 {{color:#fff; margin:0; font-weight:700;}}
      .ai-hero p {{color:#e0e7ff; margin:.3rem 0 0 0; font-size:.95rem;}}
      .kpi-ai {{
        background:#fff; border-top:4px solid {_INDIGO}; padding:1rem 1.2rem;
        border-radius:6px; box-shadow:0 2px 8px rgba(67,56,202,.08);
      }}
      .kpi-ai-label {{color:#6366f1;font-size:.8rem;text-transform:uppercase;letter-spacing:.08em;font-weight:600;}}
      .kpi-ai-value {{color:{_INDIGO};font-size:2rem;font-weight:700;margin-top:.15rem;}}
      .empty-state {{
          background:#f5f3ff; border:2px dashed #c4b5fd; padding:2rem;
          text-align:center; border-radius:8px; color:#6366f1;
      }}
      .phi-badge-clean {{background:{_EMERALD};color:#fff;padding:.15rem .45rem;border-radius:4px;font-size:.75rem;}}
      .phi-badge-dirty {{background:{_ROSE};color:#fff;padding:.15rem .45rem;border-radius:4px;font-size:.75rem;}}
    </style>
    """,
    unsafe_allow_html=True,
)


# ============================================================================
# DATA ACCESS
# ============================================================================


@contextmanager
def _conn() -> Iterator[duckdb.DuckDBPyConnection]:
    conn = duckdb.connect(WAREHOUSE_PATH, read_only=True)
    try:
        yield conn
    finally:
        conn.close()


def _try_query(sql: str, params: tuple[Any, ...] = ()) -> pd.DataFrame:
    try:
        with _conn() as c:
            return c.execute(sql, list(params)).fetchdf()
    except Exception:
        return pd.DataFrame()


# ============================================================================
# SIDEBAR FILTERS
# ============================================================================

st.sidebar.header("Filters")
days = st.sidebar.slider("Window (days)", 1, 90, 14)
crew_options = ["<all>", "pre_validation", "post_validation"]
crew_sel = st.sidebar.selectbox("Crew", crew_options, index=0)

crew_filter = ""
crew_params: tuple[Any, ...] = ()
if crew_sel != "<all>":
    crew_filter = "AND crew_name = ?"
    crew_params = (crew_sel,)


# ============================================================================
# HERO + KPIs
# ============================================================================

st.title("🤖 CrewAI Dashboard")
st.markdown(
    '<div class="ai-hero">'
    "<h2>AI-Native Data Quality, Live</h2>"
    "<p>Every agent invocation is audit-logged, token-accounted, and "
    "PHI-guarded before it reaches the LLM. This page projects the "
    "autonomous layer that differentiates DataLink from every rules-only "
    "competitor in the payer-ops market.</p>"
    "</div>",
    unsafe_allow_html=True,
)


def _kpi(label: str, value: str) -> str:
    return (
        f'<div class="kpi-ai"><div class="kpi-ai-label">{label}</div>'
        f'<div class="kpi-ai-value">{value}</div></div>'
    )


kpi_df = _try_query(
    f"""
    SELECT
      COUNT(*) AS invocations,
      COALESCE(SUM(tokens_used), 0) AS tokens,
      COALESCE(SUM(CASE WHEN phi_check = 'REDACTED' THEN 1 ELSE 0 END), 0) AS phi_hits,
      COALESCE(AVG(duration_ms), 0) AS avg_ms
    FROM CONTROL.agent_reasoning_log
    WHERE ts >= CURRENT_TIMESTAMP - INTERVAL '{days} days'
      {crew_filter}
    """,
    crew_params,
)

c1, c2, c3, c4 = st.columns(4)
if kpi_df.empty:
    inv, tok, phi, avgms = 0, 0, 0, 0
else:
    inv = int(kpi_df["invocations"].iloc[0])
    tok = int(kpi_df["tokens"].iloc[0])
    phi = int(kpi_df["phi_hits"].iloc[0])
    avgms = int(kpi_df["avg_ms"].iloc[0])

with c1:
    st.markdown(_kpi("Invocations", f"{inv:,}"), unsafe_allow_html=True)
with c2:
    st.markdown(_kpi("Tokens spent", f"{tok:,}"), unsafe_allow_html=True)
with c3:
    st.markdown(_kpi("PHI redactions", f"{phi:,}"), unsafe_allow_html=True)
with c4:
    st.markdown(_kpi("Avg latency", f"{avgms} ms"), unsafe_allow_html=True)

st.markdown("---")


# ============================================================================
# TIMELINE + CREW BREAKDOWN
# ============================================================================

col_left, col_right = st.columns([2, 1])

with col_left:
    st.subheader("Agent activity timeline")
    tl_df = _try_query(
        f"""
        SELECT ts, agent_name, crew_name, duration_ms
        FROM CONTROL.agent_reasoning_log
        WHERE ts >= CURRENT_TIMESTAMP - INTERVAL '{days} days'
          {crew_filter}
        ORDER BY ts DESC
        LIMIT 500
        """,
        crew_params,
    )
    if tl_df.empty:
        st.markdown(
            '<div class="empty-state">No agent invocations yet.<br>'
            "Trigger a DAG run with GX + agents enabled to see the crew in action.</div>",
            unsafe_allow_html=True,
        )
    else:
        fig = px.scatter(
            tl_df,
            x="ts",
            y="agent_name",
            color="crew_name",
            size="duration_ms",
            size_max=22,
            color_discrete_sequence=_AI_PALETTE,
            labels={"ts": "Time", "agent_name": "Agent", "crew_name": "Crew"},
        )
        fig.update_layout(height=400, margin=dict(t=10, b=40))
        st.plotly_chart(fig, use_container_width=True)

with col_right:
    st.subheader("Token spend by agent")
    tok_df = _try_query(
        f"""
        SELECT agent_name, COALESCE(SUM(tokens_used), 0) AS tokens
        FROM CONTROL.agent_reasoning_log
        WHERE ts >= CURRENT_TIMESTAMP - INTERVAL '{days} days'
          {crew_filter}
          AND tokens_used IS NOT NULL
        GROUP BY agent_name
        ORDER BY tokens DESC
        """,
        crew_params,
    )
    if tok_df.empty or tok_df["tokens"].sum() == 0:
        st.markdown(
            '<div class="empty-state">No token spend tracked yet.<br>'
            "Enable the anthropic LLM adapter to see live cost.</div>",
            unsafe_allow_html=True,
        )
    else:
        fig = px.pie(
            tok_df,
            names="agent_name",
            values="tokens",
            color_discrete_sequence=_AI_PALETTE,
            hole=0.5,
        )
        fig.update_traces(textinfo="label+percent")
        fig.update_layout(height=400, margin=dict(t=10, b=10))
        st.plotly_chart(fig, use_container_width=True)


# ============================================================================
# AGENT SUCCESS-RATE + REASONING EXPLORER
# ============================================================================

col_a, col_b = st.columns([1, 2])

with col_a:
    st.subheader("Invocations per agent")
    count_df = _try_query(
        f"""
        SELECT agent_name, crew_name, COUNT(*) AS n
        FROM CONTROL.agent_reasoning_log
        WHERE ts >= CURRENT_TIMESTAMP - INTERVAL '{days} days'
          {crew_filter}
        GROUP BY agent_name, crew_name
        ORDER BY n DESC
        """,
        crew_params,
    )
    if count_df.empty:
        st.markdown(
            '<div class="empty-state">No data in this window.</div>',
            unsafe_allow_html=True,
        )
    else:
        fig = px.bar(
            count_df,
            x="n",
            y="agent_name",
            color="crew_name",
            orientation="h",
            color_discrete_sequence=_AI_PALETTE,
            labels={"n": "Invocations", "agent_name": "Agent", "crew_name": "Crew"},
        )
        fig.update_layout(height=400, margin=dict(t=10, b=40, l=140))
        st.plotly_chart(fig, use_container_width=True)


with col_b:
    st.subheader("Reasoning log explorer")
    log_df = _try_query(
        f"""
        SELECT ts, agent_name, crew_name, input_preview, output_preview,
               tokens_used, duration_ms, phi_check
        FROM CONTROL.agent_reasoning_log
        WHERE ts >= CURRENT_TIMESTAMP - INTERVAL '{days} days'
          {crew_filter}
        ORDER BY ts DESC
        LIMIT 100
        """,
        crew_params,
    )
    if log_df.empty:
        st.markdown(
            '<div class="empty-state">No reasoning entries.</div>',
            unsafe_allow_html=True,
        )
    else:
        # Compact table with readable columns — PHI-safe previews only.
        display_df = log_df.assign(
            phi=log_df["phi_check"].fillna("UNKNOWN"),
            input=log_df["input_preview"].fillna("").str.slice(0, 120),
            output=log_df["output_preview"].fillna("").str.slice(0, 120),
        )[["ts", "agent_name", "crew_name", "phi", "tokens_used", "duration_ms", "input", "output"]]
        st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "ts": st.column_config.DatetimeColumn("Time", width="small"),
                "agent_name": st.column_config.TextColumn("Agent", width="small"),
                "crew_name": st.column_config.TextColumn("Crew", width="small"),
                "phi": st.column_config.TextColumn("PHI", width="small"),
                "tokens_used": st.column_config.NumberColumn("Tokens", format="%d"),
                "duration_ms": st.column_config.NumberColumn("ms"),
                "input": st.column_config.TextColumn("Input preview", width="large"),
                "output": st.column_config.TextColumn("Output preview", width="large"),
            },
        )


st.caption(
    f"Last refreshed: {pd.Timestamp.utcnow().strftime('%Y-%m-%d %H:%M UTC')}  ·  "
    f"Source: `{WAREHOUSE_PATH}`  ·  "
    f"PhiRedactionLayer.assert_clean guarantees PHI-free previews."
)
