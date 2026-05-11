"""Cost & Token Telemetry — Phase 21 (factory-pattern repoint).

Real-time AI spend visibility.  Sources:

  * CONTROL.agent_reasoning_log — canonical: every AgentBase.run() writes
                                  one row with tokens_used + duration_ms.
                                  Captures EVERY agent invocation across
                                  Pipeline Architect, DQ AI Architect,
                                  Schema Designers, etc.
  * CONTROL.agent_proposals     — legacy table (Phase 16.4) kept for
                                  back-compat; UNIONed with reasoning_log
                                  so historical proposals don't disappear.

Cost is computed on the fly from token counts using a configurable per-1K
rate (default ~$0.003 — Haiku 4.5 blended in/out).  Phase-16 rows that
already carry estimated_cost_usd preserve that value.

Panels:
  * Headline KPIs: today / 7-day / 30-day / total spend
  * Spend by agent (DqSuiteArchitectAgent, PipelineArchitectAgent, …)
  * Spend by crew (dq_ai_architect, pipeline_architect, …)
  * Daily trend chart (last 30 days)
  * Most expensive individual invocations (top 10)
  * Budget alerts (configurable threshold)
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

st.set_page_config(
    page_title="Cost & Tokens",
    page_icon="💰",
    layout="wide",
    initial_sidebar_state="expanded",
)

from datalink.quality.control import CONTROL_SCHEMA  # noqa: E402
from datalink.ui._nav import render_sidebar  # noqa: E402
from datalink.ui._query import warehouse_ctx  # noqa: E402

render_sidebar(active="Cost & Tokens")

st.title("💰 Cost & Token Telemetry")
st.caption(
    "Every LLM call's token count + dollar cost — by agent, by client, by "
    "dataset. CFO-grade transparency for AI spend."
)


# Phase 21 — token → USD conversion rate.  Haiku 4.5 blended (in/out).
# Override via env var for other models.
import os as _os

_TOKEN_RATE = float(_os.environ.get("DATALINK_AI_TOKEN_RATE_USD", "0.000003"))


# Unified UNION subquery — every cost loader reuses this so the same
# logic powers KPIs, breakdowns, and trends.  reasoning_log is the
# canonical source; agent_proposals UNIONed for historical rows.
def _union_sql(token_rate: float = _TOKEN_RATE) -> str:
    return f"""
        SELECT invocation_id AS id,
               agent_name    AS agent,
               crew_name     AS crew,
               ts            AS at,
               COALESCE(tokens_used, 0)             AS tokens,
               COALESCE(tokens_used, 0) * {token_rate} AS cost,
               duration_ms                          AS latency_ms,
               'reasoning_log' AS source
          FROM {CONTROL_SCHEMA}.agent_reasoning_log
         WHERE tokens_used IS NOT NULL
        UNION ALL
        SELECT proposal_id AS id,
               agent_type  AS agent,
               agent_type  AS crew,
               created_at  AS at,
               COALESCE(total_tokens, 0)            AS tokens,
               COALESCE(estimated_cost_usd,
                        COALESCE(total_tokens, 0) * {token_rate}) AS cost,
               latency_ms,
               'agent_proposals' AS source
          FROM {CONTROL_SCHEMA}.agent_proposals
    """


@st.cache_data(ttl=20)  # type: ignore[misc]
def _spend_kpis():
    with warehouse_ctx(readonly=True) as wh:
        try:
            return next(
                iter(
                    wh.query(
                        f"""
                WITH ai AS ({_union_sql()})
                SELECT
                  COALESCE(SUM(CASE WHEN at > DATEADD(day,  -1, CURRENT_TIMESTAMP()) THEN cost END), 0) AS spend_24h,
                  COALESCE(SUM(CASE WHEN at > DATEADD(day,  -7, CURRENT_TIMESTAMP()) THEN cost END), 0) AS spend_7d,
                  COALESCE(SUM(CASE WHEN at > DATEADD(day, -30, CURRENT_TIMESTAMP()) THEN cost END), 0) AS spend_30d,
                  COALESCE(SUM(cost),   0) AS spend_all,
                  COALESCE(SUM(tokens), 0) AS tokens_all,
                  COUNT(*) AS proposals_all
                FROM ai
                """
                    )
                )
            )
        except Exception:
            return {}


@st.cache_data(ttl=20)  # type: ignore[misc]
def _by_agent():
    with warehouse_ctx(readonly=True) as wh:
        try:
            return list(
                wh.query(
                    f"""
                WITH ai AS ({_union_sql()})
                SELECT agent AS agent_type, COUNT(*) AS proposals,
                       SUM(tokens) AS tokens,
                       SUM(cost)   AS cost,
                       AVG(cost)   AS avg_cost,
                       AVG(latency_ms) AS avg_latency_ms
                FROM ai
                GROUP BY agent ORDER BY cost DESC NULLS LAST
                """
                )
            )
        except Exception:
            return []


@st.cache_data(ttl=20)  # type: ignore[misc]
def _by_scope():
    """Phase-21 'scope' = crew_name (which agent flow ran).  Replaces the
    Phase-16 ``scope_key`` (per-client tag) since factory-pattern AI
    agents aren't client-scoped."""
    with warehouse_ctx(readonly=True) as wh:
        try:
            return list(
                wh.query(
                    f"""
                WITH ai AS ({_union_sql()})
                SELECT crew AS scope_key, COUNT(*) AS proposals,
                       SUM(tokens) AS tokens,
                       SUM(cost)   AS cost
                FROM ai
                GROUP BY crew ORDER BY cost DESC NULLS LAST LIMIT 30
                """
                )
            )
        except Exception:
            return []


@st.cache_data(ttl=20)  # type: ignore[misc]
def _daily_trend():
    with warehouse_ctx(readonly=True) as wh:
        try:
            return list(
                wh.query(
                    f"""
                WITH ai AS ({_union_sql()})
                SELECT DATE_TRUNC('day', at) AS day,
                       SUM(tokens) AS tokens,
                       SUM(cost)   AS cost,
                       COUNT(*)    AS proposals
                FROM ai
                WHERE at > DATEADD(day, -30, CURRENT_TIMESTAMP())
                GROUP BY day ORDER BY day
                """
                )
            )
        except Exception:
            return []


@st.cache_data(ttl=20)  # type: ignore[misc]
def _top_expensive():
    with warehouse_ctx(readonly=True) as wh:
        try:
            return list(
                wh.query(
                    f"""
                WITH ai AS ({_union_sql()})
                SELECT id AS proposal_id, agent AS agent_type,
                       crew AS scope_key, NULL AS version,
                       'COMPLETED' AS status,
                       tokens AS total_tokens, cost AS estimated_cost_usd,
                       latency_ms, at AS created_at, source AS created_by
                FROM ai
                ORDER BY cost DESC NULLS LAST LIMIT 10
                """
                )
            )
        except Exception:
            return []


# Headline
kpis = _spend_kpis() or {}
cols = st.columns(4)
for col, label, key in zip(
    cols,
    ["💸 Last 24h", "💸 Last 7d", "💸 Last 30d", "💸 All time"],
    ["spend_24h", "spend_7d", "spend_30d", "spend_all"],
    strict=False,
):
    with col:
        st.metric(label, f"${float(kpis.get(key, 0) or 0):.4f}")

st.caption(
    f"Total: **{int(kpis.get('proposals_all', 0) or 0)}** proposals · "
    f"**{int(kpis.get('tokens_all', 0) or 0):,}** tokens"
)

# Budget alert
st.markdown("---")
budget = st.number_input(
    "Monthly budget ($)",
    min_value=0.0,
    value=50.0,
    step=5.0,
    help="Set a soft ceiling. Page shows ⚠️ when 30-day spend exceeds.",
)
spend_30d = float(kpis.get("spend_30d", 0) or 0)
if budget > 0:
    pct = (spend_30d / budget) * 100
    if pct >= 100:
        st.error(f"🚨 **Over budget**: ${spend_30d:.2f} / ${budget:.2f} ({pct:.0f}%)")
    elif pct >= 80:
        st.warning(f"⚠️ Approaching budget: ${spend_30d:.2f} / ${budget:.2f} ({pct:.0f}%)")
    else:
        st.success(f"✅ Under budget: ${spend_30d:.2f} / ${budget:.2f} ({pct:.0f}%)")

# By agent
st.markdown("---")
st.markdown("### 🤖 Spend by agent type")
agent_rows = _by_agent()
if agent_rows:
    df = pd.DataFrame(
        [
            {
                "Agent": r["agent_type"],
                "Proposals": r["proposals"],
                "Total tokens": int(r["tokens"] or 0),
                "Total $": f"${float(r['cost'] or 0):.4f}",
                "Avg $/proposal": f"${float(r['avg_cost'] or 0):.4f}",
                "Avg latency (s)": f"{float(r['avg_latency_ms'] or 0) / 1000:.2f}",
            }
            for r in agent_rows
        ]
    )
    st.dataframe(df, use_container_width=True, hide_index=True)
else:
    st.caption("No agent activity yet.")

# Daily trend
st.markdown("### 📈 Daily spend trend (last 30 days)")
trend = _daily_trend()
if trend:
    tdf = pd.DataFrame(
        [
            {
                "day": r["day"],
                "cost": float(r["cost"] or 0),
                "tokens": int(r["tokens"] or 0),
                "proposals": r["proposals"],
            }
            for r in trend
        ]
    ).set_index("day")
    st.line_chart(tdf[["cost"]], use_container_width=True)
    st.area_chart(tdf[["tokens"]], use_container_width=True)
else:
    st.caption("Not enough history yet for daily trend.")

# By scope
st.markdown("### 🎯 Spend by scope (top 30)")
scope_rows = _by_scope()
if scope_rows:
    df = pd.DataFrame(
        [
            {
                "Scope": r["scope_key"],
                "Proposals": r["proposals"],
                "Tokens": int(r["tokens"] or 0),
                "Cost": f"${float(r['cost'] or 0):.4f}",
            }
            for r in scope_rows
        ]
    )
    st.dataframe(df, use_container_width=True, hide_index=True, height=400)

# Top expensive
st.markdown("### 🏆 Top 10 most expensive proposals")
top_rows = _top_expensive()
if top_rows:
    df = pd.DataFrame(
        [
            {
                "Cost": f"${float(r['estimated_cost_usd'] or 0):.4f}",
                "Tokens": int(r["total_tokens"] or 0),
                "Agent": r["agent_type"],
                "Scope": r["scope_key"],
                "Version": f"v{r['version']}",
                "Status": r["status"],
                "Latency (s)": f"{int(r['latency_ms'] or 0) / 1000:.2f}",
                "Created": str(r["created_at"])[:19],
                "By": r["created_by"],
            }
            for r in top_rows
        ]
    )
    st.dataframe(df, use_container_width=True, hide_index=True)
