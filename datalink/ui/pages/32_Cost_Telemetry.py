"""Cost & Token Telemetry — Phase 16.4 (Wave 4 #16).

Real-time AI spend visibility. Pulls from CONTROL.agent_proposals where
every proposal carries token counts + estimated cost (computed at save time
from Anthropic's published pricing).

Panels:
  * Headline KPIs: today / 7-day / 30-day / total spend
  * Spend by agent_type (Pipeline Architect vs Silver Designer vs DQ Proposer)
  * Spend by client (which tenant burns the most LLM tokens)
  * Spend by dataset (which datasets are AI-expensive)
  * Daily trend chart (last 30 days)
  * Most expensive individual proposals (top 10)
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


@st.cache_data(ttl=20)  # type: ignore[misc]
def _spend_kpis():
    with warehouse_ctx(readonly=True) as wh:
        try:
            return next(
                iter(
                    wh.query(
                        f"""
                SELECT
                  COALESCE(SUM(CASE WHEN created_at > DATEADD(day, -1, CURRENT_TIMESTAMP())
                                   THEN estimated_cost_usd END), 0) AS spend_24h,
                  COALESCE(SUM(CASE WHEN created_at > DATEADD(day, -7, CURRENT_TIMESTAMP())
                                   THEN estimated_cost_usd END), 0) AS spend_7d,
                  COALESCE(SUM(CASE WHEN created_at > DATEADD(day, -30, CURRENT_TIMESTAMP())
                                   THEN estimated_cost_usd END), 0) AS spend_30d,
                  COALESCE(SUM(estimated_cost_usd), 0) AS spend_all,
                  COALESCE(SUM(total_tokens), 0) AS tokens_all,
                  COUNT(*) AS proposals_all
                FROM {CONTROL_SCHEMA}.agent_proposals
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
                SELECT agent_type, COUNT(*) AS proposals,
                       SUM(total_tokens) AS tokens,
                       SUM(estimated_cost_usd) AS cost,
                       AVG(estimated_cost_usd) AS avg_cost,
                       AVG(latency_ms) AS avg_latency_ms
                FROM {CONTROL_SCHEMA}.agent_proposals
                GROUP BY agent_type ORDER BY cost DESC NULLS LAST
                """
                )
            )
        except Exception:
            return []


@st.cache_data(ttl=20)  # type: ignore[misc]
def _by_scope():
    with warehouse_ctx(readonly=True) as wh:
        try:
            return list(
                wh.query(
                    f"""
                SELECT scope_key, COUNT(*) AS proposals,
                       SUM(total_tokens) AS tokens,
                       SUM(estimated_cost_usd) AS cost
                FROM {CONTROL_SCHEMA}.agent_proposals
                GROUP BY scope_key ORDER BY cost DESC NULLS LAST LIMIT 30
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
                SELECT DATE_TRUNC('day', created_at) AS day,
                       SUM(total_tokens) AS tokens,
                       SUM(estimated_cost_usd) AS cost,
                       COUNT(*) AS proposals
                FROM {CONTROL_SCHEMA}.agent_proposals
                WHERE created_at > DATEADD(day, -30, CURRENT_TIMESTAMP())
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
                SELECT proposal_id, agent_type, scope_key, version, status,
                       total_tokens, estimated_cost_usd, latency_ms,
                       created_at, created_by
                FROM {CONTROL_SCHEMA}.agent_proposals
                ORDER BY estimated_cost_usd DESC NULLS LAST LIMIT 10
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
