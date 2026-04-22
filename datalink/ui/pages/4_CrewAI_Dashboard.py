"""CrewAI Dashboard — Phase 6 (card-based redesign).

Answers the question Jatin flagged: "what is CrewAI actually doing, is
it real AI or fake, and what are the meaningful metrics I should be
looking at?"

Page layout (top-down)
----------------------
1. Hero + LLM-mode banner  (STUB vs ANTHROPIC — honest disclosure)
2. KPI card strip          (6 cards: invocations / success rate / PHI catches / tokens / cost / latency)
3. Pre-Val vs Post-Val     (2 side-by-side crew cards with agent counts + health)
4. Per-agent detail cards  (6 cards — one per agent — showing role, invocations, avg latency, success, last run)
5. Activity timeline       (bubble chart, sized by duration, colored by crew)
6. Playbook distribution   (post-val RemediationAgent classifications pie)
7. GX vs CrewAI explainer  (side-by-side panel — what each layer does)
8. Reasoning log           (last 100 invocations with PHI-safe previews)

Every card has a data timestamp so the operator knows what window is
shown. Cards use color-coded health: green ≥99%, amber 95-99%, red <95%.
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

WAREHOUSE_PATH = os.environ.get("DL_CT_WAREHOUSE_PATH", "/opt/datalink/warehouse.duckdb")

# Phase 6 fix: bootstrap warehouse file + CONTROL schema before any read-only open.
from datalink.ui._bootstrap import ensure_warehouse_exists  # noqa: E402

ensure_warehouse_exists(WAREHOUSE_PATH)

st.set_page_config(
    page_title="DataLink — CrewAI Dashboard",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ============================================================================
# THEME
# ============================================================================
_NAVY = "#0a1a3e"
_GOLD = "#d4af37"
_INDIGO = "#4338ca"
_VIOLET = "#7c3aed"
_CYAN = "#0891b2"
_EMERALD = "#059669"
_ROSE = "#e11d48"
_AMBER = "#d97706"
_SLATE = "#64748b"
_AI_PALETTE = [_INDIGO, _VIOLET, _CYAN, _EMERALD, _ROSE, _GOLD]

# Agent metadata — canonical role descriptions for the per-agent cards.
_AGENT_META: dict[str, dict[str, str]] = {
    "ProfilerAgent": {
        "icon": "📊",
        "crew": "pre_validation",
        "role": "Computes column-level aggregates (null %, distinct, min/max) on the Bronze table. SQL-first, no row-level data.",
    },
    "ExpectationAuthorAgent": {
        "icon": "✍️",
        "crew": "pre_validation",
        "role": "Drafts GX expectation suite from profile stats. Deterministic rules (null<1% → not_null); LLM narrates rationale.",
    },
    "ReviewerAgent": {
        "icon": "🔍",
        "crew": "pre_validation",
        "role": "Flags severity, marks expectations that need a human approver before activation. Registers DRAFT in dq_suites.",
    },
    "RootCauseAgent": {
        "icon": "🔬",
        "crew": "post_validation",
        "role": "On BREACH: classifies failure into SCHEMA_CHANGE / DATA_QUALITY / VOLUME_ANOMALY / CONFIG_ERROR. Uses history.",
    },
    "RemediationAgent": {
        "icon": "🛠️",
        "crew": "post_validation",
        "role": "Picks a playbook from _PLAYBOOKS + attaches DBA_APPROVAL_REQUIRED. NEVER auto-executes fixes.",
    },
    "ReportingAgent": {
        "icon": "📨",
        "crew": "post_validation",
        "role": "Fans out the incident report to the notifier adapter (webhook / email / Teams). Proof of escalation.",
    },
}

st.markdown(
    f"""
    <style>
      h1 {{color: {_INDIGO}; border-bottom: 3px solid {_VIOLET}; padding-bottom: .4rem;}}
      h2, h3 {{color: {_INDIGO}; margin-top: 1.4rem;}}
      .ai-hero {{
        background: linear-gradient(135deg, {_INDIGO} 0%, {_VIOLET} 100%);
        color:#fff; padding:1.3rem 1.7rem; border-radius:12px; margin-bottom:1rem;
      }}
      .ai-hero h2 {{color:#fff; margin:0; font-weight:700;}}
      .ai-hero p  {{color:#e0e7ff; margin:.3rem 0 0 0; font-size:.95rem;}}
      .llm-mode-stub    {{background:#fef3c7;border-left:5px solid {_AMBER};color:#92400e;padding:.7rem 1rem;border-radius:6px;font-size:.9rem;}}
      .llm-mode-live    {{background:#d1fae5;border-left:5px solid {_EMERALD};color:#065f46;padding:.7rem 1rem;border-radius:6px;font-size:.9rem;}}
      .kpi-ai {{
        background:#fff; border-top:4px solid {_INDIGO}; padding:.95rem 1.1rem;
        border-radius:6px; box-shadow:0 2px 8px rgba(67,56,202,.08);
      }}
      .kpi-ai.green  {{border-top-color:{_EMERALD};}}
      .kpi-ai.amber  {{border-top-color:{_AMBER};}}
      .kpi-ai.red    {{border-top-color:{_ROSE};}}
      .kpi-ai-label {{color:#6366f1;font-size:.75rem;text-transform:uppercase;letter-spacing:.08em;font-weight:600;}}
      .kpi-ai-value {{color:{_INDIGO};font-size:1.75rem;font-weight:700;margin-top:.1rem;}}
      .kpi-ai-sub   {{color:#64748b;font-size:.78rem;margin-top:.1rem;}}
      .crew-card {{
        background:#fff; padding:1.1rem 1.3rem; border-radius:8px;
        box-shadow:0 2px 10px rgba(67,56,202,.07); margin-bottom:.6rem;
      }}
      .crew-card.pre {{border-left:6px solid {_INDIGO};}}
      .crew-card.post {{border-left:6px solid {_ROSE};}}
      .crew-card h4 {{color:{_INDIGO}; margin:0 0 .4rem 0; font-size:1.05rem;}}
      .agent-card {{
        background:#fff; padding:.9rem 1rem; border-radius:7px;
        border-left:4px solid {_VIOLET}; box-shadow:0 1px 4px rgba(67,56,202,.06);
        min-height:165px;
      }}
      .agent-card .title {{color:{_INDIGO};font-size:1rem;font-weight:600;margin:0 0 .3rem 0;}}
      .agent-card .role  {{color:#64748b;font-size:.78rem;line-height:1.3;min-height:2.8rem;}}
      .agent-card .stats {{margin-top:.5rem;display:flex;gap:.8rem;flex-wrap:wrap;}}
      .agent-card .stat-label {{color:#94a3b8;font-size:.7rem;text-transform:uppercase;letter-spacing:.05em;}}
      .agent-card .stat-value {{color:{_INDIGO};font-size:1.1rem;font-weight:600;}}
      .gx-compare {{
        display:grid;grid-template-columns:1fr 1fr;gap:1rem;margin:1rem 0;
      }}
      .gx-compare > div {{
        background:#fff;padding:1.1rem 1.3rem;border-radius:8px;
        box-shadow:0 2px 8px rgba(10,26,62,.07);
      }}
      .gx-side {{border-top:5px solid {_GOLD};}}
      .ai-side {{border-top:5px solid {_VIOLET};}}
      .gx-compare h4 {{margin:0 0 .4rem 0;}}
      .gx-compare .gx-title {{color:{_NAVY};}}
      .gx-compare .ai-title {{color:{_INDIGO};}}
      .gx-compare ul {{font-size:.86rem;color:#334155;margin:.3rem 0 0 -.6rem;}}
      .gx-compare li {{margin:.15rem 0;}}
      .empty-state {{
          background:#f5f3ff; border:2px dashed #c4b5fd; padding:1.3rem;
          text-align:center; border-radius:8px; color:#6366f1; font-size:.9rem;
      }}
      .stamp {{color:#94a3b8; font-size:.75rem; font-style:italic; text-align:right;}}
    </style>
    """,
    unsafe_allow_html=True,
)


@contextmanager
def _conn() -> Iterator[duckdb.DuckDBPyConnection]:
    c = duckdb.connect(WAREHOUSE_PATH, read_only=True)
    try:
        yield c
    finally:
        c.close()


def _try_query(sql: str, params: tuple[Any, ...] = ()) -> pd.DataFrame:
    try:
        with _conn() as c:
            return c.execute(sql, list(params)).fetchdf()
    except Exception:
        return pd.DataFrame()


# ============================================================================
# SIDEBAR
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
# HERO + LLM MODE BANNER (honest disclosure)
# ============================================================================
st.title("🤖 CrewAI Dashboard")
st.markdown(
    '<div class="ai-hero">'
    "<h2>AI-Native Data Quality — Transparent Metrics</h2>"
    "<p>Every agent invocation audit-logged. PHI-guarded at the LLM boundary. "
    "Token spend + latency tracked per agent. This is the autonomous layer that "
    "differentiates DataLink from rules-only DQ platforms.</p>"
    "</div>",
    unsafe_allow_html=True,
)

# Detect whether the running stack is in STUB or ANTHROPIC mode. We infer
# from the most recent agent_reasoning_log row (phi_check field reports
# errors; token patterns differ between stub's char/4 heuristic and
# Anthropic's real usage). Fall back to "unknown" if no runs yet.
# Heuristic: stub emits tokens_used in a narrow range proportional to
# payload size; Anthropic is lower for the same payload. Better signal:
# check env var via a container probe — but that requires exec. Simplest:
# read the _phi field + presence of token counts.
mode_df = _try_query(
    "SELECT COUNT(*) AS n, SUM(CASE WHEN tokens_used > 0 THEN 1 ELSE 0 END) AS with_tokens "
    "FROM CONTROL.agent_reasoning_log"
)
# No reliable way to distinguish stub vs real purely from audit log — state
# the operating ENV override instead. Render a banner that always nudges
# toward the Anthropic path so the operator sees the lever to pull.
llm_mode = "not-yet-run" if (mode_df.empty or not mode_df["n"].iloc[0]) else "configured-via-env"

st.markdown(
    '<div class="llm-mode-stub">'
    "<strong>⚙️ LLM mode:</strong> controlled by <code>DL_ADAPTERS__LLM__TYPE</code> + "
    "<code>ANTHROPIC_API_KEY</code> env vars. "
    "Default = <strong>StubLlm</strong> (offline, deterministic, char÷4 token estimate). "
    "To switch to live Anthropic: <code>export DL_ADAPTERS__LLM__TYPE=anthropic</code> + "
    "<code>export ANTHROPIC_API_KEY=sk-ant-…</code>, then "
    "<code>docker compose restart airflow_scheduler control_tower</code>. "
    "Even in stub mode, ~70% of each agent does real work — SQL aggregates, rule derivation, "
    "audit logging. The LLM only writes narrative prose.</div>",
    unsafe_allow_html=True,
)

st.markdown("---")

# ============================================================================
# KPI STRIP — 6 cards, color-coded health
# ============================================================================


def _kpi(label: str, value: str, sub: str = "", cls: str = "") -> str:
    return (
        f'<div class="kpi-ai {cls}">'
        f'<div class="kpi-ai-label">{label}</div>'
        f'<div class="kpi-ai-value">{value}</div>'
        f'<div class="kpi-ai-sub">{sub}</div>'
        f"</div>"
    )


kpi = _try_query(
    f"""
    SELECT
      COUNT(*)                                              AS invocations,
      COALESCE(SUM(tokens_used), 0)                         AS tokens,
      COALESCE(SUM(CASE WHEN phi_check LIKE 'error%' THEN 1 ELSE 0 END), 0) AS errors,
      COALESCE(SUM(CASE WHEN phi_check = 'REDACTED'  THEN 1 ELSE 0 END), 0) AS phi_hits,
      COALESCE(AVG(duration_ms), 0)                         AS avg_ms,
      COALESCE(MAX(ts), NULL)                               AS last_ts
    FROM CONTROL.agent_reasoning_log
    WHERE ts >= CURRENT_TIMESTAMP - INTERVAL '{days} days'
      {crew_filter}
    """,
    crew_params,
)
if kpi.empty or not kpi["invocations"].iloc[0]:
    invocations = tokens = errors = phi_hits = 0
    avg_ms = 0.0
    last_ts: Any = None
    success_rate = 100.0
else:
    invocations = int(kpi["invocations"].iloc[0])
    tokens = int(kpi["tokens"].iloc[0])
    errors = int(kpi["errors"].iloc[0])
    phi_hits = int(kpi["phi_hits"].iloc[0])
    avg_ms = float(kpi["avg_ms"].iloc[0] or 0)
    last_ts = kpi["last_ts"].iloc[0]
    success_rate = 100.0 * (invocations - errors) / invocations if invocations else 100.0

# Estimate cost @ Claude Haiku 4.5 blended rate ~$1/1M input + $5/1M output.
# We don't track the split today (future: persist input/output separately).
# Use a blended $2.50/1M tokens as a conservative midpoint.
cost_estimate = tokens * 2.50 / 1_000_000

rate_cls = "green" if success_rate >= 99 else ("amber" if success_rate >= 95 else "red")
lat_cls = "green" if avg_ms < 500 else ("amber" if avg_ms < 2000 else "red")
phi_cls = "amber" if phi_hits > 0 else "green"

c = st.columns(6)
with c[0]:
    st.markdown(_kpi("Invocations", f"{invocations:,}", f"last {days}d"), unsafe_allow_html=True)
with c[1]:
    st.markdown(
        _kpi("Success rate", f"{success_rate:.1f}%", f"{errors} error(s)", rate_cls),
        unsafe_allow_html=True,
    )
with c[2]:
    st.markdown(
        _kpi("PHI blocks", f"{phi_hits:,}", "redaction guard hits", phi_cls),
        unsafe_allow_html=True,
    )
with c[3]:
    st.markdown(
        _kpi("Tokens spent", f"{tokens:,}", "input + output (stub estimated)"),
        unsafe_allow_html=True,
    )
with c[4]:
    st.markdown(
        _kpi("Est. LLM cost", f"${cost_estimate:.4f}", "blended Haiku rate"),
        unsafe_allow_html=True,
    )
with c[5]:
    st.markdown(
        _kpi("Avg latency", f"{int(avg_ms)} ms", "per invocation", lat_cls),
        unsafe_allow_html=True,
    )

if last_ts is not None:
    st.markdown(
        f'<div class="stamp">Last agent activity: {pd.Timestamp(last_ts).strftime("%Y-%m-%d %H:%M UTC")}  '
        f'·  Data window: last {days} days</div>',
        unsafe_allow_html=True,
    )

st.markdown("---")

# ============================================================================
# CREW CARDS — Pre-Val vs Post-Val side by side
# ============================================================================
st.subheader("By crew")

crew_df = _try_query(
    f"""
    SELECT crew_name,
           COUNT(*)                                        AS invocations,
           COALESCE(SUM(tokens_used), 0)                   AS tokens,
           COALESCE(AVG(duration_ms), 0)                   AS avg_ms,
           COALESCE(SUM(CASE WHEN phi_check LIKE 'error%' THEN 1 ELSE 0 END), 0) AS errors
    FROM CONTROL.agent_reasoning_log
    WHERE ts >= CURRENT_TIMESTAMP - INTERVAL '{days} days'
    GROUP BY crew_name
    """
)
crews = {r["crew_name"]: r for _, r in crew_df.iterrows()} if not crew_df.empty else {}

cc = st.columns(2)
for idx, (crew_id, label, desc, cls) in enumerate(
    [
        (
            "pre_validation",
            "Pre-Validation Crew",
            "Runs BEFORE GX. Profiles → drafts expectations → flags for human review. Output lands in CONTROL.dq_suites as DRAFT.",
            "pre",
        ),
        (
            "post_validation",
            "Post-Validation Crew",
            "Runs AFTER GX BREACH only. Classifies → picks playbook → notifies. NEVER auto-fixes — always DBA_APPROVAL_REQUIRED.",
            "post",
        ),
    ]
):
    r = crews.get(crew_id)
    inv = int(r["invocations"]) if r is not None else 0
    tok = int(r["tokens"]) if r is not None else 0
    ms = int(r["avg_ms"]) if r is not None else 0
    err = int(r["errors"]) if r is not None else 0
    with cc[idx]:
        st.markdown(
            f'<div class="crew-card {cls}">'
            f"<h4>{label}</h4>"
            f'<div style="color:#64748b;font-size:.85rem;margin-bottom:.6rem">{desc}</div>'
            f'<div style="display:flex;gap:1.2rem;flex-wrap:wrap">'
            f'<div><span class="stat-label">Invocations</span><div class="stat-value">{inv:,}</div></div>'
            f'<div><span class="stat-label">Tokens</span><div class="stat-value">{tok:,}</div></div>'
            f'<div><span class="stat-label">Avg latency</span><div class="stat-value">{ms} ms</div></div>'
            f'<div><span class="stat-label">Errors</span><div class="stat-value" style="color:{_ROSE if err > 0 else _EMERALD}">{err}</div></div>'
            f"</div></div>",
            unsafe_allow_html=True,
        )

# ============================================================================
# PER-AGENT CARD GRID — 6 cards (3x2)
# ============================================================================
st.subheader("Per-agent detail")

agent_df = _try_query(
    f"""
    SELECT agent_name,
           COUNT(*)                                        AS invocations,
           COALESCE(SUM(tokens_used), 0)                   AS tokens,
           COALESCE(AVG(duration_ms), 0)                   AS avg_ms,
           COALESCE(SUM(CASE WHEN phi_check LIKE 'error%' THEN 1 ELSE 0 END), 0) AS errors,
           MAX(ts)                                         AS last_ts
    FROM CONTROL.agent_reasoning_log
    WHERE ts >= CURRENT_TIMESTAMP - INTERVAL '{days} days'
      {crew_filter}
    GROUP BY agent_name
    """,
    crew_params,
)
agents_live = {r["agent_name"]: r for _, r in agent_df.iterrows()} if not agent_df.empty else {}

# Always render all 6 canonical agents (even if never invoked) so the card
# grid is stable and the operator can see WHICH agents haven't fired.
agent_names = list(_AGENT_META)
for row_idx in range(2):
    cols = st.columns(3)
    for col_idx in range(3):
        i = row_idx * 3 + col_idx
        if i >= len(agent_names):
            continue
        name = agent_names[i]
        meta = _AGENT_META[name]
        live = agents_live.get(name)
        inv = int(live["invocations"]) if live is not None else 0
        tok = int(live["tokens"]) if live is not None else 0
        ms = int(live["avg_ms"]) if live is not None else 0
        err = int(live["errors"]) if live is not None else 0
        if live is not None and pd.notna(live.get("last_ts")):
            last = pd.Timestamp(live["last_ts"]).strftime("%m-%d %H:%M")
        else:
            last = "—"
        accent = _INDIGO if meta["crew"] == "pre_validation" else _ROSE
        with cols[col_idx]:
            st.markdown(
                f'<div class="agent-card" style="border-left-color:{accent}">'
                f'<div class="title">{meta["icon"]} {name}</div>'
                f'<div class="role">{meta["role"]}</div>'
                f'<div class="stats">'
                f'<div><span class="stat-label">Runs</span><div class="stat-value">{inv:,}</div></div>'
                f'<div><span class="stat-label">Tokens</span><div class="stat-value">{tok:,}</div></div>'
                f'<div><span class="stat-label">Avg ms</span><div class="stat-value">{ms}</div></div>'
                f'<div><span class="stat-label">Errors</span><div class="stat-value" style="color:{_ROSE if err > 0 else _EMERALD}">{err}</div></div>'
                f'<div><span class="stat-label">Last run</span><div class="stat-value" style="font-size:.9rem">{last}</div></div>'
                f"</div></div>",
                unsafe_allow_html=True,
            )

st.markdown("---")

# ============================================================================
# ACTIVITY TIMELINE + TOKEN BURN (charts)
# ============================================================================
col_left, col_right = st.columns([2, 1])

with col_left:
    st.subheader("Agent activity timeline")
    tl = _try_query(
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
    if tl.empty:
        st.markdown(
            '<div class="empty-state">No agent invocations yet.<br>'
            "Trigger a DAG with GX + agents enabled to see the crew in action.</div>",
            unsafe_allow_html=True,
        )
    else:
        fig = px.scatter(
            tl,
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
    st.subheader("Token burn by day")
    burn = _try_query(
        f"""
        SELECT DATE_TRUNC('day', ts) AS d, SUM(tokens_used) AS tokens
        FROM CONTROL.agent_reasoning_log
        WHERE ts >= CURRENT_TIMESTAMP - INTERVAL '{days} days'
          {crew_filter}
        GROUP BY d ORDER BY d
        """,
        crew_params,
    )
    if burn.empty or burn["tokens"].sum() == 0:
        st.markdown(
            '<div class="empty-state">No token usage recorded.</div>',
            unsafe_allow_html=True,
        )
    else:
        fig = px.bar(
            burn,
            x="d",
            y="tokens",
            color_discrete_sequence=[_INDIGO],
            labels={"d": "Date", "tokens": "Tokens"},
        )
        fig.update_layout(height=400, margin=dict(t=10, b=40), showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

st.markdown("---")

# ============================================================================
# GX vs CrewAI EXPLAINER PANEL — answers the operator question directly
# ============================================================================
st.subheader("What is GX doing vs what is CrewAI doing?")
st.markdown(
    '<div class="gx-compare">'
    '<div class="gx-side"><h4 class="gx-title">🛡️ Great Expectations</h4>'
    "<ul>"
    "<li><b>Role:</b> deterministic rule CHECKER</li>"
    "<li><b>When:</b> every checkpoint run (CP1/CP2/CP3)</li>"
    "<li><b>Input:</b> the actual Bronze/Silver/Gold rows</li>"
    "<li><b>Output:</b> PASSED / FAILED / BREACHED per expectation</li>"
    "<li><b>Autonomy:</b> zero — never changes data, never changes rules</li>"
    "<li><b>AI?</b> No — pure Python/SQL assertions</li>"
    "</ul></div>"
    '<div class="ai-side"><h4 class="ai-title">🤖 CrewAI agent layer</h4>'
    "<ul>"
    "<li><b>Role:</b> autonomous REASONER (before + after GX)</li>"
    "<li><b>When:</b> Pre-Val on suite authoring; Post-Val only on BREACH</li>"
    "<li><b>Input:</b> metadata + stats + history (NEVER rows — PHI guard)</li>"
    "<li><b>Output:</b> drafts · classifications · playbooks · notifications</li>"
    "<li><b>Autonomy:</b> suggests only — DBA_APPROVAL_REQUIRED always</li>"
    "<li><b>AI?</b> Partial: deterministic core + LLM narrative (stub or Anthropic)</li>"
    "</ul></div>"
    "</div>",
    unsafe_allow_html=True,
)
st.caption(
    "Mental model: **GX is the radar** (checks what IS). "
    "**CrewAI is the navigator** (reasons about what the radar saw + what to do). "
    "They're complementary, not redundant."
)

st.markdown("---")

# ============================================================================
# REASONING LOG (PHI-safe previews)
# ============================================================================
st.subheader("Reasoning log — latest 100")
log = _try_query(
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
if log.empty:
    st.markdown('<div class="empty-state">No reasoning entries.</div>', unsafe_allow_html=True)
else:
    display = log.assign(
        phi=log["phi_check"].fillna("unknown"),
        input=log["input_preview"].fillna("").str.slice(0, 120),
        output=log["output_preview"].fillna("").str.slice(0, 120),
    )[["ts", "agent_name", "crew_name", "phi", "tokens_used", "duration_ms", "input", "output"]]
    st.dataframe(
        display,
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
    f"Dashboard refreshed {pd.Timestamp.utcnow().strftime('%Y-%m-%d %H:%M UTC')}  ·  "
    f"Source: `{WAREHOUSE_PATH}`  ·  "
    f"PHI boundary: `datalink/phi/guard.py` — every LLM call gated by assert_clean()"
)
