"""AI Agents dashboard — Phase 6 (narrative-feed redesign).

Phase 8 rename: was "CrewAI Dashboard". The CrewAI PyPI package was a
declared-but-unused dependency; the agent layer is a custom AgentBase /
CrewBase implementation. The dashboard surfaces those agents — naming
them "AI Agents" is the honest label.


Every agent invocation translated into a HUMAN-READABLE CARD that says:
what was asked, what the agent concluded, what action (if any) was
recommended. No more raw JSON dumps.

Page layout (top-down)
----------------------
1. Hero + LLM-mode disclosure (Claude Max vs API honesty)
2. How It Works                — 2-flow explainer (before vs after DAG runs)
3. 6 KPI cards                  — plain-English labels
4. Pre-Checks vs Incident-Response crew summaries
5. Latest Remediation Playbook  — BIG card when post-val fired
6. Recent Activity Feed         — one narrative card per agent run
7. Per-agent summary cards
8. Token burn + timeline charts
9. GX vs CrewAI comparison
10. Raw reasoning log (expandable)
"""

from __future__ import annotations

import ast
import json
import os
from typing import Any

import pandas as pd
import plotly.express as px
import streamlit as st

# Phase 21 — Snowflake-only.  DuckDB warehouse path bootstrap removed;
# all queries go through warehouse_ctx / query_silent against CONTROL.*
from datalink.ui._query import query_silent as _wh_query_silent

WAREHOUSE_PATH = "CONTROL (Snowflake)"  # cosmetic — only displayed in footer

st.set_page_config(
    page_title="DataLink — AI Agents",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Shared sidebar nav (defined in datalink/ui/_nav.py).
from datalink.ui._nav import render_sidebar  # noqa: E402

render_sidebar(active="AI Agents")

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
_AI_PALETTE = [_INDIGO, _VIOLET, _CYAN, _EMERALD, _ROSE, _GOLD]

# ----------------------------------------------------------------------------
# Agent catalogue: icon, crew, one-line plain-English role.
# ----------------------------------------------------------------------------
_AGENT_META: dict[str, dict[str, str]] = {
    "ProfilerAgent": {
        "icon": "📊",
        "crew": "pre_validation",
        "label": "Data Profiler",
        "role": (
            "Reads the Bronze table and computes column-level statistics "
            "(row count, null %, distinct values per column). "
            "Never touches individual rows — only aggregates."
        ),
    },
    "ExpectationAuthorAgent": {
        "icon": "✍️",
        "crew": "pre_validation",
        "label": "Rule Author",
        "role": (
            "Reads the Profiler's stats and drafts a set of data-quality rules "
            "(e.g. 'claim_id must never be null'). Deterministic: always "
            "proposes the same rules for the same stats. LLM writes the rationale."
        ),
    },
    "ReviewerAgent": {
        "icon": "🔍",
        "crew": "pre_validation",
        "label": "Rule Reviewer",
        "role": (
            "Inspects the draft expectations and flags any that require human "
            "approval (e.g. value-set rules where the agent cannot "
            "independently verify the complete set). Persists the draft to "
            "CONTROL.dq_suites."
        ),
    },
    "RootCauseAgent": {
        "icon": "🔬",
        "crew": "post_validation",
        "label": "Root-Cause Analyst",
        "role": (
            "Fires ONLY when a GX checkpoint fails (BREACH). Classifies the "
            "failure into SCHEMA_CHANGE / DATA_QUALITY / VOLUME_ANOMALY / "
            "CONFIG_ERROR by consulting history. Says whether it's a first-time "
            "issue or a recurring one."
        ),
    },
    "RemediationAgent": {
        "icon": "🛠️",
        "crew": "post_validation",
        "label": "Remediation Planner",
        "role": (
            "Takes the classification and produces a step-by-step playbook "
            "(ABORT, PARTIAL_LOAD, FIX_AND_RESUME, INVESTIGATE). "
            "ALWAYS marks output as DBA_APPROVAL_REQUIRED — never auto-executes."
        ),
    },
    "ReportingAgent": {
        "icon": "📨",
        "crew": "post_validation",
        "label": "Incident Reporter",
        "role": (
            "Sends the incident summary to the configured notifier "
            "(webhook / email / Teams / Slack) with severity + subject line. "
            "Proof-of-escalation for audit."
        ),
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
      .disclosure-box {{
          background:#fffbeb; border-left:5px solid {_AMBER};
          color:#78350f; padding:.8rem 1.1rem; border-radius:6px; font-size:.88rem;
      }}
      .disclosure-box b {{color:#7c2d12;}}
      .kpi-ai {{
        background:#fff; border-top:4px solid {_INDIGO}; padding:.95rem 1.1rem;
        border-radius:6px; box-shadow:0 2px 8px rgba(67,56,202,.08);
      }}
      .kpi-ai.green {{border-top-color:{_EMERALD};}}
      .kpi-ai.amber {{border-top-color:{_AMBER};}}
      .kpi-ai.red   {{border-top-color:{_ROSE};}}
      .kpi-ai-label {{color:#6366f1;font-size:.75rem;text-transform:uppercase;letter-spacing:.08em;font-weight:600;}}
      .kpi-ai-value {{color:{_INDIGO};font-size:1.75rem;font-weight:700;margin-top:.1rem;}}
      .kpi-ai-sub   {{color:#64748b;font-size:.78rem;margin-top:.1rem;}}
      .crew-card {{
        background:#fff; padding:1.1rem 1.3rem; border-radius:8px;
        box-shadow:0 2px 10px rgba(67,56,202,.07); margin-bottom:.6rem;
      }}
      .crew-card.pre  {{border-left:6px solid {_INDIGO};}}
      .crew-card.post {{border-left:6px solid {_ROSE};}}
      .crew-card h4 {{color:{_INDIGO}; margin:0 0 .4rem 0; font-size:1.05rem;}}
      .agent-card {{
        background:#fff; padding:.9rem 1rem; border-radius:7px;
        border-left:4px solid {_VIOLET}; box-shadow:0 1px 4px rgba(67,56,202,.06);
        min-height:195px;
      }}
      .agent-card .title {{color:{_INDIGO};font-size:1rem;font-weight:600;margin:0 0 .3rem 0;}}
      .agent-card .role  {{color:#64748b;font-size:.78rem;line-height:1.35;min-height:3.4rem;}}
      .agent-card .stats {{margin-top:.5rem;display:flex;gap:.8rem;flex-wrap:wrap;}}
      .agent-card .stat-label {{color:#94a3b8;font-size:.7rem;text-transform:uppercase;letter-spacing:.05em;}}
      .agent-card .stat-value {{color:{_INDIGO};font-size:1.1rem;font-weight:600;}}

      /* Narrative feed — one card per agent invocation */
      .feed-card {{
        background:#fff; padding:.9rem 1.1rem; border-radius:8px;
        box-shadow:0 1px 4px rgba(67,56,202,.06); margin-bottom:.6rem;
        border-left:4px solid {_VIOLET};
      }}
      .feed-card.pre  {{border-left-color:{_INDIGO};}}
      .feed-card.post {{border-left-color:{_ROSE};}}
      .feed-head {{display:flex;align-items:center;gap:.6rem;font-size:.95rem;font-weight:600;color:{_INDIGO};}}
      .feed-head .ts {{color:#94a3b8;font-size:.78rem;font-weight:400;margin-left:auto;}}
      .feed-task {{color:#334155;font-size:.88rem;margin-top:.35rem;}}
      .feed-task b {{color:{_NAVY};}}
      .feed-result {{color:#0f172a;font-size:.88rem;margin-top:.3rem;padding:.5rem .7rem;
        background:#f8fafc;border-radius:4px;border-left:3px solid {_EMERALD};}}
      .feed-result.err {{border-left-color:{_ROSE}; background:#fef2f2;color:#7f1d1d;}}
      .feed-foot {{color:#94a3b8;font-size:.75rem;margin-top:.35rem;}}

      /* BIG playbook alert */
      .playbook-alert {{
        background:linear-gradient(135deg,#fef2f2 0%,#fee2e2 100%);
        border:2px solid {_ROSE}; border-radius:10px;
        padding:1.2rem 1.5rem; margin:.6rem 0 1rem 0;
      }}
      .playbook-alert h3 {{color:{_ROSE};margin:0 0 .4rem 0;}}
      .playbook-alert .pb-meta {{display:flex;gap:1.5rem;font-size:.85rem;color:#7f1d1d;margin:.4rem 0;}}
      .playbook-alert .pb-meta b {{color:#450a0a;}}
      .playbook-alert ol {{margin:.4rem 0;color:#450a0a;}}
      .playbook-alert li {{margin:.15rem 0;}}
      .playbook-alert .dba-badge {{
        display:inline-block;background:{_ROSE};color:#fff;font-weight:700;
        padding:.3rem .7rem;border-radius:4px;font-size:.8rem;letter-spacing:.05em;margin-top:.4rem;
      }}

      .gx-compare {{display:grid;grid-template-columns:1fr 1fr;gap:1rem;margin:1rem 0;}}
      .gx-compare > div {{background:#fff;padding:1.1rem 1.3rem;border-radius:8px;box-shadow:0 2px 8px rgba(10,26,62,.07);}}
      .gx-side {{border-top:5px solid {_GOLD};}}
      .ai-side {{border-top:5px solid {_VIOLET};}}
      .gx-compare h4 {{margin:0 0 .4rem 0;}}
      .gx-compare .gx-title {{color:{_NAVY};}}
      .gx-compare .ai-title {{color:{_INDIGO};}}
      .gx-compare ul {{font-size:.86rem;color:#334155;margin:.3rem 0 0 -.6rem;}}
      .empty-state {{background:#f5f3ff; border:2px dashed #c4b5fd; padding:1.3rem;text-align:center; border-radius:8px; color:#6366f1; font-size:.9rem;}}
      .stamp {{color:#94a3b8; font-size:.75rem; font-style:italic;}}
    </style>
    """,
    unsafe_allow_html=True,
)


def _try_query(sql: str, params: tuple[Any, ...] = ()) -> pd.DataFrame:
    """Backwards-compatible wrapper. Params pass through as a positional
    list so DuckDB's ``?`` placeholders bind correctly."""
    return _wh_query_silent(sql, list(params) if params else None)


def _parse_preview(raw: str | None) -> dict[str, Any]:
    """Parse the audit-log output_preview JSON string into a dict.

    The preview is already PHI-safe metadata. Some values are themselves
    stringified JSON/lists (legacy truncation) — try to parse those too.
    """
    if not raw:
        return {}
    try:
        obj = json.loads(raw)
    except Exception:
        return {}
    if not isinstance(obj, dict):
        return {}
    # Some fields (recommended_actions, classified_failures) were
    # stringified-list dumps. Attempt ast.literal_eval to recover.
    result: dict[str, Any] = {}
    for k, v in obj.items():
        if isinstance(v, str) and v.startswith(("[", "{")):
            try:
                result[k] = ast.literal_eval(v)
                continue
            except Exception:
                pass
        result[k] = v
    return result


def _fmt_time(ts: Any) -> str:
    if ts is None or pd.isna(ts):
        return "—"
    try:
        return str(pd.Timestamp(ts).strftime("%Y-%m-%d %H:%M UTC"))
    except Exception:
        return str(ts)


def _relative_time(ts: Any) -> str:
    if ts is None or pd.isna(ts):
        return "—"
    try:
        delta = pd.Timestamp.utcnow().tz_localize(None) - pd.Timestamp(ts).tz_localize(None)
        secs = int(delta.total_seconds())
    except Exception:
        return _fmt_time(ts)
    if secs < 60:
        return f"{secs}s ago"
    if secs < 3600:
        return f"{secs // 60} min ago"
    if secs < 86400:
        return f"{secs // 3600}h ago"
    return f"{secs // 86400}d ago"


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
# HERO + DISCLOSURE
# ============================================================================
st.title("🤖 AI Agents")
st.markdown(
    '<div class="ai-hero">'
    "<h2>Agentic Data Quality Operations</h2>"
    "<p>Six autonomous agents operate across two crews. The "
    "<b>Pre-Validation Crew</b> authors quality expectations before "
    "ingestion; the <b>Post-Validation Crew</b> performs root-cause "
    "analysis and recommends remediation when an expectation fails. "
    "Every agent decision is recorded to the audit log, PHI is redacted "
    "at the LLM boundary, and all structural changes require human approval.</p>"
    "</div>",
    unsafe_allow_html=True,
)

# Phase 6 Commit 2: dynamic banner. Reads the actual env-var state of the
# Streamlit container. GREEN if live Anthropic is wired. AMBER if stub.
_llm_type = os.environ.get("DL_ADAPTERS__LLM__TYPE", "stub").lower()
_llm_model = os.environ.get("DL_ADAPTERS__LLM__MODEL", "(default)")
_llm_key_set = bool(os.environ.get("ANTHROPIC_API_KEY"))
_thinking = os.environ.get("DL_FEATURES__AGENTS__THINKING_MODE", "off")

if _llm_type == "anthropic" and _llm_key_set:
    _banner_cls = "llm-mode-live"
    _banner_text = (
        f"<b>🟢 LIVE: Claude API active</b> &nbsp;&middot;&nbsp; "
        f"model: <code>{_llm_model}</code> &nbsp;&middot;&nbsp; "
        f"thinking: <b>{_thinking}</b> &nbsp;&middot;&nbsp; "
        f"key: <code>…{(os.environ.get('ANTHROPIC_API_KEY', '') or '')[-4:]}</code> "
        f"(last 4 chars). Every agent call below is a real Anthropic API request. "
        f"Tokens shown are ACTUAL usage — billed to your account. "
        f"Flip <code>DL_ADAPTERS__LLM__TYPE</code> to <code>stub</code> + restart "
        f"control_tower to go offline (zero-cost stub responses)."
    )
else:
    _banner_cls = "disclosure-box"
    _banner_text = (
        f"<b>⚙️ LLM mode: STUB (deterministic, offline, zero API cost).</b> "
        f"DL_ADAPTERS__LLM__TYPE=<code>{_llm_type}</code>, "
        f"ANTHROPIC_API_KEY set: <b>{_llm_key_set}</b>. "
        f"To activate real Claude: put ANTHROPIC_API_KEY in <code>.env</code> + set "
        f"<code>DL_ADAPTERS__LLM__TYPE=anthropic</code> + "
        f"<code>docker compose restart airflow_scheduler control_tower</code>. "
        f"Note: Claude Max subscription is a separate product (consumer chat UI) — "
        f"you need an <b>API</b> key from console.anthropic.com for programmatic use."
    )

st.markdown(
    f'<div class="{_banner_cls}">{_banner_text}</div>',
    unsafe_allow_html=True,
)

# ============================================================================
# Phase 8 — AI Memory tile (RAG state, no agent activity needed to populate)
# Read straight from agent_memory schema in Postgres. Failures degrade
# gracefully so a missing pgvector setup never crashes the dashboard.
# ============================================================================
mem_col1, mem_col2, mem_col3, mem_col4 = st.columns(4)
try:
    from datalink.adapters.embeddings.router import get_embedder
    from datalink.memory import AgentMemoryStore

    _mem = AgentMemoryStore(embedder=get_embedder())
    _mem_stats = _mem.stats()
    mem_col1.metric(
        "🧠 Suite embeddings",
        _mem_stats.get("suite_embeddings", 0),
        help="Tier A — every LIVE/ARCHIVED suite is embedded so the DQ AI Architect "
        "can ground new proposals on similar past suites for the same client.",
    )
    mem_col2.metric(
        "🧠 Reasoning embeddings",
        _mem_stats.get("reasoning_embeddings", 0),
        help="Tier B — past agent invocations (breach classifications, "
        "remediations) embedded so the Post-Val crew can recall similar "
        "past breaches when diagnosing a new failure.",
    )
    mem_col3.metric(
        "Embedding model",
        _mem_stats.get("model", "—"),
        help="`stub-1024` is the offline deterministic fallback. Set "
        "DL_ADAPTERS__EMBEDDINGS__TYPE=voyage + VOYAGE_API_KEY for "
        "production-quality retrieval.",
    )
    _latest = _mem_stats.get("latest_embed") or "—"
    mem_col4.metric(
        "Latest embed",
        _latest[:16].replace("T", " ") if _latest != "—" else "—",
        help="Most recent embedding write. Auto-updates on every "
        "SuiteRegistry.activate() (Phase 8 hook).",
    )
except Exception as _mem_err:
    mem_col1.warning(
        f"Agent memory store unreachable ({type(_mem_err).__name__}). "
        f"Run `python -m scripts.backfill_embeddings` once to provision "
        f"the pgvector tables, or check `docker exec datalink-postgres "
        f'psql -U datalink -d datalink_um -c "\\dx vector"`.'
    )

# ============================================================================
# HOW IT WORKS — expandable explainer
# ============================================================================
with st.expander("📖 Agent architecture and execution flow", expanded=False):
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("#### Flow A — Pre-Validation Crew (pre-ingestion)")
        st.markdown(
            """
Invoked when a new client is onboarded or when schema drift invalidates a
cached suite. The three agents execute sequentially:

**1. Data Profiler**
- Reads `BRONZE_{CLIENT}.RAW_CLAIMS / RAW_MEMBERSHIP / RAW_PROVIDER`
- Computes aggregate statistics: row count, null percentage per column,
  distinct-value count per column
- No row-level data is transmitted beyond the warehouse boundary

**2. Expectation Author**
- Consumes the profile and emits structured expectations, for example:
  - `claim_id` exhibits 0% nulls → `expect_column_values_to_not_be_null`
  - `claim_status` has 4 distinct values → `expect_column_values_to_be_in_set`
    (flagged for human-supplied value set)
- Persists a DRAFT suite to `CONTROL.dq_suites`

**3. Expectation Reviewer**
- Reviews the draft and flags expectations that require human approval
- Flagged items surface in the DQ Review page with status `PENDING_REVIEW`

**Outcome:** A new client is onboarded with a complete set of draft suites
ready for review or auto-approval, with zero manual authoring required.
            """
        )
    with col2:
        st.markdown("#### Flow B — Post-Validation Crew (incident response)")
        st.markdown(
            """
Invoked automatically when a Great Expectations checkpoint reports a
breach (failure percentage at or above the configured threshold):

**4. Root-Cause Analyst**
- Retrieves failing expectations from `gx_validation_results`
- Classifies each failure into one of: `SCHEMA_CHANGE`, `DATA_QUALITY`,
  `VOLUME_ANOMALY`, `CONFIG_ERROR`
- Consults historical runs to determine whether the issue is recurring
  or first observed

**5. Remediation Planner**
- Maps the classification to a predefined playbook:
  - `PB-SCHEMA-CHANGE` → ABORT_AND_INVESTIGATE
  - `PB-DATA-QUALITY` → PARTIAL_LOAD (quarantine failing records)
  - `PB-VOLUME-ANOMALY` → FIX_AND_RESUME
  - `PB-CONFIG-ERROR` → FIX_AND_RESUME
- Every recommendation is tagged `DBA_APPROVAL_REQUIRED`; the agent does
  not execute any remediation action directly

**6. Incident Reporter**
- Issues a severity-tagged notification to the configured channel
  (webhook, email, Teams). The local demo environment captures these in
  the webhook inbox at `http://localhost:9000`.

**Outcome:** A breach is converted into a fully-structured incident
record — classification, recommended playbook, action items, MTTR
estimate — within seconds of detection.
            """
        )

# ============================================================================
# KPI STRIP
# ============================================================================
kpi = _try_query(
    f"""
    SELECT
      COUNT(*)                                              AS invocations,
      COALESCE(SUM(tokens_used), 0)                         AS tokens,
      COALESCE(SUM(CASE WHEN phi_check LIKE 'error%' THEN 1 ELSE 0 END), 0) AS errors,
      COALESCE(SUM(CASE WHEN phi_check = 'REDACTED' THEN 1 ELSE 0 END), 0) AS phi_hits,
      COALESCE(AVG(duration_ms), 0)                         AS avg_ms,
      MAX(ts)                                               AS last_ts
    FROM CONTROL.agent_reasoning_log
    WHERE ts >= DATEADD(day, -{days}, CURRENT_TIMESTAMP()) {crew_filter}
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

cost_estimate = tokens * 2.50 / 1_000_000
rate_cls = "green" if success_rate >= 99 else ("amber" if success_rate >= 95 else "red")
lat_cls = "green" if avg_ms < 500 else ("amber" if avg_ms < 2000 else "red")
phi_cls = "amber" if phi_hits > 0 else "green"


def _kpi(label: str, value: str, sub: str = "", cls: str = "") -> str:
    return (
        f'<div class="kpi-ai {cls}"><div class="kpi-ai-label">{label}</div>'
        f'<div class="kpi-ai-value">{value}</div>'
        f'<div class="kpi-ai-sub">{sub}</div></div>'
    )


st.markdown("### Fleet health — last " + str(days) + " days")
c = st.columns(6)
with c[0]:
    st.markdown(
        _kpi("Agent runs", f"{invocations:,}", "each task = one agent doing one thing"),
        unsafe_allow_html=True,
    )
with c[1]:
    st.markdown(
        _kpi("Success rate", f"{success_rate:.1f}%", f"{errors} failed agent run(s)", rate_cls),
        unsafe_allow_html=True,
    )
with c[2]:
    st.markdown(
        _kpi("PHI blocks", f"{phi_hits:,}", "times guard stopped unsafe payload", phi_cls),
        unsafe_allow_html=True,
    )
with c[3]:
    st.markdown(
        _kpi("LLM tokens used", f"{tokens:,}", "StubLlm = char÷4 estimate"),
        unsafe_allow_html=True,
    )
with c[4]:
    st.markdown(
        _kpi("Est. LLM cost", f"${cost_estimate:.4f}", "if Anthropic were active"),
        unsafe_allow_html=True,
    )
with c[5]:
    st.markdown(
        _kpi("Avg time per agent", f"{int(avg_ms)} ms", "wall-clock per run", lat_cls),
        unsafe_allow_html=True,
    )

if last_ts is not None:
    st.markdown(
        f'<div class="stamp">Last agent activity: {_fmt_time(last_ts)}  '
        f"({_relative_time(last_ts)})</div>",
        unsafe_allow_html=True,
    )

# ============================================================================
# CREW SUMMARY CARDS
# ============================================================================
st.subheader("By crew — Pre-Checks vs Incident Response")

crew_df = _try_query(
    f"""
    SELECT crew_name, COUNT(*) AS invocations,
           COALESCE(SUM(tokens_used), 0) AS tokens,
           COALESCE(AVG(duration_ms), 0) AS avg_ms,
           COALESCE(SUM(CASE WHEN phi_check LIKE 'error%' THEN 1 ELSE 0 END), 0) AS errors
    FROM CONTROL.agent_reasoning_log
    WHERE ts >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
    GROUP BY crew_name
    """
)
crews = {r["crew_name"]: r for _, r in crew_df.iterrows()} if not crew_df.empty else {}

cc = st.columns(2)
for idx, (crew_id, label, desc, cls) in enumerate(
    [
        (
            "pre_validation",
            "🧭 Pre-Checks Crew (3 agents)",
            "Runs BEFORE your data is validated. Profiles the Bronze tables, drafts quality "
            "rules, flags which need human sign-off. Output: 1 new draft suite per client.",
            "pre",
        ),
        (
            "post_validation",
            "🚨 Incident Response Crew (3 agents)",
            "Runs ONLY when a GX check fails (BREACH). Classifies the failure, picks a "
            "playbook, sends an alert. Output: 1 incident report + recommended actions.",
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
        err_color = _ROSE if err > 0 else _EMERALD
        st.markdown(
            f'<div class="crew-card {cls}"><h4>{label}</h4>'
            f'<div style="color:#64748b;font-size:.85rem;margin-bottom:.6rem">{desc}</div>'
            f'<div style="display:flex;gap:1.2rem;flex-wrap:wrap">'
            f'<div><span class="stat-label">Runs</span><div class="stat-value">{inv:,}</div></div>'
            f'<div><span class="stat-label">Tokens</span><div class="stat-value">{tok:,}</div></div>'
            f'<div><span class="stat-label">Avg ms</span><div class="stat-value">{ms}</div></div>'
            f'<div><span class="stat-label">Failed runs</span>'
            f'<div class="stat-value" style="color:{err_color}">{err}</div></div>'
            f"</div></div>",
            unsafe_allow_html=True,
        )

# ============================================================================
# LATEST REMEDIATION PLAYBOOK — big alert card when post-val fired
# ============================================================================
latest_rem = _try_query(
    """
    SELECT ts, output_preview
    FROM CONTROL.agent_reasoning_log
    WHERE agent_name = 'RemediationAgent' AND phi_check = 'clean'
    ORDER BY ts DESC LIMIT 1
    """
)
if not latest_rem.empty:
    rem_data = _parse_preview(latest_rem.iloc[0]["output_preview"])
    if rem_data:
        playbook_id = rem_data.get("playbook_id", "UNKNOWN")
        priority = rem_data.get("priority", "?")
        classification = rem_data.get("primary_classification", "?")
        recommendation = rem_data.get("recommendation", "?")
        actions_raw = rem_data.get("recommended_actions", [])
        actions = actions_raw if isinstance(actions_raw, list) else []
        hours = rem_data.get("estimated_hours", "?")
        rem_ts = latest_rem.iloc[0]["ts"]

        actions_html = "".join(f"<li>{a}</li>" for a in actions) or "<li>(no actions parsed)</li>"

        st.markdown(
            f'<div class="playbook-alert">'
            f"<h3>🛠️ Latest Remediation Playbook — {_relative_time(rem_ts)}</h3>"
            f'<div class="pb-meta">'
            f"<div><b>Playbook:</b> {playbook_id}</div>"
            f"<div><b>Priority:</b> {priority}</div>"
            f"<div><b>Classification:</b> {classification}</div>"
            f"<div><b>Recommendation:</b> {recommendation}</div>"
            f"<div><b>Est. MTTR:</b> {hours}h</div>"
            f"</div>"
            f"<div><b>Recommended actions (in order):</b></div>"
            f"<ol>{actions_html}</ol>"
            f'<div class="dba-badge">🔒 DBA_APPROVAL_REQUIRED — nothing auto-executes</div>'
            f'<div style="color:#7f1d1d;font-size:.8rem;margin-top:.5rem">'
            f"Raised at {_fmt_time(rem_ts)}. Human operator must review in DQ Author / DQ Review "
            f"before any action is taken against the pipeline."
            f"</div></div>",
            unsafe_allow_html=True,
        )


# ============================================================================
# RECENT ACTIVITY — NARRATIVE FEED (the key deliverable)
# ============================================================================
st.subheader("📜 Recent agent activity — what each agent actually did")

feed_df = _try_query(
    f"""
    SELECT ts, agent_name, crew_name, input_preview, output_preview,
           tokens_used, duration_ms, phi_check
    FROM CONTROL.agent_reasoning_log
    WHERE ts >= DATEADD(day, -{days}, CURRENT_TIMESTAMP()) {crew_filter}
    ORDER BY ts DESC
    LIMIT 25
    """,
    crew_params,
)


def _render_narrative(row: pd.Series) -> None:
    """Render one agent invocation as a human-readable card."""
    meta = _AGENT_META.get(row["agent_name"], {})
    icon = meta.get("icon", "🤖")
    label = meta.get("label", row["agent_name"])
    crew_cls = "pre" if row["crew_name"] == "pre_validation" else "post"
    ts_str = _relative_time(row["ts"])
    tokens = int(row["tokens_used"] or 0)
    dur_ms = int(row["duration_ms"] or 0)
    is_error = str(row["phi_check"] or "").startswith("error")
    result_cls = "err" if is_error else ""

    output = _parse_preview(row["output_preview"])
    input_keys = json.loads(row["input_preview"]) if row["input_preview"] else []

    # Per-agent NARRATIVE rendering.
    # ------------------------------------------------------------
    task = ""
    result = ""
    agent = row["agent_name"]

    if agent == "ProfilerAgent":
        table = output.get("table_name", "(unknown)")
        row_count = output.get("row_count", "?")
        cols_raw = output.get("column_names", [])
        n_cols = len(cols_raw) if isinstance(cols_raw, list) else 0
        task = f"Profile table <b>{table}</b> ({row_count} rows, {n_cols} columns)"
        result = (
            "Computed row count + null% + distinct count for every column. "
            "Zero PHI in payload — only aggregate statistics sent onward."
        )

    elif agent == "ExpectationAuthorAgent":
        suite = output.get("suite_name", "(unknown)")
        count = output.get("expectation_count", "?")
        status = output.get("status", "?")
        task = f"Draft quality rules for suite <b>{suite}</b>"
        result = (
            f"Produced <b>{count} expectation rules</b> — saved as status=<b>{status}</b>. "
            f"Now waiting for human review in DQ Author page."
        )

    elif agent == "ReviewerAgent":
        suite = output.get("suite_name", "(unknown)")
        total = output.get("total_expectations", "?")
        flagged = output.get("flagged_for_human_review", 0)
        task = f"Review {total} draft rules in <b>{suite}</b>"
        if isinstance(flagged, int | str) and str(flagged) != "0":
            result = (
                f"<b>{flagged}</b> expectations require human approval — typically "
                f"value-set rules where the agent cannot infer the complete valid "
                f"set. The remainder qualify for auto-approval."
            )
        else:
            result = f"All {total} expectations satisfy structural-confidence criteria; no human review required."

    elif agent == "RootCauseAgent":
        run_id = output.get("run_id", "?")
        ckpt = output.get("checkpoint_name", "?")
        fails = output.get("failure_count", "?")
        task = f"Diagnose BREACH at checkpoint <b>{ckpt}</b>"
        classified = output.get("classified_failures", [])
        if isinstance(classified, list) and classified:
            classes = {c.get("classification", "?") for c in classified if isinstance(c, dict)}
            known = sum(1 for c in classified if isinstance(c, dict) and c.get("known_issue"))
            result = (
                f"Analyzed <b>{fails} failed expectation(s)</b>. "
                f"Classification(s): <b>{', '.join(classes)}</b>. "
                f"Recurring issues: {known}/{fails}. "
                f"Run: <code>{run_id[-30:] if isinstance(run_id, str) else run_id}</code>."
            )
        else:
            result = f"Reviewed {fails} failures — see downstream agents for playbook."

    elif agent == "RemediationAgent":
        pb = output.get("playbook_id", "?")
        prio = output.get("priority", "?")
        rec = output.get("recommendation", "?")
        actions = output.get("recommended_actions", [])
        n_actions = len(actions) if isinstance(actions, list) else 0
        hrs = output.get("estimated_hours", "?")
        task = "Plan remediation for the breach (classification upstream)"
        result = (
            f"Selected playbook <b>{pb}</b> (priority {prio}). "
            f"Recommended: <b>{rec}</b>. {n_actions} action step(s). "
            f"Est. MTTR: {hrs}h. <b>Requires DBA approval — no auto-execute.</b>"
        )

    elif agent == "ReportingAgent":
        title = output.get("title", "(no title)")
        sev = output.get("severity", "?")
        sent = output.get("sent", "?")
        task = "Escalate incident via notifier"
        result = (
            f"Sent: <b>{title}</b>. Severity: <b>{sev}</b>. Delivered: <b>{sent}</b>. "
            f"Check the webhook inbox at http://localhost:9000 for the full message."
        )

    else:
        task = f"Input keys: {', '.join(str(k) for k in input_keys[:6])}"
        result_keys = [k for k in output if not k.startswith("_")][:6]
        result = f"Output keys: {', '.join(result_keys)}"

    if is_error:
        result = f"❌ Agent errored: <code>{row['phi_check']}</code>"

    st.markdown(
        f'<div class="feed-card {crew_cls}">'
        f'<div class="feed-head">{icon} <span>{label}</span>'
        f'<span style="color:#a78bfa">· {row["crew_name"].replace("_", "-")}</span>'
        f'<span class="ts">{ts_str}</span></div>'
        f'<div class="feed-task"><b>Task:</b> {task}</div>'
        f'<div class="feed-result {result_cls}"><b>Result:</b> {result}</div>'
        f'<div class="feed-foot">{dur_ms} ms · {tokens:,} tokens · guard = {row["phi_check"]}</div>'
        f"</div>",
        unsafe_allow_html=True,
    )


if feed_df.empty:
    st.markdown(
        '<div class="empty-state">No agent invocations yet. Trigger a DAG with GX + '
        "agents enabled to populate this feed.</div>",
        unsafe_allow_html=True,
    )
else:
    for _, row in feed_df.iterrows():
        _render_narrative(row)


# ============================================================================
# PER-AGENT CARDS
# ============================================================================
st.markdown("---")
st.subheader("By agent — fleet summary")

agent_df = _try_query(
    f"""
    SELECT agent_name, COUNT(*) AS invocations,
           COALESCE(SUM(tokens_used), 0) AS tokens,
           COALESCE(AVG(duration_ms), 0) AS avg_ms,
           COALESCE(SUM(CASE WHEN phi_check LIKE 'error%' THEN 1 ELSE 0 END), 0) AS errors,
           MAX(ts) AS last_ts
    FROM CONTROL.agent_reasoning_log
    WHERE ts >= DATEADD(day, -{days}, CURRENT_TIMESTAMP()) {crew_filter}
    GROUP BY agent_name
    """,
    crew_params,
)
agents_live = {r["agent_name"]: r for _, r in agent_df.iterrows()} if not agent_df.empty else {}

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
        last = (
            _relative_time(live["last_ts"])
            if live is not None and pd.notna(live.get("last_ts"))
            else "—"
        )
        accent = _INDIGO if meta["crew"] == "pre_validation" else _ROSE
        err_color = _ROSE if err > 0 else _EMERALD
        with cols[col_idx]:
            st.markdown(
                f'<div class="agent-card" style="border-left-color:{accent}">'
                f'<div class="title">{meta["icon"]} {meta["label"]} '
                f'<span style="font-size:.7rem;color:#94a3b8;font-weight:400">({name})</span></div>'
                f'<div class="role">{meta["role"]}</div>'
                f'<div class="stats">'
                f'<div><span class="stat-label">Runs</span><div class="stat-value">{inv:,}</div></div>'
                f'<div><span class="stat-label">Tokens</span><div class="stat-value">{tok:,}</div></div>'
                f'<div><span class="stat-label">Avg ms</span><div class="stat-value">{ms}</div></div>'
                f'<div><span class="stat-label">Errors</span>'
                f'<div class="stat-value" style="color:{err_color}">{err}</div></div>'
                f'<div><span class="stat-label">Last run</span>'
                f'<div class="stat-value" style="font-size:.85rem">{last}</div></div>'
                f"</div></div>",
                unsafe_allow_html=True,
            )


# ============================================================================
# TIMELINE + TOKEN BURN
# ============================================================================
st.markdown("---")
col_left, col_right = st.columns([2, 1])

with col_left:
    st.subheader("Activity timeline (bubble size = duration)")
    tl = _try_query(
        f"""
        SELECT ts, agent_name, crew_name, duration_ms
        FROM CONTROL.agent_reasoning_log
        WHERE ts >= DATEADD(day, -{days}, CURRENT_TIMESTAMP()) {crew_filter}
        ORDER BY ts DESC LIMIT 500
        """,
        crew_params,
    )
    if tl.empty:
        st.markdown('<div class="empty-state">No invocations yet.</div>', unsafe_allow_html=True)
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
        fig.update_layout(height=380, margin=dict(t=10, b=40))
        st.plotly_chart(fig, use_container_width=True)

with col_right:
    st.subheader("LLM tokens per day")
    burn = _try_query(
        f"""
        SELECT DATE_TRUNC('day', ts) AS d, SUM(tokens_used) AS tokens
        FROM CONTROL.agent_reasoning_log
        WHERE ts >= DATEADD(day, -{days}, CURRENT_TIMESTAMP()) {crew_filter}
        GROUP BY d ORDER BY d
        """,
        crew_params,
    )
    if burn.empty or burn["tokens"].sum() == 0:
        st.markdown(
            '<div class="empty-state">No token usage recorded.</div>', unsafe_allow_html=True
        )
    else:
        fig = px.bar(
            burn,
            x="d",
            y="tokens",
            color_discrete_sequence=[_INDIGO],
            labels={"d": "Date", "tokens": "Tokens"},
        )
        fig.update_layout(height=380, margin=dict(t=10, b=40), showlegend=False)
        st.plotly_chart(fig, use_container_width=True)


# ============================================================================
# GX vs CrewAI COMPARISON
# ============================================================================
st.markdown("---")
st.subheader("GX vs CrewAI — what's the difference?")
st.markdown(
    '<div class="gx-compare">'
    '<div class="gx-side"><h4 class="gx-title">🛡️ Great Expectations</h4>'
    "<ul>"
    "<li><b>Role:</b> deterministic rule CHECKER</li>"
    "<li><b>When:</b> every checkpoint (CP1/CP2/CP3)</li>"
    "<li><b>Input:</b> actual Bronze/Silver/Gold rows</li>"
    "<li><b>Output:</b> PASSED / FAILED / BREACHED</li>"
    "<li><b>Autonomy:</b> zero — never changes data or rules</li>"
    "<li><b>AI?</b> No — pure Python/SQL</li>"
    "</ul></div>"
    '<div class="ai-side"><h4 class="ai-title">🤖 CrewAI agent layer</h4>'
    "<ul>"
    "<li><b>Role:</b> autonomous REASONER (before + after GX)</li>"
    "<li><b>When:</b> Pre = suite authoring; Post = on BREACH only</li>"
    "<li><b>Input:</b> metadata + stats (NEVER rows — PHI guard)</li>"
    "<li><b>Output:</b> drafts · classifications · playbooks · alerts</li>"
    "<li><b>Autonomy:</b> suggests only — DBA_APPROVAL_REQUIRED</li>"
    "<li><b>AI?</b> Partial: rules + LLM narrative (stub or Anthropic)</li>"
    "</ul></div></div>",
    unsafe_allow_html=True,
)
st.caption(
    "Mental model: **GX is the radar** (what IS wrong). "
    "**CrewAI is the navigator** (why + what to do). Complementary, not redundant."
)

# ============================================================================
# RAW REASONING LOG — expandable, for engineers
# ============================================================================
with st.expander("🔧 Raw reasoning log (for engineers) — click to expand", expanded=False):
    raw = _try_query(
        f"""
        SELECT ts, agent_name, crew_name, input_preview, output_preview,
               tokens_used, duration_ms, phi_check
        FROM CONTROL.agent_reasoning_log
        WHERE ts >= DATEADD(day, -{days}, CURRENT_TIMESTAMP()) {crew_filter}
        ORDER BY ts DESC LIMIT 100
        """,
        crew_params,
    )
    if raw.empty:
        st.markdown('<div class="empty-state">No entries.</div>', unsafe_allow_html=True)
    else:
        display = raw.assign(
            guard=raw["phi_check"].fillna("?"),
            asked=raw["input_preview"].fillna(""),
            concluded=raw["output_preview"].fillna("").str.slice(0, 200),
        )[
            [
                "ts",
                "agent_name",
                "crew_name",
                "guard",
                "tokens_used",
                "duration_ms",
                "asked",
                "concluded",
            ]
        ]
        st.dataframe(
            display,
            use_container_width=True,
            hide_index=True,
            column_config={
                "ts": st.column_config.DatetimeColumn("Time", width="small"),
                "agent_name": "Agent",
                "crew_name": "Crew",
                "guard": st.column_config.TextColumn(
                    "PHI guard", help="'clean' = payload cleared the PHI boundary check"
                ),
                "tokens_used": st.column_config.NumberColumn("Tokens", format="%d"),
                "duration_ms": st.column_config.NumberColumn("ms"),
                "asked": st.column_config.TextColumn(
                    "What was passed to this agent",
                    help="List of JSON keys in the agent's input context — NO row data, only key names",
                ),
                "concluded": st.column_config.TextColumn(
                    "What this agent returned",
                    help="First 200 chars of the agent's output payload (JSON keys + summary values, PHI-safe)",
                ),
            },
        )

st.caption(
    f"Dashboard refreshed {_fmt_time(pd.Timestamp.utcnow())}  ·  "
    f"Source: `{WAREHOUSE_PATH}`  ·  "
    f"PHI boundary: `datalink/phi/guard.py` enforces metadata-only before every LLM call"
)
