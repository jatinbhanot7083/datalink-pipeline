"""Executive Dashboard — Phase 20.3 (factory-pattern, Snowflake-only).

VP-level rollup of the entire DataLink platform.  No client filter (the
factory pattern is universal across clients); aggregation is per-dataset
+ per-layer + cross-fleet.

Page structure:
  1. KPI strip          — LIVE pipelines, runs (today), DQ pass rate,
                          rows processed, AI spend (7d)
  2. Per-dataset health — one row per dataset showing 3-layer DQ status
                          + last run + drift status
  3. Per-client mini-cards — 4-up grid of each client's footprint
  4. Embedded Grafana panels — real-time time-series for the 3 dashboards
                               (pipeline runs / DQ quality / AI economics)

Data source: ``CONTROL.*`` tables in Snowflake (no DuckDB legacy).
"""

from __future__ import annotations

import os
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.express as px
import streamlit as st

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

st.set_page_config(
    page_title="DataLink — Executive Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

from datalink.quality.control import CONTROL_SCHEMA, create_control_tables  # noqa: E402
from datalink.ui._nav import render_sidebar  # noqa: E402
from datalink.ui._query import warehouse_ctx  # noqa: E402

render_sidebar(active="Executive Dashboard")

_NAVY = "#0a1a3e"
_GOLD = "#d4af37"
_GREEN = "#15803d"
_RED = "#b91c1c"
_AMBER = "#d97706"
_PURPLE = "#7c3aed"
_GREY = "#64748b"
_BRAND = [_NAVY, _GOLD, _GREEN, _RED, _AMBER, _PURPLE, "#6366f1"]

# Grafana embed — anonymous-read should be enabled for these to render
# without auth.  Override via env if Grafana is at a different URL.
GRAFANA_BASE = os.environ.get("GRAFANA_PUBLIC_URL", "http://localhost:3000")

st.markdown(
    f"""
    <style>
      h1 {{color: {_NAVY}; border-bottom: 3px solid {_GOLD}; padding-bottom: .4rem;}}
      h2 {{color: {_NAVY}; margin-top: 1.5rem;}}
      .kpi-card {{
          background:#fff; border-left:6px solid {_GOLD}; padding:1rem 1.2rem;
          border-radius:6px; box-shadow:0 1px 2px rgba(0,0,0,.04);
          height:108px;
      }}
      .kpi-label {{color:#475569;font-size:.78rem;text-transform:uppercase;letter-spacing:.08em;font-weight:600;}}
      .kpi-value {{color:{_NAVY};font-size:2.1rem;font-weight:700;margin-top:.2rem;}}
      .kpi-sub   {{color:{_GREY};font-size:.78rem;margin-top:.2rem;}}
      .empty-state {{
          background:#f8fafc; border:2px dashed #cbd5e1; padding:2rem;
          text-align:center; border-radius:8px; color:#64748b;
      }}
      .pill {{display:inline-block;padding:.15rem .55rem;border-radius:12px;
              font-size:.72rem;font-weight:700;letter-spacing:.04em;}}
      .pill-live  {{background:#dcfce7;color:#166534}}
      .pill-none  {{background:#f3f4f6;color:#4b5563}}
      .pill-drift {{background:#fef3c7;color:#92400e}}
      iframe {{border-radius:8px;}}
    </style>
    """,
    unsafe_allow_html=True,
)


@contextmanager
def _wh(*, readonly: bool = True) -> Iterator[Any]:
    with warehouse_ctx(readonly=readonly) as w:
        yield w


with _wh(readonly=False) as _wh_boot:
    create_control_tables(_wh_boot)


# ---------------------------------------------------------------------------
# Data loaders — every query keyed off CONTROL schema, no client filter.
# ---------------------------------------------------------------------------
@st.cache_data(ttl=20)
def _kpi_rollup() -> dict[str, Any]:
    out: dict[str, Any] = {}
    with warehouse_ctx(readonly=True) as wh:
        for k, sql in [
            (
                "pipelines_live",
                f"SELECT COUNT(*) c FROM {CONTROL_SCHEMA}.client_pipeline_instances WHERE status='LIVE'",
            ),
            (
                "clients_onboarded",
                f"SELECT COUNT(DISTINCT client_id) c FROM {CONTROL_SCHEMA}.client_pipeline_instances",
            ),
            (
                "datasets_active",
                f"SELECT COUNT(DISTINCT dataset_code) c FROM {CONTROL_SCHEMA}.client_pipeline_instances WHERE status='LIVE'",
            ),
            (
                "dq_suites_live",
                f"SELECT COUNT(*) c FROM {CONTROL_SCHEMA}.dq_suites "
                f"WHERE status='LIVE' AND dataset_code IS NOT NULL",
            ),
            (
                "runs_24h",
                f"SELECT COUNT(*) c FROM {CONTROL_SCHEMA}.dataset_pipeline_runs "
                f"WHERE started_at >= DATEADD(hour,-24,CURRENT_TIMESTAMP())",
            ),
            (
                "runs_success_24h",
                f"SELECT COUNT(*) c FROM {CONTROL_SCHEMA}.dataset_pipeline_runs "
                f"WHERE started_at >= DATEADD(hour,-24,CURRENT_TIMESTAMP()) "
                f"AND status='SUCCESS'",
            ),
            (
                "rows_gold_24h",
                f"SELECT COALESCE(SUM(rows_gold),0) c FROM {CONTROL_SCHEMA}.dataset_pipeline_runs "
                f"WHERE started_at >= DATEADD(hour,-24,CURRENT_TIMESTAMP())",
            ),
            (
                "drift_7d",
                f"SELECT COUNT(*) c FROM {CONTROL_SCHEMA}.schema_drift_log "
                f"WHERE detected_at >= DATEADD(day,-7,CURRENT_TIMESTAMP())",
            ),
        ]:
            try:
                rs = list(wh.query(sql))
                out[k] = int((rs[0] if rs else {}).get("c") or 0)
            except Exception:
                out[k] = 0
        # AI cost — sum tokens × $0.000003 (Haiku 4.5 blended)
        try:
            rs = list(
                wh.query(
                    f"SELECT COALESCE(SUM(ai_token_count),0) c "
                    f"FROM {CONTROL_SCHEMA}.dq_suites "
                    f"WHERE created_at >= DATEADD(day,-7,CURRENT_TIMESTAMP())"
                )
            )
            tokens = int((rs[0] if rs else {}).get("c") or 0)
            out["ai_tokens_7d"] = tokens
            out["ai_cost_usd_7d"] = tokens * 0.000003
        except Exception:
            out["ai_tokens_7d"] = 0
            out["ai_cost_usd_7d"] = 0.0
    return out


@st.cache_data(ttl=20)
def _per_dataset_health() -> list[dict[str, Any]]:
    """Returns one row per dataset that has at least one LIVE pipeline,
    summarising client count, layer DQ status, last run, drift counts."""
    with warehouse_ctx(readonly=True) as wh:
        try:
            return list(
                wh.query(f"""
                WITH inst AS (
                  SELECT dataset_code,
                         COUNT(CASE WHEN status='LIVE' THEN 1 END) AS live_clients,
                         MAX(deployed_at) AS last_deployed
                    FROM {CONTROL_SCHEMA}.client_pipeline_instances
                   GROUP BY dataset_code
                ),
                runs AS (
                  SELECT dataset_code,
                         COUNT(*) AS runs_24h,
                         SUM(CASE WHEN status='SUCCESS' THEN 1 ELSE 0 END) AS pass_24h,
                         MAX(started_at) AS last_run
                    FROM {CONTROL_SCHEMA}.dataset_pipeline_runs
                   WHERE started_at >= DATEADD(hour,-24,CURRENT_TIMESTAMP())
                   GROUP BY dataset_code
                ),
                suites AS (
                  SELECT dataset_code,
                         COUNT(CASE WHEN UPPER(layer)='BRONZE' AND status='LIVE' THEN 1 END) AS bronze_suite,
                         COUNT(CASE WHEN UPPER(layer)='SILVER' AND status='LIVE' THEN 1 END) AS silver_suite,
                         COUNT(CASE WHEN UPPER(layer)='GOLD'   AND status='LIVE' THEN 1 END) AS gold_suite
                    FROM {CONTROL_SCHEMA}.dq_suites
                   WHERE dataset_code IS NOT NULL
                   GROUP BY dataset_code
                ),
                drift AS (
                  SELECT source_type AS dataset_code, COUNT(*) AS drift_7d
                    FROM {CONTROL_SCHEMA}.schema_drift_log
                   WHERE detected_at >= DATEADD(day,-7,CURRENT_TIMESTAMP())
                   GROUP BY source_type
                )
                SELECT i.dataset_code, i.live_clients, i.last_deployed,
                       COALESCE(r.runs_24h, 0)  AS runs_24h,
                       COALESCE(r.pass_24h, 0)  AS pass_24h,
                       r.last_run,
                       COALESCE(s.bronze_suite, 0) AS bronze_suite,
                       COALESCE(s.silver_suite, 0) AS silver_suite,
                       COALESCE(s.gold_suite, 0)   AS gold_suite,
                       COALESCE(d.drift_7d, 0) AS drift_7d
                  FROM inst i
                  LEFT JOIN runs   r ON r.dataset_code = i.dataset_code
                  LEFT JOIN suites s ON s.dataset_code = i.dataset_code
                  LEFT JOIN drift  d ON LOWER(d.dataset_code) = LOWER(i.dataset_code)
                 ORDER BY i.live_clients DESC, i.dataset_code
            """)
            )
        except Exception as exc:
            st.warning(f"Per-dataset query failed: {type(exc).__name__}: {exc}")
            return []


@st.cache_data(ttl=20)
def _per_client_health() -> list[dict[str, Any]]:
    with warehouse_ctx(readonly=True) as wh:
        try:
            return list(
                wh.query(f"""
                SELECT i.client_id,
                       COUNT(CASE WHEN i.status='LIVE' THEN 1 END)       AS live_pipelines,
                       COUNT(DISTINCT i.dataset_code)                AS datasets,
                       MAX(i.deployed_at)                            AS last_deployed,
                       COALESCE(MAX(r.last_run), NULL)               AS last_run
                  FROM {CONTROL_SCHEMA}.client_pipeline_instances i
                  LEFT JOIN (
                       SELECT client_id, MAX(started_at) AS last_run
                         FROM {CONTROL_SCHEMA}.dataset_pipeline_runs
                        GROUP BY client_id
                  ) r ON LOWER(r.client_id) = LOWER(i.client_id)
                 WHERE i.status <> 'ARCHIVED'
                 GROUP BY i.client_id
                 ORDER BY live_pipelines DESC, i.client_id
            """)
            )
        except Exception:
            return []


# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------
st.markdown("# 📊 Executive Dashboard")
st.caption(
    "Cross-tenant rollup of the DataLink platform.  Factory pattern → "
    "no client filter; per-dataset and per-client breakdowns are below.  "
    "Real-time time-series at the bottom (embedded Grafana panels)."
)


# ---------------------------------------------------------------------------
# KPI strip
# ---------------------------------------------------------------------------
kpi = _kpi_rollup()


def _kpi_card(label: str, value: str, sub: str = "") -> str:
    return (
        f'<div class="kpi-card">'
        f'<div class="kpi-label">{label}</div>'
        f'<div class="kpi-value">{value}</div>'
        f'<div class="kpi-sub">{sub}</div>'
        f"</div>"
    )


k1, k2, k3, k4, k5 = st.columns(5)
runs_24h = kpi.get("runs_24h", 0)
pass_24h = kpi.get("runs_success_24h", 0)
pass_rate = (100.0 * pass_24h / runs_24h) if runs_24h else 0.0
k1.markdown(
    _kpi_card(
        "LIVE pipelines",
        f"{kpi.get('pipelines_live', 0):,}",
        f"{kpi.get('clients_onboarded', 0)} clients · {kpi.get('datasets_active', 0)} datasets",
    ),
    unsafe_allow_html=True,
)
k2.markdown(
    _kpi_card(
        "Runs (24h)", f"{runs_24h:,}", f"{pass_24h:,} succeeded · {pass_rate:.1f}% pass rate"
    ),
    unsafe_allow_html=True,
)
k3.markdown(
    _kpi_card("DQ suites LIVE", f"{kpi.get('dq_suites_live', 0):,}", "across (dataset, layer)"),
    unsafe_allow_html=True,
)
k4.markdown(
    _kpi_card("Gold rows (24h)", f"{kpi.get('rows_gold_24h', 0):,}", "delivered downstream"),
    unsafe_allow_html=True,
)
k5.markdown(
    _kpi_card(
        "AI spend (7d)",
        f"${kpi.get('ai_cost_usd_7d', 0):.3f}",
        f"{kpi.get('ai_tokens_7d', 0):,} tokens",
    ),
    unsafe_allow_html=True,
)

# Drift banner
if kpi.get("drift_7d", 0) > 0:
    st.warning(
        f"🌀 **{kpi['drift_7d']} schema-drift event{'s' if kpi['drift_7d'] != 1 else ''} "
        f"in the last 7 days.**  Open **Schema Drift** for triage."
    )


st.markdown("---")


# ---------------------------------------------------------------------------
# Per-dataset health
# ---------------------------------------------------------------------------
st.markdown("## 🗂️ Per-dataset health")
st.caption(
    "One row per active dataset.  DQ status pills show whether a LIVE suite "
    "exists at each layer (Bronze / Silver / Gold).  Drift count is 7-day."
)

ds_rows = _per_dataset_health()
if not ds_rows:
    st.markdown(
        '<div class="empty-state">No active datasets yet.  Deploy a pipeline '
        "via 🤖 <strong>AI Architect</strong> in Pipeline Architect to populate this view.</div>",
        unsafe_allow_html=True,
    )
else:

    def _pill(have: bool, label: str) -> str:
        cls = "pill-live" if have else "pill-none"
        glyph = "🟢" if have else "—"
        return f'<span class="pill {cls}">{glyph} {label}</span>'

    table_rows = []
    for r in ds_rows:
        runs_24 = int(r.get("runs_24h") or 0)
        pass_24 = int(r.get("pass_24h") or 0)
        pr_pct = (100.0 * pass_24 / runs_24) if runs_24 else 0.0
        layers_html = " ".join(
            [
                _pill(int(r.get("bronze_suite") or 0) > 0, "Bronze"),
                _pill(int(r.get("silver_suite") or 0) > 0, "Silver"),
                _pill(int(r.get("gold_suite") or 0) > 0, "Gold"),
            ]
        )
        drift_n = int(r.get("drift_7d") or 0)
        drift_html = (
            f'<span class="pill pill-drift">🌀 {drift_n}</span>'
            if drift_n
            else '<span class="pill pill-none">—</span>'
        )
        table_rows.append(
            {
                "Dataset": r.get("dataset_code"),
                "Clients": r.get("live_clients"),
                "Runs 24h": runs_24,
                "Pass 24h": f"{pr_pct:.0f}%" if runs_24 else "—",
                "DQ suites": layers_html,
                "Drift 7d": drift_html,
                "Last run": str(r.get("last_run") or "—")[:19],
            }
        )

    # Render as raw HTML so the pills land properly.
    df_html = (
        "<table style='width:100%;border-collapse:collapse'>"
        "<thead><tr style='background:#f1f5f9'>"
        + "".join(
            f"<th style='padding:.5rem;text-align:left;font-size:.85rem'>{c}</th>"
            for c in table_rows[0].keys()
        )
        + "</tr></thead><tbody>"
    )
    for r in table_rows:
        df_html += "<tr style='border-bottom:1px solid #e2e8f0'>"
        for v in r.values():
            df_html += f"<td style='padding:.55rem;font-size:.85rem'>{v}</td>"
        df_html += "</tr>"
    df_html += "</tbody></table>"
    st.markdown(df_html, unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Per-client mini-cards
# ---------------------------------------------------------------------------
st.markdown("---")
st.markdown("## 🏢 Per-client footprint")
st.caption(
    "4-up grid of every onboarded client.  Click into Pipeline Architect for instance-level operations."
)

client_rows = _per_client_health()
if not client_rows:
    st.markdown(
        '<div class="empty-state">No clients onboarded yet.</div>',
        unsafe_allow_html=True,
    )
else:
    cols_per_row = 4
    for i in range(0, len(client_rows), cols_per_row):
        row = st.columns(cols_per_row)
        for j, c in enumerate(client_rows[i : i + cols_per_row]):
            with row[j]:
                last_run = c.get("last_run")
                last_run_str = str(last_run)[:19] if last_run else "never"
                st.markdown(
                    _kpi_card(
                        f"{c.get('client_id')}",
                        f"{int(c.get('live_pipelines') or 0):,} LIVE",
                        f"{int(c.get('datasets') or 0)} datasets · last run: {last_run_str}",
                    ),
                    unsafe_allow_html=True,
                )


# ---------------------------------------------------------------------------
# Plotly charts — at-a-glance from CONTROL.dataset_pipeline_runs
# ---------------------------------------------------------------------------
st.markdown("---")
st.markdown("## 📈 Native charts (from CONTROL.dataset_pipeline_runs)")


@st.cache_data(ttl=20)
def _runs_by_hour() -> pd.DataFrame:
    with warehouse_ctx(readonly=True) as wh:
        try:
            rows = list(
                wh.query(f"""
                SELECT DATE_TRUNC('hour', started_at) AS hour,
                       status,
                       COUNT(*) AS n
                  FROM {CONTROL_SCHEMA}.dataset_pipeline_runs
                 WHERE started_at >= DATEADD(hour,-48,CURRENT_TIMESTAMP())
                 GROUP BY 1, 2
                 ORDER BY 1
            """)
            )
        except Exception:
            rows = []
    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=["hour", "status", "n"])


cc1, cc2 = st.columns(2)
with cc1:
    runs_df = _runs_by_hour()
    if runs_df.empty:
        st.markdown(
            '<div class="empty-state">No pipeline runs in the last 48 hours.</div>',
            unsafe_allow_html=True,
        )
    else:
        runs_df.columns = [c.lower() for c in runs_df.columns]
        fig = px.bar(
            runs_df,
            x="hour",
            y="n",
            color="status",
            color_discrete_map={"SUCCESS": _GREEN, "FAILED": _RED, "RUNNING": _AMBER},
            title="Pipeline runs per hour (48h)",
        )
        fig.update_layout(
            height=320,
            margin={"l": 10, "r": 10, "t": 40, "b": 10},
            paper_bgcolor="white",
            plot_bgcolor="white",
        )
        st.plotly_chart(fig, use_container_width=True)

with cc2:
    if ds_rows:
        ds_df = pd.DataFrame(
            [
                {"dataset": r.get("dataset_code"), "live_clients": r.get("live_clients")}
                for r in ds_rows
            ]
        )
        fig = px.bar(
            ds_df.sort_values("live_clients", ascending=True).tail(15),
            x="live_clients",
            y="dataset",
            orientation="h",
            color_discrete_sequence=[_NAVY],
            title="LIVE clients per dataset (top 15)",
        )
        fig.update_layout(
            height=320,
            margin={"l": 10, "r": 10, "t": 40, "b": 10},
            paper_bgcolor="white",
            plot_bgcolor="white",
        )
        st.plotly_chart(fig, use_container_width=True)


# ---------------------------------------------------------------------------
# Embedded Grafana panels (real-time time-series)
# ---------------------------------------------------------------------------
st.markdown("---")
st.markdown("## 🔭 Real-time Grafana panels")
st.caption(
    "Live time-series straight from Prometheus.  Anonymous-read on Grafana "
    "lets these panels render without a login.  Click any chart to open the "
    "full dashboard."
)

# 3 dashboards × 2 panels each, embedded as iframes (kiosk mode).
EMBEDS = [
    (
        "DataLink — Pipeline Runs",
        f"{GRAFANA_BASE}/d/dl-pipeline-runs/datalink-pipeline-runs",
        [
            (10, "Pipeline Runs by Status"),
            (11, "Rows Processed by Layer"),
        ],
    ),
    (
        "DataLink — Data Quality",
        f"{GRAFANA_BASE}/d/dl-dq-quality/datalink-data-quality",
        [
            (10, "DQ Pass-Rate by Layer"),
            (13, "Schema Drift Timeline"),
        ],
    ),
    (
        "DataLink — AI Economics",
        f"{GRAFANA_BASE}/d/dl-ai-economics/datalink-ai-economics",
        [
            (10, "Tokens / hour by Agent"),
            (12, "Latency p50 / p95 by Agent"),
        ],
    ),
]

for title, dash_url, panels in EMBEDS:
    st.markdown(f"### [{title}]({dash_url})")
    cols = st.columns(2)
    for i, (panel_id, label) in enumerate(panels):
        with cols[i]:
            st.caption(label)
            embed_url = (
                f"{dash_url}?orgId=1&kiosk&theme=light&from=now-6h&to=now"
                f"&viewPanel={panel_id}&refresh=30s"
            )
            st.markdown(
                f'<iframe src="{embed_url}" width="100%" height="280" '
                f'frameborder="0" style="border-radius:8px;"></iframe>',
                unsafe_allow_html=True,
            )

st.caption(
    "_If panels show 'Not authorized', enable Grafana anonymous read in "
    "`docker/grafana/provisioning/grafana.ini` or set "
    "`GF_AUTH_ANONYMOUS_ENABLED=true` on the container._"
)
