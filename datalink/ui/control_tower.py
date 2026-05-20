"""DataLink Control Tower — Phase 21 (enterprise grade).

Single-pane operational console for the entire DataLink platform.  Every
element on this page MUST be:
  * actionable        — no decoration; every metric drills to its page
  * truthful          — sourced from CONTROL.* tables or live probes
  * factory-pattern   — no per-client filter at the page level
  * fail-soft         — never blocks the page if a service is down

Layout (top → bottom — operator's eye-path):

  1. 🚨 Critical Alerts        — red/amber banner, only renders when something
                                 actually needs a human (failed runs, fatal
                                 drift, dead service).  Empty by design.
  2. 🩺 System Health           — live service dots (Snowflake, Airflow, OTEL,
                                 Prom, Grafana, Loki, Tempo).  HTTP probes,
                                 cached 30 s.
  3. 📊 Headline KPIs           — 5 cards.  Bronze datasets / pipelines LIVE /
                                 runs today / DQ pass rate today / AI spend 7d.
                                 Each card linked to its detail page.
  4. 🌐 Factory Pattern Coverage — every active dataset, three DQ layer pills,
                                 # client instances, runs today.  Sortable.
  5. 🔭 Live Activity Feed      — last 10 runs, last 5 DQ failures, last 5
                                 schema-drift events — three columns.
  6. 🚀 Quick Actions           — 8 most-used workflows as deep-link buttons.
"""

from __future__ import annotations

import socket
import sys
import time
import urllib.request
from contextlib import closing
from pathlib import Path
from typing import Any

import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

st.set_page_config(
    page_title="DataLink Control Tower",
    page_icon="🏛️",
    layout="wide",
    initial_sidebar_state="expanded",
)

from datalink.quality.control import CONTROL_SCHEMA  # noqa: E402
from datalink.ui._nav import render_sidebar  # noqa: E402
from datalink.ui._query import warehouse_ctx  # noqa: E402

render_sidebar(active="Control Tower")

NAVY, GOLD = "#0a1a3e", "#d4af37"
GREEN, AMBER, RED, GREY, PURPLE = "#15803d", "#b45309", "#b91c1c", "#64748b", "#7c3aed"

st.markdown(
    f"""
    <style>
      h1 {{color:{NAVY};border-bottom:3px solid {GOLD};padding-bottom:.4rem}}
      h3 {{color:{NAVY};margin-top:1.2rem}}
      .ct-tile {{background:#fff;border:1px solid #d4d4d8;border-radius:8px;
                 padding:1rem 1.2rem;box-shadow:0 1px 3px rgba(0,0,0,.04);
                 height:108px}}
      .ct-tile-label {{font-size:.74rem;color:#64748b;text-transform:uppercase;
                       letter-spacing:.06em;font-weight:600}}
      .ct-tile-value {{font-size:1.95rem;font-weight:800;color:{NAVY};margin-top:.25rem}}
      .ct-tile-sub   {{font-size:.78rem;color:#94a3b8;margin-top:.2rem}}
      .ct-pill {{display:inline-block;padding:.12rem .55rem;border-radius:11px;
                 font-size:.7rem;font-weight:700;letter-spacing:.04em;margin-right:.25rem}}
      .ct-pill-ok    {{background:#dcfce7;color:#166534}}
      .ct-pill-warn  {{background:#fef3c7;color:#92400e}}
      .ct-pill-bad   {{background:#fee2e2;color:#991b1b}}
      .ct-pill-none  {{background:#f3f4f6;color:#4b5563}}
      .ct-svc-row    {{background:#fff;border:1px solid #e2e8f0;border-radius:8px;
                       padding:.7rem 1rem;display:flex;align-items:center;
                       justify-content:space-between;margin-bottom:.4rem}}
      .ct-svc-name   {{font-weight:600;color:{NAVY};font-size:.9rem}}
      .ct-svc-detail {{font-size:.75rem;color:#64748b}}
      .ct-alert-crit {{background:linear-gradient(90deg,#fee2e2,#fecaca);
                       border:1px solid #ef4444;border-left:6px solid #b91c1c;
                       border-radius:8px;padding:1rem 1.2rem;margin:.4rem 0}}
      .ct-alert-warn {{background:linear-gradient(90deg,#fffbeb,#fef3c7);
                       border:1px solid #f59e0b;border-left:6px solid #d97706;
                       border-radius:8px;padding:1rem 1.2rem;margin:.4rem 0}}
      .ct-alert-ok   {{background:linear-gradient(90deg,#f0fdf4,#dcfce7);
                       border:1px solid #86efac;border-left:6px solid #15803d;
                       border-radius:8px;padding:.7rem 1.2rem;margin:.4rem 0;
                       color:#166534;font-size:.88rem}}
      .ct-feed-card  {{background:#fff;border:1px solid #e2e8f0;border-radius:8px;
                       padding:.55rem .8rem;margin-bottom:.35rem;font-size:.84rem}}
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("🏛️ DataLink Control Tower")
st.caption(
    "Single-pane operational console — Bronze → Silver → Gold → Pipeline → "
    "Run.  Drill into any page from here."
)


# ===========================================================================
# Data loaders — separated by cost.  Cheap probes cached 20s; HTTP service
# probes 30s; heavy CONTROL queries 30s.
# ===========================================================================
@st.cache_data(ttl=30, show_spinner=False)
def _kpis() -> dict[str, Any]:
    out: dict[str, Any] = {
        "bronze_datasets": 0,
        "silver_live_client": 0,
        "silver_live_global": 0,
        "gold_live_client": 0,
        "gold_live_global": 0,
        "pipelines_live": 0,
        "pipelines_paused": 0,
        "pipelines_draft": 0,
        "clients_onboarded": 0,
        "datasets_active": 0,
        "dq_suites_live": 0,
        "dq_suites_draft": 0,
        "runs_24h": 0,
        "runs_24h_success": 0,
        "runs_24h_failed": 0,
        "runs_in_flight": 0,
        "rows_gold_24h": 0,
        "drift_7d": 0,
        "drift_24h": 0,
        "ai_tokens_7d": 0,
        "ai_cost_usd_7d": 0.0,
    }
    try:
        with warehouse_ctx(readonly=True) as wh:
            queries = [
                (
                    "bronze_datasets",
                    f"SELECT COUNT(*) c FROM {CONTROL_SCHEMA}.global_bronze_catalog_datasets WHERE is_active=TRUE",
                ),
                (
                    "silver_live_client",
                    f"SELECT COUNT(*) c FROM {CONTROL_SCHEMA}.global_silver_schema_datasets "
                    f"WHERE status='LIVE' AND scope_owner NOT IN ('GLOBAL_CORP','__global__','')",
                ),
                (
                    "silver_live_global",
                    f"SELECT COUNT(*) c FROM {CONTROL_SCHEMA}.global_silver_schema_datasets "
                    f"WHERE status='LIVE' AND scope_owner IN ('GLOBAL_CORP','__global__')",
                ),
                (
                    "gold_live_client",
                    f"SELECT COUNT(*) c FROM {CONTROL_SCHEMA}.global_gold_schema_datasets "
                    f"WHERE status='LIVE' AND scope_owner NOT IN ('GLOBAL_CORP','__global__','')",
                ),
                (
                    "gold_live_global",
                    f"SELECT COUNT(*) c FROM {CONTROL_SCHEMA}.global_gold_schema_datasets "
                    f"WHERE status='LIVE' AND scope_owner IN ('GLOBAL_CORP','__global__')",
                ),
                (
                    "pipelines_live",
                    f"SELECT COUNT(*) c FROM {CONTROL_SCHEMA}.client_pipeline_instances WHERE status='LIVE'",
                ),
                (
                    "pipelines_paused",
                    f"SELECT COUNT(*) c FROM {CONTROL_SCHEMA}.client_pipeline_instances WHERE status='PAUSED'",
                ),
                (
                    "pipelines_draft",
                    f"SELECT COUNT(*) c FROM {CONTROL_SCHEMA}.client_pipeline_instances "
                    f"WHERE status IN ('DRAFT','PENDING_REVIEW')",
                ),
                (
                    "clients_onboarded",
                    f"SELECT COUNT(DISTINCT client_id) c FROM {CONTROL_SCHEMA}.client_pipeline_instances "
                    f"WHERE status<>'ARCHIVED'",
                ),
                (
                    "datasets_active",
                    f"SELECT COUNT(DISTINCT dataset_code) c FROM {CONTROL_SCHEMA}.client_pipeline_instances "
                    f"WHERE status='LIVE'",
                ),
                (
                    "dq_suites_live",
                    f"SELECT COUNT(*) c FROM {CONTROL_SCHEMA}.dq_suites "
                    f"WHERE status='LIVE' AND dataset_code IS NOT NULL",
                ),
                (
                    "dq_suites_draft",
                    f"SELECT COUNT(*) c FROM {CONTROL_SCHEMA}.dq_suites "
                    f"WHERE status IN ('DRAFT','PENDING_REVIEW') AND dataset_code IS NOT NULL",
                ),
                (
                    "runs_24h",
                    f"SELECT COUNT(*) c FROM {CONTROL_SCHEMA}.dataset_pipeline_runs "
                    f"WHERE started_at >= DATEADD(hour,-24,CURRENT_TIMESTAMP())",
                ),
                (
                    "runs_24h_success",
                    f"SELECT COUNT(*) c FROM {CONTROL_SCHEMA}.dataset_pipeline_runs "
                    f"WHERE started_at >= DATEADD(hour,-24,CURRENT_TIMESTAMP()) AND status='SUCCESS'",
                ),
                (
                    "runs_24h_failed",
                    f"SELECT COUNT(*) c FROM {CONTROL_SCHEMA}.dataset_pipeline_runs "
                    f"WHERE started_at >= DATEADD(hour,-24,CURRENT_TIMESTAMP()) AND status='FAILED'",
                ),
                (
                    "runs_in_flight",
                    f"SELECT COUNT(*) c FROM {CONTROL_SCHEMA}.dataset_pipeline_runs WHERE status='RUNNING'",
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
                (
                    "drift_24h",
                    f"SELECT COUNT(*) c FROM {CONTROL_SCHEMA}.schema_drift_log "
                    f"WHERE detected_at >= DATEADD(hour,-24,CURRENT_TIMESTAMP())",
                ),
                (
                    "ai_tokens_7d",
                    f"SELECT COALESCE(SUM(ai_token_count),0) c FROM {CONTROL_SCHEMA}.dq_suites "
                    f"WHERE created_at >= DATEADD(day,-7,CURRENT_TIMESTAMP())",
                ),
            ]
            for k, q in queries:
                try:
                    rs = list(wh.query(q))
                    out[k] = int((rs[0] if rs else {}).get("c") or 0)
                except Exception:
                    pass
            out["ai_cost_usd_7d"] = round(out["ai_tokens_7d"] * 0.000003, 4)
    except Exception:
        pass
    return out


@st.cache_data(ttl=30, show_spinner=False)
def _service_health() -> list[dict[str, Any]]:
    """Probe each service.  ``status`` is OK / WARN / DOWN; ``detail`` is a
    short note (latency or error)."""
    services: list[dict[str, Any]] = []

    def _probe_http(
        name: str, url: str, expect_text: str | None = None, timeout: float = 2.0
    ) -> dict[str, Any]:
        t0 = time.perf_counter()
        try:
            with closing(urllib.request.urlopen(url, timeout=timeout)) as r:
                body = r.read(2048).decode("utf-8", errors="replace")
                if expect_text and expect_text not in body:
                    return {
                        "name": name,
                        "status": "WARN",
                        "detail": f"unexpected body ({len(body)} bytes)",
                    }
                latency_ms = int((time.perf_counter() - t0) * 1000)
                return {"name": name, "status": "OK", "detail": f"{latency_ms} ms"}
        except Exception as exc:
            return {"name": name, "status": "DOWN", "detail": str(exc)[:80]}

    def _probe_tcp(name: str, host: str, port: int, timeout: float = 1.5) -> dict[str, Any]:
        t0 = time.perf_counter()
        try:
            with closing(socket.create_connection((host, port), timeout=timeout)):
                latency_ms = int((time.perf_counter() - t0) * 1000)
                return {"name": name, "status": "OK", "detail": f"{latency_ms} ms"}
        except Exception as exc:
            return {"name": name, "status": "DOWN", "detail": str(exc)[:80]}

    # Snowflake — light query rather than TCP (the warehouse may be paused)
    t0 = time.perf_counter()
    try:
        with warehouse_ctx(readonly=True) as wh:
            list(
                wh.query(
                    f"SELECT 1 AS c FROM {CONTROL_SCHEMA}.global_bronze_catalog_datasets LIMIT 1"
                )
            )
        services.append(
            {
                "name": "Snowflake (CONTROL)",
                "status": "OK",
                "detail": f"{int((time.perf_counter()-t0)*1000)} ms",
            }
        )
    except Exception as exc:
        services.append({"name": "Snowflake (CONTROL)", "status": "DOWN", "detail": str(exc)[:80]})

    # The Docker stack — probe each service first by its compose
    # ``container_name:`` (resolves over the datalink compose network when
    # Streamlit runs inside Docker), then fall back to ``localhost:<published>``
    # (works when running outside Docker, e.g. WSL dev shell).  Bug fix
    # 2026-05-10: previously used the bare compose service name
    # (``airflow-webserver``) which DNS resolves to NEITHER form — Docker
    # gives you the literal ``container_name`` and the literal service name
    # (``airflow_webserver`` with underscore in our compose), neither of
    # which match.  Use the container_name explicitly.
    def _probe_or_fallback(name: str, primary: str, fallback: str) -> dict[str, Any]:
        r = _probe_http(name, primary)
        return r if r["status"] != "DOWN" else _probe_http(name, fallback)

    services.append(
        _probe_or_fallback(
            "Airflow webserver",
            "http://datalink-airflow-webserver:8080/health",
            "http://localhost:8088/health",
        )
    )
    services.append(
        _probe_or_fallback(
            "Prometheus",
            "http://datalink-prometheus:9090/-/ready",
            "http://localhost:9090/-/ready",
        )
    )
    services.append(
        _probe_or_fallback(
            "Grafana",
            "http://datalink-grafana:3000/api/health",
            "http://localhost:3000/api/health",
        )
    )
    services.append(
        _probe_or_fallback(
            "Loki",
            "http://datalink-loki:3100/ready",
            "http://localhost:3100/ready",
        )
    )
    services.append(
        _probe_or_fallback(
            "Tempo",
            "http://datalink-tempo:3200/ready",
            "http://localhost:3200/ready",
        )
    )
    # OTEL collector — healthz endpoint on :13133
    services.append(
        _probe_or_fallback(
            "OTEL collector",
            "http://datalink-otel:13133/",
            "http://localhost:13133/",
        )
    )

    return services


@st.cache_data(ttl=30, show_spinner=False)
def _factory_coverage() -> list[dict[str, Any]]:
    """One row per active dataset with DQ-suite presence, instance counts,
    and 24-hour run stats."""
    try:
        with warehouse_ctx(readonly=True) as wh:
            return list(
                wh.query(f"""
                WITH inst AS (
                  SELECT dataset_code,
                         COUNT(CASE WHEN status='LIVE' THEN 1 END) AS live_clients,
                         COUNT(DISTINCT client_id) AS distinct_clients,
                         MAX(deployed_at) AS last_deployed
                    FROM {CONTROL_SCHEMA}.client_pipeline_instances
                   GROUP BY dataset_code
                ),
                runs AS (
                  SELECT dataset_code,
                         COUNT(*) AS runs_24h,
                         SUM(CASE WHEN status='SUCCESS' THEN 1 ELSE 0 END) AS pass_24h,
                         SUM(CASE WHEN status='FAILED'  THEN 1 ELSE 0 END) AS fail_24h,
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
                SELECT i.dataset_code,
                       i.live_clients,
                       i.distinct_clients,
                       i.last_deployed,
                       COALESCE(r.runs_24h, 0)  AS runs_24h,
                       COALESCE(r.pass_24h, 0)  AS pass_24h,
                       COALESCE(r.fail_24h, 0)  AS fail_24h,
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
    except Exception:
        return []


@st.cache_data(ttl=15, show_spinner=False)
def _recent_runs(limit: int = 10) -> list[dict[str, Any]]:
    try:
        with warehouse_ctx(readonly=True) as wh:
            return list(
                wh.query(f"""
                SELECT run_id, client_id, dataset_code, status, triggered_by,
                       started_at, ended_at, duration_ms, rows_gold, error_task
                  FROM {CONTROL_SCHEMA}.dataset_pipeline_runs
                 ORDER BY started_at DESC
                 LIMIT {int(limit)}
            """)
            )
    except Exception:
        return []


@st.cache_data(ttl=30, show_spinner=False)
def _recent_failed_runs(limit: int = 5) -> list[dict[str, Any]]:
    try:
        with warehouse_ctx(readonly=True) as wh:
            return list(
                wh.query(f"""
                SELECT run_id, client_id, dataset_code, error_task, error_message,
                       started_at, ended_at
                  FROM {CONTROL_SCHEMA}.dataset_pipeline_runs
                 WHERE status='FAILED'
                 ORDER BY started_at DESC
                 LIMIT {int(limit)}
            """)
            )
    except Exception:
        return []


@st.cache_data(ttl=30, show_spinner=False)
def _recent_drift(limit: int = 5) -> list[dict[str, Any]]:
    try:
        with warehouse_ctx(readonly=True) as wh:
            return list(
                wh.query(f"""
                SELECT detected_at, client_id, source_type AS dataset_code,
                       drift_type, severity, action_taken,
                       added_columns, removed_columns
                  FROM {CONTROL_SCHEMA}.schema_drift_log
                 ORDER BY detected_at DESC
                 LIMIT {int(limit)}
            """)
            )
    except Exception:
        return []


# ===========================================================================
# 1 — 🚨 Critical Alerts banner
# ===========================================================================
kpi = _kpis()
services = _service_health()
recent_drift = _recent_drift(limit=20)

critical_msgs: list[str] = []
warn_msgs: list[str] = []

down_svcs = [s for s in services if s.get("status") == "DOWN"]
warn_svcs = [s for s in services if s.get("status") == "WARN"]
if down_svcs:
    critical_msgs.append(
        f"❌ **{len(down_svcs)} service{'s' if len(down_svcs) != 1 else ''} DOWN**: "
        + ", ".join(s["name"] for s in down_svcs)
    )
if warn_svcs:
    warn_msgs.append(
        f"⚠️ **{len(warn_svcs)} service{'s' if len(warn_svcs) != 1 else ''} degraded**: "
        + ", ".join(s["name"] for s in warn_svcs)
    )
if kpi.get("runs_24h_failed", 0) > 0:
    critical_msgs.append(
        f"❌ **{kpi['runs_24h_failed']} pipeline run"
        f"{'s' if kpi['runs_24h_failed'] != 1 else ''} FAILED in last 24h**"
    )
fatal_drift = sum(1 for d in recent_drift if str(d.get("action_taken") or "").upper() == "HALTED")
if fatal_drift:
    critical_msgs.append(
        f"❌ **{fatal_drift} fatal schema-drift event"
        f"{'s' if fatal_drift != 1 else ''}** halted ingest "
        "— Bronze ingest blocked until contract is bumped"
    )
if kpi.get("drift_24h", 0) and not fatal_drift:
    warn_msgs.append(
        f"🌀 **{kpi['drift_24h']} schema-drift event"
        f"{'s' if kpi['drift_24h'] != 1 else ''} in last 24h** (non-fatal)"
    )
if kpi.get("pipelines_paused", 0) > 0:
    warn_msgs.append(
        f"⏸ **{kpi['pipelines_paused']} pipeline"
        f"{'s' if kpi['pipelines_paused'] != 1 else ''} PAUSED** — scheduler skipping"
    )

if critical_msgs:
    msg_html = "<br>".join(critical_msgs)
    st.markdown(
        f"<div class='ct-alert-crit'><strong>🚨 Critical alerts</strong><br>{msg_html}</div>",
        unsafe_allow_html=True,
    )
if warn_msgs:
    msg_html = "<br>".join(warn_msgs)
    st.markdown(
        f"<div class='ct-alert-warn'><strong>⚠️ Attention</strong><br>{msg_html}</div>",
        unsafe_allow_html=True,
    )
if not critical_msgs and not warn_msgs:
    st.markdown(
        "<div class='ct-alert-ok'>✅ <strong>All systems nominal.</strong> "
        "No failed runs, no pending drift, all services responsive.</div>",
        unsafe_allow_html=True,
    )


# ===========================================================================
# 2 — 🩺 System Health
# ===========================================================================
st.markdown("### 🩺 System Health")
st.caption("HTTP / Snowflake probes refreshed every 30 seconds.")

svc_cols = st.columns(len(services))
for i, s in enumerate(services):
    pill = (
        "ct-pill-ok"
        if s["status"] == "OK"
        else "ct-pill-warn"
        if s["status"] == "WARN"
        else "ct-pill-bad"
    )
    icon = "✅" if s["status"] == "OK" else "⚠️" if s["status"] == "WARN" else "❌"
    with svc_cols[i]:
        st.markdown(
            f"<div class='ct-svc-row'>"
            f"<div><div class='ct-svc-name'>{icon} {s['name']}</div>"
            f"<div class='ct-svc-detail'>{s['detail']}</div></div>"
            f"<span class='ct-pill {pill}'>{s['status']}</span>"
            f"</div>",
            unsafe_allow_html=True,
        )


# ===========================================================================
# 3 — 📊 Headline KPIs
# ===========================================================================
st.markdown("### 📊 Headline KPIs")


def _tile(label: str, value: str, sub: str = "") -> str:
    return (
        f"<div class='ct-tile'>"
        f"<div class='ct-tile-label'>{label}</div>"
        f"<div class='ct-tile-value'>{value}</div>"
        f"<div class='ct-tile-sub'>{sub}</div></div>"
    )


k1, k2, k3, k4, k5 = st.columns(5)
runs = kpi.get("runs_24h", 0)
runs_pass = kpi.get("runs_24h_success", 0)
pass_pct = (100.0 * runs_pass / runs) if runs else 0.0

k1.markdown(
    _tile(
        "Bronze datasets",
        f"{kpi.get('bronze_datasets', 0):,}",
        f"{kpi.get('datasets_active', 0)} datasets active",
    ),
    unsafe_allow_html=True,
)
k2.markdown(
    _tile(
        "Pipelines LIVE",
        f"{kpi.get('pipelines_live', 0):,}",
        f"{kpi.get('clients_onboarded', 0)} clients · "
        f"+{kpi.get('pipelines_paused', 0)} paused · "
        f"+{kpi.get('pipelines_draft', 0)} draft",
    ),
    unsafe_allow_html=True,
)
k3.markdown(
    _tile(
        "Runs (24h)",
        f"{runs:,}",
        f"{runs_pass:,} ✅ · {kpi.get('runs_24h_failed', 0):,} ❌ · "
        f"{kpi.get('runs_in_flight', 0):,} 🟡 in flight",
    ),
    unsafe_allow_html=True,
)
k4.markdown(
    _tile(
        "DQ pass rate (24h)",
        f"{pass_pct:.1f}%" if runs else "—",
        f"{kpi.get('dq_suites_live', 0)} suites LIVE · " f"{kpi.get('dq_suites_draft', 0)} draft",
    ),
    unsafe_allow_html=True,
)
k5.markdown(
    _tile(
        "AI spend (7d)",
        f"${kpi.get('ai_cost_usd_7d', 0):.3f}",
        f"{kpi.get('ai_tokens_7d', 0):,} tokens",
    ),
    unsafe_allow_html=True,
)


# ===========================================================================
# 4 — 🌐 Factory Pattern Coverage
# ===========================================================================
st.markdown("---")
st.markdown("### 🌐 Factory Pattern Coverage")
st.caption(
    "One row per active dataset.  DQ-suite pills show whether a LIVE suite "
    "exists for each layer.  Click into Pipeline Architect or DQ Suite "
    "Registry for instance-level operations."
)

cov = _factory_coverage()
if not cov:
    st.info(
        "No active datasets yet.  Deploy your first pipeline via "
        "**🤖 AI Architect** in Pipeline Architect."
    )
else:

    def _layer_pill(have: int, label: str) -> str:
        cls = "ct-pill-ok" if have else "ct-pill-none"
        glyph = "🟢" if have else "—"
        return f"<span class='ct-pill {cls}'>{glyph} {label}</span>"

    rows_html = []
    for r in cov:
        runs_24 = int(r.get("runs_24h") or 0)
        pass_24 = int(r.get("pass_24h") or 0)
        fail_24 = int(r.get("fail_24h") or 0)
        last_run = r.get("last_run")
        layers = " ".join(
            [
                _layer_pill(int(r.get("bronze_suite") or 0), "Bronze"),
                _layer_pill(int(r.get("silver_suite") or 0), "Silver"),
                _layer_pill(int(r.get("gold_suite") or 0), "Gold"),
            ]
        )
        drift_n = int(r.get("drift_7d") or 0)
        drift_html = f"<span class='ct-pill ct-pill-warn'>🌀 {drift_n}</span>" if drift_n else "—"
        runs_html = (
            (
                f"{runs_24}  "
                f"<span style='color:{GREEN}'>✅ {pass_24}</span>"
                + (f"  <span style='color:{RED}'>❌ {fail_24}</span>" if fail_24 else "")
            )
            if runs_24
            else "—"
        )
        rows_html.append(
            {
                "Dataset": f"<code>{r.get('dataset_code')}</code>",
                "Clients": f"{int(r.get('distinct_clients') or 0)} ({int(r.get('live_clients') or 0)} LIVE)",
                "DQ suites": layers,
                "Runs 24h": runs_html,
                "Drift 7d": drift_html,
                "Last run": str(last_run or "—")[:19],
            }
        )

    table_html = (
        "<table style='width:100%;border-collapse:collapse'>"
        "<thead><tr style='background:#f1f5f9'>"
        + "".join(
            f"<th style='padding:.55rem;text-align:left;font-size:.82rem;color:#475569'>{c}</th>"
            for c in rows_html[0].keys()
        )
        + "</tr></thead><tbody>"
    )
    for r in rows_html:
        table_html += "<tr style='border-bottom:1px solid #e2e8f0'>"
        for v in r.values():
            table_html += f"<td style='padding:.5rem;font-size:.85rem'>{v}</td>"
        table_html += "</tr>"
    table_html += "</tbody></table>"
    st.markdown(table_html, unsafe_allow_html=True)


# ===========================================================================
# 5 — 🔭 Live Activity Feed
# ===========================================================================
st.markdown("---")
st.markdown("### 🔭 Live Activity Feed")
st.caption("Most-recent operational events across the platform.")

feed1, feed2, feed3 = st.columns(3)

with feed1:
    runs_recent = _recent_runs(limit=10)
    st.markdown(f"**📺 Recent runs** _(showing {len(runs_recent)} of last 10)_")
    if not runs_recent:
        st.caption("_No runs yet._")
    for r in runs_recent:
        status = str(r.get("status") or "")
        icon = {"SUCCESS": "✅", "FAILED": "❌", "RUNNING": "🟡"}.get(status, "·")
        when = str(r.get("started_at") or "")[:19]
        rows = int(r.get("rows_gold") or 0)
        rows_part = f" · {rows:,} rows" if rows else ""
        err = r.get("error_task")
        err_part = f" (failed at <code>{err}</code>)" if err else ""
        st.markdown(
            f"<div class='ct-feed-card'>{icon} <strong>{r.get('client_id')}</strong> "
            f"/ <code>{r.get('dataset_code')}</code>{err_part}<br>"
            f"<span style='color:{GREY};font-size:.75rem'>"
            f"{when}{rows_part} · via {r.get('triggered_by') or '—'}</span></div>",
            unsafe_allow_html=True,
        )

with feed2:
    failed = _recent_failed_runs(limit=5)
    st.markdown(f"**❌ Recent failures** _(showing {len(failed)} of last 5)_")
    if not failed:
        st.caption("_No failed runs.  All green._")
    for r in failed:
        when = str(r.get("started_at") or "")[:19]
        err_msg = str(r.get("error_message") or "")[:140]
        st.markdown(
            f"<div class='ct-feed-card'>❌ <strong>{r.get('client_id')}</strong> "
            f"/ <code>{r.get('dataset_code')}</code> · failed at "
            f"<code>{r.get('error_task') or '—'}</code><br>"
            f"<span style='color:{GREY};font-size:.75rem'>{when}</span><br>"
            f"<span style='color:{RED};font-size:.78rem'>{err_msg}…</span></div>",
            unsafe_allow_html=True,
        )

with feed3:
    drift = _recent_drift(limit=5)
    st.markdown(f"**🌀 Recent drift events** _(showing {len(drift)} of last 5)_")
    if not drift:
        st.caption("_No drift detected._")
    for d in drift:
        when = str(d.get("detected_at") or "")[:19]
        sev = str(d.get("severity") or "")
        action = str(d.get("action_taken") or "")
        action_color = RED if action == "HALTED" else AMBER
        added = str(d.get("added_columns") or "")[:80]
        removed = str(d.get("removed_columns") or "")[:60]
        change = []
        if added and added != "[]":
            change.append(f"➕ {added}")
        if removed and removed != "[]":
            change.append(f"➖ {removed}")
        st.markdown(
            f"<div class='ct-feed-card'>🌀 <strong>{d.get('client_id')}</strong> "
            f"/ <code>{d.get('dataset_code')}</code> · "
            f"<span style='color:{action_color};font-weight:600'>{action}</span><br>"
            f"<span style='color:{GREY};font-size:.75rem'>"
            f"{when} · {sev} · {d.get('drift_type')}</span><br>"
            f"<span style='font-size:.78rem'>{' / '.join(change) if change else '_(metadata-only)_'}</span></div>",
            unsafe_allow_html=True,
        )


# ===========================================================================
# 6 — 🚀 Quick Actions
# ===========================================================================
st.markdown("---")
st.markdown("### 🚀 Quick Actions")
st.caption("Eight most-common operator workflows — one click each.")

q1, q2, q3, q4 = st.columns(4)
with q1:
    st.page_link("pages/14_Data_Model_Designer.py", label="🥇 Author / clone schemas", icon="🥇")
with q2:
    st.page_link("pages/13_Pipeline_Architect.py", label="🏛 Deploy pipeline (AI)", icon="🏛")
with q3:
    st.page_link("pages/8_DQ_AI_Architect.py", label="🧪 Author DQ suite (AI)", icon="🧪")
with q4:
    st.page_link("pages/15_Run_Monitor.py", label="📺 Watch a run", icon="📺")

q5, q6, q7, q8 = st.columns(4)
with q5:
    st.page_link("pages/9_Schema_Drift.py", label="🌀 Triage schema drift", icon="🌀")
with q6:
    st.page_link("pages/6_DQ_Suite_Registry.py", label="📚 DQ Suite Registry", icon="📚")
with q7:
    st.page_link("pages/3_Executive_Dashboard.py", label="📊 Executive Dashboard", icon="📊")
with q8:
    # External link to Grafana — st.page_link only supports local pages,
    # so render as a styled button-link.
    st.markdown(
        "<a href='http://localhost:3000' target='_blank' "
        "style='display:inline-block;background:#fff;border:1px solid #d4d4d8;"
        "border-radius:6px;padding:.55rem .85rem;font-weight:500;"
        "text-decoration:none;color:#0a1a3e;font-size:.95rem;width:100%;text-align:center'>"
        "📈 Open Grafana</a>",
        unsafe_allow_html=True,
    )
