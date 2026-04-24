"""DataLink Control Tower — single-page executive UI for the pipeline.

One URL (http://localhost:8000). Every step visible:
  - Drag-drop file upload → SFTP
  - "Trigger Bronze / Silver / Gold" buttons → Airflow REST API
  - Live row counts per medallion zone + operational DB targets
  - GX checkpoint status + last-agent activity
  - Deep-link grid to Airflow, pgAdmin, Adminer, Grafana, GX Docs, etc.
  - Synthetic breach button → Post-Val crew demo

Design: minimal dependencies, graceful degradation — if a backend is
unreachable the tile shows "unreachable" instead of crashing.
"""

from __future__ import annotations

import os
import tempfile
import traceback
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import pandas as pd
import streamlit as st

# =============================================================================
# CONFIG — the Control Tower runs inside a container and talks to siblings on
# the compose network by DNS name. Overridable via env vars.
# =============================================================================

AIRFLOW_URL = os.environ.get("DL_CT_AIRFLOW_URL", "http://airflow_webserver:8080")
AIRFLOW_USER = os.environ.get("DL_CT_AIRFLOW_USER", "admin")
AIRFLOW_PASS = os.environ.get("DL_CT_AIRFLOW_PASS", "admin_local_only")

SFTP_HOST = os.environ.get("DL_CT_SFTP_HOST", "sftp")
SFTP_PORT = int(os.environ.get("DL_CT_SFTP_PORT", "22"))
SFTP_USER = os.environ.get("DL_CT_SFTP_USER", "datalink")
SFTP_PASS = os.environ.get("DL_CT_SFTP_PASS", "datalink_local_only")

WAREHOUSE_PATH = os.environ.get("DL_CT_WAREHOUSE_PATH", "/opt/datalink/warehouse.duckdb")

# Phase 6 fix: bootstrap warehouse file + CONTROL schema before any
# read-only probe. Prevents "Cannot open database in read-only mode"
# stack trace on fresh `docker compose up` when warehouse.duckdb does
# not yet exist. See datalink/ui/_bootstrap.py.
from datalink.ui._bootstrap import ensure_warehouse_exists  # noqa: E402

ensure_warehouse_exists(WAREHOUSE_PATH)

PG_HOST = os.environ.get("DL_CT_PG_HOST", "postgres")
PG_PORT = int(os.environ.get("DL_CT_PG_PORT", "5432"))
PG_USER = os.environ.get("DL_CT_PG_USER", "datalink")
PG_PASS = os.environ.get("DL_CT_PG_PASS", "datalink_local_only")
PG_DB = os.environ.get("DL_CT_PG_DB", "datalink_um")

PG_REPLICA_HOST = os.environ.get("DL_CT_PG_REPLICA_HOST", "postgres_replica")
PG_REPLICA_DB = os.environ.get("DL_CT_PG_REPLICA_DB", "datalink_um_replica")

MSSQL_HOST = os.environ.get("DL_CT_MSSQL_HOST", "sqlserver")
MSSQL_PORT = int(os.environ.get("DL_CT_MSSQL_PORT", "1433"))
MSSQL_USER = os.environ.get("DL_CT_MSSQL_USER", "datalink")
MSSQL_PASS = os.environ.get("DL_CT_MSSQL_PASS", "datalink_local_only")
MSSQL_DB = os.environ.get("DL_CT_MSSQL_DB", "DataLinkUM")

# URLs as seen from the USER's Windows browser — all on 127.0.0.1:PORT.
USER_FACING = {
    "Airflow": "http://localhost:8088",
    "pgAdmin": "http://localhost:5050",
    "Adminer": "http://localhost:8081",
    "Grafana": "http://localhost:3000",
    "GX Data Docs": "http://localhost:8090",
    "Webhook Inbox": "http://localhost:9000",
    "Filebrowser": "http://localhost:8082",
    "Portainer": "https://localhost:9443",
    "Prometheus": "http://localhost:9090",
}


# =============================================================================
# STYLING
# =============================================================================

st.set_page_config(
    page_title="DataLink Control Tower",
    page_icon="🏛️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Shared sidebar nav — Dashboards + External Tools + Databases.
# Defined in datalink/ui/_nav.py so every page gets the same nav.
from datalink.ui._nav import render_sidebar  # noqa: E402

render_sidebar(active="Control Tower")

_NAVY = "#0a1a3e"
_GOLD = "#d4af37"

st.markdown(
    f"""
    <style>
      .block-container {{padding-top: 1.5rem;}}
      h1 {{color: {_NAVY}; border-bottom: 3px solid {_GOLD}; padding-bottom: .4rem;}}
      h2 {{color: {_NAVY}; margin-top: 1.5rem;}}
      .ct-tile {{background:#fff;border:1px solid #e8edf5;border-radius:6px;padding:.9rem 1rem;box-shadow:0 1px 2px rgba(0,0,0,.04)}}
      .ct-tile .label {{color:#4a5a7e;font-size:.75rem;text-transform:uppercase;letter-spacing:.05em}}
      .ct-tile .value {{color:{_NAVY};font-size:1.5rem;font-weight:700;margin-top:.2rem}}
      .ct-tile .sub   {{color:#4a5a7e;font-size:.85rem;margin-top:.2rem}}
      .ok  {{color:#047857;font-weight:600}}
      .warn{{color:#d97706;font-weight:600}}
      .err {{color:#b91c1c;font-weight:600}}
      .muted {{color:#8a9bbc}}
      .deeplink a {{text-decoration:none;display:block;padding:.6rem .9rem;background:{_NAVY};color:{_GOLD};border-radius:4px;text-align:center;font-weight:600;font-size:.9rem}}
      .deeplink a:hover {{background:{_GOLD};color:{_NAVY}}}
      .flow-arrow {{font-size:2rem;color:{_GOLD};text-align:center;line-height:1}}
    </style>
    """,
    unsafe_allow_html=True,
)


# =============================================================================
# DATA ACCESS — every backend probe is wrapped so UI degrades gracefully.
# =============================================================================


@dataclass
class ProbeResult:
    ok: bool
    value: Any = None
    error: str | None = None


@contextmanager
def _swallow(label: str) -> Any:
    """Catch-all: any backend exception becomes a ProbeResult(err), UI stays up."""
    try:
        yield
    except Exception as e:
        raise RuntimeError(f"{label}: {type(e).__name__}: {e}") from e


def _existing_schemas_upper() -> set[str]:
    """Return the set of warehouse schemas that actually exist, uppercased.

    This query is cached by ``datalink.ui._query``, so the whole page
    renders use at most ONE warehouse round-trip to learn the schema
    catalog — not N per-missing-table probes. Empty set on error.
    """
    from datalink.ui._query import query_silent

    df = query_silent(
        "SELECT schema_name FROM information_schema.schemata "
        "WHERE schema_name NOT IN ('information_schema','pg_catalog','main')"
    )
    if df.empty or "schema_name" not in df.columns:
        return set()
    return {str(s).upper() for s in df["schema_name"].tolist()}


def _duckdb_count(schema: str, table: str) -> ProbeResult:
    """Query the warehouse for a row count. Returns ProbeResult.

    Named ``_duckdb_count`` for backwards compatibility with existing
    callers, but routed through ``datalink.ui._query`` so the backend
    (DuckDB or Snowflake) is config-driven.

    Phase 7 perf: checks the cached schema catalog first. If ``schema``
    doesn't exist on the current warehouse, we skip the COUNT(*) entirely
    instead of asking the warehouse about a table we know isn't there.
    This eliminates 8+ failing round-trips per page render when the
    pipelines haven't been run on the current backend yet.
    """
    try:
        from datalink.ui._query import query_scalar

        if schema.upper() not in _existing_schemas_upper():
            return ProbeResult(ok=False, error=f"{schema} schema does not exist")

        val = query_scalar(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
        if val is None:
            return ProbeResult(ok=False, error=f"{schema}.{table} not found")
        return ProbeResult(ok=True, value=int(val))
    except Exception as e:
        return ProbeResult(ok=False, error=f"{type(e).__name__}: {e}")


def _pg_count(host: str, database: str, table: str, schema: str = "um") -> ProbeResult:
    try:
        import psycopg

        dsn = f"host={host} port=5432 dbname={database} user={PG_USER} password={PG_PASS} connect_timeout=3"
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
            row = cur.fetchone()
            return ProbeResult(ok=True, value=int(row[0]) if row else 0)
    except Exception as e:
        return ProbeResult(ok=False, error=f"{type(e).__name__}: {e}")


def _mssql_count(table: str, schema: str = "UM") -> ProbeResult:
    try:
        import pyodbc

        cn = pyodbc.connect(
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={MSSQL_HOST},{MSSQL_PORT};DATABASE={MSSQL_DB};"
            f"UID={MSSQL_USER};PWD={MSSQL_PASS};"
            f"TrustServerCertificate=yes;Encrypt=optional;",
            timeout=5,
        )
        cur = cn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM [{schema}].[{table}]")
        row = cur.fetchone()
        cn.close()
        return ProbeResult(ok=True, value=int(row[0]) if row else 0)
    except Exception as e:
        return ProbeResult(ok=False, error=f"{type(e).__name__}: {e}")


def _control_query(sql: str) -> ProbeResult:
    """Query the CONTROL.* audit tables through the configured backend."""
    try:
        from datalink.ui._query import query_silent

        df = query_silent(sql)
        if df.empty:
            # query_silent returns empty frame on any failure; surface as error.
            # NOTE: a genuinely-empty result set is indistinguishable from a
            # failure here; caller must treat empty as 'no data or error'.
            return ProbeResult(ok=True, value=df)
        return ProbeResult(ok=True, value=df)
    except Exception as e:
        return ProbeResult(ok=False, error=f"{type(e).__name__}: {e}")


# =============================================================================
# AIRFLOW REST API
# =============================================================================


def _airflow(path: str, method: str = "GET", json: dict[str, Any] | None = None) -> ProbeResult:
    try:
        with httpx.Client(
            base_url=AIRFLOW_URL,
            auth=(AIRFLOW_USER, AIRFLOW_PASS),
            timeout=10.0,
        ) as cli:
            r = cli.request(method, path, json=json)
            r.raise_for_status()
            return ProbeResult(ok=True, value=r.json() if r.content else {})
    except Exception as e:
        return ProbeResult(ok=False, error=f"{type(e).__name__}: {e}")


def _list_dag_runs(dag_id: str, limit: int = 3) -> ProbeResult:
    return _airflow(f"/api/v1/dags/{dag_id}/dagRuns?limit={limit}&order_by=-execution_date")


def _trigger_dag(dag_id: str, conf: dict[str, Any] | None = None) -> ProbeResult:
    body: dict[str, Any] = {"conf": conf or {}}
    return _airflow(f"/api/v1/dags/{dag_id}/dagRuns", method="POST", json=body)


# Client selector + sentinel logic lives in datalink/ui/_nav.py as of
# Phase-7 unification — one selector for the whole app, surfaced in the
# sidebar. Pages call ``require_client()`` from that module to get the
# active tenant or halt rendering if none is selected.


# =============================================================================
# SFTP UPLOAD
# =============================================================================


def _upload_to_sftp(filename: str, data: bytes) -> ProbeResult:
    try:
        import paramiko

        with tempfile.TemporaryDirectory() as tmp:
            local = Path(tmp) / filename
            local.write_bytes(data)
            transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
            transport.connect(username=SFTP_USER, password=SFTP_PASS)
            sftp = paramiko.SFTPClient.from_transport(transport)
            remote = f"/drop/{filename}"
            assert sftp is not None
            sftp.put(str(local), remote)
            sftp.close()
            transport.close()
            return ProbeResult(ok=True, value=remote)
    except Exception as e:
        return ProbeResult(
            ok=False, error=f"{type(e).__name__}: {e}\n{traceback.format_exc(limit=3)}"
        )


# =============================================================================
# UI HELPERS
# =============================================================================


def _tile(label: str, value: str, sub: str = "", color_class: str = "") -> str:
    value_html = (
        f'<div class="value {color_class}">{value}</div>'
        if value
        else '<div class="value muted">—</div>'
    )
    sub_html = f'<div class="sub">{sub}</div>' if sub else ""
    return f'<div class="ct-tile"><div class="label">{label}</div>{value_html}{sub_html}</div>'


def _state_color(state: str | None) -> str:
    if not state:
        return "muted"
    return {
        "success": "ok",
        "running": "warn",
        "queued": "warn",
        "failed": "err",
        "up_for_retry": "warn",
    }.get(state, "muted")


def _fmt_count(p: ProbeResult) -> tuple[str, str]:
    if p.ok:
        return f"{p.value:,}", "ok"
    return "N/A", "muted"


# =============================================================================
# MAIN PAGE
# =============================================================================


def main() -> None:
    # --- Header ---------------------------------------------------------------
    st.markdown(
        f"<h1>🏛️ <span style='color:{_NAVY}'>DataLink</span> "
        f"<span style='color:{_GOLD}'>Control Tower</span></h1>",
        unsafe_allow_html=True,
    )
    col_a, col_b = st.columns([3, 1])
    with col_a:
        st.markdown(
            '<p style="color:#4a5a7e;margin:0">Single-pane-of-glass for the '
            "EvokeConnectCare™ medallion pipeline — file landing → Bronze / Silver / Gold → "
            "SQL Server + PostgreSQL — with Great Expectations + CrewAI visible end-to-end.</p>",
            unsafe_allow_html=True,
        )
    with col_b:
        refreshed_at = datetime.now(UTC).strftime("%H:%M:%S UTC")
        st.markdown(
            f'<div style="text-align:right;color:#8a9bbc">Refreshed {refreshed_at}</div>',
            unsafe_allow_html=True,
        )
        if st.button("🔄 Refresh now", use_container_width=True):
            # Dashboards use @st.cache_data (30s TTL). "Refresh now" must
            # clear the cache so the next render hits the warehouse fresh.
            from datalink.ui._query import clear_query_cache

            clear_query_cache()
            st.rerun()

    # ========================================================================
    # CLIENT GATE — the sidebar owns the selector (_nav.render_sidebar). If
    # no real tenant is selected, halt rendering with a prompt. `sel` is
    # the resolved client going forward.
    # ========================================================================
    from datalink.ui._nav import require_client

    sel = require_client()

    # ========================================================================
    # FLOW DIAGRAM — live row counts per zone
    # ========================================================================
    # Phase 6: resolve per-tenant schema names so the tiles show the selected
    # client's data (BRONZE_AETNA, SILVER_silver_AETNA, ...). For client_id
    # 'default' these resolve to the un-suffixed Phase-5.x schemas.
    from datalink.tenancy import Layer, schema_for

    bronze_schema = schema_for(sel, Layer.BRONZE)
    silver_schema = schema_for(sel, Layer.SILVER_DV)
    gold_schema = schema_for(sel, Layer.GOLD_UM)

    st.markdown(f"## 🏥 Medallion Flow — **{sel}**")

    # Probe every zone. Ordering intentional: left→right = pipeline direction.
    p_sftp = _airflow("/api/v1/dags/bronze_ingest")  # proxy for "SFTP reachable"
    p_bronze_claims = _duckdb_count(bronze_schema, "RAW_CLAIMS")
    p_silver_sat = _duckdb_count(silver_schema, "sat_claim_details")
    p_gold_auth = _duckdb_count(gold_schema, "gold_patient_auth")
    p_mssql = _mssql_count("patient_auth")
    p_pg = _pg_count(PG_HOST, PG_DB, "patient_auth")
    p_pg_replica = _pg_count(PG_REPLICA_HOST, PG_REPLICA_DB, "patient_auth")

    zones = st.columns([1, 0.15, 1, 0.15, 1, 0.15, 1, 0.15, 1])

    with zones[0]:
        sftp_val, sftp_cls = ("up", "ok") if p_sftp.ok else ("unreachable", "err")
        st.markdown(
            _tile("📁 SFTP DROP ZONE", sftp_val, "drop files here", sftp_cls),
            unsafe_allow_html=True,
        )
    with zones[1]:
        st.markdown('<div class="flow-arrow">→</div>', unsafe_allow_html=True)
    with zones[2]:
        v, cls = _fmt_count(p_bronze_claims)
        st.markdown(
            _tile("🥉 BRONZE (raw)", v, f"{bronze_schema}.RAW_CLAIMS", cls),
            unsafe_allow_html=True,
        )
    with zones[3]:
        st.markdown('<div class="flow-arrow">→</div>', unsafe_allow_html=True)
    with zones[4]:
        v, cls = _fmt_count(p_silver_sat)
        st.markdown(
            _tile("🥈 SILVER (DV 2.0)", v, f"{silver_schema}.sat_claim_details", cls),
            unsafe_allow_html=True,
        )
    with zones[5]:
        st.markdown('<div class="flow-arrow">→</div>', unsafe_allow_html=True)
    with zones[6]:
        v, cls = _fmt_count(p_gold_auth)
        st.markdown(
            _tile("🥇 GOLD UM", v, f"{gold_schema}.gold_patient_auth", cls),
            unsafe_allow_html=True,
        )
    with zones[7]:
        st.markdown('<div class="flow-arrow">→</div>', unsafe_allow_html=True)
    with zones[8]:
        rows = []
        for name, p in [("SQL Server", p_mssql), ("Postgres", p_pg), ("PG Replica", p_pg_replica)]:
            if p.ok:
                rows.append(f'<span class="ok">{name}: {p.value:,}</span>')
            else:
                rows.append(f'<span class="muted">{name}: —</span>')
        inner = "<br>".join(rows)
        st.markdown(
            f'<div class="ct-tile"><div class="label">🏛️ OPERATIONAL</div>'
            f'<div class="value" style="font-size:.95rem;line-height:1.6">{inner}</div>'
            '<div class="sub">prod targets</div></div>',
            unsafe_allow_html=True,
        )

    # ========================================================================
    # PER-SOURCE BRONZE BREAKDOWN — 3 tiles, one per source_type
    # ========================================================================
    # Phase 6: shows CLAIMS / MEMBERSHIP / PROVIDER row counts for the
    # selected client so you can see exactly what landed.
    st.markdown(f"### 📦 Bronze by source — **{sel}**")
    p_raw_claims = _duckdb_count(bronze_schema, "RAW_CLAIMS")
    p_raw_membership = _duckdb_count(bronze_schema, "RAW_MEMBERSHIP")
    p_raw_provider = _duckdb_count(bronze_schema, "RAW_PROVIDER")

    src_cols = st.columns(3)
    with src_cols[0]:
        v, cls = _fmt_count(p_raw_claims)
        st.markdown(
            _tile("📋 CLAIMS", v, f"{bronze_schema}.RAW_CLAIMS", cls),
            unsafe_allow_html=True,
        )
    with src_cols[1]:
        v, cls = _fmt_count(p_raw_membership)
        st.markdown(
            _tile("👤 MEMBERSHIP", v, f"{bronze_schema}.RAW_MEMBERSHIP", cls),
            unsafe_allow_html=True,
        )
    with src_cols[2]:
        v, cls = _fmt_count(p_raw_provider)
        st.markdown(
            _tile("🏥 PROVIDER", v, f"{bronze_schema}.RAW_PROVIDER", cls),
            unsafe_allow_html=True,
        )

    # ========================================================================
    # GX + AGENT STATUS
    # ========================================================================
    st.markdown("## 🛡️ Quality Gates  &  🤖 Agent Activity")

    gx_col, agent_col = st.columns([1, 1])

    with gx_col:
        st.markdown("**Last Great Expectations Checkpoint Results**")
        gx_df = _control_query(
            """
            SELECT checkpoint_name,
                   COUNT(*)                            AS expectations,
                   SUM(CASE WHEN success THEN 1 ELSE 0 END) AS passed,
                   ROUND(AVG(unexpected_pct),2)         AS avg_unexpected_pct,
                   MAX(ts)                             AS last_ts
            FROM CONTROL.gx_validation_results
            WHERE ts >= CURRENT_TIMESTAMP - INTERVAL 24 HOUR
            GROUP BY checkpoint_name
            ORDER BY checkpoint_name
            """
        )
        if gx_df.ok and isinstance(gx_df.value, pd.DataFrame) and not gx_df.value.empty:
            st.dataframe(gx_df.value, use_container_width=True, hide_index=True)
        else:
            reason = gx_df.error if gx_df.error else "No checkpoint runs yet — trigger a DAG."
            st.info(reason)
        st.link_button("Open Great Expectations Data Docs ↗", USER_FACING["GX Data Docs"])

    with agent_col:
        st.markdown("**Recent CrewAI Agent Invocations**")
        agent_df = _control_query(
            """
            SELECT agent_name, crew_name,
                   duration_ms,
                   SUBSTR(output_preview, 1, 60) AS output_preview,
                   ts
            FROM CONTROL.agent_reasoning_log
            WHERE ts >= CURRENT_TIMESTAMP - INTERVAL 24 HOUR
            ORDER BY ts DESC
            LIMIT 12
            """
        )
        if agent_df.ok and isinstance(agent_df.value, pd.DataFrame) and not agent_df.value.empty:
            st.dataframe(agent_df.value, use_container_width=True, hide_index=True)
        else:
            reason = (
                agent_df.error
                if agent_df.error
                else (
                    "No agent activity yet. Pre-Val agents fire on every GX checkpoint; "
                    "Post-Val fires on a BREACH."
                )
            )
            st.info(reason)
        st.link_button("Webhook Inbox (Reporting agent output) ↗", USER_FACING["Webhook Inbox"])

    # ========================================================================
    # ACTIONS — upload + trigger + inject-breach
    # ========================================================================
    st.markdown("## 🎬 Actions")

    a1, a2, a3, a4, a5 = st.columns([2, 1, 1, 1, 1])

    with a1:
        st.markdown("**Upload a CSV to the SFTP drop zone**")
        uploaded = st.file_uploader(
            "Drop a Claims / Membership / Provider CSV here",
            type=["csv"],
            label_visibility="collapsed",
        )
        if uploaded is not None and st.button(
            "📤 Send to SFTP", type="primary", use_container_width=True
        ):
            with st.spinner(f"Uploading {uploaded.name}..."):
                r = _upload_to_sftp(uploaded.name, uploaded.getvalue())
                if r.ok:
                    st.success(f"✓ Landed at {r.value}")
                else:
                    st.error(r.error)

    # `sel` is guaranteed a real client here (sentinel would have halted
    # rendering above via st.stop()). Use it directly instead of refetching
    # from session_state with a now-impossible default fallback.
    selected_client = sel

    def _trigger_ui(dag_id: str, label: str, help_text: str) -> None:
        clicked = st.button(label, use_container_width=True, help=help_text)
        if clicked:
            # Phase 5.8: pass the client_id to the DAG so the task reads
            # the LIVE suite for (client_id, checkpoint_name).
            r = _trigger_dag(dag_id, conf={"client_id": selected_client})
            if r.ok:
                run_id = r.value.get("dag_run_id") if isinstance(r.value, dict) else "triggered"
                st.success(f"Triggered {dag_id} for client `{selected_client}`\n\n`{run_id}`")
                st.markdown(f"[Watch in Airflow ↗]({USER_FACING['Airflow']}/dags/{dag_id}/grid)")
            else:
                st.error(f"Trigger failed: {r.error}")

    with a2:
        st.markdown("**Bronze**")
        _trigger_ui(
            "bronze_ingest", "▶ Run Bronze", "SFTP → DuckDB BRONZE.* + CP1 GX + Pre-Val crew"
        )
    with a3:
        st.markdown("**Silver**")
        _trigger_ui("silver_transform", "▶ Run Silver", "dbt DV 2.0 build + CP2 GX + Pre-Val crew")
    with a4:
        st.markdown("**Gold**")
        _trigger_ui(
            "gold_um_push",
            "▶ Run Gold",
            "dbt UM build + CP3 GX + router push to SQL Server + PG x2",
        )
    with a5:
        st.markdown("**BREACH demo**")
        if st.button(
            "⚠️ Inject synthetic BREACH",
            use_container_width=True,
            help="Triggers a CP2 check with forced-fail config — Post-Val crew fires, Reporting agent hits webhook",
        ):
            # Runs silver pipeline with a conf flag the suite reads to inject failures.
            r = _trigger_dag(
                "silver_transform",
                conf={"inject_breach": True, "client_id": selected_client},
            )
            if r.ok:
                st.warning(
                    "Synthetic breach triggered. Watch Webhook Inbox for Reporting agent delivery."
                )
            else:
                st.error(f"Trigger failed: {r.error}")

    # ========================================================================
    # RECENT DAG RUNS — compact status row per DAG
    # ========================================================================
    st.markdown("## 📊 Recent DAG Runs")
    dag_cols = st.columns(3)
    for col, dag_id in zip(
        dag_cols, ["bronze_ingest", "silver_transform", "gold_um_push"], strict=False
    ):
        with col:
            runs = _list_dag_runs(dag_id, limit=3)
            st.markdown(f"**{dag_id}**")
            if not runs.ok:
                st.markdown(f'<span class="err">{runs.error}</span>', unsafe_allow_html=True)
                continue
            raw_run_list = runs.value.get("dag_runs", []) if isinstance(runs.value, dict) else []
            if not raw_run_list:
                st.markdown(
                    '<span class="muted">No runs yet. Click ▶ above.</span>', unsafe_allow_html=True
                )
                continue
            rows = []
            for raw in raw_run_list:
                run_record: dict[str, Any] = raw if isinstance(raw, dict) else {}
                state = run_record.get("state")
                exec_date = (run_record.get("execution_date") or "")[:19].replace("T", " ")
                cls = _state_color(state)
                rows.append(
                    f'<tr><td style="font-family:ui-monospace">{exec_date}</td>'
                    f'<td><span class="{cls}">{state or "?"}</span></td></tr>'
                )
            html = (
                '<table style="width:100%;font-size:.85rem"><tbody>'
                + "".join(rows)
                + "</tbody></table>"
            )
            st.markdown(html, unsafe_allow_html=True)

    # ========================================================================
    # DEEP LINKS — one-click to every ops tool
    # ========================================================================
    st.markdown("## 🔗 Deep Links (ops team)")
    link_cols = st.columns(len(USER_FACING))
    for col, (name, url) in zip(link_cols, USER_FACING.items(), strict=False):
        with col:
            st.markdown(
                f'<div class="deeplink"><a href="{url}" target="_blank">{name}</a></div>',
                unsafe_allow_html=True,
            )

    # ========================================================================
    # FOOTER
    # ========================================================================
    st.markdown("---")
    st.markdown(
        '<p style="color:#8a9bbc;text-align:center;font-size:.85rem">'
        "<em>Prepared by Team DataLink · EvokeConnectCare™ · "
        "Auto-refresh: manually via 🔄 top-right (or browser F5).</em>"
        "</p>",
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
