"""Run Monitor — Phase 16.1 (Wave 1 Item 4).

Live status board for the active client + dataset's pipeline:

  * Latest run state (queued / running / success / failed)
  * Per-task progress card with live state pills
  * Task log deep-links to Airflow UI
  * Snowflake row counts post-run (Bronze / Silver / Gold) — proves data flowed
  * Recent run history (last 10 runs)

Operator never has to leave Streamlit to know what's happening.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

st.set_page_config(
    page_title="Run Monitor",
    page_icon="📺",
    layout="wide",
    initial_sidebar_state="expanded",
)

from datalink.agents.pipeline_architect import list_client_instances  # noqa: E402
from datalink.orchestration import airflow_ops  # noqa: E402
from datalink.ui._nav import render_sidebar, require_client  # noqa: E402
from datalink.ui._query import warehouse_ctx  # noqa: E402

render_sidebar(active="Run Monitor")

selected_client = require_client()
# Phase 16.6 — None means "All clients". Page renders cross-tenant view.
st.markdown(
    """
    <style>
      .rm-card {background:#fff;border:1px solid #d4d4d8;border-radius:8px;
                padding:1rem 1.2rem;margin:.5rem 0;
                box-shadow:0 1px 3px rgba(0,0,0,.04);}
      .rm-state {display:inline-block;padding:.2rem .65rem;border-radius:14px;
                 font-size:.78rem;font-weight:700;letter-spacing:.04em;
                 text-transform:uppercase;}
      .rm-state-success   {background:#dcfce7;color:#166534;}
      .rm-state-running   {background:#dbeafe;color:#1e40af;}
      .rm-state-queued    {background:#fef3c7;color:#92400e;}
      .rm-state-failed    {background:#fee2e2;color:#991b1b;}
      .rm-state-up_for_retry {background:#fef3c7;color:#92400e;}
      .rm-state-skipped   {background:#f3f4f6;color:#4b5563;}
      .rm-state-none      {background:#f3f4f6;color:#9ca3af;}
      .rm-state-removed   {background:#f3f4f6;color:#9ca3af;}
      .rm-row-count {font-size:1.6rem;font-weight:700;color:#0a1a3e;}
      .rm-row-label {font-size:.78rem;color:#64748b;text-transform:uppercase;
                     letter-spacing:.05em;font-weight:600;}
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("📺 Run Monitor")
st.caption(
    "Live state of the latest pipeline run for the active client + dataset. "
    "Auto-refreshes every 5 seconds while a run is active."
)


# ---------- Pick instance -----------------------------------------------------
# Phase 16.6 — when selected_client=None, show ALL clients' LIVE instances.
# Otherwise scope to the picked tenant.
with warehouse_ctx(readonly=True) as wh:
    instances = [
        i
        for i in list_client_instances(wh, client_id=selected_client) or []
        if i.get("status") == "LIVE"
    ]

if not instances:
    if selected_client is None:
        st.info(
            "🌐 **No LIVE pipelines deployed yet anywhere.** Open "
            "**Pipeline Architect**, pick a client, deploy a pipeline."
        )
    else:
        st.info(
            f"No LIVE pipeline instances for `{selected_client}`. "
            "Deploy one in **Pipeline Architect** first."
        )
    st.page_link(
        "pages/13_Pipeline_Architect.py",
        label="→ Open Pipeline Architect",
        icon="🏛",
    )
    st.stop()

# When no client selected, the picker shows "client / dataset" pairs
# across all tenants. When client selected, just dataset codes.
_qp_ds = st.query_params.get("dataset")

if selected_client is None:
    # Multi-tenant picker
    pair_labels = [f"{i.get('client_id')} / {i.get('dataset_code')}" for i in instances]
    default_idx = 0
    if _qp_ds:
        try:
            default_idx = next(
                idx for idx, i in enumerate(instances) if str(i.get("dataset_code")) == _qp_ds
            )
        except StopIteration:
            default_idx = 0
    sel_idx = st.selectbox(
        "Pipeline (client / dataset)",
        options=range(len(pair_labels)),
        format_func=lambda i: pair_labels[i],
        index=default_idx,
    )
    instance = instances[sel_idx]
    selected_dataset_code = str(instance.get("dataset_code"))
    effective_client = str(instance.get("client_id"))
else:
    ds_codes = [str(i.get("dataset_code")) for i in instances]
    default_idx = ds_codes.index(_qp_ds) if _qp_ds in ds_codes else 0
    selected_dataset_code = st.selectbox(
        "Dataset",
        options=ds_codes,
        index=default_idx,
    )
    if st.query_params.get("dataset") != selected_dataset_code:
        st.query_params["dataset"] = selected_dataset_code
    instance = next(i for i in instances if i.get("dataset_code") == selected_dataset_code)
    effective_client = selected_client

dag_id = f"{effective_client.lower()}_{selected_dataset_code.lower()}_pipeline"

# Pipeline target tables (for row-count cards)
bronze_table = f"{instance.get('bronze_schema')}.{instance.get('bronze_table')}"
silver_table = f"{instance.get('silver_schema')}.{instance.get('silver_table')}"
gold_table = f"{instance.get('gold_schema')}.{instance.get('gold_table')}"

st.markdown(
    f"""
    <div class="rm-card">
      <div style="font-size:.85rem;color:#64748b;letter-spacing:.05em;
                  text-transform:uppercase;font-weight:600;margin-bottom:.4rem;">
        Pipeline
      </div>
      <div style="font-size:1.05rem;font-weight:700;color:#0a1a3e;">
        {effective_client} / {selected_dataset_code} → DAG <code>{dag_id}</code>
      </div>
      <div style="margin-top:.4rem;font-size:.82rem;color:#475569;">
        🥉 <code>{bronze_table}</code>
        &nbsp;→&nbsp; 🥈 <code>{silver_table}</code>
        &nbsp;→&nbsp; 🥇 <code>{gold_table}</code>
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)

# ---------- Latest run --------------------------------------------------------
st.markdown("### 🏃 Latest run")

acol1, acol2, acol3 = st.columns([1, 1, 5])
with acol1:
    refresh_clicked = st.button("🔄 Refresh now", use_container_width=True)
with acol2:
    auto_refresh = st.toggle("Auto-refresh (5s)", value=False)
with acol3:
    if instance:
        st.page_link(
            f"http://localhost:8088/dags/{dag_id}/grid",
            label="↗ Open in Airflow UI",
            icon="🛰",
        )

try:
    latest = airflow_ops.get_latest_run(dag_id)
except Exception as exc:
    st.warning(f"Couldn't reach Airflow API: {exc}")
    latest = None

if latest is None:
    st.info(
        f"No runs for DAG `{dag_id}` yet. Trigger one from **Pipeline Architect** → "
        "🚀 Run pipeline → ▶️ Trigger DAG now."
    )
    st.markdown(
        f'<a href="/Pipeline_Architect?client={effective_client}&dataset='
        f'{selected_dataset_code}" target="_self">'
        f"🏛 → Open Pipeline Architect</a>",
        unsafe_allow_html=True,
    )
else:
    state = str(latest.get("state") or "none")
    state_class = f"rm-state rm-state-{state}"
    run_id = str(latest.get("dag_run_id") or "")
    started = str(latest.get("start_date") or "—")
    ended = str(latest.get("end_date") or "—")
    duration = "—"
    try:
        if latest.get("start_date") and latest.get("end_date"):
            from datetime import datetime

            sd = datetime.fromisoformat(str(latest["start_date"]).replace("Z", "+00:00"))
            ed = datetime.fromisoformat(str(latest["end_date"]).replace("Z", "+00:00"))
            duration = f"{(ed - sd).total_seconds():.1f}s"
    except Exception:
        pass

    st.markdown(
        f"""
        <div class="rm-card">
          <span class="{state_class}">{state}</span>
          &nbsp;<code>{run_id[:40]}</code><br>
          <div style="margin-top:.5rem;font-size:.85rem;color:#475569;">
            Started: <code>{started}</code>
            &nbsp;·&nbsp; Ended: <code>{ended}</code>
            &nbsp;·&nbsp; Duration: <code>{duration}</code>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # Per-task progress
    st.markdown("#### 🔧 Task progress")
    try:
        tasks = airflow_ops.get_task_states(dag_id, run_id)
    except Exception as exc:
        tasks = []
        st.warning(f"Task states fetch failed: {exc}")

    if tasks:
        task_rows = []
        for t in tasks:
            tstate = str(t.get("state") or "none")
            log_url = airflow_ops.get_task_log_url(
                dag_id=dag_id,
                run_id=run_id,
                task_id=str(t.get("task_id") or ""),
                try_number=int(t.get("try_number") or 1),
            )
            task_rows.append(
                {
                    "Task": t.get("task_id"),
                    "State": tstate,
                    "Try": t.get("try_number"),
                    "Started": str(t.get("start_date") or "—")[:19],
                    "Ended": str(t.get("end_date") or "—")[:19],
                    "Duration (s)": (
                        f"{float(t.get('duration')):.2f}" if t.get("duration") else "—"
                    ),
                    "Log": f"[↗ logs]({log_url})",
                }
            )
        st.dataframe(
            pd.DataFrame(task_rows),
            use_container_width=True,
            hide_index=True,
        )

    # Snowflake row counts — show only when run finished or is still going
    st.markdown("#### 📊 Row counts in Snowflake (post-run)")
    rc1, rc2, rc3 = st.columns(3)

    def _row_count(fq_table: str) -> int | str:
        try:
            with warehouse_ctx(readonly=True) as wh2:
                rows = list(wh2.query(f"SELECT COUNT(*) AS c FROM {fq_table}"))
                return int(rows[0]["c"]) if rows else 0
        except Exception as exc:
            return f"err: {str(exc)[:30]}"

    bronze_n = _row_count(bronze_table)
    silver_n = _row_count(silver_table)
    gold_n = _row_count(gold_table)

    for col, label, n, table_fq in [
        (rc1, "🥉 Bronze", bronze_n, bronze_table),
        (rc2, "🥈 Silver", silver_n, silver_table),
        (rc3, "🥇 Gold", gold_n, gold_table),
    ]:
        with col:
            st.markdown(
                f"""
                <div class="rm-card">
                  <div class="rm-row-label">{label}</div>
                  <div class="rm-row-count">{n if isinstance(n, int) else "—"}</div>
                  <div style="font-size:.7rem;color:#94a3b8;">{table_fq}</div>
                  {"" if isinstance(n, int) else f'<div style="font-size:.7rem;color:#dc2626;margin-top:.3rem;">{n}</div>'}
                </div>
                """,
                unsafe_allow_html=True,
            )


# ---------- Recent runs history ----------------------------------------------
st.markdown("### 📜 Recent runs")
try:
    history = airflow_ops.list_runs(dag_id, limit=10)
except Exception as exc:
    history = []
    st.warning(f"Couldn't fetch history: {exc}")

if history:
    hrows: list[dict[str, Any]] = []
    for r in history:
        hrows.append(
            {
                "State": r.get("state"),
                "Run ID": str(r.get("dag_run_id") or "")[:40],
                "Type": r.get("run_type"),
                "Logical date": str(r.get("logical_date") or "")[:19],
                "Started": str(r.get("start_date") or "")[:19],
                "Ended": str(r.get("end_date") or "")[:19],
                "Triggered by": (r.get("conf") or {}).get("triggered_by_ui", "—"),
            }
        )
    st.dataframe(pd.DataFrame(hrows), use_container_width=True, hide_index=True)
else:
    st.caption("No run history yet.")


# Auto-refresh: rerun every 5 seconds when toggle is on. Streamlit-native pattern.
if auto_refresh and latest and latest.get("state") in ("queued", "running"):
    import time

    time.sleep(5)
    st.rerun()
elif refresh_clicked:
    st.rerun()
