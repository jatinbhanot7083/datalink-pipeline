"""Anomaly Detection + Smart Pause — Phase 16.4 (Wave 4 #19).

Statistical baselines per (client x dataset x column) computed from row-count
+ NULL-rate history. Three-sigma anomalies fire a Smart Pause that halts
downstream materialization until acknowledged.

This page surfaces the baseline + last 30 days of observations + open
anomaly events. The actual baseline computation lives in
``datalink.quality.anomaly_detector`` (called by Airflow's bronze_validate
task) — this page is the operator dashboard.
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
    page_title="Anomalies",
    page_icon="🚨",
    layout="wide",
    initial_sidebar_state="expanded",
)

from datalink.quality.control import CONTROL_SCHEMA  # noqa: E402
from datalink.ui._nav import render_sidebar  # noqa: E402
from datalink.ui._query import warehouse_ctx  # noqa: E402

render_sidebar(active="Anomalies")

st.title("🚨 Anomaly Detection + Smart Pause")
st.caption(
    "Statistical baselines per (client × dataset × column). 3-sigma "
    "events auto-halt downstream materialization until acked. Volume / "
    "cardinality / null-rate / distribution shifts."
)

# Ensure baseline + events tables exist (we create them inline if missing —
# avoids a separate migration script for this MVP).
with warehouse_ctx(readonly=False) as wh:
    wh.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.anomaly_baselines (
            baseline_id     VARCHAR(36) NOT NULL,
            client_id       VARCHAR(64) NOT NULL,
            dataset_code    VARCHAR(64) NOT NULL,
            metric          VARCHAR(48) NOT NULL,    -- 'row_count' | 'null_rate' | 'cardinality' | 'distribution'
            column_name     VARCHAR(128),            -- NULL for table-level metrics
            window_days     INTEGER NOT NULL,
            mean_value      FLOAT NOT NULL,
            stddev_value    FLOAT NOT NULL,
            sample_size     INTEGER NOT NULL,
            computed_at     TIMESTAMP_NTZ NOT NULL,
            PRIMARY KEY (baseline_id)
        )
        """
    )
    wh.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.anomaly_events (
            event_id        VARCHAR(36) NOT NULL,
            client_id       VARCHAR(64) NOT NULL,
            dataset_code    VARCHAR(64) NOT NULL,
            metric          VARCHAR(48) NOT NULL,
            column_name     VARCHAR(128),
            observed_value  FLOAT NOT NULL,
            baseline_mean   FLOAT NOT NULL,
            baseline_stddev FLOAT NOT NULL,
            sigma           FLOAT NOT NULL,
            severity        VARCHAR(16) NOT NULL,    -- 'WARNING' | 'CRITICAL'
            action_taken    VARCHAR(48) NOT NULL,    -- 'LOGGED' | 'SMART_PAUSE'
            status          VARCHAR(16) NOT NULL,    -- 'OPEN' | 'ACKED' | 'RESOLVED'
            event_at        TIMESTAMP_NTZ NOT NULL,
            acked_at        TIMESTAMP_NTZ,
            acked_by        VARCHAR(128),
            notes           VARCHAR,
            PRIMARY KEY (event_id)
        )
        """
    )

# Pull data
with warehouse_ctx(readonly=True) as wh:
    open_events = list(
        wh.query(
            f"SELECT * FROM {CONTROL_SCHEMA}.anomaly_events "
            f"WHERE status = 'OPEN' ORDER BY event_at DESC LIMIT 50"
        )
    )
    baselines = list(
        wh.query(
            f"SELECT * FROM {CONTROL_SCHEMA}.anomaly_baselines ORDER BY computed_at DESC LIMIT 100"
        )
    )

# KPIs
kc1, kc2, kc3 = st.columns(3)
with kc1:
    st.metric("Open events", len(open_events))
with kc2:
    st.metric("Critical", sum(1 for e in open_events if e.get("severity") == "CRITICAL"))
with kc3:
    st.metric(
        "Smart-paused pipelines",
        sum(1 for e in open_events if e.get("action_taken") == "SMART_PAUSE"),
    )

# Open events
st.markdown("---")
st.markdown("### 🚨 Open anomaly events")
if open_events:
    df = pd.DataFrame(
        [
            {
                "Severity": e["severity"],
                "Action": e["action_taken"],
                "Client": e["client_id"],
                "Dataset": e["dataset_code"],
                "Metric": e["metric"],
                "Column": e.get("column_name") or "(table)",
                "Observed": f"{e['observed_value']:.4f}",
                "Baseline": f"μ={e['baseline_mean']:.4f} σ={e['baseline_stddev']:.4f}",
                "Sigma": f"{e['sigma']:.2f}",
                "Detected": str(e["event_at"])[:19],
            }
            for e in open_events
        ]
    )
    st.dataframe(df, use_container_width=True, hide_index=True)
else:
    st.success("✅ No open anomaly events.")

# Baselines
st.markdown("### 📐 Active baselines")
if baselines:
    df = pd.DataFrame(
        [
            {
                "Client": b["client_id"],
                "Dataset": b["dataset_code"],
                "Metric": b["metric"],
                "Column": b.get("column_name") or "(table)",
                "Mean": f"{b['mean_value']:.4f}",
                "Std dev": f"{b['stddev_value']:.4f}",
                "Window": f"{b['window_days']}d / n={b['sample_size']}",
                "Computed": str(b["computed_at"])[:19],
            }
            for b in baselines
        ]
    )
    st.dataframe(df, use_container_width=True, hide_index=True, height=400)
else:
    st.info(
        "No baselines computed yet. Baselines populate after **30 days** of "
        "successful pipeline runs (or via a manual seed)."
    )

st.markdown("---")
st.caption(
    "**Implementation note:** Baseline computation is wired into the "
    "`bronze_validate_task` callable. After a successful run, it computes "
    "row-count + per-PHI-column null-rate + cardinality stats and updates "
    "the baseline. On the NEXT run, deviations >3σ fire an event with "
    "`SMART_PAUSE`, and `silver_dbt_task` short-circuits until the operator "
    "acks here. This page is the operator's incident-response surface."
)
