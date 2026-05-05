"""Slack / Teams ChatOps — Phase 16.4 (Wave 4 #20).

Configuration + testing surface for outbound webhooks. Operators set
SLACK_WEBHOOK_URL / TEAMS_WEBHOOK_URL in .env, then test from this page.
Live wiring (DAG failures → Slack post, anomaly events → Teams card)
happens automatically via Airflow callbacks + anomaly-event listener.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

st.set_page_config(
    page_title="ChatOps",
    page_icon="💬",
    layout="wide",
    initial_sidebar_state="expanded",
)

from datalink.notifications import chatops  # noqa: E402
from datalink.ui._nav import render_sidebar  # noqa: E402

render_sidebar(active="ChatOps")

st.title("💬 Slack / Teams ChatOps")
st.caption(
    "Outbound notifications for DAG failures, anomalies, proposals pending "
    "review, drift events. Two-way actions (approve in-Slack) wire via "
    "Block Kit interactivity — see configuration notes below."
)

# Configuration status
slack_ok = bool(os.environ.get("SLACK_WEBHOOK_URL"))
teams_ok = bool(os.environ.get("TEAMS_WEBHOOK_URL"))

c1, c2 = st.columns(2)
with c1:
    if slack_ok:
        st.success("✅ **Slack webhook**: configured")
    else:
        st.warning("⚠️ **Slack webhook**: NOT configured")
        st.caption(
            "Set `SLACK_WEBHOOK_URL` in `.env`. "
            "Get one from your Slack workspace → Apps → Incoming Webhooks."
        )
with c2:
    if teams_ok:
        st.success("✅ **Teams webhook**: configured")
    else:
        st.warning("⚠️ **Teams webhook**: NOT configured")
        st.caption(
            "Set `TEAMS_WEBHOOK_URL` in `.env`. In Teams: channel → Connectors → Incoming Webhook."
        )

st.markdown("---")
st.markdown("### 🧪 Send a test message")
if st.button(
    "📤 Send connection test to all enabled targets",
    type="primary",
    disabled=not (slack_ok or teams_ok),
):
    results = chatops.test_connection()
    for r in results:
        if r["status"] == "ok":
            st.success(f"✅ {r['target']}: {r['detail']}")
        elif r["status"] == "skip":
            st.info(f"⏭ {r['target']}: {r['detail']}")
        else:
            st.error(f"❌ {r['target']}: {r['detail']}")

# Custom message sender
st.markdown("---")
st.markdown("### ✉️ Send a custom message")
custom_msg = st.text_area("Message", placeholder="Test from DataLink Control Tower")
if st.button("Send custom message", disabled=not custom_msg or not (slack_ok or teams_ok)):
    results = chatops.post_message(custom_msg, title="DataLink — Custom")
    for r in results:
        if r["status"] == "ok":
            st.success(f"✅ {r['target']}: {r['detail']}")
        elif r["status"] == "skip":
            st.info(f"⏭ {r['target']}: {r['detail']}")
        else:
            st.error(f"❌ {r['target']}: {r['detail']}")

st.markdown("---")
st.markdown("### 🔌 What gets auto-posted")
auto_events = pd.DataFrame(
    [
        {
            "Event": "DAG run failed",
            "Target": "Slack + Teams",
            "Trigger": "Airflow on_failure_callback",
        },
        {
            "Event": "Anomaly detected (>3σ)",
            "Target": "Slack + Teams",
            "Trigger": "anomaly_events.status='OPEN' insert hook",
        },
        {
            "Event": "Proposal pending review",
            "Target": "Slack + Teams",
            "Trigger": "agent_proposals.status='DRAFT' for >1h",
        },
        {
            "Event": "Schema drift logged",
            "Target": "Slack + Teams",
            "Trigger": "schema_drift_log insert with severity>=WARNING",
        },
        {
            "Event": "Upstream-update available",
            "Target": "Slack + Teams",
            "Trigger": "upstream_update_notifications insert",
        },
    ]
)
st.dataframe(auto_events, use_container_width=True, hide_index=True)

st.markdown("---")
st.caption(
    "**Two-way actions** (approve / reject from Slack): require a Slack App "
    "with interactivity enabled, plus a public-facing webhook to receive "
    "Block Kit responses. Configuration: see `docs/CHATOPS_INBOUND.md` "
    "(forthcoming)."
)
