"""Data Contract Marketplace — Phase 16.4 (Wave 4 #22).

Internal browse-and-clone catalog. Every LIVE pipeline + LIVE Silver/Gold
schema is automatically published. New-client onboarding becomes
"browse → clone → override" instead of "design from scratch".

Single page lists every reusable asset with:
  * Asset type (silver_schema | gold_schema | pipeline_instance)
  * Origin client (where it was first authored)
  * Reuse count (how many other clients have cloned it)
  * Tags (certified-prod, golden-reference, etc.)
  * One-click clone (sends to Pipeline Architect with conf pre-filled)
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
    page_title="Contract Marketplace",
    page_icon="🏪",
    layout="wide",
    initial_sidebar_state="expanded",
)

from datalink.quality.control import CONTROL_SCHEMA  # noqa: E402
from datalink.ui._nav import render_sidebar  # noqa: E402
from datalink.ui._query import warehouse_ctx  # noqa: E402

render_sidebar(active="Contract Marketplace")

st.title("🏪 Data Contract Marketplace")
st.caption(
    "Browse every reusable asset across the platform. Time-to-onboard a "
    "new client drops from days → minutes when they can clone an existing "
    "pipeline."
)

with warehouse_ctx(readonly=True) as wh:
    silver_assets = list(
        wh.query(
            f"""
        SELECT s.silver_dataset_id, s.dataset_code, s.version, s.pattern,
               s.designed_by, s.created_at, s.status,
               (SELECT COUNT(*) FROM {CONTROL_SCHEMA}.global_silver_schema_tables
                 WHERE silver_dataset_id = s.silver_dataset_id) AS table_count
          FROM {CONTROL_SCHEMA}.global_silver_schema_datasets s
         WHERE s.status = 'LIVE'
         ORDER BY s.dataset_code
        """
        )
    )
    gold_assets = list(
        wh.query(
            f"""
        SELECT g.gold_dataset_id, g.dataset_code, g.version, g.gold_anchor,
               g.designed_by, g.created_at, g.status,
               (SELECT COUNT(*) FROM {CONTROL_SCHEMA}.global_gold_schema_fields
                 WHERE gold_dataset_id = g.gold_dataset_id) AS column_count
          FROM {CONTROL_SCHEMA}.global_gold_schema_datasets g
         WHERE g.status = 'LIVE'
         ORDER BY g.dataset_code
        """
        )
    )
    pipeline_assets = list(
        wh.query(
            f"""
        SELECT p.instance_id, p.client_id, p.dataset_code, p.bronze_anchor,
               p.schedule_cron, p.deployed_at, p.status,
               (SELECT COUNT(*) FROM {CONTROL_SCHEMA}.client_pipeline_instances
                 WHERE cloned_from_instance = p.instance_id) AS clone_count
          FROM {CONTROL_SCHEMA}.client_pipeline_instances p
         WHERE p.status = 'LIVE'
         ORDER BY p.deployed_at DESC NULLS LAST
        """
        )
    )

# Headline KPIs
kpis = st.columns(4)
with kpis[0]:
    st.metric("Silver schemas LIVE", len(silver_assets))
with kpis[1]:
    st.metric("Gold schemas LIVE", len(gold_assets))
with kpis[2]:
    st.metric("Pipeline templates", len(pipeline_assets))
with kpis[3]:
    total_clones = sum(int(p.get("clone_count") or 0) for p in pipeline_assets)
    st.metric("Times cloned", total_clones)

st.markdown("---")

# Silver schemas
st.markdown("### 🥈 Silver schemas (cross-client reusable)")
if silver_assets:
    df = pd.DataFrame(
        [
            {
                "Dataset": s["dataset_code"],
                "Version": f"v{s['version']}",
                "Pattern": s["pattern"],
                "Tables": s["table_count"],
                "Designed by": s["designed_by"],
                "Created": str(s["created_at"])[:19],
            }
            for s in silver_assets
        ]
    )
    st.dataframe(df, use_container_width=True, hide_index=True)
else:
    st.info("No LIVE Silver schemas yet — author one in Data Model Designer.")

# Gold schemas
st.markdown("### 🥇 Gold schemas (cross-client reusable)")
if gold_assets:
    df = pd.DataFrame(
        [
            {
                "Dataset": g["dataset_code"],
                "Version": f"v{g['version']}",
                "Anchor": g["gold_anchor"],
                "Columns": g["column_count"],
                "Designed by": g["designed_by"],
                "Created": str(g["created_at"])[:19],
            }
            for g in gold_assets
        ]
    )
    st.dataframe(df, use_container_width=True, hide_index=True)
else:
    st.info("No LIVE Gold schemas yet — author one in Data Model Designer.")

# Pipeline templates
st.markdown("### 🏛 Pipeline templates (clone-ready)")
if pipeline_assets:
    rows = []
    for p in pipeline_assets:
        rows.append(
            {
                "Source client": p["client_id"],
                "Dataset": p["dataset_code"],
                "Anchor": p["bronze_anchor"],
                "Schedule": p["schedule_cron"],
                "Deployed": str(p["deployed_at"])[:19],
                "🔁 Times cloned": int(p.get("clone_count") or 0),
                "Instance ID": str(p["instance_id"])[:8] + "…",
            }
        )
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

    # Clone-to-new-client form
    st.markdown("---")
    st.markdown("### 🔁 Clone a pipeline to a new client")
    src_options = [
        f"{p['client_id']} / {p['dataset_code']} ({str(p['instance_id'])[:8]}…)"
        for p in pipeline_assets
    ]
    src_idx = st.selectbox(
        "Source pipeline", options=range(len(src_options)), format_func=lambda i: src_options[i]
    )
    src = pipeline_assets[src_idx]

    target_client = st.text_input(
        "Target client ID (e.g. bcbs)",
        placeholder="bcbs",
        help="Lower-case, no spaces. Will create BRONZE_BCBS / SILVER_BCBS / "
        "GOLD_BCBS schemas if not exist.",
    )
    notes = st.text_input(
        "Clone notes",
        placeholder=f"Cloning {src['client_id']}'s {src['dataset_code']} pipeline to {target_client}",
    )
    if st.button("🔁 Clone to target client", type="primary", disabled=not target_client.strip()):
        from datalink.agents.pipeline_architect import clone_pipeline_to_client

        with warehouse_ctx(readonly=False) as wh:
            try:
                result = clone_pipeline_to_client(
                    warehouse=wh,
                    source_instance_id=str(src["instance_id"]),
                    target_client_id=target_client.strip().lower(),
                    actor=f"ui:marketplace:{target_client.strip().lower()}",
                    notes=notes,
                )
                st.success(
                    f"✅ Cloned. New instance ID: "
                    f"`{result['new_instance_id'][:8]}…`. "
                    f"Status is DRAFT — go to **Pipeline Architect** to "
                    f"approve and deploy."
                )
                _t = target_client.strip().lower()
                _ds = src["dataset_code"]
                st.markdown(
                    f'<a href="/Pipeline_Architect?client={_t}&dataset={_ds}" '
                    f'target="_self">🏛 → Open Pipeline Architect to '
                    f"approve the clone</a>",
                    unsafe_allow_html=True,
                )
            except Exception as exc:
                st.error(f"Clone failed: {type(exc).__name__}: {exc}")
else:
    st.info("No LIVE pipeline instances yet — deploy one in Pipeline Architect.")
