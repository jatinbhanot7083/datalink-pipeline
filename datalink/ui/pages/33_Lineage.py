"""Column-Level Lineage + Impact Analysis — Phase 16.4 (Wave 4 #17).

Shows: for any chosen column at any layer, every column it depends on
(upstream) and every column that depends on it (downstream). Also shows
which DAGs / clients would break if you change it.

Data sources:
  * CONTROL.global_bronze_catalog_fields  — Bronze cols
  * CONTROL.global_silver_schema_columns  — Silver cols
  * CONTROL.global_gold_schema_fields     — Gold cols
  * CONTROL.bronze_to_silver_mappings     — B→S transforms
  * CONTROL.silver_to_gold_mappings       — S→G transforms
  * CONTROL.bronze_to_gold_mappings       — direct B→G shortcuts
  * CONTROL.client_pipeline_instances     — which clients have each LIVE
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
    page_title="Lineage",
    page_icon="🧬",
    layout="wide",
    initial_sidebar_state="expanded",
)

from datalink.quality.control import CONTROL_SCHEMA  # noqa: E402
from datalink.ui._nav import render_sidebar  # noqa: E402
from datalink.ui._query import warehouse_ctx  # noqa: E402

render_sidebar(active="Lineage")

st.title("🧬 Column-Level Lineage + Impact")
st.caption(
    "What feeds this column? What breaks if I change it? Auditor-grade "
    "lineage + customer-grade impact analysis."
)

# Pick a Gold column to trace
with warehouse_ctx(readonly=True) as wh:
    gold_cols = list(
        wh.query(
            f"""
        SELECT g.gold_dataset_id, g.gold_column_name AS column_name,
               g.logical_type, g.is_business_key,
               g.is_pii, g.is_phi, gd.dataset_code
          FROM {CONTROL_SCHEMA}.global_gold_schema_fields g
          JOIN {CONTROL_SCHEMA}.global_gold_schema_datasets gd
            ON gd.gold_dataset_id = g.gold_dataset_id
         WHERE gd.status = 'LIVE'
         ORDER BY gd.dataset_code, g.column_order
        """
        )
    )

if not gold_cols:
    st.warning("No LIVE Gold schemas yet. Author one in Data Model Designer to see lineage.")
    st.stop()

ds_options = sorted({r["dataset_code"] for r in gold_cols})
sel_ds = st.selectbox("Dataset", options=ds_options)
ds_cols = [r for r in gold_cols if r["dataset_code"] == sel_ds]
sel_col = st.selectbox(
    "Gold column to trace",
    options=[c["column_name"] for c in ds_cols],
)
target = next(c for c in ds_cols if c["column_name"] == sel_col)

# Upstream lineage
st.markdown("---")
st.markdown("### 🔼 Upstream sources")
with warehouse_ctx(readonly=True) as wh:
    bronze_to_gold = list(
        wh.query(
            f"""
        SELECT m.gold_column_name, m.transform_kind, m.transform_sql,
               m.bronze_source_columns
          FROM {CONTROL_SCHEMA}.bronze_to_gold_mappings m
         WHERE m.gold_dataset_id = $g AND m.gold_column_name = $col
        """,
            {"g": target["gold_dataset_id"], "col": target["column_name"]},
        )
    )
    silver_to_gold = list(
        wh.query(
            f"""
        SELECT m.gold_column_name, m.transform_kind, m.transform_sql,
               m.silver_source_columns, m.silver_dataset_id
          FROM {CONTROL_SCHEMA}.silver_to_gold_mappings m
         WHERE m.gold_dataset_id = $g AND m.gold_column_name = $col
        """,
            {"g": target["gold_dataset_id"], "col": target["column_name"]},
        )
    )

if bronze_to_gold:
    st.markdown("**Bronze → Gold (direct):**")
    st.dataframe(pd.DataFrame(bronze_to_gold), use_container_width=True, hide_index=True)
if silver_to_gold:
    st.markdown("**Silver → Gold:**")
    st.dataframe(pd.DataFrame(silver_to_gold), use_container_width=True, hide_index=True)
if not bronze_to_gold and not silver_to_gold:
    st.info("No mappings recorded for this column. Author them in Data Model Designer.")

# Downstream impact
st.markdown("---")
st.markdown("### 🔽 Downstream impact — who consumes this column?")
with warehouse_ctx(readonly=True) as wh:
    # Find all LIVE pipeline instances that materialize this dataset
    consumers = list(
        wh.query(
            f"""
        SELECT client_id, dataset_code, instance_id, status,
               gold_schema, gold_table, deployed_at, schedule_cron, dag_uri
          FROM {CONTROL_SCHEMA}.client_pipeline_instances
         WHERE dataset_code = $ds AND status = 'LIVE'
         ORDER BY client_id
        """,
            {"ds": sel_ds},
        )
    )

if consumers:
    st.warning(
        f"⚠️ **{len(consumers)} LIVE client pipelines** consume `{sel_ds}.{sel_col}`. "
        f"Changing this column requires coordinated communication."
    )
    df = pd.DataFrame(
        [
            {
                "Client": c["client_id"],
                "Status": c["status"],
                "Gold target": f"{c['gold_schema']}.{c['gold_table']}",
                "Schedule": c["schedule_cron"],
                "Deployed": str(c["deployed_at"])[:19],
                "DAG": c["dag_uri"],
            }
            for c in consumers
        ]
    )
    st.dataframe(df, use_container_width=True, hide_index=True)
else:
    st.success(
        f"No clients have a LIVE pipeline for `{sel_ds}` — changes to `{sel_col}` are safe to make."
    )

# Routing impact (which downstream products receive this dataset)
st.markdown("---")
st.markdown("### 📤 Downstream OnPrem routing")
with warehouse_ctx(readonly=True) as wh:
    routes = list(
        wh.query(
            f"""
        SELECT downstream_product, target_system, target_uri, is_default,
               is_active
          FROM {CONTROL_SCHEMA}.onprem_routing_rules
         WHERE dataset_code = $ds AND is_active = TRUE
         ORDER BY downstream_product
        """,
            {"ds": sel_ds},
        )
    )
if routes:
    st.dataframe(
        pd.DataFrame(routes),
        use_container_width=True,
        hide_index=True,
    )
else:
    st.caption(f"No active routing rules for `{sel_ds}`.")
