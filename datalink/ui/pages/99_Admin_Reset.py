"""Admin · Reset Demo — Phase 16.1 (Wave 1 Item 5).

Single-pane admin console for cleaning state without touching a terminal.

Five reset levels with explicit two-step confirmation per level. A "🧨 Full
demo reset" preset runs the right combo for "I want to redo the demo from
scratch" in one click. Every action lands in
``CONTROL.admin_reset_audit_log`` so you can prove what was wiped, when, by
whom.
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
    page_title="Admin · Reset Demo",
    page_icon="🧨",
    layout="wide",
    initial_sidebar_state="expanded",
)

from datalink.admin import (  # noqa: E402
    delete_generated_artifacts,
    full_reset,
    reset_airflow_metadata,
    reset_design_registry,
    reset_pipeline_data,
    reset_proposals,
)
from datalink.quality.control import CONTROL_SCHEMA  # noqa: E402
from datalink.ui._nav import render_sidebar  # noqa: E402
from datalink.ui._query import warehouse_ctx  # noqa: E402

render_sidebar(active="Admin · Reset Demo")

st.title("🧨 Admin · Reset Demo")
st.caption(
    "**Operator-grade cleanup, no terminal needed.** Every reset action is "
    "logged to `CONTROL.admin_reset_audit_log` for compliance + auditing."
)
st.warning(
    "⚠️  **Destructive operations.** Read the description on each card. "
    "Double-confirm checkbox required to enable each button."
)

st.markdown("---")

# Active client selection (some resets are scoped per client)
selected_client_input = st.text_input(
    "Client to scope per-client resets to (leave blank for all clients)",
    value=st.session_state.get("client_id", "")
    if isinstance(st.session_state.get("client_id"), str)
    else "",
    placeholder="e.g. aetna",
    help="Some resets (pipeline data, generated files) operate per-client. "
    "Others (designs, proposals, Airflow metadata) are platform-wide.",
)
selected_client = selected_client_input.strip().lower() or None

actor = f"admin:ui:{(selected_client or 'platform')}"

# ---- Quick Full-Reset preset -----------------------------------------------
st.markdown("### 🧨 Full demo reset (most common)")
st.caption(
    "One click runs: pipeline-data wipe → Airflow metadata wipe → "
    "generated-artifacts delete. Bronze catalog + standards + Silver/Gold "
    "designs are preserved (they're expensive to recreate)."
)
fcols = st.columns([1, 1, 1, 4])
with fcols[0]:
    incl_designs = st.checkbox(
        "Also wipe **designs**",
        value=False,
        help="Adds: drop Silver+Gold schema design rows. Use when starting "
        "from scratch (have to re-author Silver+Gold).",
    )
with fcols[1]:
    incl_proposals = st.checkbox(
        "Also wipe **proposals**",
        value=False,
        help="Adds: drop CONTROL.agent_proposals + tag assignments. Tag catalog kept.",
    )
with fcols[2]:
    confirm_full = st.checkbox(
        "✅ I confirm full reset",
        value=False,
        key="full_confirm",
    )
with fcols[3]:
    if st.button(
        "🧨 Run full demo reset",
        type="primary",
        use_container_width=True,
        disabled=not confirm_full or not selected_client,
    ):
        with st.spinner("Running full reset..."):
            reports = full_reset(
                client_id=selected_client or "",  # type: ignore[arg-type]
                actor=actor,
                include_designs=incl_designs,
                include_proposals=incl_proposals,
            )
        st.success("Full reset done.")
        for r in reports:
            st.markdown(f"- {r.summary_line()}")
            if r.warnings:
                with st.expander(f"⚠️ {len(r.warnings)} warning(s) for {r.operation}"):
                    for w in r.warnings:
                        st.text(w)
            if r.errors:
                with st.expander(f"❌ {len(r.errors)} error(s) for {r.operation}"):
                    for e in r.errors:
                        st.text(e)
        st.session_state["full_confirm"] = False

if not selected_client:
    st.info(
        "ℹ️ Pick a client (above) to enable the full reset button. "
        "Per-table operations below work without a client."
    )

st.markdown("---")
st.markdown("### 🧰 Granular operations")

# ---- Pipeline data --------------------------------------------------------
st.markdown("#### 1. Pipeline data + physical schemas")
st.caption(
    "**Wipes:** `client_pipeline_instances`, audit logs, overflow log, "
    "DQ suites where source='PIPELINE_ARCHITECT', plus DROP "
    "BRONZE_<client> / SILVER_<client> / GOLD_<client> schemas. "
    "**Keeps:** Bronze catalog, Silver/Gold designs, agent_proposals."
)
pcol1, pcol2, pcol3 = st.columns([1, 1, 4])
with pcol1:
    confirm_pipe = st.checkbox(
        "✅ I confirm pipeline-data wipe",
        value=False,
        key="confirm_pipe",
    )
with pcol2:
    if st.button(
        "🗑 Wipe pipeline data",
        use_container_width=True,
        disabled=not confirm_pipe,
    ):
        with st.spinner("Wiping..."):
            r = reset_pipeline_data(client_id=selected_client, actor=actor)
        st.markdown(r.summary_line())
        if r.warnings:
            for w in r.warnings:
                st.text(f"⚠️ {w}")
        st.session_state["confirm_pipe"] = False

# ---- Designs --------------------------------------------------------------
st.markdown("#### 2. Silver + Gold design registry")
st.caption(
    "**Wipes:** `global_silver_schema_*`, `global_gold_schema_*`, "
    "Bronze→Silver/Gold mappings, Silver→Gold mappings, pattern recommendations. "
    "**Keeps:** Bronze catalog (it's the source of truth, not user-authored)."
)
dcol1, dcol2 = st.columns([1, 1])
with dcol1:
    confirm_des = st.checkbox(
        "✅ I confirm design-registry wipe",
        value=False,
        key="confirm_des",
    )
with dcol2:
    if st.button(
        "🗑 Wipe design registry",
        use_container_width=True,
        disabled=not confirm_des,
    ):
        r = reset_design_registry(actor=actor)
        st.markdown(r.summary_line())
        st.session_state["confirm_des"] = False

# ---- Proposals ------------------------------------------------------------
st.markdown("#### 3. Saved AI proposals")
st.caption(
    "**Wipes:** every saved proposal across every (client, dataset) plus tag "
    "assignments. **Keeps:** the system tag catalog (18 tags)."
)
pcol1, pcol2 = st.columns([1, 1])
with pcol1:
    confirm_prop = st.checkbox(
        "✅ I confirm proposals wipe",
        value=False,
        key="confirm_prop",
    )
with pcol2:
    if st.button(
        "🗑 Wipe agent_proposals",
        use_container_width=True,
        disabled=not confirm_prop,
    ):
        r = reset_proposals(actor=actor)
        st.markdown(r.summary_line())
        st.session_state["confirm_prop"] = False

# ---- Airflow metadata -----------------------------------------------------
st.markdown("#### 4. Airflow metadata (task_instance / dag_run / xcom / log)")
st.caption(
    "**Wipes:** Airflow's task_instance + dag_run + xcom + log + dag rows. "
    "Run when DAG is stuck queued / orphaned, or before a fresh demo run."
)
acol1, acol2 = st.columns([1, 1])
with acol1:
    confirm_af = st.checkbox(
        "✅ I confirm Airflow-metadata wipe",
        value=False,
        key="confirm_af",
    )
with acol2:
    if st.button(
        "🗑 Wipe Airflow metadata",
        use_container_width=True,
        disabled=not confirm_af,
    ):
        r = reset_airflow_metadata(actor=actor)
        st.markdown(r.summary_line())
        if r.warnings:
            for w in r.warnings:
                st.text(f"⚠️ {w}")
        st.session_state["confirm_af"] = False

# ---- Generated artifacts --------------------------------------------------
st.markdown("#### 5. Generated artifact files (DAGs, dbt models, DDL files)")
st.caption(
    "**Wipes:** `dags/<client>_*_pipeline.py`, `dbt/models/silver/<client>/`, "
    "`dbt/models/gold/<client>/`, `datalink/pipeline/{bronze,gold}/ddl/<client>_*.sql`, "
    "`data/generated/<client>/`. Uses docker exec for root-owned files."
)
gcol1, gcol2 = st.columns([1, 1])
with gcol1:
    confirm_gen = st.checkbox(
        "✅ I confirm generated-files wipe",
        value=False,
        key="confirm_gen",
    )
with gcol2:
    if st.button(
        "🗑 Delete generated artifacts",
        use_container_width=True,
        disabled=not confirm_gen,
    ):
        r = delete_generated_artifacts(client_id=selected_client, actor=actor)
        st.markdown(r.summary_line())
        if r.warnings:
            for w in r.warnings:
                st.text(f"⚠️ {w}")
        st.session_state["confirm_gen"] = False


# ---- Audit log ------------------------------------------------------------
st.markdown("---")
st.markdown("### 📋 Recent reset audit log")
try:
    with warehouse_ctx(readonly=True) as wh:
        rows = list(
            wh.query(
                f"SELECT event_at, actor, operation, client_id, counts_json, "
                f"       duration_ms "
                f"FROM {CONTROL_SCHEMA}.admin_reset_audit_log "
                f"ORDER BY event_at DESC LIMIT 25"
            )
        )
    if rows:
        df = pd.DataFrame(rows)
        st.dataframe(df, use_container_width=True, hide_index=True)
    else:
        st.caption("No reset events logged yet.")
except Exception as exc:
    st.caption(f"Audit log not yet initialized: {exc}")
