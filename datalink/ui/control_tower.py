"""Control Tower — Phase 16.3 (Wave 3 Item 11).

The home dashboard. Replaces the obsolete Phase-7 Control Tower whose tiles
described features that no longer reflect reality.

Single-pane situational awareness. Bronze → Silver → Gold → Pipeline → Run.
Dive into any page from here.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pandas as pd
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

NAVY = "#0a1a3e"
GOLD = "#d4af37"
EMERALD = "#15803d"
AMBER = "#b45309"
RED = "#991b1b"

st.markdown(
    f"""
    <style>
      h1 {{color: {NAVY}; border-bottom: 3px solid {GOLD}; padding-bottom: .4rem;}}
      h3 {{color: {NAVY}; margin-top: 1.2rem;}}
      .ct-tile {{background:#fff;border:1px solid #d4d4d8;border-radius:8px;
                 padding:1rem 1.2rem;height:100%;
                 box-shadow:0 1px 3px rgba(0,0,0,.04);}}
      .ct-tile-label {{font-size:.78rem;color:#64748b;text-transform:uppercase;
                       letter-spacing:.05em;font-weight:600;}}
      .ct-tile-value {{font-size:1.9rem;font-weight:800;color:{NAVY};margin-top:.25rem;}}
      .ct-tile-sub   {{font-size:.78rem;color:#94a3b8;margin-top:.15rem;}}
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("🏛️ DataLink Control Tower")
st.caption(
    "Single-pane situational awareness. Bronze → Silver → Gold → "
    "Pipeline → Run. Dive into any page from here."
)


@st.cache_data(ttl=20)  # type: ignore[misc]
def _state() -> dict[str, Any]:
    out: dict[str, Any] = {}
    with warehouse_ctx(readonly=True) as wh:
        for key, sql in [
            (
                "bronze_datasets",
                f"SELECT COUNT(*) c FROM {CONTROL_SCHEMA}.global_bronze_catalog_datasets WHERE is_active = TRUE",
            ),
            (
                "bronze_fields",
                f"SELECT COUNT(*) c FROM {CONTROL_SCHEMA}.global_bronze_catalog_fields",
            ),
            (
                "silver_live",
                f"SELECT COUNT(*) c FROM {CONTROL_SCHEMA}.global_silver_schema_datasets WHERE status = 'LIVE'",
            ),
            (
                "gold_live",
                f"SELECT COUNT(*) c FROM {CONTROL_SCHEMA}.global_gold_schema_datasets WHERE status = 'LIVE'",
            ),
            (
                "instances_live",
                f"SELECT COUNT(*) c FROM {CONTROL_SCHEMA}.client_pipeline_instances WHERE status = 'LIVE'",
            ),
            (
                "clients_onboarded",
                f"SELECT COUNT(DISTINCT client_id) c FROM {CONTROL_SCHEMA}.client_pipeline_instances",
            ),
            # DQ rules — split authored vs baseline-seed so the tile reflects
            # demo state (authored), not platform seed data (baseline_python
            # templates auto-loaded by container bootstrap).
            (
                "dq_suites_authored",
                f"SELECT COUNT(*) c FROM {CONTROL_SCHEMA}.dq_suites "
                f"WHERE source <> 'baseline_python'",
            ),
            (
                "dq_suites_baseline",
                f"SELECT COUNT(*) c FROM {CONTROL_SCHEMA}.dq_suites "
                f"WHERE source = 'baseline_python'",
            ),
            ("standards", f"SELECT COUNT(*) c FROM {CONTROL_SCHEMA}.standard_registry"),
            ("routing_rules", f"SELECT COUNT(*) c FROM {CONTROL_SCHEMA}.onprem_routing_rules"),
        ]:
            try:
                rows = list(wh.query(sql))
                out[key] = int(rows[0]["c"]) if rows else 0
            except Exception:
                out[key] = 0
        try:
            out["per_client"] = list(
                wh.query(
                    f"SELECT client_id, COUNT(DISTINCT dataset_code) AS datasets, "
                    f"       COUNT(*) AS pipelines, MAX(deployed_at) AS last_deploy "
                    f"FROM {CONTROL_SCHEMA}.client_pipeline_instances "
                    f"WHERE status = 'LIVE' "
                    f"GROUP BY client_id ORDER BY client_id"
                )
            )
        except Exception:
            out["per_client"] = []
        try:
            out["open_notifs"] = list(
                wh.query(
                    f"SELECT severity, COUNT(*) AS c "
                    f"FROM {CONTROL_SCHEMA}.upstream_update_notifications "
                    f"WHERE status = 'OPEN' GROUP BY severity"
                )
            )
        except Exception:
            out["open_notifs"] = []
        try:
            out["ai_cost_7d"] = (
                next(
                    iter(
                        wh.query(
                            f"SELECT COALESCE(SUM(estimated_cost_usd),0) AS cost, "
                            f"       COALESCE(SUM(total_tokens),0) AS tokens, "
                            f"       COUNT(*) AS proposals "
                            f"FROM {CONTROL_SCHEMA}.agent_proposals "
                            f"WHERE created_at > DATEADD(day, -7, CURRENT_TIMESTAMP())"
                        )
                    )
                )
                or {}
            )
        except Exception:
            out["ai_cost_7d"] = {}
    return out


state = _state()

# Top KPI tiles
kpis = [
    (
        "Bronze datasets",
        state.get("bronze_datasets", 0),
        f"{state.get('bronze_fields', 0)} fields cataloged",
    ),
    ("Silver schemas LIVE", state.get("silver_live", 0), "designed in Data Model Designer"),
    ("Gold schemas LIVE", state.get("gold_live", 0), "designed in Data Model Designer"),
    (
        "Pipelines LIVE",
        state.get("instances_live", 0),
        f"across {state.get('clients_onboarded', 0)} clients",
    ),
    (
        "DQ rules · authored",
        state.get("dq_suites_authored", 0),
        f"+{state.get('dq_suites_baseline', 0)} baseline templates · "
        f"{state.get('standards', 0)} RAG standards",
    ),
]
cols = st.columns(len(kpis))
for col, (label, value, sub) in zip(cols, kpis, strict=False):
    with col:
        st.markdown(
            f"""<div class="ct-tile">
              <div class="ct-tile-label">{label}</div>
              <div class="ct-tile-value">{value}</div>
              <div class="ct-tile-sub">{sub}</div>
            </div>""",
            unsafe_allow_html=True,
        )

# Workflow status
st.markdown("---")
st.markdown("### 🛤️ Workflow status — where each client+dataset is")

per_client = state.get("per_client", [])
if not per_client:
    st.info(
        f"No clients onboarded yet. Start by **authoring schemas** in "
        f"[Data Model Designer](/Data_Model_Designer) for any of the "
        f"**{state.get('bronze_datasets', 0)} Bronze datasets** in the catalog."
    )
else:
    # Cross-tabulate per (client, dataset) using LIVE state
    with warehouse_ctx(readonly=True) as wh:
        try:
            workflow = list(
                wh.query(
                    f"""
                SELECT p.client_id, p.dataset_code,
                  CASE WHEN s.dataset_code IS NOT NULL THEN '✅ LIVE' ELSE '⏳ pending' END AS silver,
                  CASE WHEN g.dataset_code IS NOT NULL THEN '✅ LIVE' ELSE '⏳ pending' END AS gold,
                  CASE WHEN p.status = 'LIVE' THEN '✅ LIVE'
                       WHEN p.status = 'DRAFT' THEN '⏳ DRAFT'
                       ELSE '— ' || COALESCE(p.status, '') END AS pipeline,
                  p.deployed_at
                FROM {CONTROL_SCHEMA}.client_pipeline_instances p
                LEFT JOIN {CONTROL_SCHEMA}.global_silver_schema_datasets s
                       ON s.dataset_code = p.dataset_code AND s.status = 'LIVE'
                LEFT JOIN {CONTROL_SCHEMA}.global_gold_schema_datasets g
                       ON g.dataset_code = p.dataset_code AND g.status = 'LIVE'
                ORDER BY p.client_id, p.dataset_code
                """
                )
            )
        except Exception as exc:
            workflow = []
            st.warning(f"Workflow query failed: {exc}")

    if workflow:
        wf_rows = [
            {
                "Client": w["client_id"],
                "Dataset": w["dataset_code"],
                "🥈 Silver": w["silver"],
                "🥇 Gold": w["gold"],
                "🏛 Pipeline": w["pipeline"],
                "Last deploy": str(w.get("deployed_at") or "—")[:19],
            }
            for w in workflow
        ]
        st.dataframe(pd.DataFrame(wf_rows), use_container_width=True, hide_index=True)

# Per-client summary
st.markdown("---")
st.markdown("### 🏢 Clients")
if per_client:
    pc_rows = [
        {
            "Client": r.get("client_id"),
            "Datasets onboarded": r.get("datasets"),
            "🟢 LIVE pipelines": r.get("pipelines"),
            "Last deploy": str(r.get("last_deploy") or "—")[:19],
        }
        for r in per_client
    ]
    st.dataframe(pd.DataFrame(pc_rows), use_container_width=True, hide_index=True)
else:
    st.caption("No clients yet. The first deploy from Pipeline Architect creates one.")

# Needs attention + AI spend
st.markdown("---")
st.markdown("### 🔔 Needs attention")

ncol1, ncol2 = st.columns([1, 1])
with ncol1:
    notifs = state.get("open_notifs") or []
    if not notifs:
        st.success("📦 No open upstream-update notifications.")
    else:
        st.warning(
            "📦 **Upstream updates available** — downstream pipelines are "
            "consuming a stale Silver/Gold version."
        )
        st.dataframe(
            pd.DataFrame([{"Severity": n["severity"], "Count": n["c"]} for n in notifs]),
            use_container_width=True,
            hide_index=True,
        )
        st.page_link(
            "pages/16_Version_Browser.py", label="→ Open Version Browser to triage", icon="🕒"
        )

with ncol2:
    cost = state.get("ai_cost_7d") or {}
    cost_value = cost.get("cost") or 0
    tokens = cost.get("tokens") or 0
    n_p = cost.get("proposals") or 0
    st.markdown(
        f"""<div class="ct-tile">
          <div class="ct-tile-label">AI spend — last 7 days</div>
          <div class="ct-tile-value">${float(cost_value):.4f}</div>
          <div class="ct-tile-sub">{int(tokens):,} tokens · {int(n_p)} proposals</div>
        </div>""",
        unsafe_allow_html=True,
    )

# Quick actions
st.markdown("---")
st.markdown("### ⚡ Quick actions")
qcol1, qcol2, qcol3, qcol4 = st.columns(4)
with qcol1:
    st.page_link("pages/14_Data_Model_Designer.py", label="🥇 Author / edit schemas", icon="🥇")
with qcol2:
    st.page_link("pages/13_Pipeline_Architect.py", label="🏛 Deploy a pipeline", icon="🏛")
with qcol3:
    st.page_link("pages/15_Run_Monitor.py", label="📺 Watch a run", icon="📺")
with qcol4:
    st.page_link("pages/99_Admin_Reset.py", label="🧨 Reset demo state", icon="🧨")
