"""Pipeline Architect — Phase 15 — Gold-first multi-tenant pipeline orchestrator.

This is the new top-of-funnel for Author DQ. The Global Gold Catalog
(33 datasets, 943 fields) is the contract; the operator picks a dataset,
the agent proposes a fully-resolved Bronze→Silver→Gold pipeline anchored
to it, and one click deploys all 5 artifacts:

    1. Gold DDL              — datalink/pipeline/gold/ddl/<client>_<dataset>.sql
    2. Silver dbt model      — dbt/models/silver/<client>/<dataset>_clean.sql
    3. Gold dbt model        — dbt/models/gold/<client>/<dataset>.sql
    4. Airflow DAG           — dags/<client>_<dataset>_pipeline.py
    5. GX expectation suite  — CONTROL.dq_suites row (LIVE)

The agent uses real Anthropic Claude haiku-4.5 for the executive summary +
clone-vs-build narrative + override review. Deterministic builders generate
the structural artifacts so they're identical across runs.
"""

from __future__ import annotations

import sys
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

# bootstrap path
ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

st.set_page_config(
    page_title="Pipeline Architect",
    page_icon="🏛",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Phase 17.9 — imports the FOCUSED AI-Architect-for-orchestration flow uses.
# The Pipeline Architect AI's job is propose orchestration metadata + DAG +
# artifacts, NOT schema (that's DMD).  But the LLM is the heart of this
# page — without it there's no intelligence, just mechanical INSERTs.
from datalink.agents.llm_router import get_llm  # noqa: E402
from datalink.agents.pipeline_architect import (  # noqa: E402
    PipelinePrerequisitesError,
    approve_and_deploy,
    persist_proposal,
    propose_pipeline,
)
from datalink.quality.control import CONTROL_SCHEMA, create_control_tables  # noqa: E402
from datalink.ui import (
    _dmd_data as _dmd,
)
from datalink.ui import _pa_data as _pa  # noqa: E402  Pipeline Architect snapshot
from datalink.ui._nav import render_sidebar  # noqa: E402
from datalink.ui._query import warehouse_ctx  # noqa: E402

render_sidebar(active="Pipeline Architect")

_NAVY = "#0a1a3e"
_GOLD = "#d4af37"
_AGENT_BLUE = "#2563eb"
_GREEN = "#15803d"
_AMBER = "#b45309"
_RED = "#b91c1c"

st.markdown(
    f"""
    <style>
      h1 {{color: {_NAVY}; border-bottom: 3px solid {_GOLD}; padding-bottom: .4rem;}}
      h2 {{color: {_NAVY}; margin-top: 1.5rem;}}
      h3 {{color: {_NAVY};}}
      .arch-hero {{background:linear-gradient(90deg,{_NAVY}11,{_GOLD}22);
                   padding:1rem 1.2rem;border-radius:8px;border-left:4px solid {_GOLD};
                   margin:.6rem 0 1.5rem 0}}
      .arch-card {{background:#fff;border:1px solid #d4d4d8;border-radius:8px;
                   padding:1rem;margin:.5rem 0;box-shadow:0 1px 3px rgba(0,0,0,.04)}}
      .arch-pill {{display:inline-block;padding:.15rem .55rem;border-radius:12px;
                   font-size:.72rem;font-weight:700;letter-spacing:.04em;margin-right:.4rem}}
      .arch-pill-clone {{background:#dbeafe;color:#1e40af}}
      .arch-pill-build {{background:#dcfce7;color:#166534}}
      .arch-pill-live {{background:#dcfce7;color:#166534}}
      .arch-pill-pending {{background:#fef3c7;color:#92400e}}
      .arch-pill-archived {{background:#f3f4f6;color:#4b5563}}
      .arch-pill-fhir {{background:#dbeafe;color:#1e40af}}
      .arch-pill-x12 {{background:#fde68a;color:#78350f}}
      .arch-pill-flat {{background:#e0e7ff;color:#3730a3}}
      .arch-pill-ncpdp {{background:#fce7f3;color:#9d174d}}
      .arch-pill-api {{background:#d1fae5;color:#065f46}}
      .arch-pill-dev {{background:#fee2e2;color:#991b1b}}
      .arch-meta {{font-size:.85rem;color:#475569;margin-top:.4rem}}
      .arch-cite {{font-size:.78rem;color:{_AGENT_BLUE};font-style:italic;}}
      .arch-stat {{font-size:1.6rem;font-weight:700;color:{_NAVY};}}
      .arch-stat-label {{font-size:.78rem;color:#64748b;text-transform:uppercase;
                         letter-spacing:.05em;font-weight:600;}}
      .arch-route-row {{padding:.4rem .6rem;border-radius:4px;background:#f8fafc;
                        border-left:3px solid {_AGENT_BLUE};margin:.2rem 0;}}
      .arch-route-row.disabled {{border-left-color:{_RED};opacity:.6;}}

      /* Phase 16.1 — saved-proposal banner */
      .saved-prop-card {{background:linear-gradient(90deg,#fffbeb,#fef3c7);
                         border:1px solid #fbbf24;border-left:5px solid #d97706;
                         border-radius:8px;padding:1rem 1.2rem;margin:.8rem 0 1.2rem 0;
                         box-shadow:0 1px 4px rgba(0,0,0,.06);}}
      .saved-prop-card.approved {{background:linear-gradient(90deg,#f0fdf4,#dcfce7);
                                  border-color:#86efac;border-left-color:#15803d;}}
      .saved-prop-card .saved-prop-headline {{font-size:1rem;font-weight:700;
                                               color:#0a1a3e;margin-bottom:.25rem;}}
      .saved-prop-card .saved-prop-meta {{font-size:.85rem;color:#475569;}}
      .saved-prop-card code {{background:#fff;padding:1px 4px;border-radius:3px;
                              font-size:.8rem;}}
      .tag-pill {{display:inline-block;padding:.1rem .5rem;border-radius:10px;
                  font-size:.7rem;font-weight:600;color:#fff;
                  margin-right:.25rem;margin-bottom:.15rem;}}
    </style>
    """,
    unsafe_allow_html=True,
)


@contextmanager
def _warehouse(*, readonly: bool = True) -> Iterator[Any]:
    with warehouse_ctx(readonly=readonly) as wh:
        yield wh


# ---------------------------------------------------------------------------
# HEADER
# ---------------------------------------------------------------------------

st.markdown("# 🏛 Pipeline Architect")


@st.cache_data(ttl=30, show_spinner=False)
def _arch_live_counts() -> tuple[int, int, int, int]:
    """Live counts for the hero blurb so it never lies on empty / mid-load
    state.  Returns (datasets, fields, routing_rules, catalog_version)."""
    try:
        with warehouse_ctx(readonly=True) as wh:
            ds = list(
                wh.query(
                    f"SELECT COUNT(*) c, COALESCE(MAX(catalog_version),0) v "
                    f"FROM {CONTROL_SCHEMA}.global_bronze_catalog_datasets"
                )
            )[0]
            fd = list(
                wh.query(f"SELECT COUNT(*) c FROM {CONTROL_SCHEMA}.global_bronze_catalog_fields")
            )[0]["c"]
            rr = list(wh.query(f"SELECT COUNT(*) c FROM {CONTROL_SCHEMA}.onprem_routing_rules"))[0][
                "c"
            ]
            return int(ds["c"] or 0), int(fd or 0), int(rr or 0), int(ds["v"] or 0)
    except Exception:
        return 0, 0, 0, 0


_arch_ds, _arch_fd, _arch_rr, _arch_ver = _arch_live_counts()
if _arch_ds == 0 and _arch_fd == 0:
    _arch_catalog_phrase = (
        "Catalog: <strong>empty</strong> "
        "(upload a Product Catalogue via the Data Model Designer to populate)"
    )
else:
    _arch_catalog_phrase = (
        f"Catalog v{_arch_ver}: <strong>{_arch_ds:,} datasets</strong> &middot; "
        f"<strong>{_arch_fd:,} fields</strong> &middot; "
        f"<strong>{_arch_rr:,} routing rules</strong>"
    )
st.markdown(
    f"""
    <div class="arch-hero">
      <strong>Gold-first multi-tenant pipelines from the Global Gold Catalog.</strong>
      Pick a dataset, the agent proposes a fully-resolved
      Bronze→Silver→Gold pipeline anchored to the catalog. One click
      deploys: Gold DDL, Silver+Gold dbt models, Airflow DAG, GX
      expectation suite, OnPrem routing rules. CLONE from a peer client
      when one exists; BUILD from catalog when it's the first.
      <div class="arch-meta">
        Backend: <strong>Claude Haiku 4.5 + deterministic builders</strong>
        &middot; {_arch_catalog_phrase}. PHI-safe: only metadata reaches the LLM.
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)

# ═════════════════════════════════════════════════════════════════════════════
# Phase 17.7 — Fleet Console (Sections 1 + 2)
#
# These render at the top of the page regardless of which client is picked,
# because they're a FLEET view (factory catalog + per-(client,dataset)
# instance registry).  The existing per-client AI-Architect wizard renders
# BELOW this and stays unchanged — Phase 17.8 will slim it down.
#
# Architecture (per Phase 17.7 design doc):
#   * Factory pattern — one DAG per (client, dataset), generated by 33
#     dataset-sliced factory files in `dags/`.
#   * Instances live in CONTROL.client_pipeline_instances; the factories
#     loop over LIVE rows at scheduler-load time.
#   * Adding a client = INSERT one row per dataset.  Zero code change.
#   * Adding a dataset = ONE new factory file (auto-generated by
#     scripts/regenerate_dag_factories.py).
# ═════════════════════════════════════════════════════════════════════════════

# Bootstrap CONTROL tables — idempotent.  Phase 18 adds
# CONTROL.dataset_pipeline_runs for run-history; this call wires it up
# on first page load without a separate migration step.
with warehouse_ctx(readonly=False) as _wh_boot:
    create_control_tables(_wh_boot)

_pa_snap = _pa.get_snapshot()
_dmd_snap = _dmd.get_snapshot()


# ── Section 1 — 🌍 Factory Catalog ────────────────────────────────────────
st.markdown("## 🌍 Factory Catalog")
st.caption(
    "One row per Bronze dataset.  Each row corresponds to a factory file "
    "in `dags/_factory_<dataset>.py` that generates ONE Airflow DAG per LIVE "
    "row in `CONTROL.client_pipeline_instances`. Use the **♻️ Regenerate** "
    "button to refresh a factory file from the latest schema."
)

with st.expander("📊 Show / hide factory catalog", expanded=True):
    _factories = _pa_snap.factories_summary()
    if not _factories:
        st.warning(
            "No Bronze datasets in the catalog yet — run "
            "`python3 scripts/load_product_catalog.py` to seed them."
        )
    else:
        # Top-line metrics
        _m1, _m2, _m3, _m4 = st.columns(4)
        _m1.metric("📚 Datasets", len(_factories))
        _m2.metric("🏭 Factory files", sum(1 for f in _factories if f["factory_exists"]))
        _m3.metric(
            "🟢 LIVE instances",
            sum(f["live_count"] for f in _factories),
        )
        _m4.metric(
            "⏸ PAUSED instances",
            sum(f["paused_count"] for f in _factories),
        )

        # Grid
        _fc_view = [
            {
                "Dataset": f["display_name"],
                "Code": f["dataset_code"],
                "Category": f["category"],
                "Factory file": "✅" if f["factory_exists"] else "❌ missing",
                "Total": f["total_instances"],
                "🟢 LIVE": f["live_count"],
                "⏸ PAUSED": f["paused_count"],
                "📝 DRAFT": f["draft_count"],
                "🗄 ARCHIVED": f["archived_count"],
            }
            for f in _factories
        ]
        st.dataframe(
            pd.DataFrame(_fc_view),
            use_container_width=True,
            hide_index=True,
            height=420,
        )


# ── Section 2 — 🏭 Instance Fleet (per-row expanders) ─────────────────────
# Phase 17.10 — clean fleet view.  No more table-then-dropdown-then-action
# dance.  Each instance is a self-contained expander showing:
#   * one-line summary (client / dataset / status pill / schedule / last run)
#   * inline state-aware actions (Pause/Resume/Archive/View Schema/Run now)
#   * 5-most-recent run history right inside the expander
#
# Mechanical 🚀 Make LIVE button is removed — the AI Architect flow below
# is the canonical deploy path.  DRAFT rows show a "→ Design via AI
# Architect" pointer that scrolls down to the design surface.
st.markdown("## 🏭 Instance Fleet")
st.caption(
    "Every deployed pipeline instance + its run history.  Manage operational "
    "state (pause / archive / trigger a run) here.  To **design** a new "
    "pipeline use 🤖 **AI Architect** below."
)

with st.expander("📊 Show / hide instance fleet", expanded=True):
    # ── Filters ────────────────────────────────────────────────────────
    _all_clients = sorted(
        {str(r.get("client_id")) for r in _pa_snap.pipeline_instances if r.get("client_id")}
    )
    _all_datasets = sorted(
        {str(r.get("dataset_code")) for r in _pa_snap.pipeline_instances if r.get("dataset_code")}
    )
    _all_statuses = ["LIVE", "PAUSED", "DRAFT", "PENDING_REVIEW", "ARCHIVED"]

    _f1, _f2, _f3 = st.columns(3)
    with _f1:
        _flt_client = st.multiselect(
            "Filter by client",
            options=_all_clients,
            default=[],
            placeholder="All clients (default)",
            key="pa_fleet_filter_client",
        )
    with _f2:
        _flt_dataset = st.multiselect(
            "Filter by dataset",
            options=_all_datasets,
            default=[],
            placeholder="All datasets (default)",
            key="pa_fleet_filter_dataset",
        )
    with _f3:
        _flt_status = st.multiselect(
            "Filter by status",
            options=_all_statuses,
            default=["LIVE", "PAUSED", "DRAFT", "PENDING_REVIEW"],
            key="pa_fleet_filter_status",
        )

    # ── Apply filters ──────────────────────────────────────────────────
    _rows = list(_pa_snap.pipeline_instances)
    if _flt_client:
        _rows = [r for r in _rows if str(r.get("client_id")) in _flt_client]
    if _flt_dataset:
        _rows = [r for r in _rows if str(r.get("dataset_code")) in _flt_dataset]
    if _flt_status:
        _rows = [r for r in _rows if str(r.get("status")) in _flt_status]

    # Sort: LIVE > PAUSED > DRAFT > PENDING_REVIEW > ARCHIVED, then by client+dataset
    _status_order = {"LIVE": 0, "PAUSED": 1, "DRAFT": 2, "PENDING_REVIEW": 3, "ARCHIVED": 4}
    _rows.sort(
        key=lambda r: (
            _status_order.get(str(r.get("status")), 9),
            str(r.get("client_id")),
            str(r.get("dataset_code")),
        )
    )

    if not _rows:
        st.info(
            "📭 No pipeline instances match the current filters.  "
            "Use 🤖 **AI Architect** above to design + deploy a pipeline."
        )
    else:
        # ── Per-row expanders ─────────────────────────────────────────
        _STATUS_PILL = {
            "LIVE": "🟢 LIVE",
            "PAUSED": "⏸ PAUSED",
            "DRAFT": "📝 DRAFT",
            "PENDING_REVIEW": "🟡 PENDING_REVIEW",
            "ARCHIVED": "🗄 ARCHIVED",
        }
        _RUN_PILL = {
            "SUCCESS": "✅",
            "FAILED": "❌",
            "RUNNING": "🟡",
        }

        for _row in _rows:
            _iid = str(_row.get("instance_id"))
            _status = str(_row.get("status"))
            _runs = _pa_snap.recent_runs_by_instance.get(_iid, [])

            # Last-run summary string
            if _runs:
                _last = _runs[0]
                _last_pill = _RUN_PILL.get(str(_last.get("status")), "·")
                _last_when = str(_last.get("started_at") or "")[:19]
                _last_dur = (
                    f"{int(_last.get('duration_ms') or 0):,} ms"
                    if _last.get("duration_ms")
                    else "—"
                )
                _last_summary = f"last run: {_last_pill} {_last_when} ({_last_dur})"
            else:
                _last_summary = "last run: never"

            _exp_label = (
                f"{_row.get('client_id')} / {_row.get('dataset_code')}  ·  "
                f"{_STATUS_PILL.get(_status, _status)}  ·  "
                f"sched `{_row.get('schedule_cron') or '—'}`  ·  "
                f"{_last_summary}"
            )

            with st.expander(_exp_label, expanded=False):
                # ── Inline action buttons ────────────────────────────
                ac1, ac2, ac3, ac4 = st.columns([1.3, 1, 1, 1.3])

                with ac1:
                    # State-aware first action
                    if _status == "DRAFT":
                        st.markdown(
                            '<a href="#ai-architect-propose-deploy-pipeline" '
                            'style="display:inline-block;background:#7c3aed;'
                            "color:#fff;padding:.45rem .9rem;border-radius:6px;"
                            "text-decoration:none;font-weight:600;text-align:center;"
                            'width:100%;">🤖 Design via AI Architect ↓</a>',
                            unsafe_allow_html=True,
                        )
                    elif _status == "PENDING_REVIEW":
                        st.button(
                            "🟡 PENDING_REVIEW",
                            use_container_width=True,
                            disabled=True,
                            key=f"pa_pr_{_iid}",
                            help="Proposal awaiting deploy.  Re-open it in "
                            "🤖 AI Architect above to approve.",
                        )
                    elif _status == "LIVE":
                        if st.button(
                            "⏸ Pause",
                            key=f"pa_pause_{_iid}",
                            use_container_width=True,
                            help="Flip to PAUSED.  Scheduler skips this row "
                            "until you Resume.  Physical tables preserved.",
                        ):
                            _pa.update_instance_status(_iid, "PAUSED")
                            st.toast(f"⏸ Paused {_row['client_id']}/{_row['dataset_code']}")
                            st.rerun()
                    elif _status == "PAUSED":
                        if st.button(
                            "▶ Resume",
                            key=f"pa_resume_{_iid}",
                            type="primary",
                            use_container_width=True,
                            help="Flip back to LIVE.  Scheduler picks it up " "on next reload.",
                        ):
                            _pa.update_instance_status(_iid, "LIVE")
                            st.toast(f"▶ Resumed {_row['client_id']}/{_row['dataset_code']}")
                            st.rerun()
                    else:  # ARCHIVED
                        st.button(
                            "🗄 ARCHIVED",
                            use_container_width=True,
                            disabled=True,
                            key=f"pa_arch_disabled_{_iid}",
                            help="Read-only.",
                        )

                with ac2:
                    if _status != "ARCHIVED":
                        if st.button(
                            "🗄 Archive",
                            key=f"pa_archive_{_iid}",
                            use_container_width=True,
                            help="Soft-delete.  Row stays for audit; factory "
                            "stops emitting a DAG for it.",
                        ):
                            _pa.update_instance_status(_iid, "ARCHIVED")
                            st.toast(f"🗄 Archived {_row['client_id']}/{_row['dataset_code']}")
                            st.rerun()
                    else:
                        st.button(
                            "🗄 Archived",
                            use_container_width=True,
                            disabled=True,
                            key=f"pa_archive_disabled_{_iid}",
                        )

                with ac3:
                    _dmd_href = (
                        f"/Data_Model_Designer?client={_row.get('client_id')}"
                        f"&dataset={_row.get('dataset_code')}"
                    )
                    st.markdown(
                        f'<a href="{_dmd_href}" target="_self" '
                        f'style="display:inline-block;background:#1d4ed8;color:#fff;'
                        f"padding:.45rem .9rem;border-radius:6px;text-decoration:none;"
                        f'font-weight:600;text-align:center;width:100%;">'
                        f"👁 View Schema</a>",
                        unsafe_allow_html=True,
                    )

                with ac4:
                    # ▶ Run now — only enabled for LIVE
                    if _status != "LIVE":
                        st.button(
                            "▶ Run now",
                            use_container_width=True,
                            disabled=True,
                            key=f"pa_run_disabled_{_iid}",
                            help="Run now is only available for LIVE instances.",
                        )
                    else:
                        if st.button(
                            "▶ Run now",
                            key=f"pa_run_{_iid}",
                            type="primary",
                            use_container_width=True,
                            help="Synchronously run Bronze→Silver→Gold for this " "instance.",
                        ):
                            from datalink.orchestration.run_now import run_now as _run_now

                            with st.spinner(
                                f"▶ Running {_row.get('client_id')}/{_row.get('dataset_code')} "
                                "— bootstrap → bronze_land → bronze_validate → "
                                "silver_dbt → gold_dbt …"
                            ):
                                try:
                                    _outcome = _run_now(instance=_row, triggered_by="ui:run_now")
                                    if _outcome.status == "SUCCESS":
                                        st.toast(
                                            f"✅ Run succeeded — Bronze {_outcome.rows_bronze or '?'}, "
                                            f"Silver {_outcome.rows_silver or '?'}, "
                                            f"Gold {_outcome.rows_gold or '?'} rows · "
                                            f"{_outcome.duration_ms} ms",
                                            icon="🚀",
                                        )
                                    else:
                                        st.toast(
                                            f"❌ Run FAILED at {_outcome.error_task or 'unknown'}",
                                            icon="🔥",
                                        )
                                except Exception as _exc:
                                    st.error(f"Run-now crashed: {type(_exc).__name__}: {_exc}")
                            _pa.invalidate()
                            st.rerun()

                # ── Compact metadata row ──────────────────────────────
                st.markdown(
                    f"<span style='color:#64748b;font-size:.82rem'>"
                    f"instance_id `{_iid}` · "
                    f"anchor `{_row.get('bronze_anchor') or '—'}` · "
                    f"deployed_at `{str(_row.get('deployed_at') or '—')[:19]}` · "
                    f"created_by `{_row.get('created_by') or '—'}`"
                    f"</span>",
                    unsafe_allow_html=True,
                )

                # ── Inline run history ────────────────────────────────
                if _runs:
                    st.markdown("**📜 Run history** (5 most recent)")
                    _hist_view = []
                    for r in _runs:
                        _hist_view.append(
                            {
                                "Status": _RUN_PILL.get(str(r.get("status")), str(r.get("status")))
                                + " "
                                + str(r.get("status")),
                                "Started": str(r.get("started_at"))[:19],
                                "Duration": (
                                    f"{int(r.get('duration_ms') or 0):,} ms"
                                    if r.get("duration_ms")
                                    else "—"
                                ),
                                "Bronze": r.get("rows_bronze") or "—",
                                "Silver": r.get("rows_silver") or "—",
                                "Gold": r.get("rows_gold") or "—",
                                "Failed at": r.get("error_task") or "—",
                                "Triggered by": r.get("triggered_by") or "—",
                                "Run id": str(r.get("run_id"))[:8],
                            }
                        )
                    st.dataframe(
                        pd.DataFrame(_hist_view),
                        use_container_width=True,
                        hide_index=True,
                        height=min(220, 80 + 35 * len(_hist_view)),
                    )
                    _last_err = next(
                        (r for r in _runs if r.get("status") == "FAILED"),
                        None,
                    )
                    if _last_err and _last_err.get("error_message"):
                        with st.expander(
                            f"🔍 Last failure detail "
                            f"(run `{str(_last_err.get('run_id'))[:8]}`, "
                            f"failed at `{_last_err.get('error_task')}`)",
                            expanded=False,
                        ):
                            st.code(
                                str(_last_err.get("error_message"))[:6000],
                                language="text",
                            )
                else:
                    st.caption(
                        "_No runs yet — click **▶ Run now** above (LIVE only) "
                        "to materialize Bronze/Silver/Gold tables._"
                    )


# ═════════════════════════════════════════════════════════════════════════════
# Phase 17.8 — page deliberately ends here.
#
# The old AI Architect wizard (require_client gate, propose-pipeline flow,
# Global Gold Catalog browser, artifact previews, OnPrem routing planner,
# proposal history, overflow-columns panel) is GONE.  Schema authoring
# lives in Data Model Designer.  Cloning lives in DMD's Cloning Center.
# Pipeline Architect is now the FLEET CONSOLE — Factory Catalog + Instance
# Fleet + ▶ Run now + run history.  Every section above is a fully
# functional surface; nothing is staged for a 'next click.'
# ═════════════════════════════════════════════════════════════════════════════


st.markdown("---")


# ── Section 3 — 🤖 AI Architect: Propose, Review, Deploy ──────────────────
# Anthropic Claude reads the (client, dataset) context — Bronze catalog
# row, DMD's LIVE Silver+Gold, peer-client pipelines, default routing —
# and proposes the orchestration plan: schedule, Bronze anchor, GX suite,
# push targets, plus the actual artifact texts (Silver dbt, Gold dbt,
# Airflow DAG, GX suite spec, Gold DDL).  The operator REVIEWS the AI's
# work in tabs, then approves with one click — at which point
# approve_and_deploy() commits the DRAFT, executes the DDLs against
# Snowflake, writes the artifact files, and flips status to LIVE.
#
# This is the intelligence — the human reviews; the AI does the hard work.
st.markdown("## 🤖 AI Architect: Propose & Deploy Pipeline")
st.caption(
    "Anthropic Claude proposes the orchestration plan + Airflow DAG + "
    "artifacts for any (client, dataset) where Data Model Designer has "
    "Silver+Gold **LIVE**.  You review the AI's reasoning + artifacts, "
    "then **🚀 Approve & Deploy** — schemas, tables, dbt models, GX suite, "
    "DAG file all land in one click."
)

with st.expander("📊 Show / hide AI Architect", expanded=True):
    # Eligible pairs: DMD LIVE Silver+Gold without a non-archived instance.
    _ai_pairs = _pa_snap.live_dmd_pairs_without_instance(
        _dmd_snap.silver_schemas, _dmd_snap.gold_schemas
    )
    if not _ai_pairs:
        # Diagnostic: separate "DMD has no LIVE pairs" vs "every pair is
        # already deployed" so the operator knows EXACTLY what to do.
        _dmd_live_pairs = {
            (str(s.get("scope_owner")), str(s.get("dataset_code")))
            for s in _dmd_snap.silver_schemas
            if str(s.get("status")) == "LIVE"
            and str(s.get("scope_owner") or "") not in ("GLOBAL_CORP", "__global__", "")
        } & {
            (str(g.get("scope_owner")), str(g.get("dataset_code")))
            for g in _dmd_snap.gold_schemas
            if str(g.get("status")) == "LIVE"
            and str(g.get("scope_owner") or "") not in ("GLOBAL_CORP", "__global__", "")
        }
        if not _dmd_live_pairs:
            st.info(
                "📭 **No LIVE Silver+Gold pairs in Data Model Designer yet.**\n\n"
                "**Next step**: open **Data Model Designer**, clone a Global "
                "schema to a client via the **Cloning Center** → 🚀 **Save & "
                "Make LIVE**, then come back here to let the AI Architect "
                "design the pipeline."
            )
        else:
            st.info(
                f"📭 **All {len(_dmd_live_pairs)} LIVE pair"
                f"{'s' if len(_dmd_live_pairs) != 1 else ''} already deployed "
                "as LIVE / PAUSED pipelines** — nothing left for AI to "
                "propose.\n\nUse 🏭 **Instance Fleet** below to manage them, "
                "or archive an existing instance to free its slot for re-design."
            )
    else:
        # ── Step 1: pick a pair ──────────────────────────────────────
        _pair_labels = [f"{c} / {d}" for c, d in _ai_pairs]
        _pair_index_by_label = {f"{c} / {d}": (c, d) for c, d in _ai_pairs}
        _ai_pick = st.selectbox(
            "Pick a (client, dataset) to propose for",
            options=["— select —", *_pair_labels],
            index=0,
            key="pa_ai_pick",
            help="Only pairs with Silver+Gold LIVE in DMD and no existing "
            "pipeline instance show here.",
        )

        # Session state cache: proposals keyed by (client, dataset).
        if "pa_proposals" not in st.session_state:
            st.session_state["pa_proposals"] = {}

        if _ai_pick != "— select —":
            _ai_cli, _ai_ds = _pair_index_by_label[_ai_pick]
            _proposal_key = (_ai_cli, _ai_ds)

            # ── Step 2: AI proposal trigger / view cached ───────────
            _btn_col, _info_col = st.columns([1, 3])
            with _btn_col:
                _do_propose = st.button(
                    "🤖 Propose with AI"
                    if _proposal_key not in st.session_state["pa_proposals"]
                    else "🔁 Re-propose",
                    key=f"pa_propose_{_ai_cli}_{_ai_ds}",
                    type="primary",
                    use_container_width=True,
                    help="Calls Anthropic Claude to design the orchestration "
                    "plan + render Airflow DAG + Silver/Gold dbt models + "
                    "GX suite.  Costs ~1-3K tokens (~$0.005).",
                )
            with _info_col:
                if _proposal_key in st.session_state["pa_proposals"]:
                    _p = st.session_state["pa_proposals"][_proposal_key]
                    st.caption(
                        f"_Proposal cached · {_p.get('tokens_used', 0):,} tokens · "
                        f"{_p.get('duration_ms', 0)} ms · "
                        f"{_p.get('proposed_at', '')[:19]}_"
                    )

            if _do_propose:
                with st.spinner(
                    f"🤖 Anthropic is designing the {_ai_cli}/{_ai_ds} "
                    "pipeline — schedule, anchor, GX suite, push targets, "
                    "Silver/Gold dbt, Airflow DAG…"
                ):
                    try:
                        from datalink.config.loader import load_settings

                        _llm = get_llm(load_settings())
                        with warehouse_ctx(readonly=True) as _wh_p:
                            _proposal = propose_pipeline(
                                llm=_llm,
                                warehouse=_wh_p,
                                client_id=_ai_cli,
                                dataset_code=_ai_ds,
                                bronze_anchor="FLAT_FILE",
                                actor="ui:pipeline_architect",
                            )
                        _proposal["proposed_at"] = datetime.utcnow().isoformat()
                        st.session_state["pa_proposals"][_proposal_key] = _proposal
                        st.rerun()
                    except PipelinePrerequisitesError as _exc:
                        st.error(
                            f"❌ Prerequisites not met: {_exc}.  Author "
                            "Silver+Gold LIVE in Data Model Designer first."
                        )
                    except Exception as _exc:
                        st.error(f"AI proposal failed: {type(_exc).__name__}: {_exc}")

            # ── Step 3: Review the proposal ─────────────────────────
            if _proposal_key in st.session_state["pa_proposals"]:
                _p = st.session_state["pa_proposals"][_proposal_key]

                st.markdown("---")
                st.markdown(f"### 📋 Proposal · `{_ai_cli}` / `{_ai_ds}`")

                _tab_plan, _tab_rationale, _tab_silver, _tab_gold, _tab_dag, _tab_gx = st.tabs(
                    [
                        "📋 Plan",
                        "🤖 AI Rationale",
                        "🥈 Silver dbt",
                        "🥇 Gold dbt + DDL",
                        "🪂 Airflow DAG",
                        "✅ GX suite",
                    ]
                )

                with _tab_plan:
                    _plan_rows = [
                        ("Bronze anchor", _p.get("bronze_anchor", "—")),
                        ("Decision mode", _p.get("decision_mode", "—")),
                        ("Schedule (cron)", _p.get("schedule_cron", "—")),
                        ("Silver pattern", _p.get("silver_pattern", "—")),
                        (
                            "GX suite",
                            (_p.get("gx_suite") or {}).get("suite_id", "—"),
                        ),
                        (
                            "OnPrem targets",
                            ", ".join(
                                t.get("target_kind", "?") for t in (_p.get("routing_targets") or [])
                            )
                            or "(none)",
                        ),
                        (
                            "Tokens used",
                            f"{int(_p.get('tokens_used') or 0):,}",
                        ),
                        (
                            "Latency",
                            f"{int(_p.get('duration_ms') or 0):,} ms",
                        ),
                    ]
                    st.dataframe(
                        pd.DataFrame(_plan_rows, columns=["Field", "Value"]),
                        use_container_width=True,
                        hide_index=True,
                    )

                with _tab_rationale:
                    _narr = (
                        _p.get("ai_reasoning")
                        or _p.get("ai_narrative")
                        or _p.get("rationale")
                        or ""
                    )
                    if _narr:
                        st.markdown(_narr)
                    else:
                        st.caption(
                            "_(Agent did not return a top-level narrative — "
                            "see decision rationale embedded in artifacts.)_"
                        )

                with _tab_silver:
                    _silver_models = _p.get("silver_dbt_models") or {}
                    if _silver_models:
                        for _fn, _sql in _silver_models.items():
                            st.caption(f"`dbt/models/silver/{_ai_cli}/{_ai_ds}/{_fn}`")
                            st.code(_sql, language="sql")
                    else:
                        _legacy = _p.get("silver_dbt_sql") or ""
                        if _legacy:
                            st.code(_legacy, language="sql")
                        else:
                            st.caption("_(No Silver dbt model proposed.)_")

                with _tab_gold:
                    if _p.get("gold_ddl"):
                        st.caption(f"`datalink/pipeline/gold/ddl/{_ai_cli}_{_ai_ds}.sql`")
                        st.code(_p["gold_ddl"], language="sql")
                    if _p.get("gold_dbt_sql"):
                        st.caption(f"`dbt/models/gold/{_ai_cli}/{_ai_ds}.sql`")
                        st.code(_p["gold_dbt_sql"], language="sql")

                with _tab_dag:
                    if _p.get("airflow_dag_py"):
                        st.caption(
                            f"`dags/{_ai_cli}_{_ai_ds}_pipeline.py`  "
                            "_(legacy hand-written shape; the dataset-factory "
                            f"`dags/_factory_{_ai_ds}.py` ALSO emits a DAG "
                            "for this row at scheduler load — both paths are "
                            "compatible.)_"
                        )
                        st.code(_p["airflow_dag_py"], language="python")
                    else:
                        st.caption("_(No Airflow DAG body proposed.)_")

                with _tab_gx:
                    _gx = _p.get("gx_suite") or {}
                    if _gx:
                        st.json(_gx, expanded=False)
                    else:
                        st.caption("_(No GX suite proposed.)_")

                # ── Step 4: Approve & Deploy ────────────────────────
                st.markdown("---")
                _ap1, _ap2, _ap3 = st.columns([2, 2, 1.5])
                with _ap1:
                    _deploy_notes = st.text_input(
                        "Deploy notes (optional)",
                        value="",
                        key=f"pa_deploy_notes_{_ai_cli}_{_ai_ds}",
                        placeholder="e.g., 'initial production cut'",
                    )
                with _ap2:
                    st.markdown("")  # spacer
                    if st.button(
                        "🚀 Approve & Deploy",
                        key=f"pa_approve_{_ai_cli}_{_ai_ds}",
                        type="primary",
                        use_container_width=True,
                        help="Persist the proposal as a DRAFT instance, then "
                        "execute approve_and_deploy(): emits all 5 artifacts "
                        "(Bronze DDL, Gold DDL, Silver+Gold dbt, Airflow DAG, "
                        "GX suite), CREATE SCHEMA, runs Bronze DDL, flips "
                        "status to LIVE.",
                    ):
                        with st.spinner(
                            f"🚀 Deploying {_ai_cli}/{_ai_ds} — replacing "
                            "any stub DRAFT, persisting AI proposal, writing "
                            "artifacts, executing DDLs, flipping LIVE…"
                        ):
                            try:
                                # Phase 17.9 — if a stub DRAFT row exists
                                # for this (client, dataset), drop it first
                                # so the AI proposal can land cleanly.
                                _stub = _pa_snap.find_instance_by_pair(_ai_cli, _ai_ds)
                                if _stub and str(_stub.get("status")) == "DRAFT":
                                    _pa.delete_stub_instance(str(_stub.get("instance_id")))

                                with warehouse_ctx(readonly=False) as _wh_d:
                                    _new_iid = persist_proposal(
                                        warehouse=_wh_d,
                                        proposal=_p,
                                        actor="ui:pipeline_architect",
                                        notes=_deploy_notes or "",
                                    )
                                    _deploy_result = approve_and_deploy(
                                        warehouse=_wh_d,
                                        instance_id=_new_iid,
                                        proposal=_p,
                                        actor="ui:pipeline_architect",
                                        notes=_deploy_notes or "",
                                    )
                                st.toast(
                                    f"🚀 Deployed `{_new_iid[:8]}` — " f"5 artifacts + LIVE",
                                    icon="🚀",
                                )
                                # Clear the cached proposal so picker resets.
                                st.session_state["pa_proposals"].pop(_proposal_key, None)
                                _pa.invalidate()
                                _dmd.invalidate()
                                st.rerun()
                            except Exception as _exc:
                                st.error(f"Deploy failed: {type(_exc).__name__}: {_exc}")
                with _ap3:
                    st.markdown("")
                    if st.button(
                        "❌ Discard proposal",
                        key=f"pa_discard_{_ai_cli}_{_ai_ds}",
                        use_container_width=True,
                        help="Drop the cached proposal without deploying.",
                    ):
                        st.session_state["pa_proposals"].pop(_proposal_key, None)
                        st.rerun()

    # Eligible-but-deployed-pending list — DRAFT instances that need a Make-LIVE.
    _drafts = _pa_snap.instances_for(status="DRAFT")
    if _drafts:
        st.markdown("---")
        st.warning(
            f"⏳ **{len(_drafts)} DRAFT instance"
            f"{'s' if len(_drafts) != 1 else ''} pending deployment.**  "
            "These rows exist as metadata but haven't materialized physical "
            "infrastructure yet.  In 🏭 Instance Fleet below, pick one and "
            "click **🚀 Make LIVE** for a mechanical deploy, OR use the "
            "AI proposal flow above (which goes DRAFT→LIVE in one click)."
        )
