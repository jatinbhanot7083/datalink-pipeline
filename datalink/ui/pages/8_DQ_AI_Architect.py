"""DQ AI Architect — Phase 19.2 (factory-pattern, suite-level).

The page where Anthropic Claude proposes a complete Great Expectations
suite for a (dataset_code, layer) tuple — UNIVERSAL across clients.
Operator reviews + approves; the LIVE suite then runs against every
client's pipeline for that dataset/layer.

Flow:
  1. Pick (dataset, layer) — no client filter (suite is universal).
  2. Tune AI controls — temperature, grounding mode, max expectations.
  3. 🤖 Propose with AI → ~10-20 sec → returns ≤30 expectations.
  4. Review:
       📋 Plan          — header table (dim breakdown, severity, AI cost)
       🤖 Rationale     — AI's narrative
       ✅ Expectations  — full list with per-check rationale
  5. 🚀 Approve & Promote → DRAFT → LIVE (archives prior LIVE for the pair).

State machine on the suite (CONTROL.dq_suites):
  DRAFT → PENDING_REVIEW (optional) → LIVE → ARCHIVED
"""

from __future__ import annotations

import sys
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

st.set_page_config(
    page_title="DQ AI Architect",
    page_icon="🧪",
    layout="wide",
    initial_sidebar_state="expanded",
)

from datalink.agents.dq_ai_architect import (  # noqa: E402
    approve_and_promote,
    persist_suite,
    propose_suite,
)
from datalink.agents.llm_router import get_llm  # noqa: E402
from datalink.config.loader import load_settings  # noqa: E402
from datalink.quality.control import CONTROL_SCHEMA, create_control_tables  # noqa: E402
from datalink.ui._nav import render_sidebar  # noqa: E402
from datalink.ui._query import warehouse_ctx  # noqa: E402

render_sidebar(active="DQ AI Architect")

_NAVY = "#0a1a3e"
_GOLD = "#d4af37"
_PURPLE = "#7c3aed"
_GREEN = "#15803d"
_AMBER = "#b45309"
_RED = "#b91c1c"
_GREY = "#64748b"

st.markdown(
    f"""
    <style>
      h1 {{color: {_NAVY}; border-bottom: 3px solid {_GOLD}; padding-bottom: .4rem;}}
      h2 {{color: {_NAVY}; margin-top: 1.2rem;}}
      .dq-hero {{background:linear-gradient(90deg,{_NAVY}11,{_PURPLE}22);
                 padding:1rem 1.2rem;border-radius:8px;border-left:4px solid {_PURPLE};
                 margin:.6rem 0 1.4rem 0}}
      .dq-pill {{display:inline-block;padding:.15rem .55rem;border-radius:12px;
                 font-size:.72rem;font-weight:700;letter-spacing:.04em;margin-right:.4rem}}
      .dq-pill-completeness {{background:#dbeafe;color:#1e40af}}
      .dq-pill-uniqueness   {{background:#dcfce7;color:#166534}}
      .dq-pill-validity     {{background:#fef3c7;color:#92400e}}
      .dq-pill-consistency  {{background:#fce7f3;color:#9d174d}}
      .dq-pill-timeliness   {{background:#e0e7ff;color:#3730a3}}
      .dq-pill-accuracy     {{background:#d1fae5;color:#065f46}}
      .dq-pill-critical {{background:#fee2e2;color:#991b1b}}
      .dq-pill-high     {{background:#fef3c7;color:#92400e}}
      .dq-pill-medium   {{background:#dbeafe;color:#1e40af}}
      .dq-pill-low      {{background:#f3f4f6;color:{_GREY}}}
    </style>
    """,
    unsafe_allow_html=True,
)


@contextmanager
def _warehouse(*, readonly: bool = True) -> Iterator[Any]:
    with warehouse_ctx(readonly=readonly) as wh:
        yield wh


# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------
st.markdown("# 🧪 DQ AI Architect")
st.markdown(
    f"""
    <div class="dq-hero">
      <strong>Suite-level AI proposer for the factory pattern.</strong>
      Anthropic Claude designs ONE Great Expectations suite per
      <code>(dataset, layer)</code> — universal across every client running
      that dataset.  Bronze, Silver, Gold each get their own suite.  No
      per-client variation; no per-table picker.
      <div style="margin-top:.4rem;font-size:.85rem;color:{_GREY}">
        Backend: <strong>Claude Haiku 4.5 + dataset-aware schema RAG</strong>
        &middot; PHI-safe: only column metadata reaches the LLM, never row data.
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)


# Bootstrap CONTROL tables (idempotent)
with _warehouse(readonly=False) as _wh_boot:
    create_control_tables(_wh_boot)


# ---------------------------------------------------------------------------
# Session state
# ---------------------------------------------------------------------------
if "dq_proposals" not in st.session_state:
    st.session_state["dq_proposals"] = {}  # keyed by (dataset, layer) -> proposal dict


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
@st.cache_data(ttl=60)
def _list_datasets() -> list[dict[str, Any]]:
    with warehouse_ctx(readonly=True) as wh:
        try:
            rows = list(
                wh.query(
                    f"SELECT dataset_code, display_name, category "
                    f"FROM {CONTROL_SCHEMA}.global_bronze_catalog_datasets "
                    f"WHERE is_active = TRUE ORDER BY display_name"
                )
            )
        except Exception:
            rows = []
    return rows


@st.cache_data(ttl=60)
def _list_live_suites() -> list[dict[str, Any]]:
    with warehouse_ctx(readonly=True) as wh:
        try:
            rows = list(
                wh.query(
                    f"SELECT suite_id, suite_name, dataset_code, layer, version, status, "
                    f"       ai_token_count, ai_latency_ms, temperature, grounding_mode, "
                    f"       activated_at, created_at "
                    f"FROM {CONTROL_SCHEMA}.dq_suites "
                    f"WHERE dataset_code IS NOT NULL AND layer IS NOT NULL "
                    f"  AND status IN ('LIVE', 'DRAFT', 'PENDING_REVIEW') "
                    f"ORDER BY dataset_code, layer, status DESC, version DESC"
                )
            )
        except Exception:
            rows = []
    return rows


_DIMENSION_PILL = {
    "Completeness": "dq-pill-completeness",
    "Uniqueness": "dq-pill-uniqueness",
    "Validity": "dq-pill-validity",
    "Consistency": "dq-pill-consistency",
    "Timeliness": "dq-pill-timeliness",
    "Accuracy": "dq-pill-accuracy",
}
_SEVERITY_PILL = {
    "CRITICAL": "dq-pill-critical",
    "HIGH": "dq-pill-high",
    "MEDIUM": "dq-pill-medium",
    "LOW": "dq-pill-low",
}


# ===========================================================================
# Section 1 — Picker + AI controls
# ===========================================================================
st.markdown("## 🎯 Pick (dataset, layer) + AI controls")
st.caption(
    "One suite per `(dataset, layer)`.  Same suite runs against every "
    "client's pipeline for that dataset.  Bronze checks raw vendor format, "
    "Silver checks DV2 hub/sat integrity, Gold checks business-rule validity."
)

datasets = _list_datasets()
if not datasets:
    st.error(
        "Bronze catalog is empty.  Run `python3 scripts/load_product_catalog.py` "
        "to seed it first."
    )
    st.stop()

ds_options = sorted(d["dataset_code"] for d in datasets)
ds_display_map = {d["dataset_code"]: d.get("display_name") or d["dataset_code"] for d in datasets}

p1, p2, p3, p4, p5 = st.columns([2, 1, 1, 1.2, 1.2])
with p1:
    _ds = st.selectbox(
        "Dataset",
        options=["— select —", *ds_options],
        format_func=lambda c: f"{ds_display_map.get(c, c)} · `{c}`" if c != "— select —" else c,
        key="dq_ai_dataset",
    )
with p2:
    _layer = st.selectbox(
        "Layer",
        options=["BRONZE", "SILVER", "GOLD"],
        index=0,
        key="dq_ai_layer",
        help="Bronze = raw landing.  Silver = DV2-cleansed.  Gold = consumption.",
    )
with p3:
    _max_exps = st.number_input(
        "Max expectations",
        min_value=5,
        max_value=60,
        value=30,
        step=5,
        key="dq_ai_max",
        help="Cap on the suite size.  Default 30 keeps suites human-reviewable.",
    )
with p4:
    _temp = st.slider(
        "Temperature",
        min_value=0.0,
        max_value=1.0,
        value=0.0,
        step=0.1,
        key="dq_ai_temp",
        help="0.0 = deterministic.  Higher = more creative variation in checks proposed.",
    )
with p5:
    _grounding = st.selectbox(
        "Grounding mode",
        options=["off", "rag", "strict"],
        index=0,
        key="dq_ai_grounding",
        help="off = no RAG.  rag = inject standards-registry hits as guidance.  "
        "strict = require RAG hits before proposing.",
    )

_propose_btn_col, _cost_col = st.columns([1, 3])
with _propose_btn_col:
    _do_propose = st.button(
        "🤖 Propose with AI"
        if _ds == "— select —"
        else "🤖 Propose with AI"
        if (_ds, _layer) not in st.session_state["dq_proposals"]
        else "🔁 Re-propose",
        type="primary",
        use_container_width=True,
        disabled=(_ds == "— select —"),
        key="dq_ai_propose_btn",
        help="Calls Anthropic Claude to design the full suite.  Costs ~3-6K tokens (~$0.01).",
    )

with _cost_col:
    if _ds != "— select —" and (_ds, _layer) in st.session_state["dq_proposals"]:
        _p = st.session_state["dq_proposals"][(_ds, _layer)]
        st.caption(
            f"_Proposal cached · {_p.get('tokens_used', 0):,} tokens · "
            f"{_p.get('duration_ms', 0):,} ms · "
            f"proposed at {(_p.get('proposed_at') or '')[:19]}_"
        )

if _do_propose and _ds != "— select —":
    with st.spinner(
        f"🤖 Anthropic is designing the {_ds}/{_layer} suite — "
        "schema fetch → expectation balancing → rationale narrative…"
    ):
        try:
            settings = load_settings()
            llm = get_llm(settings)
            with _warehouse(readonly=True) as _wh_p:
                _p = propose_suite(
                    llm=llm,
                    warehouse=_wh_p,
                    dataset_code=_ds,
                    dataset_display=ds_display_map.get(_ds, _ds),
                    layer=_layer,
                    max_expectations=int(_max_exps),
                    temperature=float(_temp),
                    grounding_mode=_grounding,
                    actor="ui:dq_ai_architect",
                )
            st.session_state["dq_proposals"][(_ds, _layer)] = _p
            st.toast(
                f"🤖 Proposal ready — {_p.get('expectation_count')} expectations · "
                f"{_p.get('tokens_used', 0):,} tokens",
                icon="✨",
            )
            st.rerun()
        except Exception as exc:
            st.error(f"Propose failed: {type(exc).__name__}: {exc}")


st.markdown("---")


# ===========================================================================
# Section 2 — Review proposal
# ===========================================================================
if _ds != "— select —" and (_ds, _layer) in st.session_state["dq_proposals"]:
    _p = st.session_state["dq_proposals"][(_ds, _layer)]

    st.markdown(
        f"## 📋 Proposal · `{_p.get('suite_name', f'{_ds}_{_layer.lower()}')}` "
        f"<span style='color:{_GREY};font-size:.72rem;font-weight:400'>(DRAFT)</span>",
        unsafe_allow_html=True,
    )

    tab_plan, tab_rationale, tab_exps, tab_dims = st.tabs(
        ["📋 Plan", "🤖 AI Rationale", "✅ Expectations", "📊 Dimension breakdown"]
    )

    with tab_plan:
        plan_rows = [
            ("Suite name", _p.get("suite_name", "—")),
            ("Dataset", _p.get("dataset_code")),
            ("Layer", _p.get("layer")),
            ("Expectations proposed", _p.get("expectation_count")),
            ("Columns analysed", _p.get("column_count")),
            ("Business keys", ", ".join(_p.get("business_keys") or []) or "(none)"),
            ("PII columns", str(len(_p.get("pii_columns") or [])) + " flagged"),
            ("PHI columns", str(len(_p.get("phi_columns") or [])) + " flagged"),
            ("AI tokens used", f"{_p.get('tokens_used', 0):,}"),
            ("AI latency", f"{_p.get('duration_ms', 0):,} ms"),
            ("Temperature", _p.get("temperature")),
            ("Grounding mode", _p.get("grounding_mode")),
        ]
        st.dataframe(
            pd.DataFrame(plan_rows, columns=["Field", "Value"]),
            use_container_width=True,
            hide_index=True,
            height=min(440, 60 + 35 * len(plan_rows)),
        )

    with tab_rationale:
        rat = (_p.get("rationale") or "").strip()
        if rat:
            st.markdown(rat)
        else:
            st.caption("_(Agent did not return a top-level rationale.)_")

    with tab_exps:
        exps = _p.get("expectations") or []
        if not exps:
            st.warning("Proposal contains zero expectations.")
        else:
            for i, e in enumerate(exps):
                meta = e.get("meta", {})
                dim = str(meta.get("dq_dimension") or "Validity")
                sev = str(meta.get("severity") or "MEDIUM")
                kwargs = e.get("kwargs", {})
                col = kwargs.get("column") or kwargs.get("column_list") or "—"
                pill_dim = _DIMENSION_PILL.get(dim, "dq-pill-validity")
                pill_sev = _SEVERITY_PILL.get(sev, "dq-pill-medium")
                with st.container(border=True):
                    st.markdown(
                        f"<span class='dq-pill {pill_dim}'>{dim}</span>"
                        f"<span class='dq-pill {pill_sev}'>{sev}</span>"
                        f"<code>{e.get('expectation_type')}</code> on `{col}`",
                        unsafe_allow_html=True,
                    )
                    desc = meta.get("description") or ""
                    if desc:
                        st.markdown(f"**{desc}**")
                    rationale = meta.get("rationale") or ""
                    if rationale:
                        st.caption(rationale)
                    with st.expander("⚙️ kwargs", expanded=False):
                        st.json(kwargs)

    with tab_dims:
        breakdown = _p.get("dimension_breakdown") or {}
        if breakdown:
            df = pd.DataFrame(
                sorted(breakdown.items(), key=lambda kv: -kv[1]),
                columns=["Dimension", "# Expectations"],
            )
            st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.caption("_(no dimensions reported)_")

    st.markdown("---")

    # ── Approve & Promote ──────────────────────────────────────────────
    a1, a2, a3 = st.columns([2, 2, 1.5])
    with a1:
        _notes = st.text_input(
            "Approval notes (optional)",
            value="",
            key=f"dq_ai_notes_{_ds}_{_layer}",
            placeholder="e.g., 'initial production cut'",
        )
    with a2:
        st.markdown("")
        if st.button(
            "🚀 Approve & Promote (LIVE)",
            type="primary",
            use_container_width=True,
            key=f"dq_ai_approve_{_ds}_{_layer}",
            help="Persists the proposal as DRAFT, then archives any prior "
            "LIVE for this (dataset, layer) and promotes this one.",
        ):
            with st.spinner(f"🚀 Promoting {_ds}/{_layer} to LIVE…"):
                try:
                    with _warehouse(readonly=False) as _wh_d:
                        _new_id = persist_suite(
                            warehouse=_wh_d,
                            proposal=_p,
                            actor="ui:dq_ai_architect",
                            notes=_notes,
                        )
                        _result = approve_and_promote(
                            warehouse=_wh_d,
                            suite_id=_new_id,
                            actor="ui:dq_ai_architect",
                            notes=_notes,
                        )
                    st.toast(
                        f"🚀 LIVE — {_ds}/{_layer} v{_result.get('version')} promoted",
                        icon="✅",
                    )
                    st.session_state["dq_proposals"].pop((_ds, _layer), None)
                    _list_live_suites.clear()
                    st.rerun()
                except Exception as exc:
                    st.error(f"Promote failed: {type(exc).__name__}: {exc}")
    with a3:
        st.markdown("")
        if st.button(
            "❌ Discard proposal",
            use_container_width=True,
            key=f"dq_ai_discard_{_ds}_{_layer}",
            help="Drop the cached proposal without persisting.",
        ):
            st.session_state["dq_proposals"].pop((_ds, _layer), None)
            st.rerun()


# ===========================================================================
# Section 3 — Currently LIVE / DRAFT suites
# ===========================================================================
st.markdown("---")
st.markdown("## 📚 Suite Registry — current state")
st.caption(
    "Every suite per (dataset, layer).  Use **DQ Suite Registry** for "
    "filtering, drill-down, run-against-a-client, or version diff."
)

live_suites = _list_live_suites()
if not live_suites:
    st.info(
        "No DQ suites yet.  Use the picker above to design your first one — "
        "start with `membership / BRONZE` (the schema is most thoroughly "
        "annotated in the catalog)."
    )
else:
    sr_view = []
    for r in live_suites:
        status_pill = {
            "LIVE": "🟢 LIVE",
            "DRAFT": "📝 DRAFT",
            "PENDING_REVIEW": "🟡 PENDING_REVIEW",
        }.get(str(r.get("status")), str(r.get("status")))
        sr_view.append(
            {
                "Dataset": r.get("dataset_code"),
                "Layer": r.get("layer"),
                "Suite": r.get("suite_name"),
                "Status": status_pill,
                "v": r.get("version"),
                "Tokens": f"{int(r.get('ai_token_count') or 0):,}",
                "Latency": f"{int(r.get('ai_latency_ms') or 0):,} ms",
                "Temp": r.get("temperature") or 0,
                "Grounding": r.get("grounding_mode") or "off",
                "Activated": str(r.get("activated_at") or "—")[:19],
            }
        )
    st.dataframe(
        pd.DataFrame(sr_view),
        use_container_width=True,
        hide_index=True,
        height=min(420, 60 + 35 * len(sr_view)),
    )
