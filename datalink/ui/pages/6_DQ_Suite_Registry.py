"""DQ Suite Registry — Phase 19.3.

Fleet view of every DQ suite in the factory pattern: one row per
(dataset_code, layer, version) combination.  Filters by dataset / layer /
status.  No client filter — suites are universal across clients.

Per-row drill-down:
  * View expectations (the actual GX checks)
  * Promote DRAFT → LIVE / Archive a LIVE suite
  * Open in DQ Review for peer-review pattern
  * Run-against-a-client picker (Phase-19.4 wires the runtime)
"""

from __future__ import annotations

import json
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
    page_title="DQ Suite Registry",
    page_icon="📚",
    layout="wide",
    initial_sidebar_state="expanded",
)

from datalink.agents.dq_ai_architect import approve_and_promote  # noqa: E402
from datalink.quality.control import CONTROL_SCHEMA, create_control_tables  # noqa: E402
from datalink.ui._nav import render_sidebar  # noqa: E402
from datalink.ui._query import warehouse_ctx  # noqa: E402

render_sidebar(active="DQ Suite Registry")

_NAVY = "#0a1a3e"
_GOLD = "#d4af37"
_GREY = "#64748b"

st.markdown(
    f"""
    <style>
      h1 {{color: {_NAVY}; border-bottom: 3px solid {_GOLD}; padding-bottom: .4rem;}}
      h2 {{color: {_NAVY}; margin-top: 1.2rem;}}
      .sr-pill {{display:inline-block;padding:.15rem .55rem;border-radius:12px;
                 font-size:.72rem;font-weight:700;letter-spacing:.04em;margin-right:.4rem}}
    </style>
    """,
    unsafe_allow_html=True,
)


@contextmanager
def _warehouse(*, readonly: bool = True) -> Iterator[Any]:
    with warehouse_ctx(readonly=readonly) as wh:
        yield wh


with _warehouse(readonly=False) as _wh_boot:
    create_control_tables(_wh_boot)


st.markdown("# 📚 DQ Suite Registry")
st.caption(
    "Every DQ suite per (dataset, layer).  Suites are universal across "
    "clients — same suite runs against `BRONZE_AETNA.RAW_MEMBERSHIP`, "
    "`BRONZE_CIGNA.RAW_MEMBERSHIP`, etc."
)


# ---------------------------------------------------------------------------
# Snapshot
# ---------------------------------------------------------------------------
@st.cache_data(ttl=30)
def _all_suites() -> list[dict[str, Any]]:
    with warehouse_ctx(readonly=True) as wh:
        try:
            return list(
                wh.query(
                    f"""
                    SELECT suite_id, suite_name, dataset_code, layer, version, status,
                           expectations, dq_dimensions, ai_rationale,
                           ai_token_count, ai_latency_ms, temperature, grounding_mode,
                           created_by, created_at, activated_at, archived_at,
                           reviewed_by, reviewed_at, review_notes
                      FROM {CONTROL_SCHEMA}.dq_suites
                     WHERE dataset_code IS NOT NULL AND layer IS NOT NULL
                     ORDER BY dataset_code, layer, status DESC, version DESC
                    """
                )
            )
        except Exception:
            return []


@st.cache_data(ttl=60)
def _client_options() -> list[str]:
    with warehouse_ctx(readonly=True) as wh:
        try:
            rows = list(
                wh.query(
                    f"SELECT DISTINCT client_id FROM {CONTROL_SCHEMA}.client_pipeline_instances "
                    f"WHERE status <> 'ARCHIVED' ORDER BY client_id"
                )
            )
            return [str(r.get("client_id")) for r in rows if r.get("client_id")]
        except Exception:
            return []


suites = _all_suites()


# ---------------------------------------------------------------------------
# Filters + KPI tiles
# ---------------------------------------------------------------------------
all_datasets = sorted({str(s.get("dataset_code")) for s in suites if s.get("dataset_code")})
all_statuses = ["LIVE", "DRAFT", "PENDING_REVIEW", "ARCHIVED"]

m1, m2, m3, m4 = st.columns(4)
m1.metric("Total suites", len(suites))
m2.metric("🟢 LIVE", sum(1 for s in suites if str(s.get("status")) == "LIVE"))
m3.metric("📝 DRAFT", sum(1 for s in suites if str(s.get("status")) == "DRAFT"))
m4.metric("🗄 ARCHIVED", sum(1 for s in suites if str(s.get("status")) == "ARCHIVED"))

f1, f2, f3 = st.columns(3)
with f1:
    flt_dataset = st.multiselect(
        "Filter by dataset",
        options=all_datasets,
        default=[],
        placeholder="All datasets",
        key="sr_dataset",
    )
with f2:
    flt_layer = st.multiselect(
        "Filter by layer",
        options=["BRONZE", "SILVER", "GOLD"],
        default=[],
        placeholder="All layers",
        key="sr_layer",
    )
with f3:
    flt_status = st.multiselect(
        "Filter by status",
        options=all_statuses,
        default=["LIVE", "DRAFT", "PENDING_REVIEW"],
        key="sr_status",
    )


# Apply filters
rows = list(suites)
if flt_dataset:
    rows = [r for r in rows if str(r.get("dataset_code")) in flt_dataset]
if flt_layer:
    rows = [r for r in rows if str(r.get("layer")) in flt_layer]
if flt_status:
    rows = [r for r in rows if str(r.get("status")) in flt_status]


st.markdown("---")


# ---------------------------------------------------------------------------
# Per-row expanders
# ---------------------------------------------------------------------------
if not rows:
    st.info(
        "📭 No suites match the current filters.  "
        "Use **🧪 DQ AI Architect** to design your first suite."
    )
else:
    _STATUS_PILL = {
        "LIVE": "🟢 LIVE",
        "DRAFT": "📝 DRAFT",
        "PENDING_REVIEW": "🟡 PENDING_REVIEW",
        "ARCHIVED": "🗄 ARCHIVED",
    }

    for r in rows:
        suite_id = str(r.get("suite_id"))
        status = str(r.get("status"))
        ds = r.get("dataset_code")
        layer = r.get("layer")
        version = r.get("version")
        suite_name = r.get("suite_name")
        try:
            exps = json.loads(r.get("expectations") or "[]")
        except Exception:
            exps = []
        try:
            dims = json.loads(r.get("dq_dimensions") or "{}")
        except Exception:
            dims = {}

        label = (
            f"`{suite_name}` · v{version} · {_STATUS_PILL.get(status, status)} · "
            f"{len(exps)} expectations · "
            f"{int(r.get('ai_token_count') or 0):,} tokens · "
            f"created {(str(r.get('created_at') or ''))[:19]}"
        )

        with st.expander(label, expanded=False):
            # ── Action buttons ───────────────────────────────────────
            ac1, ac2, ac3 = st.columns([1.3, 1, 1.3])

            with ac1:
                if status in ("DRAFT", "PENDING_REVIEW"):
                    if st.button(
                        "🚀 Promote to LIVE",
                        key=f"sr_promote_{suite_id}",
                        type="primary",
                        use_container_width=True,
                        help="Archives any prior LIVE for this (dataset, layer) "
                        "and promotes this one.",
                    ):
                        try:
                            with _warehouse(readonly=False) as wh:
                                _r = approve_and_promote(
                                    warehouse=wh,
                                    suite_id=suite_id,
                                    actor="ui:dq_suite_registry",
                                )
                            st.toast(f"🚀 Promoted {ds}/{layer} v{version}", icon="✅")
                            _all_suites.clear()
                            st.rerun()
                        except Exception as exc:
                            st.error(f"Promote failed: {type(exc).__name__}: {exc}")
                elif status == "LIVE":
                    if st.button(
                        "🗄 Archive",
                        key=f"sr_archive_{suite_id}",
                        use_container_width=True,
                        help="Soft-delete.  Future runs won't use this suite "
                        "until a new version is promoted.",
                    ):
                        try:
                            with _warehouse(readonly=False) as wh:
                                wh.execute(
                                    f"UPDATE {CONTROL_SCHEMA}.dq_suites "
                                    f"SET status = 'ARCHIVED', archived_at = CURRENT_TIMESTAMP() "
                                    f"WHERE suite_id = $id",
                                    {"id": suite_id},
                                )
                            st.toast(f"🗄 Archived {ds}/{layer}", icon="📦")
                            _all_suites.clear()
                            st.rerun()
                        except Exception as exc:
                            st.error(f"Archive failed: {type(exc).__name__}: {exc}")
                else:
                    st.button(
                        "🗄 ARCHIVED",
                        use_container_width=True,
                        disabled=True,
                        key=f"sr_archived_disabled_{suite_id}",
                    )

            with ac2:
                # Quick deep-link to DQ Review
                st.markdown(
                    f'<a href="/DQ_Review?suite_id={suite_id}" target="_self" '
                    f'style="display:inline-block;background:#1d4ed8;color:#fff;'
                    f"padding:.45rem .9rem;border-radius:6px;text-decoration:none;"
                    f'font-weight:600;text-align:center;width:100%;">'
                    f"📝 Open in Review</a>",
                    unsafe_allow_html=True,
                )

            with ac3:
                # Run against a client picker — only meaningful for LIVE suites
                if status == "LIVE":
                    clients = _client_options()
                    pick = st.selectbox(
                        "Run against client",
                        options=["— pick a client —", *clients],
                        key=f"sr_runclient_{suite_id}",
                        label_visibility="collapsed",
                    )
                    if pick != "— pick a client —" and st.button(
                        f"▶ Run on {pick}",
                        key=f"sr_run_{suite_id}_{pick}",
                        type="primary",
                        use_container_width=True,
                        help=f"Fire suite against {layer}_{pick.upper()} "
                        f"physical table.  Wired in Pipeline Architect ▶ Run now.",
                    ):
                        st.info(
                            "⚙️ Today, suite runs are triggered via "
                            "**Pipeline Architect → ▶ Run now** (which calls "
                            "bronze_validate / silver_dq / gold_dq tasks).  "
                            "Phase 19.5 will wire a direct run-from-Registry button."
                        )

            # ── Tabs: Expectations / Rationale / Dimensions / Audit ───
            t1, t2, t3, t4 = st.tabs(
                ["✅ Expectations", "🤖 AI Rationale", "📊 Dimensions", "🗒 Audit"]
            )

            with t1:
                if not exps:
                    st.caption("_(no expectations recorded — possibly a legacy suite row)_")
                else:
                    for i, e in enumerate(exps):
                        meta = e.get("meta") or {}
                        st.markdown(
                            f"**{i+1}.** `{e.get('expectation_type')}` "
                            f"on `{(e.get('kwargs') or {}).get('column', '—')}`  "
                            f"· {meta.get('dq_dimension', '—')} / {meta.get('severity', '—')}"
                        )
                        if meta.get("description"):
                            st.caption(meta["description"])

            with t2:
                rat = (r.get("ai_rationale") or "").strip()
                if rat:
                    st.markdown(rat)
                else:
                    st.caption("_(no rationale recorded — likely manually authored)_")

            with t3:
                if dims:
                    df = pd.DataFrame(
                        sorted(dims.items(), key=lambda kv: -kv[1]),
                        columns=["Dimension", "# Expectations"],
                    )
                    st.dataframe(df, use_container_width=True, hide_index=True)
                else:
                    st.caption("_(no dimension breakdown recorded)_")

            with t4:
                st.markdown(f"**suite_id**: `{suite_id}`")
                st.markdown(
                    f"**created_by** `{r.get('created_by') or '—'}`  "
                    f"at `{str(r.get('created_at') or '')[:19]}`"
                )
                if r.get("activated_at"):
                    st.markdown(
                        f"**activated_at** `{str(r['activated_at'])[:19]}` "
                        f"by `{r.get('reviewed_by') or '—'}`"
                    )
                if r.get("archived_at"):
                    st.markdown(f"**archived_at** `{str(r['archived_at'])[:19]}`")
                st.markdown(
                    f"**Temperature**: `{r.get('temperature')}` · "
                    f"**Grounding**: `{r.get('grounding_mode')}` · "
                    f"**Tokens**: `{int(r.get('ai_token_count') or 0):,}` · "
                    f"**Latency**: `{int(r.get('ai_latency_ms') or 0):,} ms`"
                )
                if r.get("review_notes"):
                    st.markdown(f"**Review notes**: {r['review_notes']}")
