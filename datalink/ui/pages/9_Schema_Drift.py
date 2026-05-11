"""Schema Drift — Phase 19.5 (factory-pattern, dataset-scoped).

Per-dataset view of vendor-feed schema drift events.  Drift detection
already runs automatically on every ``bronze_land`` (datalink.pipeline.
bronze.ingest) — this page surfaces the queue for triage and offers a
hand-off to **🧪 DQ AI Architect** to revise the affected suite.

No client filter at the page level — drift is most actionable per
*dataset* (the canonical schema is universal; ALL clients of that
dataset see the same drift response).  Per-client drift instances ARE
shown inside each event so the operator can see which clients sent
deviating files.
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
    page_title="Schema Drift",
    page_icon="🌀",
    layout="wide",
    initial_sidebar_state="expanded",
)

from datalink.quality.control import CONTROL_SCHEMA, create_control_tables  # noqa: E402
from datalink.ui._nav import render_sidebar  # noqa: E402
from datalink.ui._query import warehouse_ctx  # noqa: E402

render_sidebar(active="Schema Drift")

_NAVY = "#0a1a3e"
_GOLD = "#d4af37"
_AMBER = "#b45309"
_RED = "#b91c1c"
_GREEN = "#15803d"

st.markdown(
    f"""
    <style>
      h1 {{color: {_NAVY}; border-bottom: 3px solid {_GOLD}; padding-bottom: .4rem;}}
      h2 {{color: {_NAVY}; margin-top: 1.2rem;}}
      .sd-pill {{display:inline-block;padding:.15rem .55rem;border-radius:12px;
                 font-size:.72rem;font-weight:700;letter-spacing:.04em;margin-right:.4rem}}
      .sd-pill-additive {{background:#dcfce7;color:#166534}}
      .sd-pill-removed  {{background:#fee2e2;color:#991b1b}}
      .sd-pill-typed    {{background:#fef3c7;color:#92400e}}
      .sd-pill-fatal    {{background:#fecaca;color:#7f1d1d}}
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


st.markdown("# 🌀 Schema Drift")
st.caption(
    "Vendor-feed schema deviations detected automatically on every Bronze "
    "ingest.  Triage by dataset; when a drift requires a new DQ check, "
    "click → **🧪 DQ AI Architect** to revise the affected suite."
)


# ---------------------------------------------------------------------------
# Data load
# ---------------------------------------------------------------------------
@st.cache_data(ttl=20)
def _drift_events(limit: int = 200) -> list[dict[str, Any]]:
    with warehouse_ctx(readonly=True) as wh:
        try:
            return list(
                wh.query(
                    f"""
                    SELECT drift_id, detected_at, client_id, source_type AS dataset_code,
                           source_file, batch_id, contract_version,
                           drift_type, severity,
                           added_columns, removed_columns, type_changes,
                           action_taken, notes
                      FROM {CONTROL_SCHEMA}.schema_drift_log
                     ORDER BY detected_at DESC
                     LIMIT {int(limit)}
                    """
                )
            )
        except Exception:
            return []


events = _drift_events()


# ---------------------------------------------------------------------------
# KPIs + filters
# ---------------------------------------------------------------------------
m1, m2, m3, m4 = st.columns(4)
m1.metric("Recent events", len(events))
m2.metric("🟥 Fatal (HALTED)", sum(1 for e in events if str(e.get("action_taken")) == "HALTED"))
m3.metric("🟧 Logged (additive)", sum(1 for e in events if str(e.get("action_taken")) == "LOGGED"))
m4.metric("Distinct datasets", len({e.get("dataset_code") for e in events}))

f1, f2, f3 = st.columns(3)
all_datasets = sorted({str(e.get("dataset_code")) for e in events if e.get("dataset_code")})
all_severities = sorted({str(e.get("severity")) for e in events if e.get("severity")})
all_actions = sorted({str(e.get("action_taken")) for e in events if e.get("action_taken")})
with f1:
    flt_ds = st.multiselect(
        "Filter by dataset",
        options=all_datasets,
        default=[],
        placeholder="All datasets",
        key="sd_ds",
    )
with f2:
    flt_sev = st.multiselect(
        "Filter by severity",
        options=all_severities or ["INFO", "WARNING", "ERROR"],
        default=[],
        placeholder="All severities",
        key="sd_sev",
    )
with f3:
    flt_action = st.multiselect(
        "Filter by action",
        options=all_actions or ["LOGGED", "HALTED"],
        default=[],
        placeholder="All actions",
        key="sd_action",
    )

rows = list(events)
if flt_ds:
    rows = [r for r in rows if str(r.get("dataset_code")) in flt_ds]
if flt_sev:
    rows = [r for r in rows if str(r.get("severity")) in flt_sev]
if flt_action:
    rows = [r for r in rows if str(r.get("action_taken")) in flt_action]


st.markdown("---")


# ---------------------------------------------------------------------------
# Inventory table
# ---------------------------------------------------------------------------
if not rows:
    st.success(
        "✅ No schema drift detected (or none matching filters).  "
        "Drift detection runs automatically on every Bronze ingest — events "
        "land here when a client's vendor feed deviates from the canonical "
        "Bronze catalog."
    )
    st.stop()


def _drift_pill(drift_type: str) -> str:
    klass = {
        "ADDITIVE": "sd-pill-additive",
        "REMOVED": "sd-pill-removed",
        "TYPE_CHANGE": "sd-pill-typed",
        "FATAL": "sd-pill-fatal",
    }.get(str(drift_type).upper(), "sd-pill-additive")
    return f"<span class='sd-pill {klass}'>{drift_type}</span>"


view = []
for r in rows:
    view.append(
        {
            "Detected": str(r.get("detected_at"))[:19],
            "Dataset": r.get("dataset_code"),
            "Client": r.get("client_id"),
            "Drift type": r.get("drift_type"),
            "Severity": r.get("severity"),
            "Action": r.get("action_taken"),
            "Added": str(r.get("added_columns") or "—")[:60],
            "Removed": str(r.get("removed_columns") or "—")[:60],
            "Source file": str(r.get("source_file") or "—")[:40],
        }
    )
st.dataframe(
    pd.DataFrame(view),
    use_container_width=True,
    hide_index=True,
    height=min(420, 60 + 35 * len(view)),
)


st.markdown("---")


# ---------------------------------------------------------------------------
# Per-dataset triage cards
# ---------------------------------------------------------------------------
st.markdown("## 🛠 Triage by dataset")
st.caption(
    "Each card aggregates drift events for one dataset.  When the deviation "
    "needs a DQ-suite revision, click **→ Revise DQ suite** to deep-link "
    "into the AI Architect for that dataset's Bronze layer."
)

by_ds: dict[str, list[dict[str, Any]]] = {}
for r in rows:
    by_ds.setdefault(str(r.get("dataset_code")), []).append(r)

for ds, ev in sorted(by_ds.items()):
    n_total = len(ev)
    n_halted = sum(1 for e in ev if str(e.get("action_taken")) == "HALTED")
    distinct_clients = sorted({str(e.get("client_id")) for e in ev if e.get("client_id")})
    most_recent = max(str(e.get("detected_at") or "") for e in ev)[:19]

    severity_emoji = "🟥" if n_halted else "🟧"
    label = (
        f"{severity_emoji} `{ds}`  ·  {n_total} event"
        f"{'s' if n_total != 1 else ''}  ·  "
        f"{n_halted} halted  ·  most recent: {most_recent}"
    )
    with st.expander(label, expanded=(n_halted > 0)):
        a, b = st.columns([2, 1])
        with a:
            st.markdown(
                f"**Affected clients ({len(distinct_clients)}):** "
                + ", ".join(f"`{c}`" for c in distinct_clients)
            )
            # Per-event details
            for e in ev[:8]:  # cap to 8 to avoid wall of text
                added = str(e.get("added_columns") or "")
                removed = str(e.get("removed_columns") or "")
                detail = []
                if added and added != "[]":
                    detail.append(f"➕ added: `{added[:120]}`")
                if removed and removed != "[]":
                    detail.append(f"➖ removed: `{removed[:120]}`")
                bullet = (
                    f"**{str(e.get('detected_at'))[:19]}** · "
                    f"client `{e.get('client_id')}` · "
                    f"action `{e.get('action_taken')}`"
                )
                st.markdown(f"- {bullet}")
                if detail:
                    for d in detail:
                        st.caption(d)
            if len(ev) > 8:
                st.caption(f"_(+ {len(ev) - 8} older events)_")
        with b:
            # Hand-off to DQ AI Architect — pre-fill dataset + Bronze layer.
            st.markdown(
                f'<a href="/DQ_AI_Architect?dataset={ds}&layer=BRONZE" target="_self" '
                f'style="display:inline-block;background:#7c3aed;color:#fff;'
                f"padding:.55rem .9rem;border-radius:6px;text-decoration:none;"
                f'font-weight:600;text-align:center;width:100%;margin-bottom:.4rem">'
                f"→ Revise Bronze DQ suite</a>",
                unsafe_allow_html=True,
            )
            st.markdown(
                f'<a href="/Schema_Drift?focus={ds}" target="_self" '
                f'style="display:inline-block;background:#1d4ed8;color:#fff;'
                f"padding:.55rem .9rem;border-radius:6px;text-decoration:none;"
                f'font-weight:600;text-align:center;width:100%">'
                f"📋 View all drifts for `{ds}`</a>",
                unsafe_allow_html=True,
            )
            st.caption(
                "_DQ suite revision opens the AI Architect pre-filled to "
                "this dataset's Bronze layer.  Re-propose, review the "
                "diff vs LIVE, and promote._"
            )
