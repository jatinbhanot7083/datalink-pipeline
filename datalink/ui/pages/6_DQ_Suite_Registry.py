"""DQ Suite Registry — versioned catalog of DQ suites and expectations.

Answers the question: "What DQ suites and expectations exist for a
given client, for a given source_type (Claims / Membership / Provider)?"

Read-only registry view over CONTROL.dq_suites. Operators can:

  * Filter by client (or view all)
  * Filter by source_type (Claims / Membership / Provider)
  * See every suite (baseline + agent-authored + UI-authored) with status
  * Expand any suite to see the complete expectation list — rule shape
    only, not row-level data (e.g. "expect_column_values_to_not_be_null
    on claim_id")
  * See each suite's schema_fingerprint so drift is attributable

Data source
-----------
CONTROL.dq_suites + CONTROL.dq_suite_audit_log. Zero joins outside CONTROL.

What this page intentionally excludes
-------------------------------------
* Actual data values → use the Warehouse Explorer
* Pass/fail statistics per suite → see the DQ Dashboard
* Agent reasoning logs → see the CrewAI Dashboard
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import duckdb
import pandas as pd
import streamlit as st

WAREHOUSE_PATH = os.environ.get("DL_CT_WAREHOUSE_PATH", "/opt/datalink/warehouse.duckdb")

from datalink.ui._bootstrap import ensure_warehouse_exists  # noqa: E402

ensure_warehouse_exists(WAREHOUSE_PATH)

st.set_page_config(
    page_title="DataLink — DQ Suite Registry",
    page_icon="📚",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Shared sidebar nav (defined in datalink/ui/_nav.py).
from datalink.ui._nav import render_sidebar  # noqa: E402

render_sidebar(active="DQ Suite Registry")

_NAVY = "#0a1a3e"
_GOLD = "#d4af37"
_EMERALD = "#059669"
_AMBER = "#d97706"
_ROSE = "#e11d48"
_INDIGO = "#4338ca"
_SLATE = "#64748b"

_STATUS_COLORS = {
    "LIVE": _EMERALD,
    "APPROVED": "#16a34a",
    "PENDING_REVIEW": _AMBER,
    "DRAFT": _SLATE,
    "ARCHIVED": "#9ca3af",
    "REJECTED": _ROSE,
}

_SOURCE_ICONS = {
    "CLAIMS": "📋",
    "MEMBERSHIP": "👤",
    "PROVIDER": "🏥",
    None: "📦",
}


st.markdown(
    f"""
    <style>
      h1 {{color:{_NAVY}; border-bottom:3px solid {_GOLD}; padding-bottom:.4rem;}}
      h2, h3 {{color:{_NAVY}; margin-top:1.3rem;}}
      .browser-hero {{
        background:linear-gradient(135deg,{_NAVY} 0%,#1e3a8a 100%);
        color:#fff; padding:1rem 1.4rem; border-radius:10px; margin-bottom:1rem;
      }}
      .browser-hero h2 {{color:#fff;margin:0;}}
      .browser-hero p {{color:#cbd5e1;margin:.3rem 0 0 0;font-size:.9rem;}}
      .suite-card {{
        background:#fff;padding:.8rem 1rem;border-radius:7px;
        box-shadow:0 1px 3px rgba(10,26,62,.06); margin-bottom:.4rem;
      }}
      .suite-head {{display:flex;align-items:center;gap:.6rem;font-weight:600;color:{_NAVY};}}
      .suite-badge {{
        display:inline-block; padding:.12rem .45rem; border-radius:3px;
        font-size:.7rem; font-weight:700; letter-spacing:.05em;
        color:#fff; margin-left:auto;
      }}
      .suite-meta {{color:#64748b;font-size:.78rem;margin-top:.25rem;}}
      .exp-row {{
        padding:.3rem .6rem;border-left:3px solid {_INDIGO};
        background:#f8fafc;margin:.2rem 0;border-radius:4px;font-size:.82rem;
      }}
      .exp-row code {{background:#e0e7ff;padding:.08rem .3rem;border-radius:3px;}}
      .exp-row.review-flag {{border-left-color:{_AMBER}; background:#fffbeb;}}
      .dim-chip {{
        display:inline-block; background:{_INDIGO}; color:#fff;
        padding:.08rem .4rem; border-radius:3px; font-size:.7rem; margin-right:.25rem;
      }}
      .empty-state {{background:#f8fafc;border:2px dashed #cbd5e1;padding:1.5rem;
          text-align:center;border-radius:8px;color:#64748b;}}
    </style>
    """,
    unsafe_allow_html=True,
)


@contextmanager
def _conn() -> Iterator[duckdb.DuckDBPyConnection]:
    c = duckdb.connect(WAREHOUSE_PATH, read_only=True)
    try:
        yield c
    finally:
        c.close()


def _query(sql: str, params: tuple[Any, ...] = ()) -> pd.DataFrame:
    try:
        with _conn() as c:
            return c.execute(sql, list(params)).fetchdf()
    except Exception:
        return pd.DataFrame()


# ============================================================================
# HEADER
# ============================================================================
st.title("📚 DQ Suite Registry")
st.markdown(
    '<div class="browser-hero">'
    "<h2>Versioned catalog of every DQ suite and expectation</h2>"
    "<p>Complete view of the quality rule set held in "
    "<code>CONTROL.dq_suites</code> — baseline suites seeded at bootstrap, "
    "agent-authored suites (auto-approved or pending human review), and "
    "suites authored via the DQ Author page. Filter by client and source, "
    "then drill into any suite to see every expectation with its kwargs "
    "and metadata. Read-only — this registry describes, it does not execute.</p>"
    "</div>",
    unsafe_allow_html=True,
)


# ============================================================================
# SIDEBAR FILTERS
# ============================================================================
st.sidebar.header("Filters")

clients_df = _query("SELECT DISTINCT client_id FROM CONTROL.dq_suites ORDER BY client_id")
client_options = ["<all>"] + (
    clients_df["client_id"].tolist() if not clients_df.empty else ["default"]
)
selected_client = st.sidebar.selectbox("Client", client_options, index=0)

source_options = ["<all>", "CLAIMS", "MEMBERSHIP", "PROVIDER", "(legacy aggregate)"]
selected_source = st.sidebar.selectbox("Source type", source_options, index=0)

status_options = ["<all>", "LIVE", "PENDING_REVIEW", "DRAFT", "APPROVED", "ARCHIVED", "REJECTED"]
selected_status = st.sidebar.selectbox("Status", status_options, index=1)  # default LIVE

source_filter = ""
source_params: list[Any] = []
if selected_source == "(legacy aggregate)":
    source_filter = "AND source_type IS NULL"
elif selected_source != "<all>":
    source_filter = "AND UPPER(source_type) = ?"
    source_params = [selected_source.upper()]

client_filter = ""
client_params: list[Any] = []
if selected_client != "<all>":
    client_filter = "AND client_id = ?"
    client_params = [selected_client]

status_filter = ""
status_params: list[Any] = []
if selected_status != "<all>":
    status_filter = "AND status = ?"
    status_params = [selected_status]

params = tuple(client_params + source_params + status_params)


# ============================================================================
# SUMMARY COUNTS
# ============================================================================
summary_df = _query(
    f"""
    SELECT
      COUNT(*)                                              AS suites,
      COUNT(DISTINCT client_id)                             AS clients,
      COUNT(DISTINCT source_type)                           AS sources,
      SUM(CASE WHEN status = 'LIVE' THEN 1 ELSE 0 END)      AS live_ct,
      SUM(CASE WHEN status = 'PENDING_REVIEW' THEN 1 ELSE 0 END) AS pending_ct,
      SUM(CASE WHEN status = 'ARCHIVED' THEN 1 ELSE 0 END)  AS archived_ct,
      SUM(CASE WHEN source = 'agent' THEN 1 ELSE 0 END)     AS agent_ct,
      SUM(CASE WHEN source = 'baseline_python' THEN 1 ELSE 0 END) AS baseline_ct,
      SUM(CASE WHEN source = 'ui' THEN 1 ELSE 0 END)        AS ui_ct
    FROM CONTROL.dq_suites
    WHERE 1=1 {client_filter} {source_filter}
    """,
    tuple(client_params + source_params),
)

cols = st.columns(6)
if summary_df.empty or not summary_df["suites"].iloc[0]:
    totals = dict.fromkeys(
        ["suites", "live_ct", "pending_ct", "agent_ct", "baseline_ct", "ui_ct"], 0
    )
else:
    totals = summary_df.iloc[0].to_dict()


def _metric(label: str, value: Any, color: str = _NAVY) -> None:
    st.markdown(
        f'<div style="background:#fff;padding:.7rem 1rem;border-top:3px solid {color};'
        'border-radius:6px;box-shadow:0 1px 3px rgba(10,26,62,.05)">'
        f'<div style="color:#64748b;font-size:.7rem;text-transform:uppercase;letter-spacing:.08em;">'
        f"{label}</div>"
        f'<div style="color:{_NAVY};font-size:1.4rem;font-weight:700">{int(value or 0):,}</div>'
        f"</div>",
        unsafe_allow_html=True,
    )


with cols[0]:
    _metric("Total suites", totals["suites"])
with cols[1]:
    _metric("LIVE", totals["live_ct"], _EMERALD)
with cols[2]:
    _metric("Pending review", totals["pending_ct"], _AMBER)
with cols[3]:
    _metric("From agent", totals["agent_ct"], _INDIGO)
with cols[4]:
    _metric("Baseline (seeded)", totals["baseline_ct"], _GOLD)
with cols[5]:
    _metric("UI-authored", totals["ui_ct"], _SLATE)

st.markdown("---")


# ============================================================================
# SUITE LIST — grouped by (client, source_type)
# ============================================================================

suites_df = _query(
    f"""
    SELECT suite_id, client_id, suite_name, version, status, source, source_type,
           schema_fingerprint, created_by, created_at, activated_at,
           expectations, dq_dimensions
    FROM CONTROL.dq_suites
    WHERE 1=1 {client_filter} {source_filter} {status_filter}
    ORDER BY client_id, source_type NULLS LAST, suite_name, version DESC
    """,
    params,
)

if suites_df.empty:
    st.markdown(
        '<div class="empty-state">'
        "No suites match the filter. Try widening Client / Source / Status in the sidebar."
        "</div>",
        unsafe_allow_html=True,
    )
    st.stop()


# Group rendering: one expander per (client, source_type), with all matching suites inside.
grouped = suites_df.groupby(["client_id", "source_type"], dropna=False)

for (client, source), group in grouped:
    src_key = source if pd.notna(source) else None
    icon = _SOURCE_ICONS.get(str(src_key).upper() if src_key else None, "📦")
    src_label = str(src_key) if src_key else "(legacy aggregate)"
    n_suites = len(group)
    exp_total = sum(len(json.loads(e) if isinstance(e, str) else e) for e in group["expectations"])

    with st.expander(
        f"{icon}  **{client}** · {src_label}  —  {n_suites} suite(s), "
        f"{exp_total} expectation(s) total",
        expanded=(selected_client != "<all>" and n_suites <= 6),
    ):
        for _, row in group.iterrows():
            status = row["status"]
            status_color = _STATUS_COLORS.get(status, _SLATE)
            is_review_suite = str(row["suite_name"]).endswith("__review")
            name_label = row["suite_name"]
            if is_review_suite:
                name_label += " 🔍"  # little marker for HITL suites

            fp = str(row.get("schema_fingerprint") or "")[:8]
            fp_display = f"· schema: <code>{fp}</code>" if fp else ""
            dims = row.get("dq_dimensions") or "[]"
            try:
                dims_list = json.loads(dims) if isinstance(dims, str) else list(dims)
            except Exception:
                dims_list = []
            dim_html = "".join(f'<span class="dim-chip">{d}</span>' for d in dims_list[:8])

            st.markdown(
                f'<div class="suite-card">'
                f'<div class="suite-head">'
                f"  <span>{name_label}</span>"
                f'  <span class="suite-badge" style="background:{status_color}">{status}</span>'
                f"</div>"
                f'<div class="suite-meta">'
                f"v{row['version']} · "
                f"source: <b>{row['source']}</b> · "
                f"created_by: <code>{row['created_by']}</code> · "
                f"created_at: {row['created_at']} {fp_display}"
                f"<br>{dim_html}"
                f"</div>"
                f"</div>",
                unsafe_allow_html=True,
            )

            # Expectation drill-down — inline, not collapsed, because that's the
            # whole point of this page.
            try:
                exps = (
                    json.loads(row["expectations"])
                    if isinstance(row["expectations"], str)
                    else list(row["expectations"])
                )
            except Exception:
                exps = []

            if not exps:
                st.markdown(
                    '<div class="empty-state" style="font-size:.8rem;padding:.6rem">'
                    "(suite has no expectations)</div>",
                    unsafe_allow_html=True,
                )
                continue

            with st.expander(f"  ▸ Show {len(exps)} expectation(s)", expanded=False):
                for exp in exps:
                    exp_type = exp.get("expectation_type", "?")
                    kwargs = exp.get("kwargs", {})
                    meta = exp.get("meta", {})
                    dim = meta.get("dq_dimension", "")
                    sev = meta.get("severity", "")
                    col = kwargs.get("column", "")
                    needs_review = meta.get("review_required", False) or exp.get("review_required")
                    rc = "review-flag" if needs_review else ""

                    # Build human phrasing per expectation type.
                    phrasing = f"<code>{exp_type}</code>"
                    if col:
                        phrasing += f" on column <code>{col}</code>"
                    # Consistency pair rules carry column_A / column_B instead of `column`.
                    col_a = kwargs.get("column_A")
                    col_b = kwargs.get("column_B")
                    if col_a and col_b:
                        op = " ≥ " if kwargs.get("or_equal") else " > "
                        phrasing += f" &middot; <code>{col_a}</code>{op}<code>{col_b}</code>"
                    extras = []
                    if kwargs.get("regex"):
                        extras.append(f"regex = <code>{kwargs['regex']}</code>")
                    if kwargs.get("value_set"):
                        vs = kwargs["value_set"]
                        vs_short = ", ".join(
                            str(v) for v in (vs[:5] if isinstance(vs, list) else [vs])
                        )
                        extras.append(f"value_set ∈ {{ <code>{vs_short}</code>... }}")
                    if kwargs.get("min_value") is not None or kwargs.get("max_value") is not None:
                        extras.append(
                            f"range: [{kwargs.get('min_value', '−∞')}, {kwargs.get('max_value', '+∞')}]"
                        )
                    if kwargs.get("mostly"):
                        extras.append(f"mostly ≥ {kwargs['mostly']}")
                    if kwargs.get("parse_strings_as_datetimes"):
                        extras.append("parse_as_datetime = true")
                    # Timeliness: sla_hours lives in meta, not kwargs.
                    sla = meta.get("sla_hours")
                    if sla:
                        extras.append(f"rolling SLA = {sla}h")
                    if extras:
                        phrasing += " &middot; " + " &middot; ".join(extras)

                    badge_html = ""
                    if dim:
                        badge_html += f'<span class="dim-chip">{dim}</span>'
                    if sev:
                        sev_color = {"HIGH": _ROSE, "MEDIUM": _AMBER, "LOW": _EMERALD}.get(
                            sev, _SLATE
                        )
                        badge_html += (
                            f'<span class="dim-chip" style="background:{sev_color}">{sev}</span>'
                        )
                    if needs_review:
                        badge_html += (
                            '<span class="dim-chip" style="background:'
                            f'{_AMBER}">⚠ NEEDS HUMAN REVIEW</span>'
                        )

                    desc = meta.get("description", "")
                    desc_html = (
                        f'<div style="color:#64748b;font-size:.75rem;margin-top:.2rem">{desc}</div>'
                        if desc
                        else ""
                    )

                    st.markdown(
                        f'<div class="exp-row {rc}">'
                        f"<div>{phrasing}</div>"
                        f"<div>{badge_html}</div>"
                        f"{desc_html}"
                        f"</div>",
                        unsafe_allow_html=True,
                    )


st.caption(
    f"Catalog refreshed {pd.Timestamp.utcnow().strftime('%Y-%m-%d %H:%M UTC')}  ·  "
    f"Source: `{WAREHOUSE_PATH}`  ·  "
    "Tip: `__review` suffix = suite waiting for human sign-off."
)
