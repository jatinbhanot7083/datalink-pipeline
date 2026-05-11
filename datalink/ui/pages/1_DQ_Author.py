"""DQ Author — Phase 19.4 (factory-pattern, manual editor).

Manual editor for DQ suites.  The AI Architect proposes; the Author lets
you tweak.  Pick a (dataset, layer) — start from the LIVE suite (or empty
if none yet) — add/remove/edit expectations — submit for review.

No client filter (factory pattern).  When submitted the result lands as
a new DRAFT version in CONTROL.dq_suites for DQ Review to promote.
"""

from __future__ import annotations

import json
import sys
import uuid
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
    page_title="DQ Author",
    page_icon="✍️",
    layout="wide",
    initial_sidebar_state="expanded",
)

from datalink.quality.control import CONTROL_SCHEMA, create_control_tables  # noqa: E402
from datalink.ui._nav import render_sidebar  # noqa: E402
from datalink.ui._query import warehouse_ctx  # noqa: E402

render_sidebar(active="DQ Author")

_NAVY = "#0a1a3e"
_GOLD = "#d4af37"
_GREY = "#64748b"

st.markdown(
    f"""
    <style>
      h1 {{color: {_NAVY}; border-bottom: 3px solid {_GOLD}; padding-bottom: .4rem;}}
      h2 {{color: {_NAVY}; margin-top: 1.2rem;}}
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


st.markdown("# ✍️ DQ Author")
st.caption(
    "Hand-edit a DQ suite for any `(dataset, layer)`.  Start from the "
    "current LIVE (or empty) — add/remove/tweak expectations — submit "
    "as a new DRAFT version for **DQ Review** to promote."
)


# ---------------------------------------------------------------------------
# Pickers
# ---------------------------------------------------------------------------
@st.cache_data(ttl=60)
def _list_datasets() -> list[dict[str, Any]]:
    with warehouse_ctx(readonly=True) as wh:
        try:
            return list(
                wh.query(
                    f"SELECT dataset_code, display_name "
                    f"FROM {CONTROL_SCHEMA}.global_bronze_catalog_datasets "
                    f"WHERE is_active = TRUE ORDER BY display_name"
                )
            )
        except Exception:
            return []


@st.cache_data(ttl=30)
def _live_suite(dataset_code: str, layer: str) -> dict[str, Any] | None:
    with warehouse_ctx(readonly=True) as wh:
        rows = list(
            wh.query(
                f"SELECT suite_id, suite_name, version, expectations, dq_dimensions "
                f"FROM {CONTROL_SCHEMA}.dq_suites "
                f"WHERE LOWER(dataset_code) = LOWER($ds) "
                f"  AND UPPER(layer) = UPPER($lyr) "
                f"  AND status = 'LIVE' "
                f"ORDER BY version DESC LIMIT 1",
                {"ds": dataset_code, "lyr": layer},
            )
        )
    return rows[0] if rows else None


datasets = _list_datasets()
ds_options = sorted(d["dataset_code"] for d in datasets)
ds_display = {d["dataset_code"]: d.get("display_name") or d["dataset_code"] for d in datasets}

p1, p2 = st.columns([2, 1])
with p1:
    ds = st.selectbox(
        "Dataset",
        options=["— select —", *ds_options],
        format_func=lambda c: f"{ds_display.get(c, c)} · `{c}`" if c != "— select —" else c,
        key="dqa_ds",
    )
with p2:
    layer = st.selectbox(
        "Layer",
        options=["BRONZE", "SILVER", "GOLD"],
        index=0,
        key="dqa_layer",
    )

if ds == "— select —":
    st.info("Pick a `(dataset, layer)` to start.")
    st.stop()

live = _live_suite(ds, layer)
if live:
    st.success(
        f"📜 Editing from LIVE `{live['suite_name']}` v{live['version']} — "
        f"submission will land as v{int(live['version']) + 1} DRAFT."
    )
else:
    st.warning(
        f"No LIVE suite for `{ds}/{layer}` yet.  You're starting from an empty "
        f"editor — submission will land as v1 DRAFT.  (You can also use "
        f"**🧪 DQ AI Architect** to bootstrap a starter suite.)"
    )


# ---------------------------------------------------------------------------
# Editable table
# ---------------------------------------------------------------------------
session_key = f"dqa_exps_{ds}_{layer}"
if session_key not in st.session_state:
    try:
        live_exps = json.loads((live or {}).get("expectations") or "[]")
    except Exception:
        live_exps = []
    # Flatten for st.data_editor
    rows = []
    for e in live_exps:
        meta = e.get("meta") or {}
        kwargs = e.get("kwargs") or {}
        rows.append(
            {
                "expectation_type": e.get("expectation_type", ""),
                "column": kwargs.get("column", ""),
                "kwargs_json": json.dumps(
                    {k: v for k, v in kwargs.items() if k != "column"},
                    default=str,
                ),
                "dimension": meta.get("dq_dimension", "Validity"),
                "severity": meta.get("severity", "MEDIUM"),
                "description": meta.get("description", ""),
                "rationale": meta.get("rationale", ""),
            }
        )
    st.session_state[session_key] = rows

st.markdown("### ✏️ Expectations")
st.caption(
    "Edit cells inline.  Add a row to add a check; delete a row to remove one.  "
    '`kwargs_json` is the JSON of additional kwargs (e.g. `{"min_value": 0}`).'
)

_KNOWN_TYPES = [
    "expect_column_values_to_not_be_null",
    "expect_column_values_to_be_unique",
    "expect_column_values_to_be_between",
    "expect_column_values_to_be_in_set",
    "expect_column_values_to_match_regex",
    "expect_column_pair_values_a_to_be_greater_than_b",
    "expect_column_max_to_be_between",
    "expect_table_row_count_to_be_between",
    "expect_compound_columns_to_be_unique",
    "expect_column_values_to_be_of_type",
    "expect_column_value_lengths_to_be_between",
]
_DIMS = ["Completeness", "Uniqueness", "Validity", "Consistency", "Timeliness", "Accuracy"]
_SEVS = ["CRITICAL", "HIGH", "MEDIUM", "LOW"]

edited = st.data_editor(
    pd.DataFrame(st.session_state[session_key]),
    use_container_width=True,
    num_rows="dynamic",
    column_config={
        "expectation_type": st.column_config.SelectboxColumn(
            "Type", options=_KNOWN_TYPES, width="medium", required=True
        ),
        "column": st.column_config.TextColumn("Column", width="small"),
        "kwargs_json": st.column_config.TextColumn(
            "kwargs JSON",
            width="medium",
            help='Extra kwargs as JSON, e.g. {"min_value": 0, "max_value": 100}',
        ),
        "dimension": st.column_config.SelectboxColumn(
            "Dimension", options=_DIMS, width="small", required=True
        ),
        "severity": st.column_config.SelectboxColumn(
            "Severity", options=_SEVS, width="small", required=True
        ),
        "description": st.column_config.TextColumn("Description", width="medium"),
        "rationale": st.column_config.TextColumn("Rationale", width="medium"),
    },
    key=f"dqa_editor_{ds}_{layer}",
    height=min(560, 80 + 40 * max(len(st.session_state[session_key]), 5)),
)


# ---------------------------------------------------------------------------
# Validation + submit
# ---------------------------------------------------------------------------
def _row_to_expectation(row: dict[str, Any]) -> dict[str, Any] | None:
    """Convert one editor row into a GX expectation dict, or None on error."""
    et = str(row.get("expectation_type") or "").strip()
    if not et:
        return None
    col = str(row.get("column") or "").strip()
    extras_raw = str(row.get("kwargs_json") or "{}").strip() or "{}"
    try:
        extras = json.loads(extras_raw)
        if not isinstance(extras, dict):
            extras = {}
    except json.JSONDecodeError:
        return None
    kwargs: dict[str, Any] = dict(extras)
    if col:
        kwargs["column"] = col
    return {
        "expectation_type": et,
        "kwargs": kwargs,
        "meta": {
            "dq_dimension": str(row.get("dimension") or "Validity"),
            "severity": str(row.get("severity") or "MEDIUM").upper(),
            "description": str(row.get("description") or "")[:280],
            "rationale": str(row.get("rationale") or "")[:600],
        },
    }


st.markdown("---")
sb1, sb2, sb3 = st.columns([2, 1, 1])

with sb1:
    notes = st.text_input(
        "Submission notes (optional)",
        value="",
        key="dqa_notes",
        placeholder="e.g., 'tightened phone-format regex'",
    )

with sb2:
    if st.button("💾 Submit as DRAFT", type="primary", use_container_width=True, key="dqa_submit"):
        try:
            new_rows = edited.to_dict("records") if hasattr(edited, "to_dict") else list(edited)
            valid: list[dict[str, Any]] = []
            invalid_count = 0
            for r in new_rows:
                exp = _row_to_expectation(r)
                if exp is None:
                    invalid_count += 1
                else:
                    valid.append(exp)
            if not valid:
                st.error("No valid expectations to submit.  Fix the table first.")
            else:
                # Compute next version
                with _warehouse(readonly=True) as wh:
                    rows = list(
                        wh.query(
                            f"SELECT COALESCE(MAX(version),0) AS v "
                            f"FROM {CONTROL_SCHEMA}.dq_suites "
                            f"WHERE LOWER(dataset_code) = LOWER($ds) "
                            f"  AND UPPER(layer) = UPPER($lyr)",
                            {"ds": ds, "lyr": layer},
                        )
                    )
                next_v = int((rows[0] if rows else {}).get("v") or 0) + 1
                # Compute dimension breakdown
                dims: dict[str, int] = {}
                for e in valid:
                    d = e["meta"]["dq_dimension"]
                    dims[d] = dims.get(d, 0) + 1
                suite_id = str(uuid.uuid4())
                suite_name = f"{ds}_{layer.lower()}"
                with _warehouse(readonly=False) as wh:
                    wh.execute(
                        f"""
                        INSERT INTO {CONTROL_SCHEMA}.dq_suites
                          (suite_id, client_id, suite_name, version, status,
                           expectations, dq_dimensions, source_type, source,
                           dataset_code, layer, created_by, created_at)
                        VALUES
                          ($id, NULL, $name, $ver, 'PENDING_REVIEW',
                           $exps, $dims, $src_type, 'manual',
                           $ds, $lyr, $by, CURRENT_TIMESTAMP())
                        """,
                        {
                            "id": suite_id,
                            "name": suite_name,
                            "ver": next_v,
                            "exps": json.dumps(valid, default=str),
                            "dims": json.dumps(dims, default=str),
                            "src_type": ds,
                            "ds": ds,
                            "lyr": layer,
                            "by": "ui:dq_author",
                        },
                    )
                    wh.execute(
                        f"""
                        INSERT INTO {CONTROL_SCHEMA}.dq_suite_audit_log
                          (audit_id, suite_id, from_status, to_status, actor, notes)
                        VALUES ($aid, $sid, NULL, 'PENDING_REVIEW', 'ui:dq_author', $notes)
                        """,
                        {
                            "aid": str(uuid.uuid4()),
                            "sid": suite_id,
                            "notes": notes or f"Manual edit submitted for {ds}/{layer}",
                        },
                    )
                # Reset session for clean next-edit
                st.session_state.pop(session_key, None)
                _live_suite.clear()
                hint = f" ({invalid_count} invalid rows ignored)" if invalid_count else ""
                st.toast(
                    f"💾 v{next_v} submitted for review — {len(valid)} expectations{hint}",
                    icon="✅",
                )
                st.rerun()
        except Exception as exc:
            st.error(f"Submit failed: {type(exc).__name__}: {exc}")

with sb3:
    if st.button("🔄 Reset from LIVE", use_container_width=True, key="dqa_reset"):
        st.session_state.pop(session_key, None)
        st.rerun()
