"""Schema Drift — Phase 11 operator UI.

Three responsibilities, one page:

  1. **Drift events** — recent rows from ``CONTROL.schema_drift_log`` for
     the active client. Each row shows: when, source_type, drift_type,
     severity, action taken (LOGGED / HALTED), and the columns that
     drifted. This is where on-call goes after a Bronze ingest aborts.

  2. **Contracts** — the active ``source_schema_contracts`` row per
     ``(client, source_type)``, with full column listing + version
     history (older versions kept for audit).

  3. **Promote-to-contract** — the operator action the SchemaDrift doc
     describes: when an additive drift event has fired, the operator
     reviews the new columns and clicks "Promote to contract", which
     bumps the contract version and adds the new columns. Future
     batches stop firing the drift warning, and (when row-level capture
     ships in a future phase) extras get lifted into Silver
     EXTRA_ATTRIBUTES.

This page does NOT add GX expectations for the new columns — that's a
separate flow on DQ AI Architect (which is linked from each promoted
column for the operator to one-click follow up).
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import pandas as pd
import streamlit as st

from datalink.quality.schema_drift import (
    ColumnContract,
    get_active_contract,
    list_contract_versions,
    list_recent_drift_events,
    register_contract,
)

WAREHOUSE_PATH = os.environ.get("DL_CT_WAREHOUSE_PATH", "/opt/datalink/warehouse.duckdb")

from datalink.ui._bootstrap import ensure_warehouse_exists  # noqa: E402

ensure_warehouse_exists(WAREHOUSE_PATH)

st.set_page_config(
    page_title="DataLink — Schema Drift",
    page_icon="🧬",
    layout="wide",
    initial_sidebar_state="expanded",
)

from datalink.ui._nav import render_sidebar, require_client  # noqa: E402

render_sidebar(active="Schema Drift")
client_id = require_client()
# Phase 16.6 — when no client picked (default = All clients), gate
# the per-client content with a friendly notice. Pages with a true
# all-clients view (Pipeline Architect) handle this differently.
if client_id is None:
    import streamlit as _st

    _st.info(
        "🌐 **All-clients view.** Pick a client from the dropdown above "
        "to load this client-scoped page. Cross-tenant dashboards "
        "(Control Tower, PHI Governance, Cost & Tokens, Lineage) live "
        "elsewhere and don't need a client picker."
    )
    _st.stop()
_NAVY = "#0a1a3e"
_GOLD = "#d4af37"

st.markdown(
    f"""
    <style>
      h1 {{color: {_NAVY}; border-bottom: 3px solid {_GOLD}; padding-bottom: .4rem;}}
      h2 {{color: {_NAVY}; margin-top: 1.5rem;}}
      .sev-INFO    {{background:#dbeafe;color:#1e3a8a;padding:.15rem .5rem;border-radius:3px;font-weight:600}}
      .sev-WARNING {{background:#fef3c7;color:#92400e;padding:.15rem .5rem;border-radius:3px;font-weight:600}}
      .sev-FATAL   {{background:#fee2e2;color:#991b1b;padding:.15rem .5rem;border-radius:3px;font-weight:700}}
      .act-LOGGED  {{background:#d1fae5;color:#047857;padding:.15rem .5rem;border-radius:3px;font-weight:600}}
      .act-HALTED  {{background:#fecaca;color:#7f1d1d;padding:.15rem .5rem;border-radius:3px;font-weight:700}}
    </style>
    """,
    unsafe_allow_html=True,
)


# ============================================================================
# WAREHOUSE CONTEXT
# ============================================================================


@contextmanager
def _warehouse(*, readonly: bool = True) -> Iterator[Any]:
    """Per-action short-lived warehouse handle. Mirrors the pattern used
    on every other Phase-9/10 page."""
    from datalink.ui._query import warehouse_ctx

    with warehouse_ctx(readonly=readonly) as wh:
        yield wh


# ============================================================================
# HEADER
# ============================================================================

st.markdown("# 🧬 Schema Drift")
st.caption(
    f"Contracts + drift events for **{client_id}**. Bronze ingest reads each "
    f"file's header against the active contract; additive drift logs and "
    f"continues, subtractive / destructive drift halts the batch."
)

# Source selector — drift events + contracts are scoped per-source.
SOURCE_TYPES = ["CLAIMS", "MEMBERSHIP", "PROVIDER"]
src1, src2 = st.columns([1, 4])
with src1:
    source_type = st.selectbox(
        "Source type",
        options=SOURCE_TYPES,
        index=1,  # MEMBERSHIP — the doc's running example
    )


# ============================================================================
# CURRENT CONTRACT
# ============================================================================

st.markdown("## Active contract")

with _warehouse(readonly=True) as wh:
    contract = get_active_contract(wh, client_id=client_id, source_type=source_type)
    versions = list_contract_versions(wh, client_id=client_id, source_type=source_type)

if contract is None:
    st.warning(
        f"⚠️ No contract registered for **{client_id} / {source_type}**. "
        f"Bronze ingest will accept any header for this tuple — drift detection "
        f"is skipped (back-compat mode). Register a contract below to enforce "
        f"Phase 11 schema guarantees."
    )

    with st.expander("➕ Register first contract from current Bronze DDL", expanded=True):
        st.caption(
            f"Reads the current `BRONZE_{client_id.upper()}.RAW_{source_type}` "
            f"table schema and registers it as v1. Use this as the "
            f"**zero-drift starting point** — every future batch is compared "
            f"against this snapshot."
        )

        if st.button("📌 Register from current Bronze schema", type="primary"):
            try:
                with _warehouse(readonly=False) as wh:
                    bronze_schema = (
                        f"BRONZE_{client_id.upper()}" if client_id != "default" else "BRONZE"
                    )
                    bronze_table = f"RAW_{source_type}"
                    # Parameterize the LIKE pattern so the literal `%`
                    # doesn't collide with Snowflake adapter's pyformat
                    # substitution. Filter audit columns (start with `_`)
                    # in Python instead of SQL — simpler and portable.
                    rows = wh.query(
                        "SELECT column_name, data_type FROM information_schema.columns "
                        "WHERE table_schema = $s AND table_name = $t "
                        "ORDER BY ordinal_position",
                        {"s": bronze_schema, "t": bronze_table},
                    )
                    rows = [
                        r
                        for r in rows
                        if not (
                            (r.get("column_name") or r.get("COLUMN_NAME") or "")
                            .lstrip()
                            .startswith("_")
                        )
                    ]
                    if not rows:
                        st.error(
                            f"No business columns found in "
                            f"{bronze_schema}.{bronze_table}. Has Bronze been "
                            f"created? Run ▶ Run Bronze on Control Tower first."
                        )
                    else:
                        cols = [
                            ColumnContract(
                                name=str(
                                    r.get("column_name") or r.get("COLUMN_NAME") or ""
                                ).lower(),
                                logical_type=str(
                                    r.get("data_type") or r.get("DATA_TYPE") or ""
                                ).upper(),
                            )
                            for r in rows
                        ]
                        cid = register_contract(
                            wh,
                            client_id=client_id,
                            source_type=source_type,
                            columns=cols,
                            created_by=st.session_state.get("user", "anonymous@local"),
                            notes=f"v1 bootstrapped from {bronze_schema}.{bronze_table}",
                        )
                        st.success(
                            f"Registered v1 with {len(cols)} columns "
                            f"(contract_id `{cid[:8]}…`). Reload to see it."
                        )
                        st.rerun()
            except Exception as e:
                st.error(f"Registration failed: {type(e).__name__}: {e}")
else:
    c1, c2, c3 = st.columns([2, 1, 1])
    with c1:
        st.markdown(
            f"**v{contract.contract_version}** · "
            f"`{contract.contract_id[:8]}…` · "
            f"by `{contract.created_by}` · "
            f"{contract.created_at.strftime('%Y-%m-%d %H:%M')}"
        )
        if contract.notes:
            st.caption(f"_{contract.notes}_")
    with c2:
        st.metric("Columns", len(contract.columns))
    with c3:
        st.metric("Versions", len(versions))

    df_cols = pd.DataFrame(
        [
            {
                "name": c.name,
                "logical_type": c.logical_type,
                "nullable": c.nullable,
            }
            for c in contract.columns
        ]
    )
    st.dataframe(df_cols, use_container_width=True, hide_index=True)

    if len(versions) > 1:
        with st.expander(f"🕰 Version history ({len(versions)} versions)"):
            df_v = pd.DataFrame(
                [
                    {
                        "version": v.contract_version,
                        "active": "✅ LIVE" if v.is_active else "archived",
                        "columns": len(v.columns),
                        "created_by": v.created_by,
                        "created_at": v.created_at.strftime("%Y-%m-%d %H:%M"),
                        "notes": v.notes or "",
                    }
                    for v in versions
                ]
            )
            st.dataframe(df_v, use_container_width=True, hide_index=True)


# ============================================================================
# RECENT DRIFT EVENTS
# ============================================================================

st.markdown("## Recent drift events")

with _warehouse(readonly=True) as wh:
    all_events = list_recent_drift_events(wh, client_id=client_id, limit=200)

# Filter to the selected source_type for tighter focus.
events = [e for e in all_events if e.get("source_type") == source_type]

if not events:
    st.info(
        f"No drift events recorded for **{client_id} / {source_type}**. "
        f"Either no Bronze ingest has run since the contract was registered, "
        f"or every file has matched cleanly."
    )
else:
    # Summary metrics across ALL events for this tuple.
    fatal_count = sum(1 for e in events if e["severity"] == "FATAL")
    warning_count = sum(1 for e in events if e["severity"] == "WARNING")
    halted_count = sum(1 for e in events if e["action_taken"] == "HALTED")

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total events", len(events))
    m2.metric("⚠️  WARNINGs", warning_count)
    m3.metric("❌ FATALs", fatal_count)
    m4.metric("🛑 Halted batches", halted_count)

    df_events = pd.DataFrame(
        [
            {
                "detected_at": (
                    e["detected_at"].strftime("%Y-%m-%d %H:%M")
                    if hasattr(e["detected_at"], "strftime")
                    else str(e["detected_at"])
                ),
                "drift_type": e["drift_type"],
                "severity": e["severity"],
                "action": e["action_taken"],
                "added": ", ".join(json.loads(e["added_columns"] or "[]")) or "—",
                "removed": ", ".join(json.loads(e["removed_columns"] or "[]")) or "—",
                "type_changes": (
                    "; ".join(
                        f"{tc['column']}: {tc['expected']}→{tc['actual']}"
                        for tc in json.loads(e["type_changes"] or "[]")
                    )
                    or "—"
                ),
                "source_file": e.get("source_file") or "—",
                "batch_id": e.get("batch_id") or "—",
            }
            for e in events
        ]
    )
    st.dataframe(df_events, use_container_width=True, hide_index=True)


# ============================================================================
# PROMOTE-TO-CONTRACT (additive only)
# ============================================================================

if contract is not None:
    # Find unique additive columns across all WARNING events that haven't
    # already been folded into the active contract.
    promotable: list[str] = []
    seen: set[str] = set()
    contract_cols = contract.column_names
    for ev in events:
        if ev["severity"] != "WARNING":
            continue
        for col in json.loads(ev["added_columns"] or "[]"):
            if col not in seen and col not in contract_cols:
                seen.add(col)
                promotable.append(col)

    if promotable:
        st.markdown("## 🚀 Promote drifted columns to contract")
        st.caption(
            f"These columns appeared in additive-drift events but aren't yet "
            f"in the contract. Promoting them creates **v{contract.contract_version + 1}** "
            f"and stops future batches from firing the drift warning. After "
            f"promoting, head to the **DQ AI Architect** page to draft GX "
            f"expectations for the new columns."
        )

        # Multi-select so operator can promote a subset (some new cols may
        # be temporary noise from a vendor that they don't want to lock in).
        chosen = st.multiselect(
            "Columns to promote",
            options=promotable,
            default=promotable,
            help="Uncheck any column you don't want in the contract — "
            "useful for transient garbage you don't want to bless.",
        )

        # Per-column logical-type picker. Default TEXT since Bronze ingests
        # everything as VARCHAR; operator can refine to DATE / INTEGER / etc
        # if they know the semantic type.
        type_pickers: dict[str, str] = {}
        if chosen:
            st.markdown("**Logical types** — pick how each new column should be classified:")
            grid_cols = st.columns(min(3, len(chosen)))
            for i, col in enumerate(chosen):
                with grid_cols[i % len(grid_cols)]:
                    type_pickers[col] = st.selectbox(
                        col,
                        options=["TEXT", "INTEGER", "DECIMAL", "DATE", "TIMESTAMP", "BOOLEAN"],
                        index=0,
                        key=f"type_{col}",
                    )

            notes = st.text_input(
                "Promotion notes (audit trail)",
                value=f"promoted {len(chosen)} column(s) from drift events",
                key="promote_notes",
            )

            if st.button(
                f"📌 Promote {len(chosen)} column(s) to contract v{contract.contract_version + 1}",
                type="primary",
            ):
                try:
                    new_columns = list(contract.columns) + [
                        ColumnContract(
                            name=col,
                            logical_type=type_pickers[col],
                            nullable=True,  # promoted cols default nullable
                        )
                        for col in chosen
                    ]
                    with _warehouse(readonly=False) as wh:
                        new_cid = register_contract(
                            wh,
                            client_id=client_id,
                            source_type=source_type,
                            columns=new_columns,
                            created_by=st.session_state.get("user", "anonymous@local"),
                            notes=notes,
                        )
                    st.success(
                        f"✅ Promoted to v{contract.contract_version + 1} "
                        f"(`{new_cid[:8]}…`). The next Bronze ingest with these "
                        f"columns present will be clean. "
                        f"Next: open **DQ AI Architect** to author GX expectations "
                        f"for the new columns."
                    )
                    st.rerun()
                except Exception as e:
                    st.error(f"Promotion failed: {type(e).__name__}: {e}")
    else:
        st.markdown("## 🚀 Promote drifted columns to contract")
        st.caption(
            "_Nothing to promote — every WARNING-level additive column is "
            "already in the active contract, or there are no WARNING events._"
        )
