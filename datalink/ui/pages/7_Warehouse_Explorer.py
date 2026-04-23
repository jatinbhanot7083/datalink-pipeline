"""Warehouse Explorer — schema and data explorer for the DuckDB warehouse.

Layout:

  ┌─────────────────────────────────────────────────────────────────────┐
  │  Warehouse Explorer  —  warehouse.duckdb                            │
  │  [Client filter]  [Layer: Bronze|Silver|Gold|Control|All]  [Search] │
  ├──────────────┬──────────────────────────────────────────────────────┤
  │ Schema Tree  │  Breadcrumb:  warehouse  >  <schema>  >  <table>    │
  │              │  ┌────────────────────────────────────────────────┐ │
  │  ▸ SCHEMA_1  │  │ Tabs:  Columns  │  Data Preview  │  Worksheet  │ │
  │  ▾ SCHEMA_2  │  ├────────────────────────────────────────────────┤ │
  │      table_a │  │  (tab content — see below)                     │ │
  │      table_b │  └────────────────────────────────────────────────┘ │
  │  ▸ SCHEMA_3  │                                                     │
  └──────────────┴──────────────────────────────────────────────────────┘

Tabs:
  * Columns      — DESCRIBE output: name, type, nullable, PK
  * Data Preview — first N rows (sortable via st.dataframe; 50/100/500/1000)
  * Worksheet    — free-form SELECT/WITH/DESCRIBE/SHOW with CSV export

Why read-only only:
  DuckDB holds an exclusive file lock when writing. During a live DAG
  run, the scheduler has the write lock and any other process can only
  open the file read-only. This page opens with `read_only=True` for
  that reason — browsing is always safe, even mid-pipeline.
"""

from __future__ import annotations

import os
from typing import Any

import pandas as pd
import streamlit as st

WAREHOUSE_PATH = os.environ.get("DL_CT_WAREHOUSE_PATH", "/opt/datalink/warehouse.duckdb")

from datalink.ui._bootstrap import ensure_warehouse_exists  # noqa: E402
from datalink.ui._query import query as _wh_query  # noqa: E402
from datalink.ui._query import query_scalar as _wh_scalar  # noqa: E402

ensure_warehouse_exists(WAREHOUSE_PATH)

st.set_page_config(
    page_title="DataLink — Warehouse Explorer",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Shared sidebar nav (defined in datalink/ui/_nav.py).
from datalink.ui._nav import render_sidebar  # noqa: E402

render_sidebar(active="Warehouse Explorer")

# ---------------------------------------------------------------------------
# Palette — match the rest of the Control Tower pages
# ---------------------------------------------------------------------------
_NAVY = "#0a1a3e"
_GOLD = "#d4af37"
_SLATE_BG = "#f8fafc"
_SLATE_BORDER = "#e2e8f0"
_SLATE_TEXT = "#334155"
_SLATE_MUTED = "#64748b"
_BLUE_CHIP = "#dbeafe"
_BLUE_CHIP_TEXT = "#1e40af"
_AMBER_CHIP = "#fef3c7"
_AMBER_CHIP_TEXT = "#92400e"
_EMERALD_CHIP = "#d1fae5"
_EMERALD_CHIP_TEXT = "#065f46"
_VIOLET_CHIP = "#ede9fe"
_VIOLET_CHIP_TEXT = "#5b21b6"

st.markdown(
    f"""
    <style>
      h1 {{color:{_NAVY}; border-bottom:3px solid {_GOLD}; padding-bottom:.4rem;}}
      h2, h3 {{color:{_NAVY}; margin-top:1.2rem;}}
      .db-hero {{
        background:linear-gradient(135deg,{_NAVY} 0%,#1e3a8a 100%);
        color:#fff; padding:.9rem 1.3rem; border-radius:10px; margin-bottom:1rem;
      }}
      .db-hero h2 {{color:#fff;margin:0;font-size:1.1rem;}}
      .db-hero p {{color:#cbd5e1;margin:.3rem 0 0 0;font-size:.82rem;}}

      /* Schema tree card (left pane) */
      .db-tree {{
        background:#fff; border:1px solid {_SLATE_BORDER}; border-radius:8px;
        padding:.4rem .4rem; max-height:72vh; overflow-y:auto;
      }}
      .db-schema-label {{
        color:{_NAVY}; font-weight:700; font-size:.85rem;
        padding:.3rem .4rem; border-bottom:1px solid {_SLATE_BORDER};
      }}
      .db-table-row {{
        display:flex; justify-content:space-between; align-items:center;
        padding:.3rem .55rem; margin:.1rem 0; border-radius:4px;
        font-size:.82rem; color:{_SLATE_TEXT};
        border-left:3px solid transparent;
      }}
      .db-table-row:hover {{background:#f1f5f9; border-left-color:{_GOLD};}}
      .db-table-row.active {{background:#fffbeb; border-left-color:{_GOLD}; font-weight:700;}}
      .db-table-row .row-count {{color:{_SLATE_MUTED}; font-size:.72rem;}}

      /* Breadcrumb above the detail pane */
      .db-breadcrumb {{
        font-size:.85rem; color:{_SLATE_MUTED}; margin:0 0 .6rem 0;
      }}
      .db-breadcrumb b {{color:{_NAVY}; font-weight:700;}}
      .db-breadcrumb .sep {{color:{_GOLD}; margin:0 .3rem;}}

      /* Stat card row on Columns tab */
      .db-stat-grid {{
        display:grid; grid-template-columns:repeat(auto-fit,minmax(150px,1fr));
        gap:.6rem; margin-bottom:.8rem;
      }}
      .db-stat {{
        background:{_SLATE_BG}; border:1px solid {_SLATE_BORDER};
        border-left:4px solid {_GOLD}; border-radius:6px;
        padding:.5rem .8rem;
      }}
      .db-stat .label {{color:{_SLATE_MUTED}; font-size:.7rem; letter-spacing:.08em; text-transform:uppercase;}}
      .db-stat .value {{color:{_NAVY}; font-weight:700; font-size:1.15rem;}}

      /* Type chip */
      .db-type-chip {{
        display:inline-block; padding:.1rem .45rem; border-radius:3px;
        font-family:ui-monospace,Menlo,monospace; font-size:.72rem; font-weight:600;
      }}
      .db-type-int    {{background:{_BLUE_CHIP};    color:{_BLUE_CHIP_TEXT};}}
      .db-type-str    {{background:{_EMERALD_CHIP}; color:{_EMERALD_CHIP_TEXT};}}
      .db-type-date   {{background:{_VIOLET_CHIP};  color:{_VIOLET_CHIP_TEXT};}}
      .db-type-bool   {{background:{_AMBER_CHIP};   color:{_AMBER_CHIP_TEXT};}}
      .db-type-other  {{background:#f1f5f9;         color:{_SLATE_TEXT};}}

      .db-empty {{
        background:{_SLATE_BG}; border:2px dashed {_SLATE_BORDER};
        padding:2rem; text-align:center; border-radius:8px; color:{_SLATE_MUTED};
      }}
    </style>
    """,
    unsafe_allow_html=True,
)


# ---------------------------------------------------------------------------
# Warehouse access — delegated to datalink.ui._query so swapping DuckDB for
# Snowflake is a config flip, not a per-page edit. Short aliases _q/_q_scalar
# keep downstream call sites unchanged.
# ---------------------------------------------------------------------------


def _q(sql: str) -> pd.DataFrame:
    df = _wh_query(sql)
    # Snowflake uppercases unquoted column names (`SCHEMA_NAME`), DuckDB
    # preserves case as-stored (`schema_name`). Normalise to lowercase so
    # downstream DataFrame indexing works for either backend.
    if not df.empty:
        df.columns = df.columns.str.lower()
    return df


def _q_scalar(sql: str) -> Any:
    return _wh_scalar(sql)


def _quote_ident(ident: str) -> str:
    """DuckDB identifier quoting — handles mixed-case + special chars."""
    return '"' + ident.replace('"', '""') + '"'


def _layer_of(schema: str) -> str:
    """Classify a schema into Bronze / Silver / Gold / Control / Other."""
    up = schema.upper()
    if up == "CONTROL":
        return "Control"
    if up.startswith("BRONZE"):
        return "Bronze"
    if up.startswith("SILVER") and "GOLD_UM" in up:
        return "Gold"  # SILVER_gold_um_AETNA is actually the Gold staging
    if up.startswith("SILVER"):
        return "Silver"
    if up.startswith("GOLD"):
        return "Gold"
    return "Other"


def _client_of(schema: str) -> str:
    """Extract client tag from the schema suffix (AETNA, CARESOURCE, …)."""
    parts = schema.split("_")
    if len(parts) >= 2 and parts[-1].isupper() and len(parts[-1]) >= 3:
        return parts[-1]
    # No suffix → default tenant (schemas like BRONZE, SILVER, GOLD, CONTROL).
    return "default"


def _type_chip_class(dtype: str) -> str:
    d = dtype.upper()
    if any(t in d for t in ("INT", "BIGINT", "DECIMAL", "DOUBLE", "FLOAT", "NUMERIC")):
        return "db-type-int"
    if any(t in d for t in ("VARCHAR", "STRING", "TEXT", "CHAR")):
        return "db-type-str"
    if any(t in d for t in ("DATE", "TIMESTAMP", "TIME")):
        return "db-type-date"
    if "BOOL" in d:
        return "db-type-bool"
    return "db-type-other"


# ---------------------------------------------------------------------------
# Header + filters
# ---------------------------------------------------------------------------
st.title("🔍 Warehouse Explorer")
st.markdown(
    '<div class="db-hero">'
    "<h2>Schema and data explorer for the DuckDB warehouse</h2>"
    "<p>Browse every schema in <code>warehouse.duckdb</code> — Bronze, "
    "Silver, Gold, and Control. Select a table to inspect its columns, "
    "preview rows, or issue ad-hoc SELECT statements in the worksheet. "
    "Read-only by design; safe to use during live pipeline execution.</p>"
    "</div>",
    unsafe_allow_html=True,
)


# ---------------------------------------------------------------------------
# Enumerate schemas + tables ONCE per page render
# ---------------------------------------------------------------------------

schemas_df = _q(
    """
    SELECT schema_name
    FROM information_schema.schemata
    WHERE schema_name NOT IN ('information_schema','pg_catalog','main')
    ORDER BY schema_name
    """
)
all_schemas = schemas_df["schema_name"].tolist() if not schemas_df.empty else []

# Build client + layer classifications
schema_meta: dict[str, dict[str, str]] = {
    s: {"layer": _layer_of(s), "client": _client_of(s)} for s in all_schemas
}

# Filter row
f1, f2, f3 = st.columns([2, 3, 2])

with f1:
    client_opts = ["<all>", *sorted({m["client"] for m in schema_meta.values()})]
    sel_client = st.selectbox("Client", client_opts, index=0)

with f2:
    layer_opts = ["All", "Bronze", "Silver", "Gold", "Control"]
    sel_layer = st.radio("Layer", layer_opts, horizontal=True, index=0)

with f3:
    search = st.text_input("Search schema/table", value="").strip().lower()


def _schema_passes(s: str) -> bool:
    m = schema_meta[s]
    if sel_client != "<all>" and m["client"] != sel_client:
        return False
    if sel_layer != "All" and m["layer"] != sel_layer:
        return False
    return not (search and search not in s.lower())


filtered_schemas = [s for s in all_schemas if _schema_passes(s)]


# ---------------------------------------------------------------------------
# Session state — remember selected table across reruns
# ---------------------------------------------------------------------------
if "db_sel_schema" not in st.session_state:
    st.session_state.db_sel_schema = None
if "db_sel_table" not in st.session_state:
    st.session_state.db_sel_table = None
if "db_sql" not in st.session_state:
    st.session_state.db_sql = ""


def _select(schema: str, table: str) -> None:
    st.session_state.db_sel_schema = schema
    st.session_state.db_sel_table = table
    # Auto-populate the worksheet with a sensible starter query.
    st.session_state.db_sql = (
        f"SELECT * FROM {_quote_ident(schema)}.{_quote_ident(table)} LIMIT 100;"
    )


# ---------------------------------------------------------------------------
# Main two-column layout: schema tree (left) + detail pane (right)
# ---------------------------------------------------------------------------
tree_col, detail_col = st.columns([1, 3], gap="medium")

# ==========================================================================
# LEFT — schema tree
# ==========================================================================
with tree_col:
    st.markdown("### Schema Tree")
    if not filtered_schemas:
        st.markdown(
            '<div class="db-empty">No schemas match the filters.</div>',
            unsafe_allow_html=True,
        )
    else:
        for schema in filtered_schemas:
            layer = schema_meta[schema]["layer"]
            client_tag = schema_meta[schema]["client"]
            sel_schema = st.session_state.db_sel_schema
            # Default-expand the schema containing the currently-selected table.
            default_open = sel_schema == schema

            header = f"{layer} · {client_tag} · {schema}"
            with st.expander(header, expanded=default_open):
                tables_df = _q(
                    "SELECT table_name, table_type FROM information_schema.tables "
                    f"WHERE table_schema='{schema}' ORDER BY table_name"
                )
                if tables_df.empty:
                    st.markdown(
                        '<div style="color:#64748b;font-size:.8rem;padding:.3rem">(no tables)</div>',
                        unsafe_allow_html=True,
                    )
                    continue
                for _, row in tables_df.iterrows():
                    tname = str(row["table_name"])
                    # One button per table — Streamlit's only reliable click target.
                    key = f"tbtn_{schema}_{tname}"
                    btn_label = f"📄 {tname}"
                    is_active = (
                        st.session_state.db_sel_schema == schema
                        and st.session_state.db_sel_table == tname
                    )
                    if is_active:
                        btn_label = f"▶ {tname}"
                    if st.button(
                        btn_label,
                        key=key,
                        use_container_width=True,
                    ):
                        _select(schema, tname)
                        st.rerun()


# ==========================================================================
# RIGHT — table detail pane
# ==========================================================================
with detail_col:
    sel_schema = st.session_state.db_sel_schema
    sel_table = st.session_state.db_sel_table

    if not sel_schema or not sel_table:
        st.markdown(
            '<div class="db-empty">'
            "👈 Pick a table from the Schema Tree on the left.<br>"
            "<span style='font-size:.8rem'>Tip: narrow the list with the "
            "Layer filter (Bronze / Silver / Gold) or the search box above.</span>"
            "</div>",
            unsafe_allow_html=True,
        )
    else:
        # Breadcrumb
        layer = schema_meta.get(sel_schema, {}).get("layer", "Other")
        st.markdown(
            f'<div class="db-breadcrumb">'
            f"<b>warehouse.duckdb</b>"
            f'<span class="sep">›</span>{layer}'
            f'<span class="sep">›</span><b>{sel_schema}</b>'
            f'<span class="sep">›</span><b>{sel_table}</b>'
            "</div>",
            unsafe_allow_html=True,
        )

        # Three tabs — Snowflake-style (Columns + Data Preview + Worksheet)
        tab_cols, tab_data, tab_sql = st.tabs(["📋 Columns", "👁 Data Preview", "⚙️ SQL Worksheet"])

        # ----- Tab: Columns ---------------------------------------------------
        with tab_cols:
            qualified = f"{_quote_ident(sel_schema)}.{_quote_ident(sel_table)}"

            # Row count + column count stats
            row_count = _q_scalar(f"SELECT COUNT(*) FROM {qualified}")
            desc_df = _q(f"DESCRIBE {qualified}")
            col_count = len(desc_df) if not desc_df.empty else 0

            stats_html = '<div class="db-stat-grid">'
            stats_html += (
                f'<div class="db-stat">'
                f'<div class="label">Rows</div>'
                f'<div class="value">{row_count:,}'
                if isinstance(row_count, int)
                else ""
            )
            if not isinstance(row_count, int):
                stats_html += '<div class="db-stat"><div class="label">Rows</div><div class="value">—</div></div>'
            else:
                stats_html += "</div></div>"
            stats_html += (
                f'<div class="db-stat">'
                f'<div class="label">Columns</div>'
                f'<div class="value">{col_count}</div>'
                "</div>"
            )
            stats_html += (
                f'<div class="db-stat">'
                f'<div class="label">Layer</div>'
                f'<div class="value" style="font-size:.95rem">{layer}</div>'
                "</div>"
            )
            stats_html += (
                f'<div class="db-stat">'
                f'<div class="label">Tenant</div>'
                f'<div class="value" style="font-size:.95rem">'
                f"{schema_meta.get(sel_schema, {}).get('client', '—')}</div>"
                "</div>"
            )
            stats_html += "</div>"
            st.markdown(stats_html, unsafe_allow_html=True)

            if desc_df.empty:
                st.info("DESCRIBE returned no columns — table may be missing or unreadable.")
            else:
                # Render a clean columns listing with typed chips.
                render_rows: list[dict[str, Any]] = []
                for _, r in desc_df.iterrows():
                    name = str(r.get("column_name") or r.get("name") or "")
                    dtype = str(r.get("column_type") or r.get("type") or "")
                    nullable = r.get("null")
                    # DuckDB returns "YES"/"NO" for nullable; normalise.
                    nullable_str = (
                        "YES" if str(nullable).upper() in ("YES", "Y", "TRUE", "1") else "NO"
                    )
                    key_val = r.get("key")
                    pk_str = "PK" if str(key_val).upper() == "PRI" else ""
                    default_val = r.get("default", "")
                    render_rows.append(
                        {
                            "#": len(render_rows) + 1,
                            "Column": name,
                            "Type": dtype,
                            "Nullable": nullable_str,
                            "Default": default_val if default_val is not None else "",
                            "Key": pk_str,
                        }
                    )
                cols_pd = pd.DataFrame(render_rows)
                st.dataframe(cols_pd, use_container_width=True, hide_index=True)

        # ----- Tab: Data Preview ---------------------------------------------
        with tab_data:
            qualified = f"{_quote_ident(sel_schema)}.{_quote_ident(sel_table)}"
            limit_pick = st.selectbox(
                "Rows to preview",
                [50, 100, 500, 1000],
                index=1,
                key="db_preview_limit",
            )
            preview_df = _q(f"SELECT * FROM {qualified} LIMIT {limit_pick}")
            if preview_df.empty:
                st.info("No rows to display.")
            else:
                st.caption(f"Showing first {len(preview_df):,} rows of {sel_table}.")
                st.dataframe(preview_df, use_container_width=True, hide_index=True)
                csv = preview_df.to_csv(index=False).encode()
                st.download_button(
                    "⬇ Download preview as CSV",
                    data=csv,
                    file_name=f"{sel_schema}__{sel_table}.csv",
                    mime="text/csv",
                )

        # ----- Tab: SQL Worksheet --------------------------------------------
        with tab_sql:
            st.caption(
                "Free-form SELECT queries (read-only). "
                "Use the Run button — do NOT paste DDL/DML; it will be rejected."
            )
            sql_text = st.text_area(
                "Query",
                value=st.session_state.db_sql,
                height=180,
                key="db_sql_input",
                label_visibility="collapsed",
            )
            c_run, c_clear = st.columns([1, 1])
            with c_run:
                run = st.button("▶ Run query", type="primary", use_container_width=True)
            with c_clear:
                if st.button("Clear", use_container_width=True):
                    st.session_state.db_sql = ""
                    st.rerun()

            if run:
                stripped = sql_text.strip().rstrip(";").strip()
                if not stripped:
                    st.warning("Query is empty.")
                elif not stripped.lower().startswith(("select", "with", "describe", "show")):
                    st.error(
                        "Only SELECT / WITH / DESCRIBE / SHOW statements are allowed here. "
                        "This page is read-only."
                    )
                else:
                    df_out = _q(stripped)
                    if df_out.empty:
                        st.info("Query returned 0 rows.")
                    else:
                        st.success(f"{len(df_out):,} row(s) returned.")
                        st.dataframe(df_out, use_container_width=True, hide_index=True)
                        csv = df_out.to_csv(index=False).encode()
                        st.download_button(
                            "⬇ Download results as CSV",
                            data=csv,
                            file_name="query_results.csv",
                            mime="text/csv",
                        )
