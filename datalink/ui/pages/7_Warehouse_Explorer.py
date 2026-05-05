"""Warehouse Explorer — Phase 16.4 (Wave 4 #21).

Snowflake-native rebuild of the old DuckDB-only explorer. Browse schemas,
tables, columns, sample data, and run free-form SELECT against the active
warehouse via the same singleton the rest of the UI uses.

Read-only by design — destructive ops belong in Admin.
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
    page_title="Warehouse Explorer",
    page_icon="🔎",
    layout="wide",
    initial_sidebar_state="expanded",
)

from datalink.ui._nav import render_sidebar  # noqa: E402
from datalink.ui._query import warehouse_ctx  # noqa: E402

render_sidebar(active="Warehouse Explorer")

st.title("🔎 Warehouse Explorer")
st.caption(
    "Browse Snowflake schemas + tables + columns + sample data. Run "
    "ad-hoc SELECT queries. Read-only — destructive ops live in Admin."
)


@st.cache_data(ttl=30)  # type: ignore[misc]
def _list_databases():
    with warehouse_ctx(readonly=True) as wh:
        rows = list(wh.query("SHOW DATABASES"))
        return [r.get("name") or r.get("NAME") for r in rows if r]


@st.cache_data(ttl=30)  # type: ignore[misc]
def _list_schemas(db: str):
    with warehouse_ctx(readonly=True) as wh:
        rows = list(wh.query(f"SHOW SCHEMAS IN DATABASE {db}"))
        return [r.get("name") or r.get("NAME") for r in rows if r]


@st.cache_data(ttl=30)  # type: ignore[misc]
def _list_tables(db: str, schema: str):
    with warehouse_ctx(readonly=True) as wh:
        rows = list(wh.query(f"SHOW TABLES IN SCHEMA {db}.{schema}"))
        return [
            {
                "name": r.get("name") or r.get("NAME"),
                "rows": r.get("rows") or r.get("ROWS") or 0,
                "bytes": r.get("bytes") or r.get("BYTES") or 0,
                "kind": r.get("kind") or r.get("KIND") or "TABLE",
            }
            for r in rows
            if r
        ]


# Database picker
dbs = _list_databases()
if not dbs:
    st.error("Couldn't list databases — check Snowflake credentials.")
    st.stop()
default_db = "DATALINK_DEV" if "DATALINK_DEV" in dbs else dbs[0]
sel_db = st.selectbox("Database", options=dbs, index=dbs.index(default_db))

# Schema picker
schemas = _list_schemas(sel_db)
sel_schema = st.selectbox("Schema", options=schemas)

# Table list
tables = _list_tables(sel_db, sel_schema)
if not tables:
    st.info(f"No tables in `{sel_db}.{sel_schema}`.")
else:
    st.markdown(f"**{len(tables)} tables in `{sel_db}.{sel_schema}`**")
    df = pd.DataFrame(
        [
            {
                "Table": t["name"],
                "Kind": t["kind"],
                "Rows": int(t["rows"]),
                "Bytes": int(t["bytes"]),
            }
            for t in tables
        ]
    )
    st.dataframe(df, use_container_width=True, hide_index=True)

    st.markdown("---")
    sel_tbl = st.selectbox("Inspect a table", options=[t["name"] for t in tables])
    fq = f"{sel_db}.{sel_schema}.{sel_tbl}"

    tab_cols, tab_data, tab_sql = st.tabs(
        [
            "📋 Columns",
            "👁 Data preview",
            "🔍 SQL worksheet",
        ]
    )
    with tab_cols:
        with warehouse_ctx(readonly=True) as wh:
            cols = list(
                wh.query(
                    f"SELECT column_name, data_type, is_nullable, column_default, comment "
                    f"FROM {sel_db}.INFORMATION_SCHEMA.COLUMNS "
                    f"WHERE TABLE_SCHEMA = '{sel_schema}' AND TABLE_NAME = '{sel_tbl}' "
                    f"ORDER BY ordinal_position"
                )
            )
        if cols:
            st.dataframe(pd.DataFrame(cols), use_container_width=True, hide_index=True)
    with tab_data:
        n = st.selectbox("Rows", options=[10, 50, 100, 500], index=1)
        try:
            with warehouse_ctx(readonly=True) as wh:
                rows = list(wh.query(f"SELECT * FROM {fq} LIMIT {n}"))
            if rows:
                st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
            else:
                st.caption("Empty.")
        except Exception as exc:
            st.error(f"Query failed: {exc}")
    with tab_sql:
        sql = st.text_area(
            "SELECT-only SQL (read-only — no DDL/DML):",
            value=f"SELECT * FROM {fq} LIMIT 100",
            height=120,
        )
        if st.button("▶️ Run", type="primary"):
            up = sql.strip().upper()
            if not (
                up.startswith("SELECT")
                or up.startswith("WITH")
                or up.startswith("DESC")
                or up.startswith("SHOW")
            ):
                st.error(
                    "Only SELECT / WITH / DESCRIBE / SHOW allowed. "
                    "Destructive ops via Admin page only."
                )
            else:
                try:
                    with warehouse_ctx(readonly=True) as wh:
                        rs = list(wh.query(sql))
                    if rs:
                        df_out = pd.DataFrame(rs)
                        st.dataframe(df_out, use_container_width=True, hide_index=True)
                        st.download_button(
                            "📥 Download CSV",
                            df_out.to_csv(index=False),
                            file_name=f"{sel_tbl}.csv",
                            mime="text/csv",
                        )
                    else:
                        st.caption("Query returned 0 rows.")
                except Exception as exc:
                    st.error(f"Query failed: {exc}")
