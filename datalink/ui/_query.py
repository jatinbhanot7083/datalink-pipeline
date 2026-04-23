"""Centralized warehouse access for Streamlit pages — Phase 7 Day 2.

Before Phase 7, every Streamlit page opened ``duckdb.connect(WAREHOUSE_PATH,
read_only=True)`` directly. That leaked a DuckDB-specific code path into 13
UI sites and made swapping the backend (e.g. to Snowflake) a per-page edit.

This module centralises all UI warehouse access behind three helpers that
honour the configured backend (``settings.adapters.warehouse.type``):

  * :func:`query`           — read-only SELECT → ``pd.DataFrame``
  * :func:`query_scalar`    — read-only SELECT → scalar (or ``None``)
  * :func:`warehouse_ctx`   — context manager yielding a Warehouse-protocol
                              object, for callers that need the full adapter
                              (e.g. :class:`SuiteRegistry`) or writes

Backend routing
---------------

``duckdb``
    Per-call short-lived connection, opened ``read_only=True`` by default.
    DuckDB holds a process-level exclusive write lock, so a long-lived
    read-only connection from Streamlit would block the scheduler during
    DAG runs. Opening briefly per query (then closing) lets multiple
    readers coexist with one writer without lock contention.

``snowflake``
    Uses the factory-built adapter from :func:`build_adapters`. No file
    locks; role-based permissions in Snowflake handle read/write concerns
    so the ``readonly`` flag is ignored for Snowflake.

Errors surface as ``st.error(...)`` on the calling page; helpers do not
raise. ``query_silent`` is available for opportunistic lookups where the
underlying table may not exist yet (e.g. a dashboard rendered before the
first pipeline run).
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from typing import Any, Protocol

import pandas as pd
import streamlit as st

WAREHOUSE_PATH = os.environ.get("DL_CT_WAREHOUSE_PATH", "/opt/datalink/warehouse.duckdb")


class _Warehouse(Protocol):
    """Minimal Warehouse protocol the Streamlit layer relies on."""

    def query(self, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]: ...
    def execute(self, sql: str, params: dict[str, Any] | None = None) -> None: ...


# ---------------------------------------------------------------------------
# DuckDB shims — short-lived connections, per call
# ---------------------------------------------------------------------------


class _DuckDBShim:
    """Opens a fresh DuckDB connection per call.

    Short-lived connections avoid fighting the scheduler for DuckDB's
    process-level write lock. ``readonly=True`` is the default for
    dashboards; ``readonly=False`` is used by DQ Author / DQ Review when
    they need to INSERT into ``CONTROL.dq_suites`` via the SuiteRegistry.
    """

    def __init__(self, path: str, *, readonly: bool) -> None:
        self._path = path
        self._readonly = readonly

    def _open(self) -> Any:
        import duckdb

        return duckdb.connect(self._path, read_only=self._readonly)

    def query(self, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        conn = self._open()
        try:
            cur = conn.execute(sql, params) if params else conn.execute(sql)
            cols = [d[0] for d in cur.description or []]
            rows = cur.fetchall()
            return [dict(zip(cols, row, strict=True)) for row in rows]
        finally:
            conn.close()

    def execute(self, sql: str, params: dict[str, Any] | None = None) -> None:
        if self._readonly:
            raise RuntimeError(
                "execute() called on a read-only DuckDB shim — "
                "open with warehouse_ctx(readonly=False) for writes."
            )
        conn = self._open()
        try:
            conn.execute(sql, params) if params else conn.execute(sql)
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# Backend selection
# ---------------------------------------------------------------------------


def _warehouse_type() -> str:
    """Read ``settings.adapters.warehouse.type``. Default to ``duckdb`` on any error."""
    try:
        from datalink.config.loader import load_settings

        settings = load_settings(env=os.environ.get("DL_ENV", "local"))
        return str(settings.adapters.warehouse.type)
    except Exception:
        return "duckdb"


def _build_backend(*, readonly: bool) -> _Warehouse:
    """Construct the right backend for the current configuration."""
    wh_type = _warehouse_type()
    if wh_type == "duckdb":
        return _DuckDBShim(WAREHOUSE_PATH, readonly=readonly)

    # Snowflake / other: use the production factory. Role-based permissions
    # in the backend handle read/write safety, so `readonly` is advisory only.
    from datalink.adapters.factory import build_adapters
    from datalink.config.loader import load_settings

    adapters = build_adapters(load_settings(env=os.environ.get("DL_ENV", "local")))
    return adapters.warehouse


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


@contextmanager
def warehouse_ctx(*, readonly: bool = True) -> Iterator[_Warehouse]:
    """Yield a Warehouse-protocol object for the duration of a block.

    Use this when a page needs to pass a warehouse to another class (e.g.
    ``SuiteRegistry``) or run multiple related statements. Closes the
    backend connection (if applicable) on block exit.
    """
    wh = _build_backend(readonly=readonly)
    try:
        yield wh
    finally:
        close_fn = getattr(wh, "close", None)
        if callable(close_fn):
            with suppress(Exception):
                close_fn()


def query(sql: str, params: dict[str, Any] | None = None) -> pd.DataFrame:
    """Run a SELECT and return a DataFrame. Errors surface as ``st.error``."""
    try:
        wh = _build_backend(readonly=True)
        try:
            rows = wh.query(sql, params)
        finally:
            close_fn = getattr(wh, "close", None)
            if callable(close_fn):
                with suppress(Exception):
                    close_fn()
        return pd.DataFrame(rows)
    except Exception as exc:
        st.error(f"Warehouse query failed: {exc}")
        return pd.DataFrame()


def query_silent(sql: str, params: dict[str, Any] | None = None) -> pd.DataFrame:
    """Like :func:`query` but swallows errors silently.

    Use for opportunistic lookups on tables that may not yet exist
    (e.g. before any pipeline run has populated BRONZE_*).
    """
    try:
        wh = _build_backend(readonly=True)
        try:
            rows = wh.query(sql, params)
        finally:
            close_fn = getattr(wh, "close", None)
            if callable(close_fn):
                with suppress(Exception):
                    close_fn()
        return pd.DataFrame(rows)
    except Exception:
        return pd.DataFrame()


def query_scalar(sql: str, params: dict[str, Any] | None = None) -> Any:
    """Run a SELECT and return the first column of the first row, or ``None``."""
    try:
        wh = _build_backend(readonly=True)
        try:
            rows = wh.query(sql, params)
        finally:
            close_fn = getattr(wh, "close", None)
            if callable(close_fn):
                with suppress(Exception):
                    close_fn()
        if not rows:
            return None
        # Warehouse.query contract guarantees list[dict[str, Any]] — take
        # the first column of the first row.
        first = rows[0]
        vals = list(first.values())
        return vals[0] if vals else None
    except Exception:
        return None
