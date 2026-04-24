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

import json
import os
from collections.abc import Iterator, Sequence
from contextlib import contextmanager, suppress
from typing import Any, Protocol, cast

import pandas as pd
import streamlit as st

# How long cached query results stay warm. Dashboards re-render on every
# user interaction; caching results for a short window turns 10 network
# round-trips per render (~1-2s on Snowflake) into 0 (<5ms).
#
# Tune via DL_CT_QUERY_TTL_SECONDS. Default 30s gives a responsive feel
# without masking real-world data movement for more than half a minute.
_QUERY_CACHE_TTL = int(os.environ.get("DL_CT_QUERY_TTL_SECONDS", "30"))

WAREHOUSE_PATH = os.environ.get("DL_CT_WAREHOUSE_PATH", "/opt/datalink/warehouse.duckdb")

# Params accepted by the Streamlit warehouse helpers.
#
# DuckDB accepts positional (list/tuple with ``?`` placeholders) and named
# (dict with ``$name`` placeholders). Legacy call sites in this codebase
# use positional ``?``, so we accept both forms and pass through verbatim.
# Snowflake's paramstyle reconciliation is Day 3 work.
_Params = dict[str, Any] | Sequence[Any] | None


class _Warehouse(Protocol):
    """Minimal Warehouse protocol the Streamlit layer relies on."""

    def query(self, sql: str, params: _Params = None) -> list[dict[str, Any]]: ...
    def execute(self, sql: str, params: _Params = None) -> None: ...


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

    def query(self, sql: str, params: _Params = None) -> list[dict[str, Any]]:
        conn = self._open()
        try:
            cur = conn.execute(sql, params) if params else conn.execute(sql)
            cols = [d[0] for d in cur.description or []]
            rows = cur.fetchall()
            return [dict(zip(cols, row, strict=True)) for row in rows]
        finally:
            conn.close()

    def execute(self, sql: str, params: _Params = None) -> None:
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


# Module-level Snowflake adapter singleton. The connector's auth handshake
# is ~2-3s; opening a fresh connection per UI query makes every page load
# wait for cumulative handshakes. Keeping one persistent connection across
# all queries in the process lets Streamlit pages render at network-latency
# speed (~100ms/query in Azure East US 2) instead of auth + network.
#
# Lifetime: survives for the lifetime of the Streamlit server process.
# snowflake.connector uses TCP keepalive + session renewal internally, so
# the connection stays healthy across an entire demo.
_snowflake_singleton: _Warehouse | None = None


def _build_backend(*, readonly: bool) -> _Warehouse:
    """Construct the right backend for the current configuration."""
    wh_type = _warehouse_type()
    if wh_type == "duckdb":
        # DuckDB uses per-query short-lived connections to avoid fighting
        # the scheduler for the file lock — stateless shim is correct here.
        return _DuckDBShim(WAREHOUSE_PATH, readonly=readonly)

    # Snowflake: reuse a single cached adapter. No file locks to juggle,
    # and the auth handshake is expensive enough that per-query recreation
    # dominates page-render latency.
    if wh_type == "snowflake":
        global _snowflake_singleton
        if _snowflake_singleton is None:
            from datalink.adapters.warehouse.snowflake_adapter import SnowflakeWarehouse
            from datalink.config.models import WarehouseConfig

            _snowflake_singleton = cast(
                "_Warehouse",
                SnowflakeWarehouse(WarehouseConfig(type="snowflake")),
            )
        return _snowflake_singleton

    # Future backends — fall back to the full factory path.
    from datalink.adapters.factory import build_adapters
    from datalink.config.loader import load_settings

    adapters = build_adapters(load_settings(env=os.environ.get("DL_ENV", "local")))
    return cast("_Warehouse", adapters.warehouse)


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


# Backends returned by _build_backend for DuckDB are stateless (per-query
# short-lived conns inside the shim); for Snowflake they're a long-lived
# singleton. In neither case do we want to call close() after a single
# query — DuckDB shim handles its own lifecycle, and closing the Snowflake
# singleton would force a full re-auth on the next page render. `warehouse_ctx`
# remains the explicit batch-boundary close point for callers who want one.


def _serialize_params(params: _Params) -> str:
    """Serialize params to a stable string key for the @st.cache_data hash.

    Dicts / lists aren't natively hashable; JSON gives a deterministic key
    that also round-trips losslessly for the allowed value types in our
    codebase (str, int, float, bool, None).
    """
    if params is None:
        return "~none~"
    if isinstance(params, dict):
        return "~dict~" + json.dumps(params, sort_keys=True, default=str)
    return "~seq~" + json.dumps(list(params), default=str)


def _deserialize_params(key: str) -> _Params:
    if key == "~none~":
        return None
    if key.startswith("~dict~"):
        return cast("dict[str, Any]", json.loads(key[len("~dict~") :]))
    if key.startswith("~seq~"):
        return cast("Sequence[Any]", json.loads(key[len("~seq~") :]))
    return None


# In-process TTL cache. Simpler + more predictable than @st.cache_data,
# which has edge-case behaviour around the Streamlit ScriptRunner context
# that we were hitting. Everything lives in this module's globals and
# survives every Streamlit rerun for the lifetime of the server process.
#
# Format: _cache[key] = (inserted_at_monotonic_seconds, rows_or_error_marker)
# A MISSING TABLE or syntax error is cached as an empty list so subsequent
# renders don't pay the 100-200ms Snowflake round-trip to re-learn the same
# error. TTL for negative results is shorter so the UI picks up newly-created
# tables within ~10s of a pipeline completing.
_cache: dict[tuple[str, str], tuple[float, list[dict[str, Any]]]] = {}
# Sentinel wrapping: when we cache a failure, we store an empty list with
# a *negative* TTL marker so the hit logic distinguishes success-empty
# from failed-empty.
_neg_cache: dict[tuple[str, str], float] = {}
# Negative TTL: long enough that fast refreshes don't re-probe a missing
# table over and over; short enough that after a pipeline creates the
# table, operators see it within a minute or two (or sooner if they hit
# the "🔄 Refresh now" button which calls clear_query_cache()).
_NEG_CACHE_TTL = 120


def _cached_rows(sql: str, params_key: str) -> list[dict[str, Any]]:
    """Cached warehouse query keyed by (sql, serialized_params).

    Positive results cached for ``DL_CT_QUERY_TTL_SECONDS`` seconds
    (default 30s). Failed queries (missing tables, syntax errors, auth
    hiccups) cached as empty result for ``_NEG_CACHE_TTL`` seconds so
    dashboards don't re-ask Snowflake about a non-existent table on
    every render.

    Every HIT, MISS, and ERROR is logged to stderr with timing so
    operators can confirm cache behaviour via ``docker logs``.
    """
    import sys as _sys
    import time as _time

    key = (sql, params_key)
    now = _time.monotonic()

    # Positive cache check
    cached = _cache.get(key)
    if cached is not None and (now - cached[0]) < _QUERY_CACHE_TTL:
        print(
            f"[_query.HIT ]       - | {sql[:90].replace(chr(10), ' ')}",
            file=_sys.stderr,
            flush=True,
        )
        return cached[1]

    # Negative cache check — don't re-ask about a missing table
    neg_at = _neg_cache.get(key)
    if neg_at is not None and (now - neg_at) < _NEG_CACHE_TTL:
        print(
            f"[_query.NEG ]       - | {sql[:90].replace(chr(10), ' ')}",
            file=_sys.stderr,
            flush=True,
        )
        return []

    # MISS — actually hit the warehouse.
    params = _deserialize_params(params_key)
    t0 = now
    try:
        rows = _build_backend(readonly=True).query(sql, params)
    except Exception as exc:
        elapsed_ms = (_time.monotonic() - t0) * 1000
        _neg_cache[key] = now
        print(
            f"[_query.ERR ] {elapsed_ms:7.0f}ms | {type(exc).__name__}: "
            f"{sql[:80].replace(chr(10), ' ')}",
            file=_sys.stderr,
            flush=True,
        )
        return []

    elapsed_ms = (_time.monotonic() - t0) * 1000
    _cache[key] = (now, rows)
    print(
        f"[_query.MISS] {elapsed_ms:7.0f}ms | {sql[:90].replace(chr(10), ' ')}",
        file=_sys.stderr,
        flush=True,
    )
    return rows


def query(sql: str, params: _Params = None) -> pd.DataFrame:
    """Run a SELECT and return a DataFrame. Errors surface as ``st.error``.

    Results cached for _QUERY_CACHE_TTL seconds (default 30s). Interactive
    re-renders within that window return instantly from memory instead of
    hitting Snowflake / DuckDB again.
    """
    try:
        rows = _cached_rows(sql, _serialize_params(params))
        return pd.DataFrame(rows)
    except Exception as exc:
        st.error(f"Warehouse query failed: {exc}")
        return pd.DataFrame()


def query_silent(sql: str, params: _Params = None) -> pd.DataFrame:
    """Like :func:`query` but swallows errors silently.

    Use for opportunistic lookups on tables that may not yet exist
    (e.g. before any pipeline run has populated BRONZE_*).
    """
    try:
        rows = _cached_rows(sql, _serialize_params(params))
        return pd.DataFrame(rows)
    except Exception:
        return pd.DataFrame()


def query_scalar(sql: str, params: _Params = None) -> Any:
    """Run a SELECT and return the first column of the first row, or ``None``."""
    try:
        rows = _cached_rows(sql, _serialize_params(params))
        if not rows:
            return None
        # Warehouse.query contract guarantees list[dict[str, Any]] — take
        # the first column of the first row.
        first = rows[0]
        vals = list(first.values())
        return vals[0] if vals else None
    except Exception:
        return None


def clear_query_cache() -> None:
    """Blow away every cached query result (positive AND negative). Call from
    a 'Refresh now' button when operators need an immediate re-fetch — e.g.
    right after a DAG run that should have just created BRONZE tables."""
    _cache.clear()
    _neg_cache.clear()
