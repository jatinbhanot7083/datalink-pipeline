"""Backend-agnostic column introspection.

DuckDB and Snowflake both have ``DESCRIBE`` but with different syntax and
result shapes:

  * DuckDB:    ``DESCRIBE schema.table`` -> rows with ``column_name``,
               ``column_type``. Identifier case is preserved as-written.
  * Snowflake: ``DESCRIBE schema.table`` is a **syntax error** — it expects
               ``DESCRIBE TABLE``. Even with that, the result column layout
               differs. Easier route: query INFORMATION_SCHEMA. Snowflake
               folds unquoted identifiers to UPPERCASE, so result keys come
               back upper unless the adapter normalises (the project's
               Snowflake adapter does lowercase keys, so callers can read
               ``r["column_name"]`` either way).

This module hides both quirks behind one function so callers don't have to
maintain parallel code paths. Use :func:`list_columns` for plain column
names, :func:`list_columns_typed` when both name and type are needed.
"""

from __future__ import annotations

from typing import Any, Protocol


class _WarehouseQuery(Protocol):
    """Minimal adapter protocol — anything with ``query()``."""

    def query(self, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]: ...


def list_columns(warehouse: _WarehouseQuery, qualified_table: str) -> list[str]:
    """Return ordered column names of ``qualified_table`` (``SCHEMA.TABLE``).

    Tries DuckDB's ``DESCRIBE`` first; on any failure falls back to
    ``INFORMATION_SCHEMA.COLUMNS`` (Snowflake-compatible). Empty result is
    NOT silently absorbed — callers that need a present-table guarantee
    should check ``len(rows) > 0`` themselves.
    """
    try:
        rows = warehouse.query(f"DESCRIBE {qualified_table}")
        return [str(r.get("column_name") or r.get("name") or "") for r in rows]
    except Exception:
        # Snowflake path. Use $-style params + dict — the Snowflake adapter
        # rewrites $name → %(name)s only when params is a dict.
        schema, name = qualified_table.split(".", 1)
        rows = warehouse.query(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = $schema AND table_name = $name "
            "ORDER BY ordinal_position",
            {"schema": schema.upper(), "name": name.upper()},
        )
        return [str(r["column_name"]) for r in rows]


def list_columns_typed(warehouse: _WarehouseQuery, qualified_table: str) -> list[tuple[str, str]]:
    """Return ordered ``(column_name, column_type)`` tuples for fingerprinting.

    Same DuckDB-then-Snowflake fallback as :func:`list_columns`. Names are
    lower-cased (case-insensitive equality across backends), types kept
    verbatim — drift detectors want to catch type changes (``VARCHAR`` →
    ``INTEGER``) which means the type string must match exactly.
    """
    try:
        rows = warehouse.query(f"DESCRIBE {qualified_table}")
        return [
            (
                str(r.get("column_name") or r.get("name") or "").lower(),
                str(r.get("column_type") or r.get("type") or ""),
            )
            for r in rows
        ]
    except Exception:
        schema, name = qualified_table.split(".", 1)
        rows = warehouse.query(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = $schema AND table_name = $name "
            "ORDER BY ordinal_position",
            {"schema": schema.upper(), "name": name.upper()},
        )
        return [(str(r["column_name"]).lower(), str(r["data_type"])) for r in rows]
