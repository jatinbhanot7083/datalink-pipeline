"""Schema fingerprint — Phase 6 Commit 2.

Deterministic 16-char hash of a table's column list + types. Used by the
Pre-Validation Crew to detect whether the schema has changed since the
last time it authored rules for (client_id, source_type).

Why:
  Pre-Val used to fire on every DAG run, making 9+ LLM calls per client
  per pipeline. That is wasteful AND expensive:
    * A client ingesting CLAIMS daily would re-author the same rules 365
      times a year for the same schema.
    * At Opus 4.7 pricing (adaptive thinking), ~$1-$3 per unnecessary run.

  With fingerprinting: the crew runs ONCE per (client, source_type,
  schema). Downstream DAG runs pass through the cache and make zero LLM
  calls. Drift is detected structurally — add/remove/retype a column and
  the fingerprint changes, crew re-runs, operator sees new DRAFT.

Contract:
  * Fingerprint is computed from `DESCRIBE {table}` output.
  * We hash only (column_name, column_type) tuples — NOT row data.
  * Sorted so order-of-DDL-addition does NOT affect the hash.
  * 16-char sha256 prefix is cryptographically unique enough for this
    domain (collision prob ~1 in 2^64 within the same client_id).

Unit test: `tests/unit/test_schema_fingerprint.py`.
"""

from __future__ import annotations

import hashlib
from typing import Any, Protocol


class _WarehouseQuery(Protocol):
    """Tiny adapter protocol — anything that can run DESCRIBE."""

    def query(self, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]: ...


def compute_fingerprint(warehouse: _WarehouseQuery, qualified_table: str) -> str:
    """Return a 16-char hex fingerprint of the table's column layout.

    Raises RuntimeError if DESCRIBE produces no columns — that indicates
    either a missing table OR a warehouse adapter bug. Silently returning
    "empty" here would risk cache poisoning (one run gets empty hash,
    next run sees new columns, drift wrongly flagged).
    """
    rows = warehouse.query(f"DESCRIBE {qualified_table}")
    if not rows:
        raise RuntimeError(
            f"DESCRIBE {qualified_table} returned zero columns — "
            f"cannot fingerprint a missing / unreadable table."
        )
    # Normalise column name casing (DuckDB sometimes returns mixed case).
    # Column type kept verbatim — type drift (VARCHAR -> INTEGER) is
    # exactly what we want the fingerprint to catch.
    col_pairs = sorted(
        (
            str(r.get("column_name") or r.get("name") or "").lower(),
            str(r.get("column_type") or r.get("type") or ""),
        )
        for r in rows
    )
    raw = "|".join(f"{name}:{dtype}" for name, dtype in col_pairs).encode()
    return hashlib.sha256(raw).hexdigest()[:16]


def columns_from_fingerprint_debug(
    warehouse: _WarehouseQuery, qualified_table: str
) -> list[tuple[str, str]]:
    """Return the actual (col_name, col_type) list that would be hashed.

    Useful for log messages when drift is detected — operators want to
    see exactly WHICH columns changed. Not used by the caching hot-path.
    """
    rows = warehouse.query(f"DESCRIBE {qualified_table}")
    return sorted(
        (
            str(r.get("column_name") or r.get("name") or "").lower(),
            str(r.get("column_type") or r.get("type") or ""),
        )
        for r in rows
    )
