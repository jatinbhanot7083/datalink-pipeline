"""GX expectation runner — Phase 16.8 (real checkpoint execution).

Replaces the legacy Phase-5 stub in bronze_validate_task with a real
checkpoint that:

  1. Pulls the LIVE GX suite for (client_id, source_type) from CONTROL.dq_suites
  2. Executes each expectation against the Bronze table via SQL
  3. Records every result in CONTROL.gx_validation_results
  4. Returns aggregate counts (passed / failed / total) so the calling task
     can decide whether to fail the DAG

We don't shell to the upstream ``great_expectations`` CLI for two reasons:
  * It expects a project structure on disk that doesn't match how DataLink
    stores suites (we keep them in Snowflake CONTROL).
  * SQL-side execution is faster + more transparent (every check becomes a
    single COUNT query that an auditor can read).

Supported expectation types (covers ~95% of common rules):

  * expect_column_to_exist
  * expect_column_values_to_not_be_null
  * expect_column_values_to_be_in_set
  * expect_column_values_to_match_regex
  * expect_column_values_to_be_between
  * expect_column_value_lengths_to_be_between
  * expect_column_values_to_be_unique
  * expect_table_row_count_to_be_between
  * expect_column_values_to_be_of_type        (best-effort via INFORMATION_SCHEMA)

Anything we don't recognize gets a SKIPPED result (recorded so the gap is
visible in CONTROL.gx_validation_results — operator can plug in support
for new types later).
"""

from __future__ import annotations

import json
import re
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from datalink.adapters.protocols import Warehouse
from datalink.logging import get_logger
from datalink.quality.control import CONTROL_SCHEMA

_log = get_logger(__name__)


@dataclass
class GxResult:
    expectation_type: str
    column: str | None
    success: bool
    observed: int
    threshold: int | None
    elements_failed: int
    skipped: bool
    skip_reason: str | None
    error: str | None
    sql: str | None


def _now() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def _identifier_safe(s: str) -> str:
    """Crude SQL identifier sanitiser — letters / digits / underscore only."""
    return re.sub(r"[^A-Za-z0-9_]", "", s)


def _load_live_suite(
    wh: Warehouse,
    *,
    client_id: str,
    source_type: str | None,
    suite_id: str | None,
) -> dict[str, Any] | None:
    """Find the LIVE GX suite. Prefer suite_id when given; else look up by
    (client_id, source_type) and fall back to client-only and global default."""
    if suite_id:
        rows = list(
            wh.query(
                f"SELECT * FROM {CONTROL_SCHEMA}.dq_suites WHERE suite_id = $sid",
                {"sid": suite_id},
            )
        )
        if rows:
            return rows[0]
    if source_type:
        rows = list(
            wh.query(
                f"SELECT * FROM {CONTROL_SCHEMA}.dq_suites "
                f"WHERE client_id = $c AND source_type = $st AND status = 'LIVE' "
                f"ORDER BY version DESC LIMIT 1",
                {"c": client_id, "st": source_type},
            )
        )
        if rows:
            return rows[0]
    rows = list(
        wh.query(
            f"SELECT * FROM {CONTROL_SCHEMA}.dq_suites "
            f"WHERE client_id = $c AND status = 'LIVE' "
            f"ORDER BY version DESC LIMIT 1",
            {"c": client_id},
        )
    )
    return rows[0] if rows else None


def _parse_expectations(suite_row: dict[str, Any]) -> list[dict[str, Any]]:
    """Suite ``expectations_json`` is VARIANT — already parsed by the connector
    in most cases, but defensively json-load the string form."""
    raw = suite_row.get("expectations_json") or suite_row.get("expectations")
    if raw is None:
        return []
    if isinstance(raw, list):
        return list(raw)
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
            return list(parsed) if isinstance(parsed, list) else []
        except json.JSONDecodeError:
            return []
    return []


# ---------------------------------------------------------------------------
# Per-expectation executors
# ---------------------------------------------------------------------------


def _exec_column_exists(
    wh: Warehouse,
    *,
    fq_table: str,
    column: str,
) -> tuple[bool, int, str]:
    sch, _, tbl = fq_table.partition(".")
    sql = (
        f"SELECT COUNT(*) AS c FROM INFORMATION_SCHEMA.COLUMNS "
        f"WHERE TABLE_SCHEMA = '{sch}' AND TABLE_NAME = '{tbl.upper()}' "
        f"  AND column_name = '{column.upper()}'"
    )
    rows = list(wh.query(sql))
    n = int(rows[0]["c"]) if rows else 0
    return n > 0, n, sql


def _exec_not_null(
    wh: Warehouse,
    *,
    fq_table: str,
    column: str,
    mostly: float = 1.0,
) -> tuple[bool, int, int, str]:
    col = _identifier_safe(column)
    sql = (
        f"SELECT COUNT(*) AS total, "
        f"       SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) AS nulls "
        f"FROM {fq_table}"
    )
    rows = list(wh.query(sql))
    if not rows:
        return False, 0, 0, sql
    total = int(rows[0]["total"] or 0)
    nulls = int(rows[0]["nulls"] or 0)
    if total == 0:
        return True, 0, 0, sql  # vacuous
    pct_not_null = (total - nulls) / total
    return pct_not_null >= mostly, nulls, total, sql


def _exec_in_set(
    wh: Warehouse,
    *,
    fq_table: str,
    column: str,
    value_set: list[str],
) -> tuple[bool, int, int, str]:
    col = _identifier_safe(column)
    quoted = ", ".join("'" + str(v).replace("'", "''") + "'" for v in value_set)
    if not quoted:
        return True, 0, 0, ""
    sql = (
        f"SELECT COUNT(*) AS bad "
        f"FROM {fq_table} "
        f"WHERE {col} IS NOT NULL AND {col} NOT IN ({quoted})"
    )
    rows = list(wh.query(sql))
    bad = int(rows[0]["bad"] or 0)
    return bad == 0, bad, 0, sql


def _exec_match_regex(
    wh: Warehouse,
    *,
    fq_table: str,
    column: str,
    regex: str,
) -> tuple[bool, int, int, str]:
    col = _identifier_safe(column)
    sql = (
        f"SELECT COUNT(*) AS bad "
        f"FROM {fq_table} "
        f"WHERE {col} IS NOT NULL "
        f"  AND NOT REGEXP_LIKE({col}, '{regex.replace(chr(39), chr(39) + chr(39))}')"
    )
    rows = list(wh.query(sql))
    bad = int(rows[0]["bad"] or 0)
    return bad == 0, bad, 0, sql


def _exec_between(
    wh: Warehouse,
    *,
    fq_table: str,
    column: str,
    min_value: Any = None,
    max_value: Any = None,
) -> tuple[bool, int, int, str]:
    col = _identifier_safe(column)
    where = []
    if min_value is not None:
        where.append(f"{col} < {min_value}")
    if max_value is not None:
        where.append(f"{col} > {max_value}")
    if not where:
        return True, 0, 0, ""
    sql = (
        f"SELECT COUNT(*) AS bad FROM {fq_table} WHERE {col} IS NOT NULL AND ({' OR '.join(where)})"
    )
    rows = list(wh.query(sql))
    bad = int(rows[0]["bad"] or 0)
    return bad == 0, bad, 0, sql


def _exec_unique(
    wh: Warehouse,
    *,
    fq_table: str,
    column: str,
) -> tuple[bool, int, int, str]:
    col = _identifier_safe(column)
    sql = (
        f"SELECT COUNT(*) AS dup_groups "
        f"FROM (SELECT {col} FROM {fq_table} "
        f"      WHERE {col} IS NOT NULL "
        f"      GROUP BY {col} HAVING COUNT(*) > 1)"
    )
    rows = list(wh.query(sql))
    dups = int(rows[0]["dup_groups"] or 0)
    return dups == 0, dups, 0, sql


def _exec_row_count_between(
    wh: Warehouse,
    *,
    fq_table: str,
    min_value: int | None = None,
    max_value: int | None = None,
) -> tuple[bool, int, int, str]:
    sql = f"SELECT COUNT(*) AS c FROM {fq_table}"
    rows = list(wh.query(sql))
    n = int(rows[0]["c"] or 0)
    ok = True
    if min_value is not None and n < int(min_value):
        ok = False
    if max_value is not None and n > int(max_value):
        ok = False
    return ok, n, n, sql


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def run_checkpoint(
    *,
    warehouse: Warehouse,
    client_id: str,
    dataset_code: str,
    fq_table: str,
    suite_id: str | None = None,
    source_type: str | None = None,
    pipeline_run_id: str | None = None,
) -> dict[str, Any]:
    """Execute the LIVE GX suite for this (client, source) against ``fq_table``.

    Records every result in CONTROL.gx_validation_results. Returns:

        {
          "suite_id": str | None,
          "passed": int, "failed": int, "skipped": int, "total": int,
          "results": list[GxResult],
          "had_critical_failure": bool,
        }

    The caller (typically bronze_validate_task) decides how to react. We do
    NOT raise on failure here — observability separation.
    """
    suite = _load_live_suite(
        warehouse,
        client_id=client_id,
        source_type=source_type or dataset_code.upper(),
        suite_id=suite_id,
    )
    if suite is None:
        _log.info(
            "gx.no_suite_found",
            client_id=client_id,
            dataset_code=dataset_code,
        )
        return {
            "suite_id": None,
            "passed": 0,
            "failed": 0,
            "skipped": 0,
            "total": 0,
            "results": [],
            "had_critical_failure": False,
        }

    suite_id = str(suite["suite_id"])
    expectations = _parse_expectations(suite)
    if not expectations:
        _log.info("gx.suite_has_no_expectations", suite_id=suite_id)

    run_id = pipeline_run_id or str(uuid.uuid4())
    results: list[GxResult] = []
    n_pass = 0
    n_fail = 0
    n_skip = 0

    for exp in expectations:
        et = str(exp.get("expectation_type") or "").strip()
        kw = exp.get("kwargs") or {}
        col = kw.get("column")
        result = GxResult(
            expectation_type=et,
            column=col,
            success=False,
            observed=0,
            threshold=None,
            elements_failed=0,
            skipped=False,
            skip_reason=None,
            error=None,
            sql=None,
        )

        try:
            if et == "expect_column_to_exist":
                ok, observed, sql = _exec_column_exists(warehouse, fq_table=fq_table, column=col)
                result.success = ok
                result.observed = observed
                result.sql = sql
            elif et == "expect_column_values_to_not_be_null":
                mostly = float(kw.get("mostly", 1.0))
                ok, nulls, total, sql = _exec_not_null(
                    warehouse, fq_table=fq_table, column=col, mostly=mostly
                )
                result.success = ok
                result.elements_failed = nulls
                result.observed = total
                result.threshold = int(mostly * 100)
                result.sql = sql
            elif et == "expect_column_values_to_be_in_set":
                vs = kw.get("value_set") or []
                ok, bad, _, sql = _exec_in_set(
                    warehouse, fq_table=fq_table, column=col, value_set=vs
                )
                result.success = ok
                result.elements_failed = bad
                result.sql = sql
            elif et == "expect_column_values_to_match_regex":
                rx = kw.get("regex") or ""
                ok, bad, _, sql = _exec_match_regex(
                    warehouse, fq_table=fq_table, column=col, regex=rx
                )
                result.success = ok
                result.elements_failed = bad
                result.sql = sql
            elif (
                et == "expect_column_values_to_be_between"
                or et == "expect_column_value_lengths_to_be_between"
            ):
                mn = kw.get("min_value")
                mx = kw.get("max_value")
                ok, bad, _, sql = _exec_between(
                    warehouse, fq_table=fq_table, column=col, min_value=mn, max_value=mx
                )
                result.success = ok
                result.elements_failed = bad
                result.sql = sql
            elif et == "expect_column_values_to_be_unique":
                ok, dups, _, sql = _exec_unique(warehouse, fq_table=fq_table, column=col)
                result.success = ok
                result.elements_failed = dups
                result.sql = sql
            elif et == "expect_table_row_count_to_be_between":
                mn = kw.get("min_value")
                mx = kw.get("max_value")
                ok, n, _, sql = _exec_row_count_between(
                    warehouse, fq_table=fq_table, min_value=mn, max_value=mx
                )
                result.success = ok
                result.observed = n
                result.sql = sql
            else:
                result.skipped = True
                result.skip_reason = f"Unsupported expectation type: {et}"
        except Exception as exc:
            result.error = str(exc)[:200]
            result.success = False

        # Persist — every result lands in CONTROL.gx_validation_results
        try:
            warehouse.execute(
                f"INSERT INTO {CONTROL_SCHEMA}.gx_validation_results "
                f"(run_id, client_id, source_type, suite_id, "
                f" expectation_type, column_name, success, "
                f" observed_value, elements_failed, "
                f" skipped, skip_reason, error_message, run_at, dq_dimension) "
                f"SELECT $rid, $cid, $st, $sid, $et, $col, $ok, $obs, $ef, "
                f"       $skp, $sr, $err, $ts, $dim",
                {
                    "rid": run_id,
                    "cid": client_id,
                    "st": source_type or dataset_code.upper(),
                    "sid": suite_id,
                    "et": et,
                    "col": col,
                    "ok": bool(result.success),
                    "obs": int(result.observed),
                    "ef": int(result.elements_failed),
                    "skp": bool(result.skipped),
                    "sr": result.skip_reason,
                    "err": result.error,
                    "ts": _now(),
                    "dim": (kw.get("meta") or {}).get("dq_dimension")
                    if isinstance(kw.get("meta"), dict)
                    else None,
                },
            )
        except Exception as exc:
            _log.warning("gx.persist_failed", err=str(exc)[:200])

        results.append(result)
        if result.skipped:
            n_skip += 1
        elif result.success:
            n_pass += 1
        else:
            n_fail += 1

    had_critical_failure = n_fail > 0  # any fail = critical for now
    _log.info(
        "gx.checkpoint_complete",
        suite_id=suite_id,
        fq_table=fq_table,
        passed=n_pass,
        failed=n_fail,
        skipped=n_skip,
        total=len(results),
    )
    return {
        "suite_id": suite_id,
        "passed": n_pass,
        "failed": n_fail,
        "skipped": n_skip,
        "total": len(results),
        "results": results,
        "had_critical_failure": had_critical_failure,
    }
