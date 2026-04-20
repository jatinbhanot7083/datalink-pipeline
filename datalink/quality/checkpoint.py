"""Checkpoint runner — evaluates a GX suite against a Warehouse table.

Wraps Great Expectations 1.x programmatic API. The suite is built by a
`GxSuite` helper (see suites/*). Results are recorded in
CONTROL.gx_validation_results.

Auto-pause logic: if `unexpected_pct` across critical expectations
exceeds `fail_threshold_pct` (from config), caller is expected to
transition the pipeline to PAUSED via PipelineControl.
"""

from __future__ import annotations

import contextlib
import os
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any

import great_expectations as gx
import pandas as pd
from great_expectations.core.expectation_suite import ExpectationSuite

from datalink.adapters.protocols import Warehouse
from datalink.logging import get_logger
from datalink.quality.control import CONTROL_SCHEMA

_log = get_logger(__name__)


class CheckpointStatus(StrEnum):
    PASSED = "PASSED"
    FAILED = "FAILED"  # some expectations failed but < threshold
    BREACHED = "BREACHED"  # fail % >= auto-pause threshold
    SKIPPED = "SKIPPED"  # GX disabled via feature flag


@dataclass(frozen=True)
class ExpectationResult:
    expectation_type: str
    column: str | None
    success: bool
    unexpected_count: int
    element_count: int
    unexpected_pct: float
    exception_info: str | None = None


@dataclass(frozen=True)
class CheckpointResult:
    checkpoint_name: str
    run_id: str
    pipeline_id: str
    row_count: int
    total_expectations: int
    failed_expectations: int
    fail_pct: float  # fraction of expectations that failed, 0..100
    status: CheckpointStatus
    expectation_results: list[ExpectationResult]


def _get_gx_context() -> Any:
    """Return a file-backed GX context if `gx/` exists; else ephemeral.

    File-backed mode writes Data Docs to `gx/uncommitted/data_docs/local_site/`
    which the nginx-gx-docs container serves at http://localhost:8090.
    Environment override: DL_GX_CONTEXT_MODE=ephemeral forces in-memory.
    """
    forced = os.environ.get("DL_GX_CONTEXT_MODE", "").strip().lower()
    if forced == "ephemeral":
        return gx.get_context(mode="ephemeral")

    # Walk up from this file to find a `gx/` directory at the project root.
    project_root = Path(__file__).resolve().parents[2]
    gx_dir = project_root / "gx"
    if gx_dir.is_dir():
        # file mode persists configuration + Data Docs to disk.
        return gx.get_context(mode="file", project_root_dir=str(project_root))
    return gx.get_context(mode="ephemeral")


def _render_data_docs(context: Any) -> None:
    """Best-effort: rebuild the HTML Data Docs after a checkpoint run.

    File-backed contexts support build_data_docs(); ephemeral ones don't.
    We swallow exceptions because Data Docs are a presentation feature —
    their absence must never fail the checkpoint itself.
    """
    builder = getattr(context, "build_data_docs", None)
    if callable(builder):
        # Data Docs are a presentation feature — their absence must never
        # fail the checkpoint itself, so swallow any GX-internal error.
        with contextlib.suppress(Exception):
            builder()


def _extract_expectation_results(
    validation_result: Any,
) -> tuple[list[ExpectationResult], int]:
    """Extract per-expectation outcomes + element count from a GX result."""
    results: list[ExpectationResult] = []
    total_rows = 0
    for r in validation_result.results:
        ec = r.get("result", {}).get("element_count", 0) or 0
        uc = r.get("result", {}).get("unexpected_count", 0) or 0
        up = r.get("result", {}).get("unexpected_percent", 0.0) or 0.0
        total_rows = max(total_rows, ec)
        results.append(
            ExpectationResult(
                expectation_type=r["expectation_config"]["type"],
                column=r["expectation_config"].get("kwargs", {}).get("column"),
                success=r["success"],
                unexpected_count=uc,
                element_count=ec,
                unexpected_pct=float(up),
                exception_info=r.get("exception_info", {}).get("exception_message"),
            )
        )
    return results, total_rows


def run_checkpoint(
    warehouse: Warehouse,
    qualified_table: str,
    suite_builder: Callable[[], ExpectationSuite],
    checkpoint_name: str,
    pipeline_id: str,
    run_id: str,
    fail_threshold_pct: float = 5.0,
    record_results: bool = True,
) -> CheckpointResult:
    """Load the table into a pandas DataFrame, evaluate the suite, record results.

    `suite_builder` is a factory callable (the module-level build_*_suite
    functions). We establish the GX context FIRST, then call the builder —
    GX 1.x needs a live context before ExpectationSuite() is instantiated
    (the suite's __init__ reaches into the singleton project manager).

    For production Snowflake volumes this would stream; for the local DuckDB
    demo a full materialization is fine (≤10k rows).

    Context mode: file-backed when a `gx/` directory exists at the project
    root (Phase 5.7 — Data Docs render to gx/uncommitted/data_docs/local_site
    so the nginx-gx-docs container can serve them). Falls back to ephemeral
    (in-memory only, no Data Docs) when `gx/` is absent — useful for unit
    tests and CI where we don't want to pollute the filesystem.
    """
    # Context MUST come first — sets the singleton project manager used by
    # ExpectationSuite.__init__.
    context = _get_gx_context()
    suite = suite_builder()

    rows = warehouse.query(f"SELECT * FROM {qualified_table}")
    df = pd.DataFrame(rows)

    batch_def = (
        context.data_sources.add_pandas(f"pandas_{checkpoint_name}")
        .add_dataframe_asset(f"asset_{checkpoint_name}")
        .add_batch_definition_whole_dataframe("batch_def")
    )
    batch = batch_def.get_batch(batch_parameters={"dataframe": df})
    validation = batch.validate(suite)

    # Render HTML Data Docs so the nginx-gx-docs site (http://localhost:8090)
    # shows the report for this checkpoint run. No-op on ephemeral contexts.
    _render_data_docs(context)

    results, row_count = _extract_expectation_results(validation)
    total = len(results)
    failed = sum(1 for r in results if not r.success)
    fail_pct = (failed / total) * 100 if total else 0.0

    if total == 0:
        status = CheckpointStatus.SKIPPED
    elif failed == 0:
        status = CheckpointStatus.PASSED
    elif fail_pct >= fail_threshold_pct:
        status = CheckpointStatus.BREACHED
    else:
        status = CheckpointStatus.FAILED

    if record_results:
        _record_results(warehouse, checkpoint_name, run_id, results)

    cr = CheckpointResult(
        checkpoint_name=checkpoint_name,
        run_id=run_id,
        pipeline_id=pipeline_id,
        row_count=row_count,
        total_expectations=total,
        failed_expectations=failed,
        fail_pct=round(fail_pct, 2),
        status=status,
        expectation_results=results,
    )

    _log.info(
        "gx.checkpoint.done",
        name=checkpoint_name,
        table=qualified_table,
        total=total,
        failed=failed,
        fail_pct=round(fail_pct, 2),
        status=status.value,
    )
    return cr


def _record_results(
    warehouse: Warehouse,
    checkpoint_name: str,
    run_id: str,
    results: list[ExpectationResult],
) -> None:
    """Persist per-expectation detail to CONTROL.gx_validation_results."""
    for r in results:
        warehouse.execute(
            f"INSERT INTO {CONTROL_SCHEMA}.gx_validation_results "
            "(validation_id, run_id, checkpoint_name, expectation, column_name, "
            " success, unexpected_count, unexpected_pct, details, ts) "
            "VALUES ($v, $r, $c, $e, $col, $s, $uc, $up, $d, $t)",
            {
                "v": str(uuid.uuid4()),
                "r": run_id,
                "c": checkpoint_name,
                "e": r.expectation_type,
                "col": r.column,
                "s": r.success,
                "uc": r.unexpected_count,
                "up": round(r.unexpected_pct, 2),
                "d": (r.exception_info or "")[:500],
                "t": datetime.now(UTC),
            },
        )


def skipped_result(
    checkpoint_name: str,
    pipeline_id: str,
    run_id: str,
) -> CheckpointResult:
    """Produce a SKIPPED marker result — used when features.gx.enabled=false."""
    return CheckpointResult(
        checkpoint_name=checkpoint_name,
        run_id=run_id,
        pipeline_id=pipeline_id,
        row_count=0,
        total_expectations=0,
        failed_expectations=0,
        fail_pct=0.0,
        status=CheckpointStatus.SKIPPED,
        expectation_results=[],
    )
