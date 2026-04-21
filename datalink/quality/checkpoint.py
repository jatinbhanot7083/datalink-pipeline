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


def _run_with_data_docs(
    context: Any,
    checkpoint_name: str,
    batch_def: Any,
    suite: ExpectationSuite,
    df: pd.DataFrame,
) -> Any:
    """Run the validation via a GX Checkpoint + UpdateDataDocsAction.

    This is the GX-1.x idiom for "validate a batch AND write the result
    into the Data Docs HTML site". Using `batch.validate(suite)` alone
    returns the result to our code but never writes it to data_docs/.

    If the Checkpoint path errors out (e.g. ephemeral context, or a GX
    store that hasn't been initialised), we fall back to the raw
    batch.validate() so the in-DB audit still captures expectation-level
    results — the Data Docs site is secondary.
    """
    try:
        from great_expectations.checkpoint import UpdateDataDocsAction
        from great_expectations.core.validation_definition import ValidationDefinition

        val_def_name = f"val_def_{checkpoint_name}"
        try:
            val_def = context.validation_definitions.get(val_def_name)
        except Exception:
            val_def = context.validation_definitions.add(
                ValidationDefinition(
                    name=val_def_name,
                    data=batch_def,
                    suite=suite,
                )
            )

        cp_name = f"cp_{checkpoint_name}"
        try:
            cp = context.checkpoints.get(cp_name)
        except Exception:
            cp = context.checkpoints.add(
                gx.Checkpoint(
                    name=cp_name,
                    validation_definitions=[val_def],
                    actions=[UpdateDataDocsAction(name="update_data_docs")],
                )
            )

        result = cp.run(batch_parameters={"dataframe": df})
        # cp.run returns a CheckpointResult; the per-validation
        # ExpectationSuiteValidationResult is inside .run_results[key].
        run_results = list(result.run_results.values())
        if run_results:
            # Also rebuild the index/nav so the new run shows up in the listing.
            _render_data_docs(context)
            return run_results[0]
    except Exception:
        # Fall back to raw validate — in-DB audit still records expectations.
        pass
    return batch_def.get_batch(batch_parameters={"dataframe": df}).validate(suite)


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


def _load_suite_from_registry(
    warehouse: Warehouse, client_id: str, suite_name: str
) -> ExpectationSuite | None:
    """Phase 5.8: look up the LIVE suite for (client_id, suite_name) in
    CONTROL.dq_suites and materialise it as a GX ExpectationSuite.

    Falls back to `client_id='default'` when the requested client has no
    LIVE suite yet (e.g., a new Client B on their first run — they use the
    default baseline until their DQ analyst authors their own).

    Returns None if neither the requested client nor default has a LIVE
    suite — caller should fall back to the legacy `suite_builder` path.
    """
    # Lazy import to avoid circularity through __init__.py.
    from datalink.quality.registry import SuiteRegistry

    reg = SuiteRegistry(warehouse)
    live = reg.get_live(client_id, suite_name)
    if live is None and client_id != "default":
        _log.info(
            "checkpoint.suite_fallback_to_default",
            requested_client=client_id,
            suite_name=suite_name,
        )
        live = reg.get_live("default", suite_name)
    if live is None:
        return None
    _log.info(
        "checkpoint.suite_loaded_from_registry",
        suite_id=live.suite_id,
        client_id=live.client_id,
        suite_name=suite_name,
        version=live.version,
        expectation_count=len(live.expectations),
    )
    return _json_to_suite(live.suite_id, suite_name, live.expectations)


def _json_to_suite(
    suite_id: str, suite_name: str, expectations: list[dict[str, Any]]
) -> ExpectationSuite:
    """Convert a JSON expectation list to a live GX ExpectationSuite.

    Class lookup: `expect_column_values_to_not_be_null` →
    `ExpectColumnValuesToNotBeNull` attribute on `great_expectations.expectations`.
    Unknown types are logged + skipped (never crashes the checkpoint).
    """
    # Import here so mypy + the top-level import block stays tidy.
    from great_expectations import expectations as gx_expectations

    suite = ExpectationSuite(name=f"{suite_name}_v{suite_id[:8]}")
    for exp in expectations:
        exp_type = exp.get("expectation_type")
        if not exp_type:
            continue
        class_name = "".join(word.capitalize() for word in exp_type.split("_"))
        exp_class = getattr(gx_expectations, class_name, None)
        if exp_class is None:
            _log.warning(
                "checkpoint.unknown_expectation_type",
                expectation_type=exp_type,
                class_name=class_name,
            )
            continue
        kwargs = exp.get("kwargs", {})
        try:
            suite.add_expectation(exp_class(**kwargs))
        except Exception as e:
            _log.warning(
                "checkpoint.expectation_instantiation_failed",
                expectation_type=exp_type,
                error=str(e),
            )
    return suite


def run_checkpoint(
    warehouse: Warehouse,
    qualified_table: str,
    checkpoint_name: str,
    pipeline_id: str,
    run_id: str,
    client_id: str = "default",
    suite_builder: Callable[[], ExpectationSuite] | None = None,
    fail_threshold_pct: float = 5.0,
    record_results: bool = True,
    source_type: str | None = None,
) -> CheckpointResult:
    """Load the table into a pandas DataFrame, evaluate the suite, record results.

    Suite resolution (Phase 5.8 change):
      1. PRIMARY — look up LIVE suite in CONTROL.dq_suites for
         (client_id, checkpoint_name). Falls back to client_id='default'
         if the client has no suite of their own yet.
      2. LEGACY — if registry returns None and `suite_builder` was
         provided, call it. This keeps old tests + Phase-5 callers working
         during migration.
      3. ERROR — if both paths fail, raise ValueError with a clear message.

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

    # Phase 5.8 primary path: load from registry.
    suite = _load_suite_from_registry(warehouse, client_id, checkpoint_name)
    if suite is None:
        if suite_builder is None:
            raise ValueError(
                f"No LIVE suite in CONTROL.dq_suites for ({client_id!r}, "
                f"{checkpoint_name!r}) and no suite_builder fallback supplied. "
                "Run seed_baselines(warehouse) on first boot."
            )
        _log.info(
            "checkpoint.fallback_to_suite_builder",
            client_id=client_id,
            suite_name=checkpoint_name,
        )
        suite = suite_builder()

    rows = warehouse.query(f"SELECT * FROM {qualified_table}")
    df = pd.DataFrame(rows)

    # file-backed GX persists datasource definitions to disk, so on the
    # 2nd+ run add_pandas() raises "already exists". Use get-or-add pattern:
    # try to retrieve the existing datasource/asset/batch-def, fall back to
    # creating only when absent.
    ds_name = f"pandas_{checkpoint_name}"
    asset_name = f"asset_{checkpoint_name}"
    batch_def_name = "batch_def"
    try:
        ds = context.data_sources.get(ds_name)
    except Exception:
        ds = context.data_sources.add_pandas(ds_name)
    try:
        asset = ds.get_asset(asset_name)
    except Exception:
        asset = ds.add_dataframe_asset(asset_name)
    try:
        batch_def = asset.get_batch_definition(batch_def_name)
    except Exception:
        batch_def = asset.add_batch_definition_whole_dataframe(batch_def_name)

    # Persist the suite so the ValidationDefinition can reference it by name.
    try:
        context.suites.add(suite)
    except Exception:
        context.suites.add_or_update(suite)

    # Use a Checkpoint + UpdateDataDocsAction so per-run validation results
    # get WRITTEN into the Data Docs site (not just returned to our code).
    # Without this, nginx-gx-docs serves only the shell + static assets.
    validation = _run_with_data_docs(context, checkpoint_name, batch_def, suite, df)

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
        _record_results(
            warehouse,
            checkpoint_name,
            run_id,
            results,
            client_id=client_id,
            source_type=source_type,
        )

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
    client_id: str | None = None,
    source_type: str | None = None,
) -> None:
    """Persist per-expectation detail to CONTROL.gx_validation_results.

    Phase 6: each row is tagged with client_id + source_type + dq_dimension
    so the DQ Dashboard can slice without re-parsing expectation meta at
    query time. Dimension is computed via datalink.quality.dimensions.
    """
    from datalink.quality.dimensions import dimension_for

    for r in results:
        warehouse.execute(
            f"INSERT INTO {CONTROL_SCHEMA}.gx_validation_results "
            "(validation_id, run_id, checkpoint_name, client_id, source_type, "
            " dq_dimension, expectation, column_name, "
            " success, unexpected_count, unexpected_pct, details, ts) "
            "VALUES ($v, $r, $c, $ci, $src, $dim, $e, $col, $s, $uc, $up, $d, $t)",
            {
                "v": str(uuid.uuid4()),
                "r": run_id,
                "c": checkpoint_name,
                "ci": client_id,
                "src": source_type,
                "dim": dimension_for(r.expectation_type),
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
