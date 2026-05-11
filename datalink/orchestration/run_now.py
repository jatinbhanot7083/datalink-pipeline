"""Phase 18 — synchronous in-process pipeline runner.

This is what fires when the operator clicks "▶ Run now" on a pipeline
instance in Pipeline Architect Section 2.  It executes the same five
dataset-parametric task functions an Airflow DAG would (`bronze_land →
bronze_validate → silver_dbt → gold_dbt → onprem_push`), but in the
Streamlit process — no Airflow / Kubernetes / Celery required.

Use case: dev + demo loop.  Production deploy still goes through Airflow
(Phase 18.x — separate work).  But for "I just authored membership in
DMD, I want to see physical Bronze/Silver/Gold tables in Snowflake right
now," this is the path.

Bootstrap step
--------------
The existing task functions assume Bronze schema + table already exist
(`bronze_land_task` does TRUNCATE before COPY INTO).  ``run_now()``
prepends a bootstrap step that:
  * CREATE SCHEMA IF NOT EXISTS BRONZE_<CLIENT>
  * CREATE SCHEMA IF NOT EXISTS SILVER_<CLIENT>
  * CREATE SCHEMA IF NOT EXISTS GOLD_<CLIENT>
  * CREATE TABLE IF NOT EXISTS BRONZE_<CLIENT>.raw_<dataset> with columns
    derived from the demo PSV header + audit columns

This makes the runner safe on a fresh Snowflake account.

Audit
-----
Every run writes one row to ``CONTROL.dataset_pipeline_runs`` with
status=RUNNING up front, then UPDATEs to SUCCESS/FAILED with task log
JSON when complete.  Pipeline Architect's run-history drill-down reads
this table.
"""

from __future__ import annotations

import json
import time
import traceback
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from datalink.observability.metrics import (
    record_dq_check_batch,
    record_layer_rows,
    record_pipeline_run,
    record_task_duration,
)
from datalink.orchestration.tasks import (
    bronze_land_task,
    bronze_validate_task,
    gold_dbt_task,
    gold_dq_task,
    onprem_push_task,
    silver_dbt_task,
    silver_dq_task,
)
from datalink.quality.control import CONTROL_SCHEMA

REPO_ROOT = Path(__file__).resolve().parents[2]


# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------
@dataclass
class TaskOutcome:
    name: str
    status: str  # SUCCESS | FAILED | SKIPPED
    duration_ms: int = 0
    rows: int | None = None
    output: dict[str, Any] = field(default_factory=dict)
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "status": self.status,
            "duration_ms": self.duration_ms,
            "rows": self.rows,
            "output": self.output,
            "error": self.error,
        }


@dataclass
class RunOutcome:
    run_id: str
    instance_id: str
    client_id: str
    dataset_code: str
    status: str  # SUCCESS | FAILED
    started_at: str
    ended_at: str
    duration_ms: int
    rows_bronze: int | None = None
    rows_silver: int | None = None
    rows_gold: int | None = None
    error_task: str | None = None
    error_message: str | None = None
    tasks: list[TaskOutcome] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Bootstrap — create schemas + Bronze table from PSV header
# ---------------------------------------------------------------------------
def _find_psv(dataset_code: str) -> Path | None:
    """Locate the demo Bronze sample file for a dataset_code."""
    candidates = [
        REPO_ROOT / "data" / "generated" / f"{dataset_code.lower()}_bronze_sample.psv",
        Path("/opt/datalink/data/generated") / f"{dataset_code.lower()}_bronze_sample.psv",
    ]
    return next((p for p in candidates if p.exists()), None)


def _read_psv_header(path: Path) -> list[str]:
    with open(path) as f:
        first = f.readline().rstrip("\n").rstrip("\r")
    return [c.strip() for c in first.split("|") if c.strip()]


def bootstrap_for_instance(
    *, client_id: str, dataset_code: str, bronze_anchor: str = "FLAT_FILE"
) -> dict[str, Any]:
    """Idempotent: create BRONZE/SILVER/GOLD schemas + Bronze table for this
    (client, dataset) tuple.  Safe to call before every run."""
    from datalink.ui._query import warehouse_ctx

    cli_u = client_id.upper()
    ds_l = dataset_code.lower()
    bronze_schema = f"BRONZE_{cli_u}"
    silver_schema = f"SILVER_{cli_u}"
    gold_schema = f"GOLD_{cli_u}"
    bronze_table = f"raw_{ds_l}"

    psv = _find_psv(ds_l) if bronze_anchor == "FLAT_FILE" else None
    cols: list[str] = []
    if psv:
        cols = _read_psv_header(psv)

    # Existing tasks.py conventions:
    #   silver_dbt_task: DROP + CREATE TABLE AS  → Silver table can be missing
    #   gold_dbt_task:   TRUNCATE + INSERT       → Gold table MUST pre-exist
    # So bootstrap needs to create Bronze + Gold; Silver is fine to skip.
    silver_table = f"{dataset_code.lower()}_clean"  # tasks.py convention
    gold_table = ds_l

    with warehouse_ctx(readonly=False) as wh:
        # Schemas
        for s in (bronze_schema, silver_schema, gold_schema):
            wh.execute(f"CREATE SCHEMA IF NOT EXISTS {s}")
        if cols:
            biz_col_defs = ",\n              ".join(f"{c.lower()} VARCHAR" for c in cols)
            audit_cols_bronze = (
                "_load_dt TIMESTAMP_NTZ,\n"
                "              _source_file VARCHAR,\n"
                "              _batch_id VARCHAR,\n"
                "              _record_source VARCHAR,\n"
                "              _extra VARIANT"
            )
            # Bronze table — VARCHAR + Bronze audit cols.
            wh.execute(
                f"CREATE TABLE IF NOT EXISTS {bronze_schema}.{bronze_table} (\n"
                f"              {biz_col_defs},\n"
                f"              {audit_cols_bronze}\n"
                f"            )"
            )
            # Gold table — pre-created so gold_dbt_task's TRUNCATE+INSERT works.
            # Same biz cols (lowercase, VARCHAR for demo simplicity).  Real
            # production would derive types from CONTROL.global_gold_schema_fields.
            wh.execute(
                f"CREATE TABLE IF NOT EXISTS {gold_schema}.{gold_table} (\n"
                f"              {biz_col_defs}\n"
                f"            )"
            )
    return {
        "bronze_schema": bronze_schema,
        "silver_schema": silver_schema,
        "gold_schema": gold_schema,
        "bronze_table": bronze_table,
        "silver_table": silver_table,
        "gold_table": gold_table,
        "psv_path": str(psv) if psv else None,
        "columns_inferred": len(cols),
    }


# ---------------------------------------------------------------------------
# Run-history persistence
# ---------------------------------------------------------------------------
def _insert_run_started(
    *,
    run_id: str,
    instance_id: str,
    client_id: str,
    dataset_code: str,
    triggered_by: str,
) -> None:
    from datalink.ui._query import warehouse_ctx

    with warehouse_ctx(readonly=False) as wh:
        wh.execute(
            f"INSERT INTO {CONTROL_SCHEMA}.dataset_pipeline_runs "
            f"(run_id, instance_id, client_id, dataset_code, status, "
            f" triggered_by, started_at) "
            f"VALUES ($rid, $iid, $cid, $ds, 'RUNNING', $by, "
            f"        CURRENT_TIMESTAMP())",
            {
                "rid": run_id,
                "iid": instance_id,
                "cid": client_id,
                "ds": dataset_code,
                "by": triggered_by,
            },
        )


def _update_run_finished(outcome: RunOutcome) -> None:
    from datalink.ui._query import warehouse_ctx

    with warehouse_ctx(readonly=False) as wh:
        wh.execute(
            f"UPDATE {CONTROL_SCHEMA}.dataset_pipeline_runs "
            f"SET status = $st, ended_at = CURRENT_TIMESTAMP(), "
            f"    duration_ms = $dur, "
            f"    rows_bronze = $rb, rows_silver = $rs, rows_gold = $rg, "
            f"    error_task = $etask, error_message = $emsg, "
            f"    task_log_json = $tlog "
            f"WHERE run_id = $rid",
            {
                "st": outcome.status,
                "dur": outcome.duration_ms,
                "rb": outcome.rows_bronze,
                "rs": outcome.rows_silver,
                "rg": outcome.rows_gold,
                "etask": outcome.error_task,
                "emsg": outcome.error_message,
                "tlog": json.dumps([t.to_dict() for t in outcome.tasks]),
                "rid": outcome.run_id,
            },
        )


# ---------------------------------------------------------------------------
# Run one task with timing + error capture
# ---------------------------------------------------------------------------
@contextmanager
def _timed(name: str) -> Iterator[TaskOutcome]:
    out = TaskOutcome(name=name, status="RUNNING")
    t0 = time.perf_counter()
    try:
        yield out
        out.status = "SUCCESS"
    except Exception as exc:
        out.status = "FAILED"
        out.error = f"{type(exc).__name__}: {exc}\n\n" + traceback.format_exc()
        raise
    finally:
        out.duration_ms = int((time.perf_counter() - t0) * 1000)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------
def run_now(
    *,
    instance: dict[str, Any],
    triggered_by: str = "ui:run_now",
    skip_push: bool = True,
) -> RunOutcome:
    """Execute the full Bronze→Silver→Gold (→ Push) chain for one instance.

    Returns a ``RunOutcome`` whether the run succeeded or failed.  Persists
    a row to ``CONTROL.dataset_pipeline_runs`` in both cases.

    ``skip_push=True`` (default) is the dev-loop choice — skip the
    onprem_push step which requires real downstream targets configured.
    Set ``skip_push=False`` once routing targets are wired in
    ``client_pipeline_instances.overrides_json``.
    """
    run_id = str(uuid.uuid4())
    instance_id = str(instance.get("instance_id"))
    client_id = str(instance.get("client_id"))
    dataset_code = str(instance.get("dataset_code"))
    bronze_anchor = str(instance.get("bronze_anchor") or "FLAT_FILE")
    started_at_iso = datetime.now(UTC).isoformat()
    t_start = time.perf_counter()

    outcome = RunOutcome(
        run_id=run_id,
        instance_id=instance_id,
        client_id=client_id,
        dataset_code=dataset_code,
        status="RUNNING",
        started_at=started_at_iso,
        ended_at="",
        duration_ms=0,
    )

    _insert_run_started(
        run_id=run_id,
        instance_id=instance_id,
        client_id=client_id,
        dataset_code=dataset_code,
        triggered_by=triggered_by,
    )

    # ── 0. Bootstrap (idempotent) ─────────────────────────────────────
    try:
        with _timed("bootstrap") as t0:
            t0.output = bootstrap_for_instance(
                client_id=client_id,
                dataset_code=dataset_code,
                bronze_anchor=bronze_anchor,
            )
        outcome.tasks.append(t0)
    except Exception:
        outcome.tasks.append(t0)
        outcome.status = "FAILED"
        outcome.error_task = "bootstrap"
        outcome.error_message = t0.error
        outcome.ended_at = datetime.now(UTC).isoformat()
        outcome.duration_ms = int((time.perf_counter() - t_start) * 1000)
        _update_run_finished(outcome)
        return outcome

    # ── 1. bronze_land ────────────────────────────────────────────────
    try:
        with _timed("bronze_land") as t1:
            t1.output = bronze_land_task(
                client_id=client_id,
                dataset_code=dataset_code,
                bronze_anchor=bronze_anchor,
            )
            t1.rows = int(t1.output.get("rows_loaded") or t1.output.get("rows") or 0) or None
            outcome.rows_bronze = t1.rows
        outcome.tasks.append(t1)
    except Exception:
        outcome.tasks.append(t1)
        outcome.status = "FAILED"
        outcome.error_task = "bronze_land"
        outcome.error_message = t1.error
        outcome.ended_at = datetime.now(UTC).isoformat()
        outcome.duration_ms = int((time.perf_counter() - t_start) * 1000)
        _update_run_finished(outcome)
        return outcome

    # ── 2. bronze_validate ────────────────────────────────────────────
    try:
        with _timed("bronze_validate") as t2:
            t2.output = bronze_validate_task(client_id=client_id, dataset_code=dataset_code)
        outcome.tasks.append(t2)
    except Exception:
        outcome.tasks.append(t2)
        # Validation failures shouldn't kill the rest of the pipeline in
        # demo mode — we record the error but continue.
        outcome.error_task = outcome.error_task or "bronze_validate"
        outcome.error_message = outcome.error_message or t2.error

    # ── 3. silver_dbt ─────────────────────────────────────────────────
    try:
        with _timed("silver_dbt") as t3:
            t3.output = silver_dbt_task(
                client_id=client_id, dataset_code=dataset_code, use_dbt=False
            )
            t3.rows = int(t3.output.get("rows") or 0) or None
            outcome.rows_silver = t3.rows
        outcome.tasks.append(t3)
    except Exception:
        outcome.tasks.append(t3)
        outcome.status = "FAILED"
        outcome.error_task = "silver_dbt"
        outcome.error_message = t3.error
        outcome.ended_at = datetime.now(UTC).isoformat()
        outcome.duration_ms = int((time.perf_counter() - t_start) * 1000)
        _update_run_finished(outcome)
        return outcome

    # ── 3.5 silver_dq — Phase 19.6 factory-pattern Silver DQ check ────
    # No-op when no suite exists; non-fatal if checkpoint errors out
    # (DQ failure shouldn't stop demo materialization mid-flight).
    try:
        with _timed("silver_dq") as t3b:
            t3b.output = silver_dq_task(client_id=client_id, dataset_code=dataset_code)
        outcome.tasks.append(t3b)
    except Exception:
        outcome.tasks.append(t3b)
        outcome.error_task = outcome.error_task or "silver_dq"
        outcome.error_message = outcome.error_message or t3b.error

    # ── 4. gold_dbt ───────────────────────────────────────────────────
    try:
        with _timed("gold_dbt") as t4:
            t4.output = gold_dbt_task(client_id=client_id, dataset_code=dataset_code, use_dbt=False)
            t4.rows = int(t4.output.get("rows") or 0) or None
            outcome.rows_gold = t4.rows
        outcome.tasks.append(t4)
    except Exception:
        outcome.tasks.append(t4)
        outcome.status = "FAILED"
        outcome.error_task = "gold_dbt"
        outcome.error_message = t4.error
        outcome.ended_at = datetime.now(UTC).isoformat()
        outcome.duration_ms = int((time.perf_counter() - t_start) * 1000)
        _update_run_finished(outcome)
        return outcome

    # ── 4.5 gold_dq — Phase 19.6 factory-pattern Gold DQ check ────────
    try:
        with _timed("gold_dq") as t4b:
            t4b.output = gold_dq_task(client_id=client_id, dataset_code=dataset_code)
        outcome.tasks.append(t4b)
    except Exception:
        outcome.tasks.append(t4b)
        outcome.error_task = outcome.error_task or "gold_dq"
        outcome.error_message = outcome.error_message or t4b.error

    # ── 5. onprem_push (optional) ─────────────────────────────────────
    if not skip_push:
        try:
            with _timed("onprem_push") as t5:
                t5.output = onprem_push_task(
                    client_id=client_id, dataset_code=dataset_code, targets=[]
                )
            outcome.tasks.append(t5)
        except Exception:
            outcome.tasks.append(t5)
            outcome.error_task = outcome.error_task or "onprem_push"
            outcome.error_message = outcome.error_message or t5.error
            # don't fail the whole pipeline on push errors

    # ── Done ──────────────────────────────────────────────────────────
    outcome.status = (
        "SUCCESS"
        if outcome.error_task is None
        else ("SUCCESS" if outcome.rows_gold is not None else "FAILED")
    )
    outcome.ended_at = datetime.now(UTC).isoformat()
    outcome.duration_ms = int((time.perf_counter() - t_start) * 1000)
    _update_run_finished(outcome)

    # Phase 20.1 — push run-level + per-layer + per-task metrics to OTEL.
    # Silent no-op if collector unreachable; never fails the pipeline.
    try:
        duration_s = outcome.duration_ms / 1000.0
        record_pipeline_run(
            dataset=dataset_code,
            client=client_id,
            status=outcome.status,
            duration_s=duration_s,
        )
        record_layer_rows(
            layer="BRONZE",
            dataset=dataset_code,
            client=client_id,
            rows=outcome.rows_bronze or 0,
        )
        record_layer_rows(
            layer="SILVER",
            dataset=dataset_code,
            client=client_id,
            rows=outcome.rows_silver or 0,
        )
        record_layer_rows(
            layer="GOLD",
            dataset=dataset_code,
            client=client_id,
            rows=outcome.rows_gold or 0,
        )
        for t in outcome.tasks:
            record_task_duration(
                task=t.name,
                dataset=dataset_code,
                client=client_id,
                status=t.status,
                duration_s=(t.duration_ms or 0) / 1000.0,
            )
            # DQ tasks may carry summary counts in their output
            if t.name in ("bronze_validate", "silver_dq", "gold_dq"):
                gx = (t.output or {}).get("gx_summary") or {}
                record_dq_check_batch(
                    dataset=dataset_code,
                    layer=t.name.replace("_validate", "").replace("_dq", "").upper() or "BRONZE",
                    passed=int(gx.get("passed") or 0),
                    failed=int(gx.get("failed") or 0),
                    skipped=int(gx.get("skipped") or 0),
                )
    except Exception:
        pass  # never fail run-level on metric emission

    return outcome


# ---------------------------------------------------------------------------
# Run-history accessors (used by Pipeline Architect's drill-down)
# ---------------------------------------------------------------------------
def list_runs(*, instance_id: str, limit: int = 20) -> list[dict[str, Any]]:
    from datalink.ui._query import warehouse_ctx

    with warehouse_ctx(readonly=True) as wh:
        return list(
            wh.query(
                f"SELECT run_id, instance_id, client_id, dataset_code, status, "
                f"       triggered_by, started_at, ended_at, duration_ms, "
                f"       rows_bronze, rows_silver, rows_gold, "
                f"       error_task, error_message "
                f"  FROM {CONTROL_SCHEMA}.dataset_pipeline_runs "
                f" WHERE instance_id = $iid "
                f" ORDER BY started_at DESC "
                f" LIMIT {int(limit)}",
                {"iid": instance_id},
            )
        )


def get_run_detail(run_id: str) -> dict[str, Any] | None:
    from datalink.ui._query import warehouse_ctx

    with warehouse_ctx(readonly=True) as wh:
        rows = list(
            wh.query(
                f"SELECT * FROM {CONTROL_SCHEMA}.dataset_pipeline_runs " f"WHERE run_id = $rid",
                {"rid": run_id},
            )
        )
    return rows[0] if rows else None
