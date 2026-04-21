"""Pipeline control — state machine + audit log.

Matches the schema in GX_Pipeline_Control_Agentic_AI_Architecture.docx §3.4:
  - pipeline_control_state        current state per pipeline
  - pipeline_checkpoints          checkpoint markers (bronze/silver/gold)
  - pipeline_task_progress        per-task completion markers (Phase 6)
  - pipeline_control_audit_log    immutable transition history
  - batch_quarantine              aborted batches for root-cause analysis
  - agent_reasoning_log           every agent invocation (Phase 5 addition)

State transitions: RUNNING -> PAUSED -> RESUMING -> RUNNING, or -> ABORTED.
Auto-pause fires when a GX checkpoint exceeds its failure threshold (default 5%).
Auto-abort on schema mismatch, file-format change, or orphaned Links.

Phase 6 auto-resume: local_sequential + Airflow consult
pipeline_task_progress on re-run. Tasks whose (run_id, task_name) rows
already exist with status='SUCCESS' are SKIPPED_RESUMED -- the pipeline
resumes from the first un-recorded task rather than restarting from scratch.

Phase 6 fail-safe: FatalPipelineError transitions state to ABORTED
(terminal) and exits. Non-fatal exceptions HALT; the run can be
resumed once the operator fixes the root cause.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, ClassVar

from datalink.adapters.protocols import Warehouse
from datalink.logging import get_logger


class FatalPipelineError(Exception):
    """Raise to signal a terminal pipeline failure.

    Local_sequential + Airflow callbacks catch this, transition the pipeline
    state to ABORTED, and stop all downstream work. The run CANNOT be
    auto-resumed -- an operator must investigate, quarantine the batch, and
    start a fresh run_id.

    Good fits: schema drift, file-format change, PHI leak detected in
    agent output, warehouse schema corruption. Bad fits (use plain
    exceptions for these): transient DB timeouts, disk-full errors,
    one-off row-validation failures.
    """

    def __init__(self, reason: str, *, severity: str = "CRITICAL") -> None:
        super().__init__(reason)
        self.reason = reason
        self.severity = severity


_log = get_logger(__name__)

CONTROL_SCHEMA = "CONTROL"


class PipelineState(StrEnum):
    RUNNING = "RUNNING"
    PAUSED = "PAUSED"
    RESUMING = "RESUMING"
    ABORTED = "ABORTED"


class Severity(StrEnum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


@dataclass(frozen=True)
class StateTransition:
    pipeline_id: str
    from_state: PipelineState
    to_state: PipelineState
    actor: str  # 'airflow-gx-callback' | 'user:<name>' | 'agent-rca' | 'system'
    reason: str
    severity: Severity = Severity.MEDIUM


_DDL = [
    f"CREATE SCHEMA IF NOT EXISTS {CONTROL_SCHEMA}",
    # Current state (one row per pipeline)
    f"""
    CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.pipeline_control_state (
        pipeline_id     VARCHAR PRIMARY KEY,
        status          VARCHAR NOT NULL,
        checkpoint_id   VARCHAR,
        paused_at       TIMESTAMP,
        paused_reason   VARCHAR,
        paused_by       VARCHAR,
        severity        VARCHAR,
        updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    # Checkpoint markers (one row per pipeline run x checkpoint).
    # Phase 6: client_id + source_type added so dashboards can slice by tenant
    # and by CLAIMS/MEMBERSHIP/PROVIDER. Both nullable for Phase-5.x back-compat;
    # forward-migration ALTER TABLE below fills them for existing DBs.
    f"""
    CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.pipeline_checkpoints (
        run_id          VARCHAR,
        pipeline_id     VARCHAR,
        checkpoint_name VARCHAR,
        client_id       VARCHAR,
        source_type     VARCHAR,
        completed_at    TIMESTAMP,
        row_count       INTEGER,
        fail_pct        DECIMAL(5,2),
        status          VARCHAR,
        PRIMARY KEY (run_id, checkpoint_name)
    )
    """,
    # Phase 6 auto-resume: per-task completion markers. One row per
    # (run_id, task_name). A re-run with the SAME run_id consults this
    # table to skip tasks already SUCCESSFUL and resume from the first
    # un-recorded task. `output_json` stores the task's return dict so
    # downstream tasks that were skipped on resume still have upstream
    # context available.
    f"""
    CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.pipeline_task_progress (
        run_id          VARCHAR NOT NULL,
        pipeline_id     VARCHAR NOT NULL,
        task_name       VARCHAR NOT NULL,
        status          VARCHAR NOT NULL,
        started_at      TIMESTAMP,
        completed_at    TIMESTAMP,
        duration_ms     INTEGER,
        output_json     VARCHAR,
        error           VARCHAR,
        PRIMARY KEY (run_id, task_name)
    )
    """,
    # Immutable transition history
    f"""
    CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.pipeline_control_audit_log (
        transition_id   VARCHAR PRIMARY KEY,
        pipeline_id     VARCHAR,
        from_state      VARCHAR,
        to_state        VARCHAR,
        ts              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        actor           VARCHAR,
        reason          VARCHAR,
        severity        VARCHAR
    )
    """,
    # Per-expectation detail from GX checkpoints (Phase 5).
    # Phase 6: client_id + source_type + dq_dimension added so the dedicated
    # DQ Dashboard can filter by tenant and show per-dimension pass rates.
    f"""
    CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.gx_validation_results (
        validation_id   VARCHAR PRIMARY KEY,
        run_id          VARCHAR,
        checkpoint_name VARCHAR,
        client_id       VARCHAR,
        source_type     VARCHAR,
        dq_dimension    VARCHAR,
        expectation     VARCHAR,
        column_name     VARCHAR,
        success         BOOLEAN,
        unexpected_count INTEGER,
        unexpected_pct  DECIMAL(5,2),
        details         VARCHAR,
        ts              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    # Quarantine
    f"""
    CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.batch_quarantine (
        batch_id        VARCHAR PRIMARY KEY,
        pipeline_id     VARCHAR,
        abort_reason    VARCHAR,
        original_data_ref VARCHAR,
        quarantined_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        metadata        VARCHAR
    )
    """,
    # Agent invocation log (fail-safe audit — every agent call recorded)
    f"""
    CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.agent_reasoning_log (
        invocation_id   VARCHAR PRIMARY KEY,
        agent_name      VARCHAR,
        crew_name       VARCHAR,
        ts              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        input_preview   VARCHAR,
        output_preview  VARCHAR,
        tokens_used     INTEGER,
        duration_ms     INTEGER,
        phi_check       VARCHAR
    )
    """,
    # Phase 5.8: DQ Control Plane — UI-authored, version-tracked, multi-tenant
    # expectation suites. One row per (client_id, suite_name, version). Exactly
    # ONE row per (client_id, suite_name) may be LIVE at a time — enforced in
    # code since DuckDB lacks partial-unique-index support.
    # Phase 6: `source_type` column identifies which source_type (CLAIMS /
    # MEMBERSHIP / PROVIDER) the suite targets. NULL = stage-aggregate (legacy,
    # pre-6.2). Per DataQuality_Metrics.docx the rule is "do not merge or mix" —
    # baseline_seeder creates one suite per (client, stage, source_type).
    f"""
    CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.dq_suites (
        suite_id        VARCHAR PRIMARY KEY,
        client_id       VARCHAR NOT NULL,
        suite_name      VARCHAR NOT NULL,
        version         INTEGER NOT NULL,
        status          VARCHAR NOT NULL,
        expectations    VARCHAR NOT NULL,
        dq_dimensions   VARCHAR,
        source_type     VARCHAR,
        source          VARCHAR NOT NULL,
        created_by      VARCHAR NOT NULL,
        created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        submitted_at    TIMESTAMP,
        reviewed_by     VARCHAR,
        reviewed_at     TIMESTAMP,
        review_notes    VARCHAR,
        activated_at    TIMESTAMP,
        archived_at     TIMESTAMP
    )
    """,
    # Phase 6 migration: add source_type column on existing DBs (idempotent
    # via try/except since DuckDB has no IF NOT EXISTS for ADD COLUMN).
    # Wrapped in a DO NOTHING sentinel via CREATE TABLE AS to avoid failing
    # the whole DDL batch — we handle the column-exists case at DDL run time
    # in create_control_tables() below.
    # Immutable audit log for every suite state transition.
    f"""
    CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.dq_suite_audit_log (
        audit_id        VARCHAR PRIMARY KEY,
        suite_id        VARCHAR NOT NULL,
        from_status     VARCHAR,
        to_status       VARCHAR NOT NULL,
        actor           VARCHAR NOT NULL,
        notes           VARCHAR,
        ts              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
]


def create_control_tables(warehouse: Warehouse) -> None:
    """Idempotently create control-layer schema + tables.

    Phase 6: also runs forward-migrations for columns added after 5.8.
    """
    helper = getattr(warehouse, "create_schema_if_not_exists", None)
    if callable(helper):
        helper(CONTROL_SCHEMA)
    for stmt in _DDL:
        warehouse.execute(stmt)
    # Forward-migrations: Phase 6 adds columns to three tables. DuckDB has
    # no IF NOT EXISTS for ADD COLUMN, so wrap each in try/except that
    # swallows only the "already exists" error.
    _migrations = [
        f"ALTER TABLE {CONTROL_SCHEMA}.dq_suites ADD COLUMN source_type VARCHAR",
        f"ALTER TABLE {CONTROL_SCHEMA}.pipeline_checkpoints ADD COLUMN client_id VARCHAR",
        f"ALTER TABLE {CONTROL_SCHEMA}.pipeline_checkpoints ADD COLUMN source_type VARCHAR",
        f"ALTER TABLE {CONTROL_SCHEMA}.gx_validation_results ADD COLUMN client_id VARCHAR",
        f"ALTER TABLE {CONTROL_SCHEMA}.gx_validation_results ADD COLUMN source_type VARCHAR",
        f"ALTER TABLE {CONTROL_SCHEMA}.gx_validation_results ADD COLUMN dq_dimension VARCHAR",
    ]
    for stmt in _migrations:
        try:
            warehouse.execute(stmt)
        except Exception as exc:
            msg = str(exc).lower()
            if "already exists" not in msg and "duplicate column" not in msg:
                raise


class PipelineControl:
    """State-machine wrapper around the control tables.

    Every state transition writes to pipeline_control_audit_log. The current
    state is stored in pipeline_control_state. Transitions are defensive:
    PAUSED → RESUMING only, not PAUSED → RUNNING directly.
    """

    _LEGAL_TRANSITIONS: ClassVar[dict[PipelineState, set[PipelineState]]] = {
        PipelineState.RUNNING: {PipelineState.PAUSED, PipelineState.ABORTED},
        PipelineState.PAUSED: {PipelineState.RESUMING, PipelineState.ABORTED},
        PipelineState.RESUMING: {PipelineState.RUNNING, PipelineState.ABORTED},
        PipelineState.ABORTED: set(),  # terminal — new run_id required
    }

    def __init__(self, warehouse: Warehouse) -> None:
        self._wh = warehouse

    def ensure(self) -> None:
        create_control_tables(self._wh)

    def current(self, pipeline_id: str) -> PipelineState | None:
        rows = self._wh.query(
            f"SELECT status FROM {CONTROL_SCHEMA}.pipeline_control_state " "WHERE pipeline_id = $p",
            {"p": pipeline_id},
        )
        if not rows:
            return None
        return PipelineState(rows[0]["status"])

    def start(self, pipeline_id: str) -> None:
        """Initialize (or reset) a pipeline to RUNNING. Idempotent."""
        existing = self.current(pipeline_id)
        if existing == PipelineState.RUNNING:
            return
        now = datetime.now(UTC)
        self._wh.execute(
            f"DELETE FROM {CONTROL_SCHEMA}.pipeline_control_state WHERE pipeline_id = $p",
            {"p": pipeline_id},
        )
        self._wh.execute(
            f"INSERT INTO {CONTROL_SCHEMA}.pipeline_control_state "
            "(pipeline_id, status, updated_at) VALUES ($p, $s, $t)",
            {"p": pipeline_id, "s": PipelineState.RUNNING.value, "t": now},
        )
        self._log_transition(
            StateTransition(
                pipeline_id=pipeline_id,
                from_state=existing or PipelineState.RUNNING,
                to_state=PipelineState.RUNNING,
                actor="system",
                reason="pipeline start",
                severity=Severity.LOW,
            )
        )

    def transition(self, t: StateTransition) -> None:
        current = self.current(t.pipeline_id) or PipelineState.RUNNING
        if t.to_state not in self._LEGAL_TRANSITIONS.get(current, set()):
            raise ValueError(f"illegal transition {current} → {t.to_state} for {t.pipeline_id!r}")
        now = datetime.now(UTC)
        if t.to_state == PipelineState.PAUSED:
            self._wh.execute(
                f"UPDATE {CONTROL_SCHEMA}.pipeline_control_state SET "
                "status = $s, paused_at = $t, paused_reason = $r, paused_by = $a, "
                "severity = $sv, updated_at = $t WHERE pipeline_id = $p",
                {
                    "s": t.to_state.value,
                    "t": now,
                    "r": t.reason,
                    "a": t.actor,
                    "sv": t.severity.value,
                    "p": t.pipeline_id,
                },
            )
        else:
            self._wh.execute(
                f"UPDATE {CONTROL_SCHEMA}.pipeline_control_state SET "
                "status = $s, updated_at = $t WHERE pipeline_id = $p",
                {"s": t.to_state.value, "t": now, "p": t.pipeline_id},
            )
        self._log_transition(t)
        _log.info(
            "pipeline_control.transition",
            pipeline_id=t.pipeline_id,
            from_state=current.value,
            to_state=t.to_state.value,
            actor=t.actor,
            severity=t.severity.value,
        )

    def record_checkpoint(
        self,
        run_id: str,
        pipeline_id: str,
        checkpoint_name: str,
        row_count: int,
        fail_pct: float,
        status: str,
        client_id: str | None = None,
        source_type: str | None = None,
    ) -> None:
        """Persist a checkpoint completion marker.

        Phase 6: `client_id` + `source_type` capture tenant and CLAIMS/
        MEMBERSHIP/PROVIDER so dashboards can slice per-(client, source).
        Both optional for Phase-5.x back-compat.
        """
        self._wh.execute(
            f"INSERT OR REPLACE INTO {CONTROL_SCHEMA}.pipeline_checkpoints "
            "(run_id, pipeline_id, checkpoint_name, client_id, source_type, "
            " completed_at, row_count, fail_pct, status) "
            "VALUES ($r, $p, $c, $ci, $src, $t, $rc, $fp, $s)",
            {
                "r": run_id,
                "p": pipeline_id,
                "c": checkpoint_name,
                "ci": client_id,
                "src": source_type,
                "t": datetime.now(UTC),
                "rc": row_count,
                "fp": fail_pct,
                "s": status,
            },
        )

    def _log_transition(self, t: StateTransition) -> None:
        self._wh.execute(
            f"INSERT INTO {CONTROL_SCHEMA}.pipeline_control_audit_log "
            "(transition_id, pipeline_id, from_state, to_state, actor, reason, severity) "
            "VALUES ($tid, $p, $fs, $ts, $a, $r, $sv)",
            {
                "tid": str(uuid.uuid4()),
                "p": t.pipeline_id,
                "fs": t.from_state.value,
                "ts": t.to_state.value,
                "a": t.actor,
                "r": t.reason,
                "sv": t.severity.value,
            },
        )


# ----------------------------------------------------------------------------
# Phase 6: Auto-resume task-progress tracker
# ----------------------------------------------------------------------------


@dataclass(frozen=True)
class TaskProgress:
    """Immutable snapshot of a single task's progress row."""

    run_id: str
    pipeline_id: str
    task_name: str
    status: str  # "SUCCESS" | "FAILED" | "RUNNING"
    started_at: datetime | None
    completed_at: datetime | None
    duration_ms: int | None
    output: dict[str, Any]
    error: str | None


class TaskProgressTracker:
    """Records per-task completion for auto-resume.

    Every task callable wraps its work between `mark_started` and
    `mark_done` (or `mark_failed`). On re-run with the same run_id,
    the orchestrator queries `completed_tasks(run_id)` and skips
    anything already recorded SUCCESSFUL. The stored `output_json`
    is re-hydrated into the TaskContext so downstream tasks see the
    same upstream state as the original run.

    Thread-safety: safe as long as each run_id is owned by one
    orchestrator process. Airflow LocalExecutor + our single-instance
    local_sequential both satisfy this.
    """

    def __init__(self, warehouse: Warehouse) -> None:
        self._wh = warehouse

    def completed_tasks(self, run_id: str) -> dict[str, TaskProgress]:
        """Map task_name -> TaskProgress for every SUCCESSFUL task in run_id."""
        rows = self._wh.query(
            f"SELECT * FROM {CONTROL_SCHEMA}.pipeline_task_progress "
            "WHERE run_id = $r AND status = 'SUCCESS' "
            "ORDER BY completed_at ASC",
            {"r": run_id},
        )
        return {r["task_name"]: _row_to_progress(r) for r in rows}

    def mark_started(self, run_id: str, pipeline_id: str, task_name: str) -> None:
        """Record that a task has begun. Safe to call multiple times (INSERT OR REPLACE)."""
        self._wh.execute(
            f"INSERT OR REPLACE INTO {CONTROL_SCHEMA}.pipeline_task_progress "
            "(run_id, pipeline_id, task_name, status, started_at) "
            "VALUES ($r, $p, $t, $s, $ts)",
            {
                "r": run_id,
                "p": pipeline_id,
                "t": task_name,
                "s": "RUNNING",
                "ts": datetime.now(UTC),
            },
        )

    def mark_done(
        self,
        run_id: str,
        pipeline_id: str,
        task_name: str,
        output: dict[str, Any],
        duration_ms: int,
    ) -> None:
        """Record task SUCCESS. On auto-resume this row causes the task to be skipped."""
        self._wh.execute(
            f"INSERT OR REPLACE INTO {CONTROL_SCHEMA}.pipeline_task_progress "
            "(run_id, pipeline_id, task_name, status, started_at, completed_at, "
            " duration_ms, output_json) "
            "VALUES ($r, $p, $t, $s, $st, $ct, $d, $o)",
            {
                "r": run_id,
                "p": pipeline_id,
                "t": task_name,
                "s": "SUCCESS",
                "st": datetime.now(UTC),
                "ct": datetime.now(UTC),
                "d": duration_ms,
                "o": json.dumps(output, default=str),
            },
        )

    def mark_failed(
        self,
        run_id: str,
        pipeline_id: str,
        task_name: str,
        error: str,
        duration_ms: int,
    ) -> None:
        """Record task FAILURE. Row is overwritten on a resume-retry."""
        self._wh.execute(
            f"INSERT OR REPLACE INTO {CONTROL_SCHEMA}.pipeline_task_progress "
            "(run_id, pipeline_id, task_name, status, started_at, completed_at, "
            " duration_ms, error) "
            "VALUES ($r, $p, $t, $s, $st, $ct, $d, $e)",
            {
                "r": run_id,
                "p": pipeline_id,
                "t": task_name,
                "s": "FAILED",
                "st": datetime.now(UTC),
                "ct": datetime.now(UTC),
                "d": duration_ms,
                "e": error[:4000],  # truncate huge stack traces
            },
        )


def _row_to_progress(row: dict[str, Any]) -> TaskProgress:
    raw_output = row.get("output_json")
    try:
        output = json.loads(raw_output) if raw_output else {}
    except json.JSONDecodeError:
        output = {}
    return TaskProgress(
        run_id=row["run_id"],
        pipeline_id=row["pipeline_id"],
        task_name=row["task_name"],
        status=row["status"],
        started_at=row.get("started_at"),
        completed_at=row.get("completed_at"),
        duration_ms=row.get("duration_ms"),
        output=output,
        error=row.get("error"),
    )
