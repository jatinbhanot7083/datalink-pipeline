"""Pipeline control — state machine + audit log.

Matches the schema in GX_Pipeline_Control_Agentic_AI_Architecture.docx §3.4:
  - pipeline_control_state        current state per pipeline
  - pipeline_checkpoints          checkpoint markers (bronze/silver/gold)
  - pipeline_control_audit_log    immutable transition history
  - batch_quarantine              aborted batches for root-cause analysis
  - agent_reasoning_log           every agent invocation (Phase 5 addition)

State transitions: RUNNING → PAUSED → RESUMING → RUNNING, or → ABORTED.
Auto-pause fires when a GX checkpoint exceeds its failure threshold (default 5%).
Auto-abort on schema mismatch, file-format change, or orphaned Links.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import ClassVar

from datalink.adapters.protocols import Warehouse
from datalink.logging import get_logger

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
    # Checkpoint markers (one row per pipeline run x checkpoint)
    f"""
    CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.pipeline_checkpoints (
        run_id          VARCHAR,
        pipeline_id     VARCHAR,
        checkpoint_name VARCHAR,
        completed_at    TIMESTAMP,
        row_count       INTEGER,
        fail_pct        DECIMAL(5,2),
        status          VARCHAR,
        PRIMARY KEY (run_id, checkpoint_name)
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
    # Per-expectation detail from GX checkpoints (Phase 5)
    f"""
    CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.gx_validation_results (
        validation_id   VARCHAR PRIMARY KEY,
        run_id          VARCHAR,
        checkpoint_name VARCHAR,
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
    f"""
    CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.dq_suites (
        suite_id        VARCHAR PRIMARY KEY,
        client_id       VARCHAR NOT NULL,
        suite_name      VARCHAR NOT NULL,
        version         INTEGER NOT NULL,
        status          VARCHAR NOT NULL,
        expectations    VARCHAR NOT NULL,
        dq_dimensions   VARCHAR,
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
    """Idempotently create control-layer schema + tables."""
    helper = getattr(warehouse, "create_schema_if_not_exists", None)
    if callable(helper):
        helper(CONTROL_SCHEMA)
    for stmt in _DDL:
        warehouse.execute(stmt)


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
    ) -> None:
        self._wh.execute(
            f"INSERT OR REPLACE INTO {CONTROL_SCHEMA}.pipeline_checkpoints "
            "(run_id, pipeline_id, checkpoint_name, completed_at, row_count, fail_pct, status) "
            "VALUES ($r, $p, $c, $t, $rc, $fp, $s)",
            {
                "r": run_id,
                "p": pipeline_id,
                "c": checkpoint_name,
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
