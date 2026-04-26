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
    # Current state (one row per (pipeline, client) tuple — Phase 9.4).
    # Migration of legacy single-PK tables is handled idempotently by
    # scripts/migrate_pipeline_control_state.py: backfills client_id =
    # 'default' for existing rows then re-establishes the composite PK.
    f"""
    CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.pipeline_control_state (
        pipeline_id     VARCHAR NOT NULL,
        client_id       VARCHAR NOT NULL DEFAULT 'default',
        status          VARCHAR NOT NULL,
        checkpoint_id   VARCHAR,
        paused_at       TIMESTAMP,
        paused_reason   VARCHAR,
        paused_by       VARCHAR,
        severity        VARCHAR,
        updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (pipeline_id, client_id)
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
    # Phase 9.5: Bronze retention audit log. One row per
    # (table, prune_run_id) — append-only. Surfaces "what got pruned,
    # when, and how much" so operators can answer audit questions
    # ("did we delete claim X's raw row before its retention deadline?")
    # with a single SQL query. Cutoff_dt + watermark_dt together
    # establish that Silver had read past the cutoff before deletion.
    #
    #   prune_run_id      one ID per script invocation
    #   table_schema      e.g. BRONZE_AETNA
    #   table_name        e.g. RAW_MEMBERSHIP
    #   retention_days    config at time of prune
    #   cutoff_dt         _load_dt < this was eligible for delete
    #   watermark_dt      latest Silver completion time consulted
    #   rows_pruned       count actually deleted (0 in dry-run)
    #   oldest_kept_dt    smallest _load_dt remaining after prune
    #   started_at / finished_at
    #   status            DRY_RUN | PRUNED | SKIPPED_NO_SILVER | FAILED
    #   error             non-null on FAILED
    f"""
    CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.bronze_retention_log (
        prune_run_id      VARCHAR,
        table_schema      VARCHAR NOT NULL,
        table_name        VARCHAR NOT NULL,
        retention_days    INTEGER NOT NULL,
        cutoff_dt         TIMESTAMP,
        watermark_dt      TIMESTAMP,
        rows_pruned       INTEGER,
        oldest_kept_dt    TIMESTAMP,
        started_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        finished_at       TIMESTAMP,
        status            VARCHAR NOT NULL,
        error             VARCHAR,
        PRIMARY KEY (prune_run_id, table_schema, table_name)
    )
    """,
    # Phase 9.3: Egress audit log. One row per (egress_batch_id, target,
    # entity). Append-only. Records every attempt to push Gold deltas to
    # an On-Prem operational DB so operators can prove "what landed on
    # Postgres / SQL Server" for any historical batch.
    #
    #   egress_batch_id   one ID per (run_id, entity, target) push attempt
    #   pipeline_run_id   Airflow run_id that triggered the push
    #   client_id         which tenant
    #   entity            'gold_patient_auth' | 'gold_um_member' | ...
    #   target            'postgres' | 'sqlserver' | 'postgres_replica'
    #   row_count         rows actually written this batch (0 if no deltas)
    #   started_at        when the push began
    #   finished_at       when it completed (NULL on still-running)
    #   status            PENDING | PUSHED | FAILED | NOOP (no rows)
    #   cutoff_dts        Silver effective_start_date watermark used to
    #                     pick deltas — drives the NEXT batch's cutoff
    #   error             non-null on FAILED
    f"""
    CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.egress_batch_log (
        egress_batch_id     VARCHAR PRIMARY KEY,
        pipeline_run_id     VARCHAR,
        client_id           VARCHAR,
        entity              VARCHAR,
        target              VARCHAR,
        row_count           INTEGER,
        started_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        finished_at         TIMESTAMP,
        status              VARCHAR NOT NULL,
        cutoff_dts          TIMESTAMP,
        error               VARCHAR
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
        suite_id           VARCHAR PRIMARY KEY,
        client_id          VARCHAR NOT NULL,
        suite_name         VARCHAR NOT NULL,
        version            INTEGER NOT NULL,
        status             VARCHAR NOT NULL,
        expectations       VARCHAR NOT NULL,
        dq_dimensions      VARCHAR,
        source_type        VARCHAR,
        source             VARCHAR NOT NULL,
        schema_fingerprint VARCHAR,
        created_by         VARCHAR NOT NULL,
        created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        submitted_at       TIMESTAMP,
        reviewed_by        VARCHAR,
        reviewed_at        TIMESTAMP,
        review_notes       VARCHAR,
        activated_at       TIMESTAMP,
        archived_at        TIMESTAMP
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
    # Phase 11.1 — Schema contracts. The agreed-upon Bronze schema for
    # each (client_id, source_type). Versioned: register_contract()
    # bumps `contract_version` and flips the prior row to is_active=FALSE
    # atomically. Exactly one is_active=TRUE row per (client, source) at
    # a time — invariant enforced in code (DuckDB lacks partial-unique
    # indexes; same pattern as dq_suites).
    #
    #   columns_json  JSON array of {name, logical_type, nullable}
    #                  (ColumnContract.to_dict()). Logical types:
    #                  TEXT | INTEGER | DECIMAL | DATE | TIMESTAMP | BOOLEAN.
    f"""
    CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.source_schema_contracts (
        contract_id        VARCHAR PRIMARY KEY,
        client_id          VARCHAR NOT NULL,
        source_type        VARCHAR NOT NULL,
        contract_version   INTEGER NOT NULL,
        columns_json       VARCHAR NOT NULL,
        is_active          BOOLEAN NOT NULL DEFAULT TRUE,
        created_by         VARCHAR NOT NULL,
        created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        notes              VARCHAR
    )
    """,
    # Phase 13.2 — AI Smart Mapper sessions. One row per mapping
    # conversation: source profile snapshot, NL Gold contract, multi-turn
    # chat history, currently-proposed Silver/Gold/On-Prem artifacts,
    # and the HITL approval state. JSON columns (conversation_json,
    # source_profile_json, sample_preview_json) keep the long, evolving
    # text payloads readable and queryable without a side-table per
    # field. Status walks DRAFT → PENDING_REVIEW → APPROVED → DEPLOYED
    # (terminal); mirrors the pattern from Phase 10 / 12.
    f"""
    CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.mapping_sessions (
        session_id              VARCHAR PRIMARY KEY,
        client_id               VARCHAR NOT NULL,
        source_qualified_table  VARCHAR NOT NULL,
        gold_contract_text      VARCHAR,
        target_mode             VARCHAR NOT NULL,
        status                  VARCHAR NOT NULL,
        source_profile_json     VARCHAR,
        conversation_json       VARCHAR,
        silver_sql              VARCHAR,
        gold_sql                VARCHAR,
        onprem_postgres_sql     VARCHAR,
        onprem_mssql_sql        VARCHAR,
        sample_preview_json     VARCHAR,
        silver_target_table     VARCHAR,
        gold_target_table       VARCHAR,
        created_by              VARCHAR NOT NULL,
        created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at              TIMESTAMP,
        submitted_at            TIMESTAMP,
        reviewed_by             VARCHAR,
        reviewed_at             TIMESTAMP,
        review_notes            VARCHAR,
        deployed_at             TIMESTAMP,
        archived_at             TIMESTAMP,
        notes                   VARCHAR
    )
    """,
    # Phase 13.2 — Mapper session audit log. Same shape as
    # dq_suite_audit_log / policy_audit_log; one row per state
    # transition + content edit.
    f"""
    CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.mapping_session_audit_log (
        audit_id        VARCHAR PRIMARY KEY,
        session_id      VARCHAR NOT NULL,
        from_status     VARCHAR,
        to_status       VARCHAR NOT NULL,
        actor           VARCHAR NOT NULL,
        notes           VARCHAR,
        ts              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    # Phase 13.8 — Deployment artifacts. One row per file-write event
    # from the deployer. Tracks which session produced which files +
    # the content hash so re-deployments are detectable. Re-runs of the
    # same session insert NEW rows (not UPSERT) — idempotent at the file
    # level (overwrite) but auditable at the deployment-event level.
    f"""
    CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.mapping_artifacts (
        artifact_id     VARCHAR PRIMARY KEY,
        session_id      VARCHAR NOT NULL,
        files_written   VARCHAR,
        content_hash    VARCHAR,
        deployed_by     VARCHAR NOT NULL,
        deployed_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    # Phase 12.1 — Pipeline-control threshold policies. Versioned config
    # for "when does the pipeline pause vs abort". Mirrors dq_suites's
    # state machine (DRAFT → PENDING_REVIEW → APPROVED → LIVE → ARCHIVED)
    # and same single-LIVE-row-per-tuple invariant. Edit via the Pipeline
    # Control UI (Phase 12.3) — every change goes through HITL review.
    #
    # Reads via PipelineControlPolicyRegistry.get_or_default() which falls
    # back to the synthetic DEFAULT_POLICY when no LIVE row exists, so
    # tenants without a registered policy keep behaving exactly as today.
    f"""
    CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.pipeline_control_policies (
        policy_id            VARCHAR PRIMARY KEY,
        client_id            VARCHAR NOT NULL,
        pipeline_id          VARCHAR NOT NULL,
        version              INTEGER NOT NULL,
        status               VARCHAR NOT NULL,
        fail_rate_pause_pct  DOUBLE NOT NULL,
        fail_rate_abort_pct  DOUBLE NOT NULL,
        schema_drift_action  VARCHAR NOT NULL,
        row_count_drop_pct   DOUBLE NOT NULL,
        created_by           VARCHAR NOT NULL,
        created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        submitted_at         TIMESTAMP,
        reviewed_by          VARCHAR,
        reviewed_at          TIMESTAMP,
        review_notes         VARCHAR,
        activated_at         TIMESTAMP,
        archived_at          TIMESTAMP,
        notes                VARCHAR
    )
    """,
    # Phase 12.1 — Audit trail for every policy state transition. Distinct
    # from pipeline_control_audit_log (runtime RUNNING/PAUSED transitions).
    # This table records who-changed-what at the CONFIG level.
    f"""
    CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.policy_audit_log (
        audit_id        VARCHAR PRIMARY KEY,
        policy_id       VARCHAR NOT NULL,
        from_status     VARCHAR,
        to_status       VARCHAR NOT NULL,
        actor           VARCHAR NOT NULL,
        notes           VARCHAR,
        ts              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    # Phase 11.3 — Schema drift audit log. One row per detected drift
    # event (clean batches do NOT emit a row — would dilute the table on
    # every successful load). Keyed by drift_id; queried by detected_at
    # for the operator UI.
    #
    #   drift_type      ADDITIVE | SUBTRACTIVE | DESTRUCTIVE | MIXED
    #   severity        INFO | WARNING | FATAL
    #   added_columns   JSON array of column names appended vs contract
    #   removed_columns JSON array of contracted columns missing
    #   type_changes    JSON array of {column, expected, actual}
    #   action_taken    LOGGED  — additive, ingest continued
    #                   HALTED  — fatal, batch raised SchemaDriftError
    f"""
    CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.schema_drift_log (
        drift_id           VARCHAR PRIMARY KEY,
        detected_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        client_id          VARCHAR NOT NULL,
        source_type        VARCHAR NOT NULL,
        source_file        VARCHAR,
        batch_id           VARCHAR,
        contract_version   INTEGER,
        drift_type         VARCHAR NOT NULL,
        severity           VARCHAR NOT NULL,
        added_columns      VARCHAR,
        removed_columns    VARCHAR,
        type_changes       VARCHAR,
        action_taken       VARCHAR NOT NULL,
        notes              VARCHAR
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
        f"ALTER TABLE {CONTROL_SCHEMA}.dq_suites ADD COLUMN schema_fingerprint VARCHAR",
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

    # ------------------------------------------------------------------
    # State lookup — Phase 9.4: client-scoped. Default 'default' keeps
    # back-compat with legacy single-key calls (sensors, demos pre-9.4).
    # ------------------------------------------------------------------
    def current(self, pipeline_id: str, client_id: str = "default") -> PipelineState | None:
        rows = self._wh.query(
            f"SELECT status FROM {CONTROL_SCHEMA}.pipeline_control_state "
            "WHERE pipeline_id = $p AND client_id = $c",
            {"p": pipeline_id, "c": client_id},
        )
        if not rows:
            return None
        return PipelineState(rows[0]["status"])

    def list_all_states(self) -> list[dict[str, Any]]:
        """Phase 9.4: feed the Control Tower Pipeline Status panel.

        Returns one dict per (pipeline_id, client_id) row with full state
        + metadata. UI sorts/filters from here.
        """
        return list(
            self._wh.query(
                f"SELECT pipeline_id, client_id, status, paused_at, "
                f"       paused_reason, paused_by, severity, updated_at "
                f"FROM {CONTROL_SCHEMA}.pipeline_control_state "
                f"ORDER BY pipeline_id, client_id"
            )
        )

    def start(self, pipeline_id: str, client_id: str = "default") -> None:
        """Initialize (or reset) a pipeline to RUNNING. Idempotent.

        Phase 9.4: client-scoped. ``client_id`` defaults to 'default' so
        old callers (and tests) keep working without modification.
        """
        existing = self.current(pipeline_id, client_id)
        if existing == PipelineState.RUNNING:
            return
        now = datetime.now(UTC)
        self._wh.execute(
            f"DELETE FROM {CONTROL_SCHEMA}.pipeline_control_state "
            "WHERE pipeline_id = $p AND client_id = $c",
            {"p": pipeline_id, "c": client_id},
        )
        self._wh.execute(
            f"INSERT INTO {CONTROL_SCHEMA}.pipeline_control_state "
            "(pipeline_id, client_id, status, updated_at) VALUES ($p, $c, $s, $t)",
            {"p": pipeline_id, "c": client_id, "s": PipelineState.RUNNING.value, "t": now},
        )
        self._log_transition(
            StateTransition(
                pipeline_id=pipeline_id,
                from_state=existing or PipelineState.RUNNING,
                to_state=PipelineState.RUNNING,
                actor="system",
                reason=f"pipeline start (client={client_id})",
                severity=Severity.LOW,
            )
        )

    def transition(self, t: StateTransition, client_id: str = "default") -> None:
        current = self.current(t.pipeline_id, client_id) or PipelineState.RUNNING
        if t.to_state not in self._LEGAL_TRANSITIONS.get(current, set()):
            raise ValueError(
                f"illegal transition {current} → {t.to_state} for "
                f"{t.pipeline_id!r}/{client_id!r}"
            )
        now = datetime.now(UTC)
        if t.to_state == PipelineState.PAUSED:
            self._wh.execute(
                f"UPDATE {CONTROL_SCHEMA}.pipeline_control_state SET "
                "status = $s, paused_at = $t, paused_reason = $r, paused_by = $a, "
                "severity = $sv, updated_at = $t "
                "WHERE pipeline_id = $p AND client_id = $c",
                {
                    "s": t.to_state.value,
                    "t": now,
                    "r": t.reason,
                    "a": t.actor,
                    "sv": t.severity.value,
                    "p": t.pipeline_id,
                    "c": client_id,
                },
            )
        else:
            self._wh.execute(
                f"UPDATE {CONTROL_SCHEMA}.pipeline_control_state SET "
                "status = $s, updated_at = $t "
                "WHERE pipeline_id = $p AND client_id = $c",
                {"s": t.to_state.value, "t": now, "p": t.pipeline_id, "c": client_id},
            )
        self._log_transition(t)
        _log.info(
            "pipeline_control.transition",
            pipeline_id=t.pipeline_id,
            client_id=client_id,
            from_state=current.value,
            to_state=t.to_state.value,
            actor=t.actor,
            severity=t.severity.value,
        )

    # ------------------------------------------------------------------
    # Phase 9.4: public state-machine helpers — operator-facing API.
    # Each method validates the source state, writes the transition, and
    # logs to pipeline_control_audit_log via _log_transition. Use these
    # from UI buttons + auto-pause hooks; never poke the table directly.
    # ------------------------------------------------------------------

    def pause(
        self,
        pipeline_id: str,
        *,
        client_id: str = "default",
        reason: str,
        actor: str,
        severity: Severity = Severity.HIGH,
    ) -> None:
        """Move a RUNNING pipeline to PAUSED. Used by auto-breach hooks."""
        self.transition(
            StateTransition(
                pipeline_id=pipeline_id,
                from_state=PipelineState.RUNNING,
                to_state=PipelineState.PAUSED,
                actor=actor,
                reason=reason,
                severity=severity,
            ),
            client_id=client_id,
        )

    def resume(
        self,
        pipeline_id: str,
        *,
        client_id: str = "default",
        actor: str,
        notes: str = "",
    ) -> None:
        """Move a PAUSED pipeline back to RUNNING. Operator-initiated.

        The state machine forbids PAUSED → RUNNING directly — it goes
        through an intermediate RESUMING state so an audit reader can
        always tell "operator clicked Resume" (RESUMING) apart from
        "fresh DAG run" (RUNNING). We walk both transitions in this
        method so the caller sees a single atomic operation.
        """
        reason = notes or "operator resume"
        # PAUSED → RESUMING (operator intent recorded).
        self.transition(
            StateTransition(
                pipeline_id=pipeline_id,
                from_state=PipelineState.PAUSED,
                to_state=PipelineState.RESUMING,
                actor=actor,
                reason=reason,
                severity=Severity.LOW,
            ),
            client_id=client_id,
        )
        # RESUMING → RUNNING (work allowed to proceed). Sensor's next
        # poke (≤10 s later) sees RUNNING and unblocks the gate.
        self.transition(
            StateTransition(
                pipeline_id=pipeline_id,
                from_state=PipelineState.RESUMING,
                to_state=PipelineState.RUNNING,
                actor=actor,
                reason=reason,
                severity=Severity.LOW,
            ),
            client_id=client_id,
        )
        # Clear paused_* columns so audit fields don't lie.
        self._wh.execute(
            f"UPDATE {CONTROL_SCHEMA}.pipeline_control_state SET "
            "paused_at = NULL, paused_reason = NULL, paused_by = NULL, severity = NULL "
            "WHERE pipeline_id = $p AND client_id = $c",
            {"p": pipeline_id, "c": client_id},
        )

    def abort(
        self,
        pipeline_id: str,
        *,
        client_id: str = "default",
        reason: str,
        actor: str,
        severity: Severity = Severity.CRITICAL,
    ) -> None:
        """Mark a pipeline as ABORTED. Terminal; future runs require
        explicit ``force_resume`` to re-arm. Use for catastrophic
        breaches (>25% fail rate, contract violations, etc.)."""
        self.transition(
            StateTransition(
                pipeline_id=pipeline_id,
                from_state=self.current(pipeline_id, client_id) or PipelineState.RUNNING,
                to_state=PipelineState.ABORTED,
                actor=actor,
                reason=reason,
                severity=severity,
            ),
            client_id=client_id,
        )

    def force_resume(
        self,
        pipeline_id: str,
        *,
        client_id: str = "default",
        actor: str,
        reason: str,
    ) -> None:
        """Bring an ABORTED pipeline back to RUNNING. **Bypasses the
        normal state machine** — written explicitly so the audit log
        carries the operator name + justification (compliance trail).
        Use sparingly; this is the "I know what I'm doing" override."""
        now = datetime.now(UTC)
        self._wh.execute(
            f"UPDATE {CONTROL_SCHEMA}.pipeline_control_state SET "
            "status = $s, paused_at = NULL, paused_reason = NULL, "
            "paused_by = NULL, severity = NULL, updated_at = $t "
            "WHERE pipeline_id = $p AND client_id = $c",
            {"s": PipelineState.RUNNING.value, "t": now, "p": pipeline_id, "c": client_id},
        )
        self._log_transition(
            StateTransition(
                pipeline_id=pipeline_id,
                from_state=PipelineState.ABORTED,
                to_state=PipelineState.RUNNING,
                actor=actor,
                reason=f"FORCE RESUME: {reason}",
                severity=Severity.CRITICAL,
            )
        )
        _log.warning(
            "pipeline_control.force_resume",
            pipeline_id=pipeline_id,
            client_id=client_id,
            actor=actor,
            reason=reason,
        )

    def _upsert(
        self,
        table: str,
        key_cols: list[str],
        all_cols: list[str],
        values: dict[str, Any],
        key_values: dict[str, Any],
    ) -> None:
        """Portable UPSERT: DELETE matching keys, then INSERT the fresh row.

        ``INSERT OR REPLACE`` is DuckDB/SQLite syntax — Snowflake rejects it.
        DELETE-then-INSERT works identically on both backends. Not atomic at
        the row level, but acceptable for CONTROL audit tables where a brief
        "gap" between delete and insert is inconsequential (these are polled,
        not read in a hot loop).
        """
        where = " AND ".join(f"{c} = ${c}" for c in key_cols)
        self._wh.execute(
            f"DELETE FROM {CONTROL_SCHEMA}.{table} WHERE {where}",
            key_values,
        )
        placeholders = ", ".join(f"${c}" for c in all_cols)
        cols_sql = ", ".join(all_cols)
        self._wh.execute(
            f"INSERT INTO {CONTROL_SCHEMA}.{table} ({cols_sql}) VALUES ({placeholders})",
            values,
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
        self._upsert(
            table="pipeline_checkpoints",
            key_cols=["run_id", "checkpoint_name"],
            all_cols=[
                "run_id",
                "pipeline_id",
                "checkpoint_name",
                "client_id",
                "source_type",
                "completed_at",
                "row_count",
                "fail_pct",
                "status",
            ],
            values={
                "run_id": run_id,
                "pipeline_id": pipeline_id,
                "checkpoint_name": checkpoint_name,
                "client_id": client_id,
                "source_type": source_type,
                "completed_at": datetime.now(UTC),
                "row_count": row_count,
                "fail_pct": fail_pct,
                "status": status,
            },
            key_values={"run_id": run_id, "checkpoint_name": checkpoint_name},
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
        """Record that a task has begun. Safe to call multiple times (upsert)."""
        self._upsert(
            table="pipeline_task_progress",
            key_cols=["run_id", "task_name"],
            all_cols=["run_id", "pipeline_id", "task_name", "status", "started_at"],
            values={
                "run_id": run_id,
                "pipeline_id": pipeline_id,
                "task_name": task_name,
                "status": "RUNNING",
                "started_at": datetime.now(UTC),
            },
            key_values={"run_id": run_id, "task_name": task_name},
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
        now = datetime.now(UTC)
        self._upsert(
            table="pipeline_task_progress",
            key_cols=["run_id", "task_name"],
            all_cols=[
                "run_id",
                "pipeline_id",
                "task_name",
                "status",
                "started_at",
                "completed_at",
                "duration_ms",
                "output_json",
            ],
            values={
                "run_id": run_id,
                "pipeline_id": pipeline_id,
                "task_name": task_name,
                "status": "SUCCESS",
                "started_at": now,
                "completed_at": now,
                "duration_ms": duration_ms,
                "output_json": json.dumps(output, default=str),
            },
            key_values={"run_id": run_id, "task_name": task_name},
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
        now = datetime.now(UTC)
        self._upsert(
            table="pipeline_task_progress",
            key_cols=["run_id", "task_name"],
            all_cols=[
                "run_id",
                "pipeline_id",
                "task_name",
                "status",
                "started_at",
                "completed_at",
                "duration_ms",
                "error",
            ],
            values={
                "run_id": run_id,
                "pipeline_id": pipeline_id,
                "task_name": task_name,
                "status": "FAILED",
                "started_at": now,
                "completed_at": now,
                "duration_ms": duration_ms,
                "error": error[:4000],  # truncate huge stack traces
            },
            key_values={"run_id": run_id, "task_name": task_name},
        )

    def _upsert(
        self,
        *,
        table: str,
        key_cols: list[str],
        all_cols: list[str],
        values: dict[str, Any],
        key_values: dict[str, Any],
    ) -> None:
        """Phase 9.4 / Phase 13 hygiene — idempotent INSERT via DELETE-then-INSERT.

        Snowflake- and DuckDB-portable. Mark started/done/failed call this
        with the same ``key_cols`` (always ``[run_id, task_name]``) so a
        retry of the same task overwrites the prior row instead of
        violating the unique-key invariant.

        Single-statement DELETE + INSERT per call. The two writes happen
        in sequence; on a crash between them the row is gone — caller's
        next mark_started attempt will simply succeed because the DELETE
        is a no-op on already-empty key. Idempotent on re-entry.
        """
        where = " AND ".join(f"{k} = ${k}" for k in key_cols)
        cols_sql = ", ".join(all_cols)
        placeholders = ", ".join(f"${c}" for c in all_cols)
        self._wh.execute(
            f"DELETE FROM {CONTROL_SCHEMA}.{table} WHERE {where}",
            key_values,
        )
        self._wh.execute(
            f"INSERT INTO {CONTROL_SCHEMA}.{table} ({cols_sql}) VALUES ({placeholders})",
            values,
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
