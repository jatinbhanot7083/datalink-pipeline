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
        source_type          VARCHAR,
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
    # ========================================================================
    # PHASE 14 — Data Contract Architect — schema authoring registry.
    # ========================================================================
    # The ContractArchitectAgent grounds proposals against a corpus of
    # industry standards (HL7 FHIR R4, X12 EDI, NCPDP D.0, CMS dictionaries,
    # Data Vault 2.0, HEDIS) plus operator-uploaded "custom" standards
    # (org-specific spec docs).
    #
    # Embedded reference content lives in Postgres `agent_memory.standard_references`
    # (pgvector). This table is just the registry of WHICH standards are loaded.
    f"""
    CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.standard_registry (
        standard_id      VARCHAR PRIMARY KEY,
        code             VARCHAR NOT NULL,            -- fhir-r4 | x12 | ncpdp-d0 | cms | dv2 | hedis | custom-<slug>
        display_name     VARCHAR NOT NULL,
        version          VARCHAR,                     -- e.g. "R4", "5010"
        is_industry      BOOLEAN NOT NULL DEFAULT TRUE,
        is_active        BOOLEAN NOT NULL DEFAULT TRUE,
        source_doc_uri   VARCHAR,                     -- URL or file path the corpus was ingested from
        chunk_count      INTEGER NOT NULL DEFAULT 0,
        registered_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        registered_by    VARCHAR,
        notes            VARCHAR
    )
    """,
    # Operator-uploaded "custom" standards (org's home-grown specs).
    # Pointer table — the actual chunks + embeddings live in
    # agent_memory.standard_references with standard_id matching here.
    f"""
    CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.custom_standards_registry (
        custom_id        VARCHAR PRIMARY KEY,
        standard_id      VARCHAR NOT NULL,            -- FK to standard_registry.standard_id
        org_id           VARCHAR NOT NULL DEFAULT 'datalink',
        name             VARCHAR NOT NULL,            -- e.g. "DataLink Internal Claims Spec v3"
        source_doc_uri   VARCHAR NOT NULL,            -- where the original doc lives
        source_doc_hash  VARCHAR,                     -- SHA-256 of the doc — detects re-upload
        ingested_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        ingested_by      VARCHAR NOT NULL,
        status           VARCHAR NOT NULL DEFAULT 'ACTIVE',  -- ACTIVE | ARCHIVED | INGESTING | FAILED
        ingestion_log    VARCHAR
    )
    """,
    # Working drafts + approved contracts authored on the Data Contract
    # Architect page. Status flow: DRAFT -> PENDING_REVIEW -> APPROVED -> ARCHIVED.
    # Approval generates the 5 artifacts (Bronze DDL, contract row, GX suite,
    # dbt scaffold, vendor spec) and writes pointers/IDs back here.
    f"""
    CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.contract_designs (
        design_id              VARCHAR PRIMARY KEY,
        client_id              VARCHAR NOT NULL,
        source_type            VARCHAR NOT NULL,             -- CLAIMS | MEMBERSHIP | PROVIDER | <custom>
        status                 VARCHAR NOT NULL,             -- DRAFT | PENDING_REVIEW | APPROVED | ARCHIVED
        version                INTEGER NOT NULL DEFAULT 1,
        mode                   VARCHAR NOT NULL,             -- FILE_DRIVEN | CONTRACT_FIRST
        anchored_standards     VARCHAR,                      -- JSON array of standard_ids
        temperature            DECIMAL(3,2) NOT NULL DEFAULT 0.0,
        grounding_k            INTEGER NOT NULL DEFAULT 3,
        strictness             DECIMAL(3,2) NOT NULL DEFAULT 0.5,
        sample_file_uri        VARCHAR,                      -- only set for FILE_DRIVEN mode
        nl_description         VARCHAR,                      -- only set for CONTRACT_FIRST mode
        proposed_columns       VARCHAR,                      -- JSON array — what AI proposed
        edited_columns         VARCHAR,                      -- JSON array — operator edits on top
        final_columns          VARCHAR,                      -- JSON array — what was approved
        proposed_ddl           VARCHAR,                      -- raw CREATE TABLE the AI emitted
        final_ddl              VARCHAR,                      -- final DDL after edits
        deviation_log          VARCHAR,                      -- JSON — vendor-vs-standard mismatches
        per_column_reasoning   VARCHAR,                      -- JSON — why AI picked each column/type
        standard_match_scores  VARCHAR,                      -- JSON — per-standard fit score (0-1)
        gx_suite_id            VARCHAR,                      -- FK to dq_suites once approved
        contract_id            VARCHAR,                      -- FK to source_schema_contracts once approved
        dbt_scaffold_uri       VARCHAR,                      -- path to generated dbt model files
        vendor_spec_md         VARCHAR,                      -- markdown for the vendor spec PDF
        vendor_spec_pdf_uri    VARCHAR,                      -- generated PDF location
        approval_mode          VARCHAR,                      -- DRAFT | HITL | AUTO_APPROVE
        created_at             TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        created_by             VARCHAR NOT NULL,
        submitted_at           TIMESTAMP,
        approved_at            TIMESTAMP,
        approved_by            VARCHAR,
        archived_at            TIMESTAMP,
        archived_by            VARCHAR,
        notes                  VARCHAR
    )
    """,
    # Immutable audit log — every state transition + every operator edit.
    # Mirrors policy_audit_log + dq_suite_audit_log shapes for consistency.
    f"""
    CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.contract_design_audit_log (
        audit_id           VARCHAR PRIMARY KEY,
        design_id          VARCHAR NOT NULL,
        client_id          VARCHAR NOT NULL,
        source_type        VARCHAR NOT NULL,
        action             VARCHAR NOT NULL,    -- PROPOSED | EDITED | SUBMITTED | APPROVED | REJECTED | ARCHIVED
        actor              VARCHAR NOT NULL,
        from_status        VARCHAR,
        to_status          VARCHAR,
        diff_summary       VARCHAR,             -- JSON object summarising changes
        ai_reasoning       VARCHAR,             -- agent's explanation for the change (if AI-driven)
        token_count        INTEGER,
        latency_ms         INTEGER,
        ts                 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        notes              VARCHAR
    )
    """,
    # ========================================================================
    # PHASE 15 — PIPELINE ARCHITECT — Bronze Catalog + Gold Schema + Instances.
    # ========================================================================
    # Architectural premise (revised after Phase 15.5 correction):
    #
    #   - Bronze schema = the AGREED MAPPING SPEC the client signed off on.
    #     Sourced from the Product Catalog xlsx (33 datasets, 943 fields).
    #     This is what the vendor sends; nothing is invented at Bronze.
    #     Anchor: CATALOG_ANCHOR by default; FHIR/X12/NCPDP/CMS/HEDIS as
    #     specialized overrides when the dataset uses one of those standards.
    #
    #   - Silver schema = ALWAYS Hub/Sat/Link (Data Vault 2.0) by default.
    #     Layer-level structural choice — independent of anchor. The agent
    #     proactively flags datasets where DV2 is overkill (tiny lookup
    #     tables, etc.) and proposes NORMALIZED as the alternative; HITL
    #     takes the call.
    #
    #   - Gold schema = GLOBAL canonical model, ONE per dataset. Three
    #     ways it can land in CONTROL.global_gold_schema_*:
    #       (a) AI_CONSTRUCTED — agent designs from Bronze + anchor
    #       (b) MANUAL         — operator hand-authors via column grid
    #       (c) IMPORTED       — uploaded DDL/YAML/FHIR profile/JSON Schema
    #     Once LIVE it's globally available; every client clones it on
    #     deploy with optional client_field_overrides.
    #
    #   - OnPrem push from Gold uses the Bronze catalog "Used by" matrix as
    #     the routing default; per-client overrides allowed.
    # ------------------------------------------------------------------------
    # Master Bronze dataset registry — 33 product-catalog datasets, one row
    # per dataset. Seeded by scripts/load_product_catalog.py from
    # data/sample/product_catalog.xlsx. THIS IS THE BRONZE LAYER (verbatim
    # from the vendor mapping spec). Renamed in Phase 15.5 from the
    # original (incorrect) `global_gold_catalog_datasets` label.
    f"""
    CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.global_bronze_catalog_datasets (
        dataset_id         VARCHAR PRIMARY KEY,
        dataset_code       VARCHAR NOT NULL,                -- snake_case: 'membership', 'member_claims'
        display_name       VARCHAR NOT NULL,                -- 'Membership', 'Member Claims'
        category           VARCHAR,                         -- Member | Claims | Pharmacy | Provider | etc.
        default_frequency  VARCHAR,                         -- Monthly | Weekly | Daily | —
        used_by            VARCHAR,                         -- JSON array of product codes: ['CC','E360','RBN','EC','ESV']
        total_fields       INTEGER NOT NULL DEFAULT 0,
        required_fields    INTEGER NOT NULL DEFAULT 0,
        optional_fields    INTEGER NOT NULL DEFAULT 0,
        default_anchor     VARCHAR NOT NULL DEFAULT 'CATALOG_ANCHOR',  -- CATALOG_ANCHOR | FHIR_R4_ANCHOR | X12_EDI_ANCHOR | NCPDP_D0_ANCHOR | CMS_ANCHOR | HEDIS_ANCHOR
        notes              VARCHAR,
        is_active          BOOLEAN NOT NULL DEFAULT TRUE,
        catalog_version    INTEGER NOT NULL DEFAULT 1,
        source_doc_uri     VARCHAR,                         -- 'data/sample/product_catalog.xlsx'
        registered_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        registered_by      VARCHAR
    )
    """,
    # Bronze field catalogue — 943 fields across 33 datasets. logical_type
    # inferred from description/example heuristics; operator can refine
    # via the UI. Every column here represents a verbatim vendor field.
    f"""
    CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.global_bronze_catalog_fields (
        field_id            VARCHAR PRIMARY KEY,
        dataset_id          VARCHAR NOT NULL,                -- FK to global_bronze_catalog_datasets.dataset_id
        dataset_code        VARCHAR NOT NULL,                -- denormalized for ergonomics
        field_order         INTEGER NOT NULL,                -- preserves catalog row order
        field_display_name  VARCHAR NOT NULL,                -- 'Member Card ID' (verbatim from xlsx)
        bronze_column_name  VARCHAR NOT NULL,                -- 'member_card_id' (snake_case for DDL)
        requirement         VARCHAR NOT NULL,                -- Required | Optional
        logical_type        VARCHAR NOT NULL DEFAULT 'TEXT', -- TEXT | INTEGER | DECIMAL | DATE | TIMESTAMP | BOOLEAN
        description         VARCHAR,
        additional_notes    VARCHAR,
        example             VARCHAR,
        is_pii              BOOLEAN NOT NULL DEFAULT FALSE,
        is_phi              BOOLEAN NOT NULL DEFAULT FALSE,
        is_business_key     BOOLEAN NOT NULL DEFAULT FALSE,
        catalog_version     INTEGER NOT NULL DEFAULT 1,
        registered_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    # ------------------------------------------------------------------------
    # GOLD SCHEMA REGISTRY — Phase 15.5. The CANONICAL model per dataset,
    # globally. Every client clones from here. Status walks
    # NONE → DRAFT → PENDING_REVIEW → LIVE → ARCHIVED.
    # ------------------------------------------------------------------------
    f"""
    CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.global_gold_schema_datasets (
        gold_dataset_id      VARCHAR PRIMARY KEY,
        dataset_code         VARCHAR NOT NULL,              -- FK to global_bronze_catalog_datasets.dataset_code
        gold_table_name      VARCHAR NOT NULL,              -- 'member', 'claim', 'pharmacy_claim'
        version              INTEGER NOT NULL DEFAULT 1,
        status               VARCHAR NOT NULL,              -- DRAFT | PENDING_REVIEW | LIVE | ARCHIVED
        gold_anchor          VARCHAR NOT NULL,              -- CATALOG_ANCHOR | FHIR_R4_ANCHOR | etc.
        source               VARCHAR NOT NULL,              -- AI_CONSTRUCTED | MANUAL | IMPORTED
        import_format        VARCHAR,                       -- DDL_SQL | DBT_YAML | FHIR_PROFILE_JSON | JSON_SCHEMA | SNOWFLAKE_DESCRIBE | NULL
        ai_proposal_json     VARCHAR,                       -- the agent's full proposal payload (when AI_CONSTRUCTED)
        ai_rationale         VARCHAR,                       -- why these columns / why this anchor
        ai_token_count       INTEGER,
        ai_latency_ms        INTEGER,
        notes                VARCHAR,
        created_by           VARCHAR NOT NULL,
        created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        submitted_at         TIMESTAMP,
        approved_by          VARCHAR,
        approved_at          TIMESTAMP,
        archived_at          TIMESTAMP
    )
    """,
    # Gold columns — one row per (gold_dataset_id, column_order). Holds the
    # canonical, business-friendly column shape. Client overrides apply on
    # top via client_field_overrides (already exists from Phase 15).
    f"""
    CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.global_gold_schema_fields (
        gold_field_id       VARCHAR PRIMARY KEY,
        gold_dataset_id     VARCHAR NOT NULL,              -- FK to global_gold_schema_datasets
        dataset_code        VARCHAR NOT NULL,              -- denormalized
        column_order        INTEGER NOT NULL,
        gold_column_name    VARCHAR NOT NULL,              -- 'member_id', 'full_name', 'effective_from'
        logical_type        VARCHAR NOT NULL DEFAULT 'TEXT',
        nullable            BOOLEAN NOT NULL DEFAULT TRUE,
        is_business_key     BOOLEAN NOT NULL DEFAULT FALSE,
        is_pii              BOOLEAN NOT NULL DEFAULT FALSE,
        is_phi              BOOLEAN NOT NULL DEFAULT FALSE,
        description         VARCHAR,
        anchor_reference    VARCHAR,                       -- e.g. 'FHIR Patient.id' or 'X12 834 INS-02'
        version             INTEGER NOT NULL DEFAULT 1,
        registered_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    # Bronze→Gold mapping rules — sparse; one row per (gold_field_id,
    # source). Holds the SQL expression that derives a Gold column from
    # one or more Bronze columns. Examples:
    #   full_name = CONCAT_WS(' ', first_name, middle_name, last_name)
    #   gender    = CASE gender_code WHEN 'M' THEN 'male' WHEN 'F' THEN 'female' ELSE 'unknown' END
    #   member_id = COALESCE(member_card_id, member_medicare_id, member_medicaid_id)
    f"""
    CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.bronze_to_gold_mappings (
        mapping_id              VARCHAR PRIMARY KEY,
        gold_field_id           VARCHAR NOT NULL,           -- FK to global_gold_schema_fields
        gold_dataset_id         VARCHAR NOT NULL,           -- denormalized
        gold_column_name        VARCHAR NOT NULL,           -- denormalized
        bronze_source_columns   VARCHAR NOT NULL,           -- JSON array: ['first_name','middle_name','last_name']
        transform_kind          VARCHAR NOT NULL,           -- DIRECT | CONCAT | COALESCE | LOOKUP | CAST | CASE | DERIVED
        transform_sql           VARCHAR NOT NULL,           -- the SQL expression (uses Bronze column names)
        rationale               VARCHAR,
        confidence              DECIMAL(3,2),               -- 0.0 - 1.0 — agent's confidence in the mapping
        created_by              VARCHAR NOT NULL,
        created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    # Gold schema audit log — every state transition + every edit.
    # Mirrors contract_design_audit_log + pipeline_instance_audit_log.
    f"""
    CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.gold_schema_audit_log (
        audit_id            VARCHAR PRIMARY KEY,
        gold_dataset_id     VARCHAR NOT NULL,
        dataset_code        VARCHAR NOT NULL,
        action              VARCHAR NOT NULL,               -- PROPOSED | EDITED | IMPORTED | SUBMITTED | APPROVED | REJECTED | ARCHIVED
        actor               VARCHAR NOT NULL,
        from_status         VARCHAR,
        to_status           VARCHAR,
        diff_summary        VARCHAR,                        -- JSON
        ai_reasoning        VARCHAR,
        token_count         INTEGER,
        latency_ms          INTEGER,
        ts                  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        notes               VARCHAR
    )
    """,
    # Silver pattern recommendations — one row per (dataset_code, version).
    # The agent flags whether DV2 (Hub/Sat/Link) is right for the dataset
    # OR whether NORMALIZED would be better (e.g., tiny lookup tables).
    # HITL gate: operator approves the recommendation; pipeline_templates
    # picks up the LIVE recommendation when generating Silver.
    f"""
    CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.silver_pattern_recommendations (
        recommendation_id     VARCHAR PRIMARY KEY,
        dataset_code          VARCHAR NOT NULL,
        recommended_pattern   VARCHAR NOT NULL,             -- HUB_SAT_LINK | NORMALIZED
        reasoning             VARCHAR NOT NULL,             -- agent's short explanation
        proposed_silver_shape VARCHAR,                      -- JSON: list of {{table_name, kind, columns}}
        bronze_field_count    INTEGER,
        business_key_count    INTEGER,
        domain_count          INTEGER,                      -- # of distinct domains in the Bronze fields
        is_overkill_flag      BOOLEAN NOT NULL DEFAULT FALSE,
        status                VARCHAR NOT NULL,             -- DRAFT | LIVE | ARCHIVED
        ai_token_count        INTEGER,
        ai_latency_ms         INTEGER,
        created_by            VARCHAR NOT NULL,
        created_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        approved_by           VARCHAR,
        approved_at           TIMESTAMP,
        archived_at           TIMESTAMP
    )
    """,
    # Pipeline templates — reusable Bronze→Silver→Gold blueprints anchored
    # to a dataset. One template per (dataset_code, version). LIVE template
    # is what the PipelineArchitectAgent uses as its anchor for new client
    # instances. Bronze pattern (FHIR / X12 / FLAT_FILE / API / NCPDP)
    # varies by client source; Silver+Gold patterns stay uniform.
    f"""
    CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.pipeline_templates (
        template_id            VARCHAR PRIMARY KEY,
        template_code          VARCHAR NOT NULL,            -- 'membership_v1'
        dataset_code           VARCHAR NOT NULL,            -- FK to datasets.dataset_code
        version                INTEGER NOT NULL DEFAULT 1,
        status                 VARCHAR NOT NULL,            -- DRAFT | LIVE | ARCHIVED
        silver_pattern         VARCHAR NOT NULL,            -- DV2 | STAR | NORMALIZED
        gold_pattern           VARCHAR NOT NULL,            -- UM_OPERATIONAL | RISK_OPERATIONAL | QUALITY | etc.
        gold_ddl_template      VARCHAR,                     -- canonical Gold CREATE TABLE (snake_case)
        silver_dbt_template    VARCHAR,                     -- dbt Silver model SQL template
        gold_dbt_template      VARCHAR,                     -- dbt Gold model SQL template
        default_dq_dimensions  VARCHAR,                     -- JSON: ['completeness','uniqueness','validity']
        default_dq_suite       VARCHAR,                     -- JSON GX expectation suite scaffold
        notes                  VARCHAR,
        created_at             TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        created_by             VARCHAR NOT NULL,
        activated_at           TIMESTAMP,
        archived_at            TIMESTAMP
    )
    """,
    # Client pipeline instances — one row per (client_id, dataset_code).
    # Materialized COPY of a template for a specific client. Clone is the
    # default; build-from-scratch is fallback. Status walks
    # DRAFT → PENDING_REVIEW → LIVE → PAUSED | ARCHIVED.
    f"""
    CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.client_pipeline_instances (
        instance_id            VARCHAR PRIMARY KEY,
        client_id              VARCHAR NOT NULL,
        dataset_code           VARCHAR NOT NULL,
        template_id            VARCHAR,                     -- FK to pipeline_templates; NULL on build-from-scratch
        cloned_from_instance   VARCHAR,                     -- FK to another client_pipeline_instances row (when cloned from peer)
        status                 VARCHAR NOT NULL,            -- DRAFT | PENDING_REVIEW | LIVE | PAUSED | ARCHIVED
        bronze_anchor          VARCHAR NOT NULL,            -- FHIR | X12 | NCPDP | FLAT_FILE | API
        bronze_schema          VARCHAR NOT NULL,            -- 'BRONZE_AETNA'
        bronze_table           VARCHAR NOT NULL,            -- 'raw_membership'
        silver_schema          VARCHAR NOT NULL,            -- 'SILVER_AETNA'
        silver_table           VARCHAR NOT NULL,            -- 'membership_clean'
        gold_schema            VARCHAR NOT NULL,            -- 'GOLD_AETNA'
        gold_table             VARCHAR NOT NULL,            -- 'membership'
        schedule_cron          VARCHAR,                     -- '0 4 * * *'
        contract_id            VARCHAR,                     -- FK to source_schema_contracts
        gx_suite_id            VARCHAR,                     -- FK to dq_suites (LIVE)
        dag_uri                VARCHAR,                     -- 'dags/aetna_membership.py'
        dbt_models_uri         VARCHAR,                     -- 'dbt/models/silver/aetna_membership/'
        overrides_json         VARCHAR,                     -- JSON snapshot of all overrides at deploy time
        deviation_count        INTEGER NOT NULL DEFAULT 0,  -- count of overrides at deploy time
        ai_proposal_json       VARCHAR,                     -- the agent's full proposal payload
        ai_reasoning           VARCHAR,                     -- agent's clone-vs-build explanation
        ai_token_count         INTEGER,
        ai_latency_ms          INTEGER,
        created_at             TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        created_by             VARCHAR NOT NULL,
        submitted_at           TIMESTAMP,
        approved_at            TIMESTAMP,
        approved_by            VARCHAR,
        deployed_at            TIMESTAMP,
        deployed_by            VARCHAR,
        paused_at              TIMESTAMP,
        archived_at            TIMESTAMP,
        notes                  VARCHAR
    )
    """,
    # Client-level overrides on the global Gold catalog. Sparse — only fields
    # the client deviates on. Allows "Aetna's Membership has aetna_segment_id
    # extra column" or "CareSource relaxes nullability on member_phone".
    # Diff'd at HITL review so reviewers see exactly what's standard vs custom.
    f"""
    CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.client_field_overrides (
        override_id        VARCHAR PRIMARY KEY,
        instance_id        VARCHAR NOT NULL,                -- FK to client_pipeline_instances
        client_id          VARCHAR NOT NULL,
        dataset_code       VARCHAR NOT NULL,
        gold_column_name   VARCHAR NOT NULL,                -- field touched by override (or new field name)
        override_kind      VARCHAR NOT NULL,                -- ADD_FIELD | RELAX_NULLABLE | TIGHTEN_NULLABLE | RENAME | TYPE_CHANGE | EXCLUDE
        original_value     VARCHAR,                         -- JSON snapshot from global catalog (NULL for ADD_FIELD)
        override_value     VARCHAR,                         -- JSON of the override
        rationale          VARCHAR NOT NULL,
        created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        created_by         VARCHAR NOT NULL
    )
    """,
    # OnPrem routing rules — driven by the product catalog "Used by" matrix.
    # Default rules are seeded automatically; client overrides take precedence.
    f"""
    CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.onprem_routing_rules (
        rule_id              VARCHAR PRIMARY KEY,
        dataset_code         VARCHAR NOT NULL,
        downstream_product   VARCHAR NOT NULL,              -- CC | E360 | RBN | EC | ESV | RAE
        target_system        VARCHAR NOT NULL,              -- postgres | sqlserver | snowflake_share | sftp
        target_uri           VARCHAR,                       -- connection ref
        is_default           BOOLEAN NOT NULL DEFAULT TRUE, -- TRUE = seeded from catalog
        is_active            BOOLEAN NOT NULL DEFAULT TRUE,
        notes                VARCHAR,
        created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        created_by           VARCHAR
    )
    """,
    # Per-client overrides on routing — disable a default route, or change target.
    f"""
    CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.client_routing_overrides (
        override_id          VARCHAR PRIMARY KEY,
        client_id            VARCHAR NOT NULL,
        dataset_code         VARCHAR NOT NULL,
        downstream_product   VARCHAR NOT NULL,
        action               VARCHAR NOT NULL,              -- ENABLE | DISABLE | OVERRIDE_TARGET
        target_system        VARCHAR,
        target_uri           VARCHAR,
        rationale            VARCHAR NOT NULL,
        created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        created_by           VARCHAR NOT NULL
    )
    """,
    # Pipeline instance audit log — every state transition + every override.
    # Same shape as contract_design_audit_log / policy_audit_log.
    f"""
    CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.pipeline_instance_audit_log (
        audit_id           VARCHAR PRIMARY KEY,
        instance_id        VARCHAR NOT NULL,
        client_id          VARCHAR NOT NULL,
        dataset_code       VARCHAR NOT NULL,
        action             VARCHAR NOT NULL,                -- PROPOSED | CLONED | OVERRIDDEN | SUBMITTED | APPROVED | DEPLOYED | PAUSED | ARCHIVED
        actor              VARCHAR NOT NULL,
        from_status        VARCHAR,
        to_status          VARCHAR,
        diff_summary       VARCHAR,                         -- JSON
        ai_reasoning       VARCHAR,
        token_count        INTEGER,
        latency_ms         INTEGER,
        ts                 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        notes              VARCHAR
    )
    """,
    # ========================================================================
    # PHASE 15.8 — INDEPENDENT SILVER AUTHORING (Silver-first, Silver-stop ready)
    # ========================================================================
    # Silver gets first-class status: own designer agent, own DRAFT/LIVE/ARCHIVED
    # lifecycle, own three authoring modes (AI / Manual / Upload). Gold reads
    # from Silver via silver_to_gold_mappings, NOT directly from Bronze. This
    # lets operators ship "Silver-stop" pipelines (clean+integrate without
    # consumption-layer push) and approve Gold later when downstream is ready.
    #
    # Mapping chain becomes:  Bronze --(bronze_to_silver_mappings)--> Silver
    #                         Silver --(silver_to_gold_mappings)--> Gold
    # ------------------------------------------------------------------------
    # Silver schema header — one row per (dataset_code, version).
    f"""
    CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.global_silver_schema_datasets (
        silver_dataset_id    VARCHAR PRIMARY KEY,
        dataset_code         VARCHAR NOT NULL,             -- FK to global_bronze_catalog_datasets.dataset_code
        silver_pattern       VARCHAR NOT NULL,             -- HUB_SAT_LINK | NORMALIZED
        version              INTEGER NOT NULL DEFAULT 1,
        status               VARCHAR NOT NULL,             -- DRAFT | PENDING_REVIEW | LIVE | ARCHIVED
        silver_anchor        VARCHAR NOT NULL,             -- CATALOG_ANCHOR | FHIR_R4_ANCHOR | X12_EDI_ANCHOR | NCPDP_D0_ANCHOR | CMS_ANCHOR | HEDIS_ANCHOR
        source               VARCHAR NOT NULL,             -- AI_CONSTRUCT | MANUAL | IMPORT
        import_format        VARCHAR,                      -- DDL_SQL | DBT_YAML | DBT_PROJECT | JSON_SCHEMA | NULL
        is_overkill_flag     BOOLEAN NOT NULL DEFAULT FALSE,
        ai_proposal_json     VARCHAR,
        ai_rationale         VARCHAR,
        ai_token_count       INTEGER,
        ai_latency_ms        INTEGER,
        notes                VARCHAR,
        created_by           VARCHAR NOT NULL,
        created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        submitted_at         TIMESTAMP,
        approved_by          VARCHAR,
        approved_at          TIMESTAMP,
        archived_at          TIMESTAMP
    )
    """,
    # Silver tables — one row per Hub / Sat / Link / Normalized table inside
    # a Silver schema. parent_silver_table_id points Sats at their Hub.
    # linked_hub_ids_json (JSON array of silver_table_ids) names the Hubs a
    # LINK connects.
    f"""
    CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.global_silver_schema_tables (
        silver_table_id          VARCHAR PRIMARY KEY,
        silver_dataset_id        VARCHAR NOT NULL,         -- FK to global_silver_schema_datasets
        dataset_code             VARCHAR NOT NULL,         -- denormalized
        table_name               VARCHAR NOT NULL,         -- HUB_MEMBER, SAT_MEMBER_DEMOGRAPHICS, LINK_MEMBER_PROVIDER, member_pcp_silver
        table_kind               VARCHAR NOT NULL,         -- HUB | SAT | LINK | NORMALIZED
        parent_silver_table_id   VARCHAR,                  -- FK to itself; SAT.parent = HUB
        business_keys_json       VARCHAR,                  -- JSON array of column names (HUB only)
        linked_hub_ids_json      VARCHAR,                  -- JSON array of silver_table_ids (LINK only)
        table_order              INTEGER NOT NULL DEFAULT 0,
        description              VARCHAR,
        created_at               TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    # Silver columns — one row per column in any Silver table.
    f"""
    CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.global_silver_schema_columns (
        silver_column_id     VARCHAR PRIMARY KEY,
        silver_table_id      VARCHAR NOT NULL,             -- FK to global_silver_schema_tables
        silver_dataset_id    VARCHAR NOT NULL,             -- denormalized
        column_order         INTEGER NOT NULL,
        column_name          VARCHAR NOT NULL,             -- e.g. 'member_id', 'hash_key', 'first_name'
        logical_type         VARCHAR NOT NULL DEFAULT 'TEXT',
        nullable             BOOLEAN NOT NULL DEFAULT TRUE,
        is_business_key      BOOLEAN NOT NULL DEFAULT FALSE,
        is_hash_key          BOOLEAN NOT NULL DEFAULT FALSE,    -- TRUE for the SHA-256 PK column on Hubs
        is_hash_diff         BOOLEAN NOT NULL DEFAULT FALSE,    -- TRUE for change-detection col on Sats
        is_pii               BOOLEAN NOT NULL DEFAULT FALSE,
        is_phi               BOOLEAN NOT NULL DEFAULT FALSE,
        description          VARCHAR,
        registered_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    # Bronze→Silver mapping rules — one row per (silver_column_id). Holds
    # the SQL expression that derives a Silver column from one or more
    # Bronze columns.
    f"""
    CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.bronze_to_silver_mappings (
        mapping_id              VARCHAR PRIMARY KEY,
        silver_column_id        VARCHAR NOT NULL,          -- FK to global_silver_schema_columns
        silver_dataset_id       VARCHAR NOT NULL,          -- denormalized
        silver_table_name       VARCHAR NOT NULL,          -- denormalized
        silver_column_name      VARCHAR NOT NULL,          -- denormalized
        bronze_source_columns   VARCHAR NOT NULL,          -- JSON array of Bronze column names
        transform_kind          VARCHAR NOT NULL,          -- DIRECT | CONCAT | COALESCE | LOOKUP | CAST | CASE | DERIVED | HASH
        transform_sql           VARCHAR NOT NULL,          -- SQL expression using Bronze column names
        rationale               VARCHAR,
        confidence              DECIMAL(3,2),
        created_by              VARCHAR NOT NULL,
        created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    # Silver schema audit log — every state transition, edit, revert.
    f"""
    CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.silver_schema_audit_log (
        audit_id            VARCHAR PRIMARY KEY,
        silver_dataset_id   VARCHAR NOT NULL,
        dataset_code        VARCHAR NOT NULL,
        action              VARCHAR NOT NULL,              -- PROPOSED | EDITED | IMPORTED | SUBMITTED | APPROVED | REJECTED | ARCHIVED | REVERTED
        actor               VARCHAR NOT NULL,
        from_status         VARCHAR,
        to_status           VARCHAR,
        diff_summary        VARCHAR,                       -- JSON
        ai_reasoning        VARCHAR,
        token_count         INTEGER,
        latency_ms          INTEGER,
        ts                  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        notes               VARCHAR
    )
    """,
    # Silver→Gold mappings — replace the Phase-15.6 bronze_to_gold_mappings
    # for the new flow. Gold columns derive from Silver tables/columns
    # (often joining multiple Sats around their Hub). silver_source_refs
    # is a JSON array of {silver_table_id, silver_column_id, alias} objects.
    f"""
    CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.silver_to_gold_mappings (
        mapping_id              VARCHAR PRIMARY KEY,
        gold_field_id           VARCHAR NOT NULL,          -- FK to global_gold_schema_fields
        gold_dataset_id         VARCHAR NOT NULL,          -- denormalized
        gold_column_name        VARCHAR NOT NULL,          -- denormalized
        silver_source_refs      VARCHAR NOT NULL,          -- JSON array of {{silver_table, silver_column}} refs
        transform_kind          VARCHAR NOT NULL,          -- DIRECT | CONCAT | COALESCE | LOOKUP | CAST | CASE | DERIVED | JOIN
        transform_sql           VARCHAR NOT NULL,          -- SQL expression using Silver table.column refs
        rationale               VARCHAR,
        confidence              DECIMAL(3,2),
        created_by              VARCHAR NOT NULL,
        created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    # ========================================================================
    # PHASE 15.7 — OVERFLOW SAFETY + GREENFIELD INGESTION
    # ========================================================================
    # Two production-grade safety mechanisms:
    #
    # 1. _extra column on every Bronze table — captures any
    #    incoming column NOT in the agreed Bronze contract as JSON. Silver
    #    and Gold transforms IGNORE the overflow column, so unexpected
    #    columns NEVER leak downstream to operational databases without
    #    an explicit handshake. This table records each unexpected
    #    column observed for HITL review.
    #
    # 2. Greenfield datasets — when a client sends data for a dataset
    #    NOT in the Bronze Master Catalog, the AI agent profiles the
    #    sample file and proposes a brand-new Bronze + Gold pair. Two
    #    stage HITL approval (Bronze first, then Gold) before the new
    #    dataset is promoted to the global registry.
    # ------------------------------------------------------------------------
    # Overflow log — append-only. One row per (client, dataset, column_name).
    # Status walks PENDING_REVIEW -> APPROVED_TO_INCORPORATE -> RESOLVED
    # (Bronze contract bumped + Gold redesigned).
    f"""
    CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.bronze_overflow_log (
        overflow_id              VARCHAR PRIMARY KEY,
        client_id                VARCHAR NOT NULL,
        dataset_code             VARCHAR NOT NULL,
        column_name              VARCHAR NOT NULL,
        first_seen_batch_id      VARCHAR,
        first_seen_source_file   VARCHAR,
        first_seen_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        last_seen_at             TIMESTAMP,
        occurrence_count         INTEGER NOT NULL DEFAULT 1,
        sample_values            VARCHAR,
        inferred_logical_type    VARCHAR,
        status                   VARCHAR NOT NULL,
        resolution_notes         VARCHAR,
        resolved_by              VARCHAR,
        resolved_at              TIMESTAMP,
        promoted_to_bronze_field VARCHAR
    )
    """,
    # Greenfield dataset proposals — one row per AI-proposed brand-new dataset.
    # Status walks DRAFT -> PENDING_REVIEW -> APPROVED_PROMOTED -> triggers
    # Gold Schema Designer.
    f"""
    CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.greenfield_dataset_proposals (
        greenfield_id            VARCHAR PRIMARY KEY,
        proposed_dataset_code    VARCHAR NOT NULL,
        proposed_display_name    VARCHAR NOT NULL,
        proposed_category        VARCHAR,
        proposed_default_anchor  VARCHAR NOT NULL DEFAULT 'CATALOG_ANCHOR',
        client_id                VARCHAR,
        source_sample_uri        VARCHAR,
        sample_file_name         VARCHAR,
        proposed_bronze_columns  VARCHAR NOT NULL,
        proposed_field_count     INTEGER,
        proposed_required_count  INTEGER,
        proposed_optional_count  INTEGER,
        ai_token_count           INTEGER,
        ai_latency_ms            INTEGER,
        ai_rationale             VARCHAR,
        status                   VARCHAR NOT NULL,
        promoted_dataset_id      VARCHAR,
        notes                    VARCHAR,
        created_by               VARCHAR NOT NULL,
        created_at               TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        submitted_at             TIMESTAMP,
        approved_by              VARCHAR,
        approved_at              TIMESTAMP,
        rejected_at              TIMESTAMP
    )
    """,
    # ------------------------------------------------------------------------
    # Phase 18 — dataset_pipeline_runs.  One row per "▶ Run now" execution
    # (or per Airflow DAG run when we wire that up later).  Captures the
    # status of each step in the Bronze→Silver→Gold→Push chain so the
    # Pipeline Architect's run-history drill-down can surface failures
    # without scraping logs.  Keyed by run_id; instance_id is the FK back
    # to client_pipeline_instances.
    # ------------------------------------------------------------------------
    f"""
    CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.dataset_pipeline_runs (
        run_id              VARCHAR PRIMARY KEY,
        instance_id         VARCHAR NOT NULL,
        client_id           VARCHAR NOT NULL,
        dataset_code        VARCHAR NOT NULL,
        status              VARCHAR NOT NULL,        -- RUNNING | SUCCESS | FAILED
        triggered_by        VARCHAR NOT NULL,        -- 'ui:run_now' | 'airflow' | 'cli'
        started_at          TIMESTAMP NOT NULL,
        ended_at            TIMESTAMP,
        duration_ms         INTEGER,
        rows_bronze         INTEGER,
        rows_silver         INTEGER,
        rows_gold           INTEGER,
        error_task          VARCHAR,                 -- which step failed (if any)
        error_message       VARCHAR,
        task_log_json       VARCHAR                  -- JSON array of per-task results
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
        # Phase 19.1 — factory-pattern DQ suites.  Suites are now scoped to
        # (dataset_code, layer) — universal across clients.  Legacy per-
        # client suites still work; client_id is now nullable (enforced in
        # code).  AI control fields preserved.
        f"ALTER TABLE {CONTROL_SCHEMA}.dq_suites ADD COLUMN dataset_code VARCHAR",
        f"ALTER TABLE {CONTROL_SCHEMA}.dq_suites ADD COLUMN layer VARCHAR",  # BRONZE | SILVER | GOLD
        f"ALTER TABLE {CONTROL_SCHEMA}.dq_suites ADD COLUMN ai_proposal_json VARCHAR",
        f"ALTER TABLE {CONTROL_SCHEMA}.dq_suites ADD COLUMN ai_rationale VARCHAR",
        f"ALTER TABLE {CONTROL_SCHEMA}.dq_suites ADD COLUMN ai_token_count INTEGER",
        f"ALTER TABLE {CONTROL_SCHEMA}.dq_suites ADD COLUMN ai_latency_ms INTEGER",
        f"ALTER TABLE {CONTROL_SCHEMA}.dq_suites ADD COLUMN temperature FLOAT",
        f"ALTER TABLE {CONTROL_SCHEMA}.dq_suites ADD COLUMN grounding_mode VARCHAR",  # off | rag | strict
        # Phase 19.1 — relax client_id NOT NULL.  Factory-pattern suites
        # are universal (no client_id); legacy per-client suites still
        # populate client_id, so this is non-breaking.
        f"ALTER TABLE {CONTROL_SCHEMA}.dq_suites ALTER COLUMN client_id DROP NOT NULL",
        f"ALTER TABLE {CONTROL_SCHEMA}.pipeline_checkpoints ADD COLUMN client_id VARCHAR",
        f"ALTER TABLE {CONTROL_SCHEMA}.pipeline_checkpoints ADD COLUMN source_type VARCHAR",
        f"ALTER TABLE {CONTROL_SCHEMA}.gx_validation_results ADD COLUMN client_id VARCHAR",
        f"ALTER TABLE {CONTROL_SCHEMA}.gx_validation_results ADD COLUMN source_type VARCHAR",
        f"ALTER TABLE {CONTROL_SCHEMA}.gx_validation_results ADD COLUMN dq_dimension VARCHAR",
        # Phase 12.5 — per-source-type policy granularity. NULL = pipeline-wide.
        f"ALTER TABLE {CONTROL_SCHEMA}.pipeline_control_policies ADD COLUMN source_type VARCHAR",
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
                f"illegal transition {current} → {t.to_state} for {t.pipeline_id!r}/{client_id!r}"
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
