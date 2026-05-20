"""Phase 23 — Bronze Inbound Mapping Agent (BIMA) schema.

Adds five CONTROL tables that back the Client Onboarding · AI Mapping flow:

  * ``bronze_inbound_mapping_proposals`` — one row per AI proposal,
    lifecycle DRAFT → PENDING_REVIEW → APPROVED → LIVE → ARCHIVED.
  * ``bronze_inbound_field_proposals`` — per-field rows inside a proposal
    (client_field_name, canonical_field, transform_sql, confidence,
    rationale).  Editable by reviewers before approval.
  * ``bronze_inbound_mappings`` — the **LIVE** mapping per (client_id,
    dataset_code).  Reads by the Bronze ingest task at run time.  At
  most one LIVE per (client, dataset); older versions ARCHIVED.
  * ``bronze_inbound_field_mappings`` — child rows of bronze_inbound_mappings
    (LIVE per-field mapping rows applied by the ingest task).
  * ``bronze_inbound_mapping_audit`` — every state transition (draft,
    propose, edit, approve, reject, archive) with actor + diff + notes.

Idempotent.  Safe to re-run.
"""

from __future__ import annotations

import sys

from datalink.adapters.factory import build_adapters
from datalink.config.loader import load_settings
from datalink.quality.control import CONTROL_SCHEMA


def main() -> int:
    wh = build_adapters(load_settings(env="dev")).warehouse

    print("Phase 23 — Bronze Inbound Mapping Agent schema")
    print("=" * 60)

    print("Step 1/6 — bronze_inbound_mapping_proposals (lifecycle head)")
    wh.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.bronze_inbound_mapping_proposals (
            proposal_id          VARCHAR(36)   NOT NULL,
            client_id            VARCHAR(64)   NOT NULL,
            dataset_code         VARCHAR(64)   NOT NULL,
            version              NUMBER        NOT NULL DEFAULT 1,
            status               VARCHAR(32)   NOT NULL,        -- DRAFT / PENDING_REVIEW / APPROVED / LIVE / ARCHIVED / REJECTED
            source_format        VARCHAR(32),                   -- csv / xlsx / json / fixed_width / docx / pdf
            source_filename      VARCHAR(512),
            source_doc_uri       VARCHAR(1024),                 -- where the file was stored (s3, blob, local)
            source_sample_rows   VARIANT,                       -- redacted sample for review surface
            source_schema_json   VARIANT,                       -- inferred shape (columns, types, hierarchy)
            ai_proposal_json     VARIANT,                       -- raw LLM JSON output for forensics
            ai_rationale         VARCHAR(8000),                 -- human-readable narrative
            ai_token_count       NUMBER,
            ai_latency_ms        NUMBER,
            ai_model             VARCHAR(64),                   -- e.g. claude-haiku-4-5-20251001
            ai_grounding_mode    VARCHAR(32),                   -- off / rag / strict
            drift_summary        VARIANT,                       -- diff vs prior LIVE mapping (if any)
            created_by           VARCHAR(128)  NOT NULL,
            created_at           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            submitted_at         TIMESTAMP_NTZ,
            submitted_by         VARCHAR(128),
            reviewed_at          TIMESTAMP_NTZ,
            reviewed_by          VARCHAR(128),
            reviewer_notes       VARCHAR(4000),                 -- HITL: required on reject; recorded on approve
            approved_at          TIMESTAMP_NTZ,
            approved_by          VARCHAR(128),
            archived_at          TIMESTAMP_NTZ,
            PRIMARY KEY (proposal_id)
        )
        """
    )

    print("Step 2/6 — bronze_inbound_field_proposals (per-field rows in a proposal)")
    wh.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.bronze_inbound_field_proposals (
            field_proposal_id    VARCHAR(36)   NOT NULL,
            proposal_id          VARCHAR(36)   NOT NULL,
            client_field_name    VARCHAR(256)  NOT NULL,        -- source-side name
            client_field_position NUMBER,                       -- for fixed-width / column ordinal
            client_field_path    VARCHAR(512),                  -- for nested JSON (dot path)
            canonical_field_code VARCHAR(64),                   -- target bronze_column_name; NULL = unmatched
            canonical_dataset_code VARCHAR(64),
            transform_sql        VARCHAR(2000),                 -- per-field transform (NULL = direct copy)
            transform_kind       VARCHAR(32),                   -- direct / cast / split / concat / lookup / regex
            confidence           FLOAT,                         -- 0.0–1.0 from the LLM
            rationale            VARCHAR(2000),                 -- one-line justification
            is_required          BOOLEAN DEFAULT FALSE,         -- canonical field requires this?
            review_status        VARCHAR(32) DEFAULT 'PENDING', -- PENDING / ACCEPTED / EDITED / REJECTED
            reviewer_override    VARIANT,                       -- if reviewer edited the AI proposal
            created_at           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            PRIMARY KEY (field_proposal_id)
        )
        """
    )

    print("Step 3/6 — bronze_inbound_mappings (LIVE — applied by ingest task)")
    wh.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.bronze_inbound_mappings (
            mapping_id           VARCHAR(36)   NOT NULL,
            client_id            VARCHAR(64)   NOT NULL,
            dataset_code         VARCHAR(64)   NOT NULL,
            version              NUMBER        NOT NULL,
            status               VARCHAR(32)   NOT NULL,        -- LIVE / ARCHIVED
            source_format        VARCHAR(32),
            promoted_from        VARCHAR(36),                   -- bronze_inbound_mapping_proposals.proposal_id
            promoted_by          VARCHAR(128),
            promoted_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            archived_at          TIMESTAMP_NTZ,
            PRIMARY KEY (mapping_id)
        )
        """
    )

    print("Step 4/6 — bronze_inbound_field_mappings (LIVE per-field rows)")
    wh.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.bronze_inbound_field_mappings (
            field_mapping_id     VARCHAR(36)   NOT NULL,
            mapping_id           VARCHAR(36)   NOT NULL,
            client_field_name    VARCHAR(256)  NOT NULL,
            client_field_position NUMBER,
            client_field_path    VARCHAR(512),
            canonical_field_code VARCHAR(64),
            canonical_dataset_code VARCHAR(64),
            transform_sql        VARCHAR(2000),
            transform_kind       VARCHAR(32),
            PRIMARY KEY (field_mapping_id)
        )
        """
    )

    print("Step 5/6 — bronze_inbound_mapping_audit (full state-change history)")
    wh.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.bronze_inbound_mapping_audit (
            audit_id             VARCHAR(36)   NOT NULL,
            proposal_id          VARCHAR(36),
            mapping_id           VARCHAR(36),
            client_id            VARCHAR(64),
            dataset_code         VARCHAR(64),
            action               VARCHAR(64)   NOT NULL,        -- propose / edit_field / submit / approve / reject / promote / archive / drift_detected
            actor                VARCHAR(128)  NOT NULL,
            from_status          VARCHAR(32),
            to_status            VARCHAR(32),
            diff_summary         VARIANT,
            notes                VARCHAR(4000),
            ts                   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            PRIMARY KEY (audit_id)
        )
        """
    )

    print("Step 6/6 — verification")
    for tbl in (
        "bronze_inbound_mapping_proposals",
        "bronze_inbound_field_proposals",
        "bronze_inbound_mappings",
        "bronze_inbound_field_mappings",
        "bronze_inbound_mapping_audit",
    ):
        rows = list(
            wh.query(
                "SELECT COUNT(*) c FROM DATALINK_DEV.INFORMATION_SCHEMA.COLUMNS "
                "WHERE table_schema = %s AND table_name = %s",
                ("CONTROL", tbl.upper()),
            )
        )
        n = int(rows[0]["c"]) if rows else 0
        print(f"  {'OK' if n > 0 else 'FAIL'} {tbl:42s} columns={n}")

    print()
    print("Phase 23 schema applied.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
