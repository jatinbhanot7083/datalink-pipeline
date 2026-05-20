"""Phase 23.5 — Canonical Extension Proposals.

The feedback loop between Client Onboarding (BIMA) and the canonical
Bronze catalog:

  1. AI sees an unmapped client field in Client Onboarding.
  2. Operator clicks "↗ Propose to canonical" — writes a row to
     ``bronze_canonical_extension_proposals`` with status PENDING_REVIEW.
  3. DBA (in DMD's DBA mode) reviews + approves.
  4. On approve, the canonical row lands in
     ``global_bronze_catalog_fields`` and the extension is marked PROMOTED.

Keeps the canonical catalog clean (only approved fields), preserves
forensic-grade audit trail of WHO promoted WHAT WHEN and WHY.
"""

from __future__ import annotations

import sys

from datalink.adapters.factory import build_adapters
from datalink.config.loader import load_settings
from datalink.quality.control import CONTROL_SCHEMA


def main() -> int:
    wh = build_adapters(load_settings(env="dev")).warehouse

    print("Phase 23.5 — bronze_canonical_extension_proposals")
    print("=" * 60)

    wh.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.bronze_canonical_extension_proposals (
            extension_id            VARCHAR(36)   NOT NULL,
            dataset_code            VARCHAR(64)   NOT NULL,
            status                  VARCHAR(32)   NOT NULL,    -- DRAFT / PENDING_REVIEW / APPROVED / REJECTED / PROMOTED

            -- The proposed canonical field shape
            proposed_display_name   VARCHAR(256)  NOT NULL,
            proposed_column_name    VARCHAR(64)   NOT NULL,
            proposed_logical_type   VARCHAR(64),
            proposed_requirement    VARCHAR(32)   DEFAULT 'Optional',  -- Required / Recommended / Optional / Conditional
            proposed_description    VARCHAR(2000),
            proposed_is_pii         BOOLEAN       DEFAULT FALSE,
            proposed_is_phi         BOOLEAN       DEFAULT FALSE,
            proposed_used_by        VARIANT,                          -- inferred product lines

            -- Origin — which (client, proposal) surfaced this
            origin_proposal_id      VARCHAR(36),
            origin_client_id        VARCHAR(64),
            origin_field_name       VARCHAR(256),
            origin_sample_values    VARIANT,
            ai_rationale            VARCHAR(2000),
            ai_confidence           FLOAT,
            ai_tokens               NUMBER,
            ai_latency_ms           NUMBER,

            -- Lifecycle
            created_by              VARCHAR(128)  NOT NULL,
            created_at              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            submitted_at            TIMESTAMP_NTZ,
            reviewed_by             VARCHAR(128),
            reviewed_at             TIMESTAMP_NTZ,
            reviewer_notes          VARCHAR(4000),

            -- On PROMOTED — points at the newly-created canonical row
            promoted_to_field_id    VARCHAR(36),
            promoted_at             TIMESTAMP_NTZ,

            PRIMARY KEY (extension_id)
        )
        """
    )
    print("OK bronze_canonical_extension_proposals created/verified")

    rows = list(
        wh.query(
            "SELECT COUNT(*) c FROM DATALINK_DEV.INFORMATION_SCHEMA.COLUMNS "
            "WHERE table_schema = %s AND table_name = %s",
            ("CONTROL", "BRONZE_CANONICAL_EXTENSION_PROPOSALS"),
        )
    )
    print(f"  columns: {rows[0]['c']}")
    print("Phase 23.5 schema applied.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
