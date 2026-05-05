"""Phase 16.1 — Agent Proposals (saved-proposal infrastructure).

Creates 3 tables in CONTROL schema:

  1. agent_proposals             — every AI proposal, every version, every status
  2. proposal_tags               — controlled vocabulary of tags (governance)
  3. proposal_tag_assignments    — junction table (multi-tag per proposal, audited)

Plus seeds the tag catalog with system-defined tags.

Why a tag catalog (not free-text array): governance. The "certified-2026-Q2"
tag should only be applicable by data-stewards. The "rollback-candidate" tag
should be used in dashboards. Free-text strings give no governance, no color,
no role-gating, no aggregation.

Run with:
    DL_ENV=dev python3 scripts/migrate_phase16_agent_proposals.py
"""

from __future__ import annotations

import os
import sys
import uuid
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

# Auto-load .env so the script works without `set -a && source .env` (Gotcha #1).
_ENV = ROOT / ".env"
if _ENV.exists():
    for raw in _ENV.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        os.environ.setdefault(k.strip(), v.strip().strip("'").strip('"'))
os.environ.setdefault("DL_ENV", "dev")

from datalink.adapters.factory import build_adapters  # noqa: E402
from datalink.config.loader import load_settings  # noqa: E402
from datalink.quality.control import CONTROL_SCHEMA  # noqa: E402

# ---------------------------------------------------------------------------
# Table DDLs
# ---------------------------------------------------------------------------

# 1. agent_proposals — the heart of saved-proposal infrastructure.
#    One row per (scope, version) tuple. Re-proposing creates a new version
#    that supersedes the prior one (which gets ARCHIVED unless pinned).
DDL_AGENT_PROPOSALS = f"""
CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.agent_proposals (
    proposal_id              VARCHAR(36) NOT NULL,                 -- UUID
    agent_type               VARCHAR(64) NOT NULL,                  -- 'silver_designer' | 'gold_designer' | 'pipeline_architect' | 'dq_proposer' | …
    scope_type               VARCHAR(32) NOT NULL,                  -- 'dataset' | 'client_dataset' | 'suite' | 'mapping_session'
    scope_key                VARCHAR(256) NOT NULL,                 -- e.g. 'membership' or 'aetna:membership'
    version                  INTEGER NOT NULL,                      -- v1, v2, … (per scope)
    supersedes_proposal_id   VARCHAR(36),                           -- prior proposal in same scope

    -- Cross-client cloning lineage (Wave 2 hookup)
    parent_client_id         VARCHAR(64),                           -- if cloned from another client's proposal
    cloned_from_proposal_id  VARCHAR(36),                           -- the specific source proposal_id

    -- Lifecycle
    status                   VARCHAR(16) NOT NULL,                  -- 'DRAFT' | 'APPROVED' | 'REJECTED' | 'ARCHIVED'
    pinned                   BOOLEAN NOT NULL DEFAULT FALSE,        -- 'never archive this' (e.g. golden reference version)

    -- Payload
    proposal_payload         VARIANT NOT NULL,                      -- full agent output JSON
    agent_input              VARIANT NOT NULL,                      -- prompt + RAG context (for replay)

    -- Telemetry
    model_id                 VARCHAR(64) NOT NULL,                  -- 'claude-haiku-4.5'
    prompt_tokens            INTEGER NOT NULL,
    completion_tokens        INTEGER NOT NULL,
    total_tokens             INTEGER NOT NULL,
    estimated_cost_usd       FLOAT NOT NULL,
    latency_ms               INTEGER NOT NULL,

    -- Linked artifact (set on APPROVED)
    linked_artifact_type     VARCHAR(64),                           -- 'pipeline_instance' | 'silver_schema' | 'gold_schema' | 'dq_suite'
    linked_artifact_id       VARCHAR(64),                           -- the UUID/PK it became

    -- Audit
    created_at               TIMESTAMP_NTZ NOT NULL,
    created_by               VARCHAR(128) NOT NULL,                 -- 'ui:aetna' | 'api:airflow' | 'cli:jatin'
    approved_at              TIMESTAMP_NTZ,
    approved_by              VARCHAR(128),
    rejected_at              TIMESTAMP_NTZ,
    rejected_by              VARCHAR(128),
    archived_at              TIMESTAMP_NTZ,
    archived_by              VARCHAR(128),

    notes                    VARCHAR,

    PRIMARY KEY (proposal_id)
)
"""

# Snowflake clustering — micro-partition pruning for the scope-lookup pattern
# (the dominant query: "find latest active proposal for scope X").
CLUSTER_AGENT_PROPOSALS = (
    f"ALTER TABLE {CONTROL_SCHEMA}.agent_proposals CLUSTER BY (scope_type, scope_key, version)"
)

# 2. proposal_tags — controlled vocabulary, governance-friendly.
DDL_PROPOSAL_TAGS = f"""
CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.proposal_tags (
    tag_id                   VARCHAR(36) NOT NULL,                  -- UUID
    tag_name                 VARCHAR(64) NOT NULL,                  -- 'certified-2026-Q2'
    tag_category             VARCHAR(32) NOT NULL,                  -- 'certification' | 'state' | 'governance' | 'custom'
    color_hex                VARCHAR(7) DEFAULT '#6b7280',          -- UI badge color
    description              VARCHAR,
    requires_role            VARCHAR(64),                           -- e.g. 'data-steward' to apply 'certified-*'
    is_system                BOOLEAN NOT NULL DEFAULT FALSE,        -- system tags can't be deleted
    created_at               TIMESTAMP_NTZ NOT NULL,
    created_by               VARCHAR(128) NOT NULL,
    PRIMARY KEY (tag_id),
    CONSTRAINT uq_tag_name UNIQUE (tag_name)
)
"""

# 3. proposal_tag_assignments — junction with full audit trail.
DDL_TAG_ASSIGNMENTS = f"""
CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.proposal_tag_assignments (
    assignment_id            VARCHAR(36) NOT NULL,                  -- UUID
    proposal_id              VARCHAR(36) NOT NULL,
    tag_id                   VARCHAR(36) NOT NULL,
    assigned_at              TIMESTAMP_NTZ NOT NULL,
    assigned_by              VARCHAR(128) NOT NULL,
    PRIMARY KEY (assignment_id),
    CONSTRAINT uq_proposal_tag UNIQUE (proposal_id, tag_id)
)
"""

CLUSTER_TAG_ASSIGNMENTS = (
    f"ALTER TABLE {CONTROL_SCHEMA}.proposal_tag_assignments CLUSTER BY (proposal_id)"
)

# ---------------------------------------------------------------------------
# System tag catalog — seeded once.
# ---------------------------------------------------------------------------
SYSTEM_TAGS = [
    # Certification (governance — typically requires role)
    (
        "certified-prod",
        "certification",
        "#10b981",
        "Production-certified by data steward.",
        "data-steward",
    ),
    (
        "certified-2026-Q2",
        "certification",
        "#10b981",
        "Certified during 2026 Q2 quarterly review.",
        "data-steward",
    ),
    # Governance — privacy + regulatory compliance posture
    (
        "phi-vetted",
        "governance",
        "#7c3aed",
        "PHI handling reviewed and approved by privacy officer.",
        "privacy-officer",
    ),
    (
        "pii-vetted",
        "governance",
        "#7c3aed",
        "PII handling reviewed and approved by privacy officer.",
        "privacy-officer",
    ),
    (
        "hipaa-audited",
        "governance",
        "#7c3aed",
        "Passed HIPAA compliance audit.",
        "compliance-officer",
    ),
    (
        "soc2-compliant",
        "governance",
        "#7c3aed",
        "Meets SOC 2 Type II controls (security, availability, confidentiality).",
        "compliance-officer",
    ),
    (
        "gdpr-compliant",
        "governance",
        "#7c3aed",
        "Meets EU GDPR data-protection requirements (lawful basis, data minimisation, right to erasure).",
        "compliance-officer",
    ),
    (
        "hitrust-validated",
        "governance",
        "#7c3aed",
        "HITRUST CSF-validated — healthcare-specific control framework.",
        "compliance-officer",
    ),
    # State (anyone can apply)
    (
        "rollback-candidate",
        "state",
        "#f59e0b",
        "Considered as rollback target if current LIVE fails.",
        None,
    ),
    ("pre-prod-review", "state", "#3b82f6", "Awaiting pre-production review.", None),
    ("experimental", "state", "#8b5cf6", "Experimental / not for production use.", None),
    ("deprecated", "state", "#ef4444", "Marked for removal — do not clone or reference.", None),
    (
        "golden-reference",
        "state",
        "#d4af37",
        "Golden reference template for cross-client cloning.",
        "data-steward",
    ),
    # Custom (operators free to use)
    ("known-good", "custom", "#22c55e", "Manually verified to work end-to-end.", None),
    ("ai-only", "custom", "#06b6d4", "AI-generated, no manual edits.", None),
    ("manual-edits", "custom", "#06b6d4", "Operator edited the AI proposal before approval.", None),
    # Client-scoped — restricts a proposal/template to a specific tenant.
    # Add more via UI as new clients onboard. These two seed the demo tenants.
    (
        "aetna-only",
        "client-scoped",
        "#0ea5e9",
        "Restricted to Aetna client. Do not clone to other tenants.",
        None,
    ),
    (
        "bcbs-only",
        "client-scoped",
        "#0ea5e9",
        "Restricted to BlueCross BlueShield. Do not clone to other tenants.",
        None,
    ),
]


def _now_utc() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def main() -> int:
    settings = load_settings(env=os.environ.get("DL_ENV", "dev"))
    wh = build_adapters(settings).warehouse
    print(f"Warehouse: {type(wh).__name__}")
    print()

    print("=" * 72)
    print(" Phase 16.1 — Creating agent_proposals + proposal_tags + assignments")
    print("=" * 72)
    print()

    for label, ddl in [
        ("agent_proposals", DDL_AGENT_PROPOSALS),
        ("proposal_tags", DDL_PROPOSAL_TAGS),
        ("proposal_tag_assignments", DDL_TAG_ASSIGNMENTS),
    ]:
        try:
            wh.execute(ddl)
            n_cols = next(
                iter(
                    wh.query(
                        f"SELECT COUNT(*) AS c FROM INFORMATION_SCHEMA.COLUMNS "
                        f"WHERE TABLE_SCHEMA = '{CONTROL_SCHEMA}' AND TABLE_NAME = UPPER('{label}')"
                    )
                )
            )["c"]
            print(f"  [OK]   {label:35s} created (cols={n_cols})")
        except Exception as e:
            print(f"  [FAIL] {label}: {str(e)[:200]}")
            return 2

    # Clustering keys (Snowflake-specific — accelerates scope-key lookups)
    print()
    print("  Applying Snowflake clustering keys...")
    for label, sql in [
        ("agent_proposals", CLUSTER_AGENT_PROPOSALS),
        ("proposal_tag_assignments", CLUSTER_TAG_ASSIGNMENTS),
    ]:
        try:
            wh.execute(sql)
            print(f"  [OK]   {label} clustered")
        except Exception as e:
            # Already-clustered or duckdb-fallback; fine to continue.
            print(f"  [skip] {label}: {str(e)[:100]}")

    # Seed system tags (idempotent — INSERT only if tag_name doesn't exist)
    print()
    print("=" * 72)
    print(" Seeding system tag catalog")
    print("=" * 72)
    seeded = 0
    skipped = 0
    actor = "system:phase16-migration"
    for tag_name, category, color, desc, role in SYSTEM_TAGS:
        existing = list(
            wh.query(
                f"SELECT tag_id FROM {CONTROL_SCHEMA}.proposal_tags WHERE tag_name = '{tag_name}'"
            )
        )
        if existing:
            skipped += 1
            continue
        # Use parameterized query — avoids f-string SQL-injection AND
        # f-string backslash issues with conditional NULL formatting.
        wh.execute(
            f"INSERT INTO {CONTROL_SCHEMA}.proposal_tags "
            f"(tag_id, tag_name, tag_category, color_hex, description, "
            f" requires_role, is_system, created_at, created_by) "
            f"SELECT %(tid)s, %(tn)s, %(cat)s, %(col)s, %(desc)s, "
            f"       %(role)s, TRUE, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ, %(by)s",
            {
                "tid": str(uuid.uuid4()),
                "tn": tag_name,
                "cat": category,
                "col": color,
                "desc": desc,
                "role": role,
                "by": actor,
            },
        )
        seeded += 1
    print(f"  Seeded: {seeded} new tags")
    print(f"  Skipped (already present): {skipped}")

    # Verify
    print()
    print("=" * 72)
    print(" Verification")
    print("=" * 72)
    n_proposals = next(
        iter(wh.query(f"SELECT COUNT(*) AS c FROM {CONTROL_SCHEMA}.agent_proposals"))
    )["c"]
    n_tags = next(iter(wh.query(f"SELECT COUNT(*) AS c FROM {CONTROL_SCHEMA}.proposal_tags")))["c"]
    n_assignments = next(
        iter(wh.query(f"SELECT COUNT(*) AS c FROM {CONTROL_SCHEMA}.proposal_tag_assignments"))
    )["c"]
    print(f"  agent_proposals:           {n_proposals} rows")
    print(f"  proposal_tags:             {n_tags} rows")
    print(f"  proposal_tag_assignments:  {n_assignments} rows")
    print()
    print("  Tag catalog by category:")
    rows = list(
        wh.query(
            f"SELECT tag_category, COUNT(*) AS c FROM {CONTROL_SCHEMA}.proposal_tags "
            f"GROUP BY tag_category ORDER BY tag_category"
        )
    )
    for r in rows:
        print(f"    {r['tag_category']:15s} = {r['c']}")
    print()
    print("Migration complete. ✅")
    return 0


if __name__ == "__main__":
    sys.exit(main())
