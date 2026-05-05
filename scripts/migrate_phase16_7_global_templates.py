"""Phase 16.7 — Global Template Layer.

Adds scope_owner + global-template tables so every artifact has a "GLOBAL"
canonical version that clients clone from (the 70-80% case) and override
selectively (the 20-30% case).

Schema additions:

  1. global_silver_schema_datasets / global_gold_schema_datasets:
        scope_owner             VARCHAR  DEFAULT '__global__'
        forked_from_global_version INTEGER (NULL for the global itself)

  2. client_pipeline_instances:
        forked_from_global_id   VARCHAR  (the global pipeline_template_id)
        forked_from_global_version INTEGER

  3. NEW table: global_pipeline_templates
        (one row per (dataset_code, version) of the canonical pipeline)

  4. NEW table: global_artifact_blobs
        (storage of canonical Bronze DDL / Silver dbt / Gold dbt / DAG /
         GX suite payloads — the binary clones operators replay onto a
         client without re-running the LLM)

The pre-existing semantics (where every silver_dataset row was implicitly
"global" because there was no client_id column) keeps working — those rows
default to scope_owner='__global__'.

Run:
    DL_ENV=dev python3 scripts/migrate_phase16_7_global_templates.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

# Auto-load .env
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

GLOBAL_SCOPE = "__global__"

ALTERS = [
    # Silver registry — add scope_owner so a row can be GLOBAL or CLIENT-specific
    f"ALTER TABLE {CONTROL_SCHEMA}.global_silver_schema_datasets ADD COLUMN scope_owner VARCHAR(64) DEFAULT '{GLOBAL_SCOPE}'",
    f"ALTER TABLE {CONTROL_SCHEMA}.global_silver_schema_datasets ADD COLUMN forked_from_global_version INTEGER",
    # Gold registry — same
    f"ALTER TABLE {CONTROL_SCHEMA}.global_gold_schema_datasets ADD COLUMN scope_owner VARCHAR(64) DEFAULT '{GLOBAL_SCOPE}'",
    f"ALTER TABLE {CONTROL_SCHEMA}.global_gold_schema_datasets ADD COLUMN forked_from_global_version INTEGER",
    # Pipeline instances — track which global template they cloned from
    f"ALTER TABLE {CONTROL_SCHEMA}.client_pipeline_instances ADD COLUMN forked_from_global_id VARCHAR(64)",
    f"ALTER TABLE {CONTROL_SCHEMA}.client_pipeline_instances ADD COLUMN forked_from_global_version INTEGER",
]

# New table: canonical pipeline templates (one row per global pipeline version)
DDL_GLOBAL_PIPELINE_TEMPLATES = f"""
CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.global_pipeline_templates (
    template_id              VARCHAR(64) NOT NULL,        -- '__global__:<dataset_code>:v<N>'
    dataset_code             VARCHAR(64) NOT NULL,
    version                  INTEGER NOT NULL,
    parent_version           INTEGER,                     -- supersedes
    status                   VARCHAR(16) NOT NULL,        -- DRAFT | LIVE | DEPRECATED | ARCHIVED
    bronze_anchor            VARCHAR(32) NOT NULL,
    schedule_cron            VARCHAR(48) NOT NULL,
    silver_schema_id         VARCHAR(64),                 -- FK to global Silver schema
    silver_schema_version    INTEGER,
    gold_schema_id           VARCHAR(64),                 -- FK to global Gold schema
    gold_schema_version      INTEGER,
    -- Designer payload — what the AI / operator originally proposed
    proposal_payload         VARIANT,
    -- Audit + lineage
    designed_by              VARCHAR(128) NOT NULL,
    designed_at              TIMESTAMP_NTZ NOT NULL,
    approved_by              VARCHAR(128),
    approved_at              TIMESTAMP_NTZ,
    archived_at              TIMESTAMP_NTZ,
    notes                    VARCHAR,
    PRIMARY KEY (template_id),
    CONSTRAINT uq_global_dataset_version UNIQUE (dataset_code, version)
)
"""

# Storage of canonical artifacts — Bronze DDL, Silver dbt files, Gold dbt,
# Airflow DAG, GX suite. Stored as TEXT so a clone is just a SELECT + write.
DDL_GLOBAL_ARTIFACT_BLOBS = f"""
CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.global_artifact_blobs (
    blob_id                  VARCHAR(36) NOT NULL,        -- UUID
    template_id              VARCHAR(64) NOT NULL,        -- FK to global_pipeline_templates
    artifact_kind            VARCHAR(48) NOT NULL,        -- 'bronze_ddl' | 'silver_dbt_file' | 'gold_dbt_file' | 'airflow_dag' | 'gx_suite' | 'routing_rules'
    artifact_filename        VARCHAR(256),                -- e.g. 'hub_member.sql' for one of the silver files
    artifact_text            VARCHAR NOT NULL,            -- raw content (Snowflake VARCHAR is unbounded)
    artifact_metadata        VARIANT,                     -- emitter version, language, etc.
    created_at               TIMESTAMP_NTZ NOT NULL,
    PRIMARY KEY (blob_id)
)
"""

# Migration plans — when global changes, downstream clients need a HITL walk.
DDL_GLOBAL_MIGRATION_PLANS = f"""
CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.global_migration_plans (
    plan_id                  VARCHAR(36) NOT NULL,
    artifact_type            VARCHAR(48) NOT NULL,        -- 'silver_schema' | 'gold_schema' | 'pipeline_template'
    artifact_id              VARCHAR(64) NOT NULL,
    from_version             INTEGER NOT NULL,
    to_version               INTEGER NOT NULL,
    diff_summary             VARIANT,
    status                   VARCHAR(16) NOT NULL,        -- 'PROPOSED' | 'APPROVED' | 'APPLYING' | 'DONE' | 'CANCELLED'
    affected_clients         ARRAY,                       -- list of client_ids cloning the old version
    auto_apply_safe          BOOLEAN DEFAULT FALSE,
    created_at               TIMESTAMP_NTZ NOT NULL,
    created_by               VARCHAR(128) NOT NULL,
    approved_at              TIMESTAMP_NTZ,
    approved_by              VARCHAR(128),
    notes                    VARCHAR,
    PRIMARY KEY (plan_id)
)
"""


def main() -> int:
    settings = load_settings(env=os.environ.get("DL_ENV", "dev"))
    wh = build_adapters(settings).warehouse
    print(f"Warehouse: {type(wh).__name__}")
    print()
    print("=" * 72)
    print(" Phase 16.7 — Global Template Layer migration")
    print("=" * 72)
    print()

    # 1. ALTER existing tables
    print("[1/3] Adding scope_owner + forked_from columns to existing tables...")
    for sql in ALTERS:
        try:
            wh.execute(sql)
            cols = sql.split("ADD COLUMN")[1].strip().split()[0]
            print(f"  [OK]   {cols}")
        except Exception as e:
            err = str(e)[:120]
            if "already exists" in err.lower() or "already" in err.lower():
                print("  [skip] (column already present)")
            else:
                print(f"  [warn] {err}")

    # 2. NEW tables
    print()
    print("[2/3] Creating global-template tables...")
    for label, ddl in [
        ("global_pipeline_templates", DDL_GLOBAL_PIPELINE_TEMPLATES),
        ("global_artifact_blobs", DDL_GLOBAL_ARTIFACT_BLOBS),
        ("global_migration_plans", DDL_GLOBAL_MIGRATION_PLANS),
    ]:
        try:
            wh.execute(ddl)
            n = next(
                iter(
                    wh.query(
                        f"SELECT COUNT(*) AS c FROM INFORMATION_SCHEMA.COLUMNS "
                        f"WHERE TABLE_SCHEMA = '{CONTROL_SCHEMA}' AND TABLE_NAME = UPPER('{label}')"
                    )
                )
            )["c"]
            print(f"  [OK]   {label:35s} created (cols={n})")
        except Exception as e:
            print(f"  [FAIL] {label}: {str(e)[:200]}")
            return 2

    # 3. Backfill scope_owner='__global__' on existing rows (already covered by DEFAULT,
    # but explicit UPDATE for clarity / pre-existing rows that may have NULL).
    print()
    print("[3/3] Backfilling scope_owner='__global__' on pre-existing rows...")
    for tbl in ("global_silver_schema_datasets", "global_gold_schema_datasets"):
        try:
            wh.execute(
                f"UPDATE {CONTROL_SCHEMA}.{tbl} SET scope_owner = '{GLOBAL_SCOPE}' "
                f"WHERE scope_owner IS NULL"
            )
            print(f"  [OK]   {tbl}")
        except Exception as e:
            print(f"  [warn] {tbl}: {str(e)[:120]}")

    print()
    print("=" * 72)
    print(" Verification")
    print("=" * 72)
    for t in (
        "global_pipeline_templates",
        "global_artifact_blobs",
        "global_migration_plans",
    ):
        n = next(iter(wh.query(f"SELECT COUNT(*) AS c FROM {CONTROL_SCHEMA}.{t}")))["c"]
        print(f"  {t:35s} = {n} rows")

    # Show how many global vs client-scoped Silver/Gold designs exist
    for label, q in [
        (
            "Silver schemas (global)",
            f"SELECT COUNT(*) c FROM {CONTROL_SCHEMA}.global_silver_schema_datasets WHERE scope_owner = '{GLOBAL_SCOPE}'",
        ),
        (
            "Silver schemas (client)",
            f"SELECT COUNT(*) c FROM {CONTROL_SCHEMA}.global_silver_schema_datasets WHERE scope_owner <> '{GLOBAL_SCOPE}'",
        ),
        (
            "Gold schemas (global)",
            f"SELECT COUNT(*) c FROM {CONTROL_SCHEMA}.global_gold_schema_datasets WHERE scope_owner = '{GLOBAL_SCOPE}'",
        ),
        (
            "Gold schemas (client)",
            f"SELECT COUNT(*) c FROM {CONTROL_SCHEMA}.global_gold_schema_datasets WHERE scope_owner <> '{GLOBAL_SCOPE}'",
        ),
    ]:
        try:
            n = next(iter(wh.query(q)))["c"]
            print(f"  {label:35s} = {n}")
        except Exception:
            pass

    print()
    print("Migration complete. ✅")
    return 0


if __name__ == "__main__":
    sys.exit(main())
