"""Phase 16.2 (Wave 2) — Versioning + audit foundation.

Creates two tables:

  1. CONTROL.version_history   — universal version registry. One row per
                                 (artifact_type, artifact_id, version).
                                 Stores full payload snapshot for diff/restore.
  2. CONTROL.version_tags      — tag system for versions (cert-prod,
                                 rollback-candidate, golden-reference, …).
                                 Reuses the same tag catalog (proposal_tags).

Why one universal table instead of per-artifact-type version tables?
  * Single diff/restore code path
  * Cross-artifact lineage (Silver v3 ← Gold v4 ← pipeline-instance v2)
  * Tagging works once across everything
  * Compliance auditors get ONE place to look

Run with:
    DL_ENV=dev python3 scripts/migrate_phase16_2_versioning.py
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

DDL_VERSION_HISTORY = f"""
CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.version_history (
    version_event_id        VARCHAR(36) NOT NULL,        -- UUID for the row
    artifact_type           VARCHAR(48) NOT NULL,        -- 'silver_schema' | 'gold_schema' | 'pipeline_instance' | 'dq_suite'
    artifact_id             VARCHAR(64) NOT NULL,        -- the artifact's stable ID
    artifact_scope_key      VARCHAR(256) NOT NULL,       -- human-readable scope, e.g. 'aetna:membership'
    version                 INTEGER NOT NULL,            -- v1, v2, … (per artifact_id)
    parent_version          INTEGER,                     -- the version this one supersedes
    cloned_from_artifact_id VARCHAR(64),                 -- cross-artifact lineage (Aetna v3 cloned from BCBS v7)
    cloned_from_version     INTEGER,
    upstream_silver_id      VARCHAR(64),                 -- when artifact_type='pipeline_instance', which Silver design it consumed
    upstream_silver_version INTEGER,
    upstream_gold_id        VARCHAR(64),                 -- ditto for Gold
    upstream_gold_version   INTEGER,
    snapshot                VARIANT NOT NULL,            -- full payload at this version (for diff/restore)
    diff_summary            VARIANT,                     -- summary of what changed vs parent_version
    change_kind             VARCHAR(32) NOT NULL,        -- 'CREATE' | 'UPDATE' | 'CLONE' | 'RESTORE' | 'AUTO_DERIVED'
    change_reason           VARCHAR,                     -- human-readable why
    pinned                  BOOLEAN NOT NULL DEFAULT FALSE,
    is_current              BOOLEAN NOT NULL DEFAULT FALSE,  -- true for the LIVE/active version
    created_at              TIMESTAMP_NTZ NOT NULL,
    created_by              VARCHAR(128) NOT NULL,
    notes                   VARCHAR,
    PRIMARY KEY (version_event_id)
)
"""

DDL_VERSION_TAGS = f"""
CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.version_tag_assignments (
    assignment_id           VARCHAR(36) NOT NULL,
    version_event_id        VARCHAR(36) NOT NULL,
    tag_id                  VARCHAR(36) NOT NULL,        -- FK → proposal_tags.tag_id (shared catalog)
    assigned_at             TIMESTAMP_NTZ NOT NULL,
    assigned_by             VARCHAR(128) NOT NULL,
    PRIMARY KEY (assignment_id),
    CONSTRAINT uq_version_tag UNIQUE (version_event_id, tag_id)
)
"""

# Notification queue for upstream-update events. When Silver bumps from v3 → v4,
# a row lands here for each pipeline_instance that consumed v3 — UI surfaces
# "📦 upstream update available" until acknowledged.
DDL_UPSTREAM_NOTIFICATIONS = f"""
CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.upstream_update_notifications (
    notification_id         VARCHAR(36) NOT NULL,
    artifact_type           VARCHAR(48) NOT NULL,        -- which downstream artifact has stale upstream
    artifact_id             VARCHAR(64) NOT NULL,
    artifact_scope_key      VARCHAR(256) NOT NULL,
    upstream_artifact_type  VARCHAR(48) NOT NULL,        -- 'silver_schema' | 'gold_schema'
    upstream_artifact_id    VARCHAR(64) NOT NULL,
    consumed_version        INTEGER NOT NULL,            -- the version downstream is locked to
    available_version       INTEGER NOT NULL,            -- the new LIVE upstream version
    severity                VARCHAR(16) NOT NULL,        -- 'INFO' | 'WARNING' | 'CRITICAL'
    status                  VARCHAR(16) NOT NULL,        -- 'OPEN' | 'ACKED' | 'APPLIED'
    created_at              TIMESTAMP_NTZ NOT NULL,
    acked_at                TIMESTAMP_NTZ,
    acked_by                VARCHAR(128),
    applied_at              TIMESTAMP_NTZ,
    applied_by              VARCHAR(128),
    PRIMARY KEY (notification_id)
)
"""

CLUSTERING = [
    f"ALTER TABLE {CONTROL_SCHEMA}.version_history "
    f"CLUSTER BY (artifact_type, artifact_id, version)",
    f"ALTER TABLE {CONTROL_SCHEMA}.version_tag_assignments CLUSTER BY (version_event_id)",
    f"ALTER TABLE {CONTROL_SCHEMA}.upstream_update_notifications CLUSTER BY (status, severity)",
]


def main() -> int:
    settings = load_settings(env=os.environ.get("DL_ENV", "dev"))
    wh = build_adapters(settings).warehouse
    print(f"Warehouse: {type(wh).__name__}")
    print()
    print("=" * 72)
    print(" Phase 16.2 — Versioning + audit foundation")
    print("=" * 72)
    print()

    for label, ddl in [
        ("version_history", DDL_VERSION_HISTORY),
        ("version_tag_assignments", DDL_VERSION_TAGS),
        ("upstream_update_notifications", DDL_UPSTREAM_NOTIFICATIONS),
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
        except Exception as exc:
            print(f"  [FAIL] {label}: {str(exc)[:200]}")
            return 2

    print()
    print("  Applying clustering keys...")
    for sql in CLUSTERING:
        try:
            wh.execute(sql)
        except Exception as exc:
            print(f"  [skip] {str(exc)[:100]}")
    print("  ✅ clustering applied")

    print()
    print("=" * 72)
    print(" Verification")
    print("=" * 72)
    for t in ("version_history", "version_tag_assignments", "upstream_update_notifications"):
        n = next(iter(wh.query(f"SELECT COUNT(*) AS c FROM {CONTROL_SCHEMA}.{t}")))["c"]
        print(f"  {t:35s}: {n} rows")
    print()
    print("Migration complete. ✅")
    return 0


if __name__ == "__main__":
    sys.exit(main())
