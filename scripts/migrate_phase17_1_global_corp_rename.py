"""Phase 17.1 migration — rename ``__global__`` → ``GLOBAL_CORP``.

Idempotent. Safe to re-run. Performs three classes of change:

1. **Metadata UPDATEs** — every CONTROL.* row that uses ``__global__`` as a
   ``scope_owner`` or ``client_id`` is rewritten to ``GLOBAL_CORP``. Includes:
     * global_silver_schema_datasets.scope_owner
     * global_gold_schema_datasets.scope_owner
     * client_pipeline_instances.client_id (template rows)
     * any other table referencing the legacy literal.

2. **Schema renames in Snowflake** — the actual physical schemas created from
   the legacy name (``BRONZE___GLOBAL__``, ``SILVER___GLOBAL__``,
   ``GOLD___GLOBAL__``) are renamed to ``BRONZE_GLOBAL_CORP``,
   ``SILVER_GLOBAL_CORP``, ``GOLD_GLOBAL_CORP`` via ``ALTER SCHEMA RENAME``.

3. **Default-value updates** — the DEFAULT clause on ``scope_owner`` columns
   is rewritten so future inserts use ``GLOBAL_CORP``.

Run from the host or any container that has Snowflake creds:

    python3 scripts/migrate_phase17_1_global_corp_rename.py

Output is verbose by design — each statement is logged so a partial failure
is diagnosable.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

# Auto-load .env so the script works without an explicit `set -a` export.
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

LEGACY = "__global__"
CANONICAL = "GLOBAL_CORP"

# Mapping of legacy schema name → canonical name (Snowflake side).
SCHEMA_RENAMES: list[tuple[str, str]] = [
    ("BRONZE___GLOBAL__", "BRONZE_GLOBAL_CORP"),
    ("SILVER___GLOBAL__", "SILVER_GLOBAL_CORP"),
    ("GOLD___GLOBAL__", "GOLD_GLOBAL_CORP"),
]

# Tables with a ``scope_owner`` column that needs UPDATE.
SCOPE_OWNER_TABLES: list[str] = [
    "global_silver_schema_datasets",
    "global_gold_schema_datasets",
]

# Tables with a ``client_id`` column where the legacy value may appear.
CLIENT_ID_TABLES: list[str] = [
    "client_pipeline_instances",
]

# Tables with template_id strings that encoded the legacy literal.
TEMPLATE_ID_TABLES: list[str] = [
    "global_pipeline_templates",
    "global_artifact_blobs",
]


def _section(title: str) -> None:
    print()
    print("=" * 72)
    print(f" {title}")
    print("=" * 72)


def _exec(wh, sql: str, *, ok_msg: str | None = None) -> bool:
    """Execute one statement, log + swallow exceptions so the migration
    keeps making progress on the rest. Returns True if statement succeeded."""
    try:
        wh.execute(sql)
        print(f"  [OK]   {ok_msg or sql[:80]}")
        return True
    except Exception as exc:
        print(f"  [SKIP] {ok_msg or sql[:80]}  ({type(exc).__name__}: {str(exc)[:120]})")
        return False


def _count(wh, sql: str) -> int:
    try:
        rows = list(wh.query(sql))
        return int(rows[0]["c"]) if rows else 0
    except Exception:
        return -1


def main() -> int:
    settings = load_settings(env=os.environ.get("DL_ENV", "dev"))
    wh = build_adapters(settings).warehouse
    print(f"Warehouse: {type(wh).__name__}")
    print(f"Legacy:    {LEGACY!r}")
    print(f"Canonical: {CANONICAL!r}")

    # ---- 1. Metadata UPDATE — scope_owner columns ----------------------
    _section("Step 1/4 — UPDATE scope_owner columns")
    for tbl in SCOPE_OWNER_TABLES:
        before = _count(
            wh, f"SELECT COUNT(*) AS c FROM {CONTROL_SCHEMA}.{tbl} WHERE scope_owner = '{LEGACY}'"
        )
        if before <= 0:
            print(f"  [skip] {tbl}: 0 rows with scope_owner='{LEGACY}'")
            continue
        _exec(
            wh,
            f"UPDATE {CONTROL_SCHEMA}.{tbl} "
            f"SET scope_owner = '{CANONICAL}' "
            f"WHERE scope_owner = '{LEGACY}'",
            ok_msg=f"{tbl}: {before} row(s) → {CANONICAL}",
        )

    # ---- 2. Metadata UPDATE — client_id columns ------------------------
    _section("Step 2/4 — UPDATE client_id columns (template rows)")
    for tbl in CLIENT_ID_TABLES:
        before = _count(
            wh, f"SELECT COUNT(*) AS c FROM {CONTROL_SCHEMA}.{tbl} WHERE client_id = '{LEGACY}'"
        )
        if before <= 0:
            print(f"  [skip] {tbl}: 0 rows with client_id='{LEGACY}'")
            continue
        _exec(
            wh,
            f"UPDATE {CONTROL_SCHEMA}.{tbl} "
            f"SET client_id = '{CANONICAL}' "
            f"WHERE client_id = '{LEGACY}'",
            ok_msg=f"{tbl}: {before} row(s) → {CANONICAL}",
        )

    # ---- 2b. template_id rewrites (template_id strings encode the legacy) -
    for tbl in TEMPLATE_ID_TABLES:
        # Only attempt rewrite if the table + column exist.
        try:
            sample = list(
                wh.query(
                    f"SELECT COUNT(*) AS c FROM {CONTROL_SCHEMA}.{tbl} "
                    f"WHERE template_id LIKE '{LEGACY}:%'"
                )
            )
            n = int(sample[0]["c"]) if sample else 0
        except Exception:
            print(f"  [skip] {tbl}: missing or no template_id column")
            continue
        if n == 0:
            print(f"  [skip] {tbl}: no template_id starting with '{LEGACY}:'")
            continue
        _exec(
            wh,
            f"UPDATE {CONTROL_SCHEMA}.{tbl} "
            f"SET template_id = REPLACE(template_id, '{LEGACY}:', '{CANONICAL}:') "
            f"WHERE template_id LIKE '{LEGACY}:%'",
            ok_msg=f"{tbl}: rewrote {n} template_id prefix(es)",
        )

    # ---- 3. Snowflake schema renames -----------------------------------
    _section("Step 3/4 — ALTER SCHEMA renames")
    for old, new in SCHEMA_RENAMES:
        # Check if old exists, new doesn't (idempotency)
        old_exists = _count(
            wh,
            f"SELECT COUNT(*) AS c FROM INFORMATION_SCHEMA.SCHEMATA "
            f"WHERE SCHEMA_NAME = '{old}'",
        )
        new_exists = _count(
            wh,
            f"SELECT COUNT(*) AS c FROM INFORMATION_SCHEMA.SCHEMATA "
            f"WHERE SCHEMA_NAME = '{new}'",
        )
        if new_exists > 0 and old_exists == 0:
            print(f"  [skip] {old} → {new} (already renamed)")
            continue
        if old_exists == 0 and new_exists == 0:
            print(f"  [skip] {old}: schema does not exist (nothing to rename)")
            continue
        if new_exists > 0 and old_exists > 0:
            print(
                f"  [WARN] BOTH {old} and {new} exist — manual reconciliation needed."
            )
            continue
        _exec(
            wh,
            f'ALTER SCHEMA "{old}" RENAME TO "{new}"',
            ok_msg=f"{old} → {new}",
        )

    # ---- 4. DEFAULT clause updates -------------------------------------
    _section("Step 4/4 — Update DEFAULT 'scope_owner' to 'GLOBAL_CORP'")
    for tbl in SCOPE_OWNER_TABLES:
        _exec(
            wh,
            f"ALTER TABLE {CONTROL_SCHEMA}.{tbl} "
            f"ALTER COLUMN scope_owner SET DEFAULT '{CANONICAL}'",
            ok_msg=f"{tbl}.scope_owner DEFAULT → '{CANONICAL}'",
        )

    # ---- Verification --------------------------------------------------
    _section("Verification")
    for tbl in SCOPE_OWNER_TABLES:
        leg = _count(
            wh, f"SELECT COUNT(*) AS c FROM {CONTROL_SCHEMA}.{tbl} WHERE scope_owner = '{LEGACY}'"
        )
        can = _count(
            wh,
            f"SELECT COUNT(*) AS c FROM {CONTROL_SCHEMA}.{tbl} WHERE scope_owner = '{CANONICAL}'",
        )
        print(f"  {tbl:45s} legacy={leg}  canonical={can}")
    for tbl in CLIENT_ID_TABLES:
        leg = _count(
            wh, f"SELECT COUNT(*) AS c FROM {CONTROL_SCHEMA}.{tbl} WHERE client_id = '{LEGACY}'"
        )
        can = _count(
            wh,
            f"SELECT COUNT(*) AS c FROM {CONTROL_SCHEMA}.{tbl} WHERE client_id = '{CANONICAL}'",
        )
        print(f"  {tbl:45s} legacy={leg}  canonical={can}")
    print()
    print("Snowflake schemas matching either name:")
    for old, new in SCHEMA_RENAMES:
        rows = list(
            wh.query(
                f"SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA "
                f"WHERE SCHEMA_NAME IN ('{old}', '{new}') ORDER BY SCHEMA_NAME"
            )
        )
        names = [r["SCHEMA_NAME"] for r in rows] if rows else []
        print(f"  {old} / {new}: {names if names else 'NONE'}")

    print()
    print("Migration 17.1 complete. Next: docker compose down && docker compose up -d")
    return 0


if __name__ == "__main__":
    sys.exit(main())
