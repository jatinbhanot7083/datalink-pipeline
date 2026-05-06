"""Phase 17.4 migration — Gold semver + per-client subscriptions.

Adds the metadata layer that lets multiple clients sit on different Gold
versions simultaneously, with explicit ADDITIVE-vs-BREAKING compatibility
tagging on every version bump.

Schema changes (idempotent — safe to re-run):

  global_gold_schema_datasets:
    + semver_major          INT       default 1
    + semver_minor          INT       default 0
    + compatibility_class   VARCHAR   nullable  ('ADDITIVE' | 'BREAKING' | NULL)
    + parent_version        INT       nullable  (FK-style: previous version)
    + migration_window_days INT       default 30
    + release_notes         VARCHAR   nullable

  NEW: client_gold_subscriptions
    + client_id              VARCHAR
    + dataset_code           VARCHAR
    + subscribed_version     INT       (matches global_gold_schema_datasets.version)
    + subscribed_at          TIMESTAMP_NTZ
    + migration_target       INT       nullable
    + migration_started_at   TIMESTAMP_NTZ nullable
    + migration_status       VARCHAR   ('NONE' | 'DUAL_RUN' | 'CUTOVER_PENDING' | 'CUTOVER_DONE')
    + PRIMARY KEY (client_id, dataset_code)

Backfill:
  * Every existing global_gold_schema_datasets row gets semver_major=1,
    semver_minor=0 if version=1, otherwise mapped from version.
  * Every LIVE client_pipeline_instances row creates a corresponding
    client_gold_subscriptions row pointing at the LIVE Gold version
    for that dataset.

Run:
  docker exec datalink-control-tower python \\
    /opt/datalink/scripts/migrate_phase17_4_gold_versioning.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

_ENV = ROOT / ".env"
if _ENV.exists():
    for raw in _ENV.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        os.environ.setdefault(k.strip(), v.strip().strip("'").strip('"'))

import snowflake.connector  # noqa: E402


def _connect():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "DATALINK_DEV"),
        role=os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
    )


def _section(t: str) -> None:
    print()
    print("=" * 72)
    print(f" {t}")
    print("=" * 72)


def _exec(cur, sql: str, *, ok_msg: str | None = None) -> bool:
    try:
        cur.execute(sql)
        print(f"  [OK]   {ok_msg or sql[:80]}")
        return True
    except Exception as exc:
        print(f"  [SKIP] {ok_msg or sql[:80]}  ({type(exc).__name__}: {str(exc)[:140]})")
        return False


def _column_exists(cur, schema: str, table: str, col: str) -> bool:
    cur.execute(
        f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS "
        f"WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}' "
        f"AND UPPER(COLUMN_NAME) = '{col.upper()}'"
    )
    return int(cur.fetchone()[0] or 0) > 0


def _table_exists(cur, schema: str, table: str) -> bool:
    cur.execute(
        f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES "
        f"WHERE TABLE_SCHEMA = '{schema}' AND UPPER(TABLE_NAME) = '{table.upper()}'"
    )
    return int(cur.fetchone()[0] or 0) > 0


def main() -> int:
    print("Phase 17.4 — Gold semver + subscriptions")
    conn = _connect()
    cur = conn.cursor()

    # ---- Step 1 — extend global_gold_schema_datasets -------------------
    _section("Step 1/3 — extend global_gold_schema_datasets")
    new_cols = [
        ("semver_major", "INT", "1"),
        ("semver_minor", "INT", "0"),
        ("compatibility_class", "VARCHAR(16)", None),  # NULL OK
        ("parent_version", "INT", None),
        ("migration_window_days", "INT", "30"),
        ("release_notes", "VARCHAR(2000)", None),
    ]
    for name, sql_type, default in new_cols:
        if _column_exists(cur, "CONTROL", "GLOBAL_GOLD_SCHEMA_DATASETS", name):
            print(f"  [skip] {name}: already exists")
            continue
        # Snowflake's ADD COLUMN syntax: defaults are added separately.
        if default is not None:
            ddl = (
                f"ALTER TABLE CONTROL.global_gold_schema_datasets "
                f"ADD COLUMN {name} {sql_type} DEFAULT {default}"
            )
        else:
            ddl = (
                f"ALTER TABLE CONTROL.global_gold_schema_datasets "
                f"ADD COLUMN {name} {sql_type}"
            )
        _exec(cur, ddl, ok_msg=f"{name} {sql_type}")

    # Backfill existing rows so semver columns are populated.
    _exec(
        cur,
        "UPDATE CONTROL.global_gold_schema_datasets "
        "SET semver_major = 1, semver_minor = 0 "
        "WHERE semver_major IS NULL",
        ok_msg="backfilled semver_major=1, semver_minor=0 on legacy rows",
    )

    # ---- Step 2 — create client_gold_subscriptions ---------------------
    _section("Step 2/3 — create client_gold_subscriptions")
    if _table_exists(cur, "CONTROL", "CLIENT_GOLD_SUBSCRIPTIONS"):
        print("  [skip] client_gold_subscriptions already exists")
    else:
        _exec(
            cur,
            """
            CREATE TABLE CONTROL.client_gold_subscriptions (
              client_id              VARCHAR(64)   NOT NULL,
              dataset_code           VARCHAR(64)   NOT NULL,
              subscribed_version     INT           NOT NULL,
              subscribed_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
              migration_target       INT,
              migration_started_at   TIMESTAMP_NTZ,
              migration_status       VARCHAR(32)   DEFAULT 'NONE',
              notes                  VARCHAR(2000),
              CONSTRAINT pk_client_gold_subscriptions PRIMARY KEY (client_id, dataset_code)
            )
            """,
            ok_msg="created client_gold_subscriptions",
        )

    # Backfill subscriptions from existing LIVE pipeline instances.
    _exec(
        cur,
        """
        INSERT INTO CONTROL.client_gold_subscriptions
          (client_id, dataset_code, subscribed_version, migration_status)
        SELECT
          p.client_id,
          p.dataset_code,
          COALESCE(g.version, 1) AS subscribed_version,
          'NONE'
        FROM CONTROL.client_pipeline_instances p
        LEFT JOIN CONTROL.global_gold_schema_datasets g
          ON g.dataset_code = p.dataset_code
         AND g.status = 'LIVE'
         AND g.scope_owner = 'GLOBAL_CORP'
        WHERE p.status = 'LIVE'
          AND NOT EXISTS (
            SELECT 1 FROM CONTROL.client_gold_subscriptions s
             WHERE s.client_id = p.client_id
               AND s.dataset_code = p.dataset_code
          )
        """,
        ok_msg="backfilled subscriptions from LIVE pipeline_instances",
    )

    # ---- Step 3 — verification report ----------------------------------
    _section("Step 3/3 — verification")
    cur.execute(
        "SELECT dataset_code, version, semver_major, semver_minor, "
        "compatibility_class, scope_owner, status "
        "FROM CONTROL.global_gold_schema_datasets "
        "ORDER BY dataset_code, version"
    )
    print("Gold versions registered:")
    for r in cur.fetchall():
        smv = f"v{r[2] or 1}.{r[3] or 0}"
        cls = f" [{r[4]}]" if r[4] else ""
        print(f"  {r[0]:25s} db_v={r[1]} semver={smv}{cls} scope={r[5]} status={r[6]}")

    cur.execute(
        "SELECT client_id, dataset_code, subscribed_version, migration_status "
        "FROM CONTROL.client_gold_subscriptions "
        "ORDER BY client_id, dataset_code"
    )
    print()
    print("Client subscriptions:")
    sub_rows = cur.fetchall()
    if not sub_rows:
        print("  (none yet)")
    else:
        for r in sub_rows:
            print(f"  {r[0]:25s} {r[1]:20s} v{r[2]} status={r[3]}")

    cur.close()
    conn.close()
    print()
    print("Migration 17.4 complete. Next: docker compose down && docker compose up -d")
    return 0


if __name__ == "__main__":
    sys.exit(main())
