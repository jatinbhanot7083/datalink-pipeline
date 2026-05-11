"""Phase 18 — full client wipe for end-to-end re-testing.

Deletes EVERYTHING for one or more client_ids so you can rebuild from
DMD → Cloning Center → Pipeline Architect → ▶ Run now without leftover
state contaminating the test.

What gets deleted (per client):
  CONTROL.client_pipeline_instances        (instance metadata)
  CONTROL.dataset_pipeline_runs            (run history rows)
  CONTROL.global_silver_schema_*           (rows where scope_owner = client)
  CONTROL.global_gold_schema_*             (rows where scope_owner = client)
  CONTROL.bronze_to_silver_mappings        (cascade via silver_dataset_id)
  BRONZE_<CLIENT>                          (DROP SCHEMA CASCADE)
  SILVER_<CLIENT>                          (DROP SCHEMA CASCADE)
  GOLD_<CLIENT>                            (DROP SCHEMA CASCADE)

What is preserved:
  CONTROL.global_bronze_catalog_datasets   (Global catalog)
  CONTROL.global_silver_schema_*  WHERE scope_owner = 'GLOBAL_CORP'
  CONTROL.global_gold_schema_*    WHERE scope_owner = 'GLOBAL_CORP'
  All factory files in dags/                (those are stateless)

USAGE
-----
    python scripts/wipe_client.py aetna
    python scripts/wipe_client.py aetna affinity
    python scripts/wipe_client.py aetna --dry-run
    python scripts/wipe_client.py --all-clients          # nuke every non-GLOBAL_CORP client

SAFETY
------
  * Refuses GLOBAL_CORP / __global__ — those are sacred templates.
  * --dry-run prints what WOULD be deleted without touching anything.
  * Confirms with operator before destructive actions unless --yes.
"""

from __future__ import annotations

import argparse
import os
import sys

import snowflake.connector

GLOBAL_RESERVED = {"GLOBAL_CORP", "__global__", "global_corp", ""}


def _connect():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ["SNOWFLAKE_ROLE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
    )


def _resolve_clients(cur, *, args_clients: list[str], all_clients: bool) -> list[str]:
    """Return the canonical client_id list to wipe, lowercased + de-duped."""
    if all_clients:
        cur.execute("SELECT DISTINCT client_id FROM CONTROL.client_pipeline_instances")
        from_inst = {str(r[0]).strip() for r in cur.fetchall() if r[0]}
        cur.execute("SELECT DISTINCT scope_owner FROM CONTROL.global_silver_schema_datasets")
        from_silver = {str(r[0]).strip() for r in cur.fetchall() if r[0]}
        cur.execute("SELECT DISTINCT scope_owner FROM CONTROL.global_gold_schema_datasets")
        from_gold = {str(r[0]).strip() for r in cur.fetchall() if r[0]}
        all_found = from_inst | from_silver | from_gold
        targets = sorted(c for c in all_found if c not in GLOBAL_RESERVED)
        return targets
    return [c for c in args_clients if c not in GLOBAL_RESERVED]


def _inventory(cur, client_id: str) -> dict[str, int]:
    """Return counts of what would be deleted for one client_id."""
    counts: dict[str, int] = {}

    def _count(label: str, sql: str, params: tuple = ()):
        try:
            cur.execute(sql, params)
            counts[label] = int(cur.fetchone()[0] or 0)
        except Exception:
            counts[label] = -1  # table missing or error — treat as zero work

    cli_u = client_id.upper()

    _count(
        "client_pipeline_instances",
        "SELECT COUNT(*) FROM CONTROL.client_pipeline_instances "
        "WHERE LOWER(client_id) = LOWER(%s)",
        (client_id,),
    )
    _count(
        "dataset_pipeline_runs",
        "SELECT COUNT(*) FROM CONTROL.dataset_pipeline_runs " "WHERE LOWER(client_id) = LOWER(%s)",
        (client_id,),
    )
    _count(
        "global_silver_schema_datasets",
        "SELECT COUNT(*) FROM CONTROL.global_silver_schema_datasets "
        "WHERE LOWER(scope_owner) = LOWER(%s)",
        (client_id,),
    )
    _count(
        "global_gold_schema_datasets",
        "SELECT COUNT(*) FROM CONTROL.global_gold_schema_datasets "
        "WHERE LOWER(scope_owner) = LOWER(%s)",
        (client_id,),
    )

    # Schema existence
    cur.execute(
        "SELECT schema_name FROM INFORMATION_SCHEMA.SCHEMATA " "WHERE schema_name IN (%s, %s, %s)",
        (f"BRONZE_{cli_u}", f"SILVER_{cli_u}", f"GOLD_{cli_u}"),
    )
    found = {r[0] for r in cur.fetchall()}
    counts["BRONZE_schema_exists"] = int(f"BRONZE_{cli_u}" in found)
    counts["SILVER_schema_exists"] = int(f"SILVER_{cli_u}" in found)
    counts["GOLD_schema_exists"] = int(f"GOLD_{cli_u}" in found)
    return counts


def _wipe_one(cur, client_id: str, *, dry_run: bool) -> dict[str, int]:
    """Delete everything for one client.  Returns row counts deleted."""
    cli_u = client_id.upper()
    deleted: dict[str, int] = {}

    def _exec(label: str, sql: str, params: tuple = ()) -> None:
        if dry_run:
            print(f"    [DRY-RUN] would: {sql.split()[0]} ... {label}")
            return
        try:
            cur.execute(sql, params)
            deleted[label] = cur.rowcount or 0
        except Exception as exc:
            deleted[label] = -1
            print(f"    WARN: {label} failed ({type(exc).__name__}: {str(exc)[:100]})")

    # CONTROL rows — children first, parents last (FK ordering by convention).
    # Children of silver: bronze_to_silver_mappings, columns, tables.
    _exec(
        "bronze_to_silver_mappings",
        "DELETE FROM CONTROL.bronze_to_silver_mappings WHERE silver_dataset_id IN ("
        "SELECT silver_dataset_id FROM CONTROL.global_silver_schema_datasets "
        "WHERE LOWER(scope_owner) = LOWER(%s))",
        (client_id,),
    )
    _exec(
        "global_silver_schema_columns",
        "DELETE FROM CONTROL.global_silver_schema_columns WHERE silver_dataset_id IN ("
        "SELECT silver_dataset_id FROM CONTROL.global_silver_schema_datasets "
        "WHERE LOWER(scope_owner) = LOWER(%s))",
        (client_id,),
    )
    _exec(
        "global_silver_schema_tables",
        "DELETE FROM CONTROL.global_silver_schema_tables WHERE silver_dataset_id IN ("
        "SELECT silver_dataset_id FROM CONTROL.global_silver_schema_datasets "
        "WHERE LOWER(scope_owner) = LOWER(%s))",
        (client_id,),
    )
    _exec(
        "global_silver_schema_datasets",
        "DELETE FROM CONTROL.global_silver_schema_datasets " "WHERE LOWER(scope_owner) = LOWER(%s)",
        (client_id,),
    )
    _exec(
        "global_gold_schema_fields",
        "DELETE FROM CONTROL.global_gold_schema_fields WHERE gold_dataset_id IN ("
        "SELECT gold_dataset_id FROM CONTROL.global_gold_schema_datasets "
        "WHERE LOWER(scope_owner) = LOWER(%s))",
        (client_id,),
    )
    _exec(
        "global_gold_schema_datasets",
        "DELETE FROM CONTROL.global_gold_schema_datasets " "WHERE LOWER(scope_owner) = LOWER(%s)",
        (client_id,),
    )
    _exec(
        "dataset_pipeline_runs",
        "DELETE FROM CONTROL.dataset_pipeline_runs " "WHERE LOWER(client_id) = LOWER(%s)",
        (client_id,),
    )
    _exec(
        "client_pipeline_instances",
        "DELETE FROM CONTROL.client_pipeline_instances " "WHERE LOWER(client_id) = LOWER(%s)",
        (client_id,),
    )

    # Drop the client's three layered schemas (CASCADE removes child tables).
    for layer in ("BRONZE", "SILVER", "GOLD"):
        schema = f"{layer}_{cli_u}"
        if dry_run:
            print(f"    [DRY-RUN] would: DROP SCHEMA IF EXISTS {schema} CASCADE")
            continue
        try:
            cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
            deleted[f"DROP_SCHEMA_{layer}"] = 1
        except Exception as exc:
            deleted[f"DROP_SCHEMA_{layer}"] = -1
            print(f"    WARN: DROP {schema} failed ({type(exc).__name__}: {str(exc)[:100]})")

    return deleted


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("clients", nargs="*", help="client_id(s) to wipe")
    parser.add_argument(
        "--all-clients",
        action="store_true",
        help="Wipe every non-GLOBAL_CORP client found in CONTROL",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what WOULD be deleted without touching anything",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Skip the interactive confirmation prompt",
    )
    args = parser.parse_args()

    if not args.clients and not args.all_clients:
        parser.print_help()
        return 2

    conn = _connect()
    cur = conn.cursor()
    try:
        clients = _resolve_clients(
            cur,
            args_clients=[c.strip() for c in args.clients if c.strip()],
            all_clients=args.all_clients,
        )
        if not clients:
            print("No client_ids to wipe (after excluding GLOBAL_CORP).")
            return 0

        print("=" * 78)
        print(f"PLAN: wipe {len(clients)} client(s) — {clients}")
        print("=" * 78)
        for c in clients:
            print(f"\nInventory for {c!r}:")
            inv = _inventory(cur, c)
            for label, n in inv.items():
                bullet = (
                    "✓"
                    if (label.endswith("_exists") and n)
                    else "—"
                    if (label.endswith("_exists") and not n)
                    else "•"
                )
                print(f"  {bullet} {label:35s} {n}")

        if not args.dry_run and not args.yes:
            print()
            confirm = input(
                f"⚠️  About to DESTROY data for {clients}. Type 'YES' to proceed: "
            ).strip()
            if confirm != "YES":
                print("Aborted.")
                return 1

        print("\n" + "=" * 78)
        print("EXECUTING" if not args.dry_run else "DRY-RUN — NO WRITES")
        print("=" * 78)
        for c in clients:
            print(f"\n→ {c}")
            stats = _wipe_one(cur, c, dry_run=args.dry_run)
            for label, n in stats.items():
                print(f"    {label:35s} {n} row(s)/op")

        if not args.dry_run:
            conn.commit()
            print("\n✅ Wipe complete + committed.")
        else:
            print("\n(dry-run — nothing written)")
        return 0
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    sys.exit(main())
