"""Phase 17.10 — re-sync dags/ folder with reality.

What lives in dags/ should be exactly what Airflow needs to scan, and no
more.  This script enforces that contract by:

  KEEPS (always):
    * dags/__init__.py
    * dags/_factory_common.py        — shared list_live_instances + builder
    * dags/_ops_publish_templates.py — global publish ops DAG
    * dags/overrides/__init__.py     — placeholder for per-dataset hooks

  KEEPS (instance-aware):
    * dags/_factory_<dataset>.py  — ONLY for datasets that have at least
                                     one non-archived row in
                                     CONTROL.client_pipeline_instances.
                                     Other dataset factories are dead
                                     weight (zero DAGs registered) — they
                                     just litter the DAG bag with empty
                                     module loads.

  REMOVES:
    * dags/<client>_<dataset>_pipeline.py — hand-written instance DAGs
      from approve_and_deploy.  Factory pattern emits the same DAG; two
      paths colliding cause Airflow import-errors.
    * dags/_factory_<dataset>.py for datasets with NO instances.
    * Phase-5 legacy DAGs: bronze_ingest_dag.py, silver_transform_dag.py,
      gold_um_push_dag.py, _dag_builder.py.

Run this:
    python scripts/cleanup_dags.py            # remove what's not needed
    python scripts/cleanup_dags.py --dry-run  # show what would change
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
DAGS = REPO / "dags"

# Hard-keep regardless of state.
ALWAYS_KEEP = {
    "__init__.py",
    "_factory_common.py",
    "_ops_publish_templates.py",
}

# Phase-5 legacy artifacts — superseded by the factory pattern in 17.7+.
LEGACY_FILES = {
    "_dag_builder.py",
    "bronze_ingest_dag.py",
    "silver_transform_dag.py",
    "gold_um_push_dag.py",
}


def _live_dataset_codes() -> set[str]:
    """Return dataset_codes that have at least one non-archived instance."""
    try:
        import snowflake.connector
    except ImportError:
        return set()
    try:
        conn = snowflake.connector.connect(
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            user=os.environ["SNOWFLAKE_USER"],
            password=os.environ["SNOWFLAKE_PASSWORD"],
            role=os.environ["SNOWFLAKE_ROLE"],
            warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
            database=os.environ["SNOWFLAKE_DATABASE"],
        )
    except Exception as exc:
        print(f"  (Snowflake connect failed: {exc!s} — assuming no datasets active)")
        return set()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT DISTINCT LOWER(dataset_code) "
            "FROM CONTROL.client_pipeline_instances "
            "WHERE status <> 'ARCHIVED'"
        )
        return {r[0] for r in cur.fetchall() if r[0]}
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


def _client_dataset_pairs() -> set[tuple[str, str]]:
    """Return (client_id, dataset_code) pairs for all non-archived instances —
    used to identify hand-written `<client>_<dataset>_pipeline.py` files
    that we want to delete (factory replaces them).  Plus a guard: don't
    delete a hand-written DAG for an instance that the factory does NOT
    yet emit.  In our flow the factory ALWAYS emits, so safe to delete."""
    try:
        import snowflake.connector
    except ImportError:
        return set()
    try:
        conn = snowflake.connector.connect(
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            user=os.environ["SNOWFLAKE_USER"],
            password=os.environ["SNOWFLAKE_PASSWORD"],
            role=os.environ["SNOWFLAKE_ROLE"],
            warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
            database=os.environ["SNOWFLAKE_DATABASE"],
        )
    except Exception:
        return set()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT DISTINCT LOWER(client_id), LOWER(dataset_code) "
            "FROM CONTROL.client_pipeline_instances "
            "WHERE status <> 'ARCHIVED'"
        )
        return {(r[0], r[1]) for r in cur.fetchall() if r[0] and r[1]}
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--dry-run", action="store_true", help="Show actions, write nothing")
    args = p.parse_args()

    if not DAGS.exists():
        print(f"ERROR: {DAGS} does not exist.")
        return 1

    live = _live_dataset_codes()
    pairs = _client_dataset_pairs()
    print(f"Active datasets: {sorted(live)}")
    print(f"Active (client, dataset) pairs: {sorted(pairs)}")
    print()

    deletions: list[Path] = []

    for entry in sorted(DAGS.iterdir()):
        if entry.is_dir():
            # Skip __pycache__ and overrides/
            continue
        name = entry.name

        if name in ALWAYS_KEEP:
            continue

        # Phase-5 legacy
        if name in LEGACY_FILES:
            deletions.append(entry)
            continue

        # _factory_<dataset>.py
        if name.startswith("_factory_") and name.endswith(".py"):
            ds = name[len("_factory_") : -len(".py")]
            if ds not in live:
                deletions.append(entry)
            continue

        # <client>_<dataset>_pipeline.py — hand-written instance DAGs.
        # Always remove: factory emits the same DAG.
        if name.endswith("_pipeline.py"):
            stem = name[: -len("_pipeline.py")]
            # Format check: <client>_<dataset> — at least one underscore.
            if "_" in stem:
                deletions.append(entry)
                continue

        # Anything else — leave untouched (operator may have added it).

    if not deletions:
        print("Nothing to clean — dags/ already in sync.")
        return 0

    print(f"Will {'(dry-run) ' if args.dry_run else ''}delete {len(deletions)} files:")
    for p in deletions:
        print(f"  - {p.relative_to(REPO)}")

    if args.dry_run:
        return 0

    for p in deletions:
        try:
            p.unlink()
        except OSError as exc:
            print(f"  WARN: could not delete {p}: {exc}")

    # Also clean up __pycache__ for safety (Airflow may have cached imports)
    pycache = DAGS / "__pycache__"
    if pycache.exists():
        for pyc in pycache.glob("*.pyc"):
            with __import__("contextlib").suppress(OSError):
                pyc.unlink()

    print(f"\nDeleted {len(deletions)} file(s) + cleared __pycache__.")
    print("Airflow will pick up the change on the next DAG file scan (≤ 30 s).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
