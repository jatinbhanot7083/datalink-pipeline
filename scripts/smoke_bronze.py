"""End-to-end Bronze smoke test.

Uploads the 3 sample CSVs to SFTP, runs Bronze ingest for each, verifies
DuckDB row counts + idempotency (re-running is a no-op).

Exits 0 on success, non-zero on any assertion failure. Designed to be wired
into `make verify-phase-2`.
"""

from __future__ import annotations

import shutil
import sys
from pathlib import Path

from datalink.adapters.factory import build_adapters
from datalink.adapters.sftp.atmoz import AtmozSftpSource
from datalink.config.loader import load_settings
from datalink.logging import configure_logging, get_logger
from datalink.pipeline.bronze import ingest_file
from datalink.pipeline.bronze.ddl_loader import BRONZE_TABLES, qualified_name


def main() -> int:
    configure_logging(level="INFO", fmt="console")
    log = get_logger("smoke_bronze")

    settings = load_settings(env="local")
    adapters = build_adapters(settings)

    sample_dir = Path(__file__).parent.parent / "data" / "sample"
    uploads = [
        ("PROVIDER", "provider_sample.csv", 500),
        ("MEMBERSHIP", "membership_sample.csv", 2_000),
        ("CLAIMS", "claims_sample.csv", 10_000),
    ]

    # Reset the warehouse for a clean run.
    wh_path = Path(settings.adapters.warehouse.path)
    if wh_path.exists() and wh_path.is_file():
        wh_path.unlink()
        log.info("reset warehouse", path=str(wh_path))

    # Reset the localfs object store for a clean run (only if it's a file:// endpoint).
    if settings.adapters.object_store.endpoint.startswith("file://"):
        root = (
            Path(settings.adapters.object_store.endpoint[len("file://") :])
            / settings.adapters.object_store.container
        )
        if root.exists():
            shutil.rmtree(root)
            log.info("reset object_store", path=str(root))

    # Upload sample CSVs to SFTP using the (not-protocol-but-handy) upload helper.
    sftp = adapters.sftp
    if not isinstance(sftp, AtmozSftpSource):
        print(f"FAIL: expected AtmozSftpSource, got {type(sftp).__name__}", file=sys.stderr)
        return 1
    for _, filename, _ in uploads:
        local = sample_dir / filename
        remote = sftp.upload(local)
        log.info("uploaded", local=str(local), remote=remote)

    # Ingest each file.
    failures: list[str] = []
    for source_type, filename, expected_rows in uploads:
        remote_path = f"{settings.adapters.sftp.remote_base_dir}/{filename}"
        result1 = ingest_file(adapters, source_type, remote_path)
        print(
            f"  {source_type:12}  pass 1: rows_in_source={result1.rows_in_source:>6}  target_after={result1.rows_in_target_after:>6}"
        )
        if result1.rows_in_source != expected_rows:
            failures.append(
                f"{source_type}: expected {expected_rows} source rows, got {result1.rows_in_source}"
            )
        if result1.rows_in_target_after != expected_rows:
            failures.append(
                f"{source_type}: expected {expected_rows} target rows after first load, got {result1.rows_in_target_after}"
            )

        # Re-run — idempotency check. Row count must NOT grow.
        result2 = ingest_file(adapters, source_type, remote_path)
        print(
            f"  {source_type:12}  pass 2: rows_in_source={result2.rows_in_source:>6}  target_after={result2.rows_in_target_after:>6}  (idempotency)"
        )
        if result2.rows_in_target_after != expected_rows:
            failures.append(
                f"{source_type}: non-idempotent re-run — target grew from {expected_rows} to {result2.rows_in_target_after}"
            )

    # Final verify: row counts in target tables.
    for source_type, _, expected_rows in uploads:
        table = qualified_name(BRONZE_TABLES[source_type])
        rows = adapters.warehouse.query(f"SELECT COUNT(*) AS c FROM {table}")
        actual = int(rows[0]["c"]) if rows else -1
        print(f"  {table:30}  rows = {actual}")
        if actual != expected_rows:
            failures.append(f"{table}: expected {expected_rows}, got {actual}")

    adapters.warehouse.close()
    adapters.sftp.close()

    if failures:
        print(f"\nFAIL: {len(failures)} assertion(s) failed:", file=sys.stderr)
        for f in failures:
            print(f"  - {f}", file=sys.stderr)
        return 1

    print("\n" + "=" * 60)
    print("BRONZE INGEST SMOKE TEST GREEN")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
