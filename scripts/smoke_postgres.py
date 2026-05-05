"""Smoke test for PostgresOperationalDb — idempotent upsert round-trip."""

from __future__ import annotations

from datalink.adapters.operational_db.postgres import PostgresOperationalDb
from datalink.config.models import OperationalDbConfig


def main() -> None:
    cfg = OperationalDbConfig(
        type="postgres",
        host="localhost",
        port=5434,  # see docker-compose.yml — remapped to avoid Windows-native PG on :5432
        user="datalink",
        password="datalink_local_only",
        database="datalink_um",
    )
    pg = PostgresOperationalDb("postgres", cfg)

    pg.execute_script(
        "CREATE SCHEMA IF NOT EXISTS test_p4;"
        "CREATE TABLE IF NOT EXISTS test_p4.ping (id INTEGER PRIMARY KEY, v VARCHAR(20));"
    )

    rows = [{"id": 1, "v": "hello"}, {"id": 2, "v": "world"}]

    pg.bulk_upsert("ping", rows, ["id"], schema="test_p4")
    count_first = pg.table_row_count("ping", schema="test_p4")
    print(f"after first upsert  : {count_first} rows")

    # Re-upsert same rows — must be idempotent (count unchanged).
    pg.bulk_upsert("ping", rows, ["id"], schema="test_p4")
    count_second = pg.table_row_count("ping", schema="test_p4")
    print(f"after second upsert : {count_second} rows  (should equal first)")

    assert count_first == count_second == 2, (
        f"idempotency broken: first={count_first} second={count_second}"
    )

    # Now change a value and re-upsert → count same, value updated
    updated = [{"id": 1, "v": "HELLO"}, {"id": 2, "v": "world"}]
    pg.bulk_upsert("ping", updated, ["id"], schema="test_p4")
    count_third = pg.table_row_count("ping", schema="test_p4")
    print(f"after update upsert : {count_third} rows  (should still be 2)")
    assert count_third == 2

    pg.execute("DROP SCHEMA test_p4 CASCADE")
    pg.close()
    print("PostgresOperationalDb round-trip GREEN")


if __name__ == "__main__":
    main()
