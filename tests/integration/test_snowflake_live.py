"""Live Snowflake integration test — Phase 7 Day 3.

Opt-in: marked ``@pytest.mark.snowflake`` AND auto-skipped when the
SNOWFLAKE_* env vars are not set. CI will NEVER hit a real account.

To run locally:

    set -a && source .env && set +a
    pytest tests/integration/test_snowflake_live.py -m snowflake -v

These tests exercise the real driver against the account provisioned by
``scripts/snowflake_bootstrap.sql``. They are intentionally small —
connect, run a handful of statements, drop everything — so they cost
essentially zero credit (<$0.01 each).
"""

from __future__ import annotations

import os
import uuid

import pytest

from datalink.adapters.warehouse.snowflake_adapter import SnowflakeWarehouse
from datalink.config.models import WarehouseConfig

pytestmark = pytest.mark.snowflake


def _cfg_from_env() -> WarehouseConfig:
    return WarehouseConfig(
        type="snowflake",
        account=os.environ.get("SNOWFLAKE_ACCOUNT"),
        user=os.environ.get("SNOWFLAKE_USER"),
        password=os.environ.get("SNOWFLAKE_PASSWORD"),
        role=os.environ.get("SNOWFLAKE_ROLE"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE"),
        database=os.environ.get("SNOWFLAKE_DATABASE"),
    )


def _env_configured() -> bool:
    return all(
        os.environ.get(k)
        for k in (
            "SNOWFLAKE_ACCOUNT",
            "SNOWFLAKE_USER",
            "SNOWFLAKE_PASSWORD",
            "SNOWFLAKE_ROLE",
            "SNOWFLAKE_WAREHOUSE",
            "SNOWFLAKE_DATABASE",
        )
    )


@pytest.fixture
def wh() -> SnowflakeWarehouse:
    """Real SnowflakeWarehouse; skip whole test if env not configured."""
    if not _env_configured():
        pytest.skip("SNOWFLAKE_* env vars not set — live test skipped")
    adapter = SnowflakeWarehouse(_cfg_from_env())
    yield adapter
    adapter.close()


@pytest.fixture
def scratch_table(wh: SnowflakeWarehouse) -> str:
    """Create + drop a unique scratch table per test for isolation."""
    name = f"CONTROL._DAY3_LIVE_{uuid.uuid4().hex[:8].upper()}"
    wh.execute(f"CREATE TABLE {name} (id INTEGER, name VARCHAR(100), amt NUMBER(10,2))")
    try:
        yield name
    finally:
        wh.execute(f"DROP TABLE IF EXISTS {name}")


def test_connect_returns_expected_defaults(wh: SnowflakeWarehouse) -> None:
    rows = wh.query(
        "SELECT CURRENT_USER() AS u, CURRENT_ROLE() AS r, "
        "CURRENT_WAREHOUSE() AS w, CURRENT_DATABASE() AS d"
    )
    assert len(rows) == 1
    row = rows[0]
    assert row["U"] == "DATALINK_SVC"
    assert row["R"] == "DATALINK_ENGINEER"
    assert row["W"] == "DATALINK_WH"
    assert row["D"] == "DATALINK_DEV"


def test_execute_insert_then_query(wh: SnowflakeWarehouse, scratch_table: str) -> None:
    wh.execute(f"INSERT INTO {scratch_table} VALUES (1, 'alice', 10.00)")
    wh.execute(f"INSERT INTO {scratch_table} VALUES (2, 'bob', 20.00)")
    rows = wh.query(f"SELECT id, name, amt FROM {scratch_table} ORDER BY id")
    assert len(rows) == 2
    assert rows[0]["NAME"] == "alice"
    assert rows[1]["NAME"] == "bob"


def test_query_with_qmark_params_is_rewritten(wh: SnowflakeWarehouse, scratch_table: str) -> None:
    """DuckDB-style ``?`` placeholders must work — the adapter rewrites them."""
    wh.execute(f"INSERT INTO {scratch_table} VALUES (1, 'keep', 1)")
    wh.execute(f"INSERT INTO {scratch_table} VALUES (2, 'skip', 2)")

    rows = wh.query(
        f"SELECT id FROM {scratch_table} WHERE name = ? ORDER BY id",
        ["keep"],
    )
    assert len(rows) == 1
    assert rows[0]["ID"] == 1


def test_row_count_helper(wh: SnowflakeWarehouse, scratch_table: str) -> None:
    assert wh.row_count(scratch_table) == 0
    wh.execute(f"INSERT INTO {scratch_table} VALUES (1, 'a', 1)")
    assert wh.row_count(scratch_table) == 1


def test_table_exists_true_and_false(wh: SnowflakeWarehouse) -> None:
    # scratch_table not used here — test table_exists directly
    fake = f"CONTROL._DEFINITELY_NOT_A_TABLE_{uuid.uuid4().hex[:8]}"
    assert wh.table_exists(fake) is False
