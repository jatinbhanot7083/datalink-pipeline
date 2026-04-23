"""Tests for datalink.adapters.warehouse.snowflake_adapter — Phase 7 Day 3.

All tests use a mocked `snowflake.connector` module so they run without
network access or Snowflake credentials. A separate integration test
(``@pytest.mark.snowflake``, opt-in) hits a real account.
"""

from __future__ import annotations

import sys
import types
from typing import Any
from unittest.mock import MagicMock

import pytest

from datalink.adapters.warehouse.snowflake_adapter import (
    SnowflakeWarehouse,
    _rewrite_qmark_to_pyformat,
    _stage_path,
)
from datalink.config.models import WarehouseConfig

# ---------------------------------------------------------------------------
# Pure-function helpers — zero fixtures needed
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_rewrite_qmark_to_pyformat_single_placeholder() -> None:
    assert _rewrite_qmark_to_pyformat("SELECT * FROM t WHERE id = ?") == (
        "SELECT * FROM t WHERE id = %s"
    )


@pytest.mark.unit
def test_rewrite_qmark_to_pyformat_multiple_placeholders() -> None:
    sql = "WHERE a = ? AND b = ? AND c = ?"
    assert _rewrite_qmark_to_pyformat(sql) == "WHERE a = %s AND b = %s AND c = %s"


@pytest.mark.unit
def test_rewrite_qmark_to_pyformat_no_placeholders() -> None:
    assert _rewrite_qmark_to_pyformat("SELECT 1") == "SELECT 1"


@pytest.mark.unit
def test_stage_path_adds_leading_at() -> None:
    assert _stage_path("STAGE/foo.csv") == "@STAGE/foo.csv"


@pytest.mark.unit
def test_stage_path_preserves_existing_at() -> None:
    assert _stage_path("@STAGE/foo.csv") == "@STAGE/foo.csv"


# ---------------------------------------------------------------------------
# Adapter-level tests with a mocked Snowflake connector
# ---------------------------------------------------------------------------


@pytest.fixture
def cfg_full() -> WarehouseConfig:
    return WarehouseConfig(
        type="snowflake",
        account="acct.east-us-2.azure",
        user="DATALINK_SVC",
        password="secret",
        role="DATALINK_ENGINEER",
        warehouse="DATALINK_WH",
        database="DATALINK_DEV",
    )


@pytest.fixture
def mock_snowflake(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    """Install a fake ``snowflake.connector`` module into sys.modules.

    The deferred import inside ``SnowflakeWarehouse._connect`` picks up this
    mock, so no live account is touched.
    """
    mock_conn = MagicMock(name="SnowflakeConnection")
    mock_cursor = MagicMock(name="Cursor")
    mock_conn.cursor.return_value = mock_cursor

    # Make cursor iterable-friendly: description + fetchall return sane defaults
    mock_cursor.description = [("C",)]
    mock_cursor.fetchall.return_value = [(0,)]
    mock_cursor.execute = MagicMock(return_value=None)
    mock_cursor.close = MagicMock(return_value=None)

    fake_module = types.ModuleType("snowflake")
    fake_connector = types.ModuleType("snowflake.connector")
    fake_connector.connect = MagicMock(return_value=mock_conn)  # type: ignore[attr-defined]
    fake_module.connector = fake_connector  # type: ignore[attr-defined]

    monkeypatch.setitem(sys.modules, "snowflake", fake_module)
    monkeypatch.setitem(sys.modules, "snowflake.connector", fake_connector)

    mock_conn.cursor.mock_cursor = mock_cursor  # type: ignore[attr-defined]  # for test access
    return mock_conn


@pytest.mark.unit
def test_connect_raises_with_clear_message_when_fields_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """WarehouseConfig without Snowflake creds AND no SNOWFLAKE_* env vars
    should fail-fast at _connect with a helpful diagnostic."""
    # Scrub env vars so the fallback can't satisfy the missing creds.
    for name in (
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_ROLE",
        "SNOWFLAKE_WAREHOUSE",
        "SNOWFLAKE_DATABASE",
    ):
        monkeypatch.delenv(name, raising=False)
    cfg = WarehouseConfig(type="snowflake")  # all optional fields default to None
    wh = SnowflakeWarehouse(cfg)
    with pytest.raises(RuntimeError, match="missing required credential"):
        wh.execute("SELECT 1")


@pytest.mark.unit
def test_execute_with_dict_params_passes_through_unchanged(
    mock_snowflake: MagicMock, cfg_full: WarehouseConfig
) -> None:
    wh = SnowflakeWarehouse(cfg_full)
    params = {"client_id": "aetna"}
    wh.execute("UPDATE t SET x = 1 WHERE client_id = %(client_id)s", params)
    cur = mock_snowflake.cursor.return_value
    cur.execute.assert_called_once_with(
        "UPDATE t SET x = 1 WHERE client_id = %(client_id)s", params
    )


@pytest.mark.unit
def test_execute_with_list_params_rewrites_qmark(
    mock_snowflake: MagicMock, cfg_full: WarehouseConfig
) -> None:
    """DuckDB-style ``?`` placeholders must become ``%s`` on Snowflake."""
    wh = SnowflakeWarehouse(cfg_full)
    wh.execute("UPDATE t SET x = ? WHERE id = ?", ["newval", 42])
    cur = mock_snowflake.cursor.return_value
    cur.execute.assert_called_once_with("UPDATE t SET x = %s WHERE id = %s", ["newval", 42])


@pytest.mark.unit
def test_query_returns_list_of_dicts_with_lowercase_keys(
    mock_snowflake: MagicMock, cfg_full: WarehouseConfig
) -> None:
    """SnowflakeWarehouse.query lowercases column names so callers written
    against DuckDB (which preserves case) keep working on Snowflake
    (which uppercases by default)."""
    cur = mock_snowflake.cursor.return_value
    cur.description = [("USER_ID",), ("NAME",)]
    cur.fetchall.return_value = [(1, "a"), (2, "b")]

    wh = SnowflakeWarehouse(cfg_full)
    rows = wh.query("SELECT user_id, name FROM t")

    assert rows == [{"user_id": 1, "name": "a"}, {"user_id": 2, "name": "b"}]


@pytest.mark.unit
def test_query_with_no_results(mock_snowflake: MagicMock, cfg_full: WarehouseConfig) -> None:
    cur = mock_snowflake.cursor.return_value
    cur.description = [("C",)]
    cur.fetchall.return_value = []

    wh = SnowflakeWarehouse(cfg_full)
    rows = wh.query("SELECT COUNT(*) AS c FROM t")
    assert rows == []


@pytest.mark.unit
def test_copy_from_stage_csv_uses_named_format(
    mock_snowflake: MagicMock, cfg_full: WarehouseConfig
) -> None:
    cur = mock_snowflake.cursor.return_value
    # row_count() calls query() which fetchall's — before, after
    cur.description = [("C",)]
    # First call (before) = 0 rows, second call (COPY INTO, returns nothing
    # meaningful for us), third call (after) = 5 rows.
    cur.fetchall.side_effect = [[(0,)], [(5,)]]

    wh = SnowflakeWarehouse(cfg_full)
    loaded = wh.copy_from_stage("DATALINK_STAGE/claims.csv", "BRONZE.RAW_CLAIMS")

    # Verify COPY INTO was called with an @-prefixed stage and a CSV file format
    copy_calls = [c for c in cur.execute.call_args_list if "COPY INTO" in str(c)]
    assert len(copy_calls) == 1
    sql_called = copy_calls[0].args[0]
    assert "FROM @DATALINK_STAGE/claims.csv" in sql_called
    assert "TYPE = 'CSV'" in sql_called
    assert "SKIP_HEADER = 1" in sql_called
    assert loaded == 5


@pytest.mark.unit
def test_copy_from_stage_rejects_unknown_format(
    mock_snowflake: MagicMock, cfg_full: WarehouseConfig
) -> None:
    wh = SnowflakeWarehouse(cfg_full)
    with pytest.raises(NotImplementedError, match=r"file_format=.*not supported"):
        wh.copy_from_stage("STAGE/x", "T", file_format="avro")


@pytest.mark.unit
def test_merge_requires_key_columns(mock_snowflake: MagicMock, cfg_full: WarehouseConfig) -> None:
    wh = SnowflakeWarehouse(cfg_full)
    with pytest.raises(ValueError, match="at least one key column"):
        wh.merge("TARGET", "SRC", [])


@pytest.mark.unit
def test_merge_builds_snowflake_merge_sql(
    mock_snowflake: MagicMock, cfg_full: WarehouseConfig
) -> None:
    cur = mock_snowflake.cursor.return_value

    # Sequence of calls made by merge():
    #   1. DESCRIBE TABLE target  → description=[("name",), ("type",)...] , fetchall → rows
    #   2. MERGE INTO              → fetchall unused
    #   3. SELECT COUNT(*) FROM source → returns 7
    describe_desc: list[tuple[str, ...]] = [
        ("name",),
        ("type",),
        ("kind",),
        ("null?",),
    ]
    describe_rows = [
        ("claim_id", "VARCHAR", "COLUMN", "N"),
        ("member_id", "VARCHAR", "COLUMN", "N"),
        ("amount", "NUMBER", "COLUMN", "Y"),
    ]
    count_desc = [("C",)]

    descriptions = [describe_desc, None, count_desc]
    fetches = [describe_rows, [(7,)]]
    description_iter = iter(descriptions)
    fetch_iter = iter(fetches)

    def _descr_side_effect(_sql: str, *_a: Any, **_k: Any) -> None:
        cur.description = next(description_iter)

    def _fetch_side_effect() -> list[Any]:
        return next(fetch_iter)

    cur.execute.side_effect = _descr_side_effect
    cur.fetchall.side_effect = _fetch_side_effect

    wh = SnowflakeWarehouse(cfg_full)
    wh.merge("BRONZE.RAW_CLAIMS", "STAGING.RAW_CLAIMS_SRC", ["claim_id"])

    # Pull out the MERGE INTO call
    merge_calls = [c for c in cur.execute.call_args_list if "MERGE INTO" in str(c)]
    assert len(merge_calls) == 1
    sql = merge_calls[0].args[0]
    assert "MERGE INTO BRONZE.RAW_CLAIMS AS target" in sql
    assert "USING STAGING.RAW_CLAIMS_SRC AS source" in sql
    assert "ON target.claim_id = source.claim_id" in sql
    assert (
        "WHEN MATCHED THEN UPDATE SET member_id = source.member_id, amount = source.amount" in sql
    )
    assert "WHEN NOT MATCHED THEN INSERT (claim_id, member_id, amount)" in sql


@pytest.mark.unit
def test_close_is_idempotent(mock_snowflake: MagicMock, cfg_full: WarehouseConfig) -> None:
    wh = SnowflakeWarehouse(cfg_full)
    wh.execute("SELECT 1")  # open connection
    wh.close()
    wh.close()  # second close must not raise
    mock_snowflake.close.assert_called_once()


@pytest.mark.unit
def test_close_without_ever_connecting_is_noop(cfg_full: WarehouseConfig) -> None:
    wh = SnowflakeWarehouse(cfg_full)
    wh.close()  # no _conn was ever built — must not raise


@pytest.mark.unit
def test_table_exists_returns_true_on_success(
    mock_snowflake: MagicMock, cfg_full: WarehouseConfig
) -> None:
    cur = mock_snowflake.cursor.return_value
    cur.description = []
    cur.fetchall.return_value = []

    wh = SnowflakeWarehouse(cfg_full)
    assert wh.table_exists("BRONZE.RAW_CLAIMS") is True


@pytest.mark.unit
def test_table_exists_returns_false_on_exception(
    mock_snowflake: MagicMock, cfg_full: WarehouseConfig
) -> None:
    cur = mock_snowflake.cursor.return_value
    cur.execute.side_effect = RuntimeError("table does not exist")

    wh = SnowflakeWarehouse(cfg_full)
    assert wh.table_exists("MISSING.TABLE") is False
