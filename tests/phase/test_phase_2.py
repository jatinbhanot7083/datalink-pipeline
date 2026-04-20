"""Phase 2 structural + unit verification — no Docker required.

These tests run under `make verify-phase-2-code`. They don't exercise
real services; they prove the Bronze code paths, config, and DDL are
internally consistent. The end-to-end smoke (real SFTP + LocalFs +
DuckDB with 12k sample rows + idempotency proof) runs under
`make verify-phase-2-demo`.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from datalink.adapters.factory import build_adapters
from datalink.adapters.object_store.localfs import LocalFsObjectStore
from datalink.adapters.warehouse.duckdb_adapter import DuckDBWarehouse
from datalink.config.loader import load_settings
from datalink.config.models import ObjectStoreConfig, WarehouseConfig
from datalink.pipeline.bronze import ingest_file
from datalink.pipeline.bronze.ddl_loader import (
    BRONZE_TABLES,
    DDL_DIR,
    BronzeTable,
    create_bronze_schema,
    qualified_name,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
SAMPLE_DIR = REPO_ROOT / "data" / "sample"


# ---------------------------------------------------------------------------
# Static / structural
# ---------------------------------------------------------------------------


@pytest.mark.phase
def test_bronze_tables_dict_complete() -> None:
    """All three source types from the Medallion doc must have a BronzeTable entry."""
    assert set(BRONZE_TABLES) == {"CLAIMS", "MEMBERSHIP", "PROVIDER"}


@pytest.mark.phase
@pytest.mark.parametrize("source_type", ["CLAIMS", "MEMBERSHIP", "PROVIDER"])
def test_bronze_ddl_files_exist(source_type: str) -> None:
    table: BronzeTable = BRONZE_TABLES[source_type]
    ddl = DDL_DIR / table.ddl_filename
    assert ddl.exists(), f"Missing DDL: {ddl}"
    body = ddl.read_text(encoding="utf-8")
    assert "{schema}" in body, f"DDL {ddl} must parameterize the schema with {{schema}}"
    assert "_load_dt" in body
    assert "_source_file" in body
    assert "_batch_id" in body
    assert "_record_source" in body
    for key in table.key_columns:
        assert key in body, f"DDL {ddl} does not reference key column {key!r}"


@pytest.mark.phase
@pytest.mark.parametrize(
    "filename, min_rows",
    [
        # Phase 5.7 uplifted to the executive-demo dataset (100K/20K/5K).
        # Tests assert a *minimum* rowcount so teams can regenerate at either
        # the small (10K/2K/500) or large (100K/20K/5K) scale without breaking.
        ("provider_sample.csv", 500),
        ("membership_sample.csv", 2_000),
        ("claims_sample.csv", 10_000),
    ],
)
def test_sample_data_committed(filename: str, min_rows: int) -> None:
    """Seeded sample CSVs must be committed — teammates need them on clone."""
    f = SAMPLE_DIR / filename
    assert f.exists(), f"Missing sample: {f}"
    with f.open(encoding="utf-8") as fh:
        row_count = sum(1 for _ in fh) - 1  # minus header
    assert row_count >= min_rows, f"{filename}: expected at least {min_rows} rows, got {row_count}"


@pytest.mark.phase
def test_local_env_uses_localfs_object_store(clean_dl_env: None) -> None:
    """Default local env uses LocalFsObjectStore (offline-friendly)."""
    settings = load_settings(env="local")
    adapters = build_adapters(settings)
    assert isinstance(adapters.object_store, LocalFsObjectStore)


# ---------------------------------------------------------------------------
# LocalFsObjectStore unit
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_localfs_round_trip(tmp_path: Path) -> None:
    cfg = ObjectStoreConfig(
        type="localfs",
        endpoint=f"file://{tmp_path}",
        container="smoke",
    )
    store = LocalFsObjectStore(cfg)

    src = tmp_path / "input.txt"
    src.write_text("payload", encoding="utf-8")

    store.put(src, "nested/key.txt")
    assert store.exists("nested/key.txt")

    dl = tmp_path / "out.txt"
    store.get("nested/key.txt", dl)
    assert dl.read_text(encoding="utf-8") == "payload"

    items = store.list("nested/")
    assert len(items) == 1 and items[0].key == "nested/key.txt"
    assert items[0].size == len(b"payload")

    store.delete("nested/key.txt")
    assert not store.exists("nested/key.txt")


@pytest.mark.unit
def test_localfs_rejects_path_traversal(tmp_path: Path) -> None:
    cfg = ObjectStoreConfig(type="localfs", endpoint=f"file://{tmp_path}", container="c")
    store = LocalFsObjectStore(cfg)
    with pytest.raises(ValueError, match="escapes container"):
        store._resolve("../../../etc/passwd")


# ---------------------------------------------------------------------------
# Bronze ingest with mocked SFTP + real LocalFs + real DuckDB :memory:
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_bronze_ingest_end_to_end_with_mock_sftp(tmp_path: Path) -> None:
    """Exercise ingest_file() with a real LocalFs + DuckDB (:memory:), mocking only SFTP."""
    # Copy the provider sample into a fake "SFTP drop" location.
    src = SAMPLE_DIR / "provider_sample.csv"
    drop = tmp_path / "drop"
    drop.mkdir()
    remote_file = drop / "provider_sample.csv"
    remote_file.write_bytes(src.read_bytes())

    # Build real adapters for this test.
    wh_cfg = WarehouseConfig(type="duckdb", path=":memory:")
    warehouse = DuckDBWarehouse(wh_cfg)

    os_cfg = ObjectStoreConfig(
        type="localfs",
        endpoint=f"file://{tmp_path}/obj",
        container="datalink-raw",
    )
    object_store = LocalFsObjectStore(os_cfg)

    # Mock SFTP: download() just copies from our fake drop to the local dest.
    sftp = MagicMock()

    def fake_download(remote_path: str, local_dest: Path) -> None:
        local_dest.parent.mkdir(parents=True, exist_ok=True)
        local_dest.write_bytes((drop / Path(remote_path).name).read_bytes())

    sftp.download.side_effect = fake_download

    # Assemble an AdapterSet-like object. Using duck-typing; ingest_file only
    # touches .sftp, .object_store, .warehouse.
    class _Adapters:
        pass

    adapters = _Adapters()
    adapters.sftp = sftp
    adapters.object_store = object_store
    adapters.warehouse = warehouse

    # Use whatever's actually in the committed sample — at Phase 2 scale that
    # was 500; Phase 5.7 ships 5,000. Either way the test is "rows go in,
    # idempotent on re-run" — not a specific count.
    with src.open(encoding="utf-8") as fh:
        expected_rows = sum(1 for _ in fh) - 1  # minus header

    # First pass — all providers loaded.
    r1 = ingest_file(adapters, "PROVIDER", "/drop/provider_sample.csv")
    assert r1.rows_in_source == expected_rows
    assert r1.rows_in_target_after == expected_rows
    assert r1.rows_affected == expected_rows

    # Second pass — idempotent, no new rows.
    r2 = ingest_file(adapters, "PROVIDER", "/drop/provider_sample.csv")
    assert r2.rows_in_source == expected_rows
    assert r2.rows_in_target_after == expected_rows  # unchanged
    assert r2.rows_affected == 0  # idempotency

    warehouse.close()


@pytest.mark.unit
def test_create_bronze_schema_idempotent() -> None:
    wh = DuckDBWarehouse(WarehouseConfig(type="duckdb", path=":memory:"))
    create_bronze_schema(wh)
    # Calling again must be a no-op (IF NOT EXISTS in DDL).
    create_bronze_schema(wh)
    # All 3 tables should exist.
    for _source_type, table in BRONZE_TABLES.items():
        q = qualified_name(table)
        rows = wh.query(f"SELECT COUNT(*) AS c FROM {q}")
        assert rows[0]["c"] == 0
    wh.close()


@pytest.mark.unit
def test_ingest_file_rejects_unknown_source_type() -> None:
    with pytest.raises(ValueError, match="Unknown source_type"):
        ingest_file(MagicMock(), "BANANAS", "/drop/file.csv")
