"""Phase 4 structural + unit verification — no services required.

The real end-to-end router test (both Postgres targets, config flip) runs
under `make verify-phase-4-router` — see scripts/smoke_router.py.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest
import yaml

from datalink.adapters.operational_db.postgres import PostgresOperationalDb
from datalink.adapters.operational_db.sqlserver import SqlServerOperationalDb
from datalink.config.loader import load_settings
from datalink.config.models import OperationalDbConfig
from datalink.pipeline.router import (
    GOLD_UM_TABLES,
    PushResult,
    push_gold_um_to_operational,
)
from datalink.pipeline.router.schema import (
    ALL_UM_TABLES,
    GOLD_UM_LOOKUPS,
    postgres_ddl,
    sqlserver_ddl,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
DBT_DIR = REPO_ROOT / "dbt"
GOLD_DIR = DBT_DIR / "models" / "gold" / "um_operational"


# ---------------------------------------------------------------------------
# Gold dbt project structure
# ---------------------------------------------------------------------------


@pytest.mark.phase
@pytest.mark.parametrize(
    "model",
    [
        "gold_patient_auth",
        "gold_auth_decision",
        "gold_auth_code",
        "gold_auth_diagnoses",
        "gold_auth_provider",
    ],
)
def test_gold_model_exists(model: str) -> None:
    f = GOLD_DIR / f"{model}.sql"
    assert f.exists(), f"Missing Gold model: {f}"
    body = f.read_text(encoding="utf-8")
    assert "materialized='table'" in body, f"{model} must be materialized=table"
    assert "schema='gold_um'" in body, f"{model} must write to gold_um schema"


@pytest.mark.phase
@pytest.mark.parametrize(
    "seed",
    ["lu_auth_status.csv", "lu_decision_status.csv", "lu_auth_type.csv"],
)
def test_gold_seed_exists(seed: str) -> None:
    f = DBT_DIR / "seeds" / seed
    assert f.exists(), f"Missing seed: {f}"
    lines = f.read_text(encoding="utf-8").splitlines()
    assert len(lines) >= 6, f"{seed}: expected header + >=5 rows, got {len(lines)}"


@pytest.mark.phase
def test_gold_schema_yml_wires_all_five_models_and_three_seeds() -> None:
    schema = yaml.safe_load((GOLD_DIR / "schema.yml").read_text(encoding="utf-8"))
    model_names = {m["name"] for m in schema["models"]}
    assert model_names == {
        "gold_patient_auth",
        "gold_auth_decision",
        "gold_auth_code",
        "gold_auth_diagnoses",
        "gold_auth_provider",
    }
    seed_names = {s["name"] for s in schema["seeds"]}
    assert seed_names == {"lu_auth_status", "lu_decision_status", "lu_auth_type"}


# ---------------------------------------------------------------------------
# Router schema / DDL
# ---------------------------------------------------------------------------


@pytest.mark.phase
def test_all_um_tables_in_router_schema() -> None:
    targets = {t.target for t in ALL_UM_TABLES}
    assert targets == {
        "lu_auth_status",
        "lu_decision_status",
        "lu_auth_type",
        "patient_auth",
        "auth_decision",
        "auth_code",
        "auth_diagnoses",
        "auth_provider",
    }
    # All lookups flagged
    assert {t.target for t in GOLD_UM_LOOKUPS} == {
        "lu_auth_status",
        "lu_decision_status",
        "lu_auth_type",
    }


@pytest.mark.unit
def test_postgres_ddl_creates_all_tables() -> None:
    sql = postgres_ddl()
    assert "CREATE SCHEMA IF NOT EXISTS um" in sql
    for tbl in ALL_UM_TABLES:
        assert (
            f"CREATE TABLE IF NOT EXISTS um.{tbl.target} (" in sql
        ), f"{tbl.target} missing from Postgres DDL"
        for col_name, _ in tbl.columns:
            assert col_name in sql


@pytest.mark.unit
def test_sqlserver_ddl_uses_bracket_pascal_identifiers() -> None:
    sql = sqlserver_ddl()
    assert "CREATE SCHEMA [UM]" in sql
    # Per UM-Gold-v2 §5, SQL Server uses [Schema].[PascalCase].
    assert "[UM].[PatientAuth]" in sql
    assert "[UM].[AuthDecision]" in sql
    assert "[UM].[LuAuthStatus]" in sql
    # BOOLEAN should be mapped to BIT.
    assert "BIT" in sql or "bit" in sql
    # TIMESTAMP → DATETIME2
    assert "DATETIME2" in sql


# ---------------------------------------------------------------------------
# Router fan-out (mocked adapters, proves config-driven routing)
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_router_fans_out_to_every_configured_target(
    monkeypatch: pytest.MonkeyPatch, clean_dl_env: None
) -> None:
    """With two targets configured, the router must call bulk_upsert on
    both for every table."""
    monkeypatch.setenv("DL_FEATURES__WAREHOUSE_ROUTER__TARGETS", "postgres,postgres_replica")
    settings = load_settings(env="local")

    mock_wh = MagicMock()
    # Return an empty row list for each table query to keep the test fast.
    mock_wh.query.return_value = []

    pg_primary = MagicMock(spec=PostgresOperationalDb)
    pg_replica = MagicMock(spec=PostgresOperationalDb)

    adapters = MagicMock()
    adapters.warehouse = mock_wh
    adapters.operational_dbs = {"postgres": pg_primary, "postgres_replica": pg_replica}

    result: PushResult = push_gold_um_to_operational(adapters, settings)

    # Every table attempted against every target.
    expected_calls = len(GOLD_UM_TABLES) + len(GOLD_UM_LOOKUPS)
    assert pg_primary.bulk_upsert.call_count == expected_calls
    assert pg_replica.bulk_upsert.call_count == expected_calls
    assert result.targets_requested == ["postgres", "postgres_replica"]
    assert result.all_green


@pytest.mark.unit
def test_router_skips_unregistered_target(
    monkeypatch: pytest.MonkeyPatch, clean_dl_env: None
) -> None:
    """If config references a target that isn't built by the factory, the
    router records it in `targets_skipped` and continues."""
    monkeypatch.setenv("DL_FEATURES__WAREHOUSE_ROUTER__TARGETS", "postgres,nonexistent")
    settings = load_settings(env="local")

    mock_wh = MagicMock()
    mock_wh.query.return_value = []
    pg = MagicMock(spec=PostgresOperationalDb)
    adapters = MagicMock()
    adapters.warehouse = mock_wh
    adapters.operational_dbs = {"postgres": pg}

    result = push_gold_um_to_operational(adapters, settings)

    assert "nonexistent" in result.targets_skipped
    assert any(t.target == "postgres" for t in result.per_target)


@pytest.mark.unit
def test_router_config_flip_limits_targets(
    monkeypatch: pytest.MonkeyPatch, clean_dl_env: None
) -> None:
    """Flipping targets=[postgres] must not call the replica adapter."""
    monkeypatch.setenv(
        "DL_FEATURES__WAREHOUSE_ROUTER__TARGETS", "postgres,"
    )  # trailing comma → list
    settings = load_settings(env="local")

    mock_wh = MagicMock()
    mock_wh.query.return_value = []
    pg_primary = MagicMock(spec=PostgresOperationalDb)
    pg_replica = MagicMock(spec=PostgresOperationalDb)
    adapters = MagicMock()
    adapters.warehouse = mock_wh
    adapters.operational_dbs = {"postgres": pg_primary, "postgres_replica": pg_replica}

    push_gold_um_to_operational(adapters, settings)

    assert pg_primary.bulk_upsert.called
    assert not pg_replica.bulk_upsert.called  # flip proven


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


@pytest.mark.phase
def test_local_config_has_three_operational_dbs(clean_dl_env: None) -> None:
    settings = load_settings(env="local")
    assert set(settings.adapters.operational_dbs) == {
        "sqlserver",
        "postgres",
        "postgres_replica",
    }
    # Default targets exclude sqlserver (unreachable) and use both postgres.
    assert settings.features.warehouse_router.targets == ["postgres", "postgres_replica"]


@pytest.mark.phase
def test_postgres_adapter_config_has_non_default_ports(clean_dl_env: None) -> None:
    settings = load_settings(env="local")
    assert settings.adapters.operational_dbs["postgres"].port == 5434
    assert settings.adapters.operational_dbs["postgres_replica"].port == 5433


@pytest.mark.unit
def test_sqlserver_adapter_is_instantiable_without_pyodbc_runtime() -> None:
    """The SQL Server adapter must be safe to build even when unixODBC isn't
    installed — pyodbc import is deferred until first .connect()."""
    cfg = OperationalDbConfig(
        type="sqlserver",
        host="localhost",
        port=1433,
        user="sa",
        password="x",
        database="datalink_um",
    )
    adapter = SqlServerOperationalDb("sqlserver", cfg)
    assert adapter.name == "sqlserver"
