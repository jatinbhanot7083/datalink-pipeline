"""Phase 3 structural verification.

Run under `make verify-phase-3-code`. These tests prove the dbt project is
well-formed (all 9 Silver models defined, schema.yml wired, macros loaded)
without executing dbt. The real dbt run + dbt test exit gate lives in
`make verify-phase-3-dbt`.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
DBT_DIR = REPO_ROOT / "dbt"
SILVER_DIR = DBT_DIR / "models" / "silver"


# ---------------------------------------------------------------------------
# dbt project layout
# ---------------------------------------------------------------------------


@pytest.mark.phase
def test_dbt_project_files_exist() -> None:
    required = [
        "dbt_project.yml",
        "profiles.yml",
        "models/sources.yml",
        "models/silver/schema.yml",
        "macros/dv_helpers.sql",
        "tests/sat_unique_composite.sql",
    ]
    missing = [p for p in required if not (DBT_DIR / p).exists()]
    assert not missing, f"dbt project incomplete: missing {missing}"


@pytest.mark.phase
def test_dbt_project_has_silver_profile() -> None:
    cfg = yaml.safe_load((DBT_DIR / "dbt_project.yml").read_text(encoding="utf-8"))
    assert cfg["profile"] == "datalink"
    models = cfg["models"]["datalink"]["silver"]
    assert models["+materialized"] == "incremental"
    assert models["+incremental_strategy"] == "append"


@pytest.mark.phase
@pytest.mark.parametrize(
    "target",
    [
        "local",  # dbt-duckdb
        "dev",  # dbt-snowflake
        "prod",  # dbt-snowflake
    ],
)
def test_dbt_profile_has_target(target: str) -> None:
    profiles = yaml.safe_load((DBT_DIR / "profiles.yml").read_text(encoding="utf-8"))
    assert target in profiles["datalink"]["outputs"]


# ---------------------------------------------------------------------------
# Silver model files
# ---------------------------------------------------------------------------


EXPECTED_HUBS = ["hub_claim", "hub_member", "hub_provider", "hub_plan"]
EXPECTED_SATS = ["sat_claim_details", "sat_member_demographics", "sat_provider_info"]
EXPECTED_LINKS = ["link_claim_member", "link_claim_provider", "link_member_plan"]


@pytest.mark.phase
@pytest.mark.parametrize("name", EXPECTED_HUBS)
def test_hub_model_exists(name: str) -> None:
    f = SILVER_DIR / "hubs" / f"{name}.sql"
    assert f.exists()
    sql = f.read_text(encoding="utf-8")
    assert "dv_hash_key" in sql, f"{name} must use the dv_hash_key macro"
    assert "is_incremental()" in sql, f"{name} must be incremental-aware"
    assert "load_dts" in sql
    assert "record_source" in sql


@pytest.mark.phase
@pytest.mark.parametrize("name", EXPECTED_SATS)
def test_sat_model_exists(name: str) -> None:
    f = SILVER_DIR / "satellites" / f"{name}.sql"
    assert f.exists()
    sql = f.read_text(encoding="utf-8")
    assert "dv_hash_key" in sql, f"{name} must use dv_hash_key for hub reference"
    assert "dv_hash_diff" in sql, f"{name} must use dv_hash_diff for SCD2 change detection"
    assert "hash_diff" in sql


@pytest.mark.phase
@pytest.mark.parametrize("name", EXPECTED_LINKS)
def test_link_model_exists(name: str) -> None:
    f = SILVER_DIR / "links" / f"{name}.sql"
    assert f.exists()
    sql = f.read_text(encoding="utf-8")
    assert "dv_hash_key" in sql
    # Links must reference at least two Hubs.
    hub_refs = sum(
        1 for h in ["hub_claim_hk", "hub_member_hk", "hub_provider_hk", "hub_plan_hk"] if h in sql
    )
    assert hub_refs >= 2, f"{name}: a link must reference >=2 hubs (found {hub_refs})"


@pytest.mark.phase
def test_all_silver_models_in_schema_yml() -> None:
    """Every Silver model must have a schema.yml entry (enables dbt tests)."""
    schema = yaml.safe_load((SILVER_DIR / "schema.yml").read_text(encoding="utf-8"))
    names = {m["name"] for m in schema["models"]}
    expected = set(EXPECTED_HUBS + EXPECTED_SATS + EXPECTED_LINKS)
    missing = expected - names
    assert not missing, f"schema.yml missing entries: {missing}"


@pytest.mark.phase
def test_every_hub_has_unique_hash_key_test() -> None:
    """Every Hub's hub_*_hk must have unique + not_null tests declared."""
    schema = yaml.safe_load((SILVER_DIR / "schema.yml").read_text(encoding="utf-8"))
    for model in schema["models"]:
        if not model["name"].startswith("hub_"):
            continue
        hk_col_name = f"{model['name']}_hk"
        hk_col = next((c for c in model.get("columns", []) if c["name"] == hk_col_name), None)
        assert hk_col, f"{model['name']}: missing column '{hk_col_name}' in schema.yml"
        tests = set(hk_col.get("tests", []))
        assert (
            "unique" in tests and "not_null" in tests
        ), f"{model['name']}.{hk_col_name} must have both unique + not_null tests; got {tests}"


@pytest.mark.phase
def test_every_sat_has_hub_relationships_test() -> None:
    """Every Satellite must declare a relationships test to its parent Hub."""
    schema = yaml.safe_load((SILVER_DIR / "schema.yml").read_text(encoding="utf-8"))
    for model in schema["models"]:
        if not model["name"].startswith("sat_"):
            continue
        # First column is the hub_*_hk — find any relationships test on it.
        found = False
        for col in model.get("columns", []):
            for t in col.get("tests", []):
                if isinstance(t, dict) and "relationships" in t:
                    found = True
                    break
        assert found, f"{model['name']}: no relationships test on hub FK — orphan risk"


@pytest.mark.phase
def test_dv_helpers_macro_file_has_required_macros() -> None:
    content = (DBT_DIR / "macros" / "dv_helpers.sql").read_text(encoding="utf-8")
    assert "macro dv_hash_key" in content
    assert "macro dv_hash_diff" in content
