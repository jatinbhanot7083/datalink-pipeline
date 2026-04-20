"""Config loader: layering, env-var overrides, every environment parseable."""

from __future__ import annotations

from pathlib import Path

import pytest

from datalink.config.loader import Settings, _coerce, _deep_merge, _env_overrides, load_settings
from datalink.config.models import EnvName, OrchestratorName


@pytest.mark.unit
def test_deep_merge_override_wins() -> None:
    base = {"a": 1, "nested": {"x": 1, "y": 2}}
    override = {"a": 99, "nested": {"x": 42, "z": 3}}
    merged = _deep_merge(base, override)
    assert merged == {"a": 99, "nested": {"x": 42, "y": 2, "z": 3}}
    # base must be untouched
    assert base == {"a": 1, "nested": {"x": 1, "y": 2}}


@pytest.mark.unit
def test_coerce_scalar_types() -> None:
    assert _coerce("true") is True
    assert _coerce("False") is False
    assert _coerce("42") == 42
    assert _coerce("3.14") == 3.14
    assert _coerce("hello") == "hello"
    assert _coerce("a,b,c") == ["a", "b", "c"]
    assert _coerce("1,2,3") == [1, 2, 3]


@pytest.mark.unit
def test_env_overrides_translates_double_underscore_to_nesting(
    monkeypatch: pytest.MonkeyPatch, clean_dl_env: None
) -> None:
    monkeypatch.setenv("DL_ADAPTERS__WAREHOUSE__PATH", "./alt.duckdb")
    monkeypatch.setenv("DL_FEATURES__GX__ENABLED", "false")
    result = _env_overrides()
    assert result["adapters"]["warehouse"]["path"] == "./alt.duckdb"
    assert result["features"]["gx"]["enabled"] is False


@pytest.mark.unit
def test_local_env_loads_and_validates(config_root: Path, clean_dl_env: None) -> None:
    settings = load_settings(config_root=config_root, env="local")
    assert isinstance(settings, Settings)
    assert settings.env is EnvName.LOCAL
    assert settings.orchestrator is OrchestratorName.LOCAL_SEQUENTIAL
    assert settings.adapters.warehouse.type == "duckdb"
    assert settings.adapters.sftp.type == "atmoz"
    assert settings.adapters.object_store.type == "localfs"
    assert "sqlserver" in settings.adapters.operational_dbs
    assert "postgres" in settings.adapters.operational_dbs
    assert settings.adapters.llm.type == "stub"
    assert settings.features.gx.enabled is True
    assert settings.features.agents.enabled is True
    # Phase 5.7: MCR pulls work on native Docker-in-WSL2, so the local
    # router targets include all three registered operational DBs
    # (sqlserver + both Postgres). Pre-Phase-5.7 sqlserver was dropped
    # because Docker Desktop 4.51.0 blocked the MCR pull.
    assert settings.features.warehouse_router.targets == [
        "sqlserver",
        "postgres",
        "postgres_replica",
    ]


@pytest.mark.unit
@pytest.mark.parametrize("env", ["dev", "stage", "prod"])
def test_non_local_envs_load_and_validate(config_root: Path, clean_dl_env: None, env: str) -> None:
    settings = load_settings(config_root=config_root, env=env)
    assert settings.env.value == env
    # non-local envs always use anthropic LLM + keyvault secrets
    assert settings.adapters.llm.type == "anthropic"
    assert settings.adapters.secrets.type == "keyvault"


@pytest.mark.unit
def test_env_var_override_beats_yaml(
    config_root: Path, monkeypatch: pytest.MonkeyPatch, clean_dl_env: None
) -> None:
    monkeypatch.setenv("DL_ADAPTERS__WAREHOUSE__PATH", "/tmp/override.duckdb")
    monkeypatch.setenv("DL_FEATURES__GX__ENABLED", "false")
    settings = load_settings(config_root=config_root, env="local")
    assert settings.adapters.warehouse.path == "/tmp/override.duckdb"
    assert settings.features.gx.enabled is False


@pytest.mark.unit
def test_plugout_flip_disables_both_features(
    config_root: Path, monkeypatch: pytest.MonkeyPatch, clean_dl_env: None
) -> None:
    """Proves the plug-out contract at the config layer. Pipeline-level plug-out
    test lives in tests/plugout/ (Phase 5)."""
    monkeypatch.setenv("DL_FEATURES__GX__ENABLED", "false")
    monkeypatch.setenv("DL_FEATURES__AGENTS__ENABLED", "false")
    settings = load_settings(config_root=config_root, env="local")
    assert settings.features.gx.enabled is False
    assert settings.features.agents.enabled is False


@pytest.mark.unit
def test_missing_env_file_raises(tmp_path: Path, clean_dl_env: None) -> None:
    (tmp_path / "base.yaml").write_text("")
    (tmp_path / "environments").mkdir()
    with pytest.raises(FileNotFoundError):
        load_settings(config_root=tmp_path, env="nonexistent")


@pytest.mark.unit
def test_settings_is_frozen(config_root: Path, clean_dl_env: None) -> None:
    """Settings must be immutable — mutations after load would surprise callers."""
    from pydantic import ValidationError

    settings = load_settings(config_root=config_root, env="local")
    with pytest.raises(ValidationError):
        settings.project_name = "mutated"  # type: ignore[misc]
