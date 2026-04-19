"""Adapter factory: builds an AdapterSet from Settings without opening connections."""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from datalink.adapters.factory import AdapterSet, build_adapters
from datalink.adapters.llm.stub import StubLlm
from datalink.adapters.notifier.file import FileNotifier
from datalink.adapters.operational_db.postgres import PostgresOperationalDb
from datalink.adapters.operational_db.sqlserver import SqlServerOperationalDb
from datalink.adapters.protocols import (
    LlmProvider,
    Notifier,
    ObjectStore,
    OperationalDb,
    SecretProvider,
    SftpSource,
    Warehouse,
)
from datalink.adapters.secrets.env import EnvSecrets
from datalink.config.loader import load_settings


@pytest.mark.unit
def test_build_adapters_for_local_returns_correct_concrete_types(
    config_root: Path, clean_dl_env: None
) -> None:
    settings = load_settings(config_root=config_root, env="local")
    adapters = build_adapters(settings)

    assert isinstance(adapters, AdapterSet)
    assert isinstance(adapters.notifier, FileNotifier)
    assert isinstance(adapters.secrets, EnvSecrets)
    assert isinstance(adapters.llm, StubLlm)
    assert isinstance(adapters.operational_dbs["sqlserver"], SqlServerOperationalDb)
    assert isinstance(adapters.operational_dbs["postgres"], PostgresOperationalDb)


@pytest.mark.unit
def test_local_adapters_satisfy_protocols(config_root: Path, clean_dl_env: None) -> None:
    settings = load_settings(config_root=config_root, env="local")
    adapters = build_adapters(settings)
    assert isinstance(adapters.sftp, SftpSource)
    assert isinstance(adapters.object_store, ObjectStore)
    assert isinstance(adapters.warehouse, Warehouse)
    assert isinstance(adapters.notifier, Notifier)
    assert isinstance(adapters.secrets, SecretProvider)
    assert isinstance(adapters.llm, LlmProvider)
    for db in adapters.operational_dbs.values():
        assert isinstance(db, OperationalDb)


@pytest.mark.unit
def test_build_adapters_for_prod_does_not_open_connections(
    config_root: Path, clean_dl_env: None
) -> None:
    """Building adapters for prod must be a pure construction step — no network
    calls. We can instantiate prod adapters from a local machine without any
    credentials set."""
    settings = load_settings(config_root=config_root, env="prod")
    adapters = build_adapters(settings)
    assert adapters.warehouse.__class__.__name__ == "SnowflakeWarehouse"
    assert adapters.object_store.__class__.__name__ == "AdlsObjectStore"
    assert adapters.secrets.__class__.__name__ == "AzureKeyVaultSecrets"


@pytest.mark.unit
def test_file_notifier_writes_jsonl(tmp_notifications_dir: Path) -> None:
    from datalink.config.models import NotifierConfig

    path = tmp_notifications_dir / "alerts.jsonl"
    notifier = FileNotifier(NotifierConfig(type="file", path=str(path)))
    notifier.notify("ops", "info", "hello", "world", metadata={"k": 1})
    notifier.notify("ops", "error", "bang", "boom")
    lines = path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2
    import json

    first = json.loads(lines[0])
    assert first["title"] == "hello"
    assert first["metadata"] == {"k": 1}


@pytest.mark.unit
def test_stub_llm_returns_deterministic_canned_response() -> None:
    from datalink.adapters.protocols import LlmMessage
    from datalink.config.models import LlmConfig

    llm = StubLlm(LlmConfig())
    messages = [LlmMessage(role="user", content="profile this table")]
    out1 = llm.complete(messages, max_tokens=100)
    out2 = llm.complete(messages, max_tokens=100)
    assert out1.content == out2.content
    assert '"stubbed":true' in out1.content
    assert out1.stop_reason == "stop_sequence"


@pytest.mark.unit
def test_env_secrets_round_trip(monkeypatch: pytest.MonkeyPatch) -> None:
    from datalink.config.models import SecretProviderConfig

    monkeypatch.setenv("TEST_SECRET", "shh")
    s = EnvSecrets(SecretProviderConfig(type="env"))
    assert s.get("TEST_SECRET") == "shh"
    assert s.get_optional("MISSING") is None
    with pytest.raises(KeyError):
        s.get("MISSING")


@pytest.mark.unit
def test_factory_raises_on_unknown_adapter_type(config_root: Path, clean_dl_env: None) -> None:
    """Editing config to an unknown type is caught at validation or build time."""
    from datalink.config.models import NotifierConfig

    with pytest.raises(ValidationError):
        NotifierConfig(type="nonexistent")  # type: ignore[arg-type]
