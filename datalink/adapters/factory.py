"""Build concrete adapters from a validated Settings object.

Exactly one entry point: `build_adapters(settings) -> AdapterSet`.
The pipeline code never imports a concrete adapter — only the AdapterSet.

This is the one place `type` strings in config turn into Python classes.
Every branch is a match on `cfg.type`. Adding a new implementation means:
  1. Add the literal to the Pydantic model in datalink.config.models
  2. Add a branch here
  3. Add tests
"""

from __future__ import annotations

from dataclasses import dataclass

from datalink.adapters.llm.anthropic import AnthropicLlm
from datalink.adapters.llm.stub import StubLlm
from datalink.adapters.notifier.file import FileNotifier
from datalink.adapters.notifier.smtp import SmtpNotifier
from datalink.adapters.notifier.teams import TeamsNotifier
from datalink.adapters.object_store.adls import AdlsObjectStore
from datalink.adapters.object_store.azurite import AzuriteObjectStore
from datalink.adapters.object_store.localfs import LocalFsObjectStore
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
from datalink.adapters.secrets.keyvault import AzureKeyVaultSecrets
from datalink.adapters.sftp.atmoz import AtmozSftpSource
from datalink.adapters.sftp.azure_sftp import AzureSftpSource
from datalink.adapters.warehouse.duckdb_adapter import DuckDBWarehouse
from datalink.adapters.warehouse.snowflake_adapter import SnowflakeWarehouse
from datalink.config.loader import Settings
from datalink.config.models import (
    AdapterConfig,
    LlmConfig,
    NotifierConfig,
    ObjectStoreConfig,
    OperationalDbConfig,
    SecretProviderConfig,
    SftpConfig,
    WarehouseConfig,
)


@dataclass(frozen=True)
class AdapterSet:
    """Collection of every adapter the pipeline needs.

    Passed around as a single value — pipeline code requests what it needs
    from this bag and never reaches for module-level globals.
    """

    sftp: SftpSource
    object_store: ObjectStore
    warehouse: Warehouse
    operational_dbs: dict[str, OperationalDb]
    notifier: Notifier
    secrets: SecretProvider
    llm: LlmProvider


class AdapterConfigError(ValueError):
    """Raised when config asks for an adapter type that doesn't exist."""


# ----------------------------------------------------------------------------
# Builders — one per adapter kind.
# ----------------------------------------------------------------------------


def _build_sftp(cfg: SftpConfig) -> SftpSource:
    match cfg.type:
        case "atmoz":
            return AtmozSftpSource(cfg)
        case "azure_sftp":
            return AzureSftpSource(cfg)
        case "stub":
            return AtmozSftpSource(cfg)  # atmoz placeholder is fine as a stub
        case _ as bad:
            raise AdapterConfigError(f"Unknown sftp type: {bad!r}")


def _build_object_store(cfg: ObjectStoreConfig) -> ObjectStore:
    match cfg.type:
        case "localfs":
            return LocalFsObjectStore(cfg)
        case "azurite":
            return AzuriteObjectStore(cfg)
        case "adls":
            return AdlsObjectStore(cfg)
        case "stub":
            return LocalFsObjectStore(cfg)
        case _ as bad:
            raise AdapterConfigError(f"Unknown object_store type: {bad!r}")


def _build_warehouse(cfg: WarehouseConfig) -> Warehouse:
    match cfg.type:
        case "duckdb":
            return DuckDBWarehouse(cfg)
        case "snowflake":
            return SnowflakeWarehouse(cfg)
        case _ as bad:
            raise AdapterConfigError(f"Unknown warehouse type: {bad!r}")


def _build_operational_db(name: str, cfg: OperationalDbConfig) -> OperationalDb:
    match cfg.type:
        case "sqlserver":
            return SqlServerOperationalDb(name, cfg)
        case "postgres":
            return PostgresOperationalDb(name, cfg)
        case "stub":
            return SqlServerOperationalDb(name, cfg)
        case _ as bad:
            raise AdapterConfigError(f"Unknown operational_db type for {name!r}: {bad!r}")


def _build_notifier(cfg: NotifierConfig) -> Notifier:
    match cfg.type:
        case "file":
            return FileNotifier(cfg)
        case "teams":
            return TeamsNotifier(cfg)
        case "smtp":
            return SmtpNotifier(cfg)
        case "stub":
            return FileNotifier(cfg)
        case _ as bad:
            raise AdapterConfigError(f"Unknown notifier type: {bad!r}")


def _build_secrets(cfg: SecretProviderConfig) -> SecretProvider:
    match cfg.type:
        case "env":
            return EnvSecrets(cfg)
        case "keyvault":
            return AzureKeyVaultSecrets(cfg)
        case _ as bad:
            raise AdapterConfigError(f"Unknown secrets type: {bad!r}")


def _build_llm(cfg: LlmConfig) -> LlmProvider:
    match cfg.type:
        case "stub":
            return StubLlm(cfg)
        case "anthropic":
            return AnthropicLlm(cfg)
        case _ as bad:
            raise AdapterConfigError(f"Unknown llm type: {bad!r}")


# ----------------------------------------------------------------------------
# Public entry point.
# ----------------------------------------------------------------------------


def build_adapters(settings: Settings) -> AdapterSet:
    """Construct every adapter. Does not open connections — that happens lazily
    on first method call. Safe to build-and-discard in tests."""
    a: AdapterConfig = settings.adapters
    return AdapterSet(
        sftp=_build_sftp(a.sftp),
        object_store=_build_object_store(a.object_store),
        warehouse=_build_warehouse(a.warehouse),
        operational_dbs={
            name: _build_operational_db(name, cfg) for name, cfg in a.operational_dbs.items()
        },
        notifier=_build_notifier(a.notifier),
        secrets=_build_secrets(a.secrets),
        llm=_build_llm(a.llm),
    )
