"""Pydantic models describing the shape of every config block.

These models are the single source of truth for what's configurable.
YAML files and env vars both flow through here — if a field isn't defined
here, it doesn't exist.
"""

from __future__ import annotations

from enum import Enum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class EnvName(str, Enum):
    LOCAL = "local"
    DEV = "dev"
    STAGE = "stage"
    PROD = "prod"


class OrchestratorName(str, Enum):
    LOCAL_SEQUENTIAL = "local_sequential"
    AIRFLOW_KIND = "airflow_kind"


class LogFormat(str, Enum):
    JSON = "json"
    CONSOLE = "console"


# ----------------------------------------------------------------------------
# Adapter configs — one per external dependency.
# Each adapter has a discriminator field `type` that selects the implementation.
# ----------------------------------------------------------------------------


class SftpConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    type: Literal["atmoz", "azure_sftp", "stub"] = "atmoz"
    host: str = "localhost"
    port: int = 2222
    user: str = "datalink"
    password: str | None = None
    private_key_path: str | None = None
    remote_base_dir: str = "/drop"


class ObjectStoreConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    type: Literal["azurite", "adls", "stub"] = "azurite"
    endpoint: str = "http://localhost:10000/devstoreaccount1"
    account: str = "devstoreaccount1"
    account_key: str | None = None
    container: str = "datalink-raw"


class WarehouseConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    type: Literal["duckdb", "snowflake"] = "duckdb"
    # DuckDB
    path: str = "./warehouse.duckdb"
    # Snowflake (populated in prod)
    account: str | None = None
    user: str | None = None
    password: str | None = None
    warehouse: str | None = None
    role: str | None = None
    database: str | None = None


class OperationalDbConfig(BaseModel):
    """One entry per target operational DB. Warehouse router fans out to all enabled."""

    model_config = ConfigDict(extra="forbid")
    type: Literal["sqlserver", "postgres", "stub"]
    host: str = "localhost"
    port: int
    user: str
    password: str | None = None
    database: str


class NotifierConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    type: Literal["file", "teams", "smtp", "stub"] = "file"
    # file
    path: str = "./logs/notifications.jsonl"
    # teams
    webhook_url: str | None = None
    # smtp
    smtp_host: str | None = None
    smtp_port: int | None = None
    smtp_user: str | None = None
    smtp_password: str | None = None
    from_addr: str | None = None
    to_addrs: list[str] = Field(default_factory=list)


class LlmConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    type: Literal["stub", "anthropic"] = "stub"
    model: str = "claude-haiku-4-5-20251001"
    max_tokens: int = 4096
    api_key: str | None = None  # loaded from ANTHROPIC_API_KEY at runtime when type=anthropic


class SecretProviderConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    type: Literal["env", "keyvault"] = "env"
    keyvault_url: str | None = None


class AdapterConfig(BaseModel):
    """Registry of all adapter configs. Factory reads this to construct instances."""

    model_config = ConfigDict(extra="forbid")
    sftp: SftpConfig = Field(default_factory=SftpConfig)
    object_store: ObjectStoreConfig = Field(default_factory=ObjectStoreConfig)
    warehouse: WarehouseConfig = Field(default_factory=WarehouseConfig)
    # Dict keyed by target name (sqlserver, postgres) — router iterates over this
    operational_dbs: dict[str, OperationalDbConfig] = Field(default_factory=dict)
    notifier: NotifierConfig = Field(default_factory=NotifierConfig)
    llm: LlmConfig = Field(default_factory=LlmConfig)
    secrets: SecretProviderConfig = Field(default_factory=SecretProviderConfig)


# ----------------------------------------------------------------------------
# Feature configs — plug-in / plug-out toggles.
# ----------------------------------------------------------------------------


class GxCheckpointConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    fail_threshold_pct: float = 5.0


class GxFeatureConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    enabled: bool = True
    checkpoints: dict[str, GxCheckpointConfig] = Field(default_factory=dict)


class CrewConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    enabled: bool = True


class AgentsFeatureConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    enabled: bool = True
    pre_validation_crew: CrewConfig = Field(default_factory=CrewConfig)
    post_validation_crew: CrewConfig = Field(default_factory=CrewConfig)
    max_tokens_per_run: int = 100_000


class WarehouseRouterConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    # Which operational DBs to push Gold into. Must match keys in adapters.operational_dbs.
    targets: list[str] = Field(default_factory=lambda: ["sqlserver", "postgres"])


class FeatureConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    gx: GxFeatureConfig = Field(default_factory=GxFeatureConfig)
    agents: AgentsFeatureConfig = Field(default_factory=AgentsFeatureConfig)
    warehouse_router: WarehouseRouterConfig = Field(default_factory=WarehouseRouterConfig)


# ----------------------------------------------------------------------------
# Processing config (pandas↔spark threshold, etc.)
# ----------------------------------------------------------------------------


class ProcessingConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    pandas_to_spark_rows_threshold: int = 500_000
    pandas_to_spark_bytes_threshold: int = 524_288_000  # 500 MB


class LoggingConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    format: LogFormat = LogFormat.CONSOLE
