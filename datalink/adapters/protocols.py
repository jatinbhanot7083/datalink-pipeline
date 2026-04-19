"""All adapter Protocol definitions in one file.

Keeping them together makes the contract surface easy to review.
Concrete implementations live under `datalink.adapters.<kind>/`.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

# ============================================================================
# Domain value objects — shared across adapters.
# ============================================================================


@dataclass(frozen=True)
class RemoteFile:
    """A file on a remote SFTP server."""

    path: str
    size: int
    modified_epoch: float


@dataclass(frozen=True)
class ObjectMeta:
    """Metadata about an object in an object store."""

    key: str
    size: int
    last_modified_epoch: float


@dataclass(frozen=True)
class NotificationSeverity:
    INFO: str = "info"
    WARNING: str = "warning"
    ERROR: str = "error"
    CRITICAL: str = "critical"


# ============================================================================
# Protocols.
# @runtime_checkable is used sparingly — only on Protocols where we want
# `isinstance()` checks in tests (factory validation).
# ============================================================================


@runtime_checkable
class SftpSource(Protocol):
    """Remote SFTP source. Local: atmoz/sftp container. Prod: Azure SFTP."""

    def list_files(self, pattern: str = "*") -> list[RemoteFile]: ...
    def download(self, remote_path: str, local_dest: Path) -> None: ...
    def close(self) -> None: ...


@runtime_checkable
class ObjectStore(Protocol):
    """Blob store — landing zone between SFTP and warehouse.

    Local: Azurite (Blob API, compatible with ADLS Gen2 SDK).
    Prod: Azure ADLS Gen2.
    """

    def put(self, local_path: Path, key: str) -> None: ...
    def get(self, key: str, local_dest: Path) -> None: ...
    def list(self, prefix: str = "") -> list[ObjectMeta]: ...
    def exists(self, key: str) -> bool: ...
    def delete(self, key: str) -> None: ...


@runtime_checkable
class Warehouse(Protocol):
    """Analytical warehouse. Local: DuckDB. Prod: Snowflake.

    `copy_from_stage` abstracts over Snowflake's COPY INTO FROM @stage
    vs DuckDB's read-file-and-INSERT. The caller supplies a stage URI; the
    adapter knows how to resolve it in its environment.
    """

    def execute(self, sql: str, params: dict[str, Any] | None = None) -> None: ...
    def query(self, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]: ...
    def copy_from_stage(
        self,
        stage_uri: str,
        target_table: str,
        file_format: str = "csv",
        options: dict[str, Any] | None = None,
    ) -> int: ...
    def merge(
        self,
        target: str,
        source: str,
        key_columns: list[str],
    ) -> int: ...
    def close(self) -> None: ...


@runtime_checkable
class OperationalDb(Protocol):
    """Target operational DB for Gold bulk push. Local: SQL Server container + Postgres.

    The router fans out to every enabled instance — each receives the same rows.
    """

    name: str  # "sqlserver" | "postgres" (keys of adapters.operational_dbs)

    def bulk_upsert(
        self,
        table: str,
        rows: Iterable[dict[str, Any]],
        key_columns: list[str],
    ) -> int:
        """Insert-or-update rows. Returns count of rows affected."""
        ...

    def execute(self, sql: str, params: dict[str, Any] | None = None) -> None: ...
    def close(self) -> None: ...


@runtime_checkable
class Notifier(Protocol):
    """Outbound alerting. Local: append-to-file. Prod: Teams webhook + SMTP."""

    def notify(
        self,
        channel: str,
        severity: str,
        title: str,
        body: str,
        metadata: dict[str, Any] | None = None,
    ) -> None: ...


@runtime_checkable
class SecretProvider(Protocol):
    """Look up secrets by name. Local: env vars. Prod: Azure Key Vault."""

    def get(self, name: str) -> str: ...
    def get_optional(self, name: str) -> str | None: ...


@dataclass(frozen=True)
class LlmMessage:
    role: str  # "system" | "user" | "assistant"
    content: str


@dataclass(frozen=True)
class LlmCompletion:
    content: str
    input_tokens: int
    output_tokens: int
    model: str
    stop_reason: str


@runtime_checkable
class LlmProvider(Protocol):
    """LLM backend for CrewAI agents. Local default: stub. Prod: Anthropic Claude.

    Every call MUST pass its input payload through PhiRedactionLayer first. The
    adapter verifies this precondition — it does not do its own redaction.
    """

    model: str

    def complete(
        self,
        messages: list[LlmMessage],
        max_tokens: int,
        temperature: float = 0.0,
    ) -> LlmCompletion: ...
