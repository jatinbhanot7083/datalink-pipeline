"""AzureSftpSource — Azure Blob Storage SFTP endpoint.

Prod implementation lands in Phase 2. Uses paramiko against Azure's
managed-identity-authenticated SFTP on the storage account.
"""

from __future__ import annotations

from pathlib import Path

from datalink.adapters.protocols import RemoteFile
from datalink.config.models import SftpConfig


class AzureSftpSource:
    def __init__(self, cfg: SftpConfig) -> None:
        self._cfg = cfg

    def list_files(self, pattern: str = "*") -> list[RemoteFile]:
        raise NotImplementedError("AzureSftpSource lands in Phase 2 (prod path)")

    def download(self, remote_path: str, local_dest: Path) -> None:
        raise NotImplementedError("AzureSftpSource lands in Phase 2 (prod path)")

    def close(self) -> None:
        return None
