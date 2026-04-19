"""AtmozSftpSource — reads files from a local atmoz/sftp container.

Phase 1: instantiable skeleton. Phase 2 implements the paramiko calls.
"""

from __future__ import annotations

from pathlib import Path

from datalink.adapters.protocols import RemoteFile
from datalink.config.models import SftpConfig


class AtmozSftpSource:
    """atmoz/sftp-backed SFTP source."""

    def __init__(self, cfg: SftpConfig) -> None:
        self._cfg = cfg

    def list_files(self, pattern: str = "*") -> list[RemoteFile]:
        raise NotImplementedError("AtmozSftpSource.list_files lands in Phase 2 (Bronze ingestion)")

    def download(self, remote_path: str, local_dest: Path) -> None:
        raise NotImplementedError("AtmozSftpSource.download lands in Phase 2 (Bronze ingestion)")

    def close(self) -> None:
        # idempotent no-op for the placeholder
        return None
