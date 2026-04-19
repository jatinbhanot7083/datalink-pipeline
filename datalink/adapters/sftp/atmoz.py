"""AtmozSftpSource — paramiko-backed SFTP source for the atmoz/sftp container.

Phase 2 implementation. Matches the SftpSource Protocol exactly. Connection
is lazy (first list_files or download opens the transport), and close() is
idempotent.
"""

from __future__ import annotations

import fnmatch
import posixpath
from pathlib import Path

import paramiko

from datalink.adapters.protocols import RemoteFile
from datalink.config.models import SftpConfig


class AtmozSftpSource:
    """Password- or key-authenticated SFTP source.

    Local: atmoz/sftp container on 127.0.0.1:2222 (from .env.example).
    Prod: Azure SFTP endpoint on the Storage Account — same adapter class,
    just different host/credentials from config.
    """

    def __init__(self, cfg: SftpConfig) -> None:
        self._cfg = cfg
        self._transport: paramiko.Transport | None = None
        self._sftp: paramiko.SFTPClient | None = None

    # --- lifecycle --------------------------------------------------------

    def _connect(self) -> paramiko.SFTPClient:
        if self._sftp is not None:
            return self._sftp
        transport = paramiko.Transport((self._cfg.host, self._cfg.port))
        if self._cfg.password is not None:
            transport.connect(username=self._cfg.user, password=self._cfg.password)
        elif self._cfg.private_key_path is not None:
            pkey = paramiko.RSAKey.from_private_key_file(self._cfg.private_key_path)
            transport.connect(username=self._cfg.user, pkey=pkey)
        else:
            raise ValueError(
                f"SFTP config for {self._cfg.host}:{self._cfg.port} has no auth method "
                "(need password or private_key_path)"
            )
        sftp = paramiko.SFTPClient.from_transport(transport)
        if sftp is None:
            transport.close()
            raise RuntimeError(f"Failed to open SFTP channel to {self._cfg.host}")
        self._transport = transport
        self._sftp = sftp
        return sftp

    def close(self) -> None:
        if self._sftp is not None:
            self._sftp.close()
            self._sftp = None
        if self._transport is not None:
            self._transport.close()
            self._transport = None

    # --- protocol methods -------------------------------------------------

    def list_files(self, pattern: str = "*") -> list[RemoteFile]:
        sftp = self._connect()
        base = self._cfg.remote_base_dir
        try:
            entries = sftp.listdir_attr(base)
        except FileNotFoundError:
            return []
        result: list[RemoteFile] = []
        for attr in entries:
            if attr.filename is None:
                continue
            if not fnmatch.fnmatch(attr.filename, pattern):
                continue
            size = attr.st_size or 0
            mtime = float(attr.st_mtime or 0.0)
            result.append(
                RemoteFile(
                    path=posixpath.join(base, attr.filename),
                    size=size,
                    modified_epoch=mtime,
                )
            )
        result.sort(key=lambda f: f.path)
        return result

    def download(self, remote_path: str, local_dest: Path) -> None:
        sftp = self._connect()
        local_dest.parent.mkdir(parents=True, exist_ok=True)
        sftp.get(remote_path, str(local_dest))

    # --- helpers (not in protocol) ---------------------------------------

    def upload(self, local_path: Path, remote_path: str | None = None) -> str:
        """Upload a file to the SFTP drop zone. Used by seed/demo scripts.
        Returns the resolved remote path."""
        sftp = self._connect()
        if remote_path is None:
            remote_path = posixpath.join(self._cfg.remote_base_dir, local_path.name)
        sftp.put(str(local_path), remote_path)
        return remote_path
