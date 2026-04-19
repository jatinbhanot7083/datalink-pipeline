"""LocalFsObjectStore — filesystem-backed ObjectStore for offline dev and CI.

Satisfies the ObjectStore Protocol identically to AzuriteObjectStore; only
the storage backend differs. Blobs are written as files under
`<base>/<container>/<key>`; `key` with slashes creates nested directories.

This adapter is used in local env when Azurite isn't available (e.g., on
dev machines where Docker Desktop 4.51.0's built-in proxy blocks the
mcr.microsoft.com Azurite image, OR offline CI). Prod still uses ADLS Gen2
via a different adapter; the config switch is endpoint-only.

Atomic writes (write to .tmp, rename) so concurrent readers don't see partial files.
"""

from __future__ import annotations

import os
import shutil
import uuid
from pathlib import Path

from datalink.adapters.protocols import ObjectMeta
from datalink.config.models import ObjectStoreConfig


class LocalFsObjectStore:
    """Filesystem-backed object store.

    The `endpoint` config field is interpreted as a filesystem path (treated
    as the "storage account" base dir). The `container` field is a subdir
    under it. Keys with `/` become nested paths inside the container dir.

    Example:
      endpoint  = file:///home/jatin/.localfs-obj
      container = datalink-raw
      key       = raw/claims/2026-04-19/claims.csv
      → file at /home/jatin/.localfs-obj/datalink-raw/raw/claims/2026-04-19/claims.csv
    """

    def __init__(self, cfg: ObjectStoreConfig) -> None:
        self._cfg = cfg
        # endpoint starts with file:// for this adapter; strip scheme.
        base = cfg.endpoint
        if base.startswith("file://"):
            base = base[len("file://") :]
        if not base:
            raise ValueError(
                f"ObjectStoreConfig.endpoint must be a file:// URL for the localfs "
                f"adapter; got {cfg.endpoint!r}"
            )
        self._root: Path = Path(base) / cfg.container
        self._root.mkdir(parents=True, exist_ok=True)

    # --- internal ---------------------------------------------------------

    def _resolve(self, key: str) -> Path:
        """Resolve a key to an absolute path under the container root. Rejects
        any key that would escape the container (path traversal)."""
        if key.startswith("/"):
            raise ValueError(f"Absolute key not allowed: {key!r}")
        target = (self._root / key).resolve()
        # Must stay under root.
        try:
            target.relative_to(self._root.resolve())
        except ValueError as exc:
            raise ValueError(f"key {key!r} escapes container root") from exc
        return target

    # --- protocol methods -------------------------------------------------

    def put(self, local_path: Path, key: str) -> None:
        dest = self._resolve(key)
        dest.parent.mkdir(parents=True, exist_ok=True)
        # Atomic: write to sibling .tmp then rename.
        tmp = dest.with_suffix(dest.suffix + f".tmp.{uuid.uuid4().hex[:8]}")
        shutil.copyfile(local_path, tmp)
        os.replace(tmp, dest)

    def get(self, key: str, local_dest: Path) -> None:
        src = self._resolve(key)
        if not src.exists():
            raise FileNotFoundError(f"Object not found: {key!r} (path={src})")
        local_dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(src, local_dest)

    def list(self, prefix: str = "") -> list[ObjectMeta]:
        start = self._resolve(prefix) if prefix else self._root
        # `prefix` may be either a directory (enumerate contents recursively)
        # or a partial filename. Handle both: if it exists as a dir, walk it;
        # otherwise, walk the parent dir and filter.
        result: list[ObjectMeta] = []
        search_root: Path
        if start.is_dir():
            search_root = start
            key_prefix = prefix
        else:
            search_root = start.parent if prefix else self._root
            key_prefix = prefix
        for path in search_root.rglob("*"):
            if not path.is_file():
                continue
            # Skip any .tmp.* half-written files.
            if ".tmp." in path.name:
                continue
            rel = path.relative_to(self._root).as_posix()
            if key_prefix and not rel.startswith(key_prefix):
                continue
            stat = path.stat()
            result.append(
                ObjectMeta(
                    key=rel,
                    size=stat.st_size,
                    last_modified_epoch=stat.st_mtime,
                )
            )
        result.sort(key=lambda m: m.key)
        return result

    def exists(self, key: str) -> bool:
        try:
            return self._resolve(key).is_file()
        except ValueError:
            return False

    def delete(self, key: str) -> None:
        try:
            path = self._resolve(key)
        except ValueError:
            return
        if path.is_file():
            path.unlink()
