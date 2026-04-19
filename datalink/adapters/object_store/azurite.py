"""AzuriteObjectStore — talks to the Azurite blob emulator.

Uses the same azure-storage-blob SDK path as prod ADLS — only the endpoint differs.
Phase 2 implements put/get/list/exists/delete.
"""

from __future__ import annotations

from pathlib import Path

from datalink.adapters.protocols import ObjectMeta
from datalink.config.models import ObjectStoreConfig


class AzuriteObjectStore:
    def __init__(self, cfg: ObjectStoreConfig) -> None:
        self._cfg = cfg

    def put(self, local_path: Path, key: str) -> None:
        raise NotImplementedError("AzuriteObjectStore.put lands in Phase 2")

    def get(self, key: str, local_dest: Path) -> None:
        raise NotImplementedError("AzuriteObjectStore.get lands in Phase 2")

    def list(self, prefix: str = "") -> list[ObjectMeta]:
        raise NotImplementedError("AzuriteObjectStore.list lands in Phase 2")

    def exists(self, key: str) -> bool:
        raise NotImplementedError("AzuriteObjectStore.exists lands in Phase 2")

    def delete(self, key: str) -> None:
        raise NotImplementedError("AzuriteObjectStore.delete lands in Phase 2")
