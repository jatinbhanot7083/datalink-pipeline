"""AdlsObjectStore — Azure Data Lake Storage Gen2 for dev/stage/prod."""

from __future__ import annotations

from pathlib import Path

from datalink.adapters.protocols import ObjectMeta
from datalink.config.models import ObjectStoreConfig


class AdlsObjectStore:
    def __init__(self, cfg: ObjectStoreConfig) -> None:
        self._cfg = cfg

    def put(self, local_path: Path, key: str) -> None:
        raise NotImplementedError("AdlsObjectStore lands in Phase 2 (prod path)")

    def get(self, key: str, local_dest: Path) -> None:
        raise NotImplementedError("AdlsObjectStore lands in Phase 2 (prod path)")

    def list(self, prefix: str = "") -> list[ObjectMeta]:
        raise NotImplementedError("AdlsObjectStore lands in Phase 2 (prod path)")

    def exists(self, key: str) -> bool:
        raise NotImplementedError("AdlsObjectStore lands in Phase 2 (prod path)")

    def delete(self, key: str) -> None:
        raise NotImplementedError("AdlsObjectStore lands in Phase 2 (prod path)")
