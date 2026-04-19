"""AzuriteObjectStore — Azure Blob SDK against Azurite (or any Azure-compatible endpoint).

The SAME class handles Azurite, ADLS Gen2, and any Azure-compatible object
store. Endpoint + credentials differ per env; the SDK call pattern is
identical — which is the point of the plug-in contract.

Container auto-creation is idempotent; safe to re-instantiate.
"""

from __future__ import annotations

import contextlib
from pathlib import Path

from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.storage.blob import BlobServiceClient

from datalink.adapters.protocols import ObjectMeta
from datalink.config.models import ObjectStoreConfig


class AzuriteObjectStore:
    def __init__(self, cfg: ObjectStoreConfig) -> None:
        self._cfg = cfg
        protocol = "http" if cfg.endpoint.startswith("http://") else "https"
        if cfg.account_key is None:
            raise ValueError(
                f"ObjectStoreConfig for account {cfg.account!r} has no account_key "
                "(set DL_ADAPTERS__OBJECT_STORE__ACCOUNT_KEY env var or config)"
            )
        conn_str = (
            f"DefaultEndpointsProtocol={protocol};"
            f"AccountName={cfg.account};"
            f"AccountKey={cfg.account_key};"
            f"BlobEndpoint={cfg.endpoint};"
        )
        self._svc: BlobServiceClient = BlobServiceClient.from_connection_string(conn_str)
        self._container = self._svc.get_container_client(cfg.container)
        with contextlib.suppress(ResourceExistsError):
            self._container.create_container()

    def put(self, local_path: Path, key: str) -> None:
        with local_path.open("rb") as f:
            self._container.upload_blob(name=key, data=f, overwrite=True)

    def get(self, key: str, local_dest: Path) -> None:
        local_dest.parent.mkdir(parents=True, exist_ok=True)
        stream = self._container.download_blob(key)
        with local_dest.open("wb") as f:
            f.write(stream.readall())

    def list(self, prefix: str = "") -> list[ObjectMeta]:
        result: list[ObjectMeta] = []
        for blob in self._container.list_blobs(name_starts_with=prefix):
            size = blob.size or 0
            last_mod_epoch = blob.last_modified.timestamp() if blob.last_modified else 0.0
            result.append(ObjectMeta(key=blob.name, size=size, last_modified_epoch=last_mod_epoch))
        result.sort(key=lambda m: m.key)
        return result

    def exists(self, key: str) -> bool:
        try:
            self._container.get_blob_client(key).get_blob_properties()
            return True
        except ResourceNotFoundError:
            return False

    def delete(self, key: str) -> None:
        with contextlib.suppress(ResourceNotFoundError):
            self._container.delete_blob(key)
