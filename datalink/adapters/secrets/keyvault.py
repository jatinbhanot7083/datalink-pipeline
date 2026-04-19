"""AzureKeyVaultSecrets — prod secret provider."""

from __future__ import annotations

from datalink.config.models import SecretProviderConfig


class AzureKeyVaultSecrets:
    def __init__(self, cfg: SecretProviderConfig) -> None:
        self._cfg = cfg

    def get(self, name: str) -> str:
        raise NotImplementedError("AzureKeyVaultSecrets lands in Phase 4 (prod path)")

    def get_optional(self, name: str) -> str | None:
        raise NotImplementedError("AzureKeyVaultSecrets lands in Phase 4 (prod path)")
