"""VoyageEmbedder — production embedding adapter (Anthropic-recommended).

Hits the Voyage API directly via the ``voyageai`` SDK. Voyage-3-lite is
the default — 1024 dimensions, $0.02/M input tokens, strong performance
on technical text. Voyage-3 (full) bumps quality + cost ~3x.

Why Voyage over OpenAI text-embedding-3? Anthropic explicitly recommends
Voyage for use alongside Claude (https://docs.anthropic.com/en/docs/build-with-claude/embeddings)
and the latency / cost / quality balance fits the project's "pure Anthropic
stack" positioning.
"""

from __future__ import annotations

import os
from typing import Any

from datalink.logging import get_logger

_log = get_logger(__name__)

# Dimension table — keep in sync with whatever Voyage publishes.
# https://docs.voyageai.com/docs/embeddings
_MODEL_DIMS: dict[str, int] = {
    "voyage-3-lite": 1024,
    "voyage-3": 1024,
    "voyage-3-large": 1024,
    "voyage-code-3": 1024,
    "voyage-finance-2": 1024,
}


class VoyageEmbedder:
    def __init__(self, model: str | None = None, api_key: str | None = None) -> None:
        self.model = model or os.environ.get("VOYAGE_MODEL", "voyage-3-lite")
        if self.model not in _MODEL_DIMS:
            raise ValueError(
                f"Unknown Voyage model {self.model!r}; expected one of "
                f"{sorted(_MODEL_DIMS)}. Set VOYAGE_MODEL env var to override."
            )
        self.dim = _MODEL_DIMS[self.model]
        self._api_key = api_key or os.environ.get("VOYAGE_API_KEY")
        if not self._api_key:
            raise RuntimeError(
                "VoyageEmbedder requires VOYAGE_API_KEY. Either set it in .env "
                "or flip DL_ADAPTERS__EMBEDDINGS__TYPE=stub for offline mode."
            )
        self._client: Any = None  # lazy SDK init — keep import out of __init__ path

    def _ensure_client(self) -> Any:
        if self._client is None:
            try:
                import voyageai  # type: ignore[import-untyped]
            except ImportError as e:
                raise RuntimeError(
                    "voyageai package not installed. Add to your image "
                    "(pip install 'voyageai>=0.3,<1.0') or use StubEmbedder."
                ) from e
            self._client = voyageai.Client(api_key=self._api_key)
        return self._client

    def embed_one(self, text: str) -> list[float]:
        return self.embed_many([text])[0]

    def embed_many(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []
        client = self._ensure_client()
        # Voyage caps ~120k tokens per request; we cap input strings at
        # 8 KB each as a defensive measure. Real-life suite metadata is
        # well under this — sample row of agent log is rarely > 3 KB.
        clipped = [t[:8000] for t in texts]
        result = client.embed(clipped, model=self.model, input_type="document")
        _log.info(
            "voyage.embed_many",
            model=self.model,
            count=len(texts),
            total_tokens=getattr(result, "total_tokens", -1),
        )
        return list(result.embeddings)
