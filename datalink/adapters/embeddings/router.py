"""Embedding router — pick stub or Voyage per settings, mirror of llm_router.

Uses the same DL_ADAPTERS__EMBEDDINGS__TYPE env var pattern as the LLM
router so configuration shape stays consistent across the stack.
"""

from __future__ import annotations

import os

from datalink.adapters.embeddings.protocol import EmbeddingProvider
from datalink.adapters.embeddings.stub import StubEmbedder
from datalink.logging import get_logger

_log = get_logger(__name__)


def get_embedder() -> EmbeddingProvider:
    """Return the appropriate EmbeddingProvider for the current settings.

    Reads from env directly (not Settings) because the embeddings adapter
    is a leaf — used by datalink.memory which sits below the agent layer.
    Pulling it through pydantic-settings for one bool just adds latency.

    Decision matrix:
      DL_ADAPTERS__EMBEDDINGS__TYPE=voyage + VOYAGE_API_KEY set  ->  Voyage
      DL_ADAPTERS__EMBEDDINGS__TYPE=voyage + no API key          ->  Stub (warning)
      DL_ADAPTERS__EMBEDDINGS__TYPE=stub                          ->  Stub
      unset                                                       ->  Stub (default)
    """
    requested = os.environ.get("DL_ADAPTERS__EMBEDDINGS__TYPE", "stub").lower()
    embedder: EmbeddingProvider
    if requested == "voyage" and os.environ.get("VOYAGE_API_KEY"):
        from datalink.adapters.embeddings.voyage import VoyageEmbedder

        embedder = VoyageEmbedder()
        _log.info("embedding_router.selected", provider="voyage", model=embedder.model)
        return embedder
    if requested == "voyage":
        _log.warning(
            "embedding_router.no_api_key",
            note="DL_ADAPTERS__EMBEDDINGS__TYPE=voyage but VOYAGE_API_KEY missing — falling back to stub.",
        )
    embedder = StubEmbedder()
    _log.info("embedding_router.selected", provider="stub", model=embedder.model)
    return embedder
