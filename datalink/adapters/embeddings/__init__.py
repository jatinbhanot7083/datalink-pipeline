"""Embedding adapters — the vector-generation half of the RAG stack.

Mirrors the LLM adapter pattern: a Protocol + two backing implementations
(real Voyage API + offline Stub) + a router that picks one based on
settings. Same shape as ``datalink.adapters.llm.*`` so any code that
already understands the LLM router reads this module instantly.

Why a separate adapter? Embeddings have different scaling characteristics
than chat completions: bursty backfills, batch-friendly, cheap per call,
*always* need a deterministic offline path for tests + air-gapped demos.

Lazy-loaded ``__getattr__`` mirrors ``datalink.agents`` so importing the
router doesn't drag the (optional) ``voyageai`` SDK into containers that
only need the stub.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from datalink.adapters.embeddings.protocol import EmbeddingProvider
    from datalink.adapters.embeddings.router import get_embedder
    from datalink.adapters.embeddings.stub import StubEmbedder
    from datalink.adapters.embeddings.voyage import VoyageEmbedder

__all__ = [
    "EmbeddingProvider",
    "StubEmbedder",
    "VoyageEmbedder",
    "get_embedder",
]


def __getattr__(name: str) -> Any:
    if name == "EmbeddingProvider":
        from datalink.adapters.embeddings.protocol import EmbeddingProvider as _P

        return _P
    if name == "StubEmbedder":
        from datalink.adapters.embeddings.stub import StubEmbedder as _S

        return _S
    if name == "VoyageEmbedder":
        from datalink.adapters.embeddings.voyage import VoyageEmbedder as _V

        return _V
    if name == "get_embedder":
        from datalink.adapters.embeddings.router import get_embedder as _G

        return _G
    raise AttributeError(f"module 'datalink.adapters.embeddings' has no attribute {name!r}")
