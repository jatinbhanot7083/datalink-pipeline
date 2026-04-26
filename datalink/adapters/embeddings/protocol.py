"""EmbeddingProvider protocol — the seam every embedding adapter implements.

Two methods only:
  * ``embed_one(text)``   — vector for a single string. Used at retrieval
                            time when the agent has one query.
  * ``embed_many(texts)`` — vector list for a batch. Used at backfill +
                            on-activate hooks.

Models declare their dimensionality via ``dim`` so callers can validate
against the pgvector column type at insert time.
"""

from __future__ import annotations

from typing import Protocol


class EmbeddingProvider(Protocol):
    """Embedding backend for AgentMemoryStore. Local default: stub. Prod: Voyage."""

    model: str
    dim: int  # vector length — must match the pgvector column type

    def embed_one(self, text: str) -> list[float]: ...

    def embed_many(self, texts: list[str]) -> list[list[float]]: ...
