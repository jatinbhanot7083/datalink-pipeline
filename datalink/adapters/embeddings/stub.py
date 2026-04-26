"""StubEmbedder — deterministic offline-friendly embedding fallback.

Hash-based unit-vector generation. Two strings that share a SHA-256 prefix
get cosine-similar vectors; unrelated strings stay near-orthogonal. Quality
is well below a real model — enough for stub demos and tests where
*deterministic* matters more than *accurate*. Production: route through
``VoyageEmbedder`` instead.

Vector dimension is fixed at 1024 to match Voyage-3-lite, so the pgvector
column type is the same in stub vs prod and migrations are not required
when flipping the embeddings adapter type.
"""

from __future__ import annotations

import hashlib
import math

_STUB_DIM = 1024


class StubEmbedder:
    model = "stub-1024"
    dim = _STUB_DIM

    def embed_one(self, text: str) -> list[float]:
        return _hash_to_unit_vector(text, self.dim)

    def embed_many(self, texts: list[str]) -> list[list[float]]:
        return [self.embed_one(t) for t in texts]


def _hash_to_unit_vector(text: str, dim: int) -> list[float]:
    """Stretch SHA-256 of `text` to `dim` floats and L2-normalise.

    Deterministic per input, sufficient spread for cosine similarity to
    distinguish "claim_id null check" from "billed_amount range check"
    when both come back as approximate text. Not a substitute for a real
    embedding model — explicitly so stub mode is obvious in eval logs.
    """
    seed = hashlib.sha256(text.encode("utf-8", errors="ignore")).digest()
    needed = dim * 4  # 4 bytes per float component
    buf = bytearray()
    counter = 0
    while len(buf) < needed:
        buf.extend(hashlib.sha256(seed + counter.to_bytes(4, "big")).digest())
        counter += 1
    raw = [
        int.from_bytes(buf[i : i + 4], "big", signed=False) / 0xFFFFFFFF * 2.0 - 1.0
        for i in range(0, needed, 4)
    ]
    norm = math.sqrt(sum(x * x for x in raw)) or 1.0
    return [x / norm for x in raw]
