"""Agent memory — pgvector-backed RAG store for the LLM agent layer.

Two corpora live here:

  * ``suite_embeddings``       Tier A — embeds every LIVE DQ suite per
                                client. Powers "AI grounded its proposal
                                on these N similar past suites" in the
                                Workshop and the Pre-Val crew.
  * ``reasoning_embeddings``   Tier B — embeds every agent invocation
                                that classified a breach or proposed a
                                remediation. Powers "AI saw N similar
                                past breaches" in the Post-Val crew.

Both tables sit on the **operational Postgres** (datalink_um), separated
from the warehouse so:

  1. Snowflake credit consumption is bounded — embeddings + vector search
     never hit Snowflake.
  2. RAG works on DuckDB-mode AND Snowflake-mode runs identically — the
     warehouse is the source of truth for SUITES, but the vector index
     is backend-agnostic.
  3. We keep PHI-safe text in pgvector. The warehouse may store source
     prompts that include user-typed phrases; embeddings here always go
     through ``PhiRedactionLayer`` first.

Public surface:
    AgentMemoryStore           main class — connect / ensure_schema /
                                upsert / query
    SuiteHit, ReasoningHit     immutable retrieval results
"""

from datalink.memory.store import AgentMemoryStore, ReasoningHit, SuiteHit

__all__ = ["AgentMemoryStore", "SuiteHit", "ReasoningHit"]
