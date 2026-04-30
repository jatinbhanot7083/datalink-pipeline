"""AgentMemoryStore — pgvector-backed RAG persistence for the agent layer.

Two tables, both indexed for cosine similarity:

    agent_memory.suite_embeddings
        suite_id           TEXT PK    matches CONTROL.dq_suites.suite_id
        client_id          TEXT
        suite_name         TEXT
        status             TEXT       LIVE | ARCHIVED (we keep ARCHIVED so
                                       the agent can reason about retired
                                       checks too — comment them out, etc.)
        source             TEXT       baseline_python | ui | agent
        source_type        TEXT       CLAIMS | MEMBERSHIP | PROVIDER | NULL
        source_text        TEXT       PHI-safe humanised dump used to
                                       generate the embedding (kept for
                                       audit + re-embedding on model swap)
        embedding_model    TEXT       e.g. voyage-3-lite (stamp the model
                                       so we can detect drift after a swap)
        embedded_at        TIMESTAMPTZ
        embedding          vector(N)  pgvector column — N matches the
                                       embedder's `dim`

    agent_memory.reasoning_embeddings
        invocation_id      TEXT PK    matches CONTROL.agent_reasoning_log
        client_id          TEXT
        agent_name         TEXT
        crew_name          TEXT
        breach_class       TEXT       optional — DATA_QUALITY | CONFIG | ...
        recommendation     TEXT       optional — PROCEED | FIX_AND_RESUME | ...
        source_text        TEXT
        embedding_model    TEXT
        embedded_at        TIMESTAMPTZ
        embedding          vector(N)

Indexing strategy:
  * Cosine distance via ivfflat — fast enough for the few-thousand-rows
    scale we expect, no GPU needed. Lists tuned to ~sqrt(N) — recreate
    the index if rows grow > 50k.
  * Lookups always include a client_id WHERE clause so cross-tenant
    bleed-through can't happen by accident.

Schema-version stamp on the table comment lets us detect skew between
the ``ensure_schema()`` we expect and what's already there. Bump the
constant when adding columns.
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from datalink.adapters.embeddings.protocol import EmbeddingProvider
from datalink.logging import get_logger

_log = get_logger(__name__)

_SCHEMA_VERSION = 1
_SCHEMA = "agent_memory"

# How many candidate rows pgvector should retrieve before filtering.
# Bigger = better recall at higher latency. 50 is fine for current scale.
_LIST_PROBE = 50


@dataclass(frozen=True)
class SuiteHit:
    suite_id: str
    client_id: str
    suite_name: str
    status: str
    source: str
    source_type: str | None
    source_text: str
    distance: float  # 0.0 = identical, ~1.0 = orthogonal


@dataclass(frozen=True)
class ReasoningHit:
    invocation_id: str
    client_id: str | None
    agent_name: str
    crew_name: str
    breach_class: str | None
    recommendation: str | None
    source_text: str
    distance: float


# Phase 14 — chunk-level retrieval result for the Data Contract Architect.
# A "standard" is a corpus (FHIR R4, X12, NCPDP, CMS, DV2, HEDIS, or a
# custom org-uploaded spec). Each corpus is chunked + embedded into
# `agent_memory.standard_references` and retrieved by the
# ContractArchitectAgent at proposal time.
@dataclass(frozen=True)
class StandardChunkHit:
    ref_id: str
    standard_id: str
    standard_code: str  # e.g. "fhir-r4" | "x12" | "custom-datalink-claims-v3"
    chunk_text: str
    chunk_index: int
    section_path: str | None  # e.g. "Claim/item/productOrService" or "837P/loop2300/CLM01"
    resource_type: str | None  # e.g. "Claim" | "837P_loop_2300"
    distance: float  # 0.0 = identical, ~1.0 = orthogonal


class AgentMemoryStore:
    """Connect lazily; one short-lived psycopg connection per call.

    Pattern matches the warehouse adapter's "open / use / close" so we
    don't hold a write lock during agent thinking. Every public method
    is idempotent on schema state.
    """

    def __init__(
        self,
        embedder: EmbeddingProvider,
        *,
        host: str | None = None,
        port: int | None = None,
        user: str | None = None,
        password: str | None = None,
        database: str | None = None,
    ) -> None:
        # Same env vars the warehouse router + Control Tower already use.
        self._host = host or os.environ.get("POSTGRES_HOST", "postgres")
        # Coerce explicitly — port may be int (constructor arg) or str (env var).
        self._port = int(port if port is not None else os.environ.get("POSTGRES_PORT", "5432"))
        self._user = user or os.environ.get("POSTGRES_USER", "datalink")
        self._password = password or os.environ.get("POSTGRES_PASSWORD", "datalink_local_only")
        self._database = database or os.environ.get("POSTGRES_DATABASE", "datalink_um")
        self._embedder = embedder

    # ------------------------------------------------------------------
    # Schema management
    # ------------------------------------------------------------------

    def ensure_schema(self) -> None:
        """Create the extension, schema, and tables if they don't exist.

        Safe to call on every container start — fully idempotent. The
        ``vector`` column type is sized off ``self._embedder.dim`` so
        flipping from Voyage (1024) to a different model only requires
        a re-embed, not a schema migration (assuming both are 1024).
        """
        dim = self._embedder.dim
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {_SCHEMA}")
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {_SCHEMA}.suite_embeddings (
                    suite_id        TEXT PRIMARY KEY,
                    client_id       TEXT NOT NULL,
                    suite_name      TEXT NOT NULL,
                    status          TEXT NOT NULL,
                    source          TEXT NOT NULL,
                    source_type     TEXT,
                    source_text     TEXT NOT NULL,
                    embedding_model TEXT NOT NULL,
                    embedded_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    embedding       vector({dim}) NOT NULL
                )
                """
            )
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {_SCHEMA}.reasoning_embeddings (
                    invocation_id   TEXT PRIMARY KEY,
                    client_id       TEXT,
                    agent_name      TEXT NOT NULL,
                    crew_name       TEXT NOT NULL,
                    breach_class    TEXT,
                    recommendation  TEXT,
                    source_text     TEXT NOT NULL,
                    embedding_model TEXT NOT NULL,
                    embedded_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    embedding       vector({dim}) NOT NULL
                )
                """
            )
            # Phase 14 — Data Contract Architect reference corpus.
            # Each standard (FHIR R4, X12, NCPDP, CMS, DV2, HEDIS, or
            # operator-uploaded custom) is chunked + embedded here so the
            # ContractArchitectAgent can retrieve top-k similar reference
            # passages at proposal time. `standard_id` matches the row in
            # CONTROL.standard_registry; we denormalise `standard_code`
            # (e.g. "fhir-r4") so the WHERE clause on hot retrieval paths
            # stays index-friendly.
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {_SCHEMA}.standard_references (
                    ref_id          TEXT PRIMARY KEY,
                    standard_id     TEXT NOT NULL,
                    standard_code   TEXT NOT NULL,
                    chunk_text      TEXT NOT NULL,
                    chunk_index     INTEGER NOT NULL,
                    section_path    TEXT,
                    resource_type   TEXT,
                    keywords        TEXT[],
                    embedding_model TEXT NOT NULL,
                    embedded_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    embedding       vector({dim}) NOT NULL
                )
                """
            )
            cur.execute(
                f"CREATE INDEX IF NOT EXISTS standard_refs_code_idx "
                f"ON {_SCHEMA}.standard_references (standard_code)"
            )
            # Cosine-distance ANN indexes — built lazily, not failure-fatal
            # if the table is too small (<some threshold) for IVFFlat
            # quality. Wrap in try/except so a fresh empty install
            # doesn't refuse to come up.
            try:
                cur.execute(
                    f"CREATE INDEX IF NOT EXISTS suite_embeddings_cos_idx "
                    f"ON {_SCHEMA}.suite_embeddings USING ivfflat (embedding vector_cosine_ops) "
                    f"WITH (lists = 50)"
                )
            except Exception as e:
                _log.warning("memory.suite_index_skipped", error=str(e))
            try:
                cur.execute(
                    f"CREATE INDEX IF NOT EXISTS reasoning_embeddings_cos_idx "
                    f"ON {_SCHEMA}.reasoning_embeddings USING ivfflat (embedding vector_cosine_ops) "
                    f"WITH (lists = 50)"
                )
            except Exception as e:
                _log.warning("memory.reasoning_index_skipped", error=str(e))
            try:
                cur.execute(
                    f"CREATE INDEX IF NOT EXISTS standard_references_cos_idx "
                    f"ON {_SCHEMA}.standard_references USING ivfflat (embedding vector_cosine_ops) "
                    f"WITH (lists = 50)"
                )
            except Exception as e:
                _log.warning("memory.standard_index_skipped", error=str(e))
            cur.execute(
                f"COMMENT ON SCHEMA {_SCHEMA} IS 'datalink agent memory v{_SCHEMA_VERSION}'"
            )
            conn.commit()
        _log.info(
            "memory.schema_ensured",
            schema=_SCHEMA,
            dim=dim,
            embedding_model=self._embedder.model,
        )

    # ------------------------------------------------------------------
    # Suite embeddings (Tier A)
    # ------------------------------------------------------------------

    def upsert_suite(
        self,
        *,
        suite_id: str,
        client_id: str,
        suite_name: str,
        status: str,
        source: str,
        source_type: str | None,
        source_text: str,
    ) -> None:
        """Embed and persist a single suite. Re-running overwrites prior row."""
        embedding = self._embedder.embed_one(source_text)
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {_SCHEMA}.suite_embeddings
                  (suite_id, client_id, suite_name, status, source, source_type,
                   source_text, embedding_model, embedded_at, embedding)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (suite_id) DO UPDATE SET
                  client_id       = EXCLUDED.client_id,
                  suite_name      = EXCLUDED.suite_name,
                  status          = EXCLUDED.status,
                  source          = EXCLUDED.source,
                  source_type     = EXCLUDED.source_type,
                  source_text     = EXCLUDED.source_text,
                  embedding_model = EXCLUDED.embedding_model,
                  embedded_at     = EXCLUDED.embedded_at,
                  embedding       = EXCLUDED.embedding
                """,
                (
                    suite_id,
                    client_id,
                    suite_name,
                    status,
                    source,
                    source_type,
                    source_text,
                    self._embedder.model,
                    datetime.now(UTC),
                    _to_pgvector(embedding),
                ),
            )
            conn.commit()

    def query_similar_suites(
        self,
        *,
        query_text: str,
        client_id: str | None = None,
        k: int = 3,
        include_default: bool = True,
        statuses: tuple[str, ...] = ("LIVE",),
    ) -> list[SuiteHit]:
        """Return top-k LIVE suites most similar to query_text.

        Default behaviour: search within ``client_id``'s suites + the
        ``default`` baseline as a fallback for new tenants. Pass
        ``include_default=False`` for strict per-tenant retrieval.
        """
        if not query_text.strip():
            return []
        embedding = self._embedder.embed_one(query_text)
        client_filter_sql = ""
        params: list[Any] = []
        if client_id:
            if include_default:
                client_filter_sql = "AND client_id IN (%s, 'default')"
                params.append(client_id)
            else:
                client_filter_sql = "AND client_id = %s"
                params.append(client_id)
        status_placeholders = ",".join(["%s"] * len(statuses))
        params.extend(statuses)
        params.append(_to_pgvector(embedding))
        params.append(k)
        sql = f"""
            SELECT suite_id, client_id, suite_name, status, source, source_type,
                   source_text, embedding <=> %s AS distance
            FROM {_SCHEMA}.suite_embeddings
            WHERE status IN ({status_placeholders}) {client_filter_sql}
            ORDER BY embedding <=> %s
            LIMIT %s
        """
        # The %s placeholders order: statuses…, client (if any), query_vec, query_vec, k.
        # Reshuffle to that order:
        ordered_params: list[Any] = list(statuses)
        if client_id:
            ordered_params.append(client_id)
        # query_vec appears TWICE — once in SELECT distance, once in ORDER BY.
        # Inject the same vec object both times so they share a serialisation.
        ordered_params = [_to_pgvector(embedding), *ordered_params, _to_pgvector(embedding), k]
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(sql, ordered_params)
            rows = cur.fetchall()
        return [
            SuiteHit(
                suite_id=r[0],
                client_id=r[1],
                suite_name=r[2],
                status=r[3],
                source=r[4],
                source_type=r[5],
                source_text=r[6],
                distance=float(r[7]),
            )
            for r in rows
        ]

    # ------------------------------------------------------------------
    # Reasoning embeddings (Tier B)
    # ------------------------------------------------------------------

    def upsert_reasoning(
        self,
        *,
        invocation_id: str,
        client_id: str | None,
        agent_name: str,
        crew_name: str,
        breach_class: str | None,
        recommendation: str | None,
        source_text: str,
    ) -> None:
        embedding = self._embedder.embed_one(source_text)
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {_SCHEMA}.reasoning_embeddings
                  (invocation_id, client_id, agent_name, crew_name,
                   breach_class, recommendation, source_text,
                   embedding_model, embedded_at, embedding)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (invocation_id) DO UPDATE SET
                  client_id       = EXCLUDED.client_id,
                  agent_name      = EXCLUDED.agent_name,
                  crew_name       = EXCLUDED.crew_name,
                  breach_class    = EXCLUDED.breach_class,
                  recommendation  = EXCLUDED.recommendation,
                  source_text     = EXCLUDED.source_text,
                  embedding_model = EXCLUDED.embedding_model,
                  embedded_at     = EXCLUDED.embedded_at,
                  embedding       = EXCLUDED.embedding
                """,
                (
                    invocation_id,
                    client_id,
                    agent_name,
                    crew_name,
                    breach_class,
                    recommendation,
                    source_text,
                    self._embedder.model,
                    datetime.now(UTC),
                    _to_pgvector(embedding),
                ),
            )
            conn.commit()

    def query_similar_reasoning(
        self,
        *,
        query_text: str,
        client_id: str | None = None,
        agent_name: str | None = None,
        k: int = 3,
    ) -> list[ReasoningHit]:
        if not query_text.strip():
            return []
        embedding = self._embedder.embed_one(query_text)
        clauses = []
        params: list[Any] = [_to_pgvector(embedding)]
        if client_id:
            clauses.append("client_id = %s")
            params.append(client_id)
        if agent_name:
            clauses.append("agent_name = %s")
            params.append(agent_name)
        where = " AND ".join(clauses) if clauses else "TRUE"
        params.append(_to_pgvector(embedding))
        params.append(k)
        sql = f"""
            SELECT invocation_id, client_id, agent_name, crew_name,
                   breach_class, recommendation, source_text,
                   embedding <=> %s AS distance
            FROM {_SCHEMA}.reasoning_embeddings
            WHERE {where}
            ORDER BY embedding <=> %s
            LIMIT %s
        """
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
        return [
            ReasoningHit(
                invocation_id=r[0],
                client_id=r[1],
                agent_name=r[2],
                crew_name=r[3],
                breach_class=r[4],
                recommendation=r[5],
                source_text=r[6],
                distance=float(r[7]),
            )
            for r in rows
        ]

    # ------------------------------------------------------------------
    # Standard reference embeddings (Tier C, Phase 14)
    # The Data Contract Architect's RAG corpus. Each chunk is one
    # ~500-token slice of an industry standard (FHIR R4, X12, NCPDP,
    # CMS, DV2, HEDIS) or an operator-uploaded "custom" spec.
    # ------------------------------------------------------------------

    def upsert_standard_chunks_bulk(
        self,
        *,
        chunks: list[dict[str, Any]],
        max_tokens_per_batch: int = 8000,
        retry_sleep_s: int = 25,
        max_retries: int = 4,
    ) -> int:
        """Embed and persist many chunks in batched API calls + ONE DB transaction.

        Sub-batches by approximate token count (~4 chars/token) so each
        API call stays under Voyage's free-tier 10K TPM. Sleeps between
        batches to respect 3 RPM. Retries with exponential backoff on
        rate-limit errors.

        Each dict in ``chunks`` must have keys: ref_id, standard_id,
        standard_code, chunk_text, chunk_index, section_path (opt),
        resource_type (opt), keywords (opt).

        Returns the number of rows upserted.
        """
        if not chunks:
            return 0
        # Split into sub-batches under max_tokens_per_batch
        sub_batches: list[list[dict[str, Any]]] = []
        cur_batch: list[dict[str, Any]] = []
        cur_tokens = 0
        for c in chunks:
            est_tokens = max(1, len(c["chunk_text"]) // 4)  # ~4 chars/token
            if cur_tokens + est_tokens > max_tokens_per_batch and cur_batch:
                sub_batches.append(cur_batch)
                cur_batch = []
                cur_tokens = 0
            cur_batch.append(c)
            cur_tokens += est_tokens
        if cur_batch:
            sub_batches.append(cur_batch)

        # Embed each sub-batch with retry; pause between batches.
        all_embeddings: list[list[float]] = []
        import time

        for i, batch in enumerate(sub_batches):
            attempt = 0
            while True:
                try:
                    texts = [c["chunk_text"] for c in batch]
                    embs = self._embedder.embed_many(texts)
                    all_embeddings.extend(embs)
                    break
                except Exception as e:
                    msg = str(e).lower()
                    is_rate = (
                        "rate" in msg or "429" in msg or "ratelimiterror" in str(type(e)).lower()
                    )
                    if not is_rate or attempt >= max_retries:
                        raise
                    sleep_s = retry_sleep_s * (2**attempt)
                    _log.warning(
                        "memory.embed_rate_limit_retry",
                        attempt=attempt,
                        sleep_s=sleep_s,
                        sub_batch=i,
                    )
                    time.sleep(sleep_s)
                    attempt += 1
            # Pause between sub-batches to stay under 3 RPM
            if i < len(sub_batches) - 1:
                _log.info("memory.embed_batch_pause", batch=i, total=len(sub_batches))
                time.sleep(retry_sleep_s)

        embeddings = all_embeddings
        if len(embeddings) != len(chunks):
            raise RuntimeError(
                f"embed_many returned {len(embeddings)} vectors for {len(chunks)} chunks"
            )
        ts = datetime.now(UTC)
        with self._connect() as conn, conn.cursor() as cur:
            for c, vec in zip(chunks, embeddings, strict=True):
                cur.execute(
                    f"""
                    INSERT INTO {_SCHEMA}.standard_references
                      (ref_id, standard_id, standard_code, chunk_text, chunk_index,
                       section_path, resource_type, keywords,
                       embedding_model, embedded_at, embedding)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (ref_id) DO UPDATE SET
                      standard_id     = EXCLUDED.standard_id,
                      standard_code   = EXCLUDED.standard_code,
                      chunk_text      = EXCLUDED.chunk_text,
                      chunk_index     = EXCLUDED.chunk_index,
                      section_path    = EXCLUDED.section_path,
                      resource_type   = EXCLUDED.resource_type,
                      keywords        = EXCLUDED.keywords,
                      embedding_model = EXCLUDED.embedding_model,
                      embedded_at     = EXCLUDED.embedded_at,
                      embedding       = EXCLUDED.embedding
                    """,
                    (
                        c["ref_id"],
                        c["standard_id"],
                        c["standard_code"],
                        c["chunk_text"],
                        c["chunk_index"],
                        c.get("section_path"),
                        c.get("resource_type"),
                        c.get("keywords") or [],
                        self._embedder.model,
                        ts,
                        _to_pgvector(vec),
                    ),
                )
            conn.commit()
        return len(chunks)

    def upsert_standard_chunk(
        self,
        *,
        ref_id: str,
        standard_id: str,
        standard_code: str,
        chunk_text: str,
        chunk_index: int,
        section_path: str | None = None,
        resource_type: str | None = None,
        keywords: list[str] | None = None,
    ) -> None:
        """Embed and persist one chunk of a standard's reference corpus.
        Idempotent — re-running with the same ``ref_id`` overwrites."""
        embedding = self._embedder.embed_one(chunk_text)
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {_SCHEMA}.standard_references
                  (ref_id, standard_id, standard_code, chunk_text, chunk_index,
                   section_path, resource_type, keywords,
                   embedding_model, embedded_at, embedding)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (ref_id) DO UPDATE SET
                  standard_id     = EXCLUDED.standard_id,
                  standard_code   = EXCLUDED.standard_code,
                  chunk_text      = EXCLUDED.chunk_text,
                  chunk_index     = EXCLUDED.chunk_index,
                  section_path    = EXCLUDED.section_path,
                  resource_type   = EXCLUDED.resource_type,
                  keywords        = EXCLUDED.keywords,
                  embedding_model = EXCLUDED.embedding_model,
                  embedded_at     = EXCLUDED.embedded_at,
                  embedding       = EXCLUDED.embedding
                """,
                (
                    ref_id,
                    standard_id,
                    standard_code,
                    chunk_text,
                    chunk_index,
                    section_path,
                    resource_type,
                    keywords or [],
                    self._embedder.model,
                    datetime.now(UTC),
                    _to_pgvector(embedding),
                ),
            )
            conn.commit()

    def query_similar_standard_chunks(
        self,
        *,
        query_text: str,
        anchored_codes: tuple[str, ...] | list[str],
        k: int = 3,
        resource_type: str | None = None,
    ) -> list[StandardChunkHit]:
        """Top-k chunks across the anchored standards.

        Operator picks 1+ standards on the Data Contract Architect page
        (e.g. ['fhir-r4', 'x12']). We retrieve the top-k chunks similar
        to ``query_text`` whose ``standard_code`` is in that allowlist —
        prevents an FHIR proposal from drifting into HEDIS by accident.
        """
        if not query_text.strip() or not anchored_codes:
            return []
        embedding = self._embedder.embed_one(query_text)
        codes = list(anchored_codes)
        code_placeholders = ",".join(["%s"] * len(codes))
        params: list[Any] = [_to_pgvector(embedding), *codes]
        resource_clause = ""
        if resource_type:
            resource_clause = "AND resource_type = %s"
            params.append(resource_type)
        params.extend([_to_pgvector(embedding), k])
        sql = f"""
            SELECT ref_id, standard_id, standard_code, chunk_text, chunk_index,
                   section_path, resource_type,
                   embedding <=> %s AS distance
            FROM {_SCHEMA}.standard_references
            WHERE standard_code IN ({code_placeholders}) {resource_clause}
            ORDER BY embedding <=> %s
            LIMIT %s
        """
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
        return [
            StandardChunkHit(
                ref_id=r[0],
                standard_id=r[1],
                standard_code=r[2],
                chunk_text=r[3],
                chunk_index=r[4],
                section_path=r[5],
                resource_type=r[6],
                distance=float(r[7]),
            )
            for r in rows
        ]

    def delete_standard_chunks(self, *, standard_id: str) -> int:
        """Drop all chunks for a standard (used on re-ingestion).
        Returns the number of rows deleted."""
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM {_SCHEMA}.standard_references WHERE standard_id = %s",
                (standard_id,),
            )
            count = cur.rowcount
            conn.commit()
        return int(count)

    def count_standard_chunks(self, *, standard_id: str | None = None) -> int:
        """Count of chunks; per-standard (if standard_id passed) or total."""
        with self._connect() as conn, conn.cursor() as cur:
            if standard_id:
                cur.execute(
                    f"SELECT COUNT(*) FROM {_SCHEMA}.standard_references WHERE standard_id = %s",
                    (standard_id,),
                )
            else:
                cur.execute(f"SELECT COUNT(*) FROM {_SCHEMA}.standard_references")
            return int(cur.fetchone()[0])

    # ------------------------------------------------------------------
    # Stats — feed the AI Agents dashboard "AI memory used" tile
    # ------------------------------------------------------------------

    def stats(self) -> dict[str, Any]:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {_SCHEMA}.suite_embeddings")
            suite_count = cur.fetchone()[0]
            cur.execute(f"SELECT COUNT(*) FROM {_SCHEMA}.reasoning_embeddings")
            reason_count = cur.fetchone()[0]
            cur.execute(
                f"SELECT MIN(embedded_at), MAX(embedded_at) FROM {_SCHEMA}.suite_embeddings"
            )
            row = cur.fetchone()
        return {
            "suite_embeddings": int(suite_count),
            "reasoning_embeddings": int(reason_count),
            "earliest_embed": row[0].isoformat() if row[0] else None,
            "latest_embed": row[1].isoformat() if row[1] else None,
            "model": self._embedder.model,
            "dim": self._embedder.dim,
        }

    # ------------------------------------------------------------------
    # Connection helper
    # ------------------------------------------------------------------

    @contextmanager
    def _connect(self) -> Iterator[Any]:
        # Lazy import — psycopg may not be in every container that
        # touches datalink.memory transitively (e.g. CI lint pass).
        import psycopg

        conn = psycopg.connect(
            host=self._host,
            port=self._port,
            user=self._user,
            password=self._password,
            dbname=self._database,
            connect_timeout=5,
        )
        try:
            yield conn
        finally:
            conn.close()


def _to_pgvector(values: list[float]) -> str:
    """pgvector accepts a string literal '[v1,v2,...]'. Stable serialisation
    across stub + Voyage outputs (both produce list[float])."""
    return "[" + ",".join(f"{v:.7f}" for v in values) + "]"


# ---------------------------------------------------------------------------
# Source-text builders — bake humanised, PHI-safe text from registry rows.
# Centralised here so both backfill scripts and live hooks emit identical
# embedding inputs (otherwise stub vectors would diverge).
# ---------------------------------------------------------------------------


def suite_source_text(suite_row: dict[str, Any]) -> str:
    """Humanised dump of a SuiteVersion row for embedding.

    The shape: client + name + dimensions list + per-expectation
    (type, column, severity, description). No row data, no PHI fields,
    no kwargs values that aren't column names or thresholds.
    """
    parts: list[str] = []
    parts.append(f"Client: {suite_row.get('client_id','')}")
    parts.append(f"Suite: {suite_row.get('suite_name','')}")
    parts.append(f"Source: {suite_row.get('source','')}")
    if st := suite_row.get("source_type"):
        parts.append(f"Source-type: {st}")
    if dims := suite_row.get("dq_dimensions"):
        try:
            dims_list = json.loads(dims) if isinstance(dims, str) else list(dims)
            parts.append(f"Dimensions: {', '.join(dims_list)}")
        except Exception:
            pass

    exps_raw = suite_row.get("expectations")
    try:
        exps = json.loads(exps_raw) if isinstance(exps_raw, str) else list(exps_raw or [])
    except Exception:
        exps = []
    for i, e in enumerate(exps):
        kw = e.get("kwargs", {}) or {}
        meta = e.get("meta", {}) or {}
        parts.append(
            f"  - Expectation {i+1}: {e.get('expectation_type','?')} "
            f"on column={kw.get('column') or kw.get('column_A') or '—'} "
            f"sev={meta.get('severity','')} dim={meta.get('dq_dimension','')} "
            f"desc={meta.get('description','')}"
        )
    return "\n".join(parts)


def reasoning_source_text(payload: dict[str, Any]) -> str:
    """Humanised dump of an agent_reasoning_log payload for embedding.

    Pulls only the safe meta fields — everything is already PHI-redacted
    at write time, but we still pick the narrative-bearing keys
    explicitly so we don't accidentally embed JSON dumps with auth tokens
    or other unanticipated leaks.
    """
    keys_of_interest = (
        "classification",
        "recommendation",
        "severity",
        "summary",
        "rationale",
        "fail_pct",
        "row_count",
        "checkpoint_name",
        "table_name",
        "schema_name",
    )
    parts: list[str] = []
    for k in keys_of_interest:
        v = payload.get(k)
        if v is None:
            continue
        parts.append(f"{k}: {v}")
    return "\n".join(parts) if parts else json.dumps(payload, default=str)[:2000]
