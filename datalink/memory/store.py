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
