"""One-shot embedder for existing data — Phase 8 RAG bootstrap.

Walks ``CONTROL.dq_suites`` and embeds every LIVE row into
``agent_memory.suite_embeddings``. Walks ``CONTROL.agent_reasoning_log``
and embeds every prior agent invocation into
``agent_memory.reasoning_embeddings``.

Idempotent — re-running just upserts the same rows; safe to schedule on
a daily cron if you want freshness without a hot path.

Run:
    python -m scripts.backfill_embeddings
    python -m scripts.backfill_embeddings --suites-only
    python -m scripts.backfill_embeddings --reasoning-only --limit 500
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any

from datalink.adapters.embeddings.router import get_embedder
from datalink.adapters.factory import build_adapters
from datalink.config.loader import load_settings
from datalink.logging import get_logger
from datalink.memory import AgentMemoryStore
from datalink.memory.store import reasoning_source_text, suite_source_text
from datalink.quality.control import CONTROL_SCHEMA

_log = get_logger(__name__)


def backfill_suites(
    memory: AgentMemoryStore, warehouse: Any, *, statuses: tuple[str, ...] = ("LIVE", "ARCHIVED")
) -> int:
    """Embed every suite row whose status matches `statuses`."""
    placeholders = ",".join(["%s"] * len(statuses))
    # Build a paramstyle-correct WHERE without leaning on the dollar
    # rewriter (this script supports DuckDB and Snowflake equivalently).
    if hasattr(warehouse, "_rewrite_dollar_to_pyformat"):
        # Snowflake: use $name. Pass a dict.
        sql = (
            f"SELECT suite_id, client_id, suite_name, version, status, source, "
            f"source_type, dq_dimensions, expectations FROM {CONTROL_SCHEMA}.dq_suites "
            f"WHERE status IN (" + ",".join(f"$s{i}" for i in range(len(statuses))) + ")"
        )
        params: dict[str, Any] = {f"s{i}": s for i, s in enumerate(statuses)}
    else:
        # DuckDB: positional ?
        sql = (
            f"SELECT suite_id, client_id, suite_name, version, status, source, "
            f"source_type, dq_dimensions, expectations FROM {CONTROL_SCHEMA}.dq_suites "
            f"WHERE status IN ({placeholders})"
        )
        params = list(statuses)  # type: ignore[assignment]
    rows = warehouse.query(sql, params)
    n = 0
    for r in rows:
        text = suite_source_text(r)
        memory.upsert_suite(
            suite_id=r["suite_id"],
            client_id=r["client_id"],
            suite_name=r["suite_name"],
            status=r["status"],
            source=r.get("source", ""),
            source_type=r.get("source_type"),
            source_text=text,
        )
        n += 1
    _log.info("backfill.suites_done", count=n)
    return n


def backfill_reasoning(memory: AgentMemoryStore, warehouse: Any, *, limit: int = 1000) -> int:
    """Embed the most recent N agent_reasoning_log rows.

    Capped at `limit` because the table can grow fast and a fresh embed
    pass over 100k rows is wasteful when only the last few thousand
    realistically inform Post-Val grounding.
    """
    # Real columns on agent_reasoning_log: invocation_id, agent_name,
    # crew_name, ts, input_preview, output_preview, tokens_used,
    # duration_ms, phi_check. Successful runs are phi_check='clean';
    # failures embed an error string in phi_check ("error: ..."). Filter
    # to clean rows so we don't embed agent stack traces.
    # See datalink/agents/base.py:_audit.
    sql = (
        f"SELECT invocation_id, agent_name, crew_name, output_preview "
        f"FROM {CONTROL_SCHEMA}.agent_reasoning_log "
        f"WHERE phi_check = 'clean' "
        f"ORDER BY ts DESC LIMIT {int(limit)}"
    )
    try:
        rows = warehouse.query(sql)
    except Exception as e:
        _log.warning("backfill.reasoning_query_failed", error=str(e))
        return 0
    n = 0
    for r in rows:
        try:
            payload = json.loads(r.get("output_preview") or "{}")
        except Exception:
            payload = {"raw": r.get("output_preview", "")[:1000]}
        if not isinstance(payload, dict):
            payload = {"raw": str(payload)[:1000]}
        text = reasoning_source_text(payload)
        if not text.strip():
            continue
        memory.upsert_reasoning(
            invocation_id=r["invocation_id"],
            client_id=payload.get("client_id"),
            agent_name=r["agent_name"],
            crew_name=r["crew_name"],
            breach_class=payload.get("classification"),
            recommendation=payload.get("recommendation"),
            source_text=text,
        )
        n += 1
    _log.info("backfill.reasoning_done", count=n)
    return n


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--suites-only", action="store_true")
    p.add_argument("--reasoning-only", action="store_true")
    p.add_argument(
        "--limit", type=int, default=1000, help="Max reasoning rows to embed (default 1000)"
    )
    args = p.parse_args(argv)

    settings = load_settings()
    adapters = build_adapters(settings)
    embedder = get_embedder()
    memory = AgentMemoryStore(embedder=embedder)
    memory.ensure_schema()

    suites_n = 0
    reasoning_n = 0
    if not args.reasoning_only:
        suites_n = backfill_suites(memory, adapters.warehouse)
    if not args.suites_only:
        reasoning_n = backfill_reasoning(memory, adapters.warehouse, limit=args.limit)

    print(f"Embedded suites: {suites_n}")
    print(f"Embedded reasoning rows: {reasoning_n}")
    print(f"Stats: {memory.stats()}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
