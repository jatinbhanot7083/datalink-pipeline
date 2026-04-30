"""Phase 14.2 — Industry-standards corpus loader for Data Contract Architect.

Reads every Markdown file under ``corpora/<standard-code>/`` and:

  1. Registers each standard as a row in ``CONTROL.standard_registry``.
  2. Chunks each file (~500 tokens, 50-token overlap), embeds via the
     active embedder (Voyage when ``DL_ADAPTERS__EMBEDDINGS__TYPE=voyage``,
     otherwise the stub for offline tests).
  3. Writes chunks to ``agent_memory.standard_references`` keyed by
     standard_id + chunk_index.
  4. Updates ``CONTROL.standard_registry.chunk_count`` so the UI can
     show "FHIR R4 — 47 chunks loaded".

Idempotent — re-running deletes prior chunks for each standard before
re-inserting (so corpus content can evolve without manual cleanup).

Usage:

    python scripts/load_industry_standards.py                  # all standards
    python scripts/load_industry_standards.py --only fhir-r4   # one standard
    python scripts/load_industry_standards.py --dry-run        # no DB writes

Run from inside the control_tower container so it sees the right
warehouse + Postgres + Voyage env vars:

    docker exec -it datalink-control-tower python3 scripts/load_industry_standards.py
"""

from __future__ import annotations

import argparse
import hashlib
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

# Path bootstrap
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from datalink.adapters.embeddings.router import get_embedder  # noqa: E402
from datalink.logging import get_logger  # noqa: E402
from datalink.memory import AgentMemoryStore  # noqa: E402
from datalink.quality.control import CONTROL_SCHEMA, create_control_tables  # noqa: E402
from datalink.ui._query import warehouse_ctx  # noqa: E402

_log = get_logger(__name__)

# ============================================================================
# 6 industry standards shipped Day 1 (operator-uploaded "custom" standards
# get added at runtime via the Data Contract Architect upload UI).
# ============================================================================

INDUSTRY_STANDARDS: list[dict[str, str]] = [
    {
        "code": "fhir-r4",
        "display_name": "HL7 FHIR R4",
        "version": "R4",
        "notes": "Fast Healthcare Interoperability Resources, Release 4. Core resources: Claim, Coverage, Patient, Practitioner, Organization, EOB.",
    },
    {
        "code": "x12",
        "display_name": "X12 EDI HIPAA",
        "version": "5010",
        "notes": "ANSI X12 transaction sets used under HIPAA: 837P/I (claims), 834 (enrollment), 270/271 (eligibility), 278 (prior auth).",
    },
    {
        "code": "ncpdp-d0",
        "display_name": "NCPDP D.0 Pharmacy",
        "version": "D.Ø",
        "notes": "National Council for Prescription Drug Programs Telecommunication Standard D.0 — real-time pharmacy claims (B1/B2/B3/D1/E1).",
    },
    {
        "code": "cms",
        "display_name": "CMS Data Dictionaries",
        "version": "current",
        "notes": "CMS Research Identifiable Files (RIF) for Medicare FFS, Medicare Advantage encounter data, T-MSIS Medicaid State Plan submissions.",
    },
    {
        "code": "dv2",
        "display_name": "Data Vault 2.0",
        "version": "2.0",
        "notes": "Dan Linstedt's Data Vault 2.0 modelling pattern: Hub, Link, Sat tables with hash-based PKs for parallel load and full audit history.",
    },
    {
        "code": "hedis",
        "display_name": "NCQA HEDIS",
        "version": "MY 2026",
        "notes": "Healthcare Effectiveness Data and Information Set — quality measures used by health plans, CMS Star Ratings, state Medicaid agencies.",
    },
]

CORPORA_DIR = ROOT / "corpora"

# Chunking parameters. Voyage's voyage-3-lite tokenizer is approximately
# 4 chars per token; we aim for ~500 tokens => ~2000 chars per chunk.
CHUNK_CHARS = 2000
CHUNK_OVERLAP_CHARS = 200


# ============================================================================
# Chunking
# ============================================================================


@dataclass
class Chunk:
    chunk_index: int
    text: str
    section_path: str | None
    resource_type: str | None


def chunk_markdown(md_text: str) -> list[Chunk]:
    """Naive but effective Markdown chunker.

    Strategy:
      * Split by H2 / H3 sections to keep semantically coherent boundaries.
      * If a section is bigger than CHUNK_CHARS, split further with overlap.
      * Tag each chunk with `section_path` (the H1 / H2 / H3 trail) so
        the agent can cite the source location precisely.
    """
    lines = md_text.splitlines()
    sections: list[tuple[str | None, list[str]]] = []  # [(section_path, lines)]
    current_path: list[str] = []
    current_lines: list[str] = []

    def flush() -> None:
        if current_lines:
            sections.append(
                (" / ".join(current_path) if current_path else None, current_lines.copy())
            )
            current_lines.clear()

    for line in lines:
        if line.startswith("# "):
            flush()
            current_path = [line.lstrip("# ").strip()]
        elif line.startswith("## "):
            flush()
            if current_path:
                current_path = [current_path[0], line.lstrip("# ").strip()]
            else:
                current_path = [line.lstrip("# ").strip()]
        elif line.startswith("### "):
            flush()
            base = current_path[:2] if len(current_path) >= 2 else current_path
            current_path = [*base, line.lstrip("# ").strip()]
        else:
            current_lines.append(line)
    flush()

    chunks: list[Chunk] = []
    idx = 0
    for section_path, sec_lines in sections:
        text = "\n".join(sec_lines).strip()
        if not text:
            continue
        # Detect resource_type from path (e.g. "Claim" from "FHIR R4 / Claim")
        resource_type = None
        if section_path:
            parts = [p for p in section_path.split(" / ") if p]
            if len(parts) > 1 and parts[1].isidentifier():
                resource_type = parts[1]

        if len(text) <= CHUNK_CHARS:
            chunks.append(
                Chunk(
                    chunk_index=idx,
                    text=text,
                    section_path=section_path,
                    resource_type=resource_type,
                )
            )
            idx += 1
        else:
            # Long section — split with overlap
            start = 0
            while start < len(text):
                end = min(start + CHUNK_CHARS, len(text))
                # Try to break on sentence boundary near the end
                if end < len(text):
                    break_at = text.rfind(". ", start, end)
                    if break_at > start + CHUNK_CHARS // 2:
                        end = break_at + 1
                chunks.append(
                    Chunk(
                        chunk_index=idx,
                        text=text[start:end].strip(),
                        section_path=section_path,
                        resource_type=resource_type,
                    )
                )
                idx += 1
                if end >= len(text):
                    break
                start = max(0, end - CHUNK_OVERLAP_CHARS)
    return chunks


# ============================================================================
# Registry helpers
# ============================================================================


def upsert_standard_registry(
    wh: object,
    *,
    standard_id: str,
    code: str,
    display_name: str,
    version: str,
    is_industry: bool,
    source_doc_uri: str,
    chunk_count: int,
    notes: str,
    actor: str,
) -> None:
    """Insert or refresh a row in CONTROL.standard_registry.

    Idempotent — re-runs replace by standard_id.
    """
    wh.execute(
        f"DELETE FROM {CONTROL_SCHEMA}.standard_registry WHERE standard_id = $sid",
        {"sid": standard_id},
    )
    wh.execute(
        f"""
        INSERT INTO {CONTROL_SCHEMA}.standard_registry
          (standard_id, code, display_name, version, is_industry, is_active,
           source_doc_uri, chunk_count, registered_at, registered_by, notes)
        VALUES ($sid, $code, $name, $ver, $ind, TRUE,
                $uri, $cnt, $ts, $actor, $notes)
        """,
        {
            "sid": standard_id,
            "code": code,
            "name": display_name,
            "ver": version,
            "ind": is_industry,
            "uri": source_doc_uri,
            "cnt": chunk_count,
            "ts": datetime.now(UTC),
            "actor": actor,
            "notes": notes,
        },
    )


def deterministic_standard_id(code: str) -> str:
    """Industry standards get a stable id derived from code so re-runs
    can be idempotent. Custom standards get random UUIDs at upload time.
    """
    return f"std-{code}"


def deterministic_ref_id(standard_id: str, chunk_index: int) -> str:
    """Stable ref_id so re-runs UPSERT (don't accumulate duplicate rows)."""
    h = hashlib.sha256(f"{standard_id}|{chunk_index}".encode()).hexdigest()
    return f"ref-{h[:24]}"


# ============================================================================
# Main
# ============================================================================


def load_one_standard(
    *,
    standard: dict[str, str],
    memory: AgentMemoryStore,
    dry_run: bool,
    actor: str,
) -> tuple[int, int]:
    """Load a single standard's corpus. Returns (files_processed, chunks_loaded)."""
    code = standard["code"]
    standard_id = deterministic_standard_id(code)
    corpus_dir = CORPORA_DIR / code
    if not corpus_dir.exists():
        _log.warning("standards.corpus_missing", code=code, path=str(corpus_dir))
        return (0, 0)

    md_files = sorted(corpus_dir.glob("*.md"))
    if not md_files:
        _log.warning("standards.no_md_files", code=code, path=str(corpus_dir))
        return (0, 0)

    if not dry_run:
        deleted = memory.delete_standard_chunks(standard_id=standard_id)
        if deleted:
            _log.info("standards.cleared_prior", code=code, deleted=deleted)

    # Collect all chunks across all files first, THEN embed in one batch.
    # Voyage's free tier rate-limits at 3 RPM — 88 sequential calls would
    # take ~30 minutes and likely hit throttle. Single batch = 1 API call.
    all_chunk_records: list[dict[str, object]] = []
    chunk_index_offset = 0
    for md_path in md_files:
        text = md_path.read_text(encoding="utf-8")
        chunks = chunk_markdown(text)
        for c in chunks:
            ref_id = deterministic_ref_id(standard_id, chunk_index_offset + c.chunk_index)
            if dry_run:
                print(
                    f"  [DRY] would embed {len(c.text)} chars  "
                    f"section={c.section_path}  ref_id={ref_id[:16]}"
                )
            else:
                all_chunk_records.append(
                    {
                        "ref_id": ref_id,
                        "standard_id": standard_id,
                        "standard_code": code,
                        "chunk_text": c.text,
                        "chunk_index": chunk_index_offset + c.chunk_index,
                        "section_path": c.section_path,
                        "resource_type": c.resource_type,
                    }
                )
        chunk_index_offset += len(chunks)
        _log.info(
            "standards.file_chunked",
            code=code,
            file=md_path.name,
            chunks=len(chunks),
        )
    total_chunks = chunk_index_offset

    # ONE embedder call for all chunks in this standard
    if not dry_run and all_chunk_records:
        upserted = memory.upsert_standard_chunks_bulk(chunks=all_chunk_records)
        _log.info("standards.bulk_upserted", code=code, count=upserted)

    if not dry_run:
        with warehouse_ctx(readonly=False) as wh:
            create_control_tables(wh)
            upsert_standard_registry(
                wh,
                standard_id=standard_id,
                code=code,
                display_name=standard["display_name"],
                version=standard["version"],
                is_industry=True,
                source_doc_uri=str(corpus_dir.relative_to(ROOT)),
                chunk_count=total_chunks,
                notes=standard["notes"],
                actor=actor,
            )
    return (len(md_files), total_chunks)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--only",
        type=str,
        default=None,
        help="Load only this standard code (fhir-r4, x12, ncpdp-d0, cms, dv2, hedis).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Don't write to DB or call the embedder. Just print what would be loaded.",
    )
    parser.add_argument(
        "--actor",
        default="phase14_loader",
        help="Audit actor recorded in standard_registry.registered_by.",
    )
    args = parser.parse_args()

    selected = INDUSTRY_STANDARDS
    if args.only:
        selected = [s for s in INDUSTRY_STANDARDS if s["code"] == args.only]
        if not selected:
            print(f"Unknown standard code: {args.only}")
            print(f"Valid codes: {[s['code'] for s in INDUSTRY_STANDARDS]}")
            return 2

    memory: AgentMemoryStore | None = None
    if not args.dry_run:
        memory = AgentMemoryStore(embedder=get_embedder())
        memory.ensure_schema()

    print()
    print("=" * 70)
    print("  Phase 14.2 — Industry standards corpus loader")
    print("=" * 70)
    if args.dry_run:
        print("  *** DRY RUN — no DB writes, no embedding API calls ***")
    print()

    total_files = 0
    total_chunks = 0
    for std in selected:
        print(f">>> Loading {std['display_name']} ({std['code']})")
        files, chunks = load_one_standard(
            standard=std,
            memory=memory,
            dry_run=args.dry_run,
            actor=args.actor,
        )
        print(f"    files={files}  chunks={chunks}")
        total_files += files
        total_chunks += chunks
        print()

    print("=" * 70)
    print(f"  Done. {len(selected)} standards / {total_files} files / {total_chunks} chunks")
    print("=" * 70)
    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
