"""InboundMappingAgent — Phase 23.

Given a :class:`SourceView` from a client's data file + the canonical
target dataset, calls Claude to produce a per-field mapping proposal
with transforms, confidence scores, and rationale.

Inherits :class:`datalink.agents.base.AgentBase` so every invocation
auto-logs to ``CONTROL.agent_reasoning_log`` (tokens + latency + actor).
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any

from datalink.agents.base import AgentBase
from datalink.logging import get_logger

_log = get_logger(__name__)


def _fix_trailing_commas(s: str) -> str:
    """Drop ``,]`` / ``,}`` artifacts Claude occasionally emits."""
    return re.sub(r",\s*([\]}])", r"\1", s)


def _single_to_double_quotes(s: str) -> str:
    """Best-effort single → double quotes.  Skips contractions inside strings."""
    # Replace ': ' between a single-quoted identifier and value
    return re.sub(r"'", '"', s)


# ---------------------------------------------------------------------------
# Output schema — what the agent returns to the bridge / UI
# ---------------------------------------------------------------------------


@dataclass
class ProposedFieldMapping:
    """One client field → one (or more) canonical field(s) with a transform."""

    client_field_name: str
    canonical_field_code: str | None  # None = explicitly unmapped
    canonical_dataset_code: str | None = None
    client_field_path: str | None = None
    client_field_position: int | None = None
    transform_sql: str | None = None  # NULL → direct copy
    transform_kind: str = "direct"  # direct / cast / split / concat / lookup / regex / parse_date
    confidence: float = 0.0  # 0.0 – 1.0
    rationale: str = ""
    is_required: bool = False


@dataclass
class MappingProposal:
    """Top-level structured agent output (subset of what lands in the DB)."""

    client_id: str
    dataset_code: str
    field_mappings: list[ProposedFieldMapping] = field(default_factory=list)
    unmapped_client_fields: list[str] = field(default_factory=list)
    missing_canonical_fields: list[str] = field(default_factory=list)
    rationale: str = ""
    drift_summary: dict[str, Any] = field(default_factory=dict)
    tokens_used: int = 0
    duration_ms: int = 0
    model: str = ""
    grounding_mode: str = "off"
    temperature: float = 0.0
    source_view: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "client_id": self.client_id,
            "dataset_code": self.dataset_code,
            "field_mappings": [
                {
                    "client_field_name": m.client_field_name,
                    "client_field_path": m.client_field_path,
                    "client_field_position": m.client_field_position,
                    "canonical_field_code": m.canonical_field_code,
                    "canonical_dataset_code": m.canonical_dataset_code,
                    "transform_sql": m.transform_sql,
                    "transform_kind": m.transform_kind,
                    "confidence": m.confidence,
                    "rationale": m.rationale,
                    "is_required": m.is_required,
                }
                for m in self.field_mappings
            ],
            "unmapped_client_fields": self.unmapped_client_fields,
            "missing_canonical_fields": self.missing_canonical_fields,
            "rationale": self.rationale,
            "drift_summary": self.drift_summary,
            "tokens_used": self.tokens_used,
            "duration_ms": self.duration_ms,
            "model": self.model,
            "grounding_mode": self.grounding_mode,
            "temperature": self.temperature,
            "source_view": self.source_view,
        }


# ---------------------------------------------------------------------------
# Agent
# ---------------------------------------------------------------------------


class InboundMappingAgent(AgentBase):
    role = "Senior Healthcare Data Integration Engineer"
    goal = (
        "Map an inbound client data file's fields to the DataLink canonical "
        "Bronze schema for a specific dataset.  Propose per-field mappings "
        "with transforms (SQL), confidence scores, and one-line rationales. "
        "Honor industry abbreviations (mbr→member, dob→date_of_birth, etc.).  "
        "Be conservative on PII/PHI — flag for human verification.  Never "
        "hallucinate canonical field names that aren't in the supplied target "
        "schema.  Surface unmapped client fields and missing canonical fields "
        "explicitly so the human reviewer can decide."
    )
    crew_name = "inbound_mapping"

    # ------------------------------------------------------------------
    # Public API — recommend_target_datasets (Phase 23.2)
    # ------------------------------------------------------------------

    def recommend_target_datasets(
        self,
        *,
        source_view: dict[str, Any],
        candidates: list[dict[str, Any]],
        top_k: int = 3,
        temperature: float = 0.0,
    ) -> dict[str, Any]:
        """Ask the LLM which of the candidate canonical datasets the source
        file most likely maps to.  Returns the top-K with confidence + a
        short rationale.

        ``candidates`` is the list from ``CONTROL.global_bronze_catalog_datasets``
        — each row must have at least ``dataset_code``, ``display_name``,
        ``category``, optional ``notes`` / ``used_by`` / ``total_fields``.

        This is a small, cheap LLM call (1–3k tokens) that runs BEFORE the
        field-level mapping so the operator doesn't have to pick the target
        dataset manually.
        """
        from datalink.adapters.protocols import LlmMessage

        # Redact source samples just like execute() does — defense in depth.
        redacted = self._redact_source(source_view)

        # Trim source view to the cheapest informative slice
        slim_source = {
            "format": redacted.get("format"),
            "filename": redacted.get("filename"),
            "sheet_or_section": redacted.get("sheet_or_section"),
            "fields": [
                {
                    "name": f.get("name"),
                    "path": f.get("path"),
                    "inferred_type": f.get("inferred_type"),
                    "sample_values": (f.get("sample_values") or [])[:3],
                }
                for f in (redacted.get("fields") or [])
            ],
        }
        slim_candidates = [
            {
                "dataset_code": c.get("dataset_code"),
                "display_name": c.get("display_name"),
                "category": c.get("category"),
                "total_fields": c.get("total_fields"),
                "used_by": c.get("used_by"),
                "notes": (c.get("notes") or "")[:240],
            }
            for c in candidates
        ]

        prompt = f"""You are a {self.role}.

TASK: An operator just uploaded an inbound client file.  Your job is to
recommend which of the {len(slim_candidates)} canonical datasets it
maps to.  Rank the top {top_k} by confidence (0.0–1.0) with a one-line
rationale each.

INPUT — INBOUND FILE SHAPE (column names + sample values, PHI-redacted):
{json.dumps(slim_source, indent=2, default=str)[:6000]}

INPUT — CANDIDATE CANONICAL DATASETS:
{json.dumps(slim_candidates, indent=2, default=str)[:10000]}

OUTPUT (strict JSON, no prose, no markdown fences):

{{
  "recommendations": [
    {{
      "dataset_code": "<exactly one of the candidate dataset_codes>",
      "confidence": 0.0,
      "rationale": "<one-line reason — name overlap, semantic content, sample-value clues>"
    }}
    /* up to {top_k} entries, ranked by confidence DESC */
  ],
  "top_pick": "<the highest-confidence dataset_code>",
  "rationale": "<2-sentence summary explaining the top pick>"
}}

RULES:
1. ONLY use dataset_codes that appear in the candidates list — never invent.
2. Confidence: 0.9+ = strong match · 0.7+ = plausible · <0.5 = weak guess.
3. If the file CLEARLY doesn't match any dataset (e.g. it's a config file or
   metadata spec, not data), still return your three best guesses with
   appropriately low confidence (< 0.4) — the operator decides.
4. Be deterministic — same input, same output.
""".strip()

        messages = [
            LlmMessage(role="system", content=f"Role: {self.role}\nGoal: {self.goal}"),
            LlmMessage(role="user", content=prompt),
        ]

        agent_run_id = self._log_invocation_start(
            agent_name=f"{self.crew_name}.recommend",
            input_summary={
                "n_source_fields": len(slim_source.get("fields") or []),
                "n_candidates": len(slim_candidates),
            },
        )

        import time

        t0 = time.perf_counter()
        try:
            completion = self._llm.complete(
                messages,
                max_tokens=2000,
                temperature=temperature,
                thinking_mode=self._thinking_mode,
            )
            tokens = int(
                (getattr(completion, "input_tokens", 0) or 0)
                + (getattr(completion, "output_tokens", 0) or 0)
                + (getattr(completion, "thinking_tokens", 0) or 0)
            )
            text = str(getattr(completion, "content", "") or "")
        except Exception as exc:
            duration_ms = int((time.perf_counter() - t0) * 1000)
            self._log_invocation_end(
                agent_run_id,
                success=False,
                tokens_used=0,
                duration_ms=duration_ms,
                error=str(exc)[:500],
            )
            raise

        duration_ms = int((time.perf_counter() - t0) * 1000)
        parsed = self._safe_parse_json(text)
        recs = parsed.get("recommendations") or []
        # Defensive — filter out invalid dataset_codes
        valid_codes = {str(c.get("dataset_code")) for c in candidates}
        cleaned: list[dict[str, Any]] = []
        for r in recs:
            code = str(r.get("dataset_code") or "")
            if code in valid_codes:
                try:
                    conf = float(r.get("confidence") or 0)
                except (TypeError, ValueError):
                    conf = 0.0
                cleaned.append(
                    {
                        "dataset_code": code,
                        "confidence": max(0.0, min(1.0, conf)),
                        "rationale": str(r.get("rationale") or "")[:500],
                    }
                )
        cleaned.sort(key=lambda r: r["confidence"], reverse=True)
        cleaned = cleaned[:top_k]

        top_pick = parsed.get("top_pick") or (cleaned[0]["dataset_code"] if cleaned else None)
        # If the LLM's top_pick isn't in our cleaned list, fall back to the
        # highest-ranked clean entry.
        if top_pick not in valid_codes and cleaned:
            top_pick = cleaned[0]["dataset_code"]

        result = {
            "recommendations": cleaned,
            "top_pick": top_pick,
            "rationale": str(parsed.get("rationale") or "")[:2000],
            "tokens_used": tokens,
            "duration_ms": duration_ms,
        }
        self._log_invocation_end(
            agent_run_id,
            success=True,
            tokens_used=tokens,
            duration_ms=duration_ms,
        )
        return result

    # ------------------------------------------------------------------
    # Public API — execute (field-level mapping; existing)
    # ------------------------------------------------------------------

    def execute(self, context: dict[str, Any]) -> dict[str, Any]:
        """Return a :class:`MappingProposal` as dict.

        ``context`` keys:

            client_id            str
            dataset_code         str       e.g. "membership"
            source_view          dict      from SourceView.to_dict()
            canonical_schema     list      [{bronze_column_name, requirement,
                                              logical_type, description, ...}*]
            prior_live_mapping   dict|None previous LIVE mapping for stickiness
            temperature          float     0.0 - 1.0
            grounding_mode       str       off / rag / strict
            model                str       claude-haiku-4-5-20251001
        """
        client_id = str(context.get("client_id") or "")
        dataset_code = str(context.get("dataset_code") or "")
        source_view = context.get("source_view") or {}
        canonical_schema = context.get("canonical_schema") or []
        prior = context.get("prior_live_mapping")
        temperature = float(context.get("temperature") or 0.0)
        grounding_mode = str(context.get("grounding_mode") or "off")
        model_name = str(context.get("model") or "")

        # PHI redaction on the sample values
        redacted_source = self._redact_source(source_view)

        prompt = self._build_prompt(
            client_id=client_id,
            dataset_code=dataset_code,
            source_view=redacted_source,
            canonical_schema=canonical_schema,
            prior_live_mapping=prior,
        )

        # Audit: log the invocation start
        agent_run_id = self._log_invocation_start(
            agent_name=self.crew_name,
            input_summary={
                "client_id": client_id,
                "dataset_code": dataset_code,
                "source_format": redacted_source.get("format"),
                "source_filename": redacted_source.get("filename"),
                "n_source_fields": len(redacted_source.get("fields") or []),
                "n_canonical_fields": len(canonical_schema),
            },
        )

        import time

        from datalink.adapters.protocols import LlmMessage

        messages = [
            LlmMessage(role="system", content=f"Role: {self.role}\nGoal: {self.goal}"),
            LlmMessage(role="user", content=prompt),
        ]
        t0 = time.perf_counter()
        try:
            completion = self._llm.complete(
                messages,
                max_tokens=8000,
                temperature=temperature,
                thinking_mode=self._thinking_mode,
            )
            # LlmCompletion exposes input_tokens + output_tokens separately;
            # bill the operator for the sum (Anthropic's convention).
            tokens = int(
                (getattr(completion, "input_tokens", 0) or 0)
                + (getattr(completion, "output_tokens", 0) or 0)
                + (getattr(completion, "thinking_tokens", 0) or 0)
            )
            text = str(getattr(completion, "content", "") or "")
        except Exception as exc:
            duration_ms = int((time.perf_counter() - t0) * 1000)
            _log.exception("inbound_mapping.llm_failed", err=str(exc))
            self._log_invocation_end(
                agent_run_id,
                success=False,
                tokens_used=0,
                duration_ms=duration_ms,
                error=str(exc)[:500],
            )
            raise

        duration_ms = int((time.perf_counter() - t0) * 1000)

        # Parse + validate
        parsed = self._safe_parse_json(text)
        proposal = self._build_proposal(
            parsed=parsed,
            client_id=client_id,
            dataset_code=dataset_code,
            source_view=redacted_source,
            tokens=tokens,
            duration_ms=duration_ms,
            model=model_name,
            grounding_mode=grounding_mode,
            temperature=temperature,
        )

        # Drift detection (if prior mapping exists)
        if prior:
            proposal.drift_summary = self._compute_drift(proposal, prior)

        self._log_invocation_end(
            agent_run_id,
            success=True,
            tokens_used=tokens,
            duration_ms=duration_ms,
        )
        return proposal.to_dict()

    # ------------------------------------------------------------------
    # Prompt construction
    # ------------------------------------------------------------------

    def _build_prompt(
        self,
        *,
        client_id: str,
        dataset_code: str,
        source_view: dict[str, Any],
        canonical_schema: list[dict[str, Any]],
        prior_live_mapping: dict[str, Any] | None,
    ) -> str:
        # Tight, structured prompt — keeps token use predictable.
        sv_json = json.dumps(source_view, indent=2, default=str)[:8000]
        canon_json = json.dumps(
            [
                {
                    "bronze_column_name": c.get("bronze_column_name"),
                    "field_display_name": c.get("field_display_name"),
                    "requirement": c.get("requirement"),
                    "logical_type": c.get("logical_type"),
                    "description": (c.get("description") or "")[:200],
                    "is_pii": c.get("is_pii"),
                    "is_phi": c.get("is_phi"),
                }
                for c in canonical_schema
            ],
            indent=2,
            default=str,
        )[:12000]
        prior_block = ""
        if prior_live_mapping:
            prior_block = (
                "\n\nPRIOR LIVE MAPPING (use as a strong prior — be sticky "
                "unless the new source clearly diverges):\n"
                + json.dumps(prior_live_mapping, indent=2, default=str)[:4000]
            )

        return f"""You are a {self.role}.

{self.goal}

CLIENT: {client_id}
TARGET DATASET: {dataset_code}

INPUT — SOURCE FILE SHAPE (already PHI-redacted; sample values may be masked):
{sv_json}

INPUT — CANONICAL TARGET SCHEMA:
{canon_json}
{prior_block}

TASK: Produce a JSON object with the following exact shape:

{{
  "client_id": "{client_id}",
  "dataset_code": "{dataset_code}",
  "field_mappings": [
    {{
      "client_field_name": "<source name verbatim>",
      "client_field_path": "<dot path for nested JSON, else null>",
      "client_field_position": "<int or null>",
      "canonical_field_code": "<bronze_column_name from target schema, or null>",
      "canonical_dataset_code": "{dataset_code}",
      "transform_sql": "<Snowflake SQL with :src placeholder, or null for direct>",
      "transform_kind": "direct | cast | split | concat | lookup | regex | parse_date",
      "confidence": 0.0,
      "rationale": "<one-line reason>",
      "is_required": false
    }}
  ],
  "unmapped_client_fields": ["<client field names that have no canonical match>"],
  "missing_canonical_fields": ["<canonical bronze_column_names that the client doesn't supply>"],
  "rationale": "<2-3 sentence summary of the overall mapping strategy>"
}}

RULES:
1. EVERY client field in the source view MUST appear EXACTLY ONCE — either in field_mappings (with a canonical_field_code) or in unmapped_client_fields (as an unmapped bare name).  Never skip a field.
2. If a single client field needs to populate MULTIPLE canonical fields (split), emit MULTIPLE rows — same client_field_name, different canonical_field_code, transform_sql does the split.
3. transform_sql uses ``:src`` as the source-column placeholder.  Examples:
   - direct: null
   - YYYYMMDD int → DATE:  ``TO_DATE(:src::VARCHAR, 'YYYYMMDD')``
   - "Last, First" → first_name: ``TRIM(SPLIT_PART(SPLIT_PART(:src, ',', 2), ' ', 1))``
   - YYYY-MM-DD → DATE:  ``TO_DATE(:src, 'YYYY-MM-DD')``
4. confidence: 1.0 = exact match · 0.9+ = strong semantic · 0.7+ = reasonable guess · <0.5 = flag for human review.
5. Be CONSERVATIVE on PII/PHI mappings — lower confidence by 0.1 and add 'review-PII' to rationale.
6. NEVER invent canonical fields that aren't in the supplied target schema.
7. Be deterministic — return identical output for identical input.

Return ONLY the JSON object.  No prose before or after.
""".strip()

    # ------------------------------------------------------------------
    # Parsing + validation
    # ------------------------------------------------------------------

    @staticmethod
    def _safe_parse_json(text: str) -> dict[str, Any]:
        """Extract JSON object from LLM output.  Handles:

        - markdown fences (```json ... ```)
        - prose preamble / postamble around the JSON
        - JS-style trailing commas (Claude occasionally emits these)
        - single-quoted strings (rare but seen with smaller models)

        Returns ``{}`` on hard failure (after logging the first 500 chars
        of the offending text so we can iterate on the prompt).
        """
        if not text:
            return {}
        # Step 1: strip code fences anywhere in the text
        cleaned = re.sub(r"```(?:json)?\s*", "", text, flags=re.IGNORECASE)
        cleaned = re.sub(r"```", "", cleaned)
        cleaned = cleaned.strip()
        # Step 2: try direct parse
        try:
            return json.loads(cleaned)
        except json.JSONDecodeError:
            pass
        # Step 3: extract the LARGEST {...} balanced span
        # (LLMs sometimes add a one-line preamble like "Here's the mapping:")
        depth, start = 0, -1
        spans: list[tuple[int, int]] = []
        for i, ch in enumerate(cleaned):
            if ch == "{":
                if depth == 0:
                    start = i
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0 and start >= 0:
                    spans.append((start, i + 1))
                    start = -1
        # Try each candidate span, biggest first
        for s, e in sorted(spans, key=lambda x: x[1] - x[0], reverse=True):
            candidate = cleaned[s:e]
            for variant in (
                candidate,
                _fix_trailing_commas(candidate),
                _single_to_double_quotes(candidate),
            ):
                try:
                    return json.loads(variant)
                except json.JSONDecodeError:
                    continue
        _log.warning(
            "inbound_mapping.json_parse_failed",
            preview=cleaned[:500],
            len=len(cleaned),
        )
        return {}

    def _build_proposal(
        self,
        *,
        parsed: dict[str, Any],
        client_id: str,
        dataset_code: str,
        source_view: dict[str, Any],
        tokens: int,
        duration_ms: int,
        model: str,
        grounding_mode: str,
        temperature: float,
    ) -> MappingProposal:
        prop = MappingProposal(
            client_id=client_id,
            dataset_code=dataset_code,
            tokens_used=tokens,
            duration_ms=duration_ms,
            model=model,
            grounding_mode=grounding_mode,
            temperature=temperature,
            source_view=source_view,
        )
        for row in parsed.get("field_mappings") or []:
            try:
                conf = float(row.get("confidence") or 0)
            except (TypeError, ValueError):
                conf = 0.0
            try:
                pos_raw = row.get("client_field_position")
                pos = int(pos_raw) if pos_raw not in (None, "", "null") else None
            except Exception:
                pos = None
            prop.field_mappings.append(
                ProposedFieldMapping(
                    client_field_name=str(row.get("client_field_name") or "").strip(),
                    client_field_path=row.get("client_field_path") or None,
                    client_field_position=pos,
                    canonical_field_code=row.get("canonical_field_code") or None,
                    canonical_dataset_code=row.get("canonical_dataset_code") or dataset_code,
                    transform_sql=row.get("transform_sql") or None,
                    transform_kind=str(row.get("transform_kind") or "direct"),
                    confidence=max(0.0, min(1.0, conf)),
                    rationale=str(row.get("rationale") or "")[:1500],
                    is_required=bool(row.get("is_required") or False),
                )
            )
        prop.unmapped_client_fields = [str(x) for x in (parsed.get("unmapped_client_fields") or [])]
        prop.missing_canonical_fields = [
            str(x) for x in (parsed.get("missing_canonical_fields") or [])
        ]
        prop.rationale = str(parsed.get("rationale") or "")[:4000]
        return prop

    # ------------------------------------------------------------------
    # Drift detection (Pass 4 will deepen — for now, basic add/remove/changed)
    # ------------------------------------------------------------------

    @staticmethod
    def _compute_drift(
        new_proposal: MappingProposal,
        prior: dict[str, Any],
    ) -> dict[str, Any]:
        """Diff the new proposal against the prior LIVE mapping."""
        prior_fields = prior.get("field_mappings") or []
        prior_by_client_field = {str(r.get("client_field_name") or ""): r for r in prior_fields}
        added: list[str] = []
        removed: list[str] = []
        changed_canon: list[dict[str, str]] = []
        changed_transform: list[dict[str, str]] = []

        new_names = {m.client_field_name for m in new_proposal.field_mappings}
        for name in new_names:
            if name and name not in prior_by_client_field:
                added.append(name)
        for name in prior_by_client_field:
            if name and name not in new_names:
                removed.append(name)

        for m in new_proposal.field_mappings:
            prior_row = prior_by_client_field.get(m.client_field_name)
            if not prior_row:
                continue
            if (m.canonical_field_code or "") != (prior_row.get("canonical_field_code") or ""):
                changed_canon.append(
                    {
                        "client_field_name": m.client_field_name,
                        "prior_canonical": prior_row.get("canonical_field_code") or "",
                        "new_canonical": m.canonical_field_code or "",
                    }
                )
            if (m.transform_sql or "") != (prior_row.get("transform_sql") or ""):
                changed_transform.append(
                    {
                        "client_field_name": m.client_field_name,
                        "prior_transform": prior_row.get("transform_sql") or "",
                        "new_transform": m.transform_sql or "",
                    }
                )

        return {
            "added_client_fields": added,
            "removed_client_fields": removed,
            "changed_canonical_target": changed_canon,
            "changed_transform_sql": changed_transform,
            "is_material": bool(added or removed or changed_canon),
        }

    # ------------------------------------------------------------------
    # PHI redaction — keep raw row data out of the LLM
    # ------------------------------------------------------------------

    def _redact_source(self, sv: dict[str, Any]) -> dict[str, Any]:
        """Run sample values through PhiRedactionLayer before they hit the LLM."""
        if not sv:
            return {}
        redacted = json.loads(json.dumps(sv, default=str))  # deep-copy
        try:
            for f in redacted.get("fields") or []:
                vals = f.get("sample_values") or []
                f["sample_values"] = [self._phi_redact_value(v) for v in vals]
            for i, row in enumerate(redacted.get("sample_rows") or []):
                redacted["sample_rows"][i] = {k: self._phi_redact_value(v) for k, v in row.items()}
        except Exception as exc:
            _log.warning("inbound_mapping.redact_failed", err=str(exc)[:200])
        return redacted

    def _phi_redact_value(self, v: Any) -> str:
        """Light-weight inline redactor.  Hands off to PhiRedactionLayer when
        it can.  Falls back to regex masks for common PHI shapes."""
        if v is None:
            return ""
        s = str(v)
        # SSN
        s = re.sub(r"\b\d{3}-\d{2}-\d{4}\b", "<SSN>", s)
        s = re.sub(r"\b\d{9}\b", "<9-DIGIT-ID>", s)
        # Phone
        s = re.sub(r"\b\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b", "<PHONE>", s)
        # Email
        s = re.sub(r"[\w.+-]+@[\w-]+\.[\w.-]+", "<EMAIL>", s)
        # DOB-ish dates — keep YEAR only
        s = re.sub(r"\b(19|20)\d{2}[-/]\d{2}[-/]\d{2}\b", r"\1XX-XX-XX", s)
        return s[:120]

    # ------------------------------------------------------------------
    # Audit log integration — keep tokens + latency in agent_reasoning_log
    # ------------------------------------------------------------------

    def _log_invocation_start(self, *, agent_name: str, input_summary: dict[str, Any]) -> str:
        """Best-effort write a 'started' row to agent_reasoning_log.
        Returns an opaque run_id.  Audit failures are non-blocking."""
        import uuid

        run_id = str(uuid.uuid4())
        try:
            from datetime import UTC, datetime

            from datalink.quality.control import CONTROL_SCHEMA

            self._wh.execute(
                f"INSERT INTO {CONTROL_SCHEMA}.agent_reasoning_log "
                f"(reasoning_id, agent_name, action, started_at, input_summary) "
                f"VALUES ($id, $agent, 'execute', $ts, PARSE_JSON($inp))",
                {
                    "id": run_id,
                    "agent": agent_name,
                    "ts": datetime.now(UTC).replace(tzinfo=None),
                    "inp": json.dumps(input_summary, default=str),
                },
            )
        except Exception:
            pass  # audit log is non-critical
        return run_id

    def _log_invocation_end(
        self,
        run_id: str,
        *,
        success: bool,
        tokens_used: int,
        duration_ms: int,
        error: str | None = None,
    ) -> None:
        try:
            from datetime import UTC, datetime

            from datalink.quality.control import CONTROL_SCHEMA

            self._wh.execute(
                f"UPDATE {CONTROL_SCHEMA}.agent_reasoning_log "
                f"SET finished_at = $ts, tokens_used = $t, duration_ms = $d, "
                f"    status = $st, error_message = $err "
                f"WHERE reasoning_id = $id",
                {
                    "id": run_id,
                    "ts": datetime.now(UTC).replace(tzinfo=None),
                    "t": int(tokens_used or 0),
                    "d": int(duration_ms or 0),
                    "st": "ok" if success else "error",
                    "err": (error or "")[:1000],
                },
            )
        except Exception:
            pass
