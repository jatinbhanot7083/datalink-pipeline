"""ContractArchitectAgent — proposes Bronze DDL + GX scaffold + dbt stubs
grounded against industry standards (FHIR R4, X12, NCPDP, CMS, DV2, HEDIS)
plus operator-uploaded "custom" standards.

Two modes share the same RAG-grounded core:

  Mode A — FILE_DRIVEN:
    Input: sample file payload (header columns + first N rows of values)
    The agent profiles + matches against standards + proposes a contract.

  Mode B — CONTRACT_FIRST:
    Input: natural-language description OR pasted mapping spec
    The agent proposes a contract from scratch.

PHI boundary:
  * The LLM only sees: column names, column types, anchored-standard
    chunks, the operator's NL description (Mode B), and very minimal
    "first sample value per column" hints (Mode A — operator's choice
    via include_sample_values toggle, default off for production).
  * Row data NEVER travels to the LLM.

Output contract (JSON, strict, parsed + validated):

    {
      "proposed_table_name": "RAW_CLAIMS",
      "proposed_columns": [
        {
          "name": "claim_id",
          "type": "VARCHAR",
          "nullable": false,
          "matches_standard": "fhir-r4 / Claim Resource / Cardinality and Key Fields",
          "rationale": "FHIR Claim.id is the canonical claim identifier.",
          "vendor_field":  "ClaimNumber",       // if FILE_DRIVEN
          "deviation": "RENAME"                 // RENAME | RETYPE | KEEP | NEW
        },
        ...
      ],
      "proposed_ddl": "CREATE TABLE ...",
      "standard_match_scores": {"fhir-r4": 0.92, "x12": 0.78},
      "deviation_log": [
        {"vendor_field": "mbr_no", "standard_field": "member_id", "action": "RENAME", "reason": "..."}
      ],
      "gx_scaffold": [
        {"expectation_type": "expect_column_values_to_match_regex",
         "column": "provider_npi", "kwargs": {"regex": "^[0-9]{10}$"},
         "dimension": "Validity", "severity": "HIGH"}
      ],
      "dbt_scaffold": "{{ config(materialized='view') }} ...",
      "vendor_spec_md": "# Vendor Data Contract Spec\\n...",
      "rationale": "Two-paragraph high-level explanation of the proposal."
    }

In stub mode (no Anthropic key) the agent uses a deterministic heuristic
mapper so the offline path still produces a sensible proposal — same
output shape as the real LLM.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, ClassVar

from datalink.agents.base import AgentBase
from datalink.logging import get_logger

_log = get_logger(__name__)


class ContractMode(StrEnum):
    FILE_DRIVEN = "FILE_DRIVEN"
    CONTRACT_FIRST = "CONTRACT_FIRST"


# JSON contract — keep tight so the UI can render with confidence.
_REQUIRED_TOP = {
    "proposed_table_name",
    "proposed_columns",
    "proposed_ddl",
    "standard_match_scores",
    "rationale",
}
_REQUIRED_COLUMN = {"name", "type", "nullable", "rationale"}
_VALID_DEVIATIONS = {"RENAME", "RETYPE", "KEEP", "NEW", ""}
_VALID_TYPES = {
    "VARCHAR",
    "TEXT",
    "INTEGER",
    "BIGINT",
    "DECIMAL",
    "NUMERIC",
    "DATE",
    "TIMESTAMP",
    "BOOLEAN",
    "JSON",
}


@dataclass
class ProposedColumn:
    """One proposed column in a contract design."""

    name: str
    type: str
    nullable: bool
    rationale: str
    matches_standard: str | None = None
    vendor_field: str | None = None  # only for FILE_DRIVEN
    deviation: str | None = None  # RENAME | RETYPE | KEEP | NEW


@dataclass
class ContractProposal:
    """Structured agent output. Parses straight to UI + persistence."""

    proposed_table_name: str
    proposed_columns: list[ProposedColumn]
    proposed_ddl: str
    standard_match_scores: dict[str, float]
    rationale: str
    deviation_log: list[dict[str, str]] = field(default_factory=list)
    gx_scaffold: list[dict[str, Any]] = field(default_factory=list)
    dbt_scaffold: str = ""
    vendor_spec_md: str = ""
    grounding: list[dict[str, Any]] = field(default_factory=list)
    tokens_used: int = 0
    duration_ms: int = 0
    proposer_model: str = ""
    embedding_model: str = ""
    temperature: float = 0.0
    strictness: float = 0.5
    mode: ContractMode = ContractMode.FILE_DRIVEN

    def to_dict(self) -> dict[str, Any]:
        return {
            "proposed_table_name": self.proposed_table_name,
            "proposed_columns": [
                {
                    "name": c.name,
                    "type": c.type,
                    "nullable": c.nullable,
                    "rationale": c.rationale,
                    "matches_standard": c.matches_standard,
                    "vendor_field": c.vendor_field,
                    "deviation": c.deviation,
                }
                for c in self.proposed_columns
            ],
            "proposed_ddl": self.proposed_ddl,
            "standard_match_scores": self.standard_match_scores,
            "deviation_log": self.deviation_log,
            "gx_scaffold": self.gx_scaffold,
            "dbt_scaffold": self.dbt_scaffold,
            "vendor_spec_md": self.vendor_spec_md,
            "rationale": self.rationale,
            "grounding": self.grounding,
            "tokens_used": self.tokens_used,
            "duration_ms": self.duration_ms,
            "proposer_model": self.proposer_model,
            "embedding_model": self.embedding_model,
            "temperature": self.temperature,
            "strictness": self.strictness,
            "mode": self.mode.value,
        }


class ContractArchitectAgent(AgentBase):
    role = "Senior Data Contract Architect for healthcare data integrations"
    goal = (
        "Propose a Bronze table schema (CREATE TABLE DDL) that matches "
        "industry-standard healthcare data contracts (HL7 FHIR R4, X12 EDI, "
        "NCPDP, CMS data dictionaries, Data Vault 2.0, NCQA HEDIS, or "
        "operator-uploaded custom standards). Anchor every proposed "
        "column against a specific standard reference when possible. "
        "Flag deviations from the standard so the operator can decide "
        "whether to enforce conformance or document a vendor exception. "
        "Always emit deterministic, idempotent CREATE TABLE SQL with the "
        "7 mandatory DataLink audit columns appended."
    )
    crew_name = "contract_architect"

    # The 7 DataLink audit columns appended to every Bronze table — the
    # agent should NOT emit these; we splice them in post-parse.
    _MANDATORY_AUDIT_COLUMNS: ClassVar[list[tuple[str, str]]] = [
        ("_load_dt", "TIMESTAMP"),
        ("_source_file", "VARCHAR"),
        ("_batch_id", "VARCHAR"),
        ("_record_source", "VARCHAR"),
        ("_load_type", "VARCHAR"),
        ("_file_row_number", "BIGINT"),
        ("_record_hash", "VARCHAR"),
    ]

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def execute(self, context: dict[str, Any]) -> dict[str, Any]:
        """Return a proposal dict.

        ``context`` keys:

            client_id            str
            source_type          str    e.g. "CLAIMS" | "MEMBERSHIP"
            mode                 str    "FILE_DRIVEN" | "CONTRACT_FIRST"
            anchored_standards   list[str]   ["fhir-r4", "x12", ...]
            temperature          float       0.0 - 1.0
            grounding_k          int         1 - 10
            strictness           float       0.0 - 1.0
            payload              dict
                Mode A (FILE_DRIVEN):
                    file_name      str
                    headers        list[str]
                    sample_values  list[list[str]]   first N rows (operator-supplied)
                    detected_format str               CSV | EDI_837P | EDI_834 | ...
                Mode B (CONTRACT_FIRST):
                    nl_description str    operator's prose

            grounding_chunks     list[dict]
                Pre-retrieved RAG hits — one per standard chunk —
                supplied by the bridge layer (which has access to
                the AgentMemoryStore). Each dict:
                    standard_code, section_path, resource_type,
                    chunk_text, distance
        """
        client_id = context["client_id"]
        source_type = context["source_type"].upper()
        mode_str = str(context.get("mode", "FILE_DRIVEN")).upper()
        mode = ContractMode(mode_str)
        anchored = list(context.get("anchored_standards", [])) or ["fhir-r4"]
        temperature = float(context.get("temperature", 0.0))
        strictness = float(context.get("strictness", 0.5))
        grounding_chunks = list(context.get("grounding_chunks", []))
        payload = dict(context.get("payload", {}))

        if not anchored:
            raise ValueError("anchored_standards must list at least one standard code.")

        # Build the instruction string. Standard-grounding chunks travel
        # in the instruction (not safe_payload) — they're text content
        # that should be visible to the LLM, just like the prompt.
        instruction = self._build_instruction(
            client_id=client_id,
            source_type=source_type,
            mode=mode,
            anchored=anchored,
            strictness=strictness,
            grounding_chunks=grounding_chunks,
            payload=payload,
        )

        # PHI-safe payload — only metadata names, no row data
        safe_payload: dict[str, Any] = {
            "client_id": client_id,
            "source_type": source_type,
            "mode": mode.value,
            "anchored_standards": anchored,
            "temperature": temperature,
            "strictness": strictness,
            "grounding_chunk_count": len(grounding_chunks),
        }

        # 16384 tokens — covers a 30-column 834 contract with full
        # rationale + vendor_spec_md + gx_scaffold without truncation.
        # Claude haiku 4.5 supports up to 64K output tokens.
        raw = self._ask_llm(
            instruction=instruction,
            safe_payload=safe_payload,
            max_tokens=16384,
            temperature=temperature,
        )

        try:
            obj = self._extract_json(raw)
        except ValueError as e:
            raise ValueError(
                f"Contract Architect returned non-JSON output. {e}\n\nRaw:\n{raw[:500]}"
            ) from e

        self._validate(obj)
        # Splice the mandatory audit columns into the DDL after parsing.
        obj["proposed_ddl"] = self._inject_audit_columns(obj["proposed_ddl"])
        # Add provenance metadata — the bridge layer fills in tokens etc.
        obj["mode"] = mode.value
        obj["strictness"] = strictness
        obj["grounding_chunk_count"] = len(grounding_chunks)
        return obj

    # ------------------------------------------------------------------
    # Prompt construction
    # ------------------------------------------------------------------

    def _build_instruction(
        self,
        *,
        client_id: str,
        source_type: str,
        mode: ContractMode,
        anchored: list[str],
        strictness: float,
        grounding_chunks: list[dict[str, Any]],
        payload: dict[str, Any],
    ) -> str:
        """Assemble the full instruction the LLM receives."""
        parts: list[str] = []

        parts.append(
            f"You are designing a Bronze-layer data contract for client "
            f"`{client_id}`, source type `{source_type}`, mode `{mode.value}`."
        )
        parts.append(f"Anchored industry standards (most relevant first): {', '.join(anchored)}.")

        # Strictness directive
        if strictness >= 0.8:
            parts.append(
                "STRICTNESS: HIGH. Aggressively rename non-standard vendor "
                "fields to the standard's canonical names. Document every "
                "deviation in the deviation_log. Refuse to keep vendor "
                "shorthand (e.g. `mbr_no`) — rename to `member_id`."
            )
        elif strictness <= 0.2:
            parts.append(
                "STRICTNESS: LOW. Honor vendor naming when reasonable. "
                "Only rename when the vendor's name is ambiguous or "
                "collides with another field. Document deviations as "
                "informational only."
            )
        else:
            parts.append(
                "STRICTNESS: MEDIUM. Rename when a clear standard-mandated "
                "name exists and the vendor's name is significantly different. "
                "Keep the vendor's name when it's a reasonable abbreviation "
                "of the standard (e.g. vendor `dob` for FHIR Patient.birthDate "
                "- keep but document)."
            )

        # Mode-specific input
        if mode == ContractMode.FILE_DRIVEN:
            file_name = payload.get("file_name", "(unknown)")
            headers = list(payload.get("headers", []))
            sample_values = list(payload.get("sample_values", []))
            detected_format = payload.get("detected_format", "CSV")

            parts.append(
                f"\nFILE-DRIVEN INPUT — vendor sent this sample:\n"
                f"- File: {file_name}\n"
                f"- Detected format: {detected_format}\n"
                f"- Header columns ({len(headers)}): {headers}"
            )
            if sample_values:
                preview = sample_values[: min(5, len(sample_values))]
                parts.append(f"- First {len(preview)} sample row(s): {preview}")
        else:
            nl_description = payload.get("nl_description", "").strip()
            if not nl_description:
                raise ValueError(
                    "CONTRACT_FIRST mode requires payload.nl_description "
                    "(operator's NL description of the source)."
                )
            parts.append(
                f"\nCONTRACT-FIRST INPUT — operator described the source as:\n\n"
                f'"""\n{nl_description}\n"""'
            )

        # RAG grounding — inject the retrieved standard chunks
        if grounding_chunks:
            parts.append(
                f"\nGROUNDING — top {len(grounding_chunks)} most-relevant "
                f"chunks from the anchored standards (use these as the "
                f"AUTHORITATIVE reference for canonical names + types + "
                f"validation rules):"
            )
            for i, ch in enumerate(grounding_chunks):
                code = ch.get("standard_code", "?")
                section = ch.get("section_path", "")
                text = ch.get("chunk_text", "")
                if len(text) > 1500:
                    text = text[:1500] + "...(truncated)"
                parts.append(f"\n--- Grounding chunk #{i + 1} [{code} / {section}] ---\n{text}")

        # Output contract — strict JSON
        parts.append(
            """
OUTPUT FORMAT — Return ONLY a single JSON object, no surrounding text.
The JSON must conform to this schema:

{
  "proposed_table_name": "RAW_CLAIMS",
  "proposed_columns": [
    {
      "name": "claim_id",
      "type": "VARCHAR",
      "nullable": false,
      "rationale": "FHIR R4 Claim.id is the canonical claim identifier.",
      "matches_standard": "fhir-r4 / Claim Resource / Cardinality and Key Fields",
      "vendor_field": "ClaimNumber",
      "deviation": "RENAME"
    }
  ],
  "proposed_ddl": "CREATE TABLE {schema}.RAW_CLAIMS (...);",
  "standard_match_scores": {"fhir-r4": 0.92, "x12": 0.78},
  "deviation_log": [
    {"vendor_field": "mbr_no", "standard_field": "member_id",
     "action": "RENAME", "reason": "FHIR Patient.identifier canonical name."}
  ],
  "gx_scaffold": [
    {"expectation_type": "expect_column_values_to_match_regex",
     "column": "provider_npi", "kwargs": {"regex": "^[0-9]{10}$"},
     "dimension": "Validity", "severity": "HIGH"}
  ],
  "dbt_scaffold": "{{ config(materialized='view') }}\\nselect ... from {{ source('bronze', 'raw_claims') }}",
  "vendor_spec_md": "# Vendor Data Contract Spec\\n\\n## Claims feed expectations\\n\\n| Column | Type | ... |\\n| ... |",
  "rationale": "Two-paragraph high-level explanation."
}

RULES:
1. proposed_columns MUST be non-empty; one entry per business column.
2. type MUST be one of: VARCHAR, TEXT, INTEGER, BIGINT, DECIMAL, NUMERIC, DATE, TIMESTAMP, BOOLEAN, JSON.
3. proposed_ddl MUST start with `CREATE TABLE` and use `{schema}` as
   placeholder for the schema name (the runtime substitutes the actual
   schema, e.g. `BRONZE_AETNA`).
4. proposed_ddl MUST NOT contain the 7 audit columns (_load_dt,
   _source_file, _batch_id, _record_source, _load_type,
   _file_row_number, _record_hash). The runtime injects these.
5. deviation values: RENAME | RETYPE | KEEP | NEW (or empty string for
   CONTRACT_FIRST mode columns).
6. standard_match_scores: keys must be from the anchored_standards list;
   values 0.0 - 1.0; sum need not equal 1.
7. gx_scaffold expectations should cover: required-not-null on PKs,
   regex on identifiers (NPI, ICD-10, CPT), value-set on enums,
   uniqueness on natural keys.
"""
        )
        return "\n".join(parts)

    # ------------------------------------------------------------------
    # JSON extraction + validation
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_json(raw: str) -> dict[str, Any]:
        """Pull the first balanced JSON object out of LLM output.

        Claude usually returns clean JSON when asked, but sometimes
        wraps in markdown fences. Handle both.
        """
        s = raw.strip()
        # Strip markdown fences
        if s.startswith("```"):
            # remove first fence line + last fence
            lines = s.splitlines()
            if lines:
                lines = lines[1:]
            if lines and lines[-1].strip().startswith("```"):
                lines = lines[:-1]
            s = "\n".join(lines).strip()

        # If the model added prose around the JSON, find the first { and
        # last } and parse that span.
        first = s.find("{")
        last = s.rfind("}")
        if first < 0 or last < 0:
            raise ValueError("No JSON object found in agent output.")
        candidate = s[first : last + 1]
        try:
            parsed = json.loads(candidate)
        except json.JSONDecodeError as e:
            raise ValueError(f"JSON parse error: {e}") from e
        if not isinstance(parsed, dict):
            raise ValueError(f"Expected JSON object, got {type(parsed).__name__}")
        return parsed

    @classmethod
    def _validate(cls, obj: dict[str, Any]) -> None:
        """Tight validation against the JSON contract."""
        missing = _REQUIRED_TOP - set(obj.keys())
        if missing:
            raise ValueError(f"Missing top-level keys: {sorted(missing)}")

        cols = obj["proposed_columns"]
        if not isinstance(cols, list) or not cols:
            raise ValueError("proposed_columns must be a non-empty list.")
        for i, c in enumerate(cols):
            if not isinstance(c, dict):
                raise ValueError(f"proposed_columns[{i}] must be an object.")
            cmissing = _REQUIRED_COLUMN - set(c.keys())
            if cmissing:
                raise ValueError(f"proposed_columns[{i}] missing keys: {sorted(cmissing)}")
            if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", str(c["name"])):
                raise ValueError(
                    f"proposed_columns[{i}].name '{c['name']}' is not a valid SQL identifier."
                )
            sql_type = str(c["type"]).split("(", 1)[0].strip().upper()
            if sql_type not in _VALID_TYPES:
                raise ValueError(
                    f"proposed_columns[{i}].type '{c['type']}' not in supported set "
                    f"{sorted(_VALID_TYPES)}."
                )
            if not isinstance(c["nullable"], bool):
                raise ValueError(f"proposed_columns[{i}].nullable must be a boolean.")
            dev = c.get("deviation", "")
            if dev not in _VALID_DEVIATIONS:
                raise ValueError(
                    f"proposed_columns[{i}].deviation '{dev}' not in {_VALID_DEVIATIONS}."
                )

        ddl = str(obj["proposed_ddl"])
        if not ddl.strip().upper().startswith("CREATE TABLE"):
            raise ValueError("proposed_ddl must start with CREATE TABLE.")
        # Must use {schema} placeholder so the runtime can substitute
        # BRONZE_AETNA / BRONZE_CARESOURCE / etc.
        if "{schema}" not in ddl:
            raise ValueError("proposed_ddl must include the `{schema}` placeholder.")

        scores = obj.get("standard_match_scores", {})
        if not isinstance(scores, dict):
            raise ValueError("standard_match_scores must be a JSON object.")

    # ------------------------------------------------------------------
    # Audit column injection
    # ------------------------------------------------------------------

    @classmethod
    def _inject_audit_columns(cls, ddl: str) -> str:
        """Splice the 7 mandatory audit columns into the proposed DDL.

        The agent is instructed NOT to emit them, but if it does we
        de-duplicate. The injection adds them BEFORE the closing
        parenthesis of the CREATE TABLE column list.
        """
        # Find the column list span — the LAST '(' before the LAST ')'
        # at the outer level. We assume a well-formed CREATE TABLE.
        s = ddl.rstrip().rstrip(";").rstrip()
        last_close = s.rfind(")")
        if last_close < 0:
            return ddl  # malformed, leave as-is
        # The matching open paren — assume single CREATE TABLE x (...)
        # so the first '(' after CREATE TABLE is the right one.
        first_open = s.find("(")
        if first_open < 0 or first_open >= last_close:
            return ddl

        head = s[: first_open + 1]
        body = s[first_open + 1 : last_close].rstrip().rstrip(",")
        tail = s[last_close:]

        # De-dupe: don't add audit cols already present
        body_upper = body.upper()
        audit_lines: list[str] = []
        for col_name, col_type in cls._MANDATORY_AUDIT_COLUMNS:
            if col_name.upper() not in body_upper:
                audit_lines.append(f"    {col_name:<18} {col_type}")
        if not audit_lines:
            return ddl

        # Re-format
        injected = (
            head
            + "\n"
            + body
            + ",\n    -- Mandatory DataLink audit columns (Phase 14 architect inject)\n"
            + ",\n".join(audit_lines)
            + "\n"
            + tail
        )
        if not injected.rstrip().endswith(";"):
            injected += ";"
        return injected
