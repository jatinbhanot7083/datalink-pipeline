"""GoldSchemaDesignerAgent — Phase 15.6 — Construct the Global Gold schema for a dataset.

The AI Agent designs the canonical Gold model for a dataset by analyzing:
  1. The Bronze catalog rows (943 fields across 33 datasets) — what the
     vendor sends per the signed mapping spec.
  2. The chosen Gold anchor — CATALOG_ANCHOR (default), FHIR_R4_ANCHOR,
     X12_EDI_ANCHOR, NCPDP_D0_ANCHOR, CMS_ANCHOR, HEDIS_ANCHOR.
  3. (Optional) RAG-retrieved standard chunks from
     agent_memory.standard_references when a specialized anchor is selected.

The agent emits TWO things in one structured proposal:

  A. Gold schema design — the canonical column list for the dataset.
     Fewer columns than Bronze (consolidation), business-key flagged,
     PII/PHI inherited, semantic types, anchor citations per column.

  B. Bronze→Gold mapping rules — one rule per Gold column. Each rule
     names the Bronze source column(s) and the SQL expression that
     derives the Gold value. Rule kinds: DIRECT, CONCAT, COALESCE,
     LOOKUP, CAST, CASE, DERIVED.

The agent also flags whether DV2 (Hub/Sat/Link) is the right Silver
pattern for this dataset — if the dataset is small/simple it proposes
NORMALIZED with a short rationale. HITL operator takes the call.

PHI boundary: payload is metadata only — column names, counts, anchor
codes, RAG chunks. No row data ever touches the LLM.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from datalink.agents.base import AgentBase
from datalink.logging import get_logger

_log = get_logger(__name__)


class DesignerMode(StrEnum):
    AI_CONSTRUCT = "AI_CONSTRUCT"
    MANUAL_AUTHOR = "MANUAL_AUTHOR"
    IMPORT = "IMPORT"


class GoldAnchor(StrEnum):
    CATALOG_ANCHOR = "CATALOG_ANCHOR"
    FHIR_R4_ANCHOR = "FHIR_R4_ANCHOR"
    X12_EDI_ANCHOR = "X12_EDI_ANCHOR"
    NCPDP_D0_ANCHOR = "NCPDP_D0_ANCHOR"
    CMS_ANCHOR = "CMS_ANCHOR"
    HEDIS_ANCHOR = "HEDIS_ANCHOR"


# Anchor → RAG corpus code mapping (matches CONTROL.standard_registry.code).
# CATALOG_ANCHOR has no external corpus — the Bronze catalog itself IS the anchor.
_ANCHOR_TO_CORPUS = {
    GoldAnchor.FHIR_R4_ANCHOR: "fhir-r4",
    GoldAnchor.X12_EDI_ANCHOR: "x12",
    GoldAnchor.NCPDP_D0_ANCHOR: "ncpdp-d0",
    GoldAnchor.CMS_ANCHOR: "cms",
    GoldAnchor.HEDIS_ANCHOR: "hedis",
}


# JSON output contract — strict so the bridge can persist with confidence.
_REQUIRED_TOP = {
    "proposed_gold_table_name",
    "proposed_columns",
    "bronze_to_gold_mappings",
    "silver_pattern_recommendation",
    "rationale",
}
_REQUIRED_COLUMN = {"gold_column_name", "logical_type", "nullable", "rationale"}
_REQUIRED_MAPPING = {"gold_column_name", "transform_kind", "transform_sql", "bronze_source_columns"}
_VALID_LOGICAL_TYPES = {"TEXT", "INTEGER", "DECIMAL", "DATE", "TIMESTAMP", "BOOLEAN"}
_VALID_TRANSFORMS = {"DIRECT", "CONCAT", "COALESCE", "LOOKUP", "CAST", "CASE", "DERIVED"}
_VALID_PATTERNS = {"HUB_SAT_LINK", "NORMALIZED"}


@dataclass
class GoldColumn:
    gold_column_name: str
    logical_type: str  # TEXT | INTEGER | DECIMAL | DATE | TIMESTAMP | BOOLEAN
    nullable: bool
    rationale: str
    is_business_key: bool = False
    is_pii: bool = False
    is_phi: bool = False
    description: str = ""
    anchor_reference: str | None = None  # e.g. 'FHIR Patient.id'


@dataclass
class BronzeToGoldMapping:
    gold_column_name: str
    transform_kind: str  # DIRECT | CONCAT | COALESCE | LOOKUP | CAST | CASE | DERIVED
    transform_sql: str  # SQL expression using Bronze column names
    bronze_source_columns: list[str]
    rationale: str = ""
    confidence: float = 0.9


@dataclass
class SilverPatternRecommendation:
    recommended_pattern: str  # HUB_SAT_LINK | NORMALIZED
    is_overkill_flag: bool
    reasoning: str
    proposed_silver_shape: dict[str, Any] = field(default_factory=dict)


@dataclass
class GoldSchemaProposal:
    """Structured agent output. Persisted to global_gold_schema_* tables."""

    dataset_code: str
    gold_anchor: str
    designer_mode: str
    proposed_gold_table_name: str
    proposed_columns: list[GoldColumn]
    bronze_to_gold_mappings: list[BronzeToGoldMapping]
    silver_pattern_recommendation: SilverPatternRecommendation
    rationale: str = ""
    grounding: list[dict[str, Any]] = field(default_factory=list)
    proposer_model: str = ""
    tokens_used: int = 0
    duration_ms: int = 0
    temperature: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "dataset_code": self.dataset_code,
            "gold_anchor": self.gold_anchor,
            "designer_mode": self.designer_mode,
            "proposed_gold_table_name": self.proposed_gold_table_name,
            "proposed_columns": [
                {
                    "gold_column_name": c.gold_column_name,
                    "logical_type": c.logical_type,
                    "nullable": c.nullable,
                    "rationale": c.rationale,
                    "is_business_key": c.is_business_key,
                    "is_pii": c.is_pii,
                    "is_phi": c.is_phi,
                    "description": c.description,
                    "anchor_reference": c.anchor_reference,
                }
                for c in self.proposed_columns
            ],
            "bronze_to_gold_mappings": [
                {
                    "gold_column_name": m.gold_column_name,
                    "transform_kind": m.transform_kind,
                    "transform_sql": m.transform_sql,
                    "bronze_source_columns": m.bronze_source_columns,
                    "rationale": m.rationale,
                    "confidence": m.confidence,
                }
                for m in self.bronze_to_gold_mappings
            ],
            "silver_pattern_recommendation": {
                "recommended_pattern": self.silver_pattern_recommendation.recommended_pattern,
                "is_overkill_flag": self.silver_pattern_recommendation.is_overkill_flag,
                "reasoning": self.silver_pattern_recommendation.reasoning,
                "proposed_silver_shape": self.silver_pattern_recommendation.proposed_silver_shape,
            },
            "rationale": self.rationale,
            "grounding": self.grounding,
            "proposer_model": self.proposer_model,
            "tokens_used": self.tokens_used,
            "duration_ms": self.duration_ms,
            "temperature": self.temperature,
        }


class GoldSchemaDesignerAgent(AgentBase):
    role = "Senior Healthcare Data Architect for canonical Gold schema design"
    goal = (
        "Design the canonical Gold (consumption-layer) schema for a healthcare "
        "dataset. Anchor against the chosen reference model — CATALOG_ANCHOR "
        "(the vendor mapping spec) by default, with FHIR R4, X12 EDI, NCPDP D.0, "
        "CMS dictionaries, or NCQA HEDIS as specialized overrides. Consolidate "
        "Bronze fields into business-meaningful Gold columns (single member_id "
        "from multiple ID columns; full_name composed from name parts; effective-"
        "dated coverage instead of multiple begin/end pairs). Emit per-column "
        "rationale, anchor citations, and Bronze→Gold mapping SQL. "
        "ALSO recommend whether DV2 (Hub/Sat/Link) is the right Silver pattern "
        "OR whether NORMALIZED is better for trivially-small datasets."
    )
    crew_name = "gold_schema_designer"

    DEFAULT_TEMPERATURE = 0.0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def execute(self, context: dict[str, Any]) -> dict[str, Any]:
        """Design Gold schema for ``dataset_code`` anchored against
        ``gold_anchor``.

        Required ``context`` keys:

            dataset_code           str
            dataset_display_name   str
            gold_anchor            str   one of GoldAnchor values
            designer_mode          str   AI_CONSTRUCT (only mode the agent serves;
                                         MANUAL_AUTHOR / IMPORT skip the LLM and
                                         persist directly via the bridge)
            bronze_fields          list[dict]  rows from global_bronze_catalog_fields
            grounding_chunks       list[dict]  pre-fetched RAG hits (when anchor != CATALOG)

        Optional:
            temperature            float       defaults to 0.0
            category               str         from Bronze catalog dataset row
            default_frequency      str         from Bronze catalog dataset row
        """
        dataset_code = str(context["dataset_code"])
        dataset_display_name = str(
            context.get("dataset_display_name") or dataset_code.replace("_", " ").title()
        )
        gold_anchor = str(context.get("gold_anchor") or "CATALOG_ANCHOR")
        designer_mode = DesignerMode(str(context.get("designer_mode") or "AI_CONSTRUCT"))
        bronze_fields: list[dict[str, Any]] = list(context.get("bronze_fields", []))
        grounding_chunks: list[dict[str, Any]] = list(context.get("grounding_chunks", []))
        temperature = float(context.get("temperature", self.DEFAULT_TEMPERATURE))
        category = context.get("category")
        default_frequency = context.get("default_frequency")

        if not bronze_fields:
            raise ValueError(
                f"GoldSchemaDesigner: bronze_fields is empty for dataset_code={dataset_code!r}. "
                f"Did the loader run?"
            )
        if designer_mode != DesignerMode.AI_CONSTRUCT:
            raise ValueError(
                f"GoldSchemaDesignerAgent.execute() handles AI_CONSTRUCT only; "
                f"got {designer_mode}. Use bridge.persist_manual() / persist_imported() "
                f"for the other modes."
            )

        instruction = self._build_prompt(
            dataset_code=dataset_code,
            dataset_display_name=dataset_display_name,
            gold_anchor=gold_anchor,
            bronze_fields=bronze_fields,
            grounding_chunks=grounding_chunks,
            category=category,
            default_frequency=default_frequency,
        )

        bronze_field_names = [
            str(f.get("bronze_column_name") or f.get("gold_column_name") or "")
            for f in bronze_fields
        ]
        business_key_count = sum(1 for f in bronze_fields if f.get("is_business_key"))
        domain_count = self._estimate_domain_count(bronze_fields)
        safe_payload: dict[str, Any] = {
            "dataset_code": dataset_code,
            "dataset_display_name": dataset_display_name,
            "gold_anchor": gold_anchor,
            "designer_mode": designer_mode.value,
            "bronze_field_count": len(bronze_fields),
            "bronze_field_names": bronze_field_names[:50],  # MAX_LIST_LEN guard
            "business_key_count": business_key_count,
            "domain_count": domain_count,
            "anchor_reference_hits": len(grounding_chunks),
            "category": category,
            "default_frequency": default_frequency,
            "temperature": temperature,
        }

        # Token budget: matches Phase 14 ContractArchitect (16384). The
        # Anthropic SDK forces streaming above ~16K for haiku-4.5; staying
        # at 16384 keeps non-streaming + covers a 50-col proposal with full
        # mapping list + Silver pattern shape + per-column rationales.
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
                f"GoldSchemaDesigner returned non-JSON. {e}\n\nRaw:\n{raw[:500]}"
            ) from e

        self._validate(obj, dataset_code=dataset_code, gold_anchor=gold_anchor)

        # Provenance metadata tacked on after validation
        obj["dataset_code"] = dataset_code
        obj["gold_anchor"] = gold_anchor
        obj["designer_mode"] = designer_mode.value
        obj["temperature"] = temperature
        return obj

    # ------------------------------------------------------------------
    # Prompt construction
    # ------------------------------------------------------------------

    def _build_prompt(
        self,
        *,
        dataset_code: str,
        dataset_display_name: str,
        gold_anchor: str,
        bronze_fields: list[dict[str, Any]],
        grounding_chunks: list[dict[str, Any]],
        category: Any,
        default_frequency: Any,
    ) -> str:
        parts: list[str] = []

        parts.append(
            f"You are designing the canonical Gold (consumption-layer) schema for "
            f"the **{dataset_display_name}** dataset (dataset_code=`{dataset_code}`). "
            f"This Gold schema will be GLOBAL — every client gets a copy of this "
            f"structure, with the option to override per-client."
        )
        if category:
            parts.append(f"Domain category: **{category}**.")
        if default_frequency:
            parts.append(f"Default ingest frequency: **{default_frequency}**.")
        parts.append(f"Gold anchor selected: **{gold_anchor}**.")

        # Anchor-specific framing
        if gold_anchor == GoldAnchor.CATALOG_ANCHOR.value:
            parts.append(
                "CATALOG_ANCHOR means the Bronze mapping spec IS the anchor. "
                "Treat each Bronze field as authoritative and consolidate / "
                "rationalise into a Gold model that's still aligned to the "
                "vendor's contract — but cleaner: combine first/middle/last into "
                "full_name; resolve code+description pairs into a single descriptive "
                "field; combine multiple ID columns into a single business key with "
                "provenance; collapse multiple effective-date pairs into one "
                "effective_from / effective_to span."
            )
        elif gold_anchor == GoldAnchor.FHIR_R4_ANCHOR.value:
            parts.append(
                "FHIR_R4_ANCHOR means align the Gold schema to HL7 FHIR R4 "
                "resource shapes (Patient, Coverage, Claim, Encounter, "
                "Practitioner, Organization, etc.). Use FHIR canonical names "
                "(`birthDate` not `dob`; `identifier` for member IDs; "
                "`address.line[]` for street). Cite the FHIR resource + element "
                "for each Gold column in `anchor_reference`."
            )
        elif gold_anchor == GoldAnchor.X12_EDI_ANCHOR.value:
            parts.append(
                "X12_EDI_ANCHOR means align to X12 5010 transactions: 837P/I "
                "for claims, 834 for enrollment, 270/271 for eligibility, 278 "
                "for prior auth. Cite the X12 segment + element (e.g. "
                "'837P 2300/CLM01') in `anchor_reference`."
            )
        elif gold_anchor == GoldAnchor.NCPDP_D0_ANCHOR.value:
            parts.append(
                "NCPDP_D0_ANCHOR means align to NCPDP Telecommunication D.0 "
                "for pharmacy claims (B1/B2/B3 transactions). Cite NCPDP "
                "field IDs (e.g. '4Ø1-D1 Date of Service')."
            )
        elif gold_anchor == GoldAnchor.CMS_ANCHOR.value:
            parts.append(
                "CMS_ANCHOR means align to CMS data dictionaries — Medicare "
                "RIF, MA Encounter (MAO-004), RAPS, MMR, MOR. Cite the CMS "
                "table + variable name."
            )
        elif gold_anchor == GoldAnchor.HEDIS_ANCHOR.value:
            parts.append(
                "HEDIS_ANCHOR means align to NCQA HEDIS measure data shapes — "
                "denominator/numerator/exclusion-eligible cohorts."
            )

        # Bronze field listing
        parts.append(
            f"\nBRONZE CATALOG ({len(bronze_fields)} fields per the vendor "
            f"mapping spec — these are what arrives on the wire):"
        )
        # Compact representation — name + type + req + brief desc
        for i, bf in enumerate(bronze_fields[:80]):  # MAX_LIST_LEN bound for prompt
            name = bf.get("bronze_column_name") or bf.get("gold_column_name") or ""
            t = bf.get("logical_type") or "TEXT"
            req = bf.get("requirement") or "Optional"
            desc = (bf.get("description") or "")[:60]
            bk = "[BK]" if bf.get("is_business_key") else "    "
            pii = "[PII]" if bf.get("is_pii") else ""
            phi = "[PHI]" if bf.get("is_phi") else ""
            parts.append(f"  {i + 1:>2}. {bk} {name:<35} {t:<10} {req:<10} {pii}{phi}  {desc}")
        if len(bronze_fields) > 80:
            parts.append(f"  ... ({len(bronze_fields) - 80} more fields not shown)")

        # Optional RAG grounding
        if grounding_chunks:
            parts.append(
                f"\nANCHOR GROUNDING — top {len(grounding_chunks)} chunks from "
                f"the {gold_anchor} reference corpus (use as the AUTHORITATIVE "
                f"source for canonical Gold field names + types):"
            )
            for i, ch in enumerate(grounding_chunks):
                code = ch.get("standard_code", "?")
                section = ch.get("section_path", "")
                text = (ch.get("chunk_text") or "")[:1200]
                parts.append(f"\n--- chunk #{i + 1} [{code} / {section}] ---\n{text}")

        # Output contract
        parts.append(
            """
OUTPUT FORMAT — Return ONLY a single JSON object, no surrounding text.
The JSON must conform to this schema:

{
  "proposed_gold_table_name": "member",
  "proposed_columns": [
    {
      "gold_column_name": "member_id",
      "logical_type": "TEXT",
      "nullable": false,
      "is_business_key": true,
      "is_pii": false,
      "is_phi": true,
      "description": "Canonical member identifier — coalesced from card/medicare/medicaid IDs.",
      "anchor_reference": "FHIR Patient.identifier",
      "rationale": "Single business key consolidates 3 source ID columns into one resolved value with provenance."
    }
  ],
  "bronze_to_gold_mappings": [
    {
      "gold_column_name": "member_id",
      "transform_kind": "COALESCE",
      "transform_sql": "COALESCE(member_card_id, member_medicare_id, member_medicaid_id)",
      "bronze_source_columns": ["member_card_id", "member_medicare_id", "member_medicaid_id"],
      "rationale": "Use card_id when present; fall back to Medicare; final fallback Medicaid.",
      "confidence": 0.95
    }
  ],
  "silver_pattern_recommendation": {
    "recommended_pattern": "HUB_SAT_LINK",
    "is_overkill_flag": false,
    "reasoning": "Member dataset has 41 fields across 4 distinct domains (demographics, address, eligibility, plan) with one business key. DV2 Hub/Sat/Link cleanly separates the change-rate domains and supports point-in-time joins. NOT overkill.",
    "proposed_silver_shape": {
      "hubs": [{"name": "HUB_MEMBER", "business_keys": ["member_card_id"]}],
      "sats": [
        {"name": "SAT_MEMBER_DEMOGRAPHICS", "hub": "HUB_MEMBER", "columns": ["first_name","middle_name","last_name","gender","birth_date"]},
        {"name": "SAT_MEMBER_ADDRESS", "hub": "HUB_MEMBER", "columns": ["street_1","street_2","city","state","zip"]},
        {"name": "SAT_MEMBER_ELIGIBILITY", "hub": "HUB_MEMBER", "columns": ["effective_from","effective_to","line_of_business","status"]}
      ],
      "links": []
    }
  },
  "rationale": "Two-paragraph high-level explanation of the design choices."
}

RULES:
1. proposed_gold_table_name: snake_case, singular noun (e.g. `member`, not `members`).
2. proposed_columns: aim for FEWER columns than Bronze — consolidate where it makes sense.
   Flag is_business_key=true for natural keys (member_id, claim_id, provider_npi).
3. bronze_to_gold_mappings: ONE entry per Gold column. transform_sql uses Bronze
   column names (lowercase from `bronze_column_name`) verbatim. transform_kind
   must be one of: DIRECT, CONCAT, COALESCE, LOOKUP, CAST, CASE, DERIVED.
4. logical_type: one of TEXT, INTEGER, DECIMAL, DATE, TIMESTAMP, BOOLEAN.
5. silver_pattern_recommendation:
   - Default to HUB_SAT_LINK (Data Vault 2.0).
   - Set is_overkill_flag=true and recommended_pattern=NORMALIZED ONLY when:
     dataset has < 10 fields AND zero or one business key AND single domain
     (e.g. a code-table or trivial reference dataset). Operator HITL gate
     decides; you just recommend.
   - proposed_silver_shape: when HUB_SAT_LINK, list the Hubs/Sats/Links the
     Silver builder should generate. When NORMALIZED, list the single
     normalized table shape.
6. anchor_reference: REQUIRED for non-CATALOG_ANCHOR proposals; OPTIONAL
   (but encouraged) for CATALOG_ANCHOR proposals.
7. Honor PII/PHI flags from the Bronze catalog — propagate is_pii/is_phi.
"""
        )
        return "\n".join(parts)

    # ------------------------------------------------------------------
    # Domain-count heuristic — used in the prompt + the safe payload to
    # help the agent reason about Silver pattern choice.
    # ------------------------------------------------------------------

    @staticmethod
    def _estimate_domain_count(bronze_fields: list[dict[str, Any]]) -> int:
        """Cheap heuristic: count distinct domain prefixes in Bronze field names.

        E.g. {member_first_name, member_phone, member_address} → 1 domain (member).
        {member_id, provider_npi, plan_id} → 3 domains (member / provider / plan).

        Used as a soft hint to the LLM about whether DV2 separation is justified.
        """
        prefixes: set[str] = set()
        for bf in bronze_fields:
            name = (bf.get("bronze_column_name") or bf.get("gold_column_name") or "").lower()
            tokens = name.split("_")
            if not tokens:
                continue
            # First token is usually the domain
            prefixes.add(tokens[0])
        return len(prefixes)

    # ------------------------------------------------------------------
    # JSON extraction + validation
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_json(raw: str) -> dict[str, Any]:
        s = raw.strip()
        if s.startswith("```"):
            lines = s.splitlines()
            if lines:
                lines = lines[1:]
            if lines and lines[-1].strip().startswith("```"):
                lines = lines[:-1]
            s = "\n".join(lines).strip()
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
    def _validate(cls, obj: dict[str, Any], *, dataset_code: str, gold_anchor: str) -> None:
        missing = _REQUIRED_TOP - set(obj.keys())
        if missing:
            raise ValueError(f"Missing top-level keys: {sorted(missing)}")

        # Gold table name — snake_case
        table_name = str(obj["proposed_gold_table_name"])
        if not re.match(r"^[a-z][a-z0-9_]*$", table_name):
            raise ValueError(
                f"proposed_gold_table_name '{table_name}' must be snake_case "
                f"(lowercase + underscores, starting with letter)."
            )

        # Columns
        cols = obj["proposed_columns"]
        if not isinstance(cols, list) or not cols:
            raise ValueError("proposed_columns must be a non-empty list.")
        gold_names: set[str] = set()
        for i, c in enumerate(cols):
            cmissing = _REQUIRED_COLUMN - set(c.keys())
            if cmissing:
                raise ValueError(f"proposed_columns[{i}] missing keys: {sorted(cmissing)}")
            name = str(c["gold_column_name"])
            if not re.match(r"^[a-z_][a-z0-9_]*$", name):
                raise ValueError(
                    f"proposed_columns[{i}].gold_column_name '{name}' must be snake_case."
                )
            if name in gold_names:
                raise ValueError(f"proposed_columns: duplicate gold_column_name '{name}'.")
            gold_names.add(name)
            if c["logical_type"] not in _VALID_LOGICAL_TYPES:
                raise ValueError(
                    f"proposed_columns[{i}].logical_type '{c['logical_type']}' "
                    f"must be one of {sorted(_VALID_LOGICAL_TYPES)}."
                )
            if not isinstance(c["nullable"], bool):
                raise ValueError(f"proposed_columns[{i}].nullable must be a boolean.")

        # Mappings — one per Gold column
        mappings = obj["bronze_to_gold_mappings"]
        if not isinstance(mappings, list):
            raise ValueError("bronze_to_gold_mappings must be a list.")
        mapped_names: set[str] = set()
        for i, m in enumerate(mappings):
            mmissing = _REQUIRED_MAPPING - set(m.keys())
            if mmissing:
                raise ValueError(f"bronze_to_gold_mappings[{i}] missing keys: {sorted(mmissing)}")
            kind = str(m["transform_kind"]).upper()
            if kind not in _VALID_TRANSFORMS:
                raise ValueError(
                    f"bronze_to_gold_mappings[{i}].transform_kind '{kind}' "
                    f"must be one of {sorted(_VALID_TRANSFORMS)}."
                )
            srcs = m["bronze_source_columns"]
            if not isinstance(srcs, list):
                raise ValueError(
                    f"bronze_to_gold_mappings[{i}].bronze_source_columns must be a list."
                )
            mapped_names.add(str(m["gold_column_name"]))

        # Cross-check: every Gold column has a mapping
        unmapped = gold_names - mapped_names
        if unmapped:
            raise ValueError(
                f"Gold columns without bronze_to_gold_mappings entries: {sorted(unmapped)}"
            )

        # Silver pattern recommendation
        rec = obj["silver_pattern_recommendation"]
        if not isinstance(rec, dict):
            raise ValueError("silver_pattern_recommendation must be an object.")
        rec_pat = str(rec.get("recommended_pattern", ""))
        if rec_pat not in _VALID_PATTERNS:
            raise ValueError(
                f"silver_pattern_recommendation.recommended_pattern '{rec_pat}' "
                f"must be one of {sorted(_VALID_PATTERNS)}."
            )
        if not isinstance(rec.get("is_overkill_flag"), bool):
            raise ValueError("silver_pattern_recommendation.is_overkill_flag must be a boolean.")
        if not str(rec.get("reasoning", "")).strip():
            raise ValueError("silver_pattern_recommendation.reasoning must be non-empty.")

        # Rationale
        if not str(obj.get("rationale", "")).strip():
            raise ValueError("rationale must be non-empty.")

    # ------------------------------------------------------------------
    # Static helper — anchor → corpus code (for the bridge layer)
    # ------------------------------------------------------------------

    @staticmethod
    def anchor_corpus_code(gold_anchor: str) -> str | None:
        """Return the standard_registry.code for the given anchor, or None
        if the anchor has no external corpus (CATALOG_ANCHOR)."""
        try:
            return _ANCHOR_TO_CORPUS.get(GoldAnchor(gold_anchor))
        except ValueError:
            return None
