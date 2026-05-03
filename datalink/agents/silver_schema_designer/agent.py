"""SilverSchemaDesignerAgent — Phase 15.8 — Construct the Silver schema for a dataset.

The Silver layer (Data Vault 2.0 by default) integrates Bronze raw rows
into Hubs (business keys), Satellites (descriptive payload with hash-diff
change-detection), and Links (relationships). For trivially small datasets
the operator can pick NORMALIZED — a single staging table with TRY_CAST +
business-key NOT NULL filter.

The agent emits THREE things in one structured proposal:

  A. Silver tables — list of Hubs / Sats / Links (or single NORMALIZED).
     Each table has its own column list + business keys (Hubs) + parent Hub
     (Sats) + linked Hubs (Links).

  B. Bronze→Silver mapping rules — one rule per Silver column. Each rule
     names the Bronze source column(s) and the SQL expression that derives
     the Silver value. Rule kinds: DIRECT, CONCAT, COALESCE, LOOKUP, CAST,
     CASE, DERIVED, HASH.

  C. Anchor citation per column — when a specialized anchor (FHIR / X12 /
     etc.) is selected, the agent cites the standard reference for each
     column where applicable.

PHI boundary: payload is metadata only — column names, counts, anchor
codes, RAG chunks. No row data ever touches the LLM.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from datalink.agents.base import AgentBase
from datalink.logging import get_logger

_log = get_logger(__name__)


class SilverPattern(StrEnum):
    HUB_SAT_LINK = "HUB_SAT_LINK"
    NORMALIZED = "NORMALIZED"


class SilverDesignerMode(StrEnum):
    AI_CONSTRUCT = "AI_CONSTRUCT"
    MANUAL_AUTHOR = "MANUAL_AUTHOR"
    IMPORT = "IMPORT"


# Anchor codes mirror Gold designer (CATALOG default; FHIR/X12/NCPDP/CMS/HEDIS)
_ANCHOR_TO_CORPUS = {
    "FHIR_R4_ANCHOR": "fhir-r4",
    "X12_EDI_ANCHOR": "x12",
    "NCPDP_D0_ANCHOR": "ncpdp-d0",
    "CMS_ANCHOR": "cms",
    "HEDIS_ANCHOR": "hedis",
}

_REQUIRED_TOP = {
    "silver_pattern",
    "silver_tables",
    "bronze_to_silver_mappings",
    "rationale",
}
_REQUIRED_TABLE = {"table_name", "table_kind", "columns"}
_REQUIRED_COLUMN = {"column_name", "logical_type", "nullable"}
_REQUIRED_MAPPING = {
    "silver_table_name",
    "silver_column_name",
    "transform_kind",
    "transform_sql",
    "bronze_source_columns",
}
_VALID_KINDS = {"HUB", "SAT", "LINK", "NORMALIZED"}
_VALID_LOGICAL = {"TEXT", "INTEGER", "DECIMAL", "DATE", "TIMESTAMP", "BOOLEAN"}
_VALID_TRANSFORMS = {"DIRECT", "CONCAT", "COALESCE", "LOOKUP", "CAST", "CASE", "DERIVED", "HASH"}


@dataclass
class SilverColumn:
    column_name: str
    logical_type: str
    nullable: bool
    is_business_key: bool = False
    is_hash_key: bool = False
    is_hash_diff: bool = False
    is_pii: bool = False
    is_phi: bool = False
    description: str = ""


@dataclass
class SilverTable:
    table_name: str
    table_kind: str  # HUB | SAT | LINK | NORMALIZED
    columns: list[SilverColumn] = field(default_factory=list)
    business_keys: list[str] = field(default_factory=list)
    parent_hub_name: str | None = None  # for SATs
    linked_hub_names: list[str] = field(default_factory=list)  # for LINKs
    description: str = ""


@dataclass
class SilverProposal:
    dataset_code: str
    silver_anchor: str
    silver_pattern: str
    designer_mode: str
    silver_tables: list[SilverTable]
    bronze_to_silver_mappings: list[dict[str, Any]]
    rationale: str = ""
    grounding: list[dict[str, Any]] = field(default_factory=list)
    proposer_model: str = ""
    tokens_used: int = 0
    duration_ms: int = 0
    temperature: float = 0.0


class SilverSchemaDesignerAgent(AgentBase):
    role = "Senior Healthcare Data Architect for Silver-layer (Data Vault 2.0) integration design"
    goal = (
        "Design the Silver-layer schema for a healthcare dataset. Default to "
        "Data Vault 2.0 Hub/Sat/Link unless the dataset is trivially small "
        "(<10 fields, single domain, ≤1 business key) — in which case "
        "NORMALIZED is acceptable. Emit Hubs (business-key registries with "
        "SHA-256 hash_key), Satellites (descriptive payload partitioned by "
        "change-rate domain, with hash_diff for change-detection), and Links "
        "(many-to-many relationships between Hubs). For each Silver column, "
        "produce a Bronze→Silver SQL transform rule citing the source Bronze "
        "column(s). Honor PII/PHI flags from the Bronze catalog."
    )
    crew_name = "silver_schema_designer"

    DEFAULT_TEMPERATURE = 0.0

    def execute(self, context: dict[str, Any]) -> dict[str, Any]:
        """Design Silver schema for ``dataset_code`` with the chosen pattern.

        Required ``context`` keys:

            dataset_code           str
            dataset_display_name   str
            silver_anchor          str   one of CATALOG / FHIR_R4 / X12 / etc.
            silver_pattern         str   HUB_SAT_LINK | NORMALIZED
            designer_mode          str   AI_CONSTRUCT (others bypass agent)
            bronze_fields          list[dict]  rows from global_bronze_catalog_fields
            grounding_chunks       list[dict]  optional RAG hits

        Optional:
            temperature            float       defaults to 0.0
            category, default_frequency        Bronze catalog hints
        """
        dataset_code = str(context["dataset_code"])
        dataset_display_name = str(
            context.get("dataset_display_name") or dataset_code.replace("_", " ").title()
        )
        silver_anchor = str(context.get("silver_anchor") or "CATALOG_ANCHOR")
        silver_pattern = str(context.get("silver_pattern") or "HUB_SAT_LINK")
        designer_mode = SilverDesignerMode(str(context.get("designer_mode") or "AI_CONSTRUCT"))
        bronze_fields: list[dict[str, Any]] = list(context.get("bronze_fields", []))
        grounding_chunks: list[dict[str, Any]] = list(context.get("grounding_chunks", []))
        temperature = float(context.get("temperature", self.DEFAULT_TEMPERATURE))
        category = context.get("category")

        if not bronze_fields:
            raise ValueError(f"SilverSchemaDesigner: bronze_fields empty for {dataset_code!r}.")
        if designer_mode != SilverDesignerMode.AI_CONSTRUCT:
            raise ValueError(
                f"SilverSchemaDesignerAgent.execute() handles AI_CONSTRUCT only; "
                f"got {designer_mode}. Use bridge persist_manual()/persist_imported()."
            )
        if silver_pattern not in {"HUB_SAT_LINK", "NORMALIZED"}:
            raise ValueError(
                f"silver_pattern must be HUB_SAT_LINK or NORMALIZED; got {silver_pattern!r}"
            )

        instruction = self._build_prompt(
            dataset_code=dataset_code,
            dataset_display_name=dataset_display_name,
            silver_anchor=silver_anchor,
            silver_pattern=silver_pattern,
            bronze_fields=bronze_fields,
            grounding_chunks=grounding_chunks,
            category=category,
        )

        bronze_field_names = [str(f.get("bronze_column_name") or "") for f in bronze_fields]
        business_key_count = sum(1 for f in bronze_fields if f.get("is_business_key"))
        safe_payload: dict[str, Any] = {
            "dataset_code": dataset_code,
            "dataset_display_name": dataset_display_name,
            "gold_anchor": silver_anchor,  # PHI guard accepts this
            "designer_mode": designer_mode.value,
            "silver_pattern": silver_pattern,
            "bronze_field_count": len(bronze_fields),
            "bronze_field_names": bronze_field_names[:50],
            "business_key_count": business_key_count,
            "anchor_reference_hits": len(grounding_chunks),
            "category": category,
            "temperature": temperature,
        }

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
                f"SilverSchemaDesigner returned non-JSON. {e}\n\nRaw:\n{raw[:500]}"
            ) from e

        self._validate(obj, silver_pattern=silver_pattern)
        # Provenance metadata
        obj["dataset_code"] = dataset_code
        obj["silver_anchor"] = silver_anchor
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
        silver_anchor: str,
        silver_pattern: str,
        bronze_fields: list[dict[str, Any]],
        grounding_chunks: list[dict[str, Any]],
        category: Any,
    ) -> str:
        parts: list[str] = []
        parts.append(
            f"You are designing the Silver-layer schema for the **{dataset_display_name}** "
            f"dataset (dataset_code=`{dataset_code}`)."
        )
        if category:
            parts.append(f"Domain category: **{category}**.")
        parts.append(f"Silver pattern: **{silver_pattern}**.")
        parts.append(f"Anchor: **{silver_anchor}** (CATALOG = vendor mapping spec).")

        if silver_pattern == "HUB_SAT_LINK":
            parts.append(
                "DV2 RULES:\n"
                "  - HUB tables hold business keys. Each Hub gets a `hash_key` (SHA-256 of BKs) "
                "+ the BKs + audit columns. INSERT-only.\n"
                "  - SAT tables hold descriptive attributes for one Hub. Each Sat has the parent "
                "Hub's `hash_key` + descriptive columns + `hash_diff` (SHA-256 of payload) + "
                "audit columns. Group attributes into Sats by domain (demographics, address, "
                "eligibility, etc.). One Sat per (Hub, change-rate domain).\n"
                "  - LINK tables hold many-to-many relationships between Hubs. Each Link has its "
                "own `link_hash_key` (SHA-256 of participant hash_keys) + each Hub's hash_key "
                "+ audit columns.\n"
                "  - DO NOT include audit columns (`_load_dt`, `_record_source`, `_batch_id`) "
                "in your column list — they're auto-injected by the Pipeline Architect."
            )
        else:
            parts.append(
                "NORMALIZED RULES:\n"
                "  - Single Silver table named `<dataset_code>_silver`.\n"
                "  - Columns mirror Bronze 1:1 (TRY_CAST + filter NULL business keys).\n"
                "  - Set `table_kind=NORMALIZED` for the single table."
            )

        # Bronze field listing (compact)
        parts.append(f"\nBRONZE CATALOG ({len(bronze_fields)} fields):")
        for i, bf in enumerate(bronze_fields[:80]):
            name = bf.get("bronze_column_name") or ""
            t = bf.get("logical_type") or "TEXT"
            req = bf.get("requirement") or "Optional"
            bk = "[BK]" if bf.get("is_business_key") else "    "
            pii = "[PII]" if bf.get("is_pii") else ""
            phi = "[PHI]" if bf.get("is_phi") else ""
            desc = (bf.get("description") or "")[:60]
            parts.append(f"  {i + 1:>2}. {bk} {name:<35} {t:<10} {req:<10} {pii}{phi}  {desc}")

        if grounding_chunks:
            parts.append(
                f"\nANCHOR GROUNDING — {len(grounding_chunks)} chunks from {silver_anchor}:"
            )
            for i, ch in enumerate(grounding_chunks):
                code = ch.get("standard_code", "?")
                section = ch.get("section_path", "")
                text = (ch.get("chunk_text") or "")[:1200]
                parts.append(f"\n--- chunk #{i + 1} [{code} / {section}] ---\n{text}")

        parts.append(
            """
OUTPUT FORMAT — Return ONLY a single JSON object:

{
  "silver_pattern": "HUB_SAT_LINK",
  "silver_tables": [
    {
      "table_name": "HUB_MEMBER",
      "table_kind": "HUB",
      "business_keys": ["member_id"],
      "parent_hub_name": null,
      "linked_hub_names": [],
      "description": "Member business-key registry",
      "columns": [
        {"column_name": "hash_key", "logical_type": "TEXT", "nullable": false, "is_hash_key": true, "description": "SHA-256 of business keys"},
        {"column_name": "member_id", "logical_type": "TEXT", "nullable": false, "is_business_key": true, "description": "Member identifier"}
      ]
    },
    {
      "table_name": "SAT_MEMBER_DEMOGRAPHICS",
      "table_kind": "SAT",
      "business_keys": [],
      "parent_hub_name": "HUB_MEMBER",
      "linked_hub_names": [],
      "description": "Member name + DOB + gender",
      "columns": [
        {"column_name": "hash_key", "logical_type": "TEXT", "nullable": false, "is_hash_key": true, "description": "FK to parent Hub"},
        {"column_name": "first_name", "logical_type": "TEXT", "nullable": true, "is_pii": true},
        {"column_name": "last_name", "logical_type": "TEXT", "nullable": true, "is_pii": true},
        {"column_name": "hash_diff", "logical_type": "TEXT", "nullable": false, "is_hash_diff": true, "description": "SHA-256 of payload — change-detect"}
      ]
    }
  ],
  "bronze_to_silver_mappings": [
    {
      "silver_table_name": "HUB_MEMBER",
      "silver_column_name": "member_id",
      "bronze_source_columns": ["member_card_id", "member_medicare_id"],
      "transform_kind": "COALESCE",
      "transform_sql": "COALESCE(member_card_id, member_medicare_id)",
      "rationale": "Coalesce 2 ID columns into single business key.",
      "confidence": 0.95
    }
  ],
  "rationale": "Two-paragraph explanation of the Silver layout choices."
}

RULES:
1. silver_pattern must equal the requested pattern.
2. table_kind one of: HUB, SAT, LINK, NORMALIZED.
3. Each HUB lists its business_keys (the Bronze column NAMES that compose the BK).
4. Each SAT names its parent_hub_name + descriptive columns + ends with hash_diff.
5. Each LINK lists linked_hub_names + has its own link_hash_key column.
6. logical_type: TEXT | INTEGER | DECIMAL | DATE | TIMESTAMP | BOOLEAN.
7. transform_kind: DIRECT | CONCAT | COALESCE | LOOKUP | CAST | CASE | DERIVED | HASH.
8. bronze_to_silver_mappings: one entry per Silver column EXCEPT hash_key (auto-derived) and hash_diff (auto-derived).
9. transform_sql uses Bronze column names verbatim.
10. Honor is_pii / is_phi flags from Bronze fields — propagate them.
"""
        )
        return "\n".join(parts)

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
    def _validate(cls, obj: dict[str, Any], *, silver_pattern: str) -> None:
        missing = _REQUIRED_TOP - set(obj.keys())
        if missing:
            raise ValueError(f"Missing top-level keys: {sorted(missing)}")

        if obj["silver_pattern"] not in {"HUB_SAT_LINK", "NORMALIZED"}:
            raise ValueError("silver_pattern must be HUB_SAT_LINK or NORMALIZED")

        tables = obj["silver_tables"]
        if not isinstance(tables, list) or not tables:
            raise ValueError("silver_tables must be a non-empty list.")

        seen_table_names: set[str] = set()
        all_silver_cols: set[tuple[str, str]] = set()
        for i, t in enumerate(tables):
            tmissing = _REQUIRED_TABLE - set(t.keys())
            if tmissing:
                raise ValueError(f"silver_tables[{i}] missing keys: {sorted(tmissing)}")
            tn = str(t["table_name"])
            if tn in seen_table_names:
                raise ValueError(f"Duplicate silver table name: {tn}")
            seen_table_names.add(tn)
            if t["table_kind"] not in _VALID_KINDS:
                raise ValueError(
                    f"silver_tables[{i}].table_kind must be one of {sorted(_VALID_KINDS)}"
                )
            cols = t.get("columns") or []
            if not isinstance(cols, list) or not cols:
                raise ValueError(f"silver_tables[{i}].columns must be non-empty list.")
            for j, c in enumerate(cols):
                cmissing = _REQUIRED_COLUMN - set(c.keys())
                if cmissing:
                    raise ValueError(
                        f"silver_tables[{i}].columns[{j}] missing keys: {sorted(cmissing)}"
                    )
                if c["logical_type"] not in _VALID_LOGICAL:
                    raise ValueError(
                        f"silver_tables[{i}].columns[{j}].logical_type "
                        f"must be one of {sorted(_VALID_LOGICAL)}"
                    )
                col_name = str(c["column_name"])
                all_silver_cols.add((tn, col_name))

        # Pattern conformance
        kinds = {t["table_kind"] for t in tables}
        if silver_pattern == "NORMALIZED":
            if kinds != {"NORMALIZED"} or len(tables) != 1:
                raise ValueError("NORMALIZED pattern requires exactly 1 table of kind NORMALIZED.")
        elif silver_pattern == "HUB_SAT_LINK":
            if "HUB" not in kinds:
                raise ValueError("HUB_SAT_LINK pattern requires at least 1 HUB table.")
            if "NORMALIZED" in kinds:
                raise ValueError("HUB_SAT_LINK pattern cannot mix NORMALIZED tables.")

        # Mapping checks
        mappings = obj["bronze_to_silver_mappings"]
        if not isinstance(mappings, list):
            raise ValueError("bronze_to_silver_mappings must be a list.")
        for k, m in enumerate(mappings):
            mmissing = _REQUIRED_MAPPING - set(m.keys())
            if mmissing:
                raise ValueError(f"bronze_to_silver_mappings[{k}] missing keys: {sorted(mmissing)}")
            kind = str(m["transform_kind"]).upper()
            if kind not in _VALID_TRANSFORMS:
                raise ValueError(
                    f"bronze_to_silver_mappings[{k}].transform_kind must be one of "
                    f"{sorted(_VALID_TRANSFORMS)}"
                )
            srcs = m.get("bronze_source_columns")
            if not isinstance(srcs, list):
                raise ValueError(
                    f"bronze_to_silver_mappings[{k}].bronze_source_columns must be list"
                )

        if not str(obj.get("rationale", "")).strip():
            raise ValueError("rationale must be non-empty.")

    @staticmethod
    def anchor_corpus_code(silver_anchor: str) -> str | None:
        return _ANCHOR_TO_CORPUS.get(silver_anchor)
