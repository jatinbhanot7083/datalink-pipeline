"""DQ Suite AI Architect — Phase 19.1.

Anthropic-backed agent that proposes a FULL Great Expectations suite for a
``(dataset_code, layer)`` pair across all clients in the factory pattern.
This is the suite-level counterpart to ``datalink.agents.dq_author.proposer``,
which proposes one expectation at a time from a natural-language ask.

Inputs (context dict):
    dataset_code      str       e.g. "membership"
    dataset_display   str       "Membership"
    layer             str       "BRONZE" | "SILVER" | "GOLD"
    columns           list[dict] [{name, logical_type, nullable, is_business_key,
                                    is_pii, is_phi, description}]
    business_keys     list[str] (subset of columns marked is_business_key)
    pii_columns       list[str]
    phi_columns       list[str]
    standards_hits    list[dict] (optional RAG grounding from standards registry)
    max_expectations  int       cap (default 30)
    temperature       float     0.0 = deterministic; 0.7 = creative
    grounding_mode    str       "off" | "rag" | "strict"

Output (strict JSON contract):
    {
      "suite_name":          "membership_bronze",
      "dataset_code":        "membership",
      "layer":               "BRONZE",
      "expectations":        [ <GX expectation dicts>, ... ],
      "rationale":           "Plain-English narrative explaining the suite design.",
      "dimension_breakdown": {"Completeness": 12, "Validity": 8, ...},
      "expectation_count":   N,
      "tokens_used":         int    (filled by AgentBase)
    }

Each expectation dict shape:
    {
      "expectation_type": "expect_column_values_to_not_be_null",
      "kwargs":           {"column": "member_card_id"},
      "meta": {
        "dq_dimension":  "Completeness",
        "severity":      "CRITICAL" | "HIGH" | "MEDIUM" | "LOW",
        "description":   "..." ,
        "rationale":     "Why this check exists for this dataset/layer."
      }
    }
"""

from __future__ import annotations

import json
import re
from typing import Any

from datalink.agents.base import AgentBase

# ---------------------------------------------------------------------------
# Contract
# ---------------------------------------------------------------------------
_VALID_DIMENSIONS = {
    "Completeness",
    "Uniqueness",
    "Timeliness",
    "Accuracy",
    "Consistency",
    "Validity",
}
_VALID_SEVERITIES = {"CRITICAL", "HIGH", "MEDIUM", "LOW"}
_VALID_LAYERS = {"BRONZE", "SILVER", "GOLD"}

# Curated palette of GX expectation types — same set as the per-expectation
# proposer agent uses, so review UIs render consistently.
_KNOWN_EXPECTATION_TYPES = {
    "expect_column_values_to_not_be_null",
    "expect_column_values_to_be_unique",
    "expect_column_values_to_be_between",
    "expect_column_values_to_be_in_set",
    "expect_column_values_to_match_regex",
    "expect_column_pair_values_a_to_be_greater_than_b",
    "expect_column_max_to_be_between",
    "expect_table_row_count_to_be_between",
    "expect_compound_columns_to_be_unique",
    "expect_column_values_to_be_of_type",
    "expect_column_value_lengths_to_be_between",
}

# Dimension hints by layer — used as system-prompt scaffolding so the LLM
# emphasises the RIGHT dimensions per layer.  Bronze ≈ raw landing checks
# (completeness, format), Silver ≈ DV2 integrity (uniqueness, BK validity),
# Gold ≈ business rules (validity, consistency).
_LAYER_FOCUS = {
    "BRONZE": (
        "Bronze is RAW vendor-landing.  Focus on Completeness "
        "(business keys not null), basic Validity (regex on identifiers, "
        "phone/email/dates), and Timeliness (refresh_date freshness)."
    ),
    "SILVER": (
        "Silver is post-DV2 cleansed.  Focus on Uniqueness "
        "(business-key uniqueness on hubs), Consistency (FK integrity "
        "between sat → hub), and Completeness (audit columns populated)."
    ),
    "GOLD": (
        "Gold is the consumption layer.  Focus on Validity "
        "(business rules: non-negative amounts, valid status codes, "
        "date-range invariants), Accuracy (cross-field math), and "
        "Uniqueness on the canonical primary key."
    ),
}


class DqSuiteArchitectAgent(AgentBase):
    role = (
        "Senior Data Quality Architect designing comprehensive Great "
        "Expectations suites for healthcare data pipelines (Bronze→Silver→Gold)."
    )
    goal = (
        "Given a (dataset, layer) and its canonical schema, propose a "
        "complete, prioritised, dimension-balanced GX expectation suite "
        "with per-check rationale.  Cap at the operator's max.  Output "
        "strict JSON.  Never include row data or PHI in the response — "
        "expectations reference COLUMNS only, never values."
    )
    crew_name = "dq_ai_architect"

    # --- public --------------------------------------------------------

    def execute(self, context: dict[str, Any]) -> dict[str, Any]:
        dataset_code = str(context["dataset_code"]).lower()
        dataset_display = str(context.get("dataset_display") or dataset_code)
        layer = str(context["layer"]).upper()
        columns = context.get("columns") or []
        business_keys = context.get("business_keys") or []
        pii_columns = context.get("pii_columns") or []
        phi_columns = context.get("phi_columns") or []
        standards_hits = context.get("standards_hits") or []
        max_expectations = int(context.get("max_expectations") or 30)
        temperature = float(context.get("temperature") or 0.0)
        grounding_mode = str(context.get("grounding_mode") or "off").lower()

        if layer not in _VALID_LAYERS:
            raise ValueError(f"layer must be one of {_VALID_LAYERS}, got {layer!r}")
        if not columns:
            raise ValueError("No columns provided — agent needs schema to propose against.")
        max_expectations = max(5, min(60, max_expectations))  # hard bounds

        instruction = self._build_instruction(
            dataset_code=dataset_code,
            dataset_display=dataset_display,
            layer=layer,
            columns=columns,
            business_keys=business_keys,
            pii_columns=pii_columns,
            phi_columns=phi_columns,
            standards_hits=standards_hits,
            max_expectations=max_expectations,
            grounding_mode=grounding_mode,
        )

        # PHI-safe payload: column metadata only.  No row values, no client
        # identifiers (suite is universal across clients).
        # PHI-guard requires top-level keys come from datalink.phi.guard.SAFE_FIELDS.
        # `column_types` is NOT in that allow-list — use the canonical
        # `data_types` instead.  Free-form metadata goes under `context`.
        safe_payload: dict[str, Any] = {
            "dataset_code": dataset_code,
            "layer": layer,
            "column_names": [c.get("name") or c.get("column_name") for c in columns],
            "data_types": [
                str(c.get("logical_type") or c.get("type") or "TEXT").upper() for c in columns
            ],
            "context": {
                "max_expectations": max_expectations,
                "business_key_count": len(business_keys),
                "pii_column_count": len(pii_columns),
                "phi_column_count": len(phi_columns),
                "grounding_mode": grounding_mode,
                "standards_hits_count": len(standards_hits),
            },
        }

        raw = self._ask_llm(
            instruction=instruction,
            safe_payload=safe_payload,
            max_tokens=4000,  # whole suite needs more headroom than single-expectation
            temperature=temperature,
        )

        proposal = self._parse_and_validate(raw, layer=layer, dataset_code=dataset_code)
        # Trim if model overshot the cap
        if len(proposal["expectations"]) > max_expectations:
            proposal["expectations"] = proposal["expectations"][:max_expectations]
        proposal["expectation_count"] = len(proposal["expectations"])
        proposal["dimension_breakdown"] = self._dimension_breakdown(proposal["expectations"])
        proposal["suite_name"] = f"{dataset_code}_{layer.lower()}"
        proposal["dataset_code"] = dataset_code
        proposal["layer"] = layer
        proposal["temperature"] = temperature
        proposal["grounding_mode"] = grounding_mode
        return proposal

    # --- prompt building -----------------------------------------------

    def _build_instruction(
        self,
        *,
        dataset_code: str,
        dataset_display: str,
        layer: str,
        columns: list[dict],
        business_keys: list[str],
        pii_columns: list[str],
        phi_columns: list[str],
        standards_hits: list[dict],
        max_expectations: int,
        grounding_mode: str,
    ) -> str:
        # Build a dense column manifest the model can scan.
        col_lines = []
        for c in columns:
            nm = c.get("name") or c.get("column_name") or "?"
            tp = str(c.get("logical_type") or c.get("type") or "TEXT").upper()
            null = "NULL" if c.get("nullable") else "NOT NULL"
            flags = []
            if c.get("is_business_key"):
                flags.append("BK")
            if c.get("is_pii"):
                flags.append("PII")
            if c.get("is_phi"):
                flags.append("PHI")
            tag = f" [{','.join(flags)}]" if flags else ""
            desc = (c.get("description") or "")[:80]
            desc_str = f" — {desc}" if desc else ""
            col_lines.append(f"  - {nm} : {tp} {null}{tag}{desc_str}")
        col_block = "\n".join(col_lines)

        layer_focus = _LAYER_FOCUS[layer]

        grounding_block = ""
        if grounding_mode != "off" and standards_hits:
            grounding_block = (
                "\n\nGROUNDING — RAG hits from standards registry "
                "(treat as guidance, not strict templates):\n"
                + "\n".join(
                    f"  - {h.get('standard_name', '?')}: {str(h.get('snippet') or '')[:200]}"
                    for h in standards_hits[:10]
                )
            )

        return f"""You are designing a Great Expectations suite for the {layer} layer of the
"{dataset_display}" dataset (dataset_code={dataset_code}).  This suite will be
applied UNIVERSALLY to every client running this dataset's pipeline — there
is NO per-client variation.  The same suite runs against
BRONZE_<CLIENT>.RAW_{dataset_code.upper()} for every client.

LAYER FOCUS:
{layer_focus}

CANONICAL COLUMNS:
{col_block}

BUSINESS KEYS: {", ".join(business_keys) if business_keys else "(none flagged)"}
PII COLUMNS:   {", ".join(pii_columns) if pii_columns else "(none)"}
PHI COLUMNS:   {", ".join(phi_columns) if phi_columns else "(none)"}{grounding_block}

REQUIREMENTS:
1. Propose a COMPREHENSIVE suite — Completeness, Uniqueness, Validity,
   Consistency, Timeliness, Accuracy.  Aim for dimension balance.
2. Cap at {max_expectations} expectations TOTAL.  Be selective — quality > quantity.
3. Use ONLY these expectation types:
{chr(10).join(f"     - {t}" for t in sorted(_KNOWN_EXPECTATION_TYPES))}
4. Every expectation MUST include meta.dq_dimension (one of: Completeness,
   Uniqueness, Timeliness, Accuracy, Consistency, Validity), meta.severity
   (CRITICAL | HIGH | MEDIUM | LOW), meta.description, and meta.rationale.
5. Severity rules:
   - CRITICAL: business-key not null / unique violations, PHI columns missing
   - HIGH:     PII format violations, monetary out-of-range
   - MEDIUM:   non-PHI completeness gaps, value-set drift
   - LOW:      cosmetic / freshness warnings
6. NEVER reference row values or specific records.  Only column names + types.
7. Output STRICT JSON — no markdown fences, no commentary.

OUTPUT JSON SCHEMA:
{{
  "expectations": [
    {{
      "expectation_type": "...",
      "kwargs": {{ "column": "...", ... }},
      "meta": {{
        "dq_dimension": "...",
        "severity": "...",
        "description": "...",
        "rationale": "..."
      }}
    }},
    ...
  ],
  "rationale": "2-3 paragraph narrative covering the suite design philosophy, what dimensions you emphasised and why, and any caveats."
}}
"""

    # --- parse + validate ----------------------------------------------

    def _parse_and_validate(self, raw: str, *, layer: str, dataset_code: str) -> dict[str, Any]:
        """Parse JSON; raise ValueError with a helpful message if invalid."""
        # Tolerate accidental markdown fences from the model.
        text = raw.strip()
        if text.startswith("```"):
            text = re.sub(r"^```[a-z]*\n", "", text)
            text = re.sub(r"\n```$", "", text)
        try:
            obj = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"DqSuiteArchitectAgent: model returned non-JSON ({exc}). "
                f"First 300 chars: {text[:300]!r}"
            ) from exc

        if not isinstance(obj, dict):
            raise ValueError("Top-level must be a JSON object.")
        if "expectations" not in obj or not isinstance(obj["expectations"], list):
            raise ValueError("Missing 'expectations' array.")
        if not obj["expectations"]:
            raise ValueError("Suite is empty — agent proposed zero expectations.")

        clean: list[dict[str, Any]] = []
        for i, exp in enumerate(obj["expectations"]):
            if not isinstance(exp, dict):
                continue
            etype = str(exp.get("expectation_type") or "")
            if etype not in _KNOWN_EXPECTATION_TYPES:
                # skip silently — model occasionally emits an unsupported type
                continue
            kwargs = exp.get("kwargs") or {}
            meta = exp.get("meta") or {}
            dim = str(meta.get("dq_dimension") or "")
            sev = str(meta.get("severity") or "").upper()
            if dim not in _VALID_DIMENSIONS:
                dim = "Validity"  # safe default
            if sev not in _VALID_SEVERITIES:
                sev = "MEDIUM"
            clean.append(
                {
                    "expectation_type": etype,
                    "kwargs": kwargs,
                    "meta": {
                        "dq_dimension": dim,
                        "severity": sev,
                        "description": str(meta.get("description") or "")[:280],
                        "rationale": str(meta.get("rationale") or "")[:600],
                    },
                }
            )

        if not clean:
            raise ValueError("All proposed expectations were invalid or unknown types.")

        return {
            "expectations": clean,
            "rationale": str(obj.get("rationale") or "")[:4000],
        }

    @staticmethod
    def _dimension_breakdown(expectations: list[dict[str, Any]]) -> dict[str, int]:
        out: dict[str, int] = {}
        for e in expectations:
            d = str((e.get("meta") or {}).get("dq_dimension") or "Validity")
            out[d] = out.get(d, 0) + 1
        return out
