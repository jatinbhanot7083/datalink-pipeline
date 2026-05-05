"""DqProposerAgent — natural-language ask -> structured GX expectation proposal.

The LLM only ever sees:
  * the user's prompt
  * the **column metadata** for the target table (name, type, nullability)
  * any prior turn's revision request (chat history)

It never sees row data — PHI boundary is enforced by AgentBase._ask_llm.

Output contract — strict JSON, parsed and validated. Schema:

    {
      "expectation_type": "expect_column_values_to_be_between",
      "kwargs":           { "column": "billed_amount", "min_value": 0, "max_value": 2000000 },
      "meta": {
        "dq_dimension":  "Validity",                 // one of the 6
        "severity":      "HIGH" | "MEDIUM" | "LOW",
        "description":   "Plain-English what + why."
      },
      "sql_preview":      "SELECT ... reproducing the check ...",
      "rationale":        "Short paragraph explaining why this expectation matches the ask."
    }

In stub mode (no API key) the agent uses a small heuristic mapper so the
demo / offline path still produces a sensible proposal — keyword-driven,
deterministic, and shaped exactly like the real LLM output.
"""

from __future__ import annotations

import json
import re
from typing import Any

from datalink.agents.base import AgentBase

# ---------------------------------------------------------------------------
# JSON contract — keep tight so the UI can render with confidence.
# ---------------------------------------------------------------------------

_REQUIRED_TOP = {"expectation_type", "kwargs", "meta", "sql_preview", "rationale"}
_REQUIRED_META = {"dq_dimension", "severity", "description"}
_VALID_DIMENSIONS = {
    "Completeness",
    "Uniqueness",
    "Timeliness",
    "Accuracy",
    "Consistency",
    "Validity",
}
_VALID_SEVERITIES = {"HIGH", "MEDIUM", "LOW"}

# Curated palette of GX expectation types we actively support — the UI
# review step uses this to gate "weird" types behind an extra confirmation.
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
}


class DqProposerAgent(AgentBase):
    role = "Senior Data Quality Engineer for healthcare claims/UM data"
    goal = (
        "Translate a natural-language data-quality ask into a Great Expectations "
        "expectation JSON object plus the SQL query that would reproduce the "
        "check on the source warehouse. Always pick the most specific built-in "
        "expectation type. Keep kwargs minimal and explicit. Justify the choice "
        "in two sentences max."
    )
    crew_name = "dq_author"

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def execute(self, context: dict[str, Any]) -> dict[str, Any]:
        """Return a proposal dict. ``context`` must include:

            client_id      str
            qualified_table str   "BRONZE_AETNA.RAW_CLAIMS"
            columns        list[{name, type}]
            prompt         str   user's NL ask
            history        list[{role, content}]   prior turns (optional)
            temperature    float                    sampling temperature (default 0.0)
            grounding      list[dict]               RAG hits to inject as
                                                     EXAMPLES — see
                                                     datalink.memory.SuiteHit
                                                     (optional)

        Raises ValueError if the LLM emits malformed JSON or the spec
        violates the contract — the UI surfaces the message verbatim so
        the user can adjust the prompt.
        """
        client_id = context["client_id"]
        table = context["qualified_table"]
        columns = context["columns"]
        prompt = context["prompt"].strip()
        history = context.get("history", [])
        temperature = float(context.get("temperature", 0.0))
        grounding = context.get("grounding", []) or []

        if not prompt:
            raise ValueError("Empty prompt — type what you want the check to enforce.")

        # Build the instruction string (carries the user prompt + chat history
        # + JSON schema reminder + retrieved grounding). The actual prompt text
        # travels INSIDE `instruction` rather than `safe_payload` because
        # `safe_payload` is PHI-guarded with a strict top-level allow-list —
        # adding new keys there requires explicit security review. Embedding
        # the prompt + grounding in `instruction` is the documented safe pattern.
        instruction = self._build_instruction(prompt, table, columns, history, grounding)

        # PHI-safe payload — every top-level key MUST be in
        # datalink.phi.guard.SAFE_FIELDS. We split qualified_table into the
        # two safe components and pass columns as parallel name/type lists.
        # Free-form context (history, table fqn) lives under "context"
        # which is a SAFE_FIELD whose nested keys are scanned for PHI
        # substrings only — not gated by the allow-list.
        schema_name, table_name = table.split(".", 1) if "." in table else ("", table)
        safe_payload: dict[str, Any] = {
            "schema_name": schema_name,
            "table_name": table_name,
            "column_names": [c["name"] for c in columns],
            "data_types": [c["type"] for c in columns],
            "context": {
                "qualified_table": table,
                "client_id": client_id,
                "prior_turns": history[-6:],  # cap chat memory; not PHI
            },
        }

        # Stub fast-path — keep the demo working with no API key. The shape
        # exactly matches the LLM contract so the UI is identical.
        if self._is_stub_llm():
            return self._stub_proposal(table, columns, prompt)

        raw = self._ask_llm(instruction, safe_payload, max_tokens=1500, temperature=temperature)
        return self._parse_and_validate(raw, columns)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _is_stub_llm(self) -> bool:
        # Cheap detection — StubLlm is the only adapter that doesn't do a
        # real network call. Avoids importing the class to keep the dep
        # graph lean.
        return self._llm.__class__.__name__ == "StubLlm"

    def _build_instruction(
        self,
        prompt: str,
        table: str,
        columns: list[dict[str, str]],
        history: list[dict[str, str]],
        grounding: list[dict[str, Any]] | None = None,
    ) -> str:
        cols_pretty = "\n".join(f"  - {c['name']} ({c['type']})" for c in columns)

        history_block = ""
        if history:
            history_block = "\nPRIOR CONVERSATION (most recent last):\n"
            for turn in history[-6:]:
                history_block += f"  {turn['role'].upper()}: {turn['content']}\n"

        # RAG grounding — inject up to N similar past suites as EXAMPLES so
        # the proposal stays consistent with the team's authoring conventions
        # (severity policy, threshold style, dq_dimension preferences).
        # Reads from the ``grounding`` list passed by the caller; each entry
        # carries the suite's humanised source_text and metadata.
        grounding_block = ""
        if grounding:
            lines = ["", "EXAMPLES OF APPROVED SUITES FOR THIS CLIENT (consult for style):"]
            for i, g in enumerate(grounding[:5], start=1):
                lines.append(
                    f"  Example {i} — suite={g.get('suite_name', '')} "
                    f"status={g.get('status', '')} source={g.get('source', '')} "
                    f"distance={g.get('distance', 0):.3f}"
                )
                # Indent the source_text body so the LLM treats it as one block.
                body = (g.get("source_text") or "").splitlines()
                for line in body[:30]:  # cap each example at 30 lines
                    lines.append(f"    {line}")
            grounding_block = "\n".join(lines) + "\n"

        return (
            f"USER ASK:\n  {prompt}\n"
            f"{grounding_block}\n"
            f"You are authoring ONE Great Expectations expectation against table\n"
            f"`{table}`.\n\n"
            f"COLUMNS:\n{cols_pretty}\n"
            f"{history_block}\n"
            f"Return STRICT JSON matching this schema (no prose, no markdown fence):\n"
            f'{{"expectation_type": "<one of GX builtin>",\n'
            f' "kwargs": {{...}},\n'
            f' "meta": {{"dq_dimension":"<Completeness|Uniqueness|Timeliness|Accuracy|Consistency|Validity>",\n'
            f'           "severity":"<HIGH|MEDIUM|LOW>",\n'
            f'           "description":"<one sentence what+why>"}},\n'
            f' "sql_preview": "<a Snowflake SELECT that returns total_rows + violation_count for this rule>",\n'
            f' "rationale": "<two sentences max — why this expectation, why these kwargs>"}}\n\n'
            f"Hard rules:\n"
            f"  * `expectation_type` MUST be a real GX expectation method name.\n"
            f"  * `sql_preview` MUST be safe SELECT only — no DDL, no DML.\n"
            f"  * Use only columns from the COLUMNS list — never invent column names.\n"
            f"  * Keep kwargs minimal — drop None/empty fields.\n"
            f"  * No PHI in any string. Reference fields by name, never by value.\n"
        )

    def _parse_and_validate(self, raw: str, columns: list[dict[str, str]]) -> dict[str, Any]:
        # Strip markdown fences if the model wrapped the JSON.
        clean = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw.strip(), flags=re.MULTILINE)
        try:
            obj = json.loads(clean)
        except json.JSONDecodeError as e:
            raise ValueError(f"Proposer returned non-JSON: {e}. Raw start: {clean[:200]!r}") from e

        if not isinstance(obj, dict):
            raise ValueError(f"Proposer must return a JSON object, got {type(obj).__name__}.")

        missing = _REQUIRED_TOP - obj.keys()
        if missing:
            raise ValueError(f"Proposal missing required fields: {sorted(missing)}.")

        meta = obj.get("meta", {}) or {}
        meta_missing = _REQUIRED_META - meta.keys()
        if meta_missing:
            raise ValueError(f"Proposal.meta missing fields: {sorted(meta_missing)}.")

        if meta["dq_dimension"] not in _VALID_DIMENSIONS:
            raise ValueError(
                f"Invalid dq_dimension {meta['dq_dimension']!r}; "
                f"must be one of {sorted(_VALID_DIMENSIONS)}."
            )
        if meta["severity"] not in _VALID_SEVERITIES:
            raise ValueError(
                f"Invalid severity {meta['severity']!r}; "
                f"must be one of {sorted(_VALID_SEVERITIES)}."
            )

        # Column-name sanity check — refuse hallucinated columns. We're
        # case-insensitive because Snowflake folds, DuckDB lower-cases.
        col_lookup = {c["name"].lower() for c in columns}
        kwargs = obj.get("kwargs", {}) or {}
        for k in ("column", "column_A", "column_B"):
            v = kwargs.get(k)
            if v and str(v).lower() not in col_lookup:
                raise ValueError(
                    f"Proposed kwargs.{k}={v!r} is not a column on this table. "
                    f"Available: {sorted(col_lookup)}"
                )

        sql_preview = str(obj.get("sql_preview", "")).strip()
        sql_lower = sql_preview.lower().lstrip()
        if not sql_lower.startswith("select") and not sql_lower.startswith("with"):
            raise ValueError("sql_preview must be a SELECT/WITH query — no DDL or DML allowed.")
        forbidden = (
            ";drop ",
            " drop ",
            " delete ",
            " update ",
            " insert ",
            " merge ",
            " truncate ",
            " grant ",
            " revoke ",
        )
        sql_padded = " " + sql_lower + " "
        for needle in forbidden:
            if needle in sql_padded:
                raise ValueError(
                    f"sql_preview contains forbidden token {needle.strip()!r}; "
                    f"check the LLM output and retry."
                )

        # Flag unknown expectation types for the UI's "extra confirmation"
        # path — don't block them outright; the user might want bleeding-edge.
        obj["meta"]["known_type"] = obj["expectation_type"] in _KNOWN_EXPECTATION_TYPES
        return obj

    # ------------------------------------------------------------------
    # Stub fallback — heuristic NL mapper. Mirrors the JSON contract.
    # ------------------------------------------------------------------

    def _stub_proposal(
        self, table: str, columns: list[dict[str, str]], prompt: str
    ) -> dict[str, Any]:
        p = prompt.lower()
        # Try to pick a column the user named.
        col_lookup = {c["name"].lower(): c["name"] for c in columns}
        target_col = next(
            (col_lookup[c] for c in col_lookup if c in p),
            columns[0]["name"] if columns else "claim_id",
        )

        if any(w in p for w in ["unique", "duplicate", "no dupes", "primary key"]):
            return {
                "expectation_type": "expect_column_values_to_be_unique",
                "kwargs": {"column": target_col},
                "meta": {
                    "dq_dimension": "Uniqueness",
                    "severity": "HIGH",
                    "description": f"{target_col} must be unique across the table.",
                    "known_type": True,
                },
                "sql_preview": (
                    f"SELECT COUNT(*) AS total_rows, "
                    f"COUNT(*) - COUNT(DISTINCT {target_col}) AS duplicate_count "
                    f"FROM {table};"
                ),
                "rationale": (
                    f"Stub heuristic matched 'unique/duplicate' keywords. "
                    f"Mapped to expect_column_values_to_be_unique on {target_col}."
                ),
            }
        if any(w in p for w in ["null", "missing", "required", "must have"]):
            return {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": target_col, "mostly": 0.99},
                "meta": {
                    "dq_dimension": "Completeness",
                    "severity": "HIGH",
                    "description": f"{target_col} must be populated for at least 99% of rows.",
                    "known_type": True,
                },
                "sql_preview": (
                    f"SELECT COUNT(*) AS total_rows, "
                    f"SUM(CASE WHEN {target_col} IS NULL THEN 1 ELSE 0 END) AS null_count, "
                    f"ROUND(100.0 * SUM(CASE WHEN {target_col} IS NULL THEN 1 ELSE 0 END) "
                    f"/ NULLIF(COUNT(*), 0), 2) AS null_pct "
                    f"FROM {table};"
                ),
                "rationale": (
                    f"Stub heuristic matched 'null/missing/required' keywords. "
                    f"Mapped to not-null with 99% threshold on {target_col}."
                ),
            }
        if any(w in p for w in ["between", "range", "min", "max", "limit"]):
            return {
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {"column": target_col, "min_value": 0, "max_value": 1000000},
                "meta": {
                    "dq_dimension": "Validity",
                    "severity": "MEDIUM",
                    "description": f"{target_col} must fall within [0, 1000000].",
                    "known_type": True,
                },
                "sql_preview": (
                    f"SELECT COUNT(*) AS total_rows, "
                    f"SUM(CASE WHEN {target_col} BETWEEN 0 AND 1000000 THEN 1 ELSE 0 END) AS in_range, "
                    f"SUM(CASE WHEN {target_col} NOT BETWEEN 0 AND 1000000 OR {target_col} IS NULL "
                    f"THEN 1 ELSE 0 END) AS out_of_range "
                    f"FROM {table};"
                ),
                "rationale": (
                    "Stub heuristic matched 'between/range/min/max'. "
                    "Default bounds shown — adjust before approving."
                ),
            }
        # Default — null check for the leading column.
        return {
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {"column": target_col},
            "meta": {
                "dq_dimension": "Completeness",
                "severity": "MEDIUM",
                "description": f"Default: {target_col} should not be null.",
                "known_type": True,
            },
            "sql_preview": (
                f"SELECT COUNT(*) AS total_rows, "
                f"SUM(CASE WHEN {target_col} IS NULL THEN 1 ELSE 0 END) AS null_count "
                f"FROM {table};"
            ),
            "rationale": (
                "Stub default — no specific keyword matched. Edit the prompt "
                "with words like 'unique', 'between', 'in set', 'matches regex' "
                "to steer the proposal, OR set DL_ADAPTERS__LLM__TYPE=anthropic "
                "with ANTHROPIC_API_KEY to engage the real model."
            ),
        }
