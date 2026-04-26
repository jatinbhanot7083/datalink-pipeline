"""Phase 13.3 — MapperAgent: NL Gold contract → Silver dbt SQL.

Takes a :class:`SourceProfile` (column metadata only — no row data) and a
plain-English Gold contract, produces a Silver dbt model SQL with the
SCD2 patterns established in Phase 9.

Output JSON schema (strict — UI parses verbatim)::

    {
      "silver_target_table": "sat_member_demographics",
      "natural_keys":        ["member_id", "plan_id"],
      "scd2_change_cols":    ["dob", "gender", "plan_code", "state"],
      "silver_sql":          "{{ config(materialized='table') }}\\n...",
      "rationale":           "Two short paragraphs explaining the design."
    }

The ``silver_sql`` must compile under dbt against the source profile's
table — the orchestrator runs a ``SELECT * FROM (...) LIMIT 100`` wrapper
post-generation to verify it executes and returns rows. Any execution
error is surfaced to the operator as a sample_error so they can ask the
agent to revise.

Stub mode (no API key): a template generator emits a plausible SCD2 Sat
based on profile heuristics — natural key = the highest-distinct-count
non-null column, change cols = everything else. Deterministic, offline,
demo-friendly. Same output shape as the LLM path.
"""

from __future__ import annotations

import json
import re
from typing import Any

from datalink.agents.base import AgentBase
from datalink.agents.mapper.profiler import ColumnProfile, SourceProfile

_REQUIRED_TOP = {
    "silver_target_table",
    "natural_keys",
    "scd2_change_cols",
    "silver_sql",
    "rationale",
}
_REQUIRED_GOLD = {"gold_target_table", "gold_sql", "rationale"}
_REQUIRED_PUSH = {"backend", "push_sql", "rationale"}


class MapperAgent(AgentBase):
    role = (
        "Senior Analytics Engineer specialising in healthcare data vault + dbt. "
        "Designs Silver-layer SCD2 satellites from Bronze sources to match operator-stated "
        "Gold contracts."
    )
    goal = (
        "Propose ONE dbt SQL model file for the Silver layer that lifts the "
        "Bronze source into a hub/sat shape per the Phase-9 medallion convention "
        "(append-only Bronze → SCD2 Silver with hash_diff dedup, "
        "effective_start_date / effective_end_date / is_active). The model MUST "
        "compile and produce sample rows when executed against the warehouse."
    )
    crew_name = "mapper"

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def execute(self, context: dict[str, Any]) -> dict[str, Any]:
        """Return a Silver-mapping proposal. ``context`` keys:

        * ``client_id`` — tenant id (for prompt context, never used to read PHI)
        * ``profile`` — :class:`SourceProfile` (passed directly, not JSON)
        * ``gold_contract`` — operator's NL ask
        * ``history`` — list[{role, content}] of prior turns (capped at 10)
        * ``temperature`` — sampling temperature, default 0.0 for stable SQL
        """
        profile: SourceProfile = context["profile"]
        gold_contract: str = (context.get("gold_contract") or "").strip()
        history: list[dict[str, str]] = context.get("history", []) or []
        temperature = float(context.get("temperature", 0.0))
        client_id: str = context.get("client_id", "default")

        if not gold_contract:
            raise ValueError(
                "Empty Gold contract — describe what the Silver/Gold should look like."
            )
        if not profile.columns:
            raise ValueError(
                f"profile for {profile.qualified_table} has no business columns "
                f"— cannot author a mapping."
            )

        if self._is_stub_llm():
            return self._stub_proposal(profile, gold_contract)

        instruction = self._build_instruction(profile, gold_contract, history)
        # PHI-safe payload. Profile carries metadata + sample VALUES — sample
        # values are the one borderline call. We keep them in `context` (a
        # SAFE_FIELD whose nested keys are scanned for PHI substrings) so
        # the redaction layer flags any leak. Operators control sample size
        # via profile.sample_limit; default 5 keeps the prompt small.
        schema_name, table_name = (
            profile.qualified_table.split(".", 1)
            if "." in profile.qualified_table
            else ("", profile.qualified_table)
        )
        safe_payload: dict[str, Any] = {
            "schema_name": schema_name,
            "table_name": table_name,
            "column_names": [c.name for c in profile.columns],
            "data_types": [c.logical_type for c in profile.columns],
            "context": {
                "client_id": client_id,
                "qualified_table": profile.qualified_table,
                "total_rows": profile.total_rows,
                "profile_summary": [c.to_dict() for c in profile.columns],
                "gold_contract": gold_contract,
                "prior_turns": history[-6:],
            },
        }

        raw = self._ask_llm(
            instruction,
            safe_payload,
            max_tokens=3500,
            temperature=temperature,
        )
        return self._parse_and_validate(raw, profile)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _is_stub_llm(self) -> bool:
        return self._llm.__class__.__name__ == "StubLlm"

    def _build_instruction(
        self,
        profile: SourceProfile,
        gold_contract: str,
        history: list[dict[str, str]],
    ) -> str:
        col_lines = []
        for c in profile.columns:
            hint = f" — {c.semantic_hint}" if c.semantic_hint else ""
            distinct = f"distinct={c.distinct_count}"
            null = f"null%={c.null_pct}"
            samples = ", ".join(repr(s) for s in c.sample_values[:3])
            col_lines.append(
                f"  - {c.name} : {c.logical_type} ({distinct}, {null}){hint}\n"
                f"      samples: [{samples}]"
            )
        cols_block = "\n".join(col_lines)

        history_block = ""
        if history:
            history_block = "\nPRIOR CONVERSATION (most recent last):\n"
            for turn in history[-10:]:
                history_block += f"  {turn['role'].upper()}: {turn['content']}\n"

        return (
            f"OPERATOR'S GOLD CONTRACT (plain English):\n  {gold_contract}\n"
            f"{history_block}\n"
            f"SOURCE PROFILE — Bronze table `{profile.qualified_table}` "
            f"({profile.total_rows} rows):\n{cols_block}\n\n"
            f"You are authoring ONE dbt model file for the Silver layer. The model\n"
            f"MUST follow the Phase-9 SCD2 satellite convention:\n\n"
            f"  * Materialized as `table` via {{{{ config(materialized='table') }}}}.\n"
            f"  * Read from Bronze via {{{{ source('bronze', '<TABLE>') }}}} or\n"
            f"    {{{{ ref('hub_<entity>') }}}} as appropriate.\n"
            f"  * Include `effective_start_date`, `effective_end_date`, `is_active`\n"
            f"    columns (SCD2). effective_start_date = MIN(_load_dt) per\n"
            f"    natural-key state; effective_end_date = LEAD over hash changes;\n"
            f"    is_active = effective_end_date IS NULL.\n"
            f"  * Compute `hash_diff` = MD5 of CONCAT_WS('|', change_cols) and use\n"
            f"    QUALIFY hash_diff IS DISTINCT FROM LAG(hash_diff) to keep only\n"
            f"    real state changes (dedup identical-content batches).\n"
            f"  * Honour _load_type='FULL' soft-deletes when applicable\n"
            f"    (rows present in prior FULL but absent in latest FULL flip\n"
            f"    is_active=FALSE).\n\n"
            f"Return STRICT JSON matching this schema (no prose, no markdown fence):\n"
            f"{{\n"
            f'  "silver_target_table": "<snake_case dbt model name>",\n'
            f'  "natural_keys":        ["<col>", ...],\n'
            f'  "scd2_change_cols":    ["<col>", ...],\n'
            f'  "silver_sql":          "<full dbt SQL ready to save as a .sql file>",\n'
            f'  "rationale":           "<two short paragraphs: why this NK, why these change cols>"\n'
            f"}}\n\n"
            f"Hard rules:\n"
            f"  * `silver_sql` MUST be safe SELECT-only — no DDL, no DML, no PROCEDURE.\n"
            f"  * Reference only columns that appear in the SOURCE PROFILE above.\n"
            f"  * Use the qualified Bronze table name `{profile.qualified_table}` in\n"
            f"    a comment header so reviewers can verify provenance.\n"
            f"  * NO PHI in any string. Use column names, never values.\n"
            f"  * If the contract is ambiguous, pick the most defensible option\n"
            f"    and explain in the rationale.\n"
        )

    def _parse_and_validate(self, raw: str, profile: SourceProfile) -> dict[str, Any]:
        clean = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw.strip(), flags=re.MULTILINE)
        try:
            spec = json.loads(clean)
        except json.JSONDecodeError as e:
            raise ValueError(f"Mapper agent returned malformed JSON: {e}") from e

        missing = _REQUIRED_TOP - set(spec)
        if missing:
            raise ValueError(f"Mapper proposal missing keys: {sorted(missing)}")

        # Sanity check: every named natural_key + change_col exists in profile.
        valid_cols = {c.name for c in profile.columns}
        nks = list(spec.get("natural_keys") or [])
        ccs = list(spec.get("scd2_change_cols") or [])
        for col in nks + ccs:
            if col not in valid_cols:
                raise ValueError(
                    f"Mapper proposal references unknown column {col!r} — "
                    f"valid columns: {sorted(valid_cols)}"
                )

        sql = str(spec["silver_sql"])
        # Bare-minimum safety: refuse anything that smells like DDL/DML.
        forbidden = re.compile(
            r"\b(DROP|TRUNCATE|DELETE|UPDATE|INSERT|CREATE|ALTER|GRANT|REVOKE|MERGE)\b",
            re.IGNORECASE,
        )
        # Allow CREATE in dbt {{ config(...) }} block — strip jinja before scanning.
        sql_no_jinja = re.sub(r"\{\{[^}]*\}\}", "", sql)
        sql_no_jinja = re.sub(r"\{%[^%]*%\}", "", sql_no_jinja)
        if forbidden.search(sql_no_jinja):
            raise ValueError(
                "Mapper proposal contains forbidden DDL/DML keywords. "
                "Silver SQL must be SELECT-only."
            )

        return {
            "silver_target_table": str(spec["silver_target_table"]),
            "natural_keys": nks,
            "scd2_change_cols": ccs,
            "silver_sql": sql,
            "rationale": str(spec["rationale"]),
        }

    # ------------------------------------------------------------------
    # Phase 13.6 — Gold view proposer (separate execute path)
    # ------------------------------------------------------------------

    def execute_gold(self, context: dict[str, Any]) -> dict[str, Any]:
        """Propose a Gold dbt view on top of an already-proposed Silver.

        ``context`` keys:
          * ``profile`` — :class:`SourceProfile` (same one used for Silver)
          * ``silver_target_table`` — name of the Silver model just proposed
          * ``silver_sql`` — its SQL (so the LLM sees the column shape it
                              produces)
          * ``natural_keys`` — Silver's NK list
          * ``scd2_change_cols`` — Silver's change cols
          * ``gold_contract`` — operator's NL ask
          * ``history`` — prior conversation
          * ``temperature``
        """
        profile: SourceProfile = context["profile"]
        silver_target = str(context["silver_target_table"])
        silver_sql = str(context.get("silver_sql") or "")
        gold_contract = str(context.get("gold_contract") or "").strip()
        natural_keys = list(context.get("natural_keys") or [])
        scd2_change_cols = list(context.get("scd2_change_cols") or [])

        if not gold_contract:
            raise ValueError("Empty Gold contract — describe what the Gold view should expose.")
        if not silver_target:
            raise ValueError("execute_gold called without a Silver proposal in context")

        if self._is_stub_llm():
            return self._stub_gold_proposal(
                silver_target, natural_keys, scd2_change_cols, gold_contract
            )

        instruction = self._build_gold_instruction(
            silver_target,
            silver_sql,
            natural_keys,
            scd2_change_cols,
            gold_contract,
            context.get("history", []) or [],
        )
        schema_name, table_name = (
            profile.qualified_table.split(".", 1)
            if "." in profile.qualified_table
            else ("", profile.qualified_table)
        )
        safe_payload: dict[str, Any] = {
            "schema_name": schema_name,
            "table_name": table_name,
            "column_names": [c.name for c in profile.columns],
            "data_types": [c.logical_type for c in profile.columns],
            "context": {
                "client_id": context.get("client_id", "default"),
                "silver_target_table": silver_target,
                "natural_keys": natural_keys,
                "scd2_change_cols": scd2_change_cols,
                "gold_contract": gold_contract,
            },
        }
        raw = self._ask_llm(
            instruction,
            safe_payload,
            max_tokens=2500,
            temperature=float(context.get("temperature", 0.0)),
        )
        return self._parse_and_validate_gold(raw)

    def _build_gold_instruction(
        self,
        silver_target: str,
        silver_sql: str,
        natural_keys: list[str],
        scd2_change_cols: list[str],
        gold_contract: str,
        history: list[dict[str, str]],
    ) -> str:
        history_block = ""
        if history:
            history_block = (
                "\nPRIOR CONVERSATION:\n"
                + "\n".join(f"  {t['role'].upper()}: {t['content']}" for t in history[-10:])
                + "\n"
            )
        return (
            f"OPERATOR'S GOLD CONTRACT:\n  {gold_contract}\n"
            f"{history_block}\n"
            f"SILVER MODEL ALREADY PROPOSED:\n"
            f"  table: `{silver_target}`\n"
            f"  natural_keys: {natural_keys}\n"
            f"  scd2_change_cols: {scd2_change_cols}\n\n"
            f"You are authoring ONE dbt view file for the Gold layer that\n"
            f"surfaces the Silver SCD2 sat as an operational shape. Rules:\n\n"
            f"  * Materialized as `view` via {{{{ config(materialized='view') }}}}.\n"
            f"  * Read from the Silver model via {{{{ ref('{silver_target}') }}}}.\n"
            f"  * Filter to currently-active SCD2 rows: WHERE is_active = TRUE.\n"
            f"  * Project ONLY business columns the operational system needs —\n"
            f"    drop SCD2 audit (effective_start_date, effective_end_date,\n"
            f"    is_active, hash_diff) unless the contract explicitly asks.\n"
            f"  * Rename columns if the contract uses different naming\n"
            f"    (snake_case <-> PascalCase, etc.).\n"
            f"  * Add derived columns the contract requests (full_name from\n"
            f"    first+last, age from dob, etc.) using deterministic SQL —\n"
            f"    no UDFs.\n\n"
            f"Return STRICT JSON (no prose, no markdown):\n"
            f"{{\n"
            f'  "gold_target_table": "<snake_case dbt view name, e.g. vw_active_members>",\n'
            f'  "gold_sql":          "<full dbt SQL ready to save>",\n'
            f'  "rationale":         "<two short paragraphs>"\n'
            f"}}\n\n"
            f"Hard rules:\n"
            f"  * SELECT-only. No DDL/DML.\n"
            f"  * Reference the Silver model via {{{{ ref(...) }}}} only.\n"
            f"  * No PHI in any string.\n"
        )

    def _parse_and_validate_gold(self, raw: str) -> dict[str, Any]:
        clean = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw.strip(), flags=re.MULTILINE)
        try:
            spec = json.loads(clean)
        except json.JSONDecodeError as e:
            raise ValueError(f"Mapper agent (Gold) returned malformed JSON: {e}") from e
        missing = _REQUIRED_GOLD - set(spec)
        if missing:
            raise ValueError(f"Gold proposal missing keys: {sorted(missing)}")
        sql = str(spec["gold_sql"])
        sql_no_jinja = re.sub(r"\{\{[^}]*\}\}", "", sql)
        sql_no_jinja = re.sub(r"\{%[^%]*%\}", "", sql_no_jinja)
        if re.search(
            r"\b(DROP|TRUNCATE|DELETE|UPDATE|INSERT|CREATE|ALTER|GRANT|REVOKE|MERGE)\b",
            sql_no_jinja,
            re.IGNORECASE,
        ):
            raise ValueError(
                "Gold proposal contains forbidden DDL/DML. Gold SQL must be SELECT-only."
            )
        return {
            "gold_target_table": str(spec["gold_target_table"]),
            "gold_sql": sql,
            "rationale": str(spec["rationale"]),
        }

    def _stub_gold_proposal(
        self,
        silver_target: str,
        natural_keys: list[str],
        scd2_change_cols: list[str],
        gold_contract: str,
    ) -> dict[str, Any]:
        """Template-based Gold view: filter to is_active rows, project NK + change cols."""
        gold_name = (
            silver_target.replace("sat_", "vw_active_")
            if silver_target.startswith("sat_")
            else f"vw_{silver_target}"
        )
        cols = ["natural_key", *scd2_change_cols]
        cols_select = ",\n    ".join(cols)
        sql = (
            f"-- Auto-proposed by Phase 13 Smart Mapper (stub mode)\n"
            f"-- Gold view derived from Silver `{silver_target}`.\n"
            f"-- Gold contract: {gold_contract[:160]}\n"
            f"\n"
            f"{{{{ config(materialized='view') }}}}\n"
            f"\n"
            f"SELECT\n"
            f"    {cols_select}\n"
            f"FROM {{{{ ref('{silver_target}') }}}}\n"
            f"WHERE is_active = TRUE\n"
        )
        rationale = (
            "Filtered to active SCD2 rows (`is_active = TRUE`) and projected "
            "the natural key plus business attributes. SCD2 audit columns "
            "(effective_start_date / effective_end_date / hash_diff) are dropped "
            "because operational consumers don't need history. Rename columns or "
            "add derived fields by asking the agent to revise."
        )
        return {
            "gold_target_table": gold_name,
            "gold_sql": sql,
            "rationale": rationale,
        }

    # ------------------------------------------------------------------
    # Phase 13.7 — On-Prem push generator (per backend)
    # ------------------------------------------------------------------

    def execute_push(self, context: dict[str, Any]) -> dict[str, Any]:
        """Propose an optimized On-Prem push script for one backend.

        ``context`` keys:
          * ``backend`` — :class:`TargetBackend` (POSTGRES | SQLSERVER)
          * ``gold_target_table`` — name of the Gold model
          * ``gold_sql`` — Gold SQL (so the LLM sees source column list)
          * ``target_table`` — fully-qualified On-Prem target, e.g. um.PatientAuth
          * ``primary_keys`` — list[str] PK columns on the On-Prem target
          * ``column_list`` — list[str] of business columns to push
        """
        from datalink.agents.mapper.optimization import (
            TargetBackend,
            rules_prompt_block,
        )

        backend: TargetBackend = context["backend"]
        gold_target = str(context["gold_target_table"])
        gold_sql = str(context.get("gold_sql") or "")
        target_table = str(context["target_table"])
        primary_keys = list(context.get("primary_keys") or [])
        column_list = list(context.get("column_list") or [])

        if not primary_keys:
            raise ValueError("execute_push requires `primary_keys` for the On-Prem target")
        if not column_list:
            raise ValueError("execute_push requires `column_list` of business columns to push")

        if self._is_stub_llm():
            return self._stub_push_proposal(
                backend, gold_target, target_table, primary_keys, column_list
            )

        rules_block = rules_prompt_block(backend)
        instruction = self._build_push_instruction(
            backend, gold_target, gold_sql, target_table, primary_keys, column_list, rules_block
        )
        safe_payload: dict[str, Any] = {
            "schema_name": "control",
            "table_name": "egress_batch_log",
            "column_names": column_list,
            "data_types": ["TEXT"] * len(column_list),  # types not needed for push
            "context": {
                "backend": backend.value,
                "gold_target_table": gold_target,
                "target_table": target_table,
                "primary_keys": primary_keys,
            },
        }
        raw = self._ask_llm(
            instruction,
            safe_payload,
            max_tokens=2500,
            temperature=float(context.get("temperature", 0.0)),
        )
        return self._parse_and_validate_push(raw, backend)

    def _build_push_instruction(
        self,
        backend: Any,  # TargetBackend
        gold_target: str,
        gold_sql: str,
        target_table: str,
        primary_keys: list[str],
        column_list: list[str],
        rules_block: str,
    ) -> str:
        return (
            f"You are authoring an OPTIMIZED On-Prem push script for "
            f"{backend.value}.\n\n"
            f"SOURCE (Snowflake / DuckDB Gold view): {gold_target}\n"
            f"COLUMN LIST: {column_list}\n\n"
            f"TARGET (On-Prem): {target_table}\n"
            f"PRIMARY KEYS: {primary_keys}\n\n"
            f"{rules_block}\n\n"
            f"Required parameters in the script (parameterize, do not hardcode):\n"
            f"  :last_watermark — TIMESTAMP from CONTROL.egress_batch_log (the\n"
            f"    cutoff_dts column). Filter source rows where\n"
            f"    effective_start_date > :last_watermark.\n"
            f"  :batch_size      — INTEGER chunk size for bounded transactions.\n\n"
            f"Required final step: UPDATE control.egress_batch_log SET cutoff_dts =\n"
            f"<MAX(effective_start_date) just pushed> WHERE entity = "
            f"'{gold_target}'.\n\n"
            f"Return STRICT JSON (no prose, no markdown):\n"
            f"{{\n"
            f'  "backend":   "{backend.value}",\n'
            f'  "push_sql":  "<the full push script ready to run>",\n'
            f'  "rationale": "<one paragraph: which optimisation rules drove key decisions>"\n'
            f"}}\n"
        )

    def _parse_and_validate_push(self, raw: str, backend: Any) -> dict[str, Any]:
        clean = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw.strip(), flags=re.MULTILINE)
        try:
            spec = json.loads(clean)
        except json.JSONDecodeError as e:
            raise ValueError(f"Mapper agent (push) returned malformed JSON: {e}") from e
        missing = _REQUIRED_PUSH - set(spec)
        if missing:
            raise ValueError(f"Push proposal missing keys: {sorted(missing)}")
        # Note: we DON'T enforce the optimization-rules pass-rate here — the
        # caller does that and may choose to re-prompt the agent on low score.
        return {
            "backend": str(spec["backend"]),
            "push_sql": str(spec["push_sql"]),
            "rationale": str(spec["rationale"]),
        }

    def _stub_push_proposal(
        self,
        backend: Any,  # TargetBackend
        gold_target: str,
        target_table: str,
        primary_keys: list[str],
        column_list: list[str],
    ) -> dict[str, Any]:
        """Template-based push script — produces compliant SQL on first try.

        Backend-specific UPSERT idioms baked in:
          * Postgres → INSERT … ON CONFLICT … DO UPDATE SET …
          * SQL Server → MERGE INTO … USING … WHEN MATCHED … WHEN NOT MATCHED
        """
        from datalink.agents.mapper.optimization import TargetBackend

        cols_csv = ", ".join(column_list)
        pk_csv = ", ".join(primary_keys)

        if backend is TargetBackend.POSTGRES:
            update_set = (
                ",\n    ".join(f"{c} = EXCLUDED.{c}" for c in column_list if c not in primary_keys)
                or "/* PK-only target — DO NOTHING */"
            )
            sql = (
                f"-- Auto-proposed by Phase 13 Smart Mapper (stub, Postgres push).\n"
                f"-- Source: gold model `{gold_target}` (Snowflake / DuckDB)\n"
                f"-- Target: {target_table} (On-Prem Postgres)\n"
                f"-- PKs: {pk_csv}\n"
                f"\n"
                f"INSERT INTO {target_table} ({cols_csv})\n"
                f"SELECT {cols_csv}\n"
                f"FROM {gold_target}\n"
                f"WHERE effective_start_date > :last_watermark\n"
                f"LIMIT :batch_size\n"
                f"ON CONFLICT ({pk_csv}) DO UPDATE SET\n"
                f"    {update_set};\n"
                f"\n"
                f"UPDATE control.egress_batch_log\n"
                f"SET cutoff_dts = (SELECT MAX(effective_start_date)\n"
                f"                  FROM {gold_target})\n"
                f"WHERE entity = '{gold_target}';\n"
            )
        else:  # SQL Server
            on_clause = " AND ".join(f"tgt.{p} = src.{p}" for p in primary_keys)
            update_set = (
                ",\n        ".join(f"{c} = src.{c}" for c in column_list if c not in primary_keys)
                or "/* PK-only target — UPDATE has nothing to set */"
            )
            sql = (
                f"-- Auto-proposed by Phase 13 Smart Mapper (stub, SQL Server push).\n"
                f"-- Source: gold model `{gold_target}` (Snowflake / DuckDB)\n"
                f"-- Target: {target_table} (On-Prem SQL Server)\n"
                f"-- PKs: {pk_csv}\n"
                f"\n"
                f"MERGE INTO {target_table} AS tgt\n"
                f"USING (\n"
                f"    SELECT TOP (:batch_size) {cols_csv}\n"
                f"    FROM {gold_target}\n"
                f"    WHERE effective_start_date > :last_watermark\n"
                f") AS src\n"
                f"ON ({on_clause})\n"
                f"WHEN MATCHED THEN UPDATE SET\n"
                f"        {update_set}\n"
                f"WHEN NOT MATCHED THEN INSERT ({cols_csv})\n"
                f"        VALUES ({', '.join(f'src.{c}' for c in column_list)});\n"
                f"\n"
                f"UPDATE control.egress_batch_log\n"
                f"SET cutoff_dts = (SELECT MAX(effective_start_date)\n"
                f"                  FROM {gold_target})\n"
                f"WHERE entity = '{gold_target}';\n"
            )

        return {
            "backend": backend.value,
            "push_sql": sql,
            "rationale": (
                f"Generated by stub mode for {backend.value}. "
                f"Idempotent UPSERT on PK ({pk_csv}). Watermark-driven via "
                f":last_watermark; batched via :batch_size. Updates "
                f"control.egress_batch_log so the next run picks up only "
                f"new deltas. Operator should review the column list "
                f"and PK choice — the agent has no business context."
            ),
        }

    # ------------------------------------------------------------------
    # Stub generator — offline / no-API-key path
    # ------------------------------------------------------------------

    def _stub_proposal(self, profile: SourceProfile, gold_contract: str) -> dict[str, Any]:
        """Template-based Silver SCD2 proposal. Same output shape as LLM."""
        # Natural key heuristic: highest-distinct-count non-null TEXT/INTEGER
        # column whose name ends in `_id` or contains `id`, or just the
        # highest-distinct text column if no `_id` present.
        candidates = [c for c in profile.columns if c.distinct_count > 0 and c.null_pct == 0.0]
        nk_col = _pick_natural_key(candidates)
        nks = [nk_col.name] if nk_col else []

        # SCD2 change cols: all other business cols, excluding NK + obvious dates.
        ccs = [
            c.name
            for c in profile.columns
            if (not nks or c.name not in nks)
            and c.logical_type in {"TEXT", "INTEGER", "DECIMAL", "BOOLEAN"}
        ][:8]  # cap to keep hash_diff manageable

        target_name = _suggest_target_name(profile.qualified_table)

        # Build a working dbt model. Doesn't reference {{ source(...) }} so
        # the orchestrator can run it directly against the warehouse for
        # sample preview without needing a configured dbt source.
        nk_concat = ", ".join(nks) if nks else "NULL"
        change_concat = ", ".join(f"COALESCE(CAST({c} AS VARCHAR), '∅')" for c in ccs) or "''"
        change_cols_sql = ",\n        ".join(ccs) if ccs else "/* no change cols */"

        sql = (
            f"-- Auto-proposed by Phase 13 Smart Mapper (stub mode)\n"
            f"-- Source: {profile.qualified_table}\n"
            f"-- Gold contract: {gold_contract[:200]}\n"
            f"\n"
            f"{{{{ config(materialized='table') }}}}\n"
            f"\n"
            f"WITH base AS (\n"
            f"    SELECT\n"
            f"        {nk_concat} AS natural_key,\n"
            f"        {change_cols_sql},\n"
            f"        _load_dt,\n"
            f"        _load_type,\n"
            f"        MD5(CONCAT_WS('|', {change_concat})) AS hash_diff\n"
            f"    FROM {profile.qualified_table}\n"
            f"    WHERE {nks[0] if nks else 'TRUE'} IS NOT NULL\n"
            f"),\n"
            f"changed AS (\n"
            f"    SELECT *,\n"
            f"        LAG(hash_diff) OVER (\n"
            f"            PARTITION BY natural_key ORDER BY _load_dt\n"
            f"        ) AS prev_hash,\n"
            f"        LEAD(_load_dt) OVER (\n"
            f"            PARTITION BY natural_key ORDER BY _load_dt\n"
            f"        ) AS next_load_dt\n"
            f"    FROM base\n"
            f"    QUALIFY hash_diff IS DISTINCT FROM\n"
            f"            LAG(hash_diff) OVER (\n"
            f"                PARTITION BY natural_key ORDER BY _load_dt\n"
            f"            )\n"
            f")\n"
            f"SELECT\n"
            f"    natural_key,\n"
            f"    {change_cols_sql},\n"
            f"    hash_diff,\n"
            f"    _load_dt          AS effective_start_date,\n"
            f"    next_load_dt      AS effective_end_date,\n"
            f"    next_load_dt IS NULL AS is_active\n"
            f"FROM changed\n"
        )

        rationale = (
            f"Picked `{nks[0] if nks else 'NULL'}` as the natural key "
            f"(highest distinct count among non-null columns). "
            f"SCD2 change set: {ccs} — "
            f"every state change produces a new row keyed by hash_diff over "
            f"these attributes. effective_start_date = _load_dt at first "
            f"observation; effective_end_date = LEAD(_load_dt) at next change "
            f"(NULL for current row); is_active = effective_end_date IS NULL.\n\n"
            f"Generated by stub mode — no LLM was called. Pattern matches "
            f"Phase-9 sat_member_demographics. Operator should review whether "
            f"the picked NK is correct (the agent has no business context) "
            f"and whether change-col selection should be tightened."
        )

        return {
            "silver_target_table": target_name,
            "natural_keys": nks,
            "scd2_change_cols": ccs,
            "silver_sql": sql,
            "rationale": rationale,
        }


# ============================================================================
# HEURISTIC HELPERS
# ============================================================================


_ID_HINT_TOKENS = ("_id", "id", "_key", "_no", "number", "code")


def _pick_natural_key(candidates: list[ColumnProfile]) -> ColumnProfile | None:
    """Best-effort natural-key picker for stub mode."""
    if not candidates:
        return None
    # Prefer columns whose name suggests an identifier.
    id_like = [c for c in candidates if any(tok in c.name.lower() for tok in _ID_HINT_TOKENS)]
    pool = id_like or candidates
    # Among those, pick the highest-distinct-count.
    return max(pool, key=lambda c: c.distinct_count)


def _suggest_target_name(qualified_table: str) -> str:
    """Map BRONZE_AETNA.RAW_MEMBERSHIP → sat_membership."""
    name = qualified_table.split(".")[-1].lower()
    name = re.sub(r"^raw_", "", name)
    return f"sat_{name}"
