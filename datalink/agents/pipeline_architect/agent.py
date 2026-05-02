"""PipelineArchitectAgent — Gold-first multi-tenant pipeline orchestrator.

The Global Gold Catalog (CONTROL.global_gold_catalog_*) is the contract.
This agent's job is to materialise that contract into a *client-specific
pipeline instance* — Bronze schema (anchored to whatever shape the client's
data arrives in), Silver staging, Gold copy, GX expectation suite, Airflow
DAG, and OnPrem routing plan.

Two modes:

  * CLONE — preferred. Take an existing peer client's instance for the
    same dataset and stamp out an identical structure, only the schema
    names + bronze_anchor change.
  * BUILD — fallback. Build from the catalog directly (no peer to copy
    from yet).

DETERMINISTIC PARTS (no LLM):
  * Gold DDL                   — built from catalog fields + overrides
  * Silver dbt SQL             — TRY_CAST + NOT NULL filters
  * Gold dbt SQL               — latest-per-business-key window
  * Airflow DAG                — fixed 5-task chain
  * GX expectation suite       — auto-anchored from catalog metadata
  * Routing plan               — Used-by matrix + client overrides

LLM PARTS (Anthropic Claude, temperature 0):
  * Clone-vs-Build decision narrative (judgment + rationale)
  * Per-override review summary
  * Operator-readable executive summary

PHI boundary: payload is metadata only — dataset_code, field counts,
catalog stats, peer client_ids. NO raw rows ever touch the LLM.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from datalink.agents.base import AgentBase
from datalink.logging import get_logger

from .builders import (
    BRONZE_ANCHOR_DESCRIPTIONS,
    CatalogDeviation,
    ResolvedField,
    RoutingPlanEntry,
    apply_overrides,
    bronze_table_name,
    build_airflow_dag,
    build_gold_dbt_sql,
    build_gold_ddl,
    build_gx_suite_scaffold,
    build_routing_plan,
    build_silver_dbt_sql,
    diff_against_global,
    gold_table_name,
    schema_name,
    silver_table_name,
)

_log = get_logger(__name__)


class DecisionMode(StrEnum):
    CLONE = "CLONE"
    BUILD = "BUILD"


# JSON output contract for the LLM narrative payload.
# The agent computes decision_mode deterministically *before* the LLM call;
# the narrative just explains the decision in plain English.
_REQUIRED_TOP = {
    "rationale",
    "executive_summary",
    "clone_recommendation",
    "override_review",
}


@dataclass
class PipelineProposal:
    """Structured agent output. Combines deterministic artifacts + LLM narrative."""

    client_id: str
    dataset_code: str
    bronze_anchor: str
    decision_mode: DecisionMode
    cloned_from_instance_id: str | None
    bronze_schema: str
    bronze_table: str
    silver_schema: str
    silver_table: str
    gold_schema: str
    gold_table: str
    schedule_cron: str
    catalog_version: int
    resolved_field_count: int
    override_count: int
    deviations: list[CatalogDeviation]
    routing_plan: list[RoutingPlanEntry]
    # Generated artifacts
    gold_ddl: str
    silver_dbt_sql: str
    gold_dbt_sql: str
    airflow_dag_py: str
    gx_suite: dict[str, Any]
    # LLM-generated narrative
    rationale: str = ""
    executive_summary: str = ""
    clone_recommendation: str = ""
    override_review: str = ""
    # Provenance
    proposer_model: str = ""
    tokens_used: int = 0
    duration_ms: int = 0
    temperature: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "client_id": self.client_id,
            "dataset_code": self.dataset_code,
            "bronze_anchor": self.bronze_anchor,
            "decision_mode": self.decision_mode.value,
            "cloned_from_instance_id": self.cloned_from_instance_id,
            "bronze_schema": self.bronze_schema,
            "bronze_table": self.bronze_table,
            "silver_schema": self.silver_schema,
            "silver_table": self.silver_table,
            "gold_schema": self.gold_schema,
            "gold_table": self.gold_table,
            "schedule_cron": self.schedule_cron,
            "catalog_version": self.catalog_version,
            "resolved_field_count": self.resolved_field_count,
            "override_count": self.override_count,
            "deviations": [
                {
                    "gold_column_name": d.gold_column_name,
                    "kind": d.kind,
                    "before": d.before,
                    "after": d.after,
                    "rationale": d.rationale,
                }
                for d in self.deviations
            ],
            "routing_plan": [
                {
                    "downstream_product": r.downstream_product,
                    "target_system": r.target_system,
                    "target_uri": r.target_uri,
                    "is_default": r.is_default,
                    "action": r.action,
                    "rationale": r.rationale,
                }
                for r in self.routing_plan
            ],
            "gold_ddl": self.gold_ddl,
            "silver_dbt_sql": self.silver_dbt_sql,
            "gold_dbt_sql": self.gold_dbt_sql,
            "airflow_dag_py": self.airflow_dag_py,
            "gx_suite": self.gx_suite,
            "rationale": self.rationale,
            "executive_summary": self.executive_summary,
            "clone_recommendation": self.clone_recommendation,
            "override_review": self.override_review,
            "proposer_model": self.proposer_model,
            "tokens_used": self.tokens_used,
            "duration_ms": self.duration_ms,
            "temperature": self.temperature,
        }


class PipelineArchitectAgent(AgentBase):
    role = "Senior Multi-Tenant Pipeline Architect for healthcare medallion lakehouses"
    goal = (
        "Decide whether to CLONE an existing peer client's pipeline or BUILD "
        "from scratch for a new (client, dataset) instance. Anchor every "
        "decision against the Global Gold Catalog (the canonical schema "
        "definition for the dataset). Render a concise executive summary, "
        "a clone-vs-build recommendation with rationale, and per-override "
        "review notes. The deterministic artifact builders (Gold DDL, "
        "Silver/Gold dbt, Airflow DAG, GX suite) run independently — your "
        "role is the human-facing judgment narrative on top of them."
    )
    crew_name = "pipeline_architect"

    DEFAULT_TEMPERATURE = 0.0
    DEFAULT_SCHEDULE_CRON = "0 4 * * *"  # 4 AM daily

    # ------------------------------------------------------------------
    # Public API — the bridge layer is responsible for catalog/instance
    # lookups and feeds the agent the pre-resolved metadata.
    # ------------------------------------------------------------------

    def execute(self, context: dict[str, Any]) -> dict[str, Any]:
        """Materialize a PipelineProposal for the (client_id, dataset_code) pair.

        Required ``context`` keys:

            client_id            str
            dataset_code         str
            dataset_display_name str
            bronze_anchor        str        FHIR | X12 | NCPDP | FLAT_FILE | API
            catalog_fields       list[dict] rows from CONTROL.global_bronze_catalog_fields
            catalog_dataset      dict       row from CONTROL.global_bronze_catalog_datasets
            existing_instances   list[dict] peer client instances for the same dataset
            client_overrides     list[dict] rows from CONTROL.client_field_overrides
            default_routing      list[dict] rows from CONTROL.onprem_routing_rules
            client_routing_overrides list[dict] rows from CONTROL.client_routing_overrides

        Optional keys:

            decision_mode        str        'CLONE' | 'BUILD' | 'AUTO' (default AUTO)
            schedule_cron        str        defaults to DEFAULT_SCHEDULE_CRON
            temperature          float      defaults to 0.0
        """
        client_id = str(context["client_id"])
        dataset_code = str(context["dataset_code"])
        bronze_anchor = str(context["bronze_anchor"]).upper()
        catalog_fields: list[dict[str, Any]] = list(context["catalog_fields"])
        catalog_dataset: dict[str, Any] = dict(context["catalog_dataset"])
        existing_instances: list[dict[str, Any]] = list(context.get("existing_instances", []))
        client_overrides: list[dict[str, Any]] = list(context.get("client_overrides", []))
        default_routing: list[dict[str, Any]] = list(context.get("default_routing", []))
        client_routing_overrides: list[dict[str, Any]] = list(
            context.get("client_routing_overrides", [])
        )
        requested_mode = str(context.get("decision_mode", "AUTO")).upper()
        schedule_cron = str(context.get("schedule_cron") or self.DEFAULT_SCHEDULE_CRON)
        temperature = float(context.get("temperature", self.DEFAULT_TEMPERATURE))

        if not catalog_fields:
            raise ValueError(
                f"PipelineArchitect: no catalog fields supplied for "
                f"dataset_code={dataset_code!r}. Did the loader run?"
            )

        # ---- Decision: CLONE vs BUILD ----
        if requested_mode == "CLONE":
            decision = DecisionMode.CLONE
        elif requested_mode == "BUILD":
            decision = DecisionMode.BUILD
        else:
            # AUTO — clone when there's at least one LIVE peer instance
            decision = (
                DecisionMode.CLONE
                if any(inst.get("status") == "LIVE" for inst in existing_instances)
                else DecisionMode.BUILD
            )

        cloned_from_instance_id: str | None = None
        if decision == DecisionMode.CLONE and existing_instances:
            # Prefer a LIVE instance; tie-break by deployed_at desc.
            live_instances = [i for i in existing_instances if i.get("status") == "LIVE"]
            ranked = sorted(
                live_instances or existing_instances,
                key=lambda i: str(i.get("deployed_at") or i.get("created_at") or ""),
                reverse=True,
            )
            if ranked:
                cloned_from_instance_id = ranked[0].get("instance_id")

        # ---- Resolve catalog + overrides ----
        resolved: list[ResolvedField] = apply_overrides(catalog_fields, client_overrides)
        deviations: list[CatalogDeviation] = diff_against_global(catalog_fields, client_overrides)

        # ---- Build deterministic artifacts ----
        gold_ddl = build_gold_ddl(
            client_id=client_id,
            dataset_code=dataset_code,
            resolved_fields=resolved,
            catalog_version=int(catalog_dataset.get("catalog_version") or 1),
        )
        silver_dbt_sql = build_silver_dbt_sql(
            client_id=client_id,
            dataset_code=dataset_code,
            resolved_fields=resolved,
            bronze_anchor=bronze_anchor,
        )
        gold_dbt_sql = build_gold_dbt_sql(
            client_id=client_id,
            dataset_code=dataset_code,
            resolved_fields=resolved,
        )
        gx_suite = build_gx_suite_scaffold(
            client_id=client_id,
            dataset_code=dataset_code,
            resolved_fields=resolved,
        )
        routing_plan = build_routing_plan(
            client_id=client_id,
            dataset_code=dataset_code,
            default_rules=default_routing,
            client_overrides=client_routing_overrides,
        )
        airflow_dag_py = build_airflow_dag(
            client_id=client_id,
            dataset_code=dataset_code,
            bronze_anchor=bronze_anchor,
            schedule_cron=schedule_cron,
            onprem_targets=[
                {
                    "downstream_product": r.downstream_product,
                    "target_system": r.target_system,
                    "target_uri": r.target_uri,
                    "action": r.action,
                }
                for r in routing_plan
                if not r.action.startswith("DISABLED")
            ],
            catalog_version=int(catalog_dataset.get("catalog_version") or 1),
        )

        # ---- LLM narrative (judgment only) ----
        downstream_products = [
            r.downstream_product for r in routing_plan if not r.action.startswith("DISABLED")
        ]
        peer_clients = sorted(
            {str(i.get("client_id")) for i in existing_instances if i.get("client_id")}
        )
        narrative = self._narrative_or_default(
            client_id=client_id,
            dataset_code=dataset_code,
            dataset_display_name=str(
                context.get("dataset_display_name")
                or catalog_dataset.get("display_name")
                or dataset_code
            ),
            bronze_anchor=bronze_anchor,
            decision=decision,
            cloned_from_instance_id=cloned_from_instance_id,
            existing_instances=existing_instances,
            peer_clients=peer_clients,
            catalog_dataset=catalog_dataset,
            override_count=len(client_overrides),
            deviation_kinds=sorted({d.kind for d in deviations}),
            downstream_products=downstream_products,
            schedule_cron=schedule_cron,
            temperature=temperature,
            resolved_field_count=len(resolved),
        )

        proposal = PipelineProposal(
            client_id=client_id,
            dataset_code=dataset_code,
            bronze_anchor=bronze_anchor,
            decision_mode=decision,
            cloned_from_instance_id=cloned_from_instance_id,
            bronze_schema=schema_name("BRONZE", client_id),
            bronze_table=bronze_table_name(dataset_code),
            silver_schema=schema_name("SILVER", client_id),
            silver_table=silver_table_name(dataset_code),
            gold_schema=schema_name("GOLD", client_id),
            gold_table=gold_table_name(dataset_code),
            schedule_cron=schedule_cron,
            catalog_version=int(catalog_dataset.get("catalog_version") or 1),
            resolved_field_count=len(resolved),
            override_count=len(client_overrides),
            deviations=deviations,
            routing_plan=routing_plan,
            gold_ddl=gold_ddl,
            silver_dbt_sql=silver_dbt_sql,
            gold_dbt_sql=gold_dbt_sql,
            airflow_dag_py=airflow_dag_py,
            gx_suite=gx_suite,
            rationale=narrative.get("rationale", ""),
            executive_summary=narrative.get("executive_summary", ""),
            clone_recommendation=narrative.get("clone_recommendation", ""),
            override_review=narrative.get("override_review", ""),
            proposer_model=getattr(self._llm, "model", ""),
            tokens_used=self._tokens_this_run,
            temperature=temperature,
        )
        return proposal.to_dict()

    # ------------------------------------------------------------------
    # LLM judgment — narrative only. Deterministic artifacts already done.
    # ------------------------------------------------------------------

    def _narrative_or_default(
        self,
        *,
        client_id: str,
        dataset_code: str,
        dataset_display_name: str,
        bronze_anchor: str,
        decision: DecisionMode,
        cloned_from_instance_id: str | None,
        existing_instances: list[dict[str, Any]],
        peer_clients: list[str],
        catalog_dataset: dict[str, Any],
        override_count: int,
        deviation_kinds: list[str],
        downstream_products: list[str],
        schedule_cron: str,
        temperature: float,
        resolved_field_count: int,
    ) -> dict[str, str]:
        """Try the LLM; on any failure fall back to a deterministic narrative."""
        instruction = self._build_narrative_prompt(
            client_id=client_id,
            dataset_code=dataset_code,
            dataset_display_name=dataset_display_name,
            bronze_anchor=bronze_anchor,
            decision=decision,
            cloned_from_instance_id=cloned_from_instance_id,
            peer_clients=peer_clients,
            catalog_dataset=catalog_dataset,
            override_count=override_count,
            deviation_kinds=deviation_kinds,
            downstream_products=downstream_products,
            schedule_cron=schedule_cron,
            resolved_field_count=resolved_field_count,
        )
        # PHI-safe — all metadata, no row data.
        safe_payload: dict[str, Any] = {
            "client_id": client_id,
            "dataset_code": dataset_code,
            "dataset_display_name": dataset_display_name,
            "bronze_anchor": bronze_anchor,
            "decision_mode": decision.value,
            "existing_instance_count": len(existing_instances),
            "peer_clients": peer_clients[:20],  # MAX_LIST_LEN guard
            "catalog_field_count": int(catalog_dataset.get("total_fields") or 0),
            "catalog_required_count": int(catalog_dataset.get("required_fields") or 0),
            "catalog_optional_count": int(catalog_dataset.get("optional_fields") or 0),
            "default_frequency": catalog_dataset.get("default_frequency"),
            "category": catalog_dataset.get("category"),
            "downstream_products": downstream_products[:20],
            "override_count": override_count,
            "schedule_cron": schedule_cron,
        }

        try:
            raw = self._ask_llm(
                instruction=instruction,
                safe_payload=safe_payload,
                max_tokens=2048,
                temperature=temperature,
            )
            obj = self._extract_json(raw)
            self._validate_narrative(obj)
            return obj
        except Exception as exc:
            _log.warning(
                "pipeline_architect.llm_narrative_failed_fallback",
                error=f"{type(exc).__name__}: {exc}",
            )
            return self._default_narrative(
                client_id=client_id,
                dataset_display_name=dataset_display_name,
                decision=decision,
                cloned_from_instance_id=cloned_from_instance_id,
                peer_clients=peer_clients,
                bronze_anchor=bronze_anchor,
                resolved_field_count=resolved_field_count,
                override_count=override_count,
                deviation_kinds=deviation_kinds,
                downstream_products=downstream_products,
                schedule_cron=schedule_cron,
            )

    def _build_narrative_prompt(
        self,
        *,
        client_id: str,
        dataset_code: str,
        dataset_display_name: str,
        bronze_anchor: str,
        decision: DecisionMode,
        cloned_from_instance_id: str | None,
        peer_clients: list[str],
        catalog_dataset: dict[str, Any],
        override_count: int,
        deviation_kinds: list[str],
        downstream_products: list[str],
        schedule_cron: str,
        resolved_field_count: int,
    ) -> str:
        cloned_clause = (
            f"cloning from peer instance {cloned_from_instance_id}"
            if cloned_from_instance_id
            else "no peer clone available — building from catalog"
        )
        bronze_desc = BRONZE_ANCHOR_DESCRIPTIONS.get(bronze_anchor, bronze_anchor)
        return f"""You are reviewing a Phase 15 pipeline proposal for a healthcare data platform.

Context (all metadata, no PHI):
  - Client: {client_id}
  - Dataset: {dataset_display_name} (code={dataset_code})
  - Bronze anchor: {bronze_anchor} — {bronze_desc}
  - Decision: {decision.value} ({cloned_clause})
  - Peer clients with the same dataset: {', '.join(peer_clients) if peer_clients else '(none yet)'}
  - Global Gold Catalog field count: {catalog_dataset.get('total_fields', '?')} (resolved after overrides: {resolved_field_count})
  - Override count: {override_count}; kinds: {', '.join(deviation_kinds) if deviation_kinds else '(none)'}
  - Downstream OnPrem products receiving Gold push: {', '.join(downstream_products) if downstream_products else '(none)'}
  - Schedule: {schedule_cron}

Your job: produce a CONCISE, operator-readable narrative covering:
  1. executive_summary (3-4 sentences) — what's about to be deployed and why it's safe.
  2. clone_recommendation (1 paragraph) — was {decision.value} the right call? Reference peer instances when relevant.
  3. override_review (1 paragraph) — if override_count > 0, summarise the deviation_kinds. If 0, say "no client-level overrides; pipeline is structurally identical to global Gold catalog".
  4. rationale (2-3 sentences) — single-line "why this matters" framing for the audit log.

OUTPUT FORMAT — Return ONLY a JSON object:
{{
  "executive_summary": "...",
  "clone_recommendation": "...",
  "override_review": "...",
  "rationale": "..."
}}

RULES:
- No PHI, no row examples, no patient data.
- Plain English, no marketing fluff.
- 4 keys exactly, all strings, all non-empty.
"""

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
            raise ValueError("no JSON object in agent output")
        candidate = s[first : last + 1]
        try:
            parsed = json.loads(candidate)
        except json.JSONDecodeError as e:
            raise ValueError(f"JSON parse error: {e}") from e
        if not isinstance(parsed, dict):
            raise ValueError(f"expected JSON object, got {type(parsed).__name__}")
        return parsed

    @staticmethod
    def _validate_narrative(obj: dict[str, Any]) -> None:
        missing = _REQUIRED_TOP - set(obj.keys())
        if missing:
            raise ValueError(f"narrative missing keys: {sorted(missing)}")
        for k in _REQUIRED_TOP:
            v = obj.get(k)
            if not isinstance(v, str) or not v.strip():
                raise ValueError(f"narrative[{k}] must be a non-empty string")

    @staticmethod
    def _default_narrative(
        *,
        client_id: str,
        dataset_display_name: str,
        decision: DecisionMode,
        cloned_from_instance_id: str | None,
        peer_clients: list[str],
        bronze_anchor: str,
        resolved_field_count: int,
        override_count: int,
        deviation_kinds: list[str],
        downstream_products: list[str],
        schedule_cron: str,
    ) -> dict[str, str]:
        """Deterministic fallback when the LLM is unavailable.

        Identical structure to the LLM output so callers don't branch.
        """
        peers = ", ".join(peer_clients) if peer_clients else "no peer clients yet"
        if decision == DecisionMode.CLONE and cloned_from_instance_id:
            clone_rec = (
                f"CLONE chosen — peer instance {cloned_from_instance_id} is LIVE for this dataset. "
                f"Cloning preserves structural parity with peers ({peers}) so cross-client "
                "validations stay coherent."
            )
        elif decision == DecisionMode.CLONE:
            clone_rec = (
                "CLONE was requested but no peer instance is currently LIVE; falling back to "
                "BUILD from the catalog."
            )
        else:
            clone_rec = (
                f"BUILD chosen — no peer client has this dataset LIVE yet "
                f"(peer set: {peers}). The pipeline is built directly from the global "
                "Gold catalog so the next client can clone from this instance."
            )

        if override_count > 0:
            kinds = ", ".join(deviation_kinds) if deviation_kinds else "n/a"
            override_review = (
                f"{override_count} client-level override(s) applied (kinds: {kinds}). "
                f"Deviations are tracked in CONTROL.client_field_overrides and visible "
                "in the deviation diff panel."
            )
        else:
            override_review = (
                "No client-level overrides — the pipeline is structurally identical "
                "to the global Gold catalog."
            )

        return {
            "executive_summary": (
                f"Deploying {dataset_display_name} pipeline for {client_id}: "
                f"{decision.value} mode, Bronze anchor {bronze_anchor}, "
                f"{resolved_field_count} resolved Gold columns, schedule {schedule_cron}, "
                f"OnPrem routing to {len(downstream_products)} downstream product(s)."
            ),
            "clone_recommendation": clone_rec,
            "override_review": override_review,
            "rationale": (
                f"Materialising the global {dataset_display_name} catalog into a client-specific "
                f"copy — Gold structure stays uniform across {client_id} and peers, only the "
                f"Bronze landing shape ({bronze_anchor}) varies."
            ),
        }
