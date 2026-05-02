"""Phase 15.7 — Data Vault 2.0 Silver dbt model builder.

Generates Hub / Satellite / Link models for the Silver layer from:
  * Bronze catalog fields (the wire-format the vendor sends)
  * Gold schema (the canonical consumption model)
  * Bronze→Gold mappings (transform SQL per Gold column)
  * silver_pattern_recommendations row (HUB_SAT_LINK | NORMALIZED + proposed_silver_shape)

The builder is deterministic — same inputs → same outputs across runs.

Outputs (all returned as plain SQL strings, written to disk by the bridge):

  HUB_<entity>            — business key + hash key + audit columns
                            One per entity in the dataset (Member, Provider, Plan, etc.)

  SAT_<entity>_<group>    — descriptive attributes for an entity, partitioned
                            by domain (DEMOGRAPHICS / ADDRESS / ELIGIBILITY / etc.)
                            One Sat per (entity, change-rate domain).
                            Hash-diff column auto-detects changes between batches.

  LINK_<entity1>_<entity2> — many-to-many relationships
                            (e.g. LINK_MEMBER_PROVIDER for PCP attribution)

NORMALIZED fallback: when Silver pattern recommendation flags DV2 as overkill,
emit a single normalized table with TRY_CAST + business-key not-null guards.
This is the same Phase 15 builder, kept here for the override path.

Hash strategy: SHA-256 over concatenated business keys (for hash_key) or
descriptive attributes (for hash_diff). All hashes are SQL-determinist — no
runtime non-determinism.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

_LOGICAL_TO_SQL: dict[str, str] = {
    "TEXT": "VARCHAR",
    "INTEGER": "INTEGER",
    "DECIMAL": "DECIMAL(20, 4)",
    "DATE": "DATE",
    "TIMESTAMP": "TIMESTAMP",
    "BOOLEAN": "BOOLEAN",
}

# Audit columns appended to Hubs (immutable — load_dt + record_source only).
HUB_AUDIT_COLUMNS: list[tuple[str, str]] = [
    ("_load_dt", "TIMESTAMP"),
    ("_record_source", "VARCHAR"),
]

# Audit columns appended to Satellites (descriptive — change-tracking via hash_diff).
SAT_AUDIT_COLUMNS: list[tuple[str, str]] = [
    ("_load_dt", "TIMESTAMP"),
    ("_record_source", "VARCHAR"),
    ("_hash_diff", "VARCHAR"),  # SHA-256 of the descriptive payload
    ("_batch_id", "VARCHAR"),
]

# Audit columns appended to Links (relationship lineage).
LINK_AUDIT_COLUMNS: list[tuple[str, str]] = [
    ("_load_dt", "TIMESTAMP"),
    ("_record_source", "VARCHAR"),
    ("_batch_id", "VARCHAR"),
]


@dataclass(frozen=True)
class GoldColumnSpec:
    """Subset of Gold column metadata needed by the Silver builder."""

    gold_column_name: str
    logical_type: str
    nullable: bool
    is_business_key: bool
    is_pii: bool
    is_phi: bool
    description: str = ""


@dataclass(frozen=True)
class MappingSpec:
    """Bronze→Gold transform rule."""

    gold_column_name: str
    transform_kind: str
    transform_sql: str
    bronze_source_columns: list[str]


# ---------------------------------------------------------------------------
# Schema-name helpers (consistent with Phase 15 conventions).
# ---------------------------------------------------------------------------


def _bronze_schema(client_id: str) -> str:
    return f"BRONZE_{client_id.upper()}"


def _silver_schema(client_id: str) -> str:
    return f"SILVER_{client_id.upper()}"


def _bronze_table(dataset_code: str) -> str:
    return f"raw_{dataset_code}"


# ---------------------------------------------------------------------------
# DV2 entity inference — when the agent hasn't explicitly proposed a Silver
# shape, derive Hubs/Sats from column name conventions.
# ---------------------------------------------------------------------------


def _infer_hubs_and_sats_from_columns(
    *,
    gold_columns: list[GoldColumnSpec],
    proposed_shape: dict[str, Any] | None,
) -> dict[str, Any]:
    """Pull a structured Hub/Sat/Link plan from the agent's recommendation
    OR derive one from column-name prefixes when the recommendation is empty.

    Returns:
        {
          "hubs": [{"name": "HUB_MEMBER", "business_keys": ["member_id"]}, ...],
          "sats": [{"name": "SAT_MEMBER_DEMOGRAPHICS", "hub": "HUB_MEMBER",
                    "columns": [<gold_column_name>, ...]}],
          "links": [{"name": "LINK_MEMBER_PROVIDER", "hubs": ["HUB_MEMBER","HUB_PROVIDER"]}],
        }
    """
    # Trust the agent's proposal if it's well-formed
    if (
        proposed_shape
        and isinstance(proposed_shape, dict)
        and any(k in proposed_shape for k in ("hubs", "sats", "links"))
    ):
        return {
            "hubs": list(proposed_shape.get("hubs") or []),
            "sats": list(proposed_shape.get("sats") or []),
            "links": list(proposed_shape.get("links") or []),
        }

    # Fallback inference from prefixes
    business_keys = [c for c in gold_columns if c.is_business_key]
    if not business_keys:
        # No flagged BKs — synthesize one HUB based on the dataset and grab
        # all columns into a single SAT_DESCRIPTIVE.
        bk_candidates = [
            c
            for c in gold_columns
            if c.gold_column_name.endswith("_id") or c.gold_column_name == "id"
        ]
        if not bk_candidates:
            bk_candidates = gold_columns[:1]
        primary_entity = bk_candidates[0].gold_column_name.removesuffix("_id") or "entity"
        hub_name = f"HUB_{primary_entity.upper()}"
        sat_name = f"SAT_{primary_entity.upper()}_DESCRIPTIVE"
        return {
            "hubs": [{"name": hub_name, "business_keys": [bk_candidates[0].gold_column_name]}],
            "sats": [
                {
                    "name": sat_name,
                    "hub": hub_name,
                    "columns": [c.gold_column_name for c in gold_columns if not c.is_business_key],
                }
            ],
            "links": [],
        }

    # One Hub per business key, Sat domains by column-prefix grouping
    hubs: list[dict[str, Any]] = [
        {
            "name": f"HUB_{bk.gold_column_name.removesuffix('_id').upper() or 'ENTITY'}",
            "business_keys": [bk.gold_column_name],
        }
        for bk in business_keys
    ]
    primary_hub = str(hubs[0]["name"])
    by_prefix: dict[str, list[str]] = {}
    for c in gold_columns:
        if c.is_business_key:
            continue
        prefix = c.gold_column_name.split("_", 1)[0] if "_" in c.gold_column_name else "general"
        by_prefix.setdefault(prefix, []).append(c.gold_column_name)
    sats: list[dict[str, Any]] = [
        {
            "name": f"SAT_{primary_hub.removeprefix('HUB_')}_{prefix.upper()}",
            "hub": primary_hub,
            "columns": cols,
        }
        for prefix, cols in by_prefix.items()
    ]
    return {"hubs": hubs, "sats": sats, "links": []}


# ---------------------------------------------------------------------------
# Per-Hub / per-Sat / per-Link DBT model rendering
# ---------------------------------------------------------------------------


def _render_hub_dbt(
    *,
    client_id: str,
    dataset_code: str,
    hub_name: str,
    business_keys: list[str],
    bronze_source_for_bk: dict[str, str],
) -> str:
    """Generate dbt model SQL for a Hub.

    Hub holds: hash_key (SHA256 of concatenated BKs) + the BKs themselves
    + audit columns. Immutable across batches; INSERT-only semantics."""
    bronze_schema = _bronze_schema(client_id)
    bronze_table = _bronze_table(dataset_code)
    src_var = f"bronze_{client_id.lower()}"

    # Concatenate BK exprs for hash_key
    hash_inputs = ", ".join(
        f"COALESCE(CAST({bronze_source_for_bk.get(bk, bk)} AS VARCHAR), '')" for bk in business_keys
    )
    hash_expr = f"SHA2_HEX(CONCAT_WS('|', {hash_inputs}), 256)"

    bk_select = ",\n        ".join(
        f"CAST({bronze_source_for_bk.get(bk, bk)} AS VARCHAR) AS {bk}" for bk in business_keys
    )

    return f"""-- Phase 15.7 DV2 Silver — Hub
-- Hub      : {hub_name}
-- Dataset  : {dataset_code}
-- Client   : {client_id}
-- Source   : {bronze_schema}.{bronze_table}
-- Pattern  : Hub (immutable business-key registry)

{{{{ config(
    materialized = 'incremental',
    unique_key   = 'hash_key',
    on_schema_change = 'fail',
    tags = ['datalink', 'phase15', 'dv2', 'silver', 'hub', '{client_id.lower()}', '{dataset_code}']
) }}}}

SELECT
    {hash_expr} AS hash_key,
    {bk_select},
    _load_dt,
    _record_source
FROM {{{{ source('{src_var}', '{bronze_table}') }}}}
WHERE {' AND '.join(f"{bronze_source_for_bk.get(bk, bk)} IS NOT NULL" for bk in business_keys)}
{{% if is_incremental() %}}
  AND {hash_expr} NOT IN (SELECT hash_key FROM {{{{ this }}}})
{{% endif %}}
GROUP BY hash_key, {', '.join(business_keys)}, _load_dt, _record_source
"""


def _render_sat_dbt(
    *,
    client_id: str,
    dataset_code: str,
    sat_name: str,
    hub_name: str,
    descriptive_columns: list[GoldColumnSpec],
    mappings_by_gold_col: dict[str, MappingSpec],
    business_keys: list[str],
    bronze_source_for_bk: dict[str, str],
) -> str:
    """Generate dbt model SQL for a Satellite.

    Sat holds: parent hash_key + descriptive attributes + hash_diff for
    change detection + audit columns. INSERT-only with hash_diff dedup."""
    bronze_schema = _bronze_schema(client_id)
    bronze_table = _bronze_table(dataset_code)
    src_var = f"bronze_{client_id.lower()}"

    hash_inputs = ", ".join(
        f"COALESCE(CAST({bronze_source_for_bk.get(bk, bk)} AS VARCHAR), '')" for bk in business_keys
    )
    hash_key_expr = f"SHA2_HEX(CONCAT_WS('|', {hash_inputs}), 256)"

    select_lines: list[str] = []
    diff_inputs: list[str] = []
    for c in descriptive_columns:
        m = mappings_by_gold_col.get(c.gold_column_name)
        sql_expr = m.transform_sql if m else c.gold_column_name
        sql_type = _LOGICAL_TO_SQL.get(c.logical_type.upper(), "VARCHAR")
        select_lines.append(f"        TRY_CAST(({sql_expr}) AS {sql_type}) AS {c.gold_column_name}")
        diff_inputs.append(f"COALESCE(CAST(({sql_expr}) AS VARCHAR), '')")

    hash_diff_expr = (
        f"SHA2_HEX(CONCAT_WS('|', {', '.join(diff_inputs)}), 256)"
        if diff_inputs
        else "SHA2_HEX('', 256)"
    )
    select_lines_str = ",\n".join(select_lines)
    where_str = " AND ".join(
        f"{bronze_source_for_bk.get(bk, bk)} IS NOT NULL" for bk in business_keys
    )

    return f"""-- Phase 15.7 DV2 Silver — Satellite
-- Sat       : {sat_name}
-- Hub       : {hub_name}
-- Dataset   : {dataset_code}
-- Client    : {client_id}
-- Source    : {bronze_schema}.{bronze_table}
-- Pattern   : Satellite (descriptive, hash-diff change-detected)

{{{{ config(
    materialized = 'incremental',
    unique_key   = ['hash_key', '_hash_diff'],
    on_schema_change = 'fail',
    tags = ['datalink', 'phase15', 'dv2', 'silver', 'sat', '{client_id.lower()}', '{dataset_code}']
) }}}}

WITH bronze AS (
    SELECT *,
        {hash_key_expr} AS hash_key
    FROM {{{{ source('{src_var}', '{bronze_table}') }}}}
)
SELECT
    hash_key,
{select_lines_str},
    _load_dt,
    _record_source,
    _batch_id,
    {hash_diff_expr} AS _hash_diff
FROM bronze
WHERE {where_str}
{{% if is_incremental() %}}
  AND ({hash_diff_expr}) NOT IN (
      SELECT _hash_diff FROM {{{{ this }}}} WHERE hash_key = bronze.hash_key
  )
{{% endif %}}
"""


def _render_link_dbt(
    *,
    client_id: str,
    dataset_code: str,
    link_name: str,
    participating_hubs: list[str],
    business_keys_per_hub: dict[str, list[str]],
    bronze_source_for_bk: dict[str, str],
) -> str:
    """Generate dbt model SQL for a Link table.

    Link holds: link_hash_key (SHA256 of concatenated participant hash_keys)
    + each participant's hash_key + audit columns. INSERT-only on first
    occurrence of the relationship.
    """
    bronze_schema = _bronze_schema(client_id)
    bronze_table = _bronze_table(dataset_code)
    src_var = f"bronze_{client_id.lower()}"

    select_parts: list[str] = []
    hash_inputs: list[str] = []
    where_clauses: list[str] = []
    for hub in participating_hubs:
        bks = business_keys_per_hub.get(hub, [])
        if not bks:
            continue
        hub_hash_inputs = ", ".join(
            f"COALESCE(CAST({bronze_source_for_bk.get(bk, bk)} AS VARCHAR), '')" for bk in bks
        )
        hub_hash_expr = f"SHA2_HEX(CONCAT_WS('|', {hub_hash_inputs}), 256)"
        col_alias = f"{hub.lower()}_hash_key"
        select_parts.append(f"        {hub_hash_expr} AS {col_alias}")
        hash_inputs.append(col_alias)
        where_clauses.extend(f"{bronze_source_for_bk.get(bk, bk)} IS NOT NULL" for bk in bks)

    if not select_parts:
        return f"-- Phase 15.7 DV2 Silver — Link {link_name}\n-- (no resolvable BKs; skipped)\n"

    link_hash_expr = f"SHA2_HEX(CONCAT_WS('|', {', '.join(hash_inputs)}), 256)"
    select_parts_str = ",\n".join(select_parts)
    hash_inputs_str = ", ".join(hash_inputs)
    hubs_str = ", ".join(participating_hubs)
    where_str = " AND ".join(where_clauses)

    return f"""-- Phase 15.7 DV2 Silver — Link
-- Link      : {link_name}
-- Dataset   : {dataset_code}
-- Client    : {client_id}
-- Source    : {bronze_schema}.{bronze_table}
-- Hubs      : {hubs_str}

{{{{ config(
    materialized = 'incremental',
    unique_key   = 'link_hash_key',
    on_schema_change = 'fail',
    tags = ['datalink', 'phase15', 'dv2', 'silver', 'link', '{client_id.lower()}', '{dataset_code}']
) }}}}

WITH bronze AS (
    SELECT *,
{select_parts_str}
    FROM {{{{ source('{src_var}', '{bronze_table}') }}}}
)
SELECT
    {link_hash_expr} AS link_hash_key,
    {hash_inputs_str},
    _load_dt,
    _record_source,
    _batch_id
FROM bronze
WHERE {where_str}
{{% if is_incremental() %}}
  AND {link_hash_expr} NOT IN (SELECT link_hash_key FROM {{{{ this }}}})
{{% endif %}}
"""


# ---------------------------------------------------------------------------
# NORMALIZED Silver fallback — single dbt model with TRY_CAST.
# ---------------------------------------------------------------------------


def _render_normalized_silver_dbt(
    *,
    client_id: str,
    dataset_code: str,
    gold_columns: list[GoldColumnSpec],
    mappings_by_gold_col: dict[str, MappingSpec],
) -> str:
    """When the agent flags DV2 as overkill, emit a single normalized
    Silver dbt model — TRY_CAST + required-not-null filter from Bronze
    via mapping rules."""
    bronze_table = _bronze_table(dataset_code)
    src_var = f"bronze_{client_id.lower()}"

    select_lines: list[str] = []
    where_clauses: list[str] = []
    for c in gold_columns:
        m = mappings_by_gold_col.get(c.gold_column_name)
        sql_expr = m.transform_sql if m else c.gold_column_name
        sql_type = _LOGICAL_TO_SQL.get(c.logical_type.upper(), "VARCHAR")
        select_lines.append(f"    TRY_CAST(({sql_expr}) AS {sql_type}) AS {c.gold_column_name}")
        if not c.nullable:
            where_clauses.append(f"({sql_expr}) IS NOT NULL")

    body = ",\n".join(select_lines)
    where_sql = " AND ".join(where_clauses[:8]) if where_clauses else "1=1"

    return f"""-- Phase 15.7 NORMALIZED Silver (DV2 deemed overkill)
-- Dataset : {dataset_code}
-- Client  : {client_id}

{{{{ config(
    materialized = 'incremental',
    unique_key   = '_record_hash',
    on_schema_change = 'fail',
    tags = ['datalink', 'phase15', 'normalized', 'silver', '{client_id.lower()}', '{dataset_code}']
) }}}}

SELECT
{body},
    _load_dt,
    _record_source,
    _batch_id,
    _record_hash
FROM {{{{ source('{src_var}', '{bronze_table}') }}}}
WHERE {where_sql}
{{% if is_incremental() %}}
  AND _load_dt > (SELECT COALESCE(MAX(_load_dt), '1900-01-01') FROM {{{{ this }}}})
{{% endif %}}
"""


# ---------------------------------------------------------------------------
# Top-level orchestrator — returns a dict of {filename: sql_text} so the
# bridge can write each model file under dbt/models/silver/<client>/<dataset>/.
# ---------------------------------------------------------------------------


def build_dv2_silver_models(
    *,
    client_id: str,
    dataset_code: str,
    gold_columns: list[GoldColumnSpec],
    mappings: list[MappingSpec],
    bronze_columns: list[str],  # the wire-format columns from global_bronze_catalog_fields
    silver_pattern: str = "HUB_SAT_LINK",
    proposed_silver_shape: dict[str, Any] | None = None,
) -> dict[str, str]:
    """Return {filename: SQL} for every Silver dbt model. Filename is the
    relative path under dbt/models/silver/<client>/<dataset>/."""
    if silver_pattern.upper() == "NORMALIZED":
        return {
            f"{dataset_code}_silver.sql": _render_normalized_silver_dbt(
                client_id=client_id,
                dataset_code=dataset_code,
                gold_columns=gold_columns,
                mappings_by_gold_col={m.gold_column_name: m for m in mappings},
            )
        }

    # HUB_SAT_LINK path
    plan = _infer_hubs_and_sats_from_columns(
        gold_columns=gold_columns,
        proposed_shape=proposed_silver_shape,
    )

    # Map Gold business-key column names to Bronze column names via the
    # mapping rules (when present) — fall back to the Gold name itself.
    mappings_by_gold_col = {m.gold_column_name: m for m in mappings}
    bronze_set = {b.lower() for b in bronze_columns}

    def _bronze_for_bk(bk_gold_col: str) -> str:
        m = mappings_by_gold_col.get(bk_gold_col)
        if m and m.bronze_source_columns:
            # Use the first source column for the BK
            first = str(m.bronze_source_columns[0]).lower()
            if first in bronze_set:
                return first
        # Direct: the Gold col is named the same as a Bronze col
        return bk_gold_col

    # Build per-Hub BK lookups
    bk_per_hub: dict[str, list[str]] = {
        h["name"]: list(h.get("business_keys", [])) for h in plan["hubs"]
    }
    bronze_source_for_bk: dict[str, str] = {}
    for bks in bk_per_hub.values():
        for bk in bks:
            bronze_source_for_bk[bk] = _bronze_for_bk(bk)

    # Build column lookup
    cols_by_name = {c.gold_column_name: c for c in gold_columns}

    artifacts: dict[str, str] = {}

    # Hubs
    for hub in plan["hubs"]:
        hub_name = str(hub["name"])
        bks = list(hub.get("business_keys", []))
        if not bks:
            continue
        artifacts[f"{hub_name.lower()}.sql"] = _render_hub_dbt(
            client_id=client_id,
            dataset_code=dataset_code,
            hub_name=hub_name,
            business_keys=bks,
            bronze_source_for_bk=bronze_source_for_bk,
        )

    # Sats
    for sat in plan["sats"]:
        sat_name = str(sat["name"])
        hub_name = str(sat.get("hub") or (plan["hubs"][0]["name"] if plan["hubs"] else ""))
        sat_col_names = [str(c) for c in (sat.get("columns") or [])]
        descriptive_columns = [cols_by_name[n] for n in sat_col_names if n in cols_by_name]
        if not descriptive_columns:
            continue
        bks = bk_per_hub.get(hub_name, [])
        if not bks:
            continue
        artifacts[f"{sat_name.lower()}.sql"] = _render_sat_dbt(
            client_id=client_id,
            dataset_code=dataset_code,
            sat_name=sat_name,
            hub_name=hub_name,
            descriptive_columns=descriptive_columns,
            mappings_by_gold_col=mappings_by_gold_col,
            business_keys=bks,
            bronze_source_for_bk=bronze_source_for_bk,
        )

    # Links
    for link in plan["links"]:
        link_name = str(link["name"])
        participating_hubs = [str(h) for h in (link.get("hubs") or [])]
        if len(participating_hubs) < 2:
            continue
        artifacts[f"{link_name.lower()}.sql"] = _render_link_dbt(
            client_id=client_id,
            dataset_code=dataset_code,
            link_name=link_name,
            participating_hubs=participating_hubs,
            business_keys_per_hub=bk_per_hub,
            bronze_source_for_bk=bronze_source_for_bk,
        )

    if not artifacts:
        # Defensive — if planning produced nothing, fall back to NORMALIZED
        artifacts[f"{dataset_code}_silver.sql"] = _render_normalized_silver_dbt(
            client_id=client_id,
            dataset_code=dataset_code,
            gold_columns=gold_columns,
            mappings_by_gold_col=mappings_by_gold_col,
        )

    return artifacts


# ---------------------------------------------------------------------------
# Bronze DDL — adds the _variant_overflow column convention (Phase 15.7).
# ---------------------------------------------------------------------------


def build_bronze_ddl_with_overflow(
    *,
    client_id: str,
    dataset_code: str,
    bronze_fields: list[dict[str, Any]],
) -> str:
    """Render a CREATE TABLE for the client's Bronze raw landing table,
    appending the mandatory `_variant_overflow VARIANT` column for
    capturing unexpected vendor-supplied columns."""
    schema = _bronze_schema(client_id)
    table = _bronze_table(dataset_code)
    fq = f"{schema}.{table}"

    lines: list[str] = []
    lines.append(
        f"-- Phase 15.7 Pipeline Architect — Bronze landing for {client_id} / {dataset_code}"
    )
    lines.append("-- Generated from CONTROL.global_bronze_catalog_fields")
    lines.append(f"-- Generated at: {datetime.now(UTC).isoformat()}")
    lines.append(f"CREATE TABLE IF NOT EXISTS {fq} (")

    col_lines: list[str] = []
    for f in bronze_fields:
        name = str(f.get("bronze_column_name") or f.get("gold_column_name") or "")
        if not name or name.startswith("_"):
            continue
        sql_type = _LOGICAL_TO_SQL.get((f.get("logical_type") or "TEXT").upper(), "VARCHAR")
        nullability = (
            "NOT NULL" if (f.get("requirement") or "").lower().startswith("req") else "NULL"
        )
        comment_bits = []
        if f.get("is_pii"):
            comment_bits.append("[PII]")
        if f.get("is_phi"):
            comment_bits.append("[PHI]")
        if f.get("is_business_key"):
            comment_bits.append("[BK]")
        comment = " ".join(comment_bits)
        suffix = f"  -- {comment}" if comment else ""
        col_lines.append(f"    {name:<32} {sql_type:<14} {nullability}{suffix}")

    # Phase 15.7 — variant overflow column
    col_lines.append(
        "    -- ── Phase 15.7 variant overflow (any column not in the agreed contract lands here) ──"
    )
    col_lines.append(
        f"    {'_variant_overflow':<32} VARIANT        NULL  -- JSON of unexpected columns; never propagates to Silver/Gold without HITL handshake"
    )

    # Audit columns
    col_lines.append("    -- ── Standard Bronze audit columns ──")
    for n, t in [
        ("_load_dt", "TIMESTAMP"),
        ("_source_file", "VARCHAR"),
        ("_batch_id", "VARCHAR"),
        ("_record_source", "VARCHAR"),
        ("_load_type", "VARCHAR"),
        ("_file_row_number", "BIGINT"),
        ("_record_hash", "VARCHAR"),
    ]:
        col_lines.append(f"    {n:<32} {t}")

    lines.append(",\n".join(col_lines))
    lines.append(");")
    return "\n".join(lines) + "\n"
