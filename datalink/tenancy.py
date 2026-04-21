"""Multi-tenant schema routing — single source of truth for per-client schemas.

Phase 6. Every write that touches a warehouse layer (Bronze, Silver, Gold)
MUST route through `schema_for(client_id, layer)` rather than hard-coding
"BRONZE" / "SILVER_silver" / "SILVER_gold_um". This module is the one
place that knows how to convert a (client_id, layer) tuple into a real
DuckDB / Snowflake / SQL Server schema identifier.

Naming rule (keeps Phase-5.x "default" baseline intact):

    client_id == "default"           →  BRONZE / SILVER_silver / SILVER_gold_um
    client_id == "acme_health"       →  BRONZE_ACME_HEALTH
                                         SILVER_silver_ACME_HEALTH
                                         SILVER_gold_um_ACME_HEALTH

The "SILVER_silver" / "SILVER_gold_um" shape comes from dbt-duckdb's
schema-concatenation rule: `{target.schema}_{model.+schema}` with the
profile's target.schema = "SILVER" and the model's +schema declared in
`dbt/dbt_project.yml`. The `generate_schema_name` macro at
`dbt/macros/generate_schema_name.sql` appends the client suffix inside
dbt so Python and dbt agree on the exact schema name.

CONTROL is deliberately NEVER suffixed — pipeline_control, dq_suites,
audit logs, etc. are **cross-tenant metadata** that the Control Tower UI
reads across every client.
"""

from __future__ import annotations

import re
from enum import StrEnum

from datalink.adapters.protocols import Warehouse
from datalink.logging import get_logger

_log = get_logger(__name__)

DEFAULT_CLIENT = "default"
_CLIENT_ID_RE = re.compile(r"^[a-z][a-z0-9_]{0,62}$")


class Layer(StrEnum):
    """Warehouse layers that are client-scoped.

    Values are the schema-name PREFIX for client_id=='default' (Phase-5.x
    baseline). For other clients, `schema_for()` appends `_<UPPER_CLIENT>`.
    """

    BRONZE = "BRONZE"
    SILVER_DV = "SILVER_silver"
    GOLD_UM = "SILVER_gold_um"
    # CONTROL is cross-tenant by design — NEVER routed through schema_for().
    CONTROL = "CONTROL"


def normalize_client_id(raw: str | None) -> str:
    """Lower-case + validate. Empty / None → DEFAULT_CLIENT.

    Client IDs must be: start with a lower-case letter, contain only
    [a-z0-9_], length 1..63. This matches Postgres / SQL Server /
    Snowflake identifier rules simultaneously and prevents SQL injection
    via client-id interpolation.
    """
    if raw is None or not raw.strip():
        return DEFAULT_CLIENT
    candidate = raw.strip().lower()
    if not _CLIENT_ID_RE.fullmatch(candidate):
        raise ValueError(
            f"invalid client_id {raw!r}: must match ^[a-z][a-z0-9_]{{0,62}}$ "
            "(lower-case letter followed by letters, digits, or underscores)"
        )
    return candidate


def schema_for(client_id: str, layer: Layer) -> str:
    """Resolve a (client_id, layer) tuple to a concrete schema identifier.

    Rule:
      CONTROL              → always "CONTROL"  (cross-tenant metadata)
      layer != CONTROL:
          client=='default' → layer.value (e.g. "BRONZE", "SILVER_silver")
          else              → f"{layer.value}_{UPPER(client_id)}"
    """
    if layer is Layer.CONTROL:
        return layer.value
    cid = normalize_client_id(client_id)
    if cid == DEFAULT_CLIENT:
        return layer.value
    return f"{layer.value}_{cid.upper()}"


def schemas_for_client(client_id: str) -> dict[Layer, str]:
    """All 3 layer-schemas a tenant owns (Bronze, Silver DV, Gold UM).

    CONTROL intentionally excluded — shared across tenants.
    """
    return {
        Layer.BRONZE: schema_for(client_id, Layer.BRONZE),
        Layer.SILVER_DV: schema_for(client_id, Layer.SILVER_DV),
        Layer.GOLD_UM: schema_for(client_id, Layer.GOLD_UM),
    }


def ensure_tenant_schemas(warehouse: Warehouse, client_id: str) -> list[str]:
    """Idempotently create the 3 client-scoped schemas in the warehouse.

    Safe to call on every pipeline start — pure `CREATE SCHEMA IF NOT EXISTS`.
    Returns the list of schema names created/ensured (for logging).
    """
    created = []
    helper = getattr(warehouse, "create_schema_if_not_exists", None)
    for layer, name in schemas_for_client(client_id).items():
        if callable(helper):
            helper(name)
        else:
            warehouse.execute(f"CREATE SCHEMA IF NOT EXISTS {name}")
        created.append(name)
        _log.debug("tenancy.schema_ensured", client_id=client_id, layer=layer.value, schema=name)
    return created
