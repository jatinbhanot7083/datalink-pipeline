"""Gold schema versioning + compatibility classification — Phase 17.4.

When a Gold dataset's column list changes, we need to know whether the
change is **ADDITIVE** (safe — every existing client keeps working) or
**BREAKING** (downstream impact — clients must opt in to migrate).

Public API:

    from datalink.versioning import gold_schema as gs

    # 1. Classify a proposed change
    result = gs.compute_compatibility(
        warehouse=wh,
        dataset_code="membership",
        from_version=1,
        to_version=2,
    )
    # → CompatibilityReport(class_='BREAKING', additive=[...], breaking=[...])

    # 2. Manage subscriptions
    gs.subscribe_client(wh, client_id="aetna", dataset_code="membership", version=1)
    gs.list_subscriptions(wh, dataset_code="membership")
    gs.start_migration(wh, client_id="aetna", target_version=2,
                       window_days=30)

Compatibility rules (industry-standard semver-for-data):

  ADDITIVE — safe, every consumer continues working
    * New nullable column added (with default or NULL backfill)
    * Type widening (VARCHAR(50) → VARCHAR(255), INT → BIGINT)
    * NOT NULL → NULL (loosening)
    * New optional metadata flags

  BREAKING — consumers may break, dual-write window required
    * Column removed
    * Column renamed (treated as remove + add)
    * Type narrowing (VARCHAR(255) → VARCHAR(50), DECIMAL → INT)
    * NULL → NOT NULL (tightening)
    * Business-key changes (anything affecting hash_key derivation)

A version bump that's only ADDITIVE → semver_minor++.
A version bump with any BREAKING change → semver_major++, semver_minor=0.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from datalink.adapters.protocols import Warehouse
from datalink.logging import get_logger
from datalink.quality.control import CONTROL_SCHEMA

_log = get_logger(__name__)

# Semver bump policy: ADDITIVE → minor; any BREAKING → major
ADDITIVE = "ADDITIVE"
BREAKING = "BREAKING"

# Type-widening rules. Direction: from → to is widening (safe).
# Stored as a precedence map: higher number = wider. Going up = additive,
# going down = breaking.
_TYPE_PRECEDENCE: dict[str, int] = {
    "VARCHAR": 100,  # always wider than narrower string types
    "TEXT": 100,
    "STRING": 100,
    "BIGINT": 60,
    "INTEGER": 50,
    "INT": 50,
    "SMALLINT": 40,
    "TINYINT": 30,
    "DOUBLE": 70,
    "FLOAT": 65,
    "DECIMAL": 75,
    "NUMERIC": 75,
    "NUMBER": 75,
    "BOOLEAN": 10,
    "DATE": 20,
    "TIMESTAMP": 30,
    "TIMESTAMP_NTZ": 30,
    "TIMESTAMP_LTZ": 30,
    "TIMESTAMP_TZ": 30,
    "VARIANT": 200,  # VARIANT can hold anything — universal target
    "OBJECT": 200,
    "ARRAY": 200,
}


def _norm_type(sql_type: str) -> str:
    """Strip parameters: 'VARCHAR(50)' → 'VARCHAR'."""
    if not sql_type:
        return ""
    return sql_type.split("(", 1)[0].strip().upper()


def _is_widening(from_type: str, to_type: str) -> bool:
    """True if to_type is a wider/superset type compared to from_type."""
    f = _norm_type(from_type)
    t = _norm_type(to_type)
    if f == t:
        return True
    return _TYPE_PRECEDENCE.get(t, 0) >= _TYPE_PRECEDENCE.get(f, 0)


@dataclass
class FieldDelta:
    """Per-column change between two versions."""

    column_name: str
    change_type: str  # 'ADDED' | 'REMOVED' | 'TYPE_CHANGED' | 'NULLABILITY_CHANGED' | 'BK_CHANGED'
    detail: str
    classification: str  # ADDITIVE | BREAKING


@dataclass
class CompatibilityReport:
    """Output of compute_compatibility(). One report per (from, to) version pair."""

    dataset_code: str
    from_version: int
    to_version: int
    overall: str  # ADDITIVE | BREAKING
    deltas: list[FieldDelta] = field(default_factory=list)

    @property
    def additive_count(self) -> int:
        return sum(1 for d in self.deltas if d.classification == ADDITIVE)

    @property
    def breaking_count(self) -> int:
        return sum(1 for d in self.deltas if d.classification == BREAKING)

    def summary(self) -> str:
        return (
            f"v{self.from_version} → v{self.to_version}: "
            f"{self.overall} "
            f"({self.additive_count} additive, {self.breaking_count} breaking)"
        )


def _ci_get(row: dict[str, Any], key: str, default: Any = None) -> Any:
    """Case-insensitive dict lookup. Snowflake returns UPPERCASE keys; some
    project adapters lowercase them. Try both."""
    if key in row:
        return row[key]
    upper = key.upper()
    if upper in row:
        return row[upper]
    lower = key.lower()
    if lower in row:
        return row[lower]
    return default


def _load_fields(warehouse: Warehouse, dataset_code: str, version: int) -> list[dict[str, Any]]:
    rows = list(
        warehouse.query(
            f"SELECT f.* FROM {CONTROL_SCHEMA}.global_gold_schema_fields f "
            f"JOIN {CONTROL_SCHEMA}.global_gold_schema_datasets d "
            f"  ON d.gold_dataset_id = f.gold_dataset_id "
            f"WHERE d.dataset_code = $ds AND d.version = $v "
            f"ORDER BY f.column_order",
            {"ds": dataset_code, "v": version},
        )
    )
    return rows


def compute_compatibility(
    *,
    warehouse: Warehouse,
    dataset_code: str,
    from_version: int,
    to_version: int,
) -> CompatibilityReport:
    """Diff two versions of a Gold dataset's column list. Classify the change."""
    from_fields = _load_fields(warehouse, dataset_code, from_version)
    to_fields = _load_fields(warehouse, dataset_code, to_version)
    # Case-insensitive column-name extraction — Snowflake returns UPPERCASE
    # keys, some project adapters lowercase them. _ci_get bridges both.
    from_by_name = {_ci_get(f, "gold_column_name"): f for f in from_fields}
    to_by_name = {_ci_get(f, "gold_column_name"): f for f in to_fields}

    deltas: list[FieldDelta] = []

    # Removed columns → BREAKING
    for name in from_by_name.keys() - to_by_name.keys():
        deltas.append(
            FieldDelta(
                column_name=name,
                change_type="REMOVED",
                detail=f"column {name} dropped from v{to_version}",
                classification=BREAKING,
            )
        )

    # Added columns. Nullable + non-business-key → ADDITIVE; otherwise BREAKING.
    for name in to_by_name.keys() - from_by_name.keys():
        f = to_by_name[name]
        is_bk = bool(_ci_get(f, "is_business_key"))
        nullable = bool(_ci_get(f, "nullable"))
        if is_bk:
            cls = BREAKING
            detail = f"new business-key column {name} — affects hash_key derivation"
        elif not nullable:
            cls = BREAKING
            detail = f"new NOT NULL column {name} with no default — back-fill required"
        else:
            cls = ADDITIVE
            detail = f"new nullable column {name} ({_ci_get(f, 'logical_type')})"
        deltas.append(
            FieldDelta(
                column_name=name, change_type="ADDED", detail=detail, classification=cls
            )
        )

    # Columns present in both — check type / nullability / BK changes
    for name in from_by_name.keys() & to_by_name.keys():
        old = from_by_name[name]
        new = to_by_name[name]
        old_type = str(_ci_get(old, "logical_type") or "")
        new_type = str(_ci_get(new, "logical_type") or "")
        old_null = bool(_ci_get(old, "nullable"))
        new_null = bool(_ci_get(new, "nullable"))
        old_bk = bool(_ci_get(old, "is_business_key"))
        new_bk = bool(_ci_get(new, "is_business_key"))

        if old_bk != new_bk:
            deltas.append(
                FieldDelta(
                    column_name=name,
                    change_type="BK_CHANGED",
                    detail=f"business-key flag flipped {old_bk}→{new_bk}",
                    classification=BREAKING,
                )
            )
        if _norm_type(old_type) != _norm_type(new_type):
            wider = _is_widening(old_type, new_type)
            cls = ADDITIVE if wider else BREAKING
            deltas.append(
                FieldDelta(
                    column_name=name,
                    change_type="TYPE_CHANGED",
                    detail=f"type {old_type} → {new_type}",
                    classification=cls,
                )
            )
        if old_null != new_null:
            # NULL → NOT NULL (tightening) is BREAKING
            # NOT NULL → NULL (loosening) is ADDITIVE
            cls = ADDITIVE if new_null else BREAKING
            deltas.append(
                FieldDelta(
                    column_name=name,
                    change_type="NULLABILITY_CHANGED",
                    detail=f"nullable {old_null}→{new_null}",
                    classification=cls,
                )
            )

    overall = BREAKING if any(d.classification == BREAKING for d in deltas) else ADDITIVE
    return CompatibilityReport(
        dataset_code=dataset_code,
        from_version=from_version,
        to_version=to_version,
        overall=overall,
        deltas=deltas,
    )


def list_subscriptions(
    warehouse: Warehouse, *, dataset_code: str | None = None
) -> list[dict[str, Any]]:
    """Return every client subscription, optionally filtered by dataset."""
    if dataset_code:
        rows = list(
            warehouse.query(
                f"SELECT * FROM {CONTROL_SCHEMA}.client_gold_subscriptions "
                f"WHERE dataset_code = $ds ORDER BY client_id",
                {"ds": dataset_code},
            )
        )
    else:
        rows = list(
            warehouse.query(
                f"SELECT * FROM {CONTROL_SCHEMA}.client_gold_subscriptions "
                f"ORDER BY client_id, dataset_code"
            )
        )
    return rows


def subscribe_client(
    warehouse: Warehouse,
    *,
    client_id: str,
    dataset_code: str,
    version: int,
    notes: str = "",
) -> None:
    """Insert or upsert a client's Gold version subscription. Idempotent."""
    warehouse.execute(
        f"MERGE INTO {CONTROL_SCHEMA}.client_gold_subscriptions t "
        f"USING (SELECT $c AS client_id, $d AS dataset_code, $v AS subscribed_version, "
        f"             $n AS notes) s "
        f"ON t.client_id = s.client_id AND t.dataset_code = s.dataset_code "
        f"WHEN MATCHED THEN UPDATE SET subscribed_version = s.subscribed_version, "
        f"                              notes = s.notes, "
        f"                              subscribed_at = CURRENT_TIMESTAMP() "
        f"WHEN NOT MATCHED THEN INSERT (client_id, dataset_code, subscribed_version, notes, "
        f"                              migration_status) "
        f"                       VALUES (s.client_id, s.dataset_code, s.subscribed_version, "
        f"                               s.notes, 'NONE')",
        {"c": client_id, "d": dataset_code, "v": version, "n": notes},
    )
    _log.info(
        "gold_versioning.subscribed",
        client_id=client_id,
        dataset_code=dataset_code,
        version=version,
    )


def start_migration(
    warehouse: Warehouse,
    *,
    client_id: str,
    dataset_code: str,
    target_version: int,
    notes: str = "",
) -> None:
    """Mark a client as in dual-run mode for migrating to ``target_version``.
    Both their current version and target run in parallel until cutover."""
    warehouse.execute(
        f"UPDATE {CONTROL_SCHEMA}.client_gold_subscriptions "
        f"SET migration_target = $t, "
        f"    migration_started_at = CURRENT_TIMESTAMP(), "
        f"    migration_status = 'DUAL_RUN', "
        f"    notes = COALESCE(notes, '') || $n "
        f"WHERE client_id = $c AND dataset_code = $d",
        {"c": client_id, "d": dataset_code, "t": target_version, "n": f" | {notes}" if notes else ""},
    )
    _log.info(
        "gold_versioning.migration_started",
        client_id=client_id,
        dataset_code=dataset_code,
        target_version=target_version,
    )


def complete_migration(
    warehouse: Warehouse,
    *,
    client_id: str,
    dataset_code: str,
) -> None:
    """Cut a client over to migration_target, clearing the dual-run state."""
    rows = list(
        warehouse.query(
            f"SELECT migration_target FROM {CONTROL_SCHEMA}.client_gold_subscriptions "
            f"WHERE client_id = $c AND dataset_code = $d",
            {"c": client_id, "d": dataset_code},
        )
    )
    if not rows or rows[0].get("migration_target") is None:
        raise ValueError(
            f"No active migration to complete for {client_id} / {dataset_code}"
        )
    target = int(rows[0]["migration_target"])
    warehouse.execute(
        f"UPDATE {CONTROL_SCHEMA}.client_gold_subscriptions "
        f"SET subscribed_version = $t, "
        f"    migration_target = NULL, "
        f"    migration_started_at = NULL, "
        f"    migration_status = 'CUTOVER_DONE' "
        f"WHERE client_id = $c AND dataset_code = $d",
        {"c": client_id, "d": dataset_code, "t": target},
    )
    _log.info(
        "gold_versioning.migration_completed",
        client_id=client_id,
        dataset_code=dataset_code,
        new_version=target,
    )
