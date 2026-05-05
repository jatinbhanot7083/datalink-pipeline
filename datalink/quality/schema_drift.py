"""Phase 11 — schema drift detection and contract management.

Industry-standard handling per ``SchemaDrift.docx`` + my expansion:

  * **Additive** drift (extra columns appended) → ``WARNING``. Log it,
    capture extras into Bronze's ``_extra_attributes`` JSON, lift to
    Silver's ``EXTRA_ATTRIBUTES VARIANT``. Continue the load.
  * **Subtractive** drift (contracted column missing) → ``FATAL``. Halt
    the batch, raise ``SchemaDriftError`` so Bronze ingest's existing
    ``hooks`` machinery pauses or aborts the pipeline.
  * **Destructive** drift (type changed: date → bool, str → int) →
    ``FATAL``. Same halt path.
  * **Reorder** → tolerate. We key by column name, not position.

Three core types:

  * :class:`ColumnContract` — one ``(name, logical_type, nullable)`` row.
  * :class:`SourceContract` — the agreed schema for a
    ``(client_id, source_type)``. Persisted in
    ``CONTROL.source_schema_contracts`` (one ACTIVE row per tuple, prior
    versions archived for audit).
  * :class:`DriftReport` — output of :func:`classify_drift`. Carries
    severity + structured lists for ``added_columns`` /
    ``removed_columns`` / ``type_changes``.

The classifier is a **pure function** — no DB, no I/O — so it can be
unit-tested in isolation. The storage API
(:func:`register_contract` / :func:`get_active_contract` / etc.) hits
the warehouse and is responsible for atomic single-ACTIVE-row
invariants.

Logical types (what we compare on, after normalisation from
warehouse-native types):

    TEXT | INTEGER | DECIMAL | DATE | TIMESTAMP | BOOLEAN

Anything outside that set is passed through verbatim — letting niche
types (``VARIANT``, ``ARRAY``) compare exact-string, which is
intentionally strict.
"""

from __future__ import annotations

import json
import re
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from datalink.adapters.protocols import Warehouse
from datalink.logging import get_logger
from datalink.quality.control import CONTROL_SCHEMA

_log = get_logger(__name__)


# ============================================================================
# CORE TYPES
# ============================================================================


class DriftType(StrEnum):
    """High-level classification of what changed vs the contract."""

    NONE = "NONE"
    ADDITIVE = "ADDITIVE"  # only new columns appended
    SUBTRACTIVE = "SUBTRACTIVE"  # contracted column(s) missing
    DESTRUCTIVE = "DESTRUCTIVE"  # column type(s) changed
    MIXED = "MIXED"  # multiple drift kinds in same batch


class DriftSeverity(StrEnum):
    """How the pipeline should react. Drives Bronze ingest's branch.

    * ``INFO``    — no drift. No-op; nothing to log.
    * ``WARNING`` — additive only. Log + capture extras, continue load.
    * ``FATAL``   — anything subtractive or destructive. Halt batch.
    """

    INFO = "INFO"
    WARNING = "WARNING"
    FATAL = "FATAL"


@dataclass(frozen=True)
class ColumnContract:
    """One column in a contract — its name, logical type, nullability."""

    name: str
    logical_type: str  # see module docstring for the canonical set
    nullable: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {"name": self.name, "logical_type": self.logical_type, "nullable": self.nullable}

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> ColumnContract:
        return cls(
            name=d["name"],
            logical_type=d["logical_type"],
            nullable=bool(d.get("nullable", True)),
        )


@dataclass(frozen=True)
class SourceContract:
    """Versioned schema agreement for a ``(client_id, source_type)``.

    The active version gates Bronze ingest. ``contract_version`` bumps
    on every renegotiation; old versions are kept (``is_active=False``)
    for forensic / replay use.
    """

    contract_id: str
    client_id: str
    source_type: str
    contract_version: int
    columns: tuple[ColumnContract, ...]
    is_active: bool
    created_by: str
    created_at: datetime
    notes: str | None = None

    @property
    def column_names(self) -> set[str]:
        return {c.name for c in self.columns}

    @property
    def by_name(self) -> dict[str, ColumnContract]:
        return {c.name: c for c in self.columns}


@dataclass(frozen=True)
class TypeChange:
    """One type-change drift event."""

    column: str
    expected_type: str
    actual_type: str


@dataclass(frozen=True)
class DriftReport:
    """Output of :func:`classify_drift`. Carries severity for the
    pipeline to act on, plus structured lists for the audit log."""

    drift_type: DriftType
    severity: DriftSeverity
    added_columns: tuple[str, ...] = ()
    removed_columns: tuple[str, ...] = ()
    type_changes: tuple[TypeChange, ...] = ()
    contract_version: int | None = None

    @property
    def is_clean(self) -> bool:
        return self.drift_type is DriftType.NONE

    @property
    def is_fatal(self) -> bool:
        return self.severity is DriftSeverity.FATAL

    def summary(self) -> str:
        """One-line human summary suitable for logs / Slack alerts.

        Lists column names for short lists (≤ 5) so on-call pages name
        the offending columns directly. Long lists fall back to a count
        ("8 added") to keep the line printable.
        """
        if self.is_clean:
            return "no drift"
        bits: list[str] = []

        def _cols(label: str, names: tuple[str, ...]) -> str:
            if not names:
                return ""
            if len(names) <= 5:
                return f"{label}: {', '.join(names)}"
            return f"{len(names)} {label}"

        if self.added_columns:
            bits.append(_cols("added", self.added_columns))
        if self.removed_columns:
            bits.append(_cols("missing", self.removed_columns))
        if self.type_changes:
            if len(self.type_changes) <= 5:
                bits.append(
                    "type-changed: "
                    + ", ".join(
                        f"{t.column} ({t.expected_type}→{t.actual_type})" for t in self.type_changes
                    )
                )
            else:
                bits.append(f"{len(self.type_changes)} type-changed")
        return f"{self.drift_type.value}/{self.severity.value}: {'; '.join(bits)}"


class SchemaDriftError(RuntimeError):
    """Raised by Bronze ingest when classify_drift returns FATAL.

    Carries the full :class:`DriftReport` so the calling pipeline hooks
    can persist it to ``CONTROL.schema_drift_log`` before halting.
    """

    def __init__(self, report: DriftReport, *, client_id: str, source_type: str) -> None:
        super().__init__(f"FATAL schema drift on ({client_id}/{source_type}): {report.summary()}")
        self.report = report
        self.client_id = client_id
        self.source_type = source_type


# ============================================================================
# TYPE NORMALISATION
# ============================================================================

_LOGICAL_TYPES = {"TEXT", "INTEGER", "DECIMAL", "DATE", "TIMESTAMP", "BOOLEAN"}


def normalize_type(raw_type: str) -> str:
    """Map a warehouse-native type string to one of the logical types.

    Strips parameters (``VARCHAR(255)`` → ``VARCHAR``) and applies the
    canonical synonyms. Unknown types pass through verbatim — the
    classifier still works, but exact-string equality is enforced.

    Examples:
        VARCHAR(255)     → TEXT
        STRING           → TEXT
        BIGINT           → INTEGER
        NUMBER(10, 0)    → INTEGER
        NUMBER(18, 2)    → DECIMAL
        TIMESTAMP_NTZ    → TIMESTAMP
        BOOL             → BOOLEAN

    Note on NUMBER: Snowflake's default. We treat NUMBER(p, 0) as
    INTEGER and NUMBER(p, s>0) as DECIMAL. NUMBER without parameters
    defaults to INTEGER (matches Snowflake's NUMBER == NUMBER(38, 0)).
    """
    t = raw_type.strip().upper()
    # Parse "NAME(args)" → name + args.
    m = re.match(r"^([A-Z_]+)\s*(?:\(([^)]*)\))?$", t)
    if not m:
        return t  # weird input — pass through
    base = m.group(1)
    args = (m.group(2) or "").strip()

    if base in {"VARCHAR", "TEXT", "STRING", "CHAR", "NVARCHAR"}:
        return "TEXT"
    if base in {"INTEGER", "INT", "BIGINT", "SMALLINT", "TINYINT", "INT4", "INT8"}:
        return "INTEGER"
    if base == "NUMBER":
        # NUMBER(p, s) — scale > 0 → DECIMAL, else INTEGER.
        if "," in args:
            try:
                _, scale = args.split(",", 1)
                if int(scale.strip()) > 0:
                    return "DECIMAL"
            except (ValueError, IndexError):
                pass
        return "INTEGER"
    if base in {"DECIMAL", "NUMERIC", "FLOAT", "DOUBLE", "REAL", "FLOAT4", "FLOAT8"}:
        return "DECIMAL"
    if base == "DATE":
        return "DATE"
    if base in {"TIMESTAMP", "DATETIME", "TIMESTAMP_NTZ", "TIMESTAMP_TZ", "TIMESTAMP_LTZ"}:
        return "TIMESTAMP"
    if base in {"BOOLEAN", "BOOL"}:
        return "BOOLEAN"
    return base  # unknown — pass through


# ============================================================================
# CLASSIFIER (pure function)
# ============================================================================


def classify_drift(
    actual_columns: list[ColumnContract] | list[dict[str, Any]],
    contract: SourceContract,
    *,
    check_types: bool = True,
) -> DriftReport:
    """Compare actual columns against a contract. Pure function.

    Accepts either a list of :class:`ColumnContract` objects (preferred)
    or a list of dicts with keys ``name`` / ``logical_type`` / ``nullable``
    (convenience for ad-hoc callers).

    Audit columns (``_load_dt`` / ``_source_file`` / ``_batch_id`` /
    ``_record_source`` / ``_load_type`` / ``_file_row_number`` /
    ``_record_hash`` / ``_extra_attributes``) are filtered out of
    ``actual_columns`` before comparison — the contract describes the
    business schema, not the audit envelope Bronze stamps on.

    ``check_types`` — set to ``False`` when the caller only has column
    NAMES (e.g. CSV header pre-load, before any type inference). With
    types disabled, only ADDITIVE and SUBTRACTIVE drift are detected;
    destructive (type-change) drift surfaces later via Silver's CAST
    failures. Defaults to ``True`` for the post-staging classifier path
    where actual types are available.

    The returned report's ``contract_version`` is populated from the
    contract for traceability — the caller persists the report verbatim
    into ``schema_drift_log``.
    """
    # Coerce dict input → ColumnContract.
    actuals: list[ColumnContract] = []
    for c in actual_columns:
        if isinstance(c, ColumnContract):
            actuals.append(c)
        else:
            actuals.append(ColumnContract.from_dict(c))

    # Strip audit envelope.
    actuals = [c for c in actuals if not c.name.startswith("_")]

    actual_by_name = {c.name: c for c in actuals}
    contract_by_name = contract.by_name

    actual_names = set(actual_by_name)
    contract_names = set(contract_by_name)

    added = sorted(actual_names - contract_names)
    removed = sorted(contract_names - actual_names)

    type_changes: list[TypeChange] = []
    if check_types:
        for name in sorted(actual_names & contract_names):
            exp = normalize_type(contract_by_name[name].logical_type)
            got = normalize_type(actual_by_name[name].logical_type)
            if exp != got:
                type_changes.append(TypeChange(column=name, expected_type=exp, actual_type=got))

    # Classify.
    has_added = bool(added)
    has_removed = bool(removed)
    has_type = bool(type_changes)

    if not (has_added or has_removed or has_type):
        return DriftReport(
            drift_type=DriftType.NONE,
            severity=DriftSeverity.INFO,
            contract_version=contract.contract_version,
        )

    # Severity: any subtractive or destructive → FATAL.
    if has_removed or has_type:
        if (has_added and has_removed) or (has_added and has_type) or (has_removed and has_type):
            kind = DriftType.MIXED
        elif has_removed:
            kind = DriftType.SUBTRACTIVE
        else:
            kind = DriftType.DESTRUCTIVE
        return DriftReport(
            drift_type=kind,
            severity=DriftSeverity.FATAL,
            added_columns=tuple(added),
            removed_columns=tuple(removed),
            type_changes=tuple(type_changes),
            contract_version=contract.contract_version,
        )

    # Pure additive — WARNING.
    return DriftReport(
        drift_type=DriftType.ADDITIVE,
        severity=DriftSeverity.WARNING,
        added_columns=tuple(added),
        contract_version=contract.contract_version,
    )


# ============================================================================
# STORAGE API (warehouse-backed)
# ============================================================================


def register_contract(
    warehouse: Warehouse,
    *,
    client_id: str,
    source_type: str,
    columns: list[ColumnContract],
    created_by: str,
    notes: str | None = None,
) -> str:
    """Register a new ACTIVE contract for ``(client_id, source_type)``.

    Atomic semantics:

      1. Look up the existing ACTIVE row (if any).
      2. Bump ``contract_version = max(prev) + 1``.
      3. Mark the previous ACTIVE row ``is_active = FALSE``.
      4. INSERT the new row with ``is_active = TRUE``.

    The two writes happen in sequence (no SAVEPOINT — DuckDB doesn't
    expose nested txns through our adapter); a crash between them leaves
    the table with NO active contract for that tuple, which the caller
    side handles by rejecting Bronze loads (no-contract → fail-closed).

    Returns the new ``contract_id`` (UUID).
    """
    existing = get_active_contract(warehouse, client_id=client_id, source_type=source_type)
    next_version = (existing.contract_version + 1) if existing else 1

    if existing is not None:
        warehouse.execute(
            f"UPDATE {CONTROL_SCHEMA}.source_schema_contracts "
            "SET is_active = FALSE WHERE contract_id = $cid",
            {"cid": existing.contract_id},
        )

    contract_id = str(uuid.uuid4())
    warehouse.execute(
        f"INSERT INTO {CONTROL_SCHEMA}.source_schema_contracts "
        "(contract_id, client_id, source_type, contract_version, columns_json, "
        " is_active, created_by, created_at, notes) "
        "VALUES ($id, $c, $st, $v, $cols, TRUE, $cb, $ts, $n)",
        {
            "id": contract_id,
            "c": client_id,
            "st": source_type,
            "v": next_version,
            "cols": json.dumps([c.to_dict() for c in columns]),
            "cb": created_by,
            "ts": datetime.now(UTC),
            "n": notes,
        },
    )
    _log.info(
        "schema_contract.registered",
        contract_id=contract_id,
        client_id=client_id,
        source_type=source_type,
        version=next_version,
        column_count=len(columns),
    )
    return contract_id


def get_active_contract(
    warehouse: Warehouse,
    *,
    client_id: str,
    source_type: str,
) -> SourceContract | None:
    """Return the ACTIVE contract for ``(client_id, source_type)`` or None.

    Bronze ingest calls this on every batch. None means "no contract
    on file" — Phase 11 policy is **fail-closed**: ingest refuses to
    proceed without a contract. The migration script
    ``scripts/migrate_schema_contracts.py`` backfills contracts for
    every existing client x source_type from current Bronze DDL.
    """
    rows = warehouse.query(
        f"SELECT * FROM {CONTROL_SCHEMA}.source_schema_contracts "
        "WHERE client_id = $c AND source_type = $st AND is_active = TRUE",
        {"c": client_id, "st": source_type},
    )
    if not rows:
        return None
    if len(rows) > 1:
        raise RuntimeError(
            f"Invariant violation: {len(rows)} active contracts for "
            f"({client_id!r}, {source_type!r}) — should be exactly 1"
        )
    return _row_to_contract(rows[0])


def list_contract_versions(
    warehouse: Warehouse,
    *,
    client_id: str,
    source_type: str,
) -> list[SourceContract]:
    """All versions for the tuple, newest first. For the audit page."""
    rows = warehouse.query(
        f"SELECT * FROM {CONTROL_SCHEMA}.source_schema_contracts "
        "WHERE client_id = $c AND source_type = $st "
        "ORDER BY contract_version DESC",
        {"c": client_id, "st": source_type},
    )
    return [_row_to_contract(r) for r in rows]


def log_drift_event(
    warehouse: Warehouse,
    *,
    client_id: str,
    source_type: str,
    source_file: str | None,
    batch_id: str | None,
    report: DriftReport,
    action_taken: str,  # "LOGGED" (continued) | "HALTED" (fatal raise)
    notes: str | None = None,
) -> str:
    """Persist a drift detection to ``CONTROL.schema_drift_log``.

    Always called, regardless of severity. INFO (clean) reports are NOT
    logged — that would dilute the audit table on every successful batch.
    Returns the new ``drift_id``.
    """
    if report.is_clean:
        raise ValueError("log_drift_event called on a clean (no-drift) report")
    drift_id = str(uuid.uuid4())
    warehouse.execute(
        f"INSERT INTO {CONTROL_SCHEMA}.schema_drift_log "
        "(drift_id, detected_at, client_id, source_type, source_file, batch_id, "
        " contract_version, drift_type, severity, added_columns, removed_columns, "
        " type_changes, action_taken, notes) "
        "VALUES ($id, $ts, $c, $st, $sf, $bid, $cv, $dt, $sv, $a, $r, $tc, $act, $n)",
        {
            "id": drift_id,
            "ts": datetime.now(UTC),
            "c": client_id,
            "st": source_type,
            "sf": source_file,
            "bid": batch_id,
            "cv": report.contract_version,
            "dt": report.drift_type.value,
            "sv": report.severity.value,
            "a": json.dumps(list(report.added_columns)),
            "r": json.dumps(list(report.removed_columns)),
            "tc": json.dumps(
                [
                    {"column": tc.column, "expected": tc.expected_type, "actual": tc.actual_type}
                    for tc in report.type_changes
                ]
            ),
            "act": action_taken,
            "n": notes,
        },
    )
    _log.info(
        "schema_drift.logged",
        drift_id=drift_id,
        client_id=client_id,
        source_type=source_type,
        drift_type=report.drift_type.value,
        severity=report.severity.value,
        action_taken=action_taken,
    )
    return drift_id


def list_recent_drift_events(
    warehouse: Warehouse,
    *,
    client_id: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """Read the drift log for the operator UI. Returns raw rows."""
    if client_id:
        return warehouse.query(
            f"SELECT * FROM {CONTROL_SCHEMA}.schema_drift_log "
            "WHERE client_id = $c "
            "ORDER BY detected_at DESC LIMIT $lim",
            {"c": client_id, "lim": limit},
        )
    return warehouse.query(
        f"SELECT * FROM {CONTROL_SCHEMA}.schema_drift_log ORDER BY detected_at DESC LIMIT $lim",
        {"lim": limit},
    )


# ============================================================================
# INTERNAL
# ============================================================================


def _row_to_contract(row: dict[str, Any]) -> SourceContract:
    cols_raw = json.loads(row["columns_json"]) if row.get("columns_json") else []
    return SourceContract(
        contract_id=row["contract_id"],
        client_id=row["client_id"],
        source_type=row["source_type"],
        contract_version=int(row["contract_version"]),
        columns=tuple(ColumnContract.from_dict(c) for c in cols_raw),
        is_active=bool(row["is_active"]),
        created_by=row["created_by"],
        created_at=row["created_at"],
        notes=row.get("notes"),
    )
