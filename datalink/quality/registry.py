"""DQ Control Plane — multi-tenant, version-tracked expectation suite registry.

Phase 5.8. Replaces the "static Python function → ExpectationSuite" pattern
with a DB-backed store so DQ analysts (non-devs) can author and review
suites via the Streamlit Control Tower UI.

State machine:

    DRAFT ──submit──→ PENDING_REVIEW ──approve──→ APPROVED ──activate──→ LIVE
      ↑                     │                                             │
      │                     └──request_changes──→ DRAFT                  │
      │                                                                   │
      └────────────── any state ─────────── archived when replaced ───────┘

Invariants:
  * At most ONE LIVE row per (client_id, suite_name) — enforced in code.
  * Every state transition is audit-logged in CONTROL.dq_suite_audit_log.
  * `source` column traces origin: 'baseline_python' (seeded from the
    hand-coded suite files at Phase-5.7), 'ui' (authored via Streamlit),
    'agent' (drafted by ExpectationAuthorAgent, needs human review).

Suite JSON shape stored in `expectations` column:
  [
    {
      "expectation_type": "expect_column_values_to_not_be_null",
      "kwargs": {"column": "claim_id"},
      "meta": {
        "dq_dimension": "Completeness",   # 1 of 6 from DataQuality_Metrics.docx
        "severity":     "HIGH",            # HIGH | MEDIUM | LOW
        "description":  "Natural-key integrity"
      }
    },
    ...
  ]
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from datalink.adapters.protocols import Warehouse
from datalink.logging import get_logger
from datalink.quality.control import CONTROL_SCHEMA

_log = get_logger(__name__)


class SuiteStatus(StrEnum):
    DRAFT = "DRAFT"
    PENDING_REVIEW = "PENDING_REVIEW"
    APPROVED = "APPROVED"
    LIVE = "LIVE"
    ARCHIVED = "ARCHIVED"
    REJECTED = "REJECTED"


class SuiteSource(StrEnum):
    BASELINE_PYTHON = "baseline_python"
    UI = "ui"
    AGENT = "agent"


class DqDimension(StrEnum):
    """The 6 data-quality dimensions from DataQuality_Metrics.docx."""

    COMPLETENESS = "Completeness"
    UNIQUENESS = "Uniqueness"
    TIMELINESS = "Timeliness"
    ACCURACY = "Accuracy"
    CONSISTENCY = "Consistency"
    VALIDITY = "Validity"


# Legal state transitions. Checked by every action method.
_LEGAL_TRANSITIONS: dict[SuiteStatus, set[SuiteStatus]] = {
    SuiteStatus.DRAFT: {SuiteStatus.PENDING_REVIEW, SuiteStatus.ARCHIVED},
    SuiteStatus.PENDING_REVIEW: {
        SuiteStatus.APPROVED,
        SuiteStatus.DRAFT,  # reviewer requests changes → back to draft
        SuiteStatus.REJECTED,
        SuiteStatus.ARCHIVED,
    },
    SuiteStatus.APPROVED: {SuiteStatus.LIVE, SuiteStatus.ARCHIVED},
    SuiteStatus.LIVE: {SuiteStatus.ARCHIVED},
    SuiteStatus.ARCHIVED: set(),  # terminal
    SuiteStatus.REJECTED: set(),  # terminal
}


@dataclass(frozen=True)
class SuiteVersion:
    """One version of a (client_id, suite_name) suite.

    Phase 6: `source_type` identifies which ingested source the suite
    targets (CLAIMS / MEMBERSHIP / PROVIDER). NULL = stage-aggregate
    (legacy 5.8 row; do not mix with per-source suites).
    """

    suite_id: str
    client_id: str
    suite_name: str
    version: int
    status: SuiteStatus
    expectations: list[dict[str, Any]]
    dq_dimensions: list[str]
    source: SuiteSource
    created_by: str
    created_at: datetime
    source_type: str | None = None
    submitted_at: datetime | None = None
    reviewed_by: str | None = None
    reviewed_at: datetime | None = None
    review_notes: str | None = None
    activated_at: datetime | None = None
    archived_at: datetime | None = None


@dataclass
class SuiteDraft:
    """User-input shape for creating a new DRAFT."""

    client_id: str
    suite_name: str
    expectations: list[dict[str, Any]]
    created_by: str
    source: SuiteSource = SuiteSource.UI
    dq_dimensions: list[str] = field(default_factory=list)
    source_type: str | None = None


# ----------------------------------------------------------------------------


class SuiteRegistry:
    """DB-backed store of versioned expectation suites.

    Every read / write hits CONTROL.dq_suites. Audit log updated on every
    state transition. Fails loudly on illegal transitions or unknown
    (client_id, suite_name, version) tuples.
    """

    def __init__(self, warehouse: Warehouse) -> None:
        self._wh = warehouse

    # ------------------------------------------------------------------
    # READS
    # ------------------------------------------------------------------

    def get_live(self, client_id: str, suite_name: str) -> SuiteVersion | None:
        """Return the single LIVE version, or None."""
        rows = self._wh.query(
            f"SELECT * FROM {CONTROL_SCHEMA}.dq_suites "
            "WHERE client_id = $c AND suite_name = $s AND status = $st",
            {"c": client_id, "s": suite_name, "st": SuiteStatus.LIVE.value},
        )
        if not rows:
            return None
        if len(rows) > 1:
            raise RuntimeError(
                f"Invariant violation: multiple LIVE suites for ({client_id!r}, {suite_name!r})"
            )
        return _row_to_version(rows[0])

    def get_by_id(self, suite_id: str) -> SuiteVersion | None:
        rows = self._wh.query(
            f"SELECT * FROM {CONTROL_SCHEMA}.dq_suites WHERE suite_id = $i",
            {"i": suite_id},
        )
        return _row_to_version(rows[0]) if rows else None

    def list_versions(self, client_id: str, suite_name: str) -> list[SuiteVersion]:
        """All versions for a (client, suite) tuple, newest first."""
        rows = self._wh.query(
            f"SELECT * FROM {CONTROL_SCHEMA}.dq_suites "
            "WHERE client_id = $c AND suite_name = $s "
            "ORDER BY version DESC",
            {"c": client_id, "s": suite_name},
        )
        return [_row_to_version(r) for r in rows]

    def list_pending_reviews(self) -> list[SuiteVersion]:
        """Every suite currently awaiting reviewer approval."""
        rows = self._wh.query(
            f"SELECT * FROM {CONTROL_SCHEMA}.dq_suites "
            "WHERE status = $st ORDER BY submitted_at ASC",
            {"st": SuiteStatus.PENDING_REVIEW.value},
        )
        return [_row_to_version(r) for r in rows]

    def list_clients(self) -> list[str]:
        rows = self._wh.query(
            f"SELECT DISTINCT client_id FROM {CONTROL_SCHEMA}.dq_suites ORDER BY client_id"
        )
        return [r["client_id"] for r in rows]

    def list_suite_names(self, client_id: str) -> list[str]:
        rows = self._wh.query(
            f"SELECT DISTINCT suite_name FROM {CONTROL_SCHEMA}.dq_suites "
            "WHERE client_id = $c ORDER BY suite_name",
            {"c": client_id},
        )
        return [r["suite_name"] for r in rows]

    def list_live_by_source(self, client_id: str, source_type: str) -> list[SuiteVersion]:
        """All LIVE suites for a client filtered by source_type (CLAIMS/MEMBERSHIP/...).

        Phase 6. Powers Executive Dashboard breakdowns and the
        per-source checkpoint router. `source_type` is matched
        case-insensitively (CLAIMS == claims).
        """
        rows = self._wh.query(
            f"SELECT * FROM {CONTROL_SCHEMA}.dq_suites "
            "WHERE client_id = $c AND status = $st "
            "AND UPPER(source_type) = UPPER($srct) "
            "ORDER BY suite_name",
            {"c": client_id, "st": SuiteStatus.LIVE.value, "srct": source_type},
        )
        return [_row_to_version(r) for r in rows]

    # ------------------------------------------------------------------
    # WRITES — state transitions
    # ------------------------------------------------------------------

    def create_draft(self, draft: SuiteDraft) -> str:
        """Start a new DRAFT. Version = max(version for client/suite) + 1."""
        existing = self.list_versions(draft.client_id, draft.suite_name)
        next_version = (existing[0].version + 1) if existing else 1
        suite_id = str(uuid.uuid4())
        self._wh.execute(
            f"INSERT INTO {CONTROL_SCHEMA}.dq_suites "
            "(suite_id, client_id, suite_name, version, status, expectations, "
            " dq_dimensions, source_type, source, created_by, created_at) "
            "VALUES ($id, $c, $s, $v, $st, $e, $d, $srct, $src, $cb, $ts)",
            {
                "id": suite_id,
                "c": draft.client_id,
                "s": draft.suite_name,
                "v": next_version,
                "st": SuiteStatus.DRAFT.value,
                "e": json.dumps(draft.expectations),
                "d": json.dumps(draft.dq_dimensions),
                "srct": draft.source_type,
                "src": draft.source.value,
                "cb": draft.created_by,
                "ts": datetime.now(UTC),
            },
        )
        self._audit(
            suite_id, None, SuiteStatus.DRAFT, draft.created_by, f"created via {draft.source.value}"
        )
        _log.info(
            "dq_suite.draft_created",
            suite_id=suite_id,
            client_id=draft.client_id,
            suite_name=draft.suite_name,
            version=next_version,
            source=draft.source.value,
        )
        return suite_id

    def update_draft(
        self,
        suite_id: str,
        expectations: list[dict[str, Any]],
        dq_dimensions: list[str],
        actor: str,
    ) -> None:
        """Overwrite a DRAFT's expectations. Illegal on non-DRAFT suites."""
        current = self._require(suite_id)
        if current.status is not SuiteStatus.DRAFT:
            raise ValueError(
                f"cannot edit suite {suite_id!r} in status {current.status} — only DRAFT is editable"
            )
        self._wh.execute(
            f"UPDATE {CONTROL_SCHEMA}.dq_suites "
            "SET expectations = $e, dq_dimensions = $d WHERE suite_id = $i",
            {"e": json.dumps(expectations), "d": json.dumps(dq_dimensions), "i": suite_id},
        )
        self._audit(suite_id, SuiteStatus.DRAFT, SuiteStatus.DRAFT, actor, "draft edited")

    def submit_for_review(self, suite_id: str, actor: str) -> None:
        self._transition(
            suite_id,
            SuiteStatus.PENDING_REVIEW,
            actor,
            notes="submitted for review",
            extra_sets={"submitted_at": datetime.now(UTC)},
        )

    def approve(self, suite_id: str, actor: str, notes: str | None = None) -> None:
        """Approve a PENDING_REVIEW → moves to APPROVED (not yet LIVE)."""
        self._transition(
            suite_id,
            SuiteStatus.APPROVED,
            actor,
            notes=notes or "approved",
            extra_sets={
                "reviewed_by": actor,
                "reviewed_at": datetime.now(UTC),
                "review_notes": notes,
            },
        )

    def activate(self, suite_id: str, actor: str) -> None:
        """Promote APPROVED → LIVE. Archives any other LIVE for the same
        (client_id, suite_name). This is the point where a suite becomes
        authoritative and starts gating pipeline runs."""
        current = self._require(suite_id)
        prev_live = self.get_live(current.client_id, current.suite_name)
        if prev_live is not None:
            # Archive the previous LIVE first. Important ordering: we update
            # prev_live's row BEFORE flipping this one to LIVE so the unique-
            # LIVE invariant is never violated mid-transaction.
            self._wh.execute(
                f"UPDATE {CONTROL_SCHEMA}.dq_suites "
                "SET status = $st, archived_at = $ts WHERE suite_id = $i",
                {
                    "st": SuiteStatus.ARCHIVED.value,
                    "ts": datetime.now(UTC),
                    "i": prev_live.suite_id,
                },
            )
            self._audit(
                prev_live.suite_id,
                prev_live.status,
                SuiteStatus.ARCHIVED,
                actor,
                f"archived: replaced by v{current.version}",
            )
        self._transition(
            suite_id,
            SuiteStatus.LIVE,
            actor,
            notes="activated",
            extra_sets={"activated_at": datetime.now(UTC)},
        )

    def reject(self, suite_id: str, actor: str, notes: str) -> None:
        self._transition(
            suite_id,
            SuiteStatus.REJECTED,
            actor,
            notes=notes,
            extra_sets={
                "reviewed_by": actor,
                "reviewed_at": datetime.now(UTC),
                "review_notes": notes,
            },
        )

    def request_changes(self, suite_id: str, actor: str, notes: str) -> None:
        """Reviewer says the draft needs more work. Suite goes PENDING_REVIEW → DRAFT."""
        self._transition(
            suite_id,
            SuiteStatus.DRAFT,
            actor,
            notes=notes,
            extra_sets={
                "reviewed_by": actor,
                "reviewed_at": datetime.now(UTC),
                "review_notes": notes,
            },
        )

    # ------------------------------------------------------------------
    # INTERNAL
    # ------------------------------------------------------------------

    def _require(self, suite_id: str) -> SuiteVersion:
        sv = self.get_by_id(suite_id)
        if sv is None:
            raise KeyError(f"Unknown suite_id: {suite_id!r}")
        return sv

    def _transition(
        self,
        suite_id: str,
        to_status: SuiteStatus,
        actor: str,
        notes: str | None = None,
        extra_sets: dict[str, Any] | None = None,
    ) -> None:
        current = self._require(suite_id)
        if to_status not in _LEGAL_TRANSITIONS.get(current.status, set()):
            raise ValueError(
                f"illegal suite transition {current.status} → {to_status} for {suite_id!r}"
            )
        sets = {"status": to_status.value}
        if extra_sets:
            sets.update(extra_sets)
        set_sql = ", ".join(f"{k} = ${k}" for k in sets)
        params = {**sets, "i": suite_id}
        self._wh.execute(
            f"UPDATE {CONTROL_SCHEMA}.dq_suites SET {set_sql} WHERE suite_id = $i",
            params,
        )
        self._audit(suite_id, current.status, to_status, actor, notes)
        _log.info(
            "dq_suite.transition",
            suite_id=suite_id,
            from_status=current.status.value,
            to_status=to_status.value,
            actor=actor,
        )

    def _audit(
        self,
        suite_id: str,
        from_status: SuiteStatus | None,
        to_status: SuiteStatus,
        actor: str,
        notes: str | None,
    ) -> None:
        self._wh.execute(
            f"INSERT INTO {CONTROL_SCHEMA}.dq_suite_audit_log "
            "(audit_id, suite_id, from_status, to_status, actor, notes) "
            "VALUES ($aid, $sid, $fs, $ts, $a, $n)",
            {
                "aid": str(uuid.uuid4()),
                "sid": suite_id,
                "fs": from_status.value if from_status else None,
                "ts": to_status.value,
                "a": actor,
                "n": notes,
            },
        )


# ----------------------------------------------------------------------------
# Row ↔ dataclass
# ----------------------------------------------------------------------------


def _row_to_version(row: dict[str, Any]) -> SuiteVersion:
    return SuiteVersion(
        suite_id=row["suite_id"],
        client_id=row["client_id"],
        suite_name=row["suite_name"],
        version=int(row["version"]),
        status=SuiteStatus(row["status"]),
        expectations=json.loads(row["expectations"]) if row["expectations"] else [],
        dq_dimensions=json.loads(row["dq_dimensions"]) if row["dq_dimensions"] else [],
        source=SuiteSource(row["source"]),
        created_by=row["created_by"],
        created_at=row["created_at"],
        # source_type is nullable — older 5.8 rows may not have it.
        source_type=row.get("source_type"),
        submitted_at=row.get("submitted_at"),
        reviewed_by=row.get("reviewed_by"),
        reviewed_at=row.get("reviewed_at"),
        review_notes=row.get("review_notes"),
        activated_at=row.get("activated_at"),
        archived_at=row.get("archived_at"),
    )
