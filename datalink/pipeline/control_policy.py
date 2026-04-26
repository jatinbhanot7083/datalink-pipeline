"""Phase 12 — pipeline-control threshold policies (config-time HITL).

Two-layer HITL design (per Jatin's requirement #3):

  * **Layer 1 — config-time HITL.** Threshold edits go through the same
    DRAFT → PENDING_REVIEW → APPROVED → LIVE state machine as Phase 10's
    expectation suites. An operator can't unilaterally raise the abort
    threshold from 25% to 75% on a Friday afternoon — the change has to
    pass review. ApprovalMode is reused from ``datalink.quality.registry``
    so the HITL semantics are byte-identical (HITL default; AUTO_APPROVE
    forbidden when editing a LIVE policy).

  * **Layer 2 — runtime is automatic.** Once a policy is LIVE, runtime
    threshold checks (in :mod:`datalink.pipeline.hooks`) fire against it
    automatically — no human in the live path. The human already
    pre-approved the rule at config time. This avoids the operational
    hazard of "every breach pages an operator before any action".

Policy granularity: per-(client_id, pipeline_id). Pipeline IDs match the
ones already used in :mod:`datalink.pipeline.hooks`:
``bronze_ingest`` / ``silver_build`` / ``gold_egress``. Stage-level
sub-granularity is intentionally NOT exposed in v1 — the operator's
mental model is "PIPELINE level", and stage-level config would 5x the
surface area for marginal value.

Back-compat: when no LIVE policy exists for ``(client, pipeline)``,
:func:`get_or_default` returns ``DEFAULT_POLICY``, whose threshold
values match the hardcoded numbers in the pre-Phase-12 ``hooks.py``.
This means tenants that haven't authored a policy yet continue to
behave exactly as today — zero regression risk.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from datalink.adapters.protocols import Warehouse
from datalink.logging import get_logger
from datalink.quality.control import CONTROL_SCHEMA
from datalink.quality.registry import ApprovalMode

_log = get_logger(__name__)


# ============================================================================
# CORE TYPES
# ============================================================================


class PolicyStatus(StrEnum):
    """Lifecycle of a threshold policy. Same shape as SuiteStatus."""

    DRAFT = "DRAFT"
    PENDING_REVIEW = "PENDING_REVIEW"
    APPROVED = "APPROVED"
    LIVE = "LIVE"
    ARCHIVED = "ARCHIVED"
    REJECTED = "REJECTED"


# Schema-drift action vocabulary — what hooks should do when Bronze
# ingest's drift pre-check classifies a fatal drift event. CONTINUE is
# the "log and proceed" mode; the drift itself is FATAL but the operator
# may have a reason to keep going (e.g. testing).
class DriftAction(StrEnum):
    PAUSE = "PAUSE"
    ABORT = "ABORT"
    CONTINUE = "CONTINUE"


_LEGAL_TRANSITIONS: dict[PolicyStatus, set[PolicyStatus]] = {
    PolicyStatus.DRAFT: {PolicyStatus.PENDING_REVIEW, PolicyStatus.ARCHIVED},
    PolicyStatus.PENDING_REVIEW: {
        PolicyStatus.APPROVED,
        PolicyStatus.DRAFT,
        PolicyStatus.REJECTED,
        PolicyStatus.ARCHIVED,
    },
    PolicyStatus.APPROVED: {PolicyStatus.LIVE, PolicyStatus.ARCHIVED},
    PolicyStatus.LIVE: {PolicyStatus.ARCHIVED},
    PolicyStatus.ARCHIVED: set(),
    PolicyStatus.REJECTED: set(),
}


@dataclass(frozen=True)
class ThresholdPolicy:
    """Runtime threshold rules for one ``(client_id, pipeline_id)``.

    Field semantics (in the order :mod:`datalink.pipeline.hooks` checks):

      * ``fail_rate_abort_pct`` — if a checkpoint's fail-pct **exceeds**
        this, the pipeline is **ABORTED** (terminal — operator must
        force-resume to re-arm).
      * ``fail_rate_pause_pct`` — if fail-pct exceeds this but is at-or-
        below the abort threshold, the pipeline is **PAUSED** (reversible
        via Resume in Control Tower).
      * ``schema_drift_action`` — what to do when Bronze drift pre-check
        classifies the drift FATAL. ``PAUSE`` = the legacy default
        (operator unsticks). ``ABORT`` = harder gate (force-resume only).
        ``CONTINUE`` = log + proceed (testing only — DO NOT use in prod).
      * ``row_count_drop_pct`` — Phase 12 placeholder for a future
        Phase-12.5 row-count guard (when N-th batch row count drops
        below X% of moving average). Threshold field is reserved
        already so adding the runtime check later doesn't need a DDL
        change.

    A "synthetic" default policy (``DEFAULT_POLICY``) exists so that
    callers always have a non-None policy to read against. Real policies
    have ``version >= 1``; the default is ``version == 0``.
    """

    policy_id: str
    client_id: str
    pipeline_id: str
    version: int
    status: PolicyStatus
    fail_rate_pause_pct: float
    fail_rate_abort_pct: float
    schema_drift_action: str
    row_count_drop_pct: float
    created_by: str
    created_at: datetime
    submitted_at: datetime | None = None
    reviewed_by: str | None = None
    reviewed_at: datetime | None = None
    review_notes: str | None = None
    activated_at: datetime | None = None
    archived_at: datetime | None = None
    notes: str | None = None

    @property
    def is_default(self) -> bool:
        return self.version == 0

    def thresholds_dict(self) -> dict[str, Any]:
        """Just the threshold fields — for diff display in the UI."""
        return {
            "fail_rate_pause_pct": self.fail_rate_pause_pct,
            "fail_rate_abort_pct": self.fail_rate_abort_pct,
            "schema_drift_action": self.schema_drift_action,
            "row_count_drop_pct": self.row_count_drop_pct,
        }


# Synthetic default policy — values mirror the hardcoded numbers that
# lived in hooks.py pre-Phase-12, so any tenant without a registered
# policy continues to behave exactly as today.
#
#   fail_rate_pause_pct = 0.0     → any failure pauses (Phase 9.4 default)
#   fail_rate_abort_pct = 25.0    → > 25% fail aborts (Phase 9.4 default)
#   schema_drift_action = PAUSE   → matches Phase 11 SchemaDriftError path
#   row_count_drop_pct  = 50.0    → reserved for Phase 12.5; harmless until then
DEFAULT_POLICY = ThresholdPolicy(
    policy_id="__default__",
    client_id="*",
    pipeline_id="*",
    version=0,
    status=PolicyStatus.LIVE,
    fail_rate_pause_pct=0.0,
    fail_rate_abort_pct=25.0,
    schema_drift_action=DriftAction.PAUSE.value,
    row_count_drop_pct=50.0,
    created_by="system",
    created_at=datetime(2024, 1, 1, tzinfo=UTC),
    notes="synthetic default — pre-Phase-12 hardcoded values",
)


@dataclass
class PolicyDraft:
    """User-input shape for creating a new DRAFT policy."""

    client_id: str
    pipeline_id: str
    fail_rate_pause_pct: float
    fail_rate_abort_pct: float
    schema_drift_action: str
    row_count_drop_pct: float
    created_by: str
    notes: str | None = None

    def validate(self) -> None:
        """Sanity checks. Raises ValueError on bad input."""
        if not 0.0 <= self.fail_rate_pause_pct <= 100.0:
            raise ValueError(
                f"fail_rate_pause_pct must be in [0, 100], got {self.fail_rate_pause_pct}"
            )
        if not 0.0 <= self.fail_rate_abort_pct <= 100.0:
            raise ValueError(
                f"fail_rate_abort_pct must be in [0, 100], got {self.fail_rate_abort_pct}"
            )
        if self.fail_rate_pause_pct > self.fail_rate_abort_pct:
            raise ValueError(
                f"fail_rate_pause_pct ({self.fail_rate_pause_pct}) cannot exceed "
                f"fail_rate_abort_pct ({self.fail_rate_abort_pct}) — pause must "
                f"trigger before abort"
            )
        if self.schema_drift_action not in {a.value for a in DriftAction}:
            raise ValueError(
                f"schema_drift_action must be one of "
                f"{[a.value for a in DriftAction]}, got {self.schema_drift_action!r}"
            )
        if not 0.0 <= self.row_count_drop_pct <= 100.0:
            raise ValueError(
                f"row_count_drop_pct must be in [0, 100], got {self.row_count_drop_pct}"
            )


# ============================================================================
# REGISTRY
# ============================================================================


class PipelineControlPolicyRegistry:
    """DB-backed store of versioned threshold policies.

    Mirrors :class:`datalink.quality.registry.SuiteRegistry`'s shape:
    every state transition audit-logged, illegal transitions raise,
    exactly one LIVE row per ``(client_id, pipeline_id)`` invariant
    enforced in code (DuckDB lacks partial-unique indexes).
    """

    def __init__(self, warehouse: Warehouse) -> None:
        self._wh = warehouse

    # ------------------------------------------------------------------
    # READS
    # ------------------------------------------------------------------

    def get_active_policy(self, client_id: str, pipeline_id: str) -> ThresholdPolicy | None:
        """Return the LIVE policy for ``(client, pipeline)`` or None."""
        rows = self._wh.query(
            f"SELECT * FROM {CONTROL_SCHEMA}.pipeline_control_policies "
            "WHERE client_id = $c AND pipeline_id = $p AND status = $st",
            {"c": client_id, "p": pipeline_id, "st": PolicyStatus.LIVE.value},
        )
        if not rows:
            return None
        if len(rows) > 1:
            raise RuntimeError(
                f"Invariant violation: {len(rows)} LIVE policies for "
                f"({client_id!r}, {pipeline_id!r}) — should be exactly 1"
            )
        return _row_to_policy(rows[0])

    def get_or_default(self, client_id: str, pipeline_id: str) -> ThresholdPolicy:
        """Active policy or the synthetic default — never returns None.

        Hot-path callers (hooks.py) use this so the runtime threshold
        check never has to reason about "what if no policy exists".
        """
        return self.get_active_policy(client_id, pipeline_id) or DEFAULT_POLICY

    def get_by_id(self, policy_id: str) -> ThresholdPolicy | None:
        rows = self._wh.query(
            f"SELECT * FROM {CONTROL_SCHEMA}.pipeline_control_policies " "WHERE policy_id = $i",
            {"i": policy_id},
        )
        return _row_to_policy(rows[0]) if rows else None

    def list_versions(self, client_id: str, pipeline_id: str) -> list[ThresholdPolicy]:
        """All versions for the tuple, newest first."""
        rows = self._wh.query(
            f"SELECT * FROM {CONTROL_SCHEMA}.pipeline_control_policies "
            "WHERE client_id = $c AND pipeline_id = $p "
            "ORDER BY version DESC",
            {"c": client_id, "p": pipeline_id},
        )
        return [_row_to_policy(r) for r in rows]

    def list_pending_reviews(self) -> list[ThresholdPolicy]:
        rows = self._wh.query(
            f"SELECT * FROM {CONTROL_SCHEMA}.pipeline_control_policies "
            "WHERE status = $st ORDER BY submitted_at ASC",
            {"st": PolicyStatus.PENDING_REVIEW.value},
        )
        return [_row_to_policy(r) for r in rows]

    def list_all_active(self) -> list[ThresholdPolicy]:
        """Every LIVE policy — for the operator UI's grid view."""
        rows = self._wh.query(
            f"SELECT * FROM {CONTROL_SCHEMA}.pipeline_control_policies "
            "WHERE status = $st "
            "ORDER BY client_id, pipeline_id",
            {"st": PolicyStatus.LIVE.value},
        )
        return [_row_to_policy(r) for r in rows]

    # ------------------------------------------------------------------
    # WRITES — state transitions
    # ------------------------------------------------------------------

    def create_draft(self, draft: PolicyDraft) -> str:
        """Start a new DRAFT. Version = max(version) + 1 for the tuple."""
        draft.validate()
        existing = self.list_versions(draft.client_id, draft.pipeline_id)
        next_version = (existing[0].version + 1) if existing else 1
        policy_id = str(uuid.uuid4())
        self._wh.execute(
            f"INSERT INTO {CONTROL_SCHEMA}.pipeline_control_policies "
            "(policy_id, client_id, pipeline_id, version, status, "
            " fail_rate_pause_pct, fail_rate_abort_pct, schema_drift_action, "
            " row_count_drop_pct, created_by, created_at, notes) "
            "VALUES ($id, $c, $p, $v, $st, $pp, $ap, $sd, $rd, $cb, $ts, $n)",
            {
                "id": policy_id,
                "c": draft.client_id,
                "p": draft.pipeline_id,
                "v": next_version,
                "st": PolicyStatus.DRAFT.value,
                "pp": draft.fail_rate_pause_pct,
                "ap": draft.fail_rate_abort_pct,
                "sd": draft.schema_drift_action,
                "rd": draft.row_count_drop_pct,
                "cb": draft.created_by,
                "ts": datetime.now(UTC),
                "n": draft.notes,
            },
        )
        self._audit(policy_id, None, PolicyStatus.DRAFT, draft.created_by, "draft created")
        _log.info(
            "policy.draft_created",
            policy_id=policy_id,
            client_id=draft.client_id,
            pipeline_id=draft.pipeline_id,
            version=next_version,
        )
        return policy_id

    def update_draft(
        self,
        policy_id: str,
        *,
        fail_rate_pause_pct: float,
        fail_rate_abort_pct: float,
        schema_drift_action: str,
        row_count_drop_pct: float,
        actor: str,
        notes: str | None = None,
    ) -> None:
        """Overwrite a DRAFT's threshold fields. Illegal on non-DRAFT."""
        current = self._require(policy_id)
        if current.status is not PolicyStatus.DRAFT:
            raise ValueError(
                f"cannot edit policy {policy_id!r} in status {current.status} — "
                f"only DRAFT is editable"
            )
        # Validate the proposed new state — same rules as create.
        PolicyDraft(
            client_id=current.client_id,
            pipeline_id=current.pipeline_id,
            fail_rate_pause_pct=fail_rate_pause_pct,
            fail_rate_abort_pct=fail_rate_abort_pct,
            schema_drift_action=schema_drift_action,
            row_count_drop_pct=row_count_drop_pct,
            created_by=actor,
            notes=notes,
        ).validate()
        self._wh.execute(
            f"UPDATE {CONTROL_SCHEMA}.pipeline_control_policies "
            "SET fail_rate_pause_pct = $pp, fail_rate_abort_pct = $ap, "
            "    schema_drift_action = $sd, row_count_drop_pct = $rd, "
            "    notes = $n "
            "WHERE policy_id = $i",
            {
                "pp": fail_rate_pause_pct,
                "ap": fail_rate_abort_pct,
                "sd": schema_drift_action,
                "rd": row_count_drop_pct,
                "n": notes,
                "i": policy_id,
            },
        )
        self._audit(policy_id, PolicyStatus.DRAFT, PolicyStatus.DRAFT, actor, "draft edited")

    def submit_for_review(self, policy_id: str, actor: str) -> None:
        self._transition(
            policy_id,
            PolicyStatus.PENDING_REVIEW,
            actor,
            notes="submitted for review",
            extra_sets={"submitted_at": datetime.now(UTC)},
        )

    def approve(self, policy_id: str, actor: str, notes: str | None = None) -> None:
        self._transition(
            policy_id,
            PolicyStatus.APPROVED,
            actor,
            notes=notes or "approved",
            extra_sets={
                "reviewed_by": actor,
                "reviewed_at": datetime.now(UTC),
                "review_notes": notes,
            },
        )

    def activate(self, policy_id: str, actor: str) -> None:
        """Promote APPROVED → LIVE. Archives any other LIVE for the same
        ``(client_id, pipeline_id)``. Critical: ordering is "archive prior
        BEFORE flipping new" to preserve the unique-LIVE invariant
        mid-transaction (DuckDB has no partial-unique-index)."""
        current = self._require(policy_id)
        prev_live = self.get_active_policy(current.client_id, current.pipeline_id)
        if prev_live is not None:
            self._wh.execute(
                f"UPDATE {CONTROL_SCHEMA}.pipeline_control_policies "
                "SET status = $st, archived_at = $ts WHERE policy_id = $i",
                {
                    "st": PolicyStatus.ARCHIVED.value,
                    "ts": datetime.now(UTC),
                    "i": prev_live.policy_id,
                },
            )
            self._audit(
                prev_live.policy_id,
                prev_live.status,
                PolicyStatus.ARCHIVED,
                actor,
                f"archived: replaced by v{current.version}",
            )
        self._transition(
            policy_id,
            PolicyStatus.LIVE,
            actor,
            notes="activated",
            extra_sets={"activated_at": datetime.now(UTC)},
        )

    def reject(self, policy_id: str, actor: str, notes: str) -> None:
        self._transition(
            policy_id,
            PolicyStatus.REJECTED,
            actor,
            notes=notes,
            extra_sets={
                "reviewed_by": actor,
                "reviewed_at": datetime.now(UTC),
                "review_notes": notes,
            },
        )

    def request_changes(self, policy_id: str, actor: str, notes: str) -> None:
        self._transition(
            policy_id,
            PolicyStatus.DRAFT,
            actor,
            notes=notes,
            extra_sets={
                "reviewed_by": actor,
                "reviewed_at": datetime.now(UTC),
                "review_notes": notes,
            },
        )

    def archive(self, policy_id: str, actor: str, reason: str = "") -> None:
        self._transition(
            policy_id,
            PolicyStatus.ARCHIVED,
            actor,
            notes=reason or "manually archived",
            extra_sets={"archived_at": datetime.now(UTC)},
        )

    def submit_with_policy(
        self,
        policy_id: str,
        *,
        mode: ApprovalMode,
        actor: str,
        approval_notes: str | None = None,
    ) -> None:
        """Phase 10-style HITL walker — same shape as
        ``SuiteRegistry.submit_with_policy``. Reusing the ApprovalMode
        enum keeps the operator-visible UX identical between expectation
        suites and threshold policies (DRAFT / HITL / AUTO_APPROVE).
        """
        current = self._require(policy_id)
        if mode is ApprovalMode.DRAFT:
            return
        if mode is ApprovalMode.HITL:
            if current.status is PolicyStatus.DRAFT:
                self.submit_for_review(policy_id, actor=actor)
            return
        if mode is ApprovalMode.AUTO_APPROVE:
            cur = self._require(policy_id)
            if cur.status is PolicyStatus.DRAFT:
                self.submit_for_review(policy_id, actor=actor)
            cur = self._require(policy_id)
            if cur.status is PolicyStatus.PENDING_REVIEW:
                self.approve(
                    policy_id,
                    actor=actor,
                    notes=approval_notes or "auto-approved by policy",
                )
            cur = self._require(policy_id)
            if cur.status is PolicyStatus.APPROVED:
                self.activate(policy_id, actor=actor)
            return
        raise ValueError(f"Unknown ApprovalMode: {mode!r}")

    def fork_for_edit(self, source_policy_id: str, *, actor: str) -> str:
        """Clone any non-DRAFT version into a new DRAFT for editing.

        Same semantics as ``SuiteRegistry.fork_for_edit``: source row
        unmutated, new draft gets ``version = max(version) + 1``.
        Operator typically calls this to bump thresholds on a LIVE
        policy — the original keeps gating runtime until the new draft
        is approved + activated, at which point ``activate()`` archives
        it atomically.
        """
        src = self._require(source_policy_id)
        existing = self.list_versions(src.client_id, src.pipeline_id)
        next_version = (existing[0].version + 1) if existing else 1
        new_id = str(uuid.uuid4())
        self._wh.execute(
            f"INSERT INTO {CONTROL_SCHEMA}.pipeline_control_policies "
            "(policy_id, client_id, pipeline_id, version, status, "
            " fail_rate_pause_pct, fail_rate_abort_pct, schema_drift_action, "
            " row_count_drop_pct, created_by, created_at, notes) "
            "VALUES ($id, $c, $p, $v, $st, $pp, $ap, $sd, $rd, $cb, $ts, $n)",
            {
                "id": new_id,
                "c": src.client_id,
                "p": src.pipeline_id,
                "v": next_version,
                "st": PolicyStatus.DRAFT.value,
                "pp": src.fail_rate_pause_pct,
                "ap": src.fail_rate_abort_pct,
                "sd": src.schema_drift_action,
                "rd": src.row_count_drop_pct,
                "cb": actor,
                "ts": datetime.now(UTC),
                "n": src.notes,
            },
        )
        self._audit(
            new_id,
            None,
            PolicyStatus.DRAFT,
            actor,
            f"forked from policy_id={source_policy_id} v{src.version} ({src.status.value})",
        )
        _log.info(
            "policy.forked_for_edit",
            new_policy_id=new_id,
            source_policy_id=source_policy_id,
            client_id=src.client_id,
            pipeline_id=src.pipeline_id,
            new_version=next_version,
        )
        return new_id

    # ------------------------------------------------------------------
    # INTERNAL
    # ------------------------------------------------------------------

    def _require(self, policy_id: str) -> ThresholdPolicy:
        p = self.get_by_id(policy_id)
        if p is None:
            raise KeyError(f"Unknown policy_id: {policy_id!r}")
        return p

    def _transition(
        self,
        policy_id: str,
        to_status: PolicyStatus,
        actor: str,
        notes: str | None = None,
        extra_sets: dict[str, Any] | None = None,
    ) -> None:
        current = self._require(policy_id)
        if to_status not in _LEGAL_TRANSITIONS.get(current.status, set()):
            raise ValueError(
                f"illegal policy transition {current.status} → {to_status} " f"for {policy_id!r}"
            )
        sets: dict[str, Any] = {"status": to_status.value}
        if extra_sets:
            sets.update(extra_sets)
        set_sql = ", ".join(f"{k} = ${k}" for k in sets)
        params = {**sets, "i": policy_id}
        self._wh.execute(
            f"UPDATE {CONTROL_SCHEMA}.pipeline_control_policies "
            f"SET {set_sql} WHERE policy_id = $i",
            params,
        )
        self._audit(policy_id, current.status, to_status, actor, notes)
        _log.info(
            "policy.transition",
            policy_id=policy_id,
            from_status=current.status.value,
            to_status=to_status.value,
            actor=actor,
        )

    def _audit(
        self,
        policy_id: str,
        from_status: PolicyStatus | None,
        to_status: PolicyStatus,
        actor: str,
        notes: str | None,
    ) -> None:
        self._wh.execute(
            f"INSERT INTO {CONTROL_SCHEMA}.policy_audit_log "
            "(audit_id, policy_id, from_status, to_status, actor, notes) "
            "VALUES ($aid, $pid, $fs, $ts, $a, $n)",
            {
                "aid": str(uuid.uuid4()),
                "pid": policy_id,
                "fs": from_status.value if from_status else None,
                "ts": to_status.value,
                "a": actor,
                "n": notes,
            },
        )


# ============================================================================
# ROW MAPPING
# ============================================================================


def _row_to_policy(row: dict[str, Any]) -> ThresholdPolicy:
    return ThresholdPolicy(
        policy_id=row["policy_id"],
        client_id=row["client_id"],
        pipeline_id=row["pipeline_id"],
        version=int(row["version"]),
        status=PolicyStatus(row["status"]),
        fail_rate_pause_pct=float(row["fail_rate_pause_pct"]),
        fail_rate_abort_pct=float(row["fail_rate_abort_pct"]),
        schema_drift_action=row["schema_drift_action"],
        row_count_drop_pct=float(row["row_count_drop_pct"]),
        created_by=row["created_by"],
        created_at=row["created_at"],
        submitted_at=row.get("submitted_at"),
        reviewed_by=row.get("reviewed_by"),
        reviewed_at=row.get("reviewed_at"),
        review_notes=row.get("review_notes"),
        activated_at=row.get("activated_at"),
        archived_at=row.get("archived_at"),
        notes=row.get("notes"),
    )
