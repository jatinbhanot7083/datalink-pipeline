"""Phase 12 / 12.5 — pipeline-control threshold policies.

Two-layer HITL (config-time review, runtime automatic) — Phase 12.
Per-source-type granularity + global defaults + cross-client clone — Phase 12.5.

Policy tuple: ``(client_id, pipeline_id, source_type)``

  * ``client_id``    — tenant. ``'*'`` is the wildcard "global default".
  * ``pipeline_id``  — ``bronze_ingest`` / ``silver_build`` / ``gold_egress``.
  * ``source_type``  — ``CLAIMS`` / ``MEMBERSHIP`` / ``PROVIDER`` / ``NULL``.
                       ``NULL`` = "all source types under this pipeline".

Lookup precedence (most specific wins, falls through on miss):

  1. ``(client, pipeline, source_type)`` — exact tenant + per-source
  2. ``(client, pipeline, NULL)``        — exact tenant + pipeline-wide
  3. ``('*',   pipeline, source_type)``  — global per-source-type
  4. ``('*',   pipeline, NULL)``         — global per-pipeline
  5. :data:`DEFAULT_POLICY`              — hardcoded synthetic fallback

So a customer-specific membership policy beats a customer-wide bronze
policy beats a global membership policy beats a global bronze policy
beats the hardcoded constant. Every layer optional — set only the ones
you want to override.

Two-layer HITL (Phase 12, unchanged):
  * **Config-time HITL** — threshold edits go through DRAFT → PENDING_REVIEW
    → APPROVED → LIVE. ApprovalMode reused from
    :mod:`datalink.quality.registry`.
  * **Runtime automatic** — once a policy is LIVE, hooks.py reads the
    cascade above and fires the right policy without human-in-the-loop.

Cross-client clone (Phase 12.5):
  * :func:`PipelineControlPolicyRegistry.clone_to_client` duplicates a
    LIVE policy as a DRAFT under a target client_id, so operators can
    standardise once on aetna and propagate to other tenants without
    re-authoring.

Back-compat: ``DEFAULT_POLICY`` values match the pre-Phase-12 hardcoded
numbers in ``hooks.py``, so tenants without registered policies behave
exactly as today.
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
    source_type: str | None  # Phase 12.5 — NULL = pipeline-wide; specific = per-source override
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

    @property
    def is_global(self) -> bool:
        """True for the wildcard `client_id='*'` tier — applies to every tenant
        unless that tenant has its own override. Phase 12.5."""
        return self.client_id == "*"

    @property
    def scope_label(self) -> str:
        """Human-readable scope: 'global / bronze_ingest / CLAIMS' or
        'aetna / silver_build / (any)' etc. For UI tables."""
        client = "global (*)" if self.client_id == "*" else self.client_id
        source = self.source_type or "(any)"
        return f"{client} / {self.pipeline_id} / {source}"

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
    source_type=None,
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

# Phase 12.5 — wildcard client for the global tier. Authored from the UI's
# "Global default" scope toggle. A LIVE row with client_id='*' applies to
# every tenant unless that tenant has its own override.
GLOBAL_CLIENT = "*"

# Source-type vocabulary — must match what bronze ingest tags
# ``BronzeIngestResult.source_type`` (Phase 9.1).
KNOWN_SOURCE_TYPES = ("CLAIMS", "MEMBERSHIP", "PROVIDER")


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
    source_type: str | None = None  # Phase 12.5 — None = pipeline-wide
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
        if self.source_type is not None and self.source_type not in KNOWN_SOURCE_TYPES:
            raise ValueError(
                f"source_type must be one of {list(KNOWN_SOURCE_TYPES)} or None "
                f"(pipeline-wide), got {self.source_type!r}"
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

    def get_active_policy(
        self,
        client_id: str,
        pipeline_id: str,
        source_type: str | None = None,
    ) -> ThresholdPolicy | None:
        """Return the LIVE policy for the EXACT
        ``(client, pipeline, source_type)`` tuple, or None.

        Phase 12.5 — exact match only; pass ``source_type=None`` to find
        the pipeline-wide row, or a specific value to find a per-source
        override. Use :meth:`get_or_default` for the full precedence
        cascade.
        """
        if source_type is None:
            where_st = "AND source_type IS NULL"
            params: dict[str, Any] = {
                "c": client_id,
                "p": pipeline_id,
                "st": PolicyStatus.LIVE.value,
            }
        else:
            where_st = "AND source_type = $src"
            params = {
                "c": client_id,
                "p": pipeline_id,
                "src": source_type,
                "st": PolicyStatus.LIVE.value,
            }
        rows = self._wh.query(
            f"SELECT * FROM {CONTROL_SCHEMA}.pipeline_control_policies "
            f"WHERE client_id = $c AND pipeline_id = $p AND status = $st {where_st}",
            params,
        )
        if not rows:
            return None
        if len(rows) > 1:
            raise RuntimeError(
                f"Invariant violation: {len(rows)} LIVE policies for "
                f"({client_id!r}, {pipeline_id!r}, {source_type!r}) — should be exactly 1"
            )
        return _row_to_policy(rows[0])

    def get_or_default(
        self,
        client_id: str,
        pipeline_id: str,
        source_type: str | None = None,
    ) -> ThresholdPolicy:
        """Phase 12.5 — full precedence cascade, never returns None.

        Looks up the most-specific LIVE policy first, falls through
        less-specific tiers, finally returns :data:`DEFAULT_POLICY` if
        nothing matches:

          1. ``(client, pipeline, source_type)`` — exact + per-source
          2. ``(client, pipeline, NULL)``        — exact tenant pipeline-wide
          3. ``('*',  pipeline, source_type)``   — global per-source-type
          4. ``('*',  pipeline, NULL)``          — global per-pipeline
          5. :data:`DEFAULT_POLICY`              — hardcoded constant

        ``source_type=None`` skips tiers 1 and 3 — useful when the
        caller doesn't yet know which source is being processed
        (e.g. silver_build that fans across all sources).
        """
        # 1. Exact tenant + per-source
        if source_type is not None:
            p = self.get_active_policy(client_id, pipeline_id, source_type=source_type)
            if p is not None:
                return p
        # 2. Exact tenant + pipeline-wide (source_type = NULL)
        p = self.get_active_policy(client_id, pipeline_id, source_type=None)
        if p is not None:
            return p
        # 3. Global + per-source-type
        if source_type is not None:
            p = self.get_active_policy(GLOBAL_CLIENT, pipeline_id, source_type=source_type)
            if p is not None:
                return p
        # 4. Global + pipeline-wide
        p = self.get_active_policy(GLOBAL_CLIENT, pipeline_id, source_type=None)
        if p is not None:
            return p
        # 5. Hardcoded fallback
        return DEFAULT_POLICY

    def get_by_id(self, policy_id: str) -> ThresholdPolicy | None:
        rows = self._wh.query(
            f"SELECT * FROM {CONTROL_SCHEMA}.pipeline_control_policies WHERE policy_id = $i",
            {"i": policy_id},
        )
        return _row_to_policy(rows[0]) if rows else None

    def list_versions(
        self,
        client_id: str,
        pipeline_id: str,
        source_type: str | None = None,
    ) -> list[ThresholdPolicy]:
        """All versions for the
        ``(client, pipeline, source_type)`` tuple, newest first.
        ``source_type=None`` matches the pipeline-wide rows."""
        if source_type is None:
            where_st = "AND source_type IS NULL"
            params: dict[str, Any] = {"c": client_id, "p": pipeline_id}
        else:
            where_st = "AND source_type = $src"
            params = {"c": client_id, "p": pipeline_id, "src": source_type}
        rows = self._wh.query(
            f"SELECT * FROM {CONTROL_SCHEMA}.pipeline_control_policies "
            f"WHERE client_id = $c AND pipeline_id = $p {where_st} "
            f"ORDER BY version DESC",
            params,
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
            "ORDER BY client_id, pipeline_id, source_type NULLS FIRST",
            {"st": PolicyStatus.LIVE.value},
        )
        return [_row_to_policy(r) for r in rows]

    # ------------------------------------------------------------------
    # WRITES — state transitions
    # ------------------------------------------------------------------

    def create_draft(self, draft: PolicyDraft) -> str:
        """Start a new DRAFT. Version = max(version) + 1 for the
        ``(client, pipeline, source_type)`` tuple."""
        draft.validate()
        existing = self.list_versions(
            draft.client_id, draft.pipeline_id, source_type=draft.source_type
        )
        next_version = (existing[0].version + 1) if existing else 1
        policy_id = str(uuid.uuid4())
        self._wh.execute(
            f"INSERT INTO {CONTROL_SCHEMA}.pipeline_control_policies "
            "(policy_id, client_id, pipeline_id, source_type, version, status, "
            " fail_rate_pause_pct, fail_rate_abort_pct, schema_drift_action, "
            " row_count_drop_pct, created_by, created_at, notes) "
            "VALUES ($id, $c, $p, $src, $v, $st, $pp, $ap, $sd, $rd, $cb, $ts, $n)",
            {
                "id": policy_id,
                "c": draft.client_id,
                "p": draft.pipeline_id,
                "src": draft.source_type,
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
            source_type=current.source_type,
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
        ``(client_id, pipeline_id, source_type)`` tuple. Critical: ordering
        is "archive prior BEFORE flipping new" to preserve the unique-LIVE
        invariant mid-transaction (DuckDB has no partial-unique-index)."""
        current = self._require(policy_id)
        prev_live = self.get_active_policy(
            current.client_id, current.pipeline_id, source_type=current.source_type
        )
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
        """Clone any non-DRAFT version into a new DRAFT for editing of the
        SAME ``(client, pipeline, source_type)`` tuple.

        Source row unmutated, new draft gets ``version = max(version) + 1``.
        Operator typically calls this to bump thresholds on a LIVE policy —
        the original keeps gating runtime until the new draft is approved +
        activated, at which point ``activate()`` archives it atomically.
        """
        src = self._require(source_policy_id)
        existing = self.list_versions(src.client_id, src.pipeline_id, source_type=src.source_type)
        next_version = (existing[0].version + 1) if existing else 1
        new_id = str(uuid.uuid4())
        self._wh.execute(
            f"INSERT INTO {CONTROL_SCHEMA}.pipeline_control_policies "
            "(policy_id, client_id, pipeline_id, source_type, version, status, "
            " fail_rate_pause_pct, fail_rate_abort_pct, schema_drift_action, "
            " row_count_drop_pct, created_by, created_at, notes) "
            "VALUES ($id, $c, $p, $src, $v, $st, $pp, $ap, $sd, $rd, $cb, $ts, $n)",
            {
                "id": new_id,
                "c": src.client_id,
                "p": src.pipeline_id,
                "src": src.source_type,
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
            source_type=src.source_type,
            new_version=next_version,
        )
        return new_id

    def clone_to_client(
        self,
        source_policy_id: str,
        target_client_id: str,
        *,
        actor: str,
        notes: str | None = None,
    ) -> str:
        """Phase 12.5 — copy a policy's threshold values to another client.

        Creates a new DRAFT under the target client_id with the same
        pipeline_id, source_type, and threshold values as the source.
        The target version starts at ``max(version-for-target-tuple) + 1``
        so a fresh client gets v1, an existing client gets v(N+1).

        Use case: standardise on aetna's policies, then propagate to the
        other 5 tenants without re-authoring. The clones land as DRAFT,
        so each one still goes through HITL approval before going LIVE
        (no auto-activate).

        Refuses to clone to the same client_id (use fork_for_edit for
        that case) and refuses if a DRAFT already exists for the target
        tuple (operator should resolve manually).
        """
        src = self._require(source_policy_id)
        if target_client_id == src.client_id:
            raise ValueError(
                f"clone_to_client target ({target_client_id!r}) is same as source — "
                f"use fork_for_edit() to bump version on the same client"
            )
        # Refuse if a DRAFT already exists for the target tuple.
        existing = self.list_versions(
            target_client_id, src.pipeline_id, source_type=src.source_type
        )
        if any(v.status is PolicyStatus.DRAFT for v in existing):
            raise ValueError(
                f"target ({target_client_id!r}, {src.pipeline_id!r}, "
                f"{src.source_type!r}) already has a DRAFT — resolve it first"
            )
        next_version = (existing[0].version + 1) if existing else 1
        new_id = str(uuid.uuid4())
        clone_notes = (
            notes
            or f"cloned from {src.client_id}/{src.pipeline_id}/{src.source_type or 'any'} v{src.version}"
        )
        self._wh.execute(
            f"INSERT INTO {CONTROL_SCHEMA}.pipeline_control_policies "
            "(policy_id, client_id, pipeline_id, source_type, version, status, "
            " fail_rate_pause_pct, fail_rate_abort_pct, schema_drift_action, "
            " row_count_drop_pct, created_by, created_at, notes) "
            "VALUES ($id, $c, $p, $src, $v, $st, $pp, $ap, $sd, $rd, $cb, $ts, $n)",
            {
                "id": new_id,
                "c": target_client_id,
                "p": src.pipeline_id,
                "src": src.source_type,
                "v": next_version,
                "st": PolicyStatus.DRAFT.value,
                "pp": src.fail_rate_pause_pct,
                "ap": src.fail_rate_abort_pct,
                "sd": src.schema_drift_action,
                "rd": src.row_count_drop_pct,
                "cb": actor,
                "ts": datetime.now(UTC),
                "n": clone_notes,
            },
        )
        self._audit(
            new_id,
            None,
            PolicyStatus.DRAFT,
            actor,
            f"cloned from policy_id={source_policy_id} ({src.client_id} → {target_client_id})",
        )
        _log.info(
            "policy.cloned_to_client",
            new_policy_id=new_id,
            source_policy_id=source_policy_id,
            source_client=src.client_id,
            target_client=target_client_id,
            pipeline_id=src.pipeline_id,
            source_type=src.source_type,
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
        # Phase 12.5 — source_type is nullable. Snowflake returns python None
        # for SQL NULL; older rows that pre-date the migration also see None.
        source_type=row.get("source_type"),
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
