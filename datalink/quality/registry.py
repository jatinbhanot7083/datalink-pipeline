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


class ApprovalMode(StrEnum):
    """Phase 10.1 — source-aware approval policy.

    Determines where a freshly-authored suite version lands in the state
    machine. Replaces the ad-hoc string-typed ``target_status`` parameter
    that ``accept_proposal()`` used to take.

    * ``AUTO_APPROVE`` — walks DRAFT → PENDING_REVIEW → APPROVED → LIVE
                          in one round-trip. Reserved for low-risk,
                          mechanically-generated content (profiler-baseline
                          expectations: null-checks, uniqueness, regex
                          inferred from column metadata).
    * ``HITL``         — lands in PENDING_REVIEW awaiting an independent
                          reviewer. The audit-friendly default for any
                          *creative* content (agent-authored proposals
                          from NL prompts) and for **any edit of a LIVE
                          suite** regardless of original source.
    * ``DRAFT``        — leaves it editable; author parks it for later.
    """

    AUTO_APPROVE = "AUTO_APPROVE"
    HITL = "HITL"
    DRAFT = "DRAFT"


def default_approval_mode(
    source: SuiteSource,
    *,
    is_edit_of_live: bool = False,
) -> ApprovalMode:
    """Source-aware default policy (Option C).

    Rules, in priority order:

      1. Editing a LIVE suite → ALWAYS HITL. A LIVE suite is currently
         gating production checkpoints; mutating it must go through review
         even if the original suite was auto-approved.
      2. ``BASELINE_PYTHON`` (profiler-mechanical) → AUTO_APPROVE.
      3. ``AGENT`` (NL-prompt-creative) → HITL.
      4. ``UI`` (manual human authoring) → HITL.
         (DQ Author's flow naturally lands in DRAFT first, but the policy
         answer for "user clicked Submit" is HITL.)

    Callers may override per-call (e.g. a senior steward bulk-importing
    trusted expectations can pass ``mode=ApprovalMode.AUTO_APPROVE``
    explicitly).
    """
    if is_edit_of_live:
        return ApprovalMode.HITL
    if source is SuiteSource.BASELINE_PYTHON:
        return ApprovalMode.AUTO_APPROVE
    return ApprovalMode.HITL


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

    Phase 6 Commit 2: `schema_fingerprint` is a 16-char sha256 of the
    (column_name, column_type) tuples of the source table the suite
    was authored against. Pre-Val crew skips re-authoring when the
    fingerprint of the target table matches the LIVE suite's fingerprint.
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
    schema_fingerprint: str | None = None
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
    schema_fingerprint: str | None = None


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
            " dq_dimensions, source_type, source, schema_fingerprint, "
            " created_by, created_at) "
            "VALUES ($id, $c, $s, $v, $st, $e, $d, $srct, $src, $fp, $cb, $ts)",
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
                "fp": draft.schema_fingerprint,
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

    # ------------------------------------------------------------------
    # Phase 10.2 — per-expectation CRUD
    #
    # Surgical edits on a DRAFT's expectations array. Use these when the
    # UI exposes per-row edit/delete buttons rather than a full-array
    # textarea (the old update_draft path is kept for bulk paste/import).
    #
    # All three methods enforce status == DRAFT. Editing a LIVE suite
    # requires fork_for_edit() first to create a new editable version —
    # a hard guarantee that LIVE expectations are immutable.
    # ------------------------------------------------------------------

    def fork_for_edit(self, source_suite_id: str, *, actor: str) -> str:
        """Clone an existing version into a new DRAFT for editing.

        The source can be in any status (LIVE, APPROVED, PENDING_REVIEW,
        ARCHIVED, REJECTED) — we read its expectations and create a new
        DRAFT with version = max(version) + 1. The source row is NOT
        mutated — its status stays exactly as before until the new draft
        is approved and activated, at which point ``activate()`` will
        archive whatever was LIVE for the same (client_id, suite_name).

        Returns the new ``suite_id`` (a fresh UUID, distinct from source).
        """
        src = self._require(source_suite_id)
        existing = self.list_versions(src.client_id, src.suite_name)
        next_version = (existing[0].version + 1) if existing else 1
        new_id = str(uuid.uuid4())
        self._wh.execute(
            f"INSERT INTO {CONTROL_SCHEMA}.dq_suites "
            "(suite_id, client_id, suite_name, version, status, expectations, "
            " dq_dimensions, source_type, source, schema_fingerprint, "
            " created_by, created_at) "
            "VALUES ($id, $c, $s, $v, $st, $e, $d, $srct, $src, $fp, $cb, $ts)",
            {
                "id": new_id,
                "c": src.client_id,
                "s": src.suite_name,
                "v": next_version,
                "st": SuiteStatus.DRAFT.value,
                "e": json.dumps(src.expectations),
                "d": json.dumps(src.dq_dimensions),
                "srct": src.source_type,
                "src": src.source.value,
                "fp": src.schema_fingerprint,
                "cb": actor,
                "ts": datetime.now(UTC),
            },
        )
        self._audit(
            new_id,
            None,
            SuiteStatus.DRAFT,
            actor,
            f"forked from suite_id={source_suite_id} v{src.version} ({src.status.value})",
        )
        _log.info(
            "dq_suite.forked_for_edit",
            new_suite_id=new_id,
            source_suite_id=source_suite_id,
            client_id=src.client_id,
            suite_name=src.suite_name,
            new_version=next_version,
        )
        return new_id

    def add_expectation(
        self,
        suite_id: str,
        expectation: dict[str, Any],
        *,
        actor: str,
    ) -> None:
        """Append one expectation to a DRAFT's array. Audits the addition."""
        current = self._require_draft(suite_id)
        new_exps = [*current.expectations, expectation]
        new_dims = _recompute_dimensions(new_exps)
        self._wh.execute(
            f"UPDATE {CONTROL_SCHEMA}.dq_suites "
            "SET expectations = $e, dq_dimensions = $d WHERE suite_id = $i",
            {"e": json.dumps(new_exps), "d": json.dumps(new_dims), "i": suite_id},
        )
        exp_type = expectation.get("expectation_type", "?")
        col = expectation.get("kwargs", {}).get("column", "?")
        self._audit(
            suite_id,
            SuiteStatus.DRAFT,
            SuiteStatus.DRAFT,
            actor,
            f"added expectation #{len(new_exps) - 1}: {exp_type} on {col}",
        )

    def update_expectation(
        self,
        suite_id: str,
        index: int,
        expectation: dict[str, Any],
        *,
        actor: str,
    ) -> None:
        """Replace one expectation by index in a DRAFT. Audits the swap."""
        current = self._require_draft(suite_id)
        if not 0 <= index < len(current.expectations):
            raise IndexError(
                f"expectation index {index} out of range for suite {suite_id!r} "
                f"(has {len(current.expectations)} expectations)"
            )
        old = current.expectations[index]
        new_exps = list(current.expectations)
        new_exps[index] = expectation
        new_dims = _recompute_dimensions(new_exps)
        self._wh.execute(
            f"UPDATE {CONTROL_SCHEMA}.dq_suites "
            "SET expectations = $e, dq_dimensions = $d WHERE suite_id = $i",
            {"e": json.dumps(new_exps), "d": json.dumps(new_dims), "i": suite_id},
        )
        old_type = old.get("expectation_type", "?")
        new_type = expectation.get("expectation_type", "?")
        self._audit(
            suite_id,
            SuiteStatus.DRAFT,
            SuiteStatus.DRAFT,
            actor,
            f"updated expectation #{index}: {old_type} → {new_type}",
        )

    def delete_expectation(
        self,
        suite_id: str,
        index: int,
        *,
        actor: str,
    ) -> None:
        """Remove one expectation by index from a DRAFT. Audits the removal."""
        current = self._require_draft(suite_id)
        if not 0 <= index < len(current.expectations):
            raise IndexError(
                f"expectation index {index} out of range for suite {suite_id!r} "
                f"(has {len(current.expectations)} expectations)"
            )
        removed = current.expectations[index]
        new_exps = [e for i, e in enumerate(current.expectations) if i != index]
        new_dims = _recompute_dimensions(new_exps)
        self._wh.execute(
            f"UPDATE {CONTROL_SCHEMA}.dq_suites "
            "SET expectations = $e, dq_dimensions = $d WHERE suite_id = $i",
            {"e": json.dumps(new_exps), "d": json.dumps(new_dims), "i": suite_id},
        )
        exp_type = removed.get("expectation_type", "?")
        col = removed.get("kwargs", {}).get("column", "?")
        self._audit(
            suite_id,
            SuiteStatus.DRAFT,
            SuiteStatus.DRAFT,
            actor,
            f"deleted expectation #{index}: {exp_type} on {col}",
        )

    def _require_draft(self, suite_id: str) -> SuiteVersion:
        """Helper: load a suite and assert it's a DRAFT — used by all
        per-expectation CRUD methods to centralise the guard."""
        v = self._require(suite_id)
        if v.status is not SuiteStatus.DRAFT:
            raise ValueError(
                f"per-expectation CRUD requires DRAFT status, "
                f"suite {suite_id!r} is {v.status.value} — "
                f"call fork_for_edit() first to create an editable version"
            )
        return v

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

        # Phase 8 — opportunistically embed the new LIVE suite into the
        # agent_memory store so future authoring runs can retrieve it as
        # an EXAMPLE. Failure is NEVER a blocker: a memory-store outage
        # must not prevent a valid suite from going live. We eat the
        # exception, log it, move on. The backfill script is the
        # safety net that catches anything skipped here.
        try:
            self._embed_for_memory(suite_id)
        except Exception as e:
            from datalink.logging import get_logger as _gl

            _gl(__name__).warning(
                "registry.activate.memory_embed_failed",
                suite_id=suite_id,
                error=str(e),
            )

    def _embed_for_memory(self, suite_id: str) -> None:
        """Embed a freshly-activated suite into the RAG store.

        Imported lazily so the registry doesn't pull psycopg / Voyage on
        modules that just need the state machine (tests, CI lint, the
        plug-out path). Reads the suite back via the registry to use the
        same humanised text builder as the backfill script — embeddings
        from the live path and the backfill path stay byte-identical.
        """
        from datalink.adapters.embeddings.router import get_embedder
        from datalink.memory import AgentMemoryStore
        from datalink.memory.store import suite_source_text

        v = self._require(suite_id)
        # Build the same dict shape suite_source_text() expects from a row.
        row_for_text = {
            "client_id": v.client_id,
            "suite_name": v.suite_name,
            "source": v.source.value if hasattr(v.source, "value") else str(v.source),
            "source_type": v.source_type,
            "dq_dimensions": v.dq_dimensions,
            "expectations": v.expectations,
        }
        text = suite_source_text(row_for_text)

        embedder = get_embedder()
        memory = AgentMemoryStore(embedder=embedder)
        memory.ensure_schema()
        memory.upsert_suite(
            suite_id=suite_id,
            client_id=v.client_id,
            suite_name=v.suite_name,
            status=v.status.value if hasattr(v.status, "value") else str(v.status),
            source=str(row_for_text["source"]),
            source_type=v.source_type,
            source_text=text,
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

    def archive(self, suite_id: str, actor: str, reason: str = "") -> None:
        """Soft-delete a suite — moves it to ARCHIVED.

        Healthcare DQ requires a tamper-proof audit trail (HIPAA / SOC 2),
        so suites are never hard-deleted. ARCHIVED suites are filtered out
        of every checkpoint query (which all match ``status = LIVE``) but
        the row stays in CONTROL.dq_suites for forever for compliance.

        Legal from any non-terminal state — DRAFT, PENDING_REVIEW, APPROVED,
        LIVE all permit transition to ARCHIVED per ``_LEGAL_TRANSITIONS``.

        Note: when a NEW version is activated for the same (client_id,
        suite_name), the previous LIVE is auto-archived by ``activate()``
        — operators rarely need to call this explicitly. Use this for
        ad-hoc retirement of an experimental DRAFT or a misguided LIVE
        that won't be replaced.
        """
        self._transition(
            suite_id,
            SuiteStatus.ARCHIVED,
            actor,
            notes=reason or "manually archived",
            extra_sets={"archived_at": datetime.now(UTC)},
        )

    def submit_with_policy(
        self,
        suite_id: str,
        *,
        mode: ApprovalMode,
        actor: str,
        approval_notes: str | None = None,
    ) -> None:
        """Walk a fresh DRAFT to the terminal state dictated by ``mode``.

        Phase 10.1. Consolidates the state-walk that used to be open-coded
        in ``baseline_seeder`` (always walked to LIVE) and in
        ``accept_proposal`` (walked based on a string ``target_status``).
        Centralising the logic here means every author path goes through
        the same audit trail and the same legality checks.

        * ``ApprovalMode.DRAFT`` — no-op. Suite stays DRAFT.
        * ``ApprovalMode.HITL`` — walks DRAFT → PENDING_REVIEW.
        * ``ApprovalMode.AUTO_APPROVE`` — walks DRAFT → PENDING_REVIEW →
          APPROVED → LIVE. The intermediate transitions are still
          recorded in the audit log so even auto-approved suites have
          a full state-history.

        Idempotent on re-entry: if the suite is already past the target
        terminal, this returns silently rather than raising. That keeps
        ``baseline_seeder`` safe to re-run on every container start.
        """
        current = self._require(suite_id)

        if mode is ApprovalMode.DRAFT:
            return

        if mode is ApprovalMode.HITL:
            if current.status is SuiteStatus.DRAFT:
                self.submit_for_review(suite_id, actor=actor)
            # if already PENDING_REVIEW or further, no-op.
            return

        if mode is ApprovalMode.AUTO_APPROVE:
            current = self._require(suite_id)  # refresh
            if current.status is SuiteStatus.DRAFT:
                self.submit_for_review(suite_id, actor=actor)
            current = self._require(suite_id)
            if current.status is SuiteStatus.PENDING_REVIEW:
                self.approve(
                    suite_id,
                    actor=actor,
                    notes=approval_notes or "auto-approved by policy",
                )
            current = self._require(suite_id)
            if current.status is SuiteStatus.APPROVED:
                self.activate(suite_id, actor=actor)
            return

        raise ValueError(f"Unknown ApprovalMode: {mode!r}")

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


def _recompute_dimensions(expectations: list[dict[str, Any]]) -> list[str]:
    """Phase 10.2 — derive ``dq_dimensions`` from the expectation array.

    Walks every expectation's ``meta.dq_dimension`` and returns the unique
    sorted set. Called after every per-expectation CRUD op so the suite's
    dimension tags stay coherent with what's actually inside it. Empty
    string and missing values are filtered out.
    """
    dims: set[str] = set()
    for e in expectations:
        meta = e.get("meta", {}) or {}
        d = meta.get("dq_dimension")
        if d:
            dims.add(d)
    return sorted(dims)


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
        # source_type + schema_fingerprint nullable — older 5.8 rows may not have them.
        source_type=row.get("source_type"),
        schema_fingerprint=row.get("schema_fingerprint"),
        submitted_at=row.get("submitted_at"),
        reviewed_by=row.get("reviewed_by"),
        reviewed_at=row.get("reviewed_at"),
        review_notes=row.get("review_notes"),
        activated_at=row.get("activated_at"),
        archived_at=row.get("archived_at"),
    )
