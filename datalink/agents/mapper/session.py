"""Phase 13.2 — mapping session storage + state machine.

A :class:`MappingSession` is the persistent envelope for one mapper
conversation: source profile snapshot, the operator's NL Gold contract,
the multi-turn chat history, the agent's currently-proposed artifacts
(Silver SQL / Gold SQL / On-Prem push), the sample-data preview, and
where the session is in the HITL review lifecycle.

State machine (mirrors Phase 10/12, third application of the pattern)::

    DRAFT ──submit──→ PENDING_REVIEW ──approve──→ APPROVED ──deploy──→ DEPLOYED
      ↑                     │                                            │
      │                     ├─request_changes──→ DRAFT                  │
      │                     └─reject────────────→ REJECTED              │
      │                                                                  │
      └──────────── any state ─────── archived ────────────── ARCHIVED ─┘

DEPLOYED is the terminal "files written to disk + dbt model registered"
state — added in 13.8 once the deployment writer ships.

Reuses :class:`datalink.quality.registry.ApprovalMode` so HITL UX
matches DQ suites and threshold policies (DRAFT / HITL / AUTO_APPROVE).
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from datalink.adapters.protocols import Warehouse
from datalink.agents.mapper.profiler import SourceProfile
from datalink.logging import get_logger
from datalink.quality.control import CONTROL_SCHEMA
from datalink.quality.registry import ApprovalMode

_log = get_logger(__name__)


# ============================================================================
# CORE TYPES
# ============================================================================


class SessionStatus(StrEnum):
    DRAFT = "DRAFT"
    PENDING_REVIEW = "PENDING_REVIEW"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"
    DEPLOYED = "DEPLOYED"
    ARCHIVED = "ARCHIVED"


class TargetMode(StrEnum):
    """How much of the medallion the operator wants the mapper to
    auto-design. Drives which artifacts the agent generates.
    """

    SILVER_ONLY = "SILVER_ONLY"  # 13.3 v1 — just the Silver dbt model
    SILVER_AND_GOLD = "SILVER_AND_GOLD"  # 13.6 — adds Gold dbt view
    FULL_STACK = "FULL_STACK"  # 13.7 — adds optimised On-Prem push


_LEGAL_TRANSITIONS: dict[SessionStatus, set[SessionStatus]] = {
    SessionStatus.DRAFT: {SessionStatus.PENDING_REVIEW, SessionStatus.ARCHIVED},
    SessionStatus.PENDING_REVIEW: {
        SessionStatus.APPROVED,
        SessionStatus.DRAFT,
        SessionStatus.REJECTED,
        SessionStatus.ARCHIVED,
    },
    SessionStatus.APPROVED: {SessionStatus.DEPLOYED, SessionStatus.ARCHIVED},
    SessionStatus.DEPLOYED: {SessionStatus.ARCHIVED},
    SessionStatus.ARCHIVED: set(),
    SessionStatus.REJECTED: set(),
}


@dataclass(frozen=True)
class ConversationTurn:
    """One operator-↔-agent exchange. ``role`` is one of
    ``"user"`` / ``"assistant"`` / ``"system"`` mirroring the Anthropic
    Messages API so we can replay turns straight back to the model.
    """

    role: str
    text: str
    ts: datetime
    tokens_used: int = 0
    duration_ms: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "role": self.role,
            "text": self.text,
            "ts": self.ts.isoformat(),
            "tokens_used": self.tokens_used,
            "duration_ms": self.duration_ms,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> ConversationTurn:
        ts_raw = d.get("ts")
        if isinstance(ts_raw, str):
            ts = datetime.fromisoformat(ts_raw)
        elif isinstance(ts_raw, datetime):
            ts = ts_raw
        else:
            ts = datetime.now(UTC)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=UTC)
        return cls(
            role=str(d.get("role", "user")),
            text=str(d.get("text", "")),
            ts=ts,
            tokens_used=int(d.get("tokens_used", 0) or 0),
            duration_ms=int(d.get("duration_ms", 0) or 0),
        )


@dataclass(frozen=True)
class MappingSession:
    """Full session record. ``source_profile`` is parsed from the JSON
    column on read; callers don't need to JSON-deserialize themselves.
    """

    session_id: str
    client_id: str
    source_qualified_table: str
    gold_contract_text: str
    target_mode: TargetMode
    status: SessionStatus
    source_profile: dict[str, Any] | None
    conversation: tuple[ConversationTurn, ...]
    silver_sql: str | None
    gold_sql: str | None
    onprem_postgres_sql: str | None
    onprem_mssql_sql: str | None
    sample_preview: list[dict[str, Any]] | None
    silver_target_table: str | None
    gold_target_table: str | None
    created_by: str
    created_at: datetime
    updated_at: datetime | None = None
    submitted_at: datetime | None = None
    reviewed_by: str | None = None
    reviewed_at: datetime | None = None
    review_notes: str | None = None
    deployed_at: datetime | None = None
    archived_at: datetime | None = None
    notes: str | None = None

    @property
    def has_silver_proposal(self) -> bool:
        return bool(self.silver_sql and self.silver_sql.strip())

    @property
    def turn_count(self) -> int:
        return len(self.conversation)


# ============================================================================
# REGISTRY
# ============================================================================


class MappingSessionRegistry:
    """DB-backed store of mapper conversations. Audit-logs every state
    transition; rejects illegal moves the same way SuiteRegistry does.
    """

    def __init__(self, warehouse: Warehouse) -> None:
        self._wh = warehouse

    # ------------------------------------------------------------------
    # READS
    # ------------------------------------------------------------------

    def get_session(self, session_id: str) -> MappingSession | None:
        rows = self._wh.query(
            f"SELECT * FROM {CONTROL_SCHEMA}.mapping_sessions WHERE session_id = $i",
            {"i": session_id},
        )
        return _row_to_session(rows[0]) if rows else None

    def list_sessions(
        self,
        *,
        client_id: str | None = None,
        status: SessionStatus | None = None,
        limit: int = 50,
    ) -> list[MappingSession]:
        """List sessions, optionally filtered. Newest first."""
        clauses = []
        params: dict[str, Any] = {"lim": limit}
        if client_id:
            clauses.append("client_id = $c")
            params["c"] = client_id
        if status:
            clauses.append("status = $st")
            params["st"] = status.value
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        rows = self._wh.query(
            f"SELECT * FROM {CONTROL_SCHEMA}.mapping_sessions"
            f"{where} ORDER BY created_at DESC LIMIT $lim",
            params,
        )
        return [_row_to_session(r) for r in rows]

    def list_pending_reviews(self, *, client_id: str | None = None) -> list[MappingSession]:
        return self.list_sessions(client_id=client_id, status=SessionStatus.PENDING_REVIEW)

    # ------------------------------------------------------------------
    # WRITES — creation + content updates
    # ------------------------------------------------------------------

    def create_session(
        self,
        *,
        client_id: str,
        source_qualified_table: str,
        target_mode: TargetMode,
        created_by: str,
        gold_contract_text: str = "",
        notes: str | None = None,
    ) -> str:
        """Open a new DRAFT session."""
        session_id = str(uuid.uuid4())
        now = datetime.now(UTC)
        self._wh.execute(
            f"INSERT INTO {CONTROL_SCHEMA}.mapping_sessions "
            "(session_id, client_id, source_qualified_table, gold_contract_text, "
            " target_mode, status, conversation_json, created_by, created_at, "
            " updated_at, notes) "
            "VALUES ($id, $c, $sqt, $gct, $tm, $st, $cj, $cb, $ts, $ts, $n)",
            {
                "id": session_id,
                "c": client_id,
                "sqt": source_qualified_table,
                "gct": gold_contract_text,
                "tm": target_mode.value,
                "st": SessionStatus.DRAFT.value,
                "cj": json.dumps([]),
                "cb": created_by,
                "ts": now,
                "n": notes,
            },
        )
        self._audit(
            session_id,
            None,
            SessionStatus.DRAFT,
            created_by,
            f"session created (target_mode={target_mode.value})",
        )
        _log.info(
            "mapper.session.created",
            session_id=session_id,
            client_id=client_id,
            source=source_qualified_table,
            target_mode=target_mode.value,
        )
        return session_id

    def add_turn(
        self,
        session_id: str,
        *,
        role: str,
        text: str,
        actor: str,
        tokens_used: int = 0,
        duration_ms: int = 0,
    ) -> None:
        """Append one conversation turn. Allowed only on DRAFT — once
        a session is in PENDING_REVIEW the conversation is frozen."""
        sess = self._require_draft(session_id)
        new_turn = ConversationTurn(
            role=role,
            text=text,
            ts=datetime.now(UTC),
            tokens_used=tokens_used,
            duration_ms=duration_ms,
        )
        new_conv = [*list(sess.conversation), new_turn]
        self._wh.execute(
            f"UPDATE {CONTROL_SCHEMA}.mapping_sessions "
            "SET conversation_json = $cj, updated_at = $ts "
            "WHERE session_id = $i",
            {
                "cj": json.dumps([t.to_dict() for t in new_conv]),
                "ts": datetime.now(UTC),
                "i": session_id,
            },
        )
        self._audit(
            session_id,
            SessionStatus.DRAFT,
            SessionStatus.DRAFT,
            actor,
            f"turn appended ({role}, {len(text)} chars)",
        )

    def update_source_profile(
        self,
        session_id: str,
        profile: SourceProfile,
        *,
        actor: str,
    ) -> None:
        """Snapshot the profile output into the session — DRAFT-only.

        Stored as JSON so the LLM can be re-prompted with the exact
        profile that produced the current proposal even if the source
        table changes later.
        """
        self._require_draft(session_id)
        self._wh.execute(
            f"UPDATE {CONTROL_SCHEMA}.mapping_sessions "
            "SET source_profile_json = $sp, updated_at = $ts "
            "WHERE session_id = $i",
            {
                "sp": profile.to_json(),
                "ts": datetime.now(UTC),
                "i": session_id,
            },
        )
        self._audit(
            session_id,
            SessionStatus.DRAFT,
            SessionStatus.DRAFT,
            actor,
            f"source profile snapshotted ({len(profile.columns)} cols)",
        )

    def update_gold_contract(
        self,
        session_id: str,
        text: str,
        *,
        actor: str,
    ) -> None:
        """Operator updates the NL Gold contract."""
        self._require_draft(session_id)
        self._wh.execute(
            f"UPDATE {CONTROL_SCHEMA}.mapping_sessions "
            "SET gold_contract_text = $gct, updated_at = $ts "
            "WHERE session_id = $i",
            {
                "gct": text,
                "ts": datetime.now(UTC),
                "i": session_id,
            },
        )
        self._audit(
            session_id,
            SessionStatus.DRAFT,
            SessionStatus.DRAFT,
            actor,
            f"gold contract updated ({len(text)} chars)",
        )

    def update_artifacts(
        self,
        session_id: str,
        *,
        actor: str,
        silver_sql: str | None = None,
        gold_sql: str | None = None,
        onprem_postgres_sql: str | None = None,
        onprem_mssql_sql: str | None = None,
        sample_preview: list[dict[str, Any]] | None = None,
        silver_target_table: str | None = None,
        gold_target_table: str | None = None,
    ) -> None:
        """Update one or more generated artifacts. Pass ``None`` to
        leave a field untouched; pass an empty string to explicitly
        clear one. Allowed only on DRAFT.
        """
        self._require_draft(session_id)
        sets: dict[str, Any] = {"updated_at": datetime.now(UTC)}
        if silver_sql is not None:
            sets["silver_sql"] = silver_sql
        if gold_sql is not None:
            sets["gold_sql"] = gold_sql
        if onprem_postgres_sql is not None:
            sets["onprem_postgres_sql"] = onprem_postgres_sql
        if onprem_mssql_sql is not None:
            sets["onprem_mssql_sql"] = onprem_mssql_sql
        if sample_preview is not None:
            sets["sample_preview_json"] = json.dumps(sample_preview, default=str)
        if silver_target_table is not None:
            sets["silver_target_table"] = silver_target_table
        if gold_target_table is not None:
            sets["gold_target_table"] = gold_target_table

        if len(sets) == 1:  # only updated_at
            return  # nothing real to write
        set_sql = ", ".join(f"{k} = ${k}" for k in sets)
        params = {**sets, "i": session_id}
        self._wh.execute(
            f"UPDATE {CONTROL_SCHEMA}.mapping_sessions SET {set_sql} WHERE session_id = $i",
            params,
        )
        # Audit names just the fields that changed (terse).
        changed = sorted(k for k in sets if k != "updated_at")
        self._audit(
            session_id,
            SessionStatus.DRAFT,
            SessionStatus.DRAFT,
            actor,
            f"artifacts updated: {', '.join(changed)}",
        )

    # ------------------------------------------------------------------
    # WRITES — state transitions
    # ------------------------------------------------------------------

    def submit_for_review(self, session_id: str, actor: str) -> None:
        self._transition(
            session_id,
            SessionStatus.PENDING_REVIEW,
            actor,
            notes="submitted for review",
            extra_sets={"submitted_at": datetime.now(UTC)},
        )

    def approve(self, session_id: str, actor: str, notes: str | None = None) -> None:
        self._transition(
            session_id,
            SessionStatus.APPROVED,
            actor,
            notes=notes or "approved",
            extra_sets={
                "reviewed_by": actor,
                "reviewed_at": datetime.now(UTC),
                "review_notes": notes,
            },
        )

    def reject(self, session_id: str, actor: str, notes: str) -> None:
        self._transition(
            session_id,
            SessionStatus.REJECTED,
            actor,
            notes=notes,
            extra_sets={
                "reviewed_by": actor,
                "reviewed_at": datetime.now(UTC),
                "review_notes": notes,
            },
        )

    def request_changes(self, session_id: str, actor: str, notes: str) -> None:
        """Reviewer kicks back to DRAFT for more revision."""
        self._transition(
            session_id,
            SessionStatus.DRAFT,
            actor,
            notes=notes,
            extra_sets={
                "reviewed_by": actor,
                "reviewed_at": datetime.now(UTC),
                "review_notes": notes,
            },
        )

    def mark_deployed(self, session_id: str, actor: str, deploy_notes: str | None = None) -> None:
        """Phase 13.8 — files written to disk + dbt model registered.

        Approved → DEPLOYED. Once DEPLOYED, the only legal transition is
        ARCHIVED (e.g. retiring the mapping when the source schema
        finally changes for real).
        """
        self._transition(
            session_id,
            SessionStatus.DEPLOYED,
            actor,
            notes=deploy_notes or "deployed to disk",
            extra_sets={"deployed_at": datetime.now(UTC)},
        )

    def archive(self, session_id: str, actor: str, reason: str = "") -> None:
        self._transition(
            session_id,
            SessionStatus.ARCHIVED,
            actor,
            notes=reason or "manually archived",
            extra_sets={"archived_at": datetime.now(UTC)},
        )

    def submit_with_policy(
        self,
        session_id: str,
        *,
        mode: ApprovalMode,
        actor: str,
        approval_notes: str | None = None,
    ) -> None:
        """Phase 10-style HITL walker. AUTO_APPROVE walks
        DRAFT → PENDING_REVIEW → APPROVED in one call. DEPLOYED still
        requires an explicit ``mark_deployed()`` call from the deployer
        (13.8) — auto-approve does NOT auto-write files.
        """
        current = self._require(session_id)
        if mode is ApprovalMode.DRAFT:
            return
        if mode is ApprovalMode.HITL:
            if current.status is SessionStatus.DRAFT:
                self.submit_for_review(session_id, actor=actor)
            return
        if mode is ApprovalMode.AUTO_APPROVE:
            cur = self._require(session_id)
            if cur.status is SessionStatus.DRAFT:
                self.submit_for_review(session_id, actor=actor)
            cur = self._require(session_id)
            if cur.status is SessionStatus.PENDING_REVIEW:
                self.approve(
                    session_id,
                    actor=actor,
                    notes=approval_notes or "auto-approved by policy",
                )
            return
        raise ValueError(f"Unknown ApprovalMode: {mode!r}")

    # ------------------------------------------------------------------
    # INTERNAL
    # ------------------------------------------------------------------

    def _require(self, session_id: str) -> MappingSession:
        s = self.get_session(session_id)
        if s is None:
            raise KeyError(f"Unknown session_id: {session_id!r}")
        return s

    def _require_draft(self, session_id: str) -> MappingSession:
        s = self._require(session_id)
        if s.status is not SessionStatus.DRAFT:
            raise ValueError(
                f"session {session_id!r} is in status {s.status.value} — only DRAFT is editable"
            )
        return s

    def _transition(
        self,
        session_id: str,
        to_status: SessionStatus,
        actor: str,
        notes: str | None = None,
        extra_sets: dict[str, Any] | None = None,
    ) -> None:
        current = self._require(session_id)
        if to_status not in _LEGAL_TRANSITIONS.get(current.status, set()):
            raise ValueError(
                f"illegal session transition {current.status} → {to_status} for {session_id!r}"
            )
        sets: dict[str, Any] = {"status": to_status.value, "updated_at": datetime.now(UTC)}
        if extra_sets:
            sets.update(extra_sets)
        set_sql = ", ".join(f"{k} = ${k}" for k in sets)
        params = {**sets, "i": session_id}
        self._wh.execute(
            f"UPDATE {CONTROL_SCHEMA}.mapping_sessions SET {set_sql} WHERE session_id = $i",
            params,
        )
        self._audit(session_id, current.status, to_status, actor, notes)
        _log.info(
            "mapper.session.transition",
            session_id=session_id,
            from_status=current.status.value,
            to_status=to_status.value,
            actor=actor,
        )

    def _audit(
        self,
        session_id: str,
        from_status: SessionStatus | None,
        to_status: SessionStatus,
        actor: str,
        notes: str | None,
    ) -> None:
        self._wh.execute(
            f"INSERT INTO {CONTROL_SCHEMA}.mapping_session_audit_log "
            "(audit_id, session_id, from_status, to_status, actor, notes) "
            "VALUES ($aid, $sid, $fs, $ts, $a, $n)",
            {
                "aid": str(uuid.uuid4()),
                "sid": session_id,
                "fs": from_status.value if from_status else None,
                "ts": to_status.value,
                "a": actor,
                "n": notes,
            },
        )


# ============================================================================
# ROW MAPPING
# ============================================================================


def _row_to_session(row: dict[str, Any]) -> MappingSession:
    conv_raw = row.get("conversation_json")
    conv: list[ConversationTurn] = []
    if conv_raw:
        try:
            conv = [ConversationTurn.from_dict(d) for d in json.loads(conv_raw)]
        except (json.JSONDecodeError, TypeError, KeyError):
            conv = []

    profile_raw = row.get("source_profile_json")
    profile: dict[str, Any] | None = None
    if profile_raw:
        try:
            profile = json.loads(profile_raw)
        except json.JSONDecodeError:
            profile = None

    sample_raw = row.get("sample_preview_json")
    sample: list[dict[str, Any]] | None = None
    if sample_raw:
        try:
            sample = json.loads(sample_raw)
        except json.JSONDecodeError:
            sample = None

    return MappingSession(
        session_id=row["session_id"],
        client_id=row["client_id"],
        source_qualified_table=row["source_qualified_table"],
        gold_contract_text=row.get("gold_contract_text") or "",
        target_mode=TargetMode(row["target_mode"]),
        status=SessionStatus(row["status"]),
        source_profile=profile,
        conversation=tuple(conv),
        silver_sql=row.get("silver_sql"),
        gold_sql=row.get("gold_sql"),
        onprem_postgres_sql=row.get("onprem_postgres_sql"),
        onprem_mssql_sql=row.get("onprem_mssql_sql"),
        sample_preview=sample,
        silver_target_table=row.get("silver_target_table"),
        gold_target_table=row.get("gold_target_table"),
        created_by=row["created_by"],
        created_at=row["created_at"],
        updated_at=row.get("updated_at"),
        submitted_at=row.get("submitted_at"),
        reviewed_by=row.get("reviewed_by"),
        reviewed_at=row.get("reviewed_at"),
        review_notes=row.get("review_notes"),
        deployed_at=row.get("deployed_at"),
        archived_at=row.get("archived_at"),
        notes=row.get("notes"),
    )
