"""Saved-proposal store — backend for Phase 16.1 saved-proposal infrastructure.

Single point of truth for every AI proposal across every Author page. UI calls
``find_active(...)`` on page load to decide whether to show the "saved proposal"
banner; ``save_proposal(...)`` after every LLM round-trip; ``approve(...)`` /
``reject(...)`` / ``archive(...)`` on lifecycle transitions.

Versioning is per (scope_type, scope_key) — every save bumps version by 1.
The previous version is auto-archived on save UNLESS pinned.

Cross-client cloning lineage: when a proposal is created from cloning another
client's proposal, set ``parent_client_id`` and ``cloned_from_proposal_id`` on
``save_proposal`` so the lineage is visible in history + the upstream client
can be notified when the cloned-from proposal gets a new version (Wave 2).
"""

from __future__ import annotations

import json
import os
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

# IMPORTANT: do not import build_adapters at module load. The factory eagerly
# imports every adapter (azurite, sftp, snowflake, …) which means any
# environment missing an optional adapter dep (e.g. control_tower lacks the
# `azure` SDK) blows up at `import datalink.proposals`. Lazy-imported inside
# ``_wh()`` so consumers only pay for adapters they actually use.
from datalink.logging import get_logger
from datalink.quality.control import CONTROL_SCHEMA

_log = get_logger(__name__)


# ---------------------------------------------------------------------------
# Public dataclasses
# ---------------------------------------------------------------------------


class ProposalStatus(StrEnum):
    """Lifecycle states.

    DRAFT     — just-generated, awaiting human action (the active workspace state)
    APPROVED  — human approved; linked_artifact_* now points at downstream artifact
    REJECTED  — human discarded explicitly
    ARCHIVED  — superseded by a newer version (or manually archived) — kept for
                history but not the "active" proposal for the scope
    """

    DRAFT = "DRAFT"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"
    ARCHIVED = "ARCHIVED"


@dataclass
class Proposal:
    """One row from CONTROL.agent_proposals plus its tag list."""

    proposal_id: str
    agent_type: str
    scope_type: str
    scope_key: str
    version: int
    status: ProposalStatus
    pinned: bool
    proposal_payload: dict[str, Any]
    agent_input: dict[str, Any]
    model_id: str
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int
    estimated_cost_usd: float
    latency_ms: int
    created_at: datetime
    created_by: str

    # Optional / lineage
    supersedes_proposal_id: str | None = None
    parent_client_id: str | None = None
    cloned_from_proposal_id: str | None = None
    linked_artifact_type: str | None = None
    linked_artifact_id: str | None = None
    approved_at: datetime | None = None
    approved_by: str | None = None
    rejected_at: datetime | None = None
    rejected_by: str | None = None
    archived_at: datetime | None = None
    archived_by: str | None = None
    notes: str | None = None
    tags: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Cost estimation — Anthropic Claude Haiku 4.5 list price as of 2026-Q2.
# ---------------------------------------------------------------------------

# $/1M tokens. Update when pricing changes.
_PRICING_USD_PER_1M = {
    "claude-haiku-4.5": {"input": 1.00, "output": 5.00},
    "claude-haiku-4-5": {"input": 1.00, "output": 5.00},  # alt slug
    "claude-sonnet-4.5": {"input": 3.00, "output": 15.00},
    "claude-opus-4.5": {"input": 15.00, "output": 75.00},
    # Fallback for unrecognised models — assumes Haiku-tier
    "_default": {"input": 1.00, "output": 5.00},
}


def estimate_cost_usd(*, model_id: str, prompt_tokens: int, completion_tokens: int) -> float:
    """Return the rough USD cost for a single LLM call. Used at save time so
    every proposal row has a cost stamp without needing a live billing query."""
    rate = _PRICING_USD_PER_1M.get(model_id, _PRICING_USD_PER_1M["_default"])
    return (prompt_tokens / 1_000_000) * rate["input"] + (completion_tokens / 1_000_000) * rate[
        "output"
    ]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _wh():
    """Return the active warehouse adapter.

    Prefers the Streamlit-side Snowflake singleton (``datalink.ui._query``)
    when available — that path keeps a long-lived connection across page
    renders, which is the perf design we already pay for. Falls back to
    ``build_adapters`` for non-UI callers (CLI / Airflow / migration scripts).
    Lazy imports both so ``import datalink.proposals`` doesn't pull the
    whole adapter chain.
    """
    # Try the Streamlit-side singleton first — this is the path we want when
    # called from a Streamlit page (control_tower container).
    try:
        from datalink.ui._query import _build_backend

        return _build_backend(readonly=False)
    except Exception:
        # Fallback for non-UI callers (Airflow worker, CLI scripts).
        from datalink.adapters.factory import build_adapters
        from datalink.config.loader import load_settings

        return build_adapters(load_settings(env=os.environ.get("DL_ENV", "dev"))).warehouse


def _now_utc() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def _to_proposal(row: dict[str, Any], tags: list[str] | None = None) -> Proposal:
    """Map a SELECT * row from agent_proposals into a Proposal dataclass."""
    payload_raw = row.get("proposal_payload")
    input_raw = row.get("agent_input")
    payload = json.loads(payload_raw) if isinstance(payload_raw, str) else (payload_raw or {})
    input_ = json.loads(input_raw) if isinstance(input_raw, str) else (input_raw or {})

    return Proposal(
        proposal_id=str(row["proposal_id"]),
        agent_type=str(row["agent_type"]),
        scope_type=str(row["scope_type"]),
        scope_key=str(row["scope_key"]),
        version=int(row["version"]),
        supersedes_proposal_id=row.get("supersedes_proposal_id"),
        parent_client_id=row.get("parent_client_id"),
        cloned_from_proposal_id=row.get("cloned_from_proposal_id"),
        status=ProposalStatus(str(row["status"])),
        pinned=bool(row.get("pinned", False)),
        proposal_payload=payload,
        agent_input=input_,
        model_id=str(row["model_id"]),
        prompt_tokens=int(row["prompt_tokens"]),
        completion_tokens=int(row["completion_tokens"]),
        total_tokens=int(row["total_tokens"]),
        estimated_cost_usd=float(row["estimated_cost_usd"]),
        latency_ms=int(row["latency_ms"]),
        linked_artifact_type=row.get("linked_artifact_type"),
        linked_artifact_id=row.get("linked_artifact_id"),
        created_at=row["created_at"],
        created_by=str(row["created_by"]),
        approved_at=row.get("approved_at"),
        approved_by=row.get("approved_by"),
        rejected_at=row.get("rejected_at"),
        rejected_by=row.get("rejected_by"),
        archived_at=row.get("archived_at"),
        archived_by=row.get("archived_by"),
        notes=row.get("notes"),
        tags=tags or [],
    )


def _next_version(scope_type: str, scope_key: str) -> int:
    """Return the next version number for the given scope (1 if first)."""
    rows = list(
        _wh().query(
            f"SELECT MAX(version) AS v FROM {CONTROL_SCHEMA}.agent_proposals "
            f"WHERE scope_type = %(st)s AND scope_key = %(sk)s",
            {"st": scope_type, "sk": scope_key},
        )
    )
    if not rows or rows[0]["v"] is None:
        return 1
    return int(rows[0]["v"]) + 1


def _archive_predecessors(scope_type: str, scope_key: str, archived_by: str) -> int:
    """Auto-archive any DRAFT or APPROVED in this scope EXCEPT pinned ones.

    Per Jatin's 2026-05-04 directive: re-proposing fully supersedes the
    prior version, regardless of whether it was DRAFT or APPROVED. This
    enforces the "saved proposal pattern" — at any moment exactly ONE
    active proposal per scope (DRAFT or APPROVED), all older versions
    archived, all pinned versions preserved.

    The downstream artifact (pipeline_instance, dq_suite, …) is NOT
    archived here — the proposal row's ``linked_artifact_id`` still
    points at it, but the proposal itself is now ARCHIVED for history
    purposes. Operators wanting to keep an APPROVED version active
    forever should pin() it before re-proposing.
    """
    rows = list(
        _wh().query(
            f"SELECT proposal_id, status FROM {CONTROL_SCHEMA}.agent_proposals "
            f"WHERE scope_type = %(st)s AND scope_key = %(sk)s "
            f"  AND status IN ('DRAFT', 'APPROVED') AND pinned = FALSE",
            {"st": scope_type, "sk": scope_key},
        )
    )
    n = 0
    for r in rows:
        _wh().execute(
            f"UPDATE {CONTROL_SCHEMA}.agent_proposals "
            f"SET status = 'ARCHIVED', archived_at = %(ts)s, archived_by = %(by)s "
            f"WHERE proposal_id = %(pid)s",
            {"ts": _now_utc(), "by": archived_by, "pid": r["proposal_id"]},
        )
        _log.info(
            "proposals.predecessor_archived",
            proposal_id=r["proposal_id"],
            prior_status=r["status"],
            archived_by=archived_by,
        )
        n += 1
    return n


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def save_proposal(
    *,
    agent_type: str,
    scope_type: str,
    scope_key: str,
    payload: dict[str, Any],
    agent_input: dict[str, Any],
    prompt_tokens: int,
    completion_tokens: int,
    model_id: str,
    latency_ms: int,
    created_by: str,
    parent_client_id: str | None = None,
    cloned_from_proposal_id: str | None = None,
    notes: str | None = None,
    auto_archive_predecessors: bool = True,
) -> Proposal:
    """Persist a new proposal. Bumps version per (scope_type, scope_key).

    By default any prior DRAFT for the same scope is auto-archived (since the
    new proposal supersedes it). Set ``auto_archive_predecessors=False`` if
    you need both versions live (rare — usually only for diff-comparison flows).
    """
    proposal_id = str(uuid.uuid4())
    version = _next_version(scope_type, scope_key)
    total_tokens = prompt_tokens + completion_tokens
    cost = estimate_cost_usd(
        model_id=model_id, prompt_tokens=prompt_tokens, completion_tokens=completion_tokens
    )

    # Find the predecessor (if any) for the supersedes link
    pred_rows = list(
        _wh().query(
            f"SELECT proposal_id FROM {CONTROL_SCHEMA}.agent_proposals "
            f"WHERE scope_type = %(st)s AND scope_key = %(sk)s "
            f"ORDER BY version DESC LIMIT 1",
            {"st": scope_type, "sk": scope_key},
        )
    )
    supersedes_id = pred_rows[0]["proposal_id"] if pred_rows else None

    if auto_archive_predecessors:
        n = _archive_predecessors(scope_type, scope_key, archived_by=created_by)
        if n:
            _log.info(
                "proposals.predecessors_archived",
                count=n,
                scope_type=scope_type,
                scope_key=scope_key,
            )

    _wh().execute(
        f"INSERT INTO {CONTROL_SCHEMA}.agent_proposals "
        f"(proposal_id, agent_type, scope_type, scope_key, version, "
        f" supersedes_proposal_id, parent_client_id, cloned_from_proposal_id, "
        f" status, pinned, "
        f" proposal_payload, agent_input, "
        f" model_id, prompt_tokens, completion_tokens, total_tokens, "
        f" estimated_cost_usd, latency_ms, "
        f" created_at, created_by, notes) "
        f"SELECT %(pid)s, %(at)s, %(st)s, %(sk)s, %(v)s, "
        f"       %(sup)s, %(pcid)s, %(cf)s, "
        f"       'DRAFT', FALSE, "
        f"       PARSE_JSON(%(pp)s), PARSE_JSON(%(ai)s), "
        f"       %(mid)s, %(pt)s, %(ct)s, %(tt)s, "
        f"       %(cost)s, %(lat)s, "
        f"       %(ts)s, %(by)s, %(notes)s",
        {
            "pid": proposal_id,
            "at": agent_type,
            "st": scope_type,
            "sk": scope_key,
            "v": version,
            "sup": supersedes_id,
            "pcid": parent_client_id,
            "cf": cloned_from_proposal_id,
            "pp": json.dumps(payload),
            "ai": json.dumps(agent_input),
            "mid": model_id,
            "pt": prompt_tokens,
            "ct": completion_tokens,
            "tt": total_tokens,
            "cost": cost,
            "lat": latency_ms,
            "ts": _now_utc(),
            "by": created_by,
            "notes": notes,
        },
    )
    _log.info(
        "proposals.saved",
        proposal_id=proposal_id,
        scope_type=scope_type,
        scope_key=scope_key,
        version=version,
        agent_type=agent_type,
        total_tokens=total_tokens,
        cost=cost,
    )
    rows = list(
        _wh().query(
            f"SELECT * FROM {CONTROL_SCHEMA}.agent_proposals WHERE proposal_id = %(pid)s",
            {"pid": proposal_id},
        )
    )
    return _to_proposal(rows[0])


def find_active(*, scope_type: str, scope_key: str) -> Proposal | None:
    """Return the latest DRAFT or APPROVED proposal for the given scope.

    Returns None if no proposal exists, or the most-recent one is REJECTED/ARCHIVED.
    Use this on page load to decide whether to show the "saved proposal" banner.
    """
    rows = list(
        _wh().query(
            f"SELECT * FROM {CONTROL_SCHEMA}.agent_proposals "
            f"WHERE scope_type = %(st)s AND scope_key = %(sk)s "
            f"  AND status IN ('DRAFT', 'APPROVED') "
            f"ORDER BY version DESC LIMIT 1",
            {"st": scope_type, "sk": scope_key},
        )
    )
    if not rows:
        return None
    p = _to_proposal(rows[0])
    p.tags = list_tags(p.proposal_id)
    return p


def history(*, scope_type: str, scope_key: str, limit: int = 50) -> list[Proposal]:
    """Return every proposal for the scope (newest first), with tags loaded."""
    rows = list(
        _wh().query(
            f"SELECT * FROM {CONTROL_SCHEMA}.agent_proposals "
            f"WHERE scope_type = %(st)s AND scope_key = %(sk)s "
            f"ORDER BY version DESC LIMIT %(l)s",
            {"st": scope_type, "sk": scope_key, "l": limit},
        )
    )
    out: list[Proposal] = []
    for r in rows:
        p = _to_proposal(r)
        p.tags = list_tags(p.proposal_id)
        out.append(p)
    return out


def get(proposal_id: str) -> Proposal | None:
    """Fetch a specific proposal by ID."""
    rows = list(
        _wh().query(
            f"SELECT * FROM {CONTROL_SCHEMA}.agent_proposals WHERE proposal_id = %(pid)s",
            {"pid": proposal_id},
        )
    )
    if not rows:
        return None
    p = _to_proposal(rows[0])
    p.tags = list_tags(p.proposal_id)
    return p


def approve(
    proposal_id: str,
    *,
    approved_by: str,
    linked_artifact_type: str,
    linked_artifact_id: str,
    notes: str | None = None,
) -> Proposal:
    """Mark APPROVED + link to downstream artifact."""
    _wh().execute(
        f"UPDATE {CONTROL_SCHEMA}.agent_proposals "
        f"SET status = 'APPROVED', "
        f"    approved_at = %(ts)s, approved_by = %(by)s, "
        f"    linked_artifact_type = %(lat)s, linked_artifact_id = %(lai)s, "
        f"    notes = COALESCE(%(notes)s, notes) "
        f"WHERE proposal_id = %(pid)s",
        {
            "ts": _now_utc(),
            "by": approved_by,
            "lat": linked_artifact_type,
            "lai": linked_artifact_id,
            "notes": notes,
            "pid": proposal_id,
        },
    )
    _log.info(
        "proposals.approved",
        proposal_id=proposal_id,
        approved_by=approved_by,
        linked=f"{linked_artifact_type}:{linked_artifact_id}",
    )
    out = get(proposal_id)
    if out is None:
        raise RuntimeError(f"Proposal {proposal_id} vanished after approve()")
    return out


def reject(proposal_id: str, *, rejected_by: str, notes: str | None = None) -> Proposal:
    _wh().execute(
        f"UPDATE {CONTROL_SCHEMA}.agent_proposals "
        f"SET status = 'REJECTED', rejected_at = %(ts)s, rejected_by = %(by)s, "
        f"    notes = COALESCE(%(notes)s, notes) "
        f"WHERE proposal_id = %(pid)s",
        {"ts": _now_utc(), "by": rejected_by, "notes": notes, "pid": proposal_id},
    )
    _log.info("proposals.rejected", proposal_id=proposal_id, rejected_by=rejected_by)
    out = get(proposal_id)
    if out is None:
        raise RuntimeError(f"Proposal {proposal_id} vanished after reject()")
    return out


def archive(proposal_id: str, *, archived_by: str) -> Proposal:
    _wh().execute(
        f"UPDATE {CONTROL_SCHEMA}.agent_proposals "
        f"SET status = 'ARCHIVED', archived_at = %(ts)s, archived_by = %(by)s "
        f"WHERE proposal_id = %(pid)s",
        {"ts": _now_utc(), "by": archived_by, "pid": proposal_id},
    )
    _log.info("proposals.archived", proposal_id=proposal_id, archived_by=archived_by)
    out = get(proposal_id)
    if out is None:
        raise RuntimeError(f"Proposal {proposal_id} vanished after archive()")
    return out


def pin(proposal_id: str) -> Proposal:
    """Pin a proposal — protects it from auto-archive on next save_proposal()."""
    _wh().execute(
        f"UPDATE {CONTROL_SCHEMA}.agent_proposals SET pinned = TRUE WHERE proposal_id = %(pid)s",
        {"pid": proposal_id},
    )
    out = get(proposal_id)
    if out is None:
        raise RuntimeError(f"Proposal {proposal_id} vanished after pin()")
    return out


def unpin(proposal_id: str) -> Proposal:
    _wh().execute(
        f"UPDATE {CONTROL_SCHEMA}.agent_proposals SET pinned = FALSE WHERE proposal_id = %(pid)s",
        {"pid": proposal_id},
    )
    out = get(proposal_id)
    if out is None:
        raise RuntimeError(f"Proposal {proposal_id} vanished after unpin()")
    return out


# ---------------------------------------------------------------------------
# Role enforcement (stub today — Wave 3 plugs in real RBAC)
# ---------------------------------------------------------------------------


class TagRoleError(PermissionError):
    """Raised when a tag with ``requires_role`` is applied without the role."""


def _actor_roles(actor: str) -> set[str]:
    """Return the set of roles held by ``actor``.

    Stub today — every actor gets every role so demos don't break. Wave 3
    replaces this with a real RBAC lookup against an authentication
    provider (Azure AD groups / Okta groups / etc.). The call site
    (``_check_role_required``) is already in place so when Wave 3 ships,
    no other code needs to change.

    Common actor formats:
      ``ui:aetna``       — Streamlit page tied to a tenant
      ``api:airflow``    — backend service-account
      ``cli:jatin``      — operator at the terminal
      ``system:phase16`` — internal migration / housekeeping
    """
    # TODO Wave 3 — replace with real role lookup.
    # For now return all roles so nothing blocks. Audit in tag_assigned
    # logs makes it discoverable post-hoc which actor used which role.
    return {"data-steward", "privacy-officer", "compliance-officer", "operator", "admin"}


def _check_role_required(tag_name: str, actor: str) -> None:
    """Raise :class:`TagRoleError` if the tag's ``requires_role`` isn't held.

    Lookups the tag's ``requires_role`` from the catalog. If NULL, anyone
    can apply. If set, the actor's role set (from ``_actor_roles``) must
    include it. Logs the check either way for audit.
    """
    rows = list(
        _wh().query(
            f"SELECT requires_role FROM {CONTROL_SCHEMA}.proposal_tags WHERE tag_name = %(tn)s",
            {"tn": tag_name},
        )
    )
    if not rows:
        return  # tag not found — handled by caller
    required = rows[0].get("requires_role")
    if not required:
        return  # public tag, anyone can apply
    held = _actor_roles(actor)
    if required not in held:
        raise TagRoleError(
            f"Tag '{tag_name}' requires role '{required}'. Actor '{actor}' "
            f"only has roles: {sorted(held)}"
        )
    _log.info(
        "proposals.role_check_passed",
        tag_name=tag_name,
        actor=actor,
        required_role=required,
    )


# ---------------------------------------------------------------------------
# Tag operations
# ---------------------------------------------------------------------------


def all_tags() -> list[dict[str, Any]]:
    """Return the entire tag catalog (system + custom), newest first."""
    return list(
        _wh().query(
            f"SELECT tag_id, tag_name, tag_category, color_hex, description, "
            f"       requires_role, is_system, created_at, created_by "
            f"FROM {CONTROL_SCHEMA}.proposal_tags "
            f"ORDER BY tag_category, tag_name"
        )
    )


def list_tags(proposal_id: str) -> list[str]:
    """Return tag NAMES applied to a proposal (sorted alphabetically)."""
    rows = list(
        _wh().query(
            f"SELECT t.tag_name "
            f"FROM {CONTROL_SCHEMA}.proposal_tag_assignments a "
            f"JOIN {CONTROL_SCHEMA}.proposal_tags t ON t.tag_id = a.tag_id "
            f"WHERE a.proposal_id = %(pid)s "
            f"ORDER BY t.tag_name",
            {"pid": proposal_id},
        )
    )
    return [str(r["tag_name"]) for r in rows]


def add_tag(*, proposal_id: str, tag_name: str, assigned_by: str) -> None:
    """Apply a tag (by name) to a proposal. Idempotent — already-assigned is a no-op.

    Raises :class:`TagRoleError` if the tag has ``requires_role`` and the
    actor doesn't hold it. (Today every actor holds every role; Wave 3
    plugs in real RBAC — see ``_actor_roles`` docstring.)
    """
    rows = list(
        _wh().query(
            f"SELECT tag_id FROM {CONTROL_SCHEMA}.proposal_tags WHERE tag_name = %(tn)s",
            {"tn": tag_name},
        )
    )
    if not rows:
        raise ValueError(
            f"Tag '{tag_name}' is not in the catalog. Use all_tags() to list valid names."
        )
    # Role enforcement — Wave 3 plugs in real RBAC; today this is permissive.
    _check_role_required(tag_name, assigned_by)
    tag_id = rows[0]["tag_id"]
    existing = list(
        _wh().query(
            f"SELECT assignment_id FROM {CONTROL_SCHEMA}.proposal_tag_assignments "
            f"WHERE proposal_id = %(pid)s AND tag_id = %(tid)s",
            {"pid": proposal_id, "tid": tag_id},
        )
    )
    if existing:
        return  # already assigned
    _wh().execute(
        f"INSERT INTO {CONTROL_SCHEMA}.proposal_tag_assignments "
        f"(assignment_id, proposal_id, tag_id, assigned_at, assigned_by) "
        f"SELECT %(aid)s, %(pid)s, %(tid)s, %(ts)s, %(by)s",
        {
            "aid": str(uuid.uuid4()),
            "pid": proposal_id,
            "tid": tag_id,
            "ts": _now_utc(),
            "by": assigned_by,
        },
    )
    _log.info("proposals.tag_added", proposal_id=proposal_id, tag_name=tag_name)


def remove_tag(*, proposal_id: str, tag_name: str, removed_by: str) -> bool:
    """Remove a tag from a proposal. Returns True if removed, False if not present."""
    rows = list(
        _wh().query(
            f"SELECT a.assignment_id "
            f"FROM {CONTROL_SCHEMA}.proposal_tag_assignments a "
            f"JOIN {CONTROL_SCHEMA}.proposal_tags t ON t.tag_id = a.tag_id "
            f"WHERE a.proposal_id = %(pid)s AND t.tag_name = %(tn)s",
            {"pid": proposal_id, "tn": tag_name},
        )
    )
    if not rows:
        return False
    _wh().execute(
        f"DELETE FROM {CONTROL_SCHEMA}.proposal_tag_assignments WHERE assignment_id = %(aid)s",
        {"aid": rows[0]["assignment_id"]},
    )
    _log.info(
        "proposals.tag_removed", proposal_id=proposal_id, tag_name=tag_name, removed_by=removed_by
    )
    return True
