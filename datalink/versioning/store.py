"""Universal version registry — backend (Phase 16.2 / Wave 2).

Single source of truth for every artifact version across the platform.
Store + diff + restore + tag + notify-upstream-changes — all in one module.
"""

from __future__ import annotations

import json
import os
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from datalink.logging import get_logger
from datalink.quality.control import CONTROL_SCHEMA

_log = get_logger(__name__)


# ---------------------------------------------------------------------------
# Public dataclasses
# ---------------------------------------------------------------------------


class VersionRestoreError(Exception):
    """Raised when restore() fails (artifact not found, scope corrupted)."""


@dataclass
class VersionEvent:
    """One row from CONTROL.version_history."""

    version_event_id: str
    artifact_type: str
    artifact_id: str
    artifact_scope_key: str
    version: int
    change_kind: str  # CREATE | UPDATE | CLONE | RESTORE | AUTO_DERIVED
    snapshot: dict[str, Any]
    is_current: bool
    pinned: bool
    created_at: datetime
    created_by: str

    parent_version: int | None = None
    cloned_from_artifact_id: str | None = None
    cloned_from_version: int | None = None
    upstream_silver_id: str | None = None
    upstream_silver_version: int | None = None
    upstream_gold_id: str | None = None
    upstream_gold_version: int | None = None
    diff_summary: dict[str, Any] | None = None
    change_reason: str | None = None
    notes: str | None = None
    tags: list[str] = field(default_factory=list)


@dataclass
class UpstreamNotification:
    """One row from CONTROL.upstream_update_notifications."""

    notification_id: str
    artifact_type: str
    artifact_id: str
    artifact_scope_key: str
    upstream_artifact_type: str
    upstream_artifact_id: str
    consumed_version: int
    available_version: int
    severity: str
    status: str  # OPEN | ACKED | APPLIED
    created_at: datetime
    acked_at: datetime | None = None
    acked_by: str | None = None
    applied_at: datetime | None = None
    applied_by: str | None = None


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------


def _wh():
    """Lazy warehouse adapter (same pattern as proposals/store)."""
    try:
        from datalink.ui._query import _build_backend

        return _build_backend(readonly=False)
    except Exception:
        from datalink.adapters.factory import build_adapters
        from datalink.config.loader import load_settings

        return build_adapters(load_settings(env=os.environ.get("DL_ENV", "dev"))).warehouse


def _now() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def _to_event(row: dict[str, Any], tags: list[str] | None = None) -> VersionEvent:
    snap_raw = row.get("snapshot")
    diff_raw = row.get("diff_summary")
    return VersionEvent(
        version_event_id=str(row["version_event_id"]),
        artifact_type=str(row["artifact_type"]),
        artifact_id=str(row["artifact_id"]),
        artifact_scope_key=str(row["artifact_scope_key"]),
        version=int(row["version"]),
        change_kind=str(row["change_kind"]),
        snapshot=json.loads(snap_raw) if isinstance(snap_raw, str) else (snap_raw or {}),
        is_current=bool(row.get("is_current", False)),
        pinned=bool(row.get("pinned", False)),
        created_at=row["created_at"],
        created_by=str(row["created_by"]),
        parent_version=row.get("parent_version"),
        cloned_from_artifact_id=row.get("cloned_from_artifact_id"),
        cloned_from_version=row.get("cloned_from_version"),
        upstream_silver_id=row.get("upstream_silver_id"),
        upstream_silver_version=row.get("upstream_silver_version"),
        upstream_gold_id=row.get("upstream_gold_id"),
        upstream_gold_version=row.get("upstream_gold_version"),
        diff_summary=(json.loads(diff_raw) if isinstance(diff_raw, str) else (diff_raw or None)),
        change_reason=row.get("change_reason"),
        notes=row.get("notes"),
        tags=tags or [],
    )


def _next_version(artifact_type: str, artifact_id: str) -> int:
    rows = list(
        _wh().query(
            f"SELECT MAX(version) AS v FROM {CONTROL_SCHEMA}.version_history "
            f"WHERE artifact_type = %(at)s AND artifact_id = %(aid)s",
            {"at": artifact_type, "aid": artifact_id},
        )
    )
    return int(rows[0]["v"]) + 1 if rows and rows[0]["v"] is not None else 1


def _compute_diff(prev_snap: dict[str, Any], new_snap: dict[str, Any]) -> dict[str, Any]:
    """Compute a semantic diff between two snapshots.

    Returns a structure like:
      {
        "added_keys": [...], "removed_keys": [...], "changed_keys": [{"key": k, "old": ..., "new": ...}],
        "added_columns": [...], "removed_columns": [...], "changed_columns": [...]
      }

    Treats top-level dict keys as "fields" and any list-of-dict with a "name"
    key as "columns" (Bronze/Silver/Gold schemas + column lists fit this).
    """
    diff: dict[str, Any] = {
        "added_keys": [],
        "removed_keys": [],
        "changed_keys": [],
        "added_columns": [],
        "removed_columns": [],
        "changed_columns": [],
    }
    if not isinstance(prev_snap, dict):
        prev_snap = {}
    if not isinstance(new_snap, dict):
        new_snap = {}

    p_keys = set(prev_snap.keys())
    n_keys = set(new_snap.keys())
    diff["added_keys"] = sorted(n_keys - p_keys)
    diff["removed_keys"] = sorted(p_keys - n_keys)
    for k in sorted(p_keys & n_keys):
        if prev_snap[k] != new_snap[k]:
            # If it's a list of dicts with "name" → diff column-by-column
            pv = prev_snap[k]
            nv = new_snap[k]
            if (
                isinstance(pv, list)
                and isinstance(nv, list)
                and (not pv or isinstance(pv[0], dict))
                and (not nv or isinstance(nv[0], dict))
            ):
                p_by_name = {d.get("name"): d for d in pv if isinstance(d, dict)}
                n_by_name = {d.get("name"): d for d in nv if isinstance(d, dict)}
                added = sorted(set(n_by_name) - set(p_by_name))
                removed = sorted(set(p_by_name) - set(n_by_name))
                changed = sorted(
                    [n for n in (set(p_by_name) & set(n_by_name)) if p_by_name[n] != n_by_name[n]]
                )
                if added or removed or changed:
                    diff["added_columns"].extend([f"{k}.{a}" for a in added])
                    diff["removed_columns"].extend([f"{k}.{r}" for r in removed])
                    for c in changed:
                        diff["changed_columns"].append(
                            {
                                "key": f"{k}.{c}",
                                "old": p_by_name[c],
                                "new": n_by_name[c],
                            }
                        )
            else:
                diff["changed_keys"].append({"key": k, "old": pv, "new": nv})
    return diff


# ---------------------------------------------------------------------------
# Public API — versioning
# ---------------------------------------------------------------------------


def record_version(
    *,
    artifact_type: str,
    artifact_id: str,
    artifact_scope_key: str,
    snapshot: dict[str, Any],
    change_kind: str,
    created_by: str,
    change_reason: str | None = None,
    cloned_from_artifact_id: str | None = None,
    cloned_from_version: int | None = None,
    upstream_silver_id: str | None = None,
    upstream_silver_version: int | None = None,
    upstream_gold_id: str | None = None,
    upstream_gold_version: int | None = None,
    notes: str | None = None,
    set_as_current: bool = True,
) -> VersionEvent:
    """Persist a new version event.

    Auto-computes the diff against the prior current version (if any).
    Marks this version as ``is_current=True`` and any prior current as
    ``is_current=False`` (unless ``set_as_current=False``).
    """
    version = _next_version(artifact_type, artifact_id)

    # Find prior current to compute diff + parent_version
    prev_rows = list(
        _wh().query(
            f"SELECT version, snapshot FROM {CONTROL_SCHEMA}.version_history "
            f"WHERE artifact_type = %(at)s AND artifact_id = %(aid)s "
            f"  AND is_current = TRUE LIMIT 1",
            {"at": artifact_type, "aid": artifact_id},
        )
    )
    parent_v: int | None = None
    diff_summary: dict[str, Any] | None = None
    if prev_rows:
        parent_v = int(prev_rows[0]["version"])
        try:
            prev_snap_raw = prev_rows[0]["snapshot"]
            prev_snap = (
                json.loads(prev_snap_raw)
                if isinstance(prev_snap_raw, str)
                else (prev_snap_raw or {})
            )
            diff_summary = _compute_diff(prev_snap, snapshot)
        except Exception:
            diff_summary = None

    # Demote prior current
    if set_as_current:
        _wh().execute(
            f"UPDATE {CONTROL_SCHEMA}.version_history SET is_current = FALSE "
            f"WHERE artifact_type = %(at)s AND artifact_id = %(aid)s "
            f"  AND is_current = TRUE",
            {"at": artifact_type, "aid": artifact_id},
        )

    event_id = str(uuid.uuid4())
    _wh().execute(
        f"INSERT INTO {CONTROL_SCHEMA}.version_history "
        f"(version_event_id, artifact_type, artifact_id, artifact_scope_key, "
        f" version, parent_version, cloned_from_artifact_id, cloned_from_version, "
        f" upstream_silver_id, upstream_silver_version, "
        f" upstream_gold_id, upstream_gold_version, "
        f" snapshot, diff_summary, change_kind, change_reason, "
        f" pinned, is_current, created_at, created_by, notes) "
        f"SELECT %(eid)s, %(at)s, %(aid)s, %(sk)s, %(v)s, %(pv)s, %(cfa)s, %(cfv)s, "
        f"       %(usi)s, %(usv)s, %(ugi)s, %(ugv)s, "
        f"       PARSE_JSON(%(snap)s), "
        f"       {'PARSE_JSON(%(diff)s)' if diff_summary is not None else 'NULL'}, "
        f"       %(ck)s, %(cr)s, FALSE, %(cur)s, %(ts)s, %(by)s, %(notes)s",
        {
            "eid": event_id,
            "at": artifact_type,
            "aid": artifact_id,
            "sk": artifact_scope_key,
            "v": version,
            "pv": parent_v,
            "cfa": cloned_from_artifact_id,
            "cfv": cloned_from_version,
            "usi": upstream_silver_id,
            "usv": upstream_silver_version,
            "ugi": upstream_gold_id,
            "ugv": upstream_gold_version,
            "snap": json.dumps(snapshot),
            "diff": json.dumps(diff_summary) if diff_summary is not None else None,
            "ck": change_kind,
            "cr": change_reason,
            "cur": set_as_current,
            "ts": _now(),
            "by": created_by,
            "notes": notes,
        },
    )
    _log.info(
        "versioning.recorded",
        artifact_type=artifact_type,
        artifact_id=artifact_id,
        version=version,
        change_kind=change_kind,
        by=created_by,
    )
    out = get(event_id)
    if out is None:
        raise RuntimeError("version event vanished after insert")
    return out


def get(version_event_id: str) -> VersionEvent | None:
    rows = list(
        _wh().query(
            f"SELECT * FROM {CONTROL_SCHEMA}.version_history WHERE version_event_id = %(eid)s",
            {"eid": version_event_id},
        )
    )
    if not rows:
        return None
    return _to_event(rows[0], tags=_list_event_tags(version_event_id))


def history(
    *,
    artifact_type: str,
    artifact_id: str,
    limit: int = 50,
) -> list[VersionEvent]:
    """Return version history for an artifact (newest first)."""
    rows = list(
        _wh().query(
            f"SELECT * FROM {CONTROL_SCHEMA}.version_history "
            f"WHERE artifact_type = %(at)s AND artifact_id = %(aid)s "
            f"ORDER BY version DESC LIMIT %(l)s",
            {"at": artifact_type, "aid": artifact_id, "l": limit},
        )
    )
    return [_to_event(r, tags=_list_event_tags(r["version_event_id"])) for r in rows]


def diff(version_event_id_a: str, version_event_id_b: str) -> dict[str, Any]:
    """Compute semantic diff between two arbitrary versions."""
    a = get(version_event_id_a)
    b = get(version_event_id_b)
    if a is None or b is None:
        raise VersionRestoreError("Version event(s) not found.")
    return _compute_diff(a.snapshot, b.snapshot)


def restore(
    version_event_id: str, *, restored_by: str, change_reason: str | None = None
) -> VersionEvent:
    """Make a historical version current. Records a new event with
    change_kind=RESTORE so the audit trail is preserved."""
    src = get(version_event_id)
    if src is None:
        raise VersionRestoreError(f"Version {version_event_id} not found.")
    return record_version(
        artifact_type=src.artifact_type,
        artifact_id=src.artifact_id,
        artifact_scope_key=src.artifact_scope_key,
        snapshot=src.snapshot,
        change_kind="RESTORE",
        change_reason=change_reason or f"Restored from v{src.version}",
        created_by=restored_by,
        cloned_from_artifact_id=src.artifact_id,
        cloned_from_version=src.version,
    )


def set_current(version_event_id: str, *, by: str) -> VersionEvent:
    """Promote a specific event to is_current=True; demote prior currents
    on the same artifact. Doesn't create a new row."""
    src = get(version_event_id)
    if src is None:
        raise VersionRestoreError(f"Version {version_event_id} not found.")
    _wh().execute(
        f"UPDATE {CONTROL_SCHEMA}.version_history SET is_current = FALSE "
        f"WHERE artifact_type = %(at)s AND artifact_id = %(aid)s",
        {"at": src.artifact_type, "aid": src.artifact_id},
    )
    _wh().execute(
        f"UPDATE {CONTROL_SCHEMA}.version_history SET is_current = TRUE "
        f"WHERE version_event_id = %(eid)s",
        {"eid": version_event_id},
    )
    _log.info("versioning.set_current", version_event_id=version_event_id, by=by)
    out = get(version_event_id)
    if out is None:
        raise RuntimeError("vanished")
    return out


def pin(version_event_id: str) -> VersionEvent:
    _wh().execute(
        f"UPDATE {CONTROL_SCHEMA}.version_history SET pinned = TRUE "
        f"WHERE version_event_id = %(eid)s",
        {"eid": version_event_id},
    )
    out = get(version_event_id)
    if out is None:
        raise RuntimeError("vanished")
    return out


def unpin(version_event_id: str) -> VersionEvent:
    _wh().execute(
        f"UPDATE {CONTROL_SCHEMA}.version_history SET pinned = FALSE "
        f"WHERE version_event_id = %(eid)s",
        {"eid": version_event_id},
    )
    out = get(version_event_id)
    if out is None:
        raise RuntimeError("vanished")
    return out


# ---------------------------------------------------------------------------
# Tags (reuses CONTROL.proposal_tags catalog)
# ---------------------------------------------------------------------------


def _list_event_tags(version_event_id: str) -> list[str]:
    try:
        rows = list(
            _wh().query(
                f"SELECT t.tag_name "
                f"FROM {CONTROL_SCHEMA}.version_tag_assignments a "
                f"JOIN {CONTROL_SCHEMA}.proposal_tags t ON t.tag_id = a.tag_id "
                f"WHERE a.version_event_id = %(eid)s "
                f"ORDER BY t.tag_name",
                {"eid": version_event_id},
            )
        )
    except Exception:
        return []
    return [str(r["tag_name"]) for r in rows]


def add_tag(*, version_event_id: str, tag_name: str, assigned_by: str) -> None:
    rows = list(
        _wh().query(
            f"SELECT tag_id FROM {CONTROL_SCHEMA}.proposal_tags WHERE tag_name = %(tn)s",
            {"tn": tag_name},
        )
    )
    if not rows:
        raise ValueError(f"Tag {tag_name!r} not in catalog")
    tag_id = rows[0]["tag_id"]
    existing = list(
        _wh().query(
            f"SELECT assignment_id FROM {CONTROL_SCHEMA}.version_tag_assignments "
            f"WHERE version_event_id = %(eid)s AND tag_id = %(tid)s",
            {"eid": version_event_id, "tid": tag_id},
        )
    )
    if existing:
        return
    _wh().execute(
        f"INSERT INTO {CONTROL_SCHEMA}.version_tag_assignments "
        f"(assignment_id, version_event_id, tag_id, assigned_at, assigned_by) "
        f"SELECT %(aid)s, %(eid)s, %(tid)s, %(ts)s, %(by)s",
        {
            "aid": str(uuid.uuid4()),
            "eid": version_event_id,
            "tid": tag_id,
            "ts": _now(),
            "by": assigned_by,
        },
    )


def remove_tag(*, version_event_id: str, tag_name: str, removed_by: str) -> bool:
    rows = list(
        _wh().query(
            f"SELECT a.assignment_id "
            f"FROM {CONTROL_SCHEMA}.version_tag_assignments a "
            f"JOIN {CONTROL_SCHEMA}.proposal_tags t ON t.tag_id = a.tag_id "
            f"WHERE a.version_event_id = %(eid)s AND t.tag_name = %(tn)s",
            {"eid": version_event_id, "tn": tag_name},
        )
    )
    if not rows:
        return False
    _wh().execute(
        f"DELETE FROM {CONTROL_SCHEMA}.version_tag_assignments WHERE assignment_id = %(aid)s",
        {"aid": rows[0]["assignment_id"]},
    )
    return True


# ---------------------------------------------------------------------------
# Upstream-update notifications
# ---------------------------------------------------------------------------


def notify_upstream_update(
    *,
    downstream_artifact_type: str,
    downstream_artifact_id: str,
    downstream_scope_key: str,
    upstream_artifact_type: str,
    upstream_artifact_id: str,
    consumed_version: int,
    new_version: int,
    severity: str = "WARNING",
) -> str:
    """Record that ``new_version`` of an upstream artifact is now LIVE while
    a downstream artifact is still pinned to ``consumed_version``."""
    nid = str(uuid.uuid4())
    _wh().execute(
        f"INSERT INTO {CONTROL_SCHEMA}.upstream_update_notifications "
        f"(notification_id, artifact_type, artifact_id, artifact_scope_key, "
        f" upstream_artifact_type, upstream_artifact_id, "
        f" consumed_version, available_version, severity, status, created_at) "
        f"SELECT %(n)s, %(da)s, %(di)s, %(sk)s, %(ua)s, %(ui)s, %(cv)s, %(av)s, "
        f"       %(s)s, 'OPEN', %(ts)s",
        {
            "n": nid,
            "da": downstream_artifact_type,
            "di": downstream_artifact_id,
            "sk": downstream_scope_key,
            "ua": upstream_artifact_type,
            "ui": upstream_artifact_id,
            "cv": consumed_version,
            "av": new_version,
            "s": severity,
            "ts": _now(),
        },
    )
    return nid


def list_open_notifications(
    *,
    artifact_id: str | None = None,
    limit: int = 50,
) -> list[UpstreamNotification]:
    where = "WHERE status = 'OPEN'"
    params: dict[str, Any] = {}
    if artifact_id:
        where += " AND artifact_id = %(aid)s"
        params["aid"] = artifact_id
    rows = list(
        _wh().query(
            f"SELECT * FROM {CONTROL_SCHEMA}.upstream_update_notifications "
            f"{where} ORDER BY created_at DESC LIMIT %(l)s",
            {**params, "l": limit},
        )
    )
    return [
        UpstreamNotification(
            notification_id=str(r["notification_id"]),
            artifact_type=str(r["artifact_type"]),
            artifact_id=str(r["artifact_id"]),
            artifact_scope_key=str(r["artifact_scope_key"]),
            upstream_artifact_type=str(r["upstream_artifact_type"]),
            upstream_artifact_id=str(r["upstream_artifact_id"]),
            consumed_version=int(r["consumed_version"]),
            available_version=int(r["available_version"]),
            severity=str(r["severity"]),
            status=str(r["status"]),
            created_at=r["created_at"],
            acked_at=r.get("acked_at"),
            acked_by=r.get("acked_by"),
            applied_at=r.get("applied_at"),
            applied_by=r.get("applied_by"),
        )
        for r in rows
    ]


def ack_notification(*, notification_id: str, acked_by: str) -> None:
    _wh().execute(
        f"UPDATE {CONTROL_SCHEMA}.upstream_update_notifications "
        f"SET status = 'ACKED', acked_at = %(ts)s, acked_by = %(by)s "
        f"WHERE notification_id = %(n)s",
        {"n": notification_id, "ts": _now(), "by": acked_by},
    )


def apply_notification(*, notification_id: str, applied_by: str) -> None:
    _wh().execute(
        f"UPDATE {CONTROL_SCHEMA}.upstream_update_notifications "
        f"SET status = 'APPLIED', applied_at = %(ts)s, applied_by = %(by)s "
        f"WHERE notification_id = %(n)s",
        {"n": notification_id, "ts": _now(), "by": applied_by},
    )
