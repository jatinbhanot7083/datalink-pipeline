"""Data Model Designer — single-shot bulk fetch + edit-buffer + submit-all
infrastructure (Phase 17.6).

The whole page is built around three rules the operator demanded:

  1. **One server round-trip on page load.**  Every grid, picker,
     drill-down reads from this in-memory cache.  Filters, sorts,
     selections, expanders all operate on the cached snapshot —
     no Snowflake call.

  2. **Edits stay client-side until "Submit all changes".**  Per-row
     dirty buffer in ``st.session_state``.  Visual badges show
     unsaved state.  Nothing is written to Snowflake until the
     operator explicitly batches the submit.

  3. **Optimistic concurrency on submit.**  Each cached row carries
     the ``version`` it was loaded at.  On submit, the server
     compares against current.  If another user advanced the row
     meanwhile, the submit is REJECTED with a forced-review
     diff (industry pattern for collaborative authoring).

Public API
----------

    from datalink.ui import _dmd_data as dmd

    snap = dmd.get_snapshot()       # cached, refetches every 60s OR on
                                     # invalidate()
    dmd.invalidate()                 # clear cache, force next get_snapshot()
                                     # to refetch
    dmd.is_dirty()                   # bool — any unsaved edits buffered?
    dmd.dirty_count()                # how many entities touched
    dmd.stash_edit(kind, id, patch)  # buffer one edit
    dmd.discard_edits()              # drop buffer (Cancel button)
    dmd.submit_all()                 # apply with optimistic-concurrency
                                     # check, returns SubmitReport
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Literal

import streamlit as st

from datalink.quality.control import CONTROL_SCHEMA
from datalink.ui._query import warehouse_ctx

EditKind = Literal[
    "silver_status",
    "gold_status",
    "silver_archive",
    "gold_archive",
    "clone_silver",
    "clone_gold",
]

_SNAP_KEY = "__dmd_snapshot__"
_DIRTY_KEY = "__dmd_dirty_buffer__"


@dataclass
class Snapshot:
    """All data the Data Model Designer page needs, fetched in one pass."""

    bronze_datasets: list[dict[str, Any]] = field(default_factory=list)
    silver_schemas: list[dict[str, Any]] = field(default_factory=list)
    gold_schemas: list[dict[str, Any]] = field(default_factory=list)
    silver_columns_by_dataset: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    gold_fields_by_dataset: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    distinct_clients: list[str] = field(default_factory=list)
    # Phase 22 — product-line scoping.  Maps dataset_code → list of
    # product_line_codes (e.g. {'membership': ['CC', 'E360', 'RBN']}).
    product_lines_by_dataset: dict[str, list[str]] = field(default_factory=dict)
    loaded_at: str = ""

    @property
    def silver_by_id(self) -> dict[str, dict[str, Any]]:
        return {str(s["silver_dataset_id"]): s for s in self.silver_schemas}

    @property
    def gold_by_id(self) -> dict[str, dict[str, Any]]:
        return {str(g["gold_dataset_id"]): g for g in self.gold_schemas}

    def silver_for(
        self, *, scope_owner: str, dataset_code: str, exclude_archived: bool = True
    ) -> list[dict[str, Any]]:
        """Filter silver_schemas to a specific (scope, dataset) cell."""
        out = []
        for s in self.silver_schemas:
            if str(s.get("scope_owner")) not in (scope_owner, _legacy_alias(scope_owner)):
                continue
            if str(s.get("dataset_code")) != dataset_code:
                continue
            if exclude_archived and str(s.get("status")) == "ARCHIVED":
                continue
            out.append(s)
        return out

    def gold_for(
        self, *, scope_owner: str, dataset_code: str, exclude_archived: bool = True
    ) -> list[dict[str, Any]]:
        out = []
        for g in self.gold_schemas:
            if str(g.get("scope_owner")) not in (scope_owner, _legacy_alias(scope_owner)):
                continue
            if str(g.get("dataset_code")) != dataset_code:
                continue
            if exclude_archived and str(g.get("status")) == "ARCHIVED":
                continue
            out.append(g)
        return out


def _legacy_alias(scope_owner: str) -> str:
    """During the 17.1 transition, accept both forms when filtering."""
    return "__global__" if scope_owner == "GLOBAL_CORP" else scope_owner


@dataclass
class EditPatch:
    """One pending edit. Buffer key = (kind, entity_id)."""

    kind: EditKind
    entity_id: str
    payload: dict[str, Any]
    base_version: int  # what we read at snapshot time — used for OCC check
    base_status: str
    queued_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())


@dataclass
class SubmitOutcome:
    kind: EditKind
    entity_id: str
    status: Literal["applied", "conflict", "error"]
    detail: str = ""
    new_version: int | None = None


@dataclass
class SubmitReport:
    outcomes: list[SubmitOutcome] = field(default_factory=list)
    started_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())
    finished_at: str | None = None

    @property
    def applied_count(self) -> int:
        return sum(1 for o in self.outcomes if o.status == "applied")

    @property
    def conflict_count(self) -> int:
        return sum(1 for o in self.outcomes if o.status == "conflict")

    @property
    def error_count(self) -> int:
        return sum(1 for o in self.outcomes if o.status == "error")

    @property
    def all_clean(self) -> bool:
        return self.conflict_count == 0 and self.error_count == 0


# ---------------------------------------------------------------------------
# Bulk fetch — single Snowflake round-trip for the entire page
# ---------------------------------------------------------------------------


@st.cache_data(ttl=60, show_spinner="Loading Data Model Designer data…")
def _fetch_snapshot_uncached() -> Snapshot:
    """Pull everything the DMD page needs. Streamlit caches the result for
    60 s OR until ``invalidate()`` is called."""
    snap = Snapshot()
    with warehouse_ctx(readonly=True) as wh:
        # 1. Bronze catalog datasets (the 33 canonical ones)
        snap.bronze_datasets = list(
            wh.query(
                f"SELECT * FROM {CONTROL_SCHEMA}.global_bronze_catalog_datasets "
                f"ORDER BY display_name"
            )
        )

        # 2. ALL silver schemas — every status, every scope_owner.
        # Phase 17.6 — pull AI fields too (ai_proposal_json, ai_rationale,
        # ai_token_count, ai_latency_ms) so View Schema's AI Proposal tab
        # has something to render.  Also pull import_format + is_overkill
        # for completeness so the inspector can surface them.
        snap.silver_schemas = list(
            wh.query(
                f"""
                SELECT silver_dataset_id, dataset_code, silver_pattern, version,
                       status, silver_anchor, source, scope_owner, import_format,
                       is_overkill_flag, ai_proposal_json, ai_rationale,
                       ai_token_count, ai_latency_ms,
                       forked_from_global_version, created_by, created_at,
                       submitted_at, approved_by, approved_at, archived_at, notes
                  FROM {CONTROL_SCHEMA}.global_silver_schema_datasets
                """
            )
        )

        # 3. ALL gold schemas — same AI-fields uplift as silver above.
        snap.gold_schemas = list(
            wh.query(
                f"""
                SELECT gold_dataset_id, dataset_code, gold_table_name, version,
                       status, gold_anchor, source, scope_owner, import_format,
                       ai_proposal_json, ai_rationale, ai_token_count,
                       ai_latency_ms,
                       forked_from_global_version,
                       semver_major, semver_minor, compatibility_class,
                       parent_version, migration_window_days,
                       created_by, created_at, approved_by, approved_at,
                       archived_at, notes
                  FROM {CONTROL_SCHEMA}.global_gold_schema_datasets
                """
            )
        )

        # 4. Silver columns — bulk pull (one query, group in Python)
        try:
            silver_cols = list(
                wh.query(
                    f"SELECT silver_dataset_id, column_order, column_name AS gold_column_name, "
                    f"       logical_type, nullable, is_business_key, is_pii, is_phi, "
                    f"       description "
                    f"FROM {CONTROL_SCHEMA}.global_silver_schema_columns "
                    f"ORDER BY silver_dataset_id, column_order"
                )
            )
        except Exception:
            silver_cols = []
        for c in silver_cols:
            sid = str(c.get("silver_dataset_id"))
            snap.silver_columns_by_dataset.setdefault(sid, []).append(c)

        # 5. Gold fields — same pattern
        try:
            gold_fields = list(
                wh.query(
                    f"SELECT gold_dataset_id, column_order, gold_column_name, "
                    f"       logical_type, nullable, is_business_key, is_pii, is_phi, "
                    f"       description, anchor_reference "
                    f"FROM {CONTROL_SCHEMA}.global_gold_schema_fields "
                    f"ORDER BY gold_dataset_id, column_order"
                )
            )
        except Exception:
            gold_fields = []
        for f_row in gold_fields:
            gid = str(f_row.get("gold_dataset_id"))
            snap.gold_fields_by_dataset.setdefault(gid, []).append(f_row)

        # 6. Distinct clients — three-source UNION so the dropdown always has
        # SOMETHING to show, even on a fresh Snowflake account that hasn't
        # onboarded anyone yet:
        #   (a) scope_owner on existing silver/gold rows
        #   (b) client_id on existing pipeline_instances rows
        #   (c) seed list from data/generated/ — the canonical demo roster
        #       that ships with the repo
        # Always excludes the GLOBAL_CORP pseudo-client + 'default' (legacy
        # single-tenant marker).
        clients: set[str] = set()
        for s in snap.silver_schemas:
            so = str(s.get("scope_owner") or "")
            if so and so not in ("GLOBAL_CORP", "__global__", "default"):
                clients.add(so)
        for g in snap.gold_schemas:
            so = str(g.get("scope_owner") or "")
            if so and so not in ("GLOBAL_CORP", "__global__", "default"):
                clients.add(so)
        try:
            for r in wh.query(
                f"SELECT DISTINCT client_id FROM {CONTROL_SCHEMA}.client_pipeline_instances "
                f"WHERE client_id NOT IN ('GLOBAL_CORP','__global__','default')"
            ):
                cid = str(r.get("client_id") or "")
                if cid:
                    clients.add(cid)
        except Exception:
            pass
        # Seed list from data/generated/ — survives a fresh DB.
        try:
            from pathlib import Path

            _data_gen = Path(__file__).resolve().parents[2] / "data" / "generated"
            if _data_gen.is_dir():
                for child in _data_gen.iterdir():
                    if not child.is_dir():
                        continue
                    nm = child.name.lower()
                    if nm in ("global_corp", "__global__", "default"):
                        continue
                    clients.add(nm)
        except Exception:
            pass
        # NOTE: previously hardcoded `clients.add("aetna")` here as a
        # demo placeholder.  Removed in May 2026 — it caused the DMD page
        # to show "Total Bronze LIVE clients = 1" + an "AETNA — no
        # authored schemas yet" expander even after a fresh wipe.  If you
        # need a baseline tenant for smoke tests, register it via
        # CONTROL.client_pipeline_instances (or drop a folder under
        # data/generated/aetna/), don't hardcode it here.
        snap.distinct_clients = sorted(clients)

        # 7. Product-line scoping (Phase 22).  CONTROL.dataset_product_lines
        # may not exist on accounts that haven't run migration 22 yet — be
        # defensive.
        try:
            for r in wh.query(
                f"SELECT dataset_code, product_line_code "
                f"FROM {CONTROL_SCHEMA}.dataset_product_lines "
                f"ORDER BY dataset_code, product_line_code"
            ):
                ds = str(r.get("dataset_code") or "")
                pl = str(r.get("product_line_code") or "")
                if ds and pl:
                    snap.product_lines_by_dataset.setdefault(ds, []).append(pl)
        except Exception:
            # Migration not applied — silently leave the map empty.
            pass

    snap.loaded_at = datetime.now(UTC).isoformat()
    return snap


def get_snapshot() -> Snapshot:
    """Return the page's cached data snapshot. First call this turn fetches
    from Snowflake; subsequent calls in the same script run reuse it via
    Streamlit's @st.cache_data."""
    return _fetch_snapshot_uncached()


def invalidate() -> None:
    """Drop the cached snapshot. Next get_snapshot() will refetch."""
    _fetch_snapshot_uncached.clear()


# ---------------------------------------------------------------------------
# Edit buffer — per-session, never reaches the server until submit_all()
# ---------------------------------------------------------------------------


def _buffer() -> dict[tuple[str, str], EditPatch]:
    if _DIRTY_KEY not in st.session_state:
        st.session_state[_DIRTY_KEY] = {}
    return st.session_state[_DIRTY_KEY]


def is_dirty() -> bool:
    return bool(_buffer())


def dirty_count() -> int:
    return len(_buffer())


def list_dirty() -> list[EditPatch]:
    return list(_buffer().values())


def stash_edit(
    *,
    kind: EditKind,
    entity_id: str,
    payload: dict[str, Any],
    base_version: int,
    base_status: str,
) -> None:
    """Buffer one edit. Last write wins for the same (kind, entity_id)."""
    _buffer()[(kind, entity_id)] = EditPatch(
        kind=kind,
        entity_id=entity_id,
        payload=payload,
        base_version=base_version,
        base_status=base_status,
    )


def discard_edits() -> int:
    n = dirty_count()
    st.session_state[_DIRTY_KEY] = {}
    return n


def is_buffered(kind: EditKind, entity_id: str) -> bool:
    return (kind, entity_id) in _buffer()


# ---------------------------------------------------------------------------
# Submit-all — batched server write with optimistic-concurrency check
# ---------------------------------------------------------------------------


def _check_silver_version(wh, silver_dataset_id: str) -> tuple[int, str] | None:
    rows = list(
        wh.query(
            f"SELECT version, status FROM {CONTROL_SCHEMA}.global_silver_schema_datasets "
            f"WHERE silver_dataset_id = $id",
            {"id": silver_dataset_id},
        )
    )
    if not rows:
        return None
    r = rows[0]
    return int(r.get("version") or 0), str(r.get("status") or "")


def _check_gold_version(wh, gold_dataset_id: str) -> tuple[int, str] | None:
    rows = list(
        wh.query(
            f"SELECT version, status FROM {CONTROL_SCHEMA}.global_gold_schema_datasets "
            f"WHERE gold_dataset_id = $id",
            {"id": gold_dataset_id},
        )
    )
    if not rows:
        return None
    r = rows[0]
    return int(r.get("version") or 0), str(r.get("status") or "")


def submit_all() -> SubmitReport:
    """Apply every buffered edit with optimistic-concurrency check.

    For each patch:
      * Read the row's CURRENT version + status from Snowflake.
      * Compare against base_version + base_status the patch carries.
      * If matched → apply the patch, mark 'applied'.
      * If drifted → mark 'conflict' (caller surfaces a forced-review modal).

    If ANY patch comes back 'conflict', the buffer is preserved (operator
    must resolve before re-submitting).  If all clean, buffer is dropped
    and snapshot is invalidated so the next render shows fresh data.
    """
    report = SubmitReport()
    buffered = list(_buffer().values())
    if not buffered:
        report.finished_at = datetime.now(UTC).isoformat()
        return report

    with warehouse_ctx(readonly=False) as wh:
        for patch in buffered:
            try:
                if patch.kind in ("silver_status", "silver_archive"):
                    cur = _check_silver_version(wh, patch.entity_id)
                elif patch.kind in ("gold_status", "gold_archive"):
                    cur = _check_gold_version(wh, patch.entity_id)
                elif patch.kind in ("clone_silver", "clone_gold"):
                    # Clones don't carry an entity_id we own yet — apply unconditionally.
                    cur = (patch.base_version, patch.base_status)
                else:
                    report.outcomes.append(
                        SubmitOutcome(
                            kind=patch.kind,
                            entity_id=patch.entity_id,
                            status="error",
                            detail=f"Unknown edit kind {patch.kind!r}",
                        )
                    )
                    continue

                if cur is None:
                    report.outcomes.append(
                        SubmitOutcome(
                            kind=patch.kind,
                            entity_id=patch.entity_id,
                            status="error",
                            detail="Source row no longer exists",
                        )
                    )
                    continue

                cur_version, cur_status = cur
                if cur_version != patch.base_version or cur_status != patch.base_status:
                    report.outcomes.append(
                        SubmitOutcome(
                            kind=patch.kind,
                            entity_id=patch.entity_id,
                            status="conflict",
                            detail=(
                                f"Loaded version={patch.base_version} "
                                f"status={patch.base_status!r}; current is "
                                f"version={cur_version} status={cur_status!r}. "
                                "Forced review required."
                            ),
                        )
                    )
                    continue

                # Apply the patch
                _apply_patch(wh, patch)
                report.outcomes.append(
                    SubmitOutcome(
                        kind=patch.kind,
                        entity_id=patch.entity_id,
                        status="applied",
                        new_version=cur_version,
                    )
                )
            except Exception as exc:
                report.outcomes.append(
                    SubmitOutcome(
                        kind=patch.kind,
                        entity_id=patch.entity_id,
                        status="error",
                        detail=f"{type(exc).__name__}: {str(exc)[:200]}",
                    )
                )

    report.finished_at = datetime.now(UTC).isoformat()

    # If all clean, clear buffer + invalidate snapshot.
    if report.all_clean:
        st.session_state[_DIRTY_KEY] = {}
        invalidate()
    return report


def _apply_patch(wh, patch: EditPatch) -> None:
    """Translate a buffered patch into the corresponding write."""
    if patch.kind == "silver_status":
        new_status = str(patch.payload["new_status"])
        set_clauses = ["status = $st"]
        params = {"id": patch.entity_id, "st": new_status}
        if new_status == "APPROVED":
            set_clauses.append("approved_at = CURRENT_TIMESTAMP()")
            set_clauses.append("approved_by = $by")
            params["by"] = str(patch.payload.get("actor", "ui:dmd"))
        wh.execute(
            f"UPDATE {CONTROL_SCHEMA}.global_silver_schema_datasets "
            f"SET {', '.join(set_clauses)} "
            f"WHERE silver_dataset_id = $id",
            params,
        )
    elif patch.kind == "gold_status":
        new_status = str(patch.payload["new_status"])
        set_clauses = ["status = $st"]
        params = {"id": patch.entity_id, "st": new_status}
        if new_status == "APPROVED":
            set_clauses.append("approved_at = CURRENT_TIMESTAMP()")
            set_clauses.append("approved_by = $by")
            params["by"] = str(patch.payload.get("actor", "ui:dmd"))
        wh.execute(
            f"UPDATE {CONTROL_SCHEMA}.global_gold_schema_datasets "
            f"SET {', '.join(set_clauses)} "
            f"WHERE gold_dataset_id = $id",
            params,
        )
    elif patch.kind == "silver_archive":
        wh.execute(
            f"UPDATE {CONTROL_SCHEMA}.global_silver_schema_datasets "
            f"SET status = 'ARCHIVED', archived_at = CURRENT_TIMESTAMP() "
            f"WHERE silver_dataset_id = $id",
            {"id": patch.entity_id},
        )
    elif patch.kind == "gold_archive":
        wh.execute(
            f"UPDATE {CONTROL_SCHEMA}.global_gold_schema_datasets "
            f"SET status = 'ARCHIVED', archived_at = CURRENT_TIMESTAMP() "
            f"WHERE gold_dataset_id = $id",
            {"id": patch.entity_id},
        )
    elif patch.kind in ("clone_silver", "clone_gold"):
        # Clone implementation lives in the page itself (uses uuid + multi-INSERT).
        # The patch carries a pre-built callable in payload["apply"] that takes the wh.
        applier = patch.payload.get("apply")
        if callable(applier):
            applier(wh)
        else:
            raise RuntimeError(
                "clone patch missing 'apply' callable in payload — bug in the UI code"
            )


def serialize_buffer() -> str:
    """JSON-serialise the dirty buffer for debug/inspection. Excludes
    non-serialisable bits (like callable cloners)."""
    out = []
    for patch in _buffer().values():
        safe_payload = {
            k: v
            for k, v in patch.payload.items()
            if isinstance(v, (str, int, float, bool, list, dict))
        }
        out.append(
            {
                "kind": patch.kind,
                "entity_id": patch.entity_id,
                "base_version": patch.base_version,
                "base_status": patch.base_status,
                "payload": safe_payload,
                "queued_at": patch.queued_at,
            }
        )
    return json.dumps(out, indent=2)
