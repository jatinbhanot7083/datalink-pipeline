"""Pipeline Architect — single-shot bulk fetch + helper accessors (Phase 17.7).

The redesigned Pipeline Architect page (Sections 1–2: 🌍 Factory Catalog and
🏭 Instance Fleet) needs three pieces of state on every render:

  1. The Bronze catalog (33 canonical datasets) — to know what factories
     SHOULD exist.
  2. The on-disk DAG factory files in ``dags/`` — to know what factories DO
     exist + their last-mtime stamp.
  3. ``CONTROL.client_pipeline_instances`` — the runtime registry that the
     factories loop over.  One row per (client_id, dataset_code) tuple.

We pull all three in a single ``@st.cache_data(ttl=60)`` snapshot so
filters / sorts / drill-downs operate on memory, not Snowflake.

Public API mirrors ``datalink.ui._dmd_data``:

    from datalink.ui import _pa_data as pa
    snap = pa.get_snapshot()
    pa.invalidate()
    rows = snap.instances_for(client_id="aetna", status="LIVE")
    summary = snap.factories_summary()  # one row per dataset
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import streamlit as st

from datalink.quality.control import CONTROL_SCHEMA
from datalink.ui._query import warehouse_ctx

# Canonical anchor for the factory files.  Page renders / regen script
# both walk this directory.
DAGS_DIR = Path(__file__).resolve().parents[2] / "dags"


def _pa_pick(d: dict, *keys: str, default: Any = None) -> Any:
    """First-non-None lookup across multiple keys.  Mirrors the
    ``_cc_pick`` helper in DMD — defends against the ``False or None``
    coercion bug for boolean / 0 / empty-string values returned by
    Snowflake.  Snowflake auto-lowercases column keys via
    ``warehouse_ctx`` but we keep the uppercase variant as a defensive
    second key for any caller that builds the dict differently.
    """
    for k in keys:
        v = d.get(k)
        if v is not None:
            return v
    return default


# ---------------------------------------------------------------------------
# Snapshot dataclass
# ---------------------------------------------------------------------------
@dataclass
class Snapshot:
    """All data the Pipeline Architect page needs, in one pass."""

    bronze_datasets: list[dict[str, Any]] = field(default_factory=list)
    pipeline_instances: list[dict[str, Any]] = field(default_factory=list)
    factory_files: dict[str, dict[str, Any]] = field(default_factory=dict)
    distinct_clients: list[str] = field(default_factory=list)
    # Phase 17.10 — recent runs indexed by instance_id, sorted newest first.
    # Bulk-fetched once on snapshot load so per-row expanders don't need
    # per-instance Snowflake queries.  Up to ``RUNS_PER_INSTANCE`` per row.
    recent_runs_by_instance: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    loaded_at: str = ""

    # ---- accessors --------------------------------------------------------

    def instances_for(
        self,
        *,
        client_id: str | None = None,
        dataset_code: str | None = None,
        status: str | list[str] | None = None,
        exclude_archived: bool = False,
    ) -> list[dict[str, Any]]:
        """Filter the full instance list.  All args optional."""
        rows = self.pipeline_instances
        if client_id is not None:
            rows = [r for r in rows if str(r.get("client_id")) == client_id]
        if dataset_code is not None:
            rows = [r for r in rows if str(r.get("dataset_code")) == dataset_code]
        if status is not None:
            wanted = {status} if isinstance(status, str) else set(status)
            rows = [r for r in rows if str(r.get("status")) in wanted]
        if exclude_archived:
            rows = [r for r in rows if str(r.get("status")) != "ARCHIVED"]
        return rows

    def factories_summary(self) -> list[dict[str, Any]]:
        """One row per Bronze dataset — joins file existence + instance counts.

        Returned columns are the shape Section 1 (🌍 Factory Catalog) wants:
          dataset_code, display_name, category, anchor, factory_exists,
          factory_path, factory_mtime, total_instances, live_count,
          paused_count, draft_count, archived_count.
        """
        # Index instances by dataset_code → list
        by_ds: dict[str, list[dict[str, Any]]] = {}
        for inst in self.pipeline_instances:
            by_ds.setdefault(str(inst.get("dataset_code")), []).append(inst)

        out: list[dict[str, Any]] = []
        for ds in self.bronze_datasets:
            code = str(_pa_pick(ds, "dataset_code", "DATASET_CODE", default=""))
            ff = self.factory_files.get(code, {})
            insts = by_ds.get(code, [])

            def _count(rows: list[dict[str, Any]], st: str) -> int:
                return sum(1 for r in rows if str(r.get("status")) == st)

            out.append(
                {
                    "dataset_code": code,
                    "display_name": _pa_pick(ds, "display_name", "DISPLAY_NAME", default=code),
                    "category": _pa_pick(ds, "category", "CATEGORY", default="—"),
                    "factory_exists": bool(ff),
                    "factory_path": ff.get("path"),
                    "factory_mtime": ff.get("mtime"),
                    "total_instances": len(insts),
                    "live_count": _count(insts, "LIVE"),
                    "paused_count": _count(insts, "PAUSED"),
                    "draft_count": _count(insts, "DRAFT"),
                    "archived_count": _count(insts, "ARCHIVED"),
                }
            )
        return out

    def live_dmd_pairs_without_instance(
        self,
        dmd_silver: list[dict[str, Any]],
        dmd_gold: list[dict[str, Any]],
        *,
        also_include_draft: bool = True,
    ) -> list[tuple[str, str]]:
        """Pairs eligible for AI proposal / deploy.

        Returns ``[(client_id, dataset_code), ...]`` where Data Model
        Designer has BOTH Silver and Gold LIVE for that pair AND no
        LIVE/PAUSED instance exists.  By default DRAFT pairs ARE
        surfaced — that's the whole point of the AI flow: turn a stub
        DRAFT into an AI-designed deployed pipeline.  Pass
        ``also_include_draft=False`` to filter them out (used by the
        legacy mechanical "+ Create instance" banner if it ever returns).

        ``dmd_silver`` and ``dmd_gold`` are the snapshots from
        ``_dmd_data`` — we accept them as args to avoid pulling DMD's
        snapshot dependency directly.
        """
        live_silver = {
            (str(s.get("scope_owner")), str(s.get("dataset_code")))
            for s in dmd_silver
            if str(s.get("status")) == "LIVE"
            and str(s.get("scope_owner") or "") not in ("GLOBAL_CORP", "__global__", "")
        }
        live_gold = {
            (str(g.get("scope_owner")), str(g.get("dataset_code")))
            for g in dmd_gold
            if str(g.get("status")) == "LIVE"
            and str(g.get("scope_owner") or "") not in ("GLOBAL_CORP", "__global__", "")
        }
        live_pairs = live_silver & live_gold

        # Block only LIVE / PAUSED / PENDING_REVIEW — don't block DRAFT
        # by default.  DRAFT means "row exists but not deployed yet" —
        # the AI flow REPLACES the stub DRAFT with a designed plan.
        blocking_statuses = {"LIVE", "PAUSED", "PENDING_REVIEW"}
        if not also_include_draft:
            blocking_statuses.add("DRAFT")
        existing = {
            (str(r.get("client_id")), str(r.get("dataset_code")))
            for r in self.pipeline_instances
            if str(r.get("status")) in blocking_statuses
        }
        return sorted(live_pairs - existing)

    def find_instance_by_pair(
        self, client_id: str, dataset_code: str, *, exclude_archived: bool = True
    ) -> dict[str, Any] | None:
        """Return the existing instance row for (client, dataset), if any.
        Used by the AI deploy flow to detect a stub DRAFT it should
        replace before inserting the AI-designed proposal."""
        for r in self.pipeline_instances:
            if str(r.get("client_id")) == client_id and str(r.get("dataset_code")) == dataset_code:
                if exclude_archived and str(r.get("status")) == "ARCHIVED":
                    continue
                return r
        return None


# ---------------------------------------------------------------------------
# Bulk fetch — single Snowflake round-trip + filesystem walk
# ---------------------------------------------------------------------------
@st.cache_data(ttl=60, show_spinner="Loading Pipeline Architect data…")
def _fetch_snapshot_uncached() -> Snapshot:
    snap = Snapshot()
    with warehouse_ctx(readonly=True) as wh:
        # 1. Bronze catalog datasets — used to drive the factory catalog grid.
        snap.bronze_datasets = list(
            wh.query(
                f"SELECT * FROM {CONTROL_SCHEMA}.global_bronze_catalog_datasets "
                f"ORDER BY display_name"
            )
        )

        # 2. Pipeline instances — the runtime registry that factories loop over.
        try:
            snap.pipeline_instances = list(
                wh.query(
                    f"""
                    SELECT instance_id, client_id, dataset_code, template_id,
                           cloned_from_instance, status, bronze_anchor,
                           bronze_schema, bronze_table,
                           silver_schema, silver_table,
                           gold_schema, gold_table,
                           schedule_cron, contract_id, gx_suite_id,
                           dag_uri, dbt_models_uri,
                           overrides_json, deviation_count,
                           created_at, created_by, submitted_at,
                           approved_at, approved_by, deployed_at
                      FROM {CONTROL_SCHEMA}.client_pipeline_instances
                    """
                )
            )
        except Exception:
            # Table may not exist on a fresh DB — fail soft.
            snap.pipeline_instances = []

    # 3. Factory files on disk — what the scheduler will register.
    if DAGS_DIR.is_dir():
        for p in sorted(DAGS_DIR.glob("_factory_*.py")):
            if p.stem == "_factory_common":
                continue  # the helper is not a per-dataset factory
            code = p.stem[len("_factory_") :]
            try:
                snap.factory_files[code] = {
                    "path": str(p),
                    "mtime": datetime.fromtimestamp(p.stat().st_mtime, UTC).isoformat(),
                    "size_bytes": p.stat().st_size,
                }
            except OSError:
                snap.factory_files[code] = {"path": str(p), "mtime": None}

    # 4. Distinct clients — derived from instances + standard fallback list.
    clients: set[str] = set()
    for r in snap.pipeline_instances:
        cid = str(r.get("client_id") or "")
        if cid:
            clients.add(cid)
    snap.distinct_clients = sorted(clients)

    # 5. Recent runs per instance — bulk pull, group in Python.  Up to 5 per
    # instance.  Used by Section 3's per-row expanders to show last-run
    # status + collapse a short history without N+1 queries.
    try:
        with warehouse_ctx(readonly=True) as wh:
            run_rows = list(
                wh.query(
                    f"""
                    SELECT run_id, instance_id, status, triggered_by,
                           started_at, ended_at, duration_ms,
                           rows_bronze, rows_silver, rows_gold,
                           error_task, error_message
                      FROM {CONTROL_SCHEMA}.dataset_pipeline_runs
                     ORDER BY started_at DESC
                    """
                )
            )
        for r in run_rows:
            iid = str(r.get("instance_id"))
            bucket = snap.recent_runs_by_instance.setdefault(iid, [])
            if len(bucket) < 5:  # keep newest 5 per instance
                bucket.append(r)
    except Exception:
        # Table may be missing on a fresh DB — fail soft.
        snap.recent_runs_by_instance = {}

    snap.loaded_at = datetime.now(UTC).isoformat()
    return snap


def get_snapshot() -> Snapshot:
    """Page-cached snapshot.  Reuses Streamlit's @st.cache_data — refetches
    every 60 s OR after ``invalidate()``."""
    return _fetch_snapshot_uncached()


def invalidate() -> None:
    """Drop the cache.  Next ``get_snapshot()`` re-pulls Snowflake + walks
    ``dags/`` again."""
    _fetch_snapshot_uncached.clear()


# ---------------------------------------------------------------------------
# Direct-write actions — same pattern as DMD Phase 17.6.  No buffering.
# ---------------------------------------------------------------------------
def update_instance_status(
    instance_id: str,
    new_status: str,
    *,
    set_deployed_at: bool = False,
) -> None:
    """Flip a pipeline instance's status (LIVE / PAUSED / ARCHIVED / DRAFT).

    Called from per-row action buttons in Section 2 (Instance Fleet).
    """
    sql_extra = ""
    if set_deployed_at:
        sql_extra = ", deployed_at = CURRENT_TIMESTAMP()"
    from datalink.ui._query import warehouse_ctx as _wh

    with _wh(readonly=False) as w:
        w.execute(
            f"UPDATE {CONTROL_SCHEMA}.client_pipeline_instances "
            f"SET status = $st{sql_extra} "
            f"WHERE instance_id = $iid",
            {"st": new_status, "iid": instance_id},
        )
    invalidate()


def delete_stub_instance(instance_id: str) -> int:
    """Hard-delete a DRAFT pipeline instance row.  Used by the AI flow to
    replace a stub DRAFT with an AI-designed plan via persist_proposal.
    Refuses to delete LIVE / PAUSED / PENDING_REVIEW rows — those went
    through deploy and shouldn't be silently removed.

    Returns rowcount (0 or 1)."""
    from datalink.ui._query import warehouse_ctx as _wh

    with _wh(readonly=False) as w:
        rows = list(
            w.query(
                f"SELECT status FROM {CONTROL_SCHEMA}.client_pipeline_instances "
                f"WHERE instance_id = $iid",
                {"iid": instance_id},
            )
        )
        if not rows:
            return 0
        st = str(rows[0].get("status"))
        if st not in ("DRAFT",):
            raise ValueError(
                f"Refusing to delete instance {instance_id!r} with status "
                f"{st!r}.  Only DRAFT may be replaced; archive or pause "
                "first if you really mean to drop a deployed instance."
            )
        # Children of pipeline_instances (deviation_audit_log etc.) cascade
        # via ON DELETE CASCADE in the DDL; if a future FK lacks cascade,
        # extend this block to clean it up explicitly.
        w.execute(
            f"DELETE FROM {CONTROL_SCHEMA}.client_pipeline_instances " f"WHERE instance_id = $iid",
            {"iid": instance_id},
        )
    invalidate()
    return 1


def deploy_instance(instance_id: str) -> dict[str, Any]:
    """Materialize the physical infrastructure for an instance and flip
    its status DRAFT → LIVE.  Idempotent.

    Steps:
      1. Read the instance row (client_id, dataset_code, bronze_anchor).
      2. CREATE SCHEMA IF NOT EXISTS BRONZE/SILVER/GOLD_<CLIENT>.
      3. CREATE TABLE IF NOT EXISTS for Bronze (cols from PSV header) + Gold.
         (Silver is created by silver_dbt_task at run time via DROP+CREATE.)
      4. UPDATE status='LIVE', deployed_at=CURRENT_TIMESTAMP().

    After this call, an Airflow scheduler that imports the dataset's
    factory file will see the row and emit a DAG.  ▶ Run now will execute
    the pipeline against the now-existing physical tables.

    Returns a manifest of what was created.  Raises ValueError if the
    instance is missing.
    """
    from datalink.orchestration.run_now import bootstrap_for_instance
    from datalink.ui._query import warehouse_ctx as _wh

    with _wh(readonly=True) as w:
        rows = list(
            w.query(
                f"SELECT instance_id, client_id, dataset_code, bronze_anchor, status "
                f"FROM {CONTROL_SCHEMA}.client_pipeline_instances "
                f"WHERE instance_id = $iid",
                {"iid": instance_id},
            )
        )
    if not rows:
        raise ValueError(f"Instance {instance_id!r} not found.")
    inst = rows[0]
    if str(inst.get("status")) == "ARCHIVED":
        raise ValueError(f"Instance {instance_id!r} is ARCHIVED — restore from archive first.")

    manifest = bootstrap_for_instance(
        client_id=str(inst["client_id"]),
        dataset_code=str(inst["dataset_code"]),
        bronze_anchor=str(inst.get("bronze_anchor") or "FLAT_FILE"),
    )

    update_instance_status(instance_id, "LIVE", set_deployed_at=True)
    return manifest


def build_and_deploy(
    *,
    client_id: str,
    dataset_code: str,
    bronze_anchor: str = "FLAT_FILE",
    schedule_cron: str = "0 4 * * *",
    created_by: str = "ui:pipeline_architect",
) -> tuple[str, dict[str, Any]]:
    """One-shot Build & Deploy: insert a DRAFT instance row, materialize
    physical schemas + tables, flip status to LIVE.

    Returns ``(instance_id, manifest)``.  Used by Section 3
    "🚀 Build & Deploy Pipeline" — gives the operator a single click to
    go from "DMD has Silver+Gold LIVE" to "pipeline is fully deployed."
    """
    instance_id = insert_instance_from_live_schema(
        client_id=client_id,
        dataset_code=dataset_code,
        bronze_anchor=bronze_anchor,
        schedule_cron=schedule_cron,
        created_by=created_by,
        initial_status="DRAFT",
    )
    manifest = deploy_instance(instance_id)
    return instance_id, manifest


def insert_instance_from_live_schema(
    *,
    client_id: str,
    dataset_code: str,
    bronze_anchor: str = "FLAT_FILE",
    schedule_cron: str = "0 4 * * *",
    created_by: str = "ui:pipeline_architect",
    initial_status: str = "DRAFT",
) -> str:
    """Create a NEW pipeline_instance row from a (client, dataset) pair that
    already has Silver+Gold LIVE in Data Model Designer.

    The instance lands as **DRAFT by default** — registered metadata only,
    no physical Snowflake infrastructure yet.  Call
    ``deploy_instance(instance_id)`` (or use ``build_and_deploy()`` which
    chains both) to materialize schemas/tables and flip status to LIVE.

    Conventions baked in (overridable later via the 🛠 Edit overrides panel).
    These MATCH the names datalink.orchestration.tasks materializes so
    metadata and warehouse don't diverge:
      * bronze_schema  = BRONZE_<CLIENT>
      * bronze_table   = raw_<dataset>
      * silver_schema  = SILVER_<CLIENT>
      * silver_table   = <dataset>_clean      ← matches silver_dbt_task convention
      * gold_schema    = GOLD_<CLIENT>
      * gold_table     = <dataset>            ← matches gold_dbt_task convention
      * schedule_cron  = '0 4 * * *' (overridable)

    Returns the new instance_id.
    """
    import uuid as _uuid

    iid = str(_uuid.uuid4())
    cli_u = client_id.upper()
    ds_l = dataset_code.lower()
    # deployed_at is only set when status is LIVE up front (legacy flow).
    # Build & Deploy: starts as DRAFT here, flips to LIVE in deploy_instance()
    # which sets deployed_at then.
    deployed_at_clause = "CURRENT_TIMESTAMP()" if initial_status == "LIVE" else "NULL"
    from datalink.ui._query import warehouse_ctx as _wh

    with _wh(readonly=False) as w:
        w.execute(
            f"INSERT INTO {CONTROL_SCHEMA}.client_pipeline_instances ("
            f"instance_id, client_id, dataset_code, status, bronze_anchor, "
            f"bronze_schema, bronze_table, silver_schema, silver_table, "
            f"gold_schema, gold_table, schedule_cron, "
            f"deviation_count, created_by, created_at, deployed_at"
            f") VALUES ("
            f"$iid, $cid, $ds, $st0, $ba, "
            f"$bs, $bt, $ss, $st, $gs, $gt, $sc, "
            f"0, $by, CURRENT_TIMESTAMP(), {deployed_at_clause}"
            f")",
            {
                "iid": iid,
                "cid": client_id,
                "ds": dataset_code,
                "st0": initial_status,
                "ba": bronze_anchor,
                "bs": f"BRONZE_{cli_u}",
                "bt": f"raw_{ds_l}",
                "ss": f"SILVER_{cli_u}",
                "st": f"{ds_l}_clean",  # matches silver_dbt_task convention
                "gs": f"GOLD_{cli_u}",
                "gt": ds_l,
                "sc": schedule_cron,
                "by": created_by,
            },
        )
    invalidate()
    return iid
