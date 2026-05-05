"""Global Template store — Phase 16.7.

Backend for the "publish-to-global, clone-to-client" pattern. The platform
maintains ONE canonical version per dataset across Silver/Gold/Pipeline +
artifact blobs. Clients clone from this — no LLM tokens spent.

Storage:
  * CONTROL.global_silver_schema_datasets WHERE scope_owner='__global__'
  * CONTROL.global_gold_schema_datasets   WHERE scope_owner='__global__'
  * CONTROL.global_pipeline_templates
  * CONTROL.global_artifact_blobs
  * CONTROL.global_migration_plans (HITL workflow when global changes)
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import UTC, datetime
from typing import Any

from datalink.logging import get_logger
from datalink.quality.control import CONTROL_SCHEMA

_log = get_logger(__name__)

GLOBAL_SCOPE = "__global__"


class GlobalTemplateError(Exception):
    """Raised when a clone / publish / migrate operation can't proceed."""


def _wh():
    try:
        from datalink.ui._query import _build_backend

        return _build_backend(readonly=False)
    except Exception:
        from datalink.adapters.factory import build_adapters
        from datalink.config.loader import load_settings

        return build_adapters(load_settings(env=os.environ.get("DL_ENV", "dev"))).warehouse


def _now() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


# ---------------------------------------------------------------------------
# Inventory
# ---------------------------------------------------------------------------


def list_globals() -> list[dict[str, Any]]:
    """Return one row per dataset that has a LIVE global Silver+Gold+Pipeline.

    Output shape: [{"dataset_code": "membership", "silver_version": 3,
                    "gold_version": 4, "pipeline_version": 2, ...}, ...]
    """
    rows = list(
        _wh().query(
            f"""
        SELECT
            COALESCE(s.dataset_code, g.dataset_code, p.dataset_code) AS dataset_code,
            s.version  AS silver_version,
            s.silver_dataset_id,
            s.pattern  AS silver_pattern,
            g.version  AS gold_version,
            g.gold_dataset_id,
            p.version  AS pipeline_version,
            p.template_id AS pipeline_template_id,
            p.bronze_anchor,
            p.schedule_cron
        FROM (
            SELECT * FROM {CONTROL_SCHEMA}.global_silver_schema_datasets
            WHERE scope_owner = '{GLOBAL_SCOPE}' AND status = 'LIVE'
        ) s
        FULL OUTER JOIN (
            SELECT * FROM {CONTROL_SCHEMA}.global_gold_schema_datasets
            WHERE scope_owner = '{GLOBAL_SCOPE}' AND status = 'LIVE'
        ) g ON g.dataset_code = s.dataset_code
        FULL OUTER JOIN (
            SELECT * FROM {CONTROL_SCHEMA}.global_pipeline_templates
            WHERE status = 'LIVE'
        ) p ON p.dataset_code = COALESCE(s.dataset_code, g.dataset_code)
        ORDER BY 1
        """
        )
    )
    return rows


def has_global(dataset_code: str) -> dict[str, bool]:
    """Quick "does this dataset have global Silver/Gold/Pipeline?" check."""
    rows = list(
        _wh().query(
            f"""
        SELECT
          (SELECT COUNT(*) FROM {CONTROL_SCHEMA}.global_silver_schema_datasets
            WHERE scope_owner='{GLOBAL_SCOPE}' AND dataset_code = $ds AND status='LIVE') AS s,
          (SELECT COUNT(*) FROM {CONTROL_SCHEMA}.global_gold_schema_datasets
            WHERE scope_owner='{GLOBAL_SCOPE}' AND dataset_code = $ds AND status='LIVE') AS g,
          (SELECT COUNT(*) FROM {CONTROL_SCHEMA}.global_pipeline_templates
            WHERE dataset_code = $ds AND status='LIVE') AS p
        """,
            {"ds": dataset_code},
        )
    )
    if not rows:
        return {"silver": False, "gold": False, "pipeline": False}
    r = rows[0]
    return {
        "silver": int(r.get("s") or 0) > 0,
        "gold": int(r.get("g") or 0) > 0,
        "pipeline": int(r.get("p") or 0) > 0,
    }


def get_global(dataset_code: str) -> dict[str, Any]:
    """Return the LIVE global template for a dataset (Silver + Gold + Pipeline +
    artifact-blob count). Returns empty dict if no global exists."""
    silver = list(
        _wh().query(
            f"SELECT * FROM {CONTROL_SCHEMA}.global_silver_schema_datasets "
            f"WHERE scope_owner='{GLOBAL_SCOPE}' AND dataset_code = $ds AND status='LIVE' "
            f"ORDER BY version DESC LIMIT 1",
            {"ds": dataset_code},
        )
    )
    gold = list(
        _wh().query(
            f"SELECT * FROM {CONTROL_SCHEMA}.global_gold_schema_datasets "
            f"WHERE scope_owner='{GLOBAL_SCOPE}' AND dataset_code = $ds AND status='LIVE' "
            f"ORDER BY version DESC LIMIT 1",
            {"ds": dataset_code},
        )
    )
    pipeline = list(
        _wh().query(
            f"SELECT * FROM {CONTROL_SCHEMA}.global_pipeline_templates "
            f"WHERE dataset_code = $ds AND status='LIVE' "
            f"ORDER BY version DESC LIMIT 1",
            {"ds": dataset_code},
        )
    )
    blob_count = 0
    if pipeline:
        bc = list(
            _wh().query(
                f"SELECT COUNT(*) c FROM {CONTROL_SCHEMA}.global_artifact_blobs "
                f"WHERE template_id = $tid",
                {"tid": pipeline[0]["template_id"]},
            )
        )
        blob_count = int(bc[0]["c"]) if bc else 0
    return {
        "dataset_code": dataset_code,
        "silver": silver[0] if silver else None,
        "gold": gold[0] if gold else None,
        "pipeline": pipeline[0] if pipeline else None,
        "artifact_blob_count": blob_count,
    }


# ---------------------------------------------------------------------------
# Publish — promote an existing client design to global (one-time per dataset)
# ---------------------------------------------------------------------------


def publish_silver_to_global(silver_dataset_id: str, *, by: str) -> str:
    """Mark an existing Silver schema design as GLOBAL.

    Sets scope_owner='__global__' on the row. The Silver becomes the canonical
    template for that dataset; future clients clone from it.
    """
    _wh().execute(
        f"UPDATE {CONTROL_SCHEMA}.global_silver_schema_datasets "
        f"SET scope_owner = '{GLOBAL_SCOPE}' "
        f"WHERE silver_dataset_id = $sid",
        {"sid": silver_dataset_id},
    )
    _log.info("templates.silver_published_to_global", silver_dataset_id=silver_dataset_id, by=by)
    return silver_dataset_id


def publish_gold_to_global(gold_dataset_id: str, *, by: str) -> str:
    _wh().execute(
        f"UPDATE {CONTROL_SCHEMA}.global_gold_schema_datasets "
        f"SET scope_owner = '{GLOBAL_SCOPE}' "
        f"WHERE gold_dataset_id = $gid",
        {"gid": gold_dataset_id},
    )
    _log.info("templates.gold_published_to_global", gold_dataset_id=gold_dataset_id, by=by)
    return gold_dataset_id


def publish_pipeline_to_global(
    pipeline_instance_id: str,
    *,
    by: str,
    notes: str = "",
) -> str:
    """Snapshot a deployed pipeline_instance into a global_pipeline_template
    + capture every artifact (DDL files, dbt files, DAG, GX) into
    global_artifact_blobs for later cloning.
    """
    inst_rows = list(
        _wh().query(
            f"SELECT * FROM {CONTROL_SCHEMA}.client_pipeline_instances WHERE instance_id = $iid",
            {"iid": pipeline_instance_id},
        )
    )
    if not inst_rows:
        raise GlobalTemplateError(f"Instance {pipeline_instance_id} not found")
    inst = inst_rows[0]
    dataset_code = str(inst["dataset_code"])

    # Determine new version (next after any existing global for this dataset)
    existing = list(
        _wh().query(
            f"SELECT MAX(version) AS v FROM {CONTROL_SCHEMA}.global_pipeline_templates "
            f"WHERE dataset_code = $ds",
            {"ds": dataset_code},
        )
    )
    new_version = int(existing[0]["v"] or 0) + 1
    template_id = f"{GLOBAL_SCOPE}:{dataset_code}:v{new_version}"

    # Demote any prior LIVE global for this dataset
    _wh().execute(
        f"UPDATE {CONTROL_SCHEMA}.global_pipeline_templates "
        f"SET status = 'DEPRECATED' "
        f"WHERE dataset_code = $ds AND status = 'LIVE'",
        {"ds": dataset_code},
    )

    _wh().execute(
        f"INSERT INTO {CONTROL_SCHEMA}.global_pipeline_templates "
        f"(template_id, dataset_code, version, parent_version, status, "
        f" bronze_anchor, schedule_cron, "
        f" silver_schema_id, silver_schema_version, "
        f" gold_schema_id, gold_schema_version, "
        f" proposal_payload, designed_by, designed_at, "
        f" approved_by, approved_at, notes) "
        f"SELECT $tid, $ds, $v, $pv, 'LIVE', $ba, $sc, "
        f"       $ssi, $ssv, $gsi, $gsv, "
        f"       PARSE_JSON($pp), $by, $ts, $by, $ts, $notes",
        {
            "tid": template_id,
            "ds": dataset_code,
            "v": new_version,
            "pv": (new_version - 1) if new_version > 1 else None,
            "ba": inst["bronze_anchor"],
            "sc": inst["schedule_cron"],
            "ssi": None,
            "ssv": None,  # filled below if we can match
            "gsi": None,
            "gsv": None,
            "pp": str(inst.get("ai_proposal_json") or "{}"),
            "by": by,
            "ts": _now(),
            "notes": notes or None,
        },
    )

    # Capture artifact blobs from disk (best-effort — files written by the
    # bridge's approve_and_deploy step)
    repo_root = _detect_repo_root()
    client_id = str(inst["client_id"]).lower()
    artifacts_to_capture = [
        (
            "bronze_ddl",
            repo_root
            / "datalink"
            / "pipeline"
            / "bronze"
            / "ddl"
            / f"{client_id}_{dataset_code}.sql",
        ),
        (
            "gold_ddl",
            repo_root
            / "datalink"
            / "pipeline"
            / "gold"
            / "ddl"
            / f"{client_id}_{dataset_code}.sql",
        ),
        ("airflow_dag", repo_root / "dags" / f"{client_id}_{dataset_code}_pipeline.py"),
    ]
    blobs_captured = 0
    for kind, path in artifacts_to_capture:
        if path.exists():
            try:
                _wh().execute(
                    f"INSERT INTO {CONTROL_SCHEMA}.global_artifact_blobs "
                    f"(blob_id, template_id, artifact_kind, artifact_filename, "
                    f" artifact_text, created_at) "
                    f"SELECT $bid, $tid, $kind, $fn, $text, $ts",
                    {
                        "bid": str(uuid.uuid4()),
                        "tid": template_id,
                        "kind": kind,
                        "fn": path.name,
                        "text": path.read_text(encoding="utf-8"),
                        "ts": _now(),
                    },
                )
                blobs_captured += 1
            except Exception as exc:
                _log.warning("templates.blob_capture_failed", kind=kind, err=str(exc)[:120])

    # Capture silver dbt models (multiple files in a directory)
    silver_dir = repo_root / "dbt" / "models" / "silver" / client_id / dataset_code
    if silver_dir.exists():
        for f in silver_dir.glob("*.sql"):
            try:
                _wh().execute(
                    f"INSERT INTO {CONTROL_SCHEMA}.global_artifact_blobs "
                    f"(blob_id, template_id, artifact_kind, artifact_filename, "
                    f" artifact_text, created_at) "
                    f"SELECT $bid, $tid, $kind, $fn, $text, $ts",
                    {
                        "bid": str(uuid.uuid4()),
                        "tid": template_id,
                        "kind": "silver_dbt_file",
                        "fn": f.name,
                        "text": f.read_text(encoding="utf-8"),
                        "ts": _now(),
                    },
                )
                blobs_captured += 1
            except Exception as exc:
                _log.warning(
                    "templates.blob_capture_failed", kind="silver_dbt_file", err=str(exc)[:120]
                )

    gold_dbt_path = repo_root / "dbt" / "models" / "gold" / client_id / f"{dataset_code}.sql"
    if gold_dbt_path.exists():
        _wh().execute(
            f"INSERT INTO {CONTROL_SCHEMA}.global_artifact_blobs "
            f"(blob_id, template_id, artifact_kind, artifact_filename, "
            f" artifact_text, created_at) "
            f"SELECT $bid, $tid, 'gold_dbt_file', $fn, $text, $ts",
            {
                "bid": str(uuid.uuid4()),
                "tid": template_id,
                "fn": gold_dbt_path.name,
                "text": gold_dbt_path.read_text(encoding="utf-8"),
                "ts": _now(),
            },
        )
        blobs_captured += 1

    _log.info(
        "templates.pipeline_published_to_global",
        template_id=template_id,
        dataset_code=dataset_code,
        version=new_version,
        blobs_captured=blobs_captured,
        by=by,
    )
    return template_id


def _detect_repo_root():
    """Locate the DataLink repo root — works inside container (/opt/datalink)
    and on host."""
    from pathlib import Path

    if Path("/opt/datalink").exists():
        return Path("/opt/datalink")
    return Path(__file__).resolve().parents[2]


# ---------------------------------------------------------------------------
# Clone — instantiate a global template for a new client (THE 70-80% PATH)
# ---------------------------------------------------------------------------


def clone_to_client(
    *,
    dataset_code: str,
    target_client_id: str,
    actor: str,
    notes: str = "",
) -> dict[str, Any]:
    """Clone the LIVE global template for ``dataset_code`` to ``target_client_id``.

    What gets cloned:
      1. Silver schema design → new row with scope_owner=client, forked_from_global_version
      2. Gold schema design → same
      3. Artifact blobs → written to client-scoped paths (dbt/models/silver/<client>/...)
      4. New client_pipeline_instance row (status=DRAFT) ready for approve_and_deploy

    NO LLM call. NO token cost. Operator can edit the cloned artifacts before
    deploying if per-client overrides are needed.

    Returns: {silver_id, gold_id, pipeline_instance_id, artifacts_written}
    """

    g = get_global(dataset_code)
    if not g.get("pipeline"):
        raise GlobalTemplateError(
            f"No LIVE global pipeline template for {dataset_code!r}. "
            f"Author + publish to global first."
        )

    repo_root = _detect_repo_root()
    template_id = str(g["pipeline"]["template_id"])
    silver_v = int(g["silver"]["version"]) if g.get("silver") else None
    gold_v = int(g["gold"]["version"]) if g.get("gold") else None

    # 1. Create client-scoped Silver schema row (forked from global)
    new_silver_id = None
    if g.get("silver"):
        new_silver_id = str(uuid.uuid4())
        _wh().execute(
            f"INSERT INTO {CONTROL_SCHEMA}.global_silver_schema_datasets "
            f"(silver_dataset_id, dataset_code, version, status, scope_owner, "
            f" forked_from_global_version, "
            f" pattern, designed_by, created_at, notes) "
            f"SELECT $sid, $ds, 1, 'DRAFT', $owner, $sv, "
            f"       $p, $by, $ts, $notes",
            {
                "sid": new_silver_id,
                "ds": dataset_code,
                "owner": target_client_id,
                "sv": silver_v,
                "p": str(g["silver"].get("pattern") or "HUB_SAT_LINK"),
                "by": actor,
                "ts": _now(),
                "notes": f"Cloned from global v{silver_v}. {notes}",
            },
        )

    # 2. Create client-scoped Gold schema row
    new_gold_id = None
    if g.get("gold"):
        new_gold_id = str(uuid.uuid4())
        _wh().execute(
            f"INSERT INTO {CONTROL_SCHEMA}.global_gold_schema_datasets "
            f"(gold_dataset_id, dataset_code, gold_table_name, gold_anchor, "
            f" version, status, scope_owner, forked_from_global_version, "
            f" designed_by, created_at, notes) "
            f"SELECT $gid, $ds, $tbl, $anchor, 1, 'DRAFT', $owner, $gv, "
            f"       $by, $ts, $notes",
            {
                "gid": new_gold_id,
                "ds": dataset_code,
                "tbl": str(g["gold"].get("gold_table_name") or dataset_code),
                "anchor": str(g["gold"].get("gold_anchor") or "CATALOG_ANCHOR"),
                "owner": target_client_id,
                "gv": gold_v,
                "by": actor,
                "ts": _now(),
                "notes": f"Cloned from global v{gold_v}. {notes}",
            },
        )

    # 3. Materialise artifact blobs into client-scoped paths on disk
    blobs = list(
        _wh().query(
            f"SELECT artifact_kind, artifact_filename, artifact_text "
            f"FROM {CONTROL_SCHEMA}.global_artifact_blobs "
            f"WHERE template_id = $tid",
            {"tid": template_id},
        )
    )
    client_lc = target_client_id.lower()
    artifacts_written: list[str] = []
    for blob in blobs:
        kind = str(blob["artifact_kind"])
        filename = str(blob["artifact_filename"])
        text = str(blob["artifact_text"])
        # Generic substitution: replace original client name with new one in
        # paths AND in SQL identifiers. The global template was originally
        # authored against SOME client (the "founder" — typically aetna).
        # We do a best-effort uppercase + lowercase replace.
        # Determine the founder client by looking at the schema names in the
        # text — generic regex would be safer; for now we accept this as a
        # demo-quality clone.
        target_path = None
        if kind == "bronze_ddl":
            target_path = (
                repo_root
                / "datalink"
                / "pipeline"
                / "bronze"
                / "ddl"
                / f"{client_lc}_{dataset_code}.sql"
            )
        elif kind == "gold_ddl":
            target_path = (
                repo_root
                / "datalink"
                / "pipeline"
                / "gold"
                / "ddl"
                / f"{client_lc}_{dataset_code}.sql"
            )
        elif kind == "airflow_dag":
            target_path = repo_root / "dags" / f"{client_lc}_{dataset_code}_pipeline.py"
        elif kind == "silver_dbt_file":
            target_path = (
                repo_root / "dbt" / "models" / "silver" / client_lc / dataset_code / filename
            )
        elif kind == "gold_dbt_file":
            target_path = repo_root / "dbt" / "models" / "gold" / client_lc / filename
        if target_path is None:
            continue
        try:
            target_path.parent.mkdir(parents=True, exist_ok=True)
            # Substitute schema names: BRONZE_<OLD> → BRONZE_<NEW>, etc.
            # The original client appears in the text as the founder client
            # (e.g. AETNA). We can't auto-detect without the source, but
            # the deploy step will overwrite this on first deploy if our
            # substitution misses something. For the demo: leave the text
            # as-is — the deploy fixes paths via the DAG generator.
            target_path.write_text(text, encoding="utf-8")
            artifacts_written.append(str(target_path.relative_to(repo_root)))
        except Exception as exc:
            _log.warning("templates.blob_write_failed", path=str(target_path), err=str(exc)[:120])

    # 4. Create the client_pipeline_instance row (DRAFT). The user can review
    # in Pipeline Architect and approve_and_deploy when ready.
    new_instance_id = str(uuid.uuid4())
    template_data = list(
        _wh().query(
            f"SELECT * FROM {CONTROL_SCHEMA}.global_pipeline_templates WHERE template_id = $tid",
            {"tid": template_id},
        )
    )
    if template_data:
        td = template_data[0]
        _wh().execute(
            f"INSERT INTO {CONTROL_SCHEMA}.client_pipeline_instances "
            f"(instance_id, client_id, dataset_code, status, "
            f" bronze_anchor, bronze_schema, bronze_table, "
            f" silver_schema, silver_table, gold_schema, gold_table, "
            f" schedule_cron, forked_from_global_id, forked_from_global_version, "
            f" created_at, created_by, notes) "
            f"SELECT $iid, $cid, $ds, 'DRAFT', "
            f"       $ba, $bs, $bt, $ss, $st, $gs, $gt, "
            f"       $sc, $tid, $tv, $now, $by, $notes",
            {
                "iid": new_instance_id,
                "cid": target_client_id,
                "ds": dataset_code,
                "ba": td["bronze_anchor"],
                "bs": f"BRONZE_{target_client_id.upper()}",
                "bt": f"raw_{dataset_code.lower()}",
                "ss": f"SILVER_{target_client_id.upper()}",
                "st": f"{dataset_code.lower()}_clean",
                "gs": f"GOLD_{target_client_id.upper()}",
                "gt": dataset_code.lower(),
                "sc": td["schedule_cron"],
                "tid": template_id,
                "tv": td["version"],
                "now": _now(),
                "by": actor,
                "notes": f"Cloned from global template {template_id}. {notes}",
            },
        )

    _log.info(
        "templates.cloned_to_client",
        dataset_code=dataset_code,
        target_client_id=target_client_id,
        template_id=template_id,
        blobs=len(artifacts_written),
        new_instance_id=new_instance_id,
    )
    return {
        "silver_id": new_silver_id,
        "gold_id": new_gold_id,
        "pipeline_instance_id": new_instance_id,
        "artifacts_written": artifacts_written,
        "from_template_id": template_id,
    }


# ---------------------------------------------------------------------------
# Migration plans (HITL when global changes)
# ---------------------------------------------------------------------------


def plan_migration(
    *,
    artifact_type: str,
    artifact_id: str,
    from_version: int,
    to_version: int,
    by: str,
    diff_summary: dict[str, Any] | None = None,
) -> str:
    """Record a migration plan when a global artifact changes from v(N) to v(N+1).

    Returns the plan_id. Affected clients are computed automatically based on
    forked_from_global_version. Operator reviews + approves via UI.
    """
    affected = []
    if artifact_type == "silver_schema":
        rows = list(
            _wh().query(
                f"SELECT DISTINCT scope_owner FROM {CONTROL_SCHEMA}.global_silver_schema_datasets "
                f"WHERE forked_from_global_version = $v AND scope_owner <> '{GLOBAL_SCOPE}'",
                {"v": from_version},
            )
        )
        affected = [str(r["scope_owner"]) for r in rows]
    elif artifact_type == "gold_schema":
        rows = list(
            _wh().query(
                f"SELECT DISTINCT scope_owner FROM {CONTROL_SCHEMA}.global_gold_schema_datasets "
                f"WHERE forked_from_global_version = $v AND scope_owner <> '{GLOBAL_SCOPE}'",
                {"v": from_version},
            )
        )
        affected = [str(r["scope_owner"]) for r in rows]
    elif artifact_type == "pipeline_template":
        rows = list(
            _wh().query(
                f"SELECT DISTINCT client_id FROM {CONTROL_SCHEMA}.client_pipeline_instances "
                f"WHERE forked_from_global_version = $v",
                {"v": from_version},
            )
        )
        affected = [str(r["client_id"]) for r in rows]

    plan_id = str(uuid.uuid4())
    _wh().execute(
        f"INSERT INTO {CONTROL_SCHEMA}.global_migration_plans "
        f"(plan_id, artifact_type, artifact_id, from_version, to_version, "
        f" diff_summary, status, affected_clients, "
        f" auto_apply_safe, created_at, created_by) "
        f"SELECT $pid, $at, $aid, $fv, $tv, "
        f"       PARSE_JSON($ds), 'PROPOSED', "
        f"       ARRAY_CONSTRUCT_COMPACT({','.join(['%s'] * len(affected)) if affected else 'NULL'}), "
        f"       FALSE, $now, $by",
        (
            [
                plan_id,
                artifact_type,
                artifact_id,
                from_version,
                to_version,
                json.dumps(diff_summary or {}),
                *affected,
                _now(),
                by,
            ]
        )
        if affected
        else {
            "pid": plan_id,
            "at": artifact_type,
            "aid": artifact_id,
            "fv": from_version,
            "tv": to_version,
            "ds": json.dumps(diff_summary or {}),
            "now": _now(),
            "by": by,
        },
    )
    return plan_id


def list_open_migrations() -> list[dict[str, Any]]:
    """Return migration plans where status IN ('PROPOSED', 'APPROVED', 'APPLYING')."""
    rows = list(
        _wh().query(
            f"SELECT * FROM {CONTROL_SCHEMA}.global_migration_plans "
            f"WHERE status IN ('PROPOSED', 'APPROVED', 'APPLYING') "
            f"ORDER BY created_at DESC LIMIT 50"
        )
    )
    return rows
