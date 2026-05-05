"""Reset operations — Phase 16.1 (Wave 1 Item 5).

UI-callable equivalents of ``scripts/master_reset.sh`` and friends. Every
operation is reversible-explicit (must be ticked in UI) and logged to
``CONTROL.admin_reset_audit_log`` so we have a paper trail of every wipe.

Reset levels (most-aggressive first):

  * **truncate_pipeline_data**
        Drops physical schemas BRONZE_<client> / SILVER_<client> / GOLD_<client>
        + their tables. Truncates CONTROL.client_pipeline_instances + overflow
        + DQ suites where source='PIPELINE_ARCHITECT'.
        Pretty much "the demo data is gone, schemas are gone, but the design
        registry (Silver/Gold designs) is preserved".

  * **truncate_design_registry**
        Drops CONTROL.global_silver_schema_*, global_gold_schema_*,
        bronze_to_silver_mappings, bronze_to_gold_mappings,
        silver_to_gold_mappings, silver_pattern_recommendations.
        Use to start design from scratch. Bronze catalog stays (it's the
        source of truth, loaded from product_catalog.xlsx).

  * **truncate_proposals**
        Wipes CONTROL.agent_proposals + proposal_tag_assignments. Tag catalog
        (proposal_tags) stays — it's system seed data.

  * **truncate_airflow_metadata**
        Deletes Airflow's task_instance / dag_run / log / xcom rows for
        client/dataset DAGs. Use when Airflow gets stuck (queued forever).

  * **delete_generated_artifacts**
        Removes auto-generated files: dags/<client>_*.py, dbt/models/silver/<client>/,
        dbt/models/gold/<client>/, datalink/pipeline/{bronze,gold}/ddl/<client>_*.sql,
        data/generated/<client>/. Uses docker exec to handle root-owned files.

A "Full reset" preset runs ALL FIVE in dependency order.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from datalink.logging import get_logger
from datalink.quality.control import CONTROL_SCHEMA

_log = get_logger(__name__)

REPO_ROOT = Path(os.environ.get("DL_REPO_ROOT", "/opt/datalink"))


@dataclass
class ResetReport:
    """Summary of a reset operation. Returned by every reset_* function so
    the UI can show "✅ truncated 19 tables / dropped 3 schemas / deleted 12
    files" rather than a vague success-toast."""

    operation: str
    actor: str
    started_at: datetime
    finished_at: datetime | None = None
    counts: dict[str, int] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    @property
    def succeeded(self) -> bool:
        return not self.errors

    def summary_line(self) -> str:
        if not self.succeeded:
            return f"❌ {self.operation}: {len(self.errors)} error(s)"
        bits = [f"{k}={v}" for k, v in self.counts.items() if v]
        return f"✅ {self.operation}: " + (" · ".join(bits) if bits else "no-op")


def _wh():
    """Same lazy-warehouse pattern used in datalink.proposals.store."""
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
# Audit log
# ---------------------------------------------------------------------------


def ensure_audit_table() -> None:
    """Create CONTROL.admin_reset_audit_log if missing. Idempotent."""
    _wh().execute(
        f"""
        CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.admin_reset_audit_log (
            event_id        VARCHAR(36) NOT NULL,
            event_at        TIMESTAMP_NTZ NOT NULL,
            actor           VARCHAR(128) NOT NULL,
            operation       VARCHAR(64) NOT NULL,
            client_id       VARCHAR(64),
            counts_json     VARIANT,
            warnings_json   VARIANT,
            errors_json     VARIANT,
            duration_ms     INTEGER,
            PRIMARY KEY (event_id)
        )
        """
    )


def _log_audit(report: ResetReport, client_id: str | None) -> None:
    import json

    try:
        ensure_audit_table()
        duration_ms = (
            int((report.finished_at - report.started_at).total_seconds() * 1000)
            if report.finished_at
            else 0
        )
        _wh().execute(
            f"INSERT INTO {CONTROL_SCHEMA}.admin_reset_audit_log "
            f"(event_id, event_at, actor, operation, client_id, "
            f" counts_json, warnings_json, errors_json, duration_ms) "
            f"SELECT %(eid)s, %(ts)s, %(actor)s, %(op)s, %(cid)s, "
            f"PARSE_JSON(%(c)s), PARSE_JSON(%(w)s), PARSE_JSON(%(e)s), %(d)s",
            {
                "eid": str(uuid.uuid4()),
                "ts": _now(),
                "actor": report.actor,
                "op": report.operation,
                "cid": client_id,
                "c": json.dumps(report.counts),
                "w": json.dumps(report.warnings),
                "e": json.dumps(report.errors),
                "d": duration_ms,
            },
        )
    except Exception as exc:
        _log.warning("reset.audit_log_failed", err=str(exc)[:200])


# ---------------------------------------------------------------------------
# Reset operations
# ---------------------------------------------------------------------------

# Tables that hold per-client pipeline / DQ artifacts we're OK truncating.
PIPELINE_DATA_TABLES = [
    "client_pipeline_instances",
    "pipeline_instance_audit_log",
    "client_field_overrides",
    "client_routing_overrides",
    "bronze_overflow_log",
    "greenfield_dataset_proposals",
]

DESIGN_REGISTRY_TABLES = [
    "bronze_to_silver_mappings",
    "global_silver_schema_columns",
    "global_silver_schema_tables",
    "global_silver_schema_audit_log",
    "silver_schema_audit_log",
    "silver_to_gold_mappings",
    "global_silver_schema_datasets",
    "bronze_to_gold_mappings",
    "global_gold_schema_fields",
    "gold_schema_audit_log",
    "global_gold_schema_datasets",
    "silver_pattern_recommendations",
]


def reset_pipeline_data(*, client_id: str | None = None, actor: str) -> ResetReport:
    """Wipe deployed pipeline state. Designs are preserved.

    When ``client_id`` is None, this affects every client. Otherwise scoped.
    """
    rpt = ResetReport(operation="reset_pipeline_data", actor=actor, started_at=_now())
    rpt.counts["tables_truncated"] = 0
    rpt.counts["schemas_dropped"] = 0
    rpt.counts["dq_suites_truncated"] = 0

    where_client = "WHERE client_id = %(cid)s" if client_id else ""
    params = {"cid": client_id} if client_id else None

    for tbl in PIPELINE_DATA_TABLES:
        try:
            if where_client:
                _wh().execute(f"DELETE FROM {CONTROL_SCHEMA}.{tbl} {where_client}", params)
            else:
                _wh().execute(f"DELETE FROM {CONTROL_SCHEMA}.{tbl}")
            rpt.counts["tables_truncated"] += 1
        except Exception as exc:
            rpt.warnings.append(f"{tbl}: {str(exc)[:120]}")

    # DQ suites emitted by Pipeline Architect
    try:
        if client_id:
            _wh().execute(
                f"DELETE FROM {CONTROL_SCHEMA}.dq_suites "
                f"WHERE source = 'PIPELINE_ARCHITECT' AND client_id = %(cid)s",
                {"cid": client_id},
            )
        else:
            _wh().execute(
                f"DELETE FROM {CONTROL_SCHEMA}.dq_suites WHERE source = 'PIPELINE_ARCHITECT'"
            )
        rpt.counts["dq_suites_truncated"] = 1
    except Exception as exc:
        rpt.warnings.append(f"dq_suites cleanup: {str(exc)[:120]}")

    # Drop physical schemas
    if client_id:
        targets = [
            f"BRONZE_{client_id.upper()}",
            f"SILVER_{client_id.upper()}",
            f"GOLD_{client_id.upper()}",
        ]
    else:
        # All clients — fetch from instances pre-truncate (we already truncated, so
        # use the deduplicated list of clients we saw in audit)
        targets = []  # safe default; full sweep done via per-client UI flow
    for sch in targets:
        try:
            _wh().execute(f"DROP SCHEMA IF EXISTS {sch} CASCADE")
            rpt.counts["schemas_dropped"] += 1
        except Exception as exc:
            rpt.warnings.append(f"DROP {sch}: {str(exc)[:120]}")

    rpt.finished_at = _now()
    _log_audit(rpt, client_id)
    return rpt


def reset_design_registry(*, actor: str) -> ResetReport:
    """Wipe Silver/Gold design registry. Bronze catalog stays."""
    rpt = ResetReport(operation="reset_design_registry", actor=actor, started_at=_now())
    rpt.counts["tables_truncated"] = 0
    for tbl in DESIGN_REGISTRY_TABLES:
        try:
            _wh().execute(f"DELETE FROM {CONTROL_SCHEMA}.{tbl}")
            rpt.counts["tables_truncated"] += 1
        except Exception as exc:
            rpt.warnings.append(f"{tbl}: {str(exc)[:120]}")
    rpt.finished_at = _now()
    _log_audit(rpt, None)
    return rpt


def reset_proposals(*, actor: str) -> ResetReport:
    """Wipe agent_proposals + tag assignments. Tag catalog preserved."""
    rpt = ResetReport(operation="reset_proposals", actor=actor, started_at=_now())
    try:
        _wh().execute(f"DELETE FROM {CONTROL_SCHEMA}.proposal_tag_assignments")
        _wh().execute(f"DELETE FROM {CONTROL_SCHEMA}.agent_proposals")
        rpt.counts["tables_truncated"] = 2
    except Exception as exc:
        rpt.errors.append(f"proposals truncate: {str(exc)[:200]}")
    rpt.finished_at = _now()
    _log_audit(rpt, None)
    return rpt


def reset_airflow_metadata(*, dag_id: str | None = None, actor: str) -> ResetReport:
    """Delete Airflow metadata for one DAG (or all if None) via docker exec.

    Direct DB access from Streamlit isn't possible (different network) — we
    shell into the airflow_db container.
    """
    rpt = ResetReport(operation="reset_airflow_metadata", actor=actor, started_at=_now())
    where = f"WHERE dag_id = '{dag_id}'" if dag_id else "WHERE TRUE"
    sql = (
        f"DELETE FROM task_instance {where}; "
        f"DELETE FROM xcom {where}; "
        f"DELETE FROM log {where}; "
        f"DELETE FROM dag_run {where}; "
        f"DELETE FROM dag {where};"
    )
    try:
        cmd = [
            "docker",
            "exec",
            "-i",
            "datalink-airflow-db",
            "psql",
            "-U",
            "airflow",
            "-d",
            "airflow",
            "-c",
            sql,
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode == 0:
            # Each DELETE prints "DELETE N" — sum the Ns.
            total = sum(
                int(line.split()[-1])
                for line in result.stdout.splitlines()
                if line.startswith("DELETE ")
            )
            rpt.counts["airflow_rows_deleted"] = total
        else:
            rpt.warnings.append(
                f"docker exec returned rc={result.returncode}: {result.stderr[:200]}"
            )
    except FileNotFoundError:
        rpt.warnings.append(
            "docker not available from Streamlit container — "
            "Airflow metadata reset skipped. Run via host terminal."
        )
    except Exception as exc:
        rpt.errors.append(f"airflow reset: {str(exc)[:200]}")
    rpt.finished_at = _now()
    _log_audit(rpt, None)
    return rpt


def delete_generated_artifacts(*, client_id: str | None = None, actor: str) -> ResetReport:
    """Remove auto-generated files. Uses both Path.unlink and docker exec
    (for root-owned files written by Airflow worker)."""
    rpt = ResetReport(operation="delete_generated_artifacts", actor=actor, started_at=_now())
    rpt.counts["files_removed"] = 0

    client_glob = client_id.lower() if client_id else "*"
    candidates = [
        REPO_ROOT / "dags" / f"{client_glob}_*_pipeline.py",
        REPO_ROOT / "datalink" / "pipeline" / "bronze" / "ddl" / f"{client_glob}_*.sql",
        REPO_ROOT / "datalink" / "pipeline" / "gold" / "ddl" / f"{client_glob}_*.sql",
    ]
    for pat in candidates:
        for p in pat.parent.glob(pat.name):
            try:
                p.unlink()
                rpt.counts["files_removed"] += 1
            except Exception as exc:
                rpt.warnings.append(f"unlink {p}: {str(exc)[:80]}")

    # Directories — these can be root-owned (Airflow writes them as root).
    # Try shutil.rmtree first; if PermissionError, fall back to docker exec.
    dirs = [
        REPO_ROOT / "dbt" / "models" / "silver" / (client_glob if client_id else "aetna"),
        REPO_ROOT / "dbt" / "models" / "gold" / (client_glob if client_id else "aetna"),
        REPO_ROOT / "data" / "generated" / (client_glob if client_id else "aetna"),
    ]
    for d in dirs:
        if not d.exists():
            continue
        try:
            shutil.rmtree(d)
            rpt.counts["files_removed"] += 1
        except PermissionError:
            # docker exec --user root fallback
            try:
                # Translate from container path to repo path
                container_path = str(d).replace(str(REPO_ROOT), "/opt/datalink")
                subprocess.run(
                    [
                        "docker",
                        "exec",
                        "--user",
                        "root",
                        "datalink-airflow-scheduler",
                        "rm",
                        "-rf",
                        container_path,
                    ],
                    capture_output=True,
                    text=True,
                    timeout=30,
                )
                if not d.exists():
                    rpt.counts["files_removed"] += 1
                else:
                    rpt.warnings.append(f"rm -rf failed for {d}")
            except Exception as exc:
                rpt.warnings.append(f"docker rm {d}: {str(exc)[:120]}")
        except Exception as exc:
            rpt.warnings.append(f"rmtree {d}: {str(exc)[:120]}")

    rpt.finished_at = _now()
    _log_audit(rpt, client_id)
    return rpt


def full_reset(
    *, client_id: str, actor: str, include_designs: bool = False, include_proposals: bool = False
) -> list[ResetReport]:
    """One-call full reset. Order matters — drop physical first, then
    metadata, then files. Designs and proposals are preserved unless
    explicitly opted-in (they're harder to regenerate)."""
    reports: list[ResetReport] = []
    f"{client_id.lower()}_*_pipeline"  # we wipe all DAGs for this client

    reports.append(reset_pipeline_data(client_id=client_id, actor=actor))
    reports.append(reset_airflow_metadata(dag_id=None, actor=actor))  # cluster-wide
    reports.append(delete_generated_artifacts(client_id=client_id, actor=actor))

    if include_designs:
        reports.append(reset_design_registry(actor=actor))
    if include_proposals:
        reports.append(reset_proposals(actor=actor))

    return reports
