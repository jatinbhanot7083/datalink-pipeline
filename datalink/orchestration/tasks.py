"""Pipeline task callables — shared by Airflow DAGs and local_sequential runner.

Each task function has the signature:

    def task_<name>(ctx: TaskContext) -> dict[str, Any]

`ctx` carries run-level state (pipeline_id, run_id, Settings, AdapterSet).
The return dict is merged into the next task's context so downstream tasks
can consult upstream outputs.

Why one shared layer: Airflow's `PythonOperator` and the local_sequential
runner both invoke the same callable. Swapping orchestrators is config-only,
no task-code changes. When Jatin flips `DL_ORCHESTRATOR=airflow_kind` in
prod, these exact functions run inside Airflow worker pods.
"""

from __future__ import annotations

import subprocess
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

from datalink.config.loader import load_settings
from datalink.logging import get_logger
from datalink.tenancy import Layer, schema_for

if TYPE_CHECKING:
    from datalink.adapters.factory import AdapterSet
    from datalink.config.loader import Settings

_log = get_logger(__name__)


# ----------------------------------------------------------------------------
# Shared context the orchestrator threads through each task
# ----------------------------------------------------------------------------


@dataclass
class TaskContext:
    """State threaded through every task in a pipeline run.

    Kept small + serializable so Airflow can XCom-pass it between pods
    (minus the live AdapterSet — Airflow tasks rebuild that from settings
    at entry, which is what you want for pod isolation anyway).
    """

    pipeline_id: str
    run_id: str
    env: str = "local"
    # Phase 5.8: which client's DQ suite to use. "default" = the baseline
    # suites seeded from the Phase-5 Python files; real tenants pass their
    # own id via Airflow DAG conf (see DAG config "client_id" field).
    client_id: str = "default"
    # Populated lazily at first .adapters access so each Airflow task can
    # build its own in-process AdapterSet without upstream tasks' state.
    _settings: Settings | None = None
    _adapters: AdapterSet | None = None
    # Outputs from prior tasks — key = task name, value = whatever that task returned.
    upstream: dict[str, Any] = field(default_factory=dict)

    @property
    def settings(self) -> Settings:
        if self._settings is None:
            self._settings = load_settings(env=self.env)
        return self._settings

    @property
    def adapters(self) -> AdapterSet:
        if self._adapters is None:
            from datalink.adapters.factory import build_adapters

            self._adapters = build_adapters(self.settings)
        return self._adapters


# ----------------------------------------------------------------------------
# BRONZE tasks
# ----------------------------------------------------------------------------


def task_bronze_ingest(ctx: TaskContext) -> dict[str, Any]:
    """Ingest all 3 sample CSVs (claims / membership / provider) from SFTP → Bronze."""
    from datalink.adapters.sftp.atmoz import AtmozSftpSource
    from datalink.pipeline.bronze import ingest_file

    sample_dir = Path(__file__).resolve().parents[2] / "data" / "sample"
    uploads = [
        ("PROVIDER", "provider_sample.csv"),
        ("MEMBERSHIP", "membership_sample.csv"),
        ("CLAIMS", "claims_sample.csv"),
    ]
    sftp = ctx.adapters.sftp
    # Ensure files are on the SFTP drop — upload helper is AtmozSftpSource-specific.
    if isinstance(sftp, AtmozSftpSource):
        for _, fn in uploads:
            local = sample_dir / fn
            if local.exists():
                sftp.upload(local)

    results: dict[str, Any] = {}
    for source_type, fn in uploads:
        remote = f"{ctx.settings.adapters.sftp.remote_base_dir}/{fn}"
        # Phase 6: per-source delimiter / format config (defaults to CSV).
        source_fmt = ctx.settings.sources.for_source(source_type)
        r = ingest_file(
            ctx.adapters,
            source_type,
            remote,
            batch_id=ctx.run_id,
            client_id=ctx.client_id,
            source_format=source_fmt,
        )
        results[source_type] = {
            "rows_in_source": r.rows_in_source,
            "rows_in_target_after": r.rows_in_target_after,
            "batch_id": r.batch_id,
        }
    _log.info(
        "task.bronze_ingest.done", **{k: v["rows_in_target_after"] for k, v in results.items()}
    )
    return {"ingested": results}


def task_bronze_checkpoint(ctx: TaskContext) -> dict[str, Any]:
    """CP1 Bronze structural checkpoint via the Phase-5 hooks module."""
    from datalink.pipeline.hooks import run_checkpoint_with_hooks
    from datalink.quality.suites import BRONZE_STRUCTURAL, build_bronze_suite

    hc = run_checkpoint_with_hooks(
        adapters=ctx.adapters,
        settings=ctx.settings,
        pipeline_id=ctx.pipeline_id,
        run_id=ctx.run_id,
        client_id=ctx.client_id,
        checkpoint_name=BRONZE_STRUCTURAL,
        qualified_table=f"{schema_for(ctx.client_id, Layer.BRONZE)}.RAW_CLAIMS",
        suite_builder=build_bronze_suite,
    )
    return _checkpoint_summary(hc, BRONZE_STRUCTURAL)


# ----------------------------------------------------------------------------
# SILVER tasks (dbt-driven)
# ----------------------------------------------------------------------------


def task_dbt_run_silver(ctx: TaskContext) -> dict[str, Any]:
    """Invoke `dbt run --select silver` — Silver DV 2.0 build from Bronze."""
    return _run_dbt(["run", "--select", "silver"], ctx=ctx)


def task_dbt_test_silver(ctx: TaskContext) -> dict[str, Any]:
    """Invoke `dbt test --select silver` — 60 DV 2.0 tests."""
    return _run_dbt(["test", "--select", "silver"], ctx=ctx)


def task_silver_checkpoint(ctx: TaskContext) -> dict[str, Any]:
    """CP2 Silver clinical checkpoint."""
    from datalink.pipeline.hooks import run_checkpoint_with_hooks
    from datalink.quality.suites import SILVER_CLINICAL, build_silver_suite

    hc = run_checkpoint_with_hooks(
        adapters=ctx.adapters,
        settings=ctx.settings,
        pipeline_id=ctx.pipeline_id,
        run_id=ctx.run_id,
        client_id=ctx.client_id,
        checkpoint_name=SILVER_CLINICAL,
        qualified_table=f"{schema_for(ctx.client_id, Layer.SILVER_DV)}.sat_claim_details",
        suite_builder=build_silver_suite,
    )
    return _checkpoint_summary(hc, SILVER_CLINICAL)


# ----------------------------------------------------------------------------
# GOLD tasks (dbt-driven + router fan-out)
# ----------------------------------------------------------------------------


def task_dbt_seed_gold(ctx: TaskContext) -> dict[str, Any]:
    """Load Lu* seed CSVs into the warehouse — `dbt seed`."""
    return _run_dbt(["seed"], ctx=ctx)


def task_dbt_run_gold(ctx: TaskContext) -> dict[str, Any]:
    """Invoke `dbt run --select gold` — build 5 Gold UM models from Silver."""
    return _run_dbt(["run", "--select", "gold"], ctx=ctx)


def task_dbt_test_gold(ctx: TaskContext) -> dict[str, Any]:
    """Invoke `dbt test --select gold`."""
    return _run_dbt(["test", "--select", "gold"], ctx=ctx)


def task_gold_checkpoint(ctx: TaskContext) -> dict[str, Any]:
    """CP3 Gold business-rule checkpoint."""
    from datalink.pipeline.hooks import run_checkpoint_with_hooks
    from datalink.quality.suites import GOLD_BUSINESS, build_gold_suite

    hc = run_checkpoint_with_hooks(
        adapters=ctx.adapters,
        settings=ctx.settings,
        pipeline_id=ctx.pipeline_id,
        run_id=ctx.run_id,
        client_id=ctx.client_id,
        checkpoint_name=GOLD_BUSINESS,
        qualified_table=f"{schema_for(ctx.client_id, Layer.GOLD_UM)}.gold_patient_auth",
        suite_builder=build_gold_suite,
    )
    return _checkpoint_summary(hc, GOLD_BUSINESS)


def task_router_push(ctx: TaskContext) -> dict[str, Any]:
    """Fan Gold UM out to every target in features.warehouse_router.targets."""
    from datalink.pipeline.router import push_gold_um_to_operational

    result = push_gold_um_to_operational(ctx.adapters, ctx.settings)
    return {
        "all_green": result.all_green,
        "targets_requested": result.targets_requested,
        "targets_skipped": result.targets_skipped,
        "per_target_row_totals": {t.target: t.total_rows for t in result.per_target},
    }


# ----------------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------------


def _run_dbt(args: list[str], ctx: TaskContext | None = None) -> dict[str, Any]:
    """Shell out to `dbt <args>` — use the same project-dir + profiles-dir the Makefile uses.

    IMPORTANT: DuckDB holds an exclusive process-level file lock. If the caller
    has an open warehouse connection (lazily built on `ctx.adapters`), dbt's
    subprocess can't open the same file. We close + null the in-process
    warehouse handle before shelling out, and let the next task re-open it
    lazily. This is transparent to caller code — just don't keep a reference
    to `ctx.adapters.warehouse` across a dbt task.

    Phase 6: passes `--vars '{client_id: <ctx.client_id>}'` so the
    `generate_schema_name.sql` macro can suffix schemas per tenant. Absent
    ctx or default client, no `--vars` flag is added (backward compat).
    """
    if ctx is not None and ctx._adapters is not None:
        close_fn = getattr(ctx._adapters.warehouse, "close", None)
        if callable(close_fn):
            close_fn()
        # Force the next `ctx.adapters` access to rebuild fresh.
        ctx._adapters = None

    repo_root = Path(__file__).resolve().parents[2]
    cmd = ["dbt", *args, "--project-dir", "dbt", "--profiles-dir", "dbt"]
    # Thread tenant through as a dbt var so generate_schema_name.sql picks it up.
    if ctx is not None and ctx.client_id and ctx.client_id != "default":
        cmd.extend(["--vars", f"{{client_id: {ctx.client_id}}}"])
    _log.info("task.dbt.run", cmd=" ".join(cmd))
    proc = subprocess.run(
        cmd,
        cwd=str(repo_root),
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        _log.error(
            "task.dbt.failed",
            returncode=proc.returncode,
            stdout_tail=proc.stdout[-1000:],
            stderr_tail=proc.stderr[-1000:],
        )
        raise RuntimeError(f"dbt {' '.join(args)} failed (exit {proc.returncode})")
    return {
        "returncode": proc.returncode,
        "stdout_tail": proc.stdout[-500:],
    }


def _checkpoint_summary(hc: Any, name: str) -> dict[str, Any]:
    """Strip a HookedCheckpoint to a JSON-safe dict (Airflow XCom-friendly)."""
    cp = hc.checkpoint
    return {
        "checkpoint_name": name,
        "status": cp.status.value,
        "total_expectations": cp.total_expectations,
        "failed_expectations": cp.failed_expectations,
        "fail_pct": cp.fail_pct,
        "row_count": cp.row_count,
        "pipeline_paused": hc.pipeline_paused,
        "pre_val_agent_count": len(hc.pre_val_results),
        "post_val_agent_count": len(hc.post_val_results),
    }


def generate_run_id() -> str:
    """Stable run-id helper — used when the orchestrator doesn't supply one."""
    return f"run-{uuid.uuid4().hex[:12]}"
