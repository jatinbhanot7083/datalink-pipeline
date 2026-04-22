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
    """Ingest per-client CSVs (claims / membership / provider) from SFTP → Bronze.

    Phase 6 data volumes (production-realistic):
        Membership:    500K rows/client
        Provider:      100K rows/client
        Claims:          1M rows/client

    Data is generated ONCE per client (cached at data/generated/{client}/)
    using the deterministic-per-client generator in
    datalink.data_gen.client_data. First run for a new client takes
    ~30-60 s to generate + ingest; subsequent runs reuse the cached CSVs.

    Uploaded files are renamed per-client with UTC timestamp so the
    Bronze _source_file audit column captures provenance:
        claims_AETNA_20260421_213045.csv
    """
    import posixpath
    from datetime import UTC, datetime

    from datalink.adapters.sftp.atmoz import AtmozSftpSource

    # Import the generator lazily — keeps test-time imports fast, and
    # generator depends on numpy which is already in the Airflow image.
    from datalink.data_gen.client_data import generate_for_client
    from datalink.pipeline.bronze import ingest_file

    repo_root = Path(__file__).resolve().parents[2]
    generated_dir = repo_root / "data" / "generated" / ctx.client_id
    _log.info(
        "task.bronze_ingest.generating_if_needed",
        client_id=ctx.client_id,
        target_dir=str(generated_dir),
    )
    generated_paths = generate_for_client(ctx.client_id, generated_dir)
    _log.info(
        "task.bronze_ingest.generation_done",
        client_id=ctx.client_id,
        files={k: str(v) for k, v in generated_paths.items()},
    )

    client_tag = ctx.client_id.upper()
    ts_tag = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    # Per-source upload plan: local generated file → timestamped SFTP filename.
    # Order: PROVIDER → MEMBERSHIP → CLAIMS so referential checks at Silver work.
    sources: list[tuple[str, Path, str]] = [
        ("PROVIDER", generated_paths["PROVIDER"], f"provider_{client_tag}_{ts_tag}.csv"),
        ("MEMBERSHIP", generated_paths["MEMBERSHIP"], f"membership_{client_tag}_{ts_tag}.csv"),
        ("CLAIMS", generated_paths["CLAIMS"], f"claims_{client_tag}_{ts_tag}.csv"),
    ]

    sftp = ctx.adapters.sftp
    base_dir = ctx.settings.adapters.sftp.remote_base_dir
    # Ensure files are on the SFTP drop — upload helper is AtmozSftpSource-specific.
    if isinstance(sftp, AtmozSftpSource):
        for _, local_path, remote_fn in sources:
            if local_path.exists():
                sftp.upload(local_path, posixpath.join(base_dir, remote_fn))

    results: dict[str, Any] = {}
    for source_type, _local_path, remote_fn in sources:
        remote = posixpath.join(base_dir, remote_fn)
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
            "source_file": remote_fn,  # what landed in the audit column
            "client_id": ctx.client_id,
            "timestamp": ts_tag,
        }
    _log.info(
        "task.bronze_ingest.done",
        client_id=ctx.client_id,
        timestamp=ts_tag,
        **{k: v["rows_in_target_after"] for k, v in results.items()},
    )
    return {"ingested": results, "client_id": ctx.client_id, "timestamp": ts_tag}


def task_bronze_checkpoint(ctx: TaskContext) -> dict[str, Any]:
    """CP1 Bronze structural checkpoint — runs per (client, source_type).

    Phase 6: loops CLAIMS / MEMBERSHIP / PROVIDER. Each source's dedicated
    suite (e.g. bronze_claims, bronze_membership, bronze_provider) is
    looked up in the DB registry. Per-source results are aggregated into
    a single task output; dashboards drill into each via source_type.

    Falls back to the legacy aggregate 'bronze_structural' suite if a
    per-source suite isn't found — keeps Phase-5.x test harnesses working.
    """
    from datalink.pipeline.hooks import run_checkpoint_with_hooks
    from datalink.quality.suites import BRONZE_STRUCTURAL, build_bronze_suite

    bronze_schema = schema_for(ctx.client_id, Layer.BRONZE)
    source_targets = [
        ("CLAIMS", "bronze_claims", f"{bronze_schema}.RAW_CLAIMS"),
        ("MEMBERSHIP", "bronze_membership", f"{bronze_schema}.RAW_MEMBERSHIP"),
        ("PROVIDER", "bronze_provider", f"{bronze_schema}.RAW_PROVIDER"),
    ]
    per_source: dict[str, Any] = {}
    for source_type, suite_name, table in source_targets:
        # Use per-source suite as primary; legacy aggregate as fallback.
        hc = run_checkpoint_with_hooks(
            adapters=ctx.adapters,
            settings=ctx.settings,
            pipeline_id=ctx.pipeline_id,
            run_id=f"{ctx.run_id}__{source_type.lower()}",
            client_id=ctx.client_id,
            source_type=source_type,
            checkpoint_name=suite_name,
            qualified_table=table,
            suite_builder=build_bronze_suite,
        )
        per_source[source_type] = _checkpoint_summary(hc, suite_name)
    # Legacy aggregate write — keeps Phase-5.x demo scripts green.
    hc_legacy = run_checkpoint_with_hooks(
        adapters=ctx.adapters,
        settings=ctx.settings,
        pipeline_id=ctx.pipeline_id,
        run_id=ctx.run_id,
        client_id=ctx.client_id,
        source_type="CLAIMS",
        checkpoint_name=BRONZE_STRUCTURAL,
        qualified_table=f"{bronze_schema}.RAW_CLAIMS",
        suite_builder=build_bronze_suite,
    )
    return {"per_source": per_source, "legacy": _checkpoint_summary(hc_legacy, BRONZE_STRUCTURAL)}


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
    """CP2 Silver clinical checkpoint — per (client, source_type).

    Phase 6: each source_type validates its own satellite:
      CLAIMS      -> sat_claim_details       (suite: silver_claims)
      MEMBERSHIP  -> sat_member_demographics (suite: silver_membership)
      PROVIDER    -> sat_provider_info       (suite: silver_provider)
    Legacy aggregate (silver_clinical) runs last for Phase-5.x compat.
    """
    from datalink.pipeline.hooks import run_checkpoint_with_hooks
    from datalink.quality.suites import SILVER_CLINICAL, build_silver_suite

    silver_schema = schema_for(ctx.client_id, Layer.SILVER_DV)
    source_targets = [
        ("CLAIMS", "silver_claims", f"{silver_schema}.sat_claim_details"),
        ("MEMBERSHIP", "silver_membership", f"{silver_schema}.sat_member_demographics"),
        ("PROVIDER", "silver_provider", f"{silver_schema}.sat_provider_info"),
    ]
    per_source: dict[str, Any] = {}
    for source_type, suite_name, table in source_targets:
        hc = run_checkpoint_with_hooks(
            adapters=ctx.adapters,
            settings=ctx.settings,
            pipeline_id=ctx.pipeline_id,
            run_id=f"{ctx.run_id}__{source_type.lower()}",
            client_id=ctx.client_id,
            source_type=source_type,
            checkpoint_name=suite_name,
            qualified_table=table,
            suite_builder=build_silver_suite,
        )
        per_source[source_type] = _checkpoint_summary(hc, suite_name)
    hc_legacy = run_checkpoint_with_hooks(
        adapters=ctx.adapters,
        settings=ctx.settings,
        pipeline_id=ctx.pipeline_id,
        run_id=ctx.run_id,
        client_id=ctx.client_id,
        source_type="CLAIMS",
        checkpoint_name=SILVER_CLINICAL,
        qualified_table=f"{silver_schema}.sat_claim_details",
        suite_builder=build_silver_suite,
    )
    return {"per_source": per_source, "legacy": _checkpoint_summary(hc_legacy, SILVER_CLINICAL)}


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
    """CP3 Gold business-rule checkpoint — per (client, source_type).

    Phase 6: Gold UM consolidates all 3 sources into auth-centric tables.
    Per-source suites validate the aspect each source is responsible for,
    all targeting gold_patient_auth (the join point). Legacy aggregate
    (gold_business) runs last for Phase-5.x compat.
    """
    from datalink.pipeline.hooks import run_checkpoint_with_hooks
    from datalink.quality.suites import GOLD_BUSINESS, build_gold_suite

    gold_schema = schema_for(ctx.client_id, Layer.GOLD_UM)
    auth_table = f"{gold_schema}.gold_patient_auth"
    source_suites = [
        ("CLAIMS", "gold_claims"),
        ("MEMBERSHIP", "gold_membership"),
        ("PROVIDER", "gold_provider"),
    ]
    per_source: dict[str, Any] = {}
    for source_type, suite_name in source_suites:
        hc = run_checkpoint_with_hooks(
            adapters=ctx.adapters,
            settings=ctx.settings,
            pipeline_id=ctx.pipeline_id,
            run_id=f"{ctx.run_id}__{source_type.lower()}",
            client_id=ctx.client_id,
            source_type=source_type,
            checkpoint_name=suite_name,
            qualified_table=auth_table,
            suite_builder=build_gold_suite,
        )
        per_source[source_type] = _checkpoint_summary(hc, suite_name)
    hc_legacy = run_checkpoint_with_hooks(
        adapters=ctx.adapters,
        settings=ctx.settings,
        pipeline_id=ctx.pipeline_id,
        run_id=ctx.run_id,
        client_id=ctx.client_id,
        source_type="CLAIMS",
        checkpoint_name=GOLD_BUSINESS,
        qualified_table=auth_table,
        suite_builder=build_gold_suite,
    )
    return {"per_source": per_source, "legacy": _checkpoint_summary(hc_legacy, GOLD_BUSINESS)}


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
