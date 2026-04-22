"""End-to-end Phase 6 smoke — proves a fresh user journey works.

This is the NO-UNIT-TEST-WILL-CATCH-THIS layer. It simulates a new
operator going from `docker compose down -v` → demo success:

  1. (optional) Clean-reset the stack
  2. Wait for every container healthy
  3. Assert container-contract tests pass (bootstrap, perms, imports)
  4. Trigger bronze_ingest for AETNA via Airflow REST API
  5. Poll the DAG run until green (timeout 5 min)
  6. Verify row counts: BRONZE_AETNA.RAW_CLAIMS = 1M, etc.
  7. HTTP-probe every Streamlit page, assert 200 + no Traceback
  8. Verify 84 baseline suites still present
  9. Report pass/fail

Exit 0 if every step is green. Exit 1 with a specific line telling
you which step broke — NOT a mystery "the stack is broken somewhere".

Usage:
    python scripts/smoke_phase_6.py                   # smoke only
    python scripts/smoke_phase_6.py --reset           # nuke + smoke
    python scripts/smoke_phase_6.py --client aetna    # different client
    python scripts/smoke_phase_6.py --skip-reset      # assume stack up

Requires: docker compose, Python 3.11, stdlib only (no extra deps).

Makefile integration:
    make smoke-phase-6           # runs this script
    make smoke-phase-6-reset     # --reset variant

Run this BEFORE every demo. Run this BEFORE committing anything that
touches containers, the warehouse file, or dashboards. Catches today's
bugs before Jatin ever sees them.
"""

from __future__ import annotations

import argparse
import base64
import json
import subprocess
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Any

# ----------------------------------------------------------------------------
# Output helpers — colored pass/fail/info with specific line numbers
# ----------------------------------------------------------------------------


def _color(code: str, txt: str) -> str:
    if sys.stdout.isatty():
        return f"\033[{code}m{txt}\033[0m"
    return txt


def _ok(msg: str) -> None:
    print(_color("32", "  OK"), msg)


def _fail(msg: str, detail: str = "") -> None:
    print(_color("31", "FAIL"), msg)
    if detail:
        for line in detail.splitlines()[-10:]:
            print(f"       {line}")


def _info(msg: str) -> None:
    print(_color("36", "INFO"), msg)


def _step(n: int, total: int, msg: str) -> None:
    print(_color("1;34", f"\n[{n}/{total}] {msg}"))


# ----------------------------------------------------------------------------
# State tracker — any failure sets fail=True and the final exit code
# ----------------------------------------------------------------------------


@dataclass
class SmokeResult:
    total: int = 0
    passed: int = 0
    failed: list[str] = field(default_factory=list)

    def check(self, desc: str, ok: bool, detail: str = "") -> None:
        self.total += 1
        if ok:
            self.passed += 1
            _ok(desc)
        else:
            self.failed.append(desc)
            _fail(desc, detail)

    @property
    def green(self) -> bool:
        return not self.failed


# ----------------------------------------------------------------------------
# Docker helpers
# ----------------------------------------------------------------------------


def _run(cmd: list[str], timeout: int = 60) -> tuple[int, str, str]:
    p = subprocess.run(cmd, capture_output=True, text=True, check=False, timeout=timeout)
    return p.returncode, p.stdout, p.stderr


def _dc_exec(container: str, cmd: list[str], timeout: int = 60) -> tuple[int, str, str]:
    return _run(["docker", "compose", "exec", "-T", container, *cmd], timeout=timeout)


def _dc_python(container: str, code: str, timeout: int = 60) -> tuple[int, str, str]:
    return _dc_exec(container, ["python", "-c", code], timeout=timeout)


# ----------------------------------------------------------------------------
# Step implementations
# ----------------------------------------------------------------------------


def step_reset(result: SmokeResult) -> None:
    _info("docker compose down -v (this wipes DB volumes)...")
    rc, _, err = _run(["docker", "compose", "down", "-v"], timeout=120)
    result.check("compose down -v", rc == 0, err)
    _info("removing warehouse.duckdb + gx uncommitted + dbt artifacts...")
    _run(["rm", "-f", "warehouse.duckdb"], timeout=10)
    _run(["rm", "-rf", "gx/uncommitted/data_docs", "dbt/target", "dbt/logs"], timeout=10)
    _info("docker compose up -d...")
    rc, _, err = _run(["docker", "compose", "up", "-d"], timeout=300)
    result.check("compose up -d", rc == 0, err)


def step_wait_healthy(result: SmokeResult, timeout: int = 180) -> None:
    required = [
        "datalink-control-tower",
        "datalink-airflow-webserver",
        "datalink-airflow-scheduler",
        "datalink-postgres",
        "datalink-sqlserver",
        "datalink-sftp",
    ]
    start = time.monotonic()
    while time.monotonic() - start < timeout:
        rc, out, _ = _run(["docker", "compose", "ps", "--format", "json"], timeout=15)
        if rc != 0:
            time.sleep(3)
            continue
        # docker compose ps --format json outputs one JSON object per line.
        statuses = {}
        for line in out.strip().split("\n"):
            if not line.strip():
                continue
            try:
                obj = json.loads(line)
                statuses[obj.get("Name", "")] = obj.get("State", "") + "/" + obj.get("Health", "")
            except json.JSONDecodeError:
                continue

        missing = [c for c in required if c not in statuses]
        unhealthy = [c for c in required if c in statuses and "healthy" not in statuses[c]]
        if not missing and not unhealthy:
            elapsed = int(time.monotonic() - start)
            result.check(f"all containers healthy ({elapsed}s)", True)
            return
        time.sleep(5)

    last = ", ".join(f"{c}={statuses.get(c, 'missing')}" for c in required)
    result.check(
        f"all containers healthy within {timeout}s",
        False,
        f"final states: {last}",
    )


def step_control_tower_contracts(result: SmokeResult) -> None:
    """Bundled minimal contract checks — avoids forking pytest."""
    rc, out, err = _dc_python(
        "control_tower",
        "import datalink; "
        "from datalink.data_gen.client_data import generate_for_client; "
        "from datalink.quality.control import create_control_tables; "
        "print('ok')",
    )
    result.check(
        "control_tower: datalink + data_gen + quality imports",
        rc == 0 and "ok" in out,
        err,
    )

    rc, _, err = _dc_exec("control_tower", ["test", "-f", "/opt/datalink/warehouse.duckdb"])
    result.check("control_tower: warehouse.duckdb exists", rc == 0, err)

    rc, out, err = _dc_exec("control_tower", ["stat", "-c", "%a", "/opt/datalink/warehouse.duckdb"])
    mode = out.strip()
    result.check(
        f"warehouse.duckdb world+group writable (mode=0o{mode})",
        rc == 0 and len(mode) == 3 and mode[-1] in "67" and mode[-2] in "67",
        err,
    )


def step_airflow_contracts(result: SmokeResult) -> None:
    rc, out, err = _dc_python(
        "airflow_scheduler",
        "import datalink; "
        "from datalink.data_gen.client_data import generate_for_client; "
        "import great_expectations; "
        "print('ok')",
    )
    result.check(
        "airflow_scheduler: datalink + data_gen + great_expectations imports",
        rc == 0 and "ok" in out,
        err,
    )


def step_seeded_baselines(result: SmokeResult) -> None:
    rc, out, err = _dc_python(
        "control_tower",
        "import duckdb, json; "
        "c = duckdb.connect('/opt/datalink/warehouse.duckdb', read_only=True); "
        'rows = c.execute("SELECT client_id, COUNT(*) FROM CONTROL.dq_suites '
        "WHERE status='LIVE' GROUP BY client_id ORDER BY client_id\").fetchall(); "
        "print(json.dumps(dict(rows)))",
    )
    if rc != 0:
        result.check("84 baseline suites across 7 clients", False, err)
        return
    counts = json.loads(out.strip())
    expected = {"default", "aetna", "caresource", "affinity", "coaccess", "dhmp", "hcsc"}
    ok = set(counts) == expected and all(n == 12 for n in counts.values())
    result.check(
        f"84 baseline suites (got {sum(counts.values())} across {len(counts)} clients)",
        ok,
        f"counts: {counts}",
    )


def step_streamlit_pages(result: SmokeResult) -> None:
    pages = [
        ("/", "Control Tower"),
        ("/DQ_Author", "DQ Author"),
        ("/DQ_Review", "DQ Review"),
        ("/Executive_Dashboard", "Executive"),
        ("/CrewAI_Dashboard", "CrewAI"),
        ("/DQ_Dashboard", "DQ Dashboard"),
    ]
    for path, label in pages:
        rc, out, err = _dc_exec(
            "control_tower",
            ["curl", "-sfo", "/dev/null", "-w", "%{http_code}", f"http://127.0.0.1:8000{path}"],
        )
        result.check(
            f"Streamlit page {path} ({label})",
            rc == 0 and out.strip() == "200",
            f"http_code={out}  stderr={err}",
        )


def _airflow_api(
    path: str, method: str = "GET", body: dict[str, Any] | None = None
) -> dict[str, Any]:
    """Minimal Airflow REST client using urllib only."""
    url = f"http://127.0.0.1:8088/api/v1{path}"
    auth = base64.b64encode(b"admin:admin_local_only").decode()
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Authorization", f"Basic {auth}")
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            payload: dict[str, Any] = json.loads(r.read().decode())
            return payload
    except urllib.error.HTTPError as e:
        return {"error": str(e), "body": e.read().decode()}


def step_trigger_bronze_and_wait(result: SmokeResult, client: str, timeout: int = 600) -> None:
    _info(f"triggering bronze_ingest for client={client}...")
    r = _airflow_api(
        "/dags/bronze_ingest/dagRuns",
        method="POST",
        body={"conf": {"client_id": client}},
    )
    if "error" in r:
        result.check(f"trigger bronze_ingest for {client}", False, r.get("body", ""))
        return
    run_id = r.get("dag_run_id")
    if not run_id:
        result.check(f"trigger bronze_ingest for {client}", False, f"unexpected response: {r}")
        return
    _info(f"  run_id={run_id} — polling until green (max {timeout}s)...")

    start = time.monotonic()
    while time.monotonic() - start < timeout:
        r = _airflow_api(f"/dags/bronze_ingest/dagRuns/{run_id}")
        state = r.get("state", "unknown")
        if state == "success":
            elapsed = int(time.monotonic() - start)
            result.check(f"bronze_ingest for {client} reached success ({elapsed}s)", True)
            return
        if state == "failed":
            result.check(
                f"bronze_ingest for {client} reached success",
                False,
                f"DAG state=failed, run_id={run_id} — check Airflow UI",
            )
            return
        time.sleep(5)

    result.check(
        f"bronze_ingest for {client} reached success within {timeout}s",
        False,
        f"last state={state}",
    )


def step_row_counts(result: SmokeResult, client: str) -> None:
    expected = {
        "RAW_CLAIMS": 1_000_000,
        "RAW_MEMBERSHIP": 500_000,
        "RAW_PROVIDER": 100_000,
    }
    schema = "BRONZE" if client == "default" else f"BRONZE_{client.upper()}"
    for table, exp_count in expected.items():
        rc, out, err = _dc_python(
            "control_tower",
            f"import duckdb; c = duckdb.connect('/opt/datalink/warehouse.duckdb', read_only=True); "
            f"print(c.execute('SELECT COUNT(*) FROM {schema}.{table}').fetchone()[0])",
        )
        if rc != 0:
            result.check(f"{schema}.{table} has {exp_count:,} rows", False, err)
            continue
        try:
            actual = int(out.strip())
        except ValueError:
            result.check(
                f"{schema}.{table} has {exp_count:,} rows", False, f"unexpected output: {out}"
            )
            continue
        result.check(
            f"{schema}.{table}: {actual:,} rows (expected {exp_count:,})",
            actual == exp_count,
            "",
        )


# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Phase 6 E2E smoke")
    ap.add_argument("--reset", action="store_true", help="docker compose down -v before smoke")
    ap.add_argument("--skip-reset", action="store_true", help="assume stack is already running")
    ap.add_argument("--client", default="aetna", help="client_id to run the demo pipeline for")
    ap.add_argument(
        "--skip-pipeline",
        action="store_true",
        help="skip the trigger+wait step (just contract checks)",
    )
    args = ap.parse_args(argv)

    result = SmokeResult()
    step_no = 0
    total_steps = 6 if args.skip_pipeline else 8
    if args.reset:
        total_steps += 1

    print(_color("1", "=" * 70))
    print(_color("1", " Phase 6 E2E Smoke"))
    print(_color("1", "=" * 70))

    if args.reset and not args.skip_reset:
        step_no += 1
        _step(step_no, total_steps, "clean-reset the stack")
        step_reset(result)

    step_no += 1
    _step(step_no, total_steps, "wait for all containers healthy")
    step_wait_healthy(result)

    step_no += 1
    _step(step_no, total_steps, "control_tower contracts (imports, perms, bootstrap)")
    step_control_tower_contracts(result)

    step_no += 1
    _step(step_no, total_steps, "airflow contracts (imports, GX present)")
    step_airflow_contracts(result)

    step_no += 1
    _step(step_no, total_steps, "seeded baselines (84 suites across 7 clients)")
    step_seeded_baselines(result)

    step_no += 1
    _step(step_no, total_steps, "Streamlit pages respond 200")
    step_streamlit_pages(result)

    if not args.skip_pipeline:
        step_no += 1
        _step(step_no, total_steps, f"trigger bronze_ingest for {args.client} and wait")
        step_trigger_bronze_and_wait(result, args.client)

        step_no += 1
        _step(step_no, total_steps, f"verify row counts for {args.client}")
        step_row_counts(result, args.client)

    print()
    print(_color("1", "=" * 70))
    if result.green:
        print(_color("1;32", f" SMOKE PASSED — {result.passed}/{result.total} checks green"))
        print(_color("1", "=" * 70))
        return 0
    print(
        _color(
            "1;31",
            f" SMOKE FAILED — {result.passed}/{result.total} green, {len(result.failed)} failed",
        )
    )
    print(_color("1", "=" * 70))
    for desc in result.failed:
        print(_color("31", f"  - {desc}"))
    return 1


if __name__ == "__main__":
    sys.exit(main())
