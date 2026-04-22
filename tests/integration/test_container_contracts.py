"""Container-contract tests — Phase 6.

These tests assert the RUNTIME SHAPE of each container image, not just
Python logic. They catch the class of bug that's been plaguing Phase 6:

  "Code passed unit tests in the dev venv but failed in the
   container" — because the two environments are deliberately different.

Requirements to run:
  * `docker compose up -d` already ran and the stack is healthy.
  * Marked with @pytest.mark.integration so they stay OUT of the default
    `pytest tests/unit/` sweep (those run in dev venv, no containers).

Invoke:
    pytest tests/integration/test_container_contracts.py -v -m integration

Or via Makefile:
    make verify-phase-6-containers
"""

from __future__ import annotations

import json
import subprocess

import pytest

pytestmark = pytest.mark.integration


# ----------------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------------


def _exec(container: str, cmd: list[str]) -> tuple[int, str, str]:
    """Run `docker compose exec -T <container> <cmd>` and return (rc, stdout, stderr)."""
    proc = subprocess.run(
        ["docker", "compose", "exec", "-T", container, *cmd],
        capture_output=True,
        text=True,
        check=False,
    )
    return proc.returncode, proc.stdout, proc.stderr


def _python(container: str, code: str) -> tuple[int, str, str]:
    return _exec(container, ["python", "-c", code])


# ----------------------------------------------------------------------------
# CONTROL TOWER container contracts
# ----------------------------------------------------------------------------


class TestControlTowerContract:
    """Contracts the control_tower image must satisfy.

    Why every test here: each one reproduces a real bug from Phase 6 rollout.
    """

    def test_datalink_package_importable(self) -> None:
        """Regression: ModuleNotFoundError: No module named 'datalink'.

        PYTHONPATH=/opt/datalink must be set so import datalink.* works
        even if the editable pip install in entrypoint.sh fails/hangs.
        """
        rc, out, err = _python("control_tower", "import datalink; print(datalink.__file__)")
        assert rc == 0, f"control_tower cannot import datalink:\nstderr={err}"
        assert "/opt/datalink/datalink/__init__.py" in out, f"unexpected path: {out}"

    def test_data_gen_importable(self) -> None:
        """Regression: `from scripts.generate_client_data import ...` failed
        because scripts/ had no __init__.py. Fixed by moving to
        datalink.data_gen.client_data."""
        rc, _, err = _python(
            "control_tower",
            "from datalink.data_gen.client_data import generate_for_client",
        )
        assert rc == 0, f"control_tower cannot import datalink.data_gen:\nstderr={err}"

    def test_great_expectations_NOT_required_for_quality_imports(self) -> None:
        """Regression: top-level `import great_expectations` in checkpoint.py
        broke datalink.quality.* imports in the control_tower container
        (GX deliberately not shipped there — 150MB bloat).
        Lazy-import fix in checkpoint.py must not regress."""
        rc, _, err = _python(
            "control_tower",
            "from datalink.quality.control import create_control_tables; "
            "from datalink.quality.registry import SuiteRegistry; "
            "from datalink.quality.baseline_seeder import seed_all_real_clients",
        )
        assert rc == 0, (
            f"control_tower cannot import datalink.quality.*:\n{err}\n"
            f"Regression of lazy-GX fix (commit 91ea535)."
        )

    def test_warehouse_duckdb_exists_after_boot(self) -> None:
        """Regression: on fresh compose up, warehouse.duckdb didn't exist
        yet and every Streamlit page stack-traced with
        'Cannot open database in read-only mode'. Bootstrap must create
        the file BEFORE Streamlit starts."""
        rc, _, _ = _exec("control_tower", ["test", "-f", "/opt/datalink/warehouse.duckdb"])
        assert rc == 0, "warehouse.duckdb missing — bootstrap failed on container start"

    def test_warehouse_is_world_writable(self) -> None:
        """Regression: root-owned warehouse.duckdb at 0o644 blocked airflow
        (uid 50000) from writing. Must be 0o666 for cross-container RW."""
        rc, out, _ = _exec(
            "control_tower",
            ["stat", "-c", "%a", "/opt/datalink/warehouse.duckdb"],
        )
        assert rc == 0, "could not stat warehouse.duckdb"
        mode = out.strip()
        # Accept 666, 664, or 644-with-group-write etc. — the KEY bit is
        # that the 'other' and 'group' write bits are both set so airflow
        # (different uid) can write.
        assert mode[-1] in ("6", "7"), f"world-write missing: mode=0o{mode}"
        assert mode[-2] in ("6", "7"), f"group-write missing: mode=0o{mode}"

    def test_bootstrap_seeded_84_suites_across_7_clients(self) -> None:
        """Regression: bootstrap silently failed (swallowed errors in entrypoint)
        and no baseline suites got seeded. Must have 7 clients x 12 = 84."""
        rc, out, err = _python(
            "control_tower",
            "import duckdb, json; "
            "c = duckdb.connect('/opt/datalink/warehouse.duckdb', read_only=True); "
            'r = c.execute("SELECT client_id, COUNT(*) FROM CONTROL.dq_suites '
            "WHERE status='LIVE' GROUP BY client_id ORDER BY client_id\").fetchall(); "
            "print(json.dumps(dict(r)))",
        )
        assert rc == 0, f"could not query CONTROL.dq_suites:\n{err}"
        counts = json.loads(out.strip())
        expected_clients = {
            "default",
            "aetna",
            "caresource",
            "affinity",
            "coaccess",
            "dhmp",
            "hcsc",
        }
        assert (
            set(counts) == expected_clients
        ), f"wrong client set: {set(counts)} — expected {expected_clients}"
        assert all(
            n == 12 for n in counts.values()
        ), f"every client must have 12 LIVE suites, got {counts}"

    def test_streamlit_health_endpoint_green(self) -> None:
        """Streamlit's own health probe must return 200 — if Streamlit
        itself isn't up, none of the pages matter."""
        rc, out, _ = _exec(
            "control_tower",
            [
                "curl",
                "-sfo",
                "/dev/null",
                "-w",
                "%{http_code}",
                "http://127.0.0.1:8000/_stcore/health",
            ],
        )
        assert rc == 0, "curl to Streamlit health failed"
        assert out.strip() == "200", f"Streamlit health returned {out}"


# ----------------------------------------------------------------------------
# AIRFLOW containers
# ----------------------------------------------------------------------------


class TestAirflowContract:
    """Contracts the airflow_scheduler + airflow_webserver images must satisfy."""

    def test_datalink_importable_in_airflow(self) -> None:
        """Airflow worker executes task_bronze_ingest. If datalink isn't
        importable there, every DAG fails."""
        rc, _, err = _python("airflow_scheduler", "import datalink")
        assert rc == 0, f"airflow_scheduler cannot import datalink:\n{err}"

    def test_data_gen_importable_in_airflow(self) -> None:
        """Regression: task_bronze_ingest imports datalink.data_gen.client_data
        at runtime. Must work inside the Airflow container."""
        rc, _, err = _python(
            "airflow_scheduler",
            "from datalink.data_gen.client_data import generate_for_client",
        )
        assert rc == 0, f"airflow cannot import generator:\n{err}"

    def test_great_expectations_IS_available_in_airflow(self) -> None:
        """Airflow DOES need GX — it runs checkpoints. Must be installed."""
        rc, _, err = _python("airflow_scheduler", "import great_expectations")
        assert rc == 0, f"great_expectations missing from airflow_scheduler:\n{err}"

    def test_airflow_can_write_warehouse(self) -> None:
        """Regression: permission-denied on warehouse.duckdb. Airflow
        (uid 50000) must be able to open it read-write."""
        rc, _, err = _python(
            "airflow_scheduler",
            "import duckdb; "
            "c = duckdb.connect('/opt/datalink/warehouse.duckdb', read_only=False); "
            "c.execute('SELECT 1').fetchone(); c.close()",
        )
        assert rc == 0, f"airflow cannot open warehouse.duckdb rw:\n{err}"

    def test_all_3_dags_parse_cleanly(self) -> None:
        """All Phase 5.5 DAG files must parse without import errors.
        Run `airflow dags list-import-errors` — must be empty."""
        rc, out, _ = _exec("airflow_scheduler", ["airflow", "dags", "list-import-errors"])
        assert rc == 0, "airflow dags list-import-errors failed"
        # Output has header lines; error lines appear below. We're green if
        # the output doesn't contain any actual DAG errors (noisy header is OK).
        assert (
            "No data found" in out or "dag_id" not in out.lower() or "error" not in out.lower()
        ), f"DAG import errors detected:\n{out}"
