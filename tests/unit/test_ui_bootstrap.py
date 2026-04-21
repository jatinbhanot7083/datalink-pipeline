"""Regression tests for datalink.ui._bootstrap — Phase 6 fresh-boot fix.

Guards the 'IOException: Cannot open database in read-only mode' bug
that surfaced on the very first `docker compose up` before any DAG had
run. Without the bootstrap helper, every Streamlit page stack-traces
because `warehouse.duckdb` doesn't exist yet.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from datalink.ui._bootstrap import ensure_warehouse_exists


@pytest.mark.unit
def test_ensure_warehouse_creates_missing_file(tmp_path: Path) -> None:
    """Fresh-boot scenario: file doesn't exist, bootstrap must create it."""
    db = tmp_path / "wh.duckdb"
    assert not db.exists()
    ensure_warehouse_exists(str(db))
    assert db.exists(), "bootstrap must create the file"


@pytest.mark.unit
def test_ensure_warehouse_creates_control_schema(tmp_path: Path) -> None:
    """After bootstrap, CONTROL schema must be queryable read-only."""
    db = tmp_path / "wh.duckdb"
    ensure_warehouse_exists(str(db))

    # This is the EXACT scenario that broke before: open read-only right after boot.
    conn = duckdb.connect(str(db), read_only=True)
    try:
        rows = conn.execute(
            "SELECT table_name FROM information_schema.tables " "WHERE table_schema = 'CONTROL'"
        ).fetchall()
        tables = {r[0] for r in rows}
    finally:
        conn.close()

    # Every table UI pages query must exist after bootstrap.
    required = {
        "dq_suites",
        "pipeline_control_state",
        "pipeline_task_progress",
        "gx_validation_results",
        "agent_reasoning_log",
        "pipeline_checkpoints",
    }
    missing = required - tables
    assert not missing, f"bootstrap must create: {missing}"


@pytest.mark.unit
def test_ensure_warehouse_is_idempotent(tmp_path: Path) -> None:
    """Repeated calls must NOT wipe existing data or error."""
    db = tmp_path / "wh.duckdb"
    ensure_warehouse_exists(str(db))

    # Write a sentinel row that must survive a second bootstrap call.
    conn = duckdb.connect(str(db), read_only=False)
    conn.execute(
        "INSERT INTO CONTROL.dq_suites "
        "(suite_id, client_id, suite_name, version, status, expectations, source, created_by) "
        "VALUES ('s1', 'c1', 'n1', 1, 'LIVE', '[]', 'ui', 'alice')"
    )
    conn.close()

    # Second call — must be a no-op for the data.
    ensure_warehouse_exists(str(db))

    conn = duckdb.connect(str(db), read_only=True)
    try:
        rows = conn.execute("SELECT suite_id FROM CONTROL.dq_suites").fetchall()
    finally:
        conn.close()
    assert [r[0] for r in rows] == ["s1"], "idempotent bootstrap must preserve data"


@pytest.mark.unit
def test_ensure_warehouse_swallows_errors(tmp_path: Path) -> None:
    """Bootstrap must never raise — page render must not depend on it."""
    # Unwritable parent directory → real error under the hood, but the
    # contract is that nothing propagates out.
    try:
        ensure_warehouse_exists("/proc/1/cmdline/not-a-real-path.duckdb")
    except Exception as exc:  # pragma: no cover — would fail the test
        pytest.fail(f"bootstrap must not raise on errors, got {exc!r}")


@pytest.mark.unit
def test_read_only_open_after_bootstrap_does_not_raise(tmp_path: Path) -> None:
    """The end-to-end regression: after bootstrap, the original failing
    duckdb.connect(path, read_only=True) must succeed. THIS IS THE BUG."""
    db = tmp_path / "fresh_wh.duckdb"
    assert not db.exists()

    # Before bootstrap — original bug must still exist.
    with pytest.raises(Exception, match="read-only"):
        duckdb.connect(str(db), read_only=True)

    # After bootstrap — must succeed.
    ensure_warehouse_exists(str(db))
    conn = duckdb.connect(str(db), read_only=True)
    conn.close()  # if we get here, the fix works
