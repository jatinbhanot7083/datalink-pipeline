"""Phase 11 verifier — schema drift, end-to-end.

Mirrors the ``runbook_verify_phase{9,10}.py`` pattern: assertion-based
PASS/FAIL output, exit 1 on any failure, runs against a throwaway
DuckDB so it's safe on any laptop and in CI.

Three sections:

  1. **Header reader** — ``_read_csv_header`` trims whitespace + returns
     names in source order.

  2. **Bronze drift pre-check** — exercises ``_check_schema_drift``
     against four scenarios:

       * No contract registered → silent pass (back-compat).
       * Clean batch → no log row.
       * Additive drift → ``schema_drift_log`` row with WARNING/LOGGED;
         load continues.
       * Subtractive drift → raises ``SchemaDriftError`` and writes a
         FATAL/HALTED log row.

  3. **Promote-to-contract** — simulates the Schema Drift UI's promote
     action: registers v2 with the additive columns appended; verifies
     a re-run of the same additive CSV is now clean (drift WARNING does
     NOT re-fire).

Usage::

    source .venv/bin/activate
    python -m scripts.runbook_verify_phase11

Exit 0 on green, 1 on any FAIL.
"""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

os.environ["DL_ADAPTERS__WAREHOUSE__TYPE"] = "duckdb"
TMPROOT = Path(tempfile.mkdtemp(prefix="phase11_3_"))
TMPDB = TMPROOT / "test.duckdb"
os.environ["DL_ADAPTERS__WAREHOUSE__PATH"] = str(TMPDB)

import contextlib  # noqa: E402

from datalink.adapters.factory import build_adapters  # noqa: E402
from datalink.config.loader import load_settings  # noqa: E402
from datalink.pipeline.bronze.ingest import (  # noqa: E402
    _check_schema_drift,
    _read_csv_header,
)
from datalink.pipeline.bronze.parsers import SourceFormatConfig  # noqa: E402
from datalink.quality.control import PipelineControl  # noqa: E402
from datalink.quality.schema_drift import (  # noqa: E402
    ColumnContract,
    SchemaDriftError,
    list_recent_drift_events,
    register_contract,
)

MEMBERSHIP_COLS = [
    ColumnContract("member_id", "TEXT", nullable=False),
    ColumnContract("subscriber_id", "TEXT"),
    ColumnContract("dob", "DATE"),
    ColumnContract("gender", "TEXT"),
    ColumnContract("plan_id", "TEXT"),
    ColumnContract("group_id", "TEXT"),
    ColumnContract("effective_date", "DATE"),
    ColumnContract("termination_date", "DATE"),
    ColumnContract("coverage_type", "TEXT"),
    ColumnContract("state", "TEXT"),
]


def _write_csv(path: Path, header: list[str], rows: list[list[str]]) -> None:
    import csv

    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


def main() -> int:
    failures: list[str] = []

    def fail(msg: str) -> None:
        failures.append(msg)
        print(f"   ❌ {msg}")

    def ok(msg: str) -> None:
        print(f"   ✅ {msg}")

    settings = load_settings()
    adapters = build_adapters(settings)
    wh = adapters.warehouse
    PipelineControl(wh).ensure()
    fmt = SourceFormatConfig()

    print()
    print("=== Header reader ===")

    # 1. Header read returns trimmed cols.
    csv_clean = TMPROOT / "clean.csv"
    _write_csv(csv_clean, ["member_id", " dob ", "gender"], [["M001", "1980-01-01", "F"]])
    cols = _read_csv_header(csv_clean, fmt)
    if cols != ["member_id", "dob", "gender"]:
        fail(f"header read wrong: {cols}")
    else:
        ok(f"_read_csv_header trims and returns {cols}")

    print()
    print("=== Drift pre-check (no contract = back-compat path) ===")

    # 2. No contract → returns silently, no drift log row.
    _check_schema_drift(
        wh,
        client_id="acme",
        source_type="MEMBERSHIP",
        local_csv=csv_clean,
        fmt=fmt,
        source_file="clean.csv",
        batch_id="batch-NO-CONTRACT",
    )
    events = list_recent_drift_events(wh, client_id="acme")
    if events:
        fail(f"no-contract path should not log drift; got {len(events)} rows")
    else:
        ok("no-contract path: silent skip, no drift log row")

    # Register contract for the rest of the scenarios.
    register_contract(
        wh,
        client_id="acme",
        source_type="MEMBERSHIP",
        columns=MEMBERSHIP_COLS,
        created_by="test",
        notes="initial contract",
    )

    print()
    print("=== Clean CSV with contract registered ===")

    # 3. Clean CSV — all 10 contract cols, nothing extra. No drift, no log.
    csv_full_clean = TMPROOT / "membership_clean.csv"
    _write_csv(
        csv_full_clean,
        [c.name for c in MEMBERSHIP_COLS],
        [["M001", "S001", "1980-01-01", "F", "P1", "G1", "2026-01-01", "", "PPO", "VA"]],
    )
    _check_schema_drift(
        wh,
        client_id="acme",
        source_type="MEMBERSHIP",
        local_csv=csv_full_clean,
        fmt=fmt,
        source_file="membership_clean.csv",
        batch_id="batch-CLEAN",
    )
    events = list_recent_drift_events(wh, client_id="acme")
    if events:
        fail(f"clean batch should not log drift; got {len(events)} rows")
    else:
        ok("clean batch: silent return, no drift log row")

    print()
    print("=== Additive drift (extra columns appended) ===")

    # 4. Additive — 2 extra cols.
    csv_additive = TMPROOT / "membership_additive.csv"
    additive_header = [c.name for c in MEMBERSHIP_COLS] + ["middle_initial", "marketing_opt_in"]
    _write_csv(
        csv_additive,
        additive_header,
        [["M001", "S001", "1980-01-01", "F", "P1", "G1", "2026-01-01", "", "PPO", "VA", "Q", "Y"]],
    )
    _check_schema_drift(
        wh,
        client_id="acme",
        source_type="MEMBERSHIP",
        local_csv=csv_additive,
        fmt=fmt,
        source_file="membership_additive.csv",
        batch_id="batch-ADD",
    )
    events = list_recent_drift_events(wh, client_id="acme")
    if not events:
        fail("additive should log; got 0 rows")
    elif events[0]["severity"] != "WARNING":
        fail(f"additive severity wrong: {events[0]['severity']}")
    elif events[0]["action_taken"] != "LOGGED":
        fail(f"additive action_taken wrong: {events[0]['action_taken']}")
    elif events[0]["drift_type"] != "ADDITIVE":
        fail(f"additive drift_type wrong: {events[0]['drift_type']}")
    else:
        ok(
            f"additive logged: severity=WARNING, action=LOGGED, type=ADDITIVE, "
            f"added cols in log = {events[0]['added_columns']}"
        )

    print()
    print("=== Subtractive drift (contracted column missing) ===")

    # 5. Subtractive — drop dob column. Should raise SchemaDriftError.
    csv_subtractive = TMPROOT / "membership_subtractive.csv"
    sub_header = [c.name for c in MEMBERSHIP_COLS if c.name != "dob"]
    _write_csv(
        csv_subtractive,
        sub_header,
        [["M001", "S001", "F", "P1", "G1", "2026-01-01", "", "PPO", "VA"]],
    )
    raised = False
    try:
        _check_schema_drift(
            wh,
            client_id="acme",
            source_type="MEMBERSHIP",
            local_csv=csv_subtractive,
            fmt=fmt,
            source_file="membership_subtractive.csv",
            batch_id="batch-SUB",
        )
    except SchemaDriftError as e:
        raised = True
        if "dob" not in str(e):
            fail(f"SchemaDriftError missing 'dob' in summary: {e}")
        else:
            ok(f"subtractive raised SchemaDriftError: {e}")

    if not raised:
        fail("subtractive should have raised SchemaDriftError; did not")

    events = list_recent_drift_events(wh, client_id="acme")
    halted = [e for e in events if e["action_taken"] == "HALTED"]
    if not halted:
        fail("subtractive should have logged with action_taken=HALTED")
    elif halted[0]["severity"] != "FATAL":
        fail(f"halted severity wrong: {halted[0]['severity']}")
    else:
        ok(
            f"subtractive logged: action=HALTED, severity=FATAL, drift_type={halted[0]['drift_type']}"
        )

    print()
    print("=== Drift log accumulates per batch ===")

    # 6. Should now have at least 2 rows (additive + subtractive).
    all_events = list_recent_drift_events(wh, client_id="acme", limit=50)
    if len(all_events) < 2:
        fail(f"expected >= 2 drift events accumulated, got {len(all_events)}")
    else:
        statuses = sorted(e["action_taken"] for e in all_events)
        ok(f"{len(all_events)} drift events accumulated, action_taken in {statuses}")

    print()
    print("=== Promote-to-contract loop (simulates Schema Drift UI action) ===")

    # 7. Promote: register v2 with the additive cols appended.
    promoted_columns = [
        *MEMBERSHIP_COLS,
        ColumnContract("middle_initial", "TEXT"),
        ColumnContract("marketing_opt_in", "BOOLEAN"),
    ]
    register_contract(
        wh,
        client_id="acme",
        source_type="MEMBERSHIP",
        columns=promoted_columns,
        created_by="user:alice",
        notes="promoted middle_initial + marketing_opt_in from drift events",
    )
    from datalink.quality.schema_drift import get_active_contract  # local import for clarity

    new_active = get_active_contract(wh, client_id="acme", source_type="MEMBERSHIP")
    if new_active is None or new_active.contract_version != 2:
        fail(
            f"contract v2 not active after promote: "
            f"{new_active.contract_version if new_active else None}"
        )
    elif len(new_active.columns) != 12:
        fail(f"v2 column count wrong: {len(new_active.columns)}")
    else:
        ok(f"contract bumped to v2 with {len(new_active.columns)} cols (was 10)")

    # 8. Re-run the SAME additive CSV — should now be clean (no new drift event).
    events_before = len(list_recent_drift_events(wh, client_id="acme", limit=200))
    _check_schema_drift(
        wh,
        client_id="acme",
        source_type="MEMBERSHIP",
        local_csv=csv_additive,
        fmt=fmt,
        source_file="membership_additive_replay.csv",
        batch_id="batch-ADD-REPLAY",
    )
    events_after = len(list_recent_drift_events(wh, client_id="acme", limit=200))
    if events_after != events_before:
        fail(
            f"expected zero new drift events after promote (was {events_before}, "
            f"now {events_after}); promote→clean loop failed"
        )
    else:
        ok("replaying same additive CSV against v2 → 0 new drift events (clean)")

    print()
    if failures:
        print(f"❌ {len(failures)} FAIL")
        for f in failures:
            print(f"   - {f}")
        return 1
    print(
        "✅ Phase 11 PASS — drift detection + contract promote loop verified " "(8 scenarios green)"
    )
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    finally:
        # Best-effort cleanup of the temp DB tree
        import shutil

        with contextlib.suppress(OSError):
            shutil.rmtree(TMPROOT, ignore_errors=True)
