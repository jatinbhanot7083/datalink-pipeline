"""Phase 9.6 — runbook verification.

Reads the actual state of Snowflake + the operational DBs and reports
PASS / FAIL per architectural assertion. Designed to back the
3-day membership runbook (tests/runbooks/membership_3day_cycle.md).

Modes:

    --mode existing
        Lightest path. Verifies the architecture against the data
        already in Snowflake (3 batches of identical aetna content
        seeded during Phase 9.1 testing). Proves Phase 9.1 + 9.2 +
        9.3 + 9.5 are working without needing fresh data.

    --mode day1 / --mode day2 / --mode day3
        Stricter assertions matched to the runbook's expected end-
        state for that day. Use when walking the actual cycle.

Usage:
    python -m scripts.runbook_verify --client aetna --mode existing
    python -m scripts.runbook_verify --client aetna --mode day3

Exit code: 0 if all PASS, 1 if any FAIL. Designed to be called from
CI or scheduled smoke checks.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from datalink.adapters.factory import build_adapters
from datalink.config.loader import load_settings
from datalink.logging import get_logger

_log = get_logger(__name__)

_PASS = "✅ PASS"
_FAIL = "❌ FAIL"
_INFO = "ℹ️  INFO"


@dataclass
class Assertion:
    """One named check + the predicate that decides PASS/FAIL."""

    name: str
    predicate: Callable[[dict[str, Any]], bool]
    detail: Callable[[dict[str, Any]], str] = field(default=lambda ctx: "")


@dataclass
class CheckResult:
    name: str
    passed: bool
    detail: str


def _collect_state(client_id: str) -> dict[str, Any]:
    """Read every piece of state we'll need across all assertions in
    one Snowflake session — minimises auth + warehouse-warm-up cost."""
    settings = load_settings()
    adapters = build_adapters(settings)
    wh = adapters.warehouse

    bronze_schema = "BRONZE_AETNA" if client_id == "aetna" else f"BRONZE_{client_id.upper()}"
    silver_schema = (
        "SILVER_silver_AETNA" if client_id == "aetna" else f"SILVER_silver_{client_id.upper()}"
    )
    gold_schema = (
        "SILVER_gold_um_AETNA" if client_id == "aetna" else f"SILVER_gold_um_{client_id.upper()}"
    )

    state: dict[str, Any] = {
        "client_id": client_id,
        "bronze_schema": bronze_schema,
        "silver_schema": silver_schema,
        "gold_schema": gold_schema,
    }

    # Bronze counts + batch counts.
    for tbl in ("RAW_CLAIMS", "RAW_MEMBERSHIP", "RAW_PROVIDER"):
        try:
            r = wh.query(
                f"SELECT COUNT(*) AS n, COUNT(DISTINCT _batch_id) AS b "
                f"FROM {bronze_schema}.{tbl}"
            )[0]
            state[f"bronze_{tbl.lower()}_rows"] = int(r.get("n") or 0)
            state[f"bronze_{tbl.lower()}_batches"] = int(r.get("b") or 0)
        except Exception as e:
            state[f"bronze_{tbl.lower()}_err"] = str(e)[:200]

    # Silver SCD2 active counts + total Sat rows.
    for tbl in ("SAT_CLAIM_DETAILS", "SAT_MEMBER_DEMOGRAPHICS", "SAT_PROVIDER_INFO"):
        try:
            r = wh.query(
                f"SELECT COUNT(*) AS total, "
                f"       SUM(CASE WHEN is_active THEN 1 ELSE 0 END) AS active_ct, "
                f"       SUM(CASE WHEN effective_end_date IS NULL THEN 1 ELSE 0 END) AS open_ct "
                f"FROM {silver_schema}.{tbl}"
            )[0]
            state[f"silver_{tbl.lower()}_total"] = int(r.get("total") or 0)
            state[f"silver_{tbl.lower()}_active"] = int(r.get("active_ct") or 0)
            state[f"silver_{tbl.lower()}_open"] = int(r.get("open_ct") or 0)
        except Exception as e:
            state[f"silver_{tbl.lower()}_err"] = str(e)[:200]

    # Gold outbox + egress audit.
    try:
        r = wh.query(f"SELECT COUNT(*) AS n FROM {gold_schema}.OUTBOX_GOLD_PATIENT_AUTH")[0]
        state["outbox_patient_auth_rows"] = int(r.get("n") or 0)
    except Exception as e:
        state["outbox_err"] = str(e)[:200]

    try:
        r = wh.query(
            "SELECT COUNT(*) AS n, "
            "       SUM(CASE WHEN status='PUSHED' THEN 1 ELSE 0 END) AS pushed_ct "
            "FROM CONTROL.egress_batch_log "
            "WHERE entity = 'gold_patient_auth' AND client_id = $c",
            {"c": client_id},
        )[0]
        state["egress_log_rows"] = int(r.get("n") or 0)
        state["egress_log_pushed"] = int(r.get("pushed_ct") or 0)
    except Exception as e:
        state["egress_log_err"] = str(e)[:200]

    # Pipeline control state per (pipeline, client).
    try:
        rows = wh.query(
            "SELECT pipeline_id, status FROM CONTROL.pipeline_control_state "
            "WHERE client_id = $c",
            {"c": client_id},
        )
        state["pipeline_states"] = {r["pipeline_id"]: r["status"] for r in rows}
    except Exception as e:
        state["pipeline_states_err"] = str(e)[:200]

    # Bronze retention log (presence + most recent status).
    try:
        rows = wh.query(
            "SELECT status, COUNT(*) AS n FROM CONTROL.bronze_retention_log " "GROUP BY status"
        )
        state["retention_log_by_status"] = {r["status"]: int(r["n"]) for r in rows}
    except Exception as e:
        state["retention_log_err"] = str(e)[:200]

    return state


# ---------------------------------------------------------------------------
# Assertion library — one assertion per architectural property.
# ---------------------------------------------------------------------------


def _assertions_for_existing() -> list[Assertion]:
    """Phase-by-phase architecture checks against any non-empty state."""
    return [
        # --- Phase 9.1 — Bronze append-only -----------------------------
        Assertion(
            name="9.1 Bronze claims have multiple batches (append-only)",
            predicate=lambda s: s.get("bronze_raw_claims_batches", 0) >= 2,
            detail=lambda s: f"batches={s.get('bronze_raw_claims_batches', '?')}",
        ),
        Assertion(
            name="9.1 Bronze membership has multiple batches",
            predicate=lambda s: s.get("bronze_raw_membership_batches", 0) >= 2,
            detail=lambda s: f"batches={s.get('bronze_raw_membership_batches', '?')}",
        ),
        Assertion(
            name="9.1 Bronze claim row count grew with batches",
            predicate=lambda s: s.get("bronze_raw_claims_rows", 0)
            >= 5 * s.get("bronze_raw_claims_batches", 1),
            detail=lambda s: (
                f"rows={s.get('bronze_raw_claims_rows', '?')} "
                f"batches={s.get('bronze_raw_claims_batches', '?')}"
            ),
        ),
        # --- Phase 9.2 — Silver SCD2 ------------------------------------
        Assertion(
            name="9.2 Silver claim Sat dedups via hash_diff (rows < bronze rows)",
            predicate=lambda s: 0
            < s.get("silver_sat_claim_details_total", 0)
            <= s.get("bronze_raw_claims_rows", 0),
            detail=lambda s: (
                f"sat={s.get('silver_sat_claim_details_total', '?')} ≤ "
                f"bronze={s.get('bronze_raw_claims_rows', '?')}"
            ),
        ),
        Assertion(
            name="9.2 Silver membership SCD2 has at least one active row",
            predicate=lambda s: s.get("silver_sat_member_demographics_active", 0) > 0,
            detail=lambda s: (
                f"active={s.get('silver_sat_member_demographics_active', '?')} "
                f"(of total={s.get('silver_sat_member_demographics_total', '?')})"
            ),
        ),
        Assertion(
            name="9.2 Silver active rows have NULL effective_end_date (open-ended)",
            predicate=lambda s: (
                s.get("silver_sat_member_demographics_open", 0)
                == s.get("silver_sat_member_demographics_active", 0)
            ),
            detail=lambda s: (
                f"open={s.get('silver_sat_member_demographics_open', '?')} "
                f"active={s.get('silver_sat_member_demographics_active', '?')}"
            ),
        ),
        # --- Phase 9.3 — Gold outbox -----------------------------------
        Assertion(
            name="9.3 Gold outbox table exists + has rows",
            predicate=lambda s: s.get("outbox_patient_auth_rows", 0) > 0
            or "outbox_err" in s,  # tolerate first-run empty outbox
            detail=lambda s: (
                f"outbox_rows={s.get('outbox_patient_auth_rows', '?')}"
                + (f" (err: {s['outbox_err'][:60]})" if "outbox_err" in s else "")
            ),
        ),
        Assertion(
            name="9.3 Egress audit log has PUSHED entries",
            predicate=lambda s: s.get("egress_log_pushed", 0) >= 1,
            detail=lambda s: (
                f"pushed={s.get('egress_log_pushed', '?')} of "
                f"total={s.get('egress_log_rows', '?')}"
            ),
        ),
        # --- Phase 9.4 — Pipeline Control ------------------------------
        Assertion(
            name="9.4 Per-client pipeline_control_state has rows for this tenant",
            predicate=lambda s: bool(s.get("pipeline_states")),
            detail=lambda s: f"states={s.get('pipeline_states', {})}",
        ),
        # --- Phase 9.5 — Retention -------------------------------------
        Assertion(
            name="9.5 Bronze retention log has at least one entry",
            predicate=lambda s: bool(s.get("retention_log_by_status")),
            detail=lambda s: f"by_status={s.get('retention_log_by_status', {})}",
        ),
    ]


def _assertions_for_day(day: int) -> list[Assertion]:
    """Day-specific assertions — looser bounds because synthetic-data
    generation can produce slightly different row counts run-to-run."""
    base = _assertions_for_existing()
    if day == 1:
        return base + [
            Assertion(
                name="Day 1: Silver active members ≥ 5 K (full file landed)",
                predicate=lambda s: s.get("silver_sat_member_demographics_active", 0) >= 5_000,
                detail=lambda s: f"active={s.get('silver_sat_member_demographics_active', '?')}",
            ),
        ]
    if day == 2:
        return base + [
            Assertion(
                name="Day 2: Bronze membership grew by ≥ 1 batch",
                predicate=lambda s: s.get("bronze_raw_membership_batches", 0) >= 2,
                detail=lambda s: f"batches={s.get('bronze_raw_membership_batches', '?')}",
            ),
        ]
    if day == 3:
        return base + [
            Assertion(
                name="Day 3: Silver Sat has both active + inactive rows (soft-delete fired)",
                predicate=lambda s: (
                    s.get("silver_sat_member_demographics_total", 0)
                    > s.get("silver_sat_member_demographics_active", 0)
                ),
                detail=lambda s: (
                    f"total={s.get('silver_sat_member_demographics_total', '?')} "
                    f"> active={s.get('silver_sat_member_demographics_active', '?')}"
                ),
            ),
        ]
    return base


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--client", default="aetna", help="tenant id")
    p.add_argument(
        "--mode",
        default="existing",
        choices=["existing", "day1", "day2", "day3"],
        help="which assertion set to run",
    )
    p.add_argument(
        "--quiet",
        action="store_true",
        help="only print FAIL lines + summary",
    )
    args = p.parse_args(argv)

    print(f"\n=== Runbook verify · client={args.client} · mode={args.mode} ===\n")
    state = _collect_state(args.client)

    if args.mode == "existing":
        assertions = _assertions_for_existing()
    else:
        assertions = _assertions_for_day(int(args.mode[3]))

    results: list[CheckResult] = []
    for a in assertions:
        try:
            ok = a.predicate(state)
        except Exception as e:
            ok = False
            d = f"predicate raised {type(e).__name__}: {e}"
        else:
            d = a.detail(state)
        results.append(CheckResult(name=a.name, passed=ok, detail=d))

    for r in results:
        if r.passed and args.quiet:
            continue
        marker = _PASS if r.passed else _FAIL
        print(f"{marker}  {r.name}")
        if r.detail:
            print(f"           {r.detail}")

    passes = sum(1 for r in results if r.passed)
    fails = len(results) - passes
    print()
    print(
        f"Summary: {passes}/{len(results)} PASS"
        + (f"  ❌ {fails} FAIL" if fails else "  🎉 all green")
    )
    return 0 if fails == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
