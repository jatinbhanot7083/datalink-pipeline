"""Generate deterministic synthetic healthcare sample data for the Bronze demo.

Produces three CSVs matching the Bronze source-file contracts:
  - claims_sample.csv      (default 10,000 rows)
  - membership_sample.csv  (default  2,000 rows)
  - provider_sample.csv    (default    500 rows)

The data is internally consistent: every claim references a member_id that
exists in the membership file, and a provider_npi that exists in the provider
file. Service dates fall within each member's coverage window.

All names are prefixed FICTIONAL_ so no one mistakes this for real PHI.

Deterministic: same --seed always produces the same data.

Usage:
  python scripts/seed_sample_data.py                     # defaults, writes to data/sample/
  python scripts/seed_sample_data.py --out-dir /tmp/s    # custom out dir
  python scripts/seed_sample_data.py --claims 1000 ...   # smaller dataset
"""

from __future__ import annotations

import argparse
import csv
import random
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

# ---------------------------------------------------------------------------
# Reference data — short, realistic-ish pools.
# ---------------------------------------------------------------------------

CPT_CODES = [
    ("99213", "Office visit, established patient, low complexity"),
    ("99214", "Office visit, established patient, moderate complexity"),
    ("99215", "Office visit, established patient, high complexity"),
    ("99491", "Chronic care management, 30+ min, physician"),
    ("27447", "Total knee arthroplasty"),
    ("73721", "MRI lower extremity joint without contrast"),
    ("80053", "Comprehensive metabolic panel"),
    ("J0178", "Injection, aflibercept, 1 mg"),
]

ICD10_PRIMARY = [
    "E11.9",  # Type 2 diabetes without complications
    "I10",  # Essential hypertension
    "M16.11",  # Osteoarthritis of hip
    "J45.909",  # Asthma, unspecified
    "F32.9",  # Depression, unspecified
    "K21.9",  # GERD
    "N18.3",  # CKD stage 3
    "G47.33",  # Obstructive sleep apnea
]

ICD10_SECONDARY = [*ICD10_PRIMARY, "", ""]  # some claims have no secondary

SPECIALTIES = [
    ("207Q00000X", "Family Medicine"),
    ("207R00000X", "Internal Medicine"),
    ("2080P0203X", "Pediatrics"),
    ("207X00000X", "Orthopaedic Surgery"),
    ("207RC0000X", "Cardiovascular Disease"),
    ("2084P0800X", "Psychiatry"),
    ("261Q00000X", "FQHC Facility"),
    ("282N00000X", "General Acute Care Hospital"),
]

PLANS = [
    "UHCMA-2026",  # UnitedHealthcare Medicare Advantage
    "AETNA-MA-2026",  # Aetna Medicare Advantage
    "BCBS-MA-2026",  # Blue Cross Blue Shield MA
    "CIGNA-MA-2026",  # Cigna MA
    "HUMANA-MA-2026",
]

STATES = ["VA", "MD", "NC", "PA", "DC", "WV", "DE"]

CLAIM_STATUS = ["SUBMITTED", "APPROVED", "DENIED", "PENDED"]

NETWORK_STATUS = ["IN_NETWORK", "OUT_OF_NETWORK", "TIER_1", "TIER_2"]

COVERAGE_TYPE = [
    "MEDICARE_ADVANTAGE",
    "COMMERCIAL",
    "MEDICAID",
    "DUAL_ELIGIBLE",
]

FIRST_NAMES = [
    "ALEX",
    "JORDAN",
    "TAYLOR",
    "MORGAN",
    "CASEY",
    "RILEY",
    "JAMIE",
    "AVERY",
    "CAMERON",
    "DREW",
    "HAYDEN",
    "QUINN",
    "SAGE",
    "SKYLAR",
]
LAST_NAMES = [
    "JOHNSON",
    "WILLIAMS",
    "BROWN",
    "JONES",
    "GARCIA",
    "MILLER",
    "DAVIS",
    "RODRIGUEZ",
    "MARTINEZ",
    "HERNANDEZ",
    "LOPEZ",
    "GONZALEZ",
    "WILSON",
]


# ---------------------------------------------------------------------------
# NPI Luhn validation helpers.
# ---------------------------------------------------------------------------


def _luhn_check_digit(npi_base: str) -> str:
    """Return the check digit for a 9-digit NPI base.

    NPI uses Luhn with an "80840" prefix per CMS spec. For simplicity we use
    the standard Luhn (the reference table loads a "loose" validator at
    Silver). Good enough for synthetic data.
    """
    digits = [int(c) for c in ("80840" + npi_base)]
    total = 0
    for i, d in enumerate(reversed(digits)):
        if i % 2 == 0:
            d *= 2
            if d > 9:
                d -= 9
        total += d
    return str((10 - total % 10) % 10)


def make_npi(rng: random.Random) -> str:
    base = "".join(str(rng.randint(0, 9)) for _ in range(9))
    return base + _luhn_check_digit(base)


# ---------------------------------------------------------------------------
# Generators — each returns a list of dicts matching the CSV schema.
# ---------------------------------------------------------------------------


def gen_providers(n: int, rng: random.Random) -> list[dict]:
    rows: list[dict] = []
    for i in range(n):
        specialty_code, _specialty_name = rng.choice(SPECIALTIES)
        entity_type = rng.choices(["1", "2"], weights=[85, 15])[0]  # mostly individuals
        if entity_type == "1":
            name = f"FICTIONAL_{rng.choice(FIRST_NAMES)}_{rng.choice(LAST_NAMES)}_MD"
        else:
            name = f"FICTIONAL_{rng.choice(LAST_NAMES)}_MEDICAL_CENTER"
        state = rng.choice(STATES)
        rows.append(
            {
                "npi": make_npi(rng),
                "provider_name": name,
                "entity_type": entity_type,
                "specialty_code": specialty_code,
                "tin": f"{rng.randint(10, 99)}-{rng.randint(1000000, 9999999)}",
                "network_status": rng.choice(NETWORK_STATUS),
                "address_line1": f"{rng.randint(100, 9999)} FICTIONAL_MEDICAL_DR",
                "city": f"FICTIONAL_CITY_{i % 40}",
                "state": state,
                "license_state": state,
            }
        )
    return rows


def gen_members(n: int, rng: random.Random) -> list[dict]:
    rows: list[dict] = []
    today = date(2026, 4, 19)
    for i in range(n):
        dob = today - timedelta(days=rng.randint(18 * 365, 85 * 365))
        effective = today - timedelta(days=rng.randint(30, 365))
        # 10% of members are terminated
        if rng.random() < 0.1:
            termination: str | date = effective + timedelta(days=rng.randint(60, 330))
        else:
            termination = ""
        rows.append(
            {
                "member_id": f"MBR-{i:08d}",
                "subscriber_id": f"SUB-{i:08d}",
                "dob": dob.isoformat(),
                "gender": rng.choice(["F", "M"]),
                "plan_id": rng.choice(PLANS),
                "group_id": f"GRP-{rng.randint(10000, 99999)}",
                "effective_date": effective.isoformat(),
                "termination_date": termination.isoformat()
                if isinstance(termination, date)
                else "",
                "coverage_type": rng.choice(COVERAGE_TYPE),
                "state": rng.choice(STATES),
            }
        )
    return rows


def gen_claims(
    n: int,
    members: list[dict],
    providers: list[dict],
    rng: random.Random,
) -> list[dict]:
    rows: list[dict] = []
    today = date(2026, 4, 19)
    for i in range(n):
        member = rng.choice(members)
        provider = rng.choice(providers)
        # Service date: within the past 90 days AND within member's coverage window.
        eff = date.fromisoformat(member["effective_date"])
        term_s = member["termination_date"]
        term = date.fromisoformat(term_s) if term_s else today
        window_start = max(eff, today - timedelta(days=90))
        window_end = min(term, today)
        if window_start >= window_end:
            service_date = window_end
        else:
            days_in_window = (window_end - window_start).days
            service_date = window_start + timedelta(days=rng.randint(0, days_in_window))
        cpt_code, _ = rng.choice(CPT_CODES)
        # Billed amount: $50 to $10,000, skewed toward lower end.
        billed = Decimal(rng.choice([50, 75, 125, 200, 450, 1200, 4250, 9800]))
        rows.append(
            {
                "claim_id": f"CLM-2026-{i:08d}",
                "member_id": member["member_id"],
                "provider_npi": provider["npi"],
                "cpt_code": cpt_code,
                "icd10_primary": rng.choice(ICD10_PRIMARY),
                "icd10_secondary": rng.choice(ICD10_SECONDARY),
                "service_date": service_date.isoformat(),
                "billed_amount": f"{billed:.2f}",
                "claim_status": rng.choice(CLAIM_STATUS),
                "plan_id": member["plan_id"],
                "prior_auth_ref": f"AUTH-{rng.randint(1000000, 9999999)}"
                if rng.random() < 0.4
                else "",
            }
        )
    return rows


# ---------------------------------------------------------------------------
# CSV writer.
# ---------------------------------------------------------------------------


def write_csv(path: Path, rows: list[dict]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


# ---------------------------------------------------------------------------
# CLI.
# ---------------------------------------------------------------------------


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--out-dir", type=Path, default=Path("data/sample"))
    p.add_argument("--seed", type=int, default=20260419)
    p.add_argument("--claims", type=int, default=10_000)
    p.add_argument("--members", type=int, default=2_000)
    p.add_argument("--providers", type=int, default=500)
    args = p.parse_args()

    rng = random.Random(args.seed)

    print(f"generating {args.providers} providers…")
    providers = gen_providers(args.providers, rng)
    print(f"generating {args.members} members…")
    members = gen_members(args.members, rng)
    print(f"generating {args.claims} claims…")
    claims = gen_claims(args.claims, members, providers, rng)

    out = args.out_dir
    write_csv(out / "provider_sample.csv", providers)
    write_csv(out / "membership_sample.csv", members)
    write_csv(out / "claims_sample.csv", claims)

    print()
    print("wrote:")
    print(f"  {out / 'provider_sample.csv'}    ({len(providers):>6} rows)")
    print(f"  {out / 'membership_sample.csv'}  ({len(members):>6} rows)")
    print(f"  {out / 'claims_sample.csv'}      ({len(claims):>6} rows)")


if __name__ == "__main__":
    main()
