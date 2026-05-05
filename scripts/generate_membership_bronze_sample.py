"""Generate a 100-row synthetic Membership PSV for the Aetna demo.

Columns match BRONZE_AETNA.RAW_MEMBERSHIP exactly (41 business cols + 7 audit
cols are NOT in the file — those are populated by the COPY INTO logic).

Output: data/generated/membership_bronze_sample.psv

Usage:
    python scripts/generate_membership_bronze_sample.py [--rows N]
"""

from __future__ import annotations

import argparse
import csv
import os
import random
from datetime import date, timedelta
from pathlib import Path

random.seed(42)  # deterministic demo data

ROOT = Path(__file__).resolve().parents[1]

# Auto-load .env (no Snowflake calls here, but keeps the demo scripts uniform)
_ENV = ROOT / ".env"
if _ENV.exists():
    for raw in _ENV.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        os.environ.setdefault(k.strip(), v.strip().strip("'").strip('"'))

# Exact column order from BRONZE_AETNA.RAW_MEMBERSHIP DDL (41 business cols).
HEADERS = [
    "payer_name",
    "member_card_id",
    "member_medicare_id",
    "member_medicaid_id",
    "member_first_name",
    "member_middle_name",
    "member_last_name",
    "member_gender",
    "member_birth_date",
    "member_phone",
    "member_street_address_line1",
    "member_street_address_line2",
    "member_county_code",
    "member_county_name",
    "member_city",
    "member_state",
    "member_zip_code",
    "member_active_status",
    "member_relationship_code",
    "member_medicaid_indicator",
    "member_medicaid_dual_eligibility_indicator",
    "member_dual_eligibility_begin_date",
    "member_dual_eligibility_end_date",
    "member_coverage_effective_date",
    "member_coverage_end_date",
    "member_provider_pcp_begin_date",
    "member_health_plan_begin_date",
    "member_enrollment_date",
    "member_line_of_business",
    "member_product",
    "product_description",
    "plan_benenfit_package_id",
    "plan_benefit_package_name",
    "segment_id_plan_benefit_package",
    "member_product_begin_date",
    "managed_care_organization_contract_number",
    "attributed_provider_id",
    "atrributed_provider_npi",
    "attributed_provider_contract_id",
    "plan_benefit_package_id",
    "refresh_date",
]

FIRST_NAMES = [
    "John",
    "Mary",
    "James",
    "Patricia",
    "Robert",
    "Jennifer",
    "Michael",
    "Linda",
    "William",
    "Elizabeth",
    "David",
    "Barbara",
    "Richard",
    "Susan",
    "Joseph",
    "Jessica",
    "Thomas",
    "Sarah",
    "Charles",
    "Karen",
]
MIDDLE_INITIALS = [
    "A",
    "B",
    "C",
    "D",
    "E",
    "F",
    "G",
    "H",
    "J",
    "K",
    "L",
    "M",
    "N",
    "P",
    "R",
    "S",
    "T",
]
LAST_NAMES = [
    "Smith",
    "Johnson",
    "Williams",
    "Brown",
    "Jones",
    "Garcia",
    "Miller",
    "Davis",
    "Rodriguez",
    "Martinez",
    "Hernandez",
    "Lopez",
    "Gonzalez",
    "Wilson",
    "Anderson",
    "Thomas",
    "Taylor",
    "Moore",
    "Jackson",
    "Martin",
]
GENDERS = ["M", "F"]
CITIES = [
    ("Hartford", "CT"),
    ("Boston", "MA"),
    ("Buffalo", "NY"),
    ("Newark", "NJ"),
    ("Philadelphia", "PA"),
    ("Cleveland", "OH"),
    ("Detroit", "MI"),
    ("Chicago", "IL"),
    ("Houston", "TX"),
    ("Phoenix", "AZ"),
]
LINES_OF_BUSINESS = ["MEDICARE", "MEDICAID", "COMMERCIAL", "DUAL"]
PRODUCTS = [
    ("HMO_PRIME", "Aetna Prime HMO"),
    ("PPO_GOLD", "Aetna Gold PPO"),
    ("POS_VALUE", "Aetna Value POS"),
    ("DSNP_CARE", "Aetna Dual Care DSNP"),
    ("HMO_BASIC", "Aetna Basic HMO"),
]
RELATIONSHIPS = ["18", "01", "19", "34"]  # 18=self, 01=spouse, 19=child, 34=other
ACTIVE_STATUS = ["ACTIVE", "TERMED", "ACTIVE", "ACTIVE"]  # weighted toward active


def random_date(start_year: int, end_year: int) -> date:
    start = date(start_year, 1, 1)
    end = date(end_year, 12, 31)
    return start + timedelta(days=random.randint(0, (end - start).days))


def generate_row(i: int) -> list[str]:
    first = random.choice(FIRST_NAMES)
    middle = random.choice(MIDDLE_INITIALS)
    last = random.choice(LAST_NAMES)
    city, state = random.choice(CITIES)
    lob = random.choice(LINES_OF_BUSINESS)
    product_code, product_name = random.choice(PRODUCTS)
    is_medicaid = lob in ("MEDICAID", "DUAL")
    is_dual = lob == "DUAL"
    birth = random_date(1940, 2010)
    coverage_start = random_date(2024, 2026)
    coverage_end = coverage_start + timedelta(days=random.choice([90, 180, 365, 730]))
    pcp_start = coverage_start + timedelta(days=random.randint(0, 30))
    plan_start = coverage_start
    enrollment = coverage_start - timedelta(days=random.randint(0, 60))
    refresh = date(2026, 5, 1)
    pbp_id = random.randint(1000, 9999)
    pbp_name = f"{product_name} - PBP {pbp_id}"

    return [
        "AETNA",  # payer_name
        f"AET{1000000 + i:08d}",  # member_card_id
        f"{random.randint(100000000, 999999999)}A"
        if lob in ("MEDICARE", "DUAL")
        else "",  # medicare_id
        f"M{random.randint(100000000, 999999999)}" if is_medicaid else "",  # medicaid_id
        first,  # first_name
        middle,  # middle_name
        last,  # last_name
        random.choice(GENDERS),  # gender
        birth.isoformat(),  # birth_date
        f"555-{random.randint(100, 999)}-{random.randint(1000, 9999)}",  # phone
        f"{random.randint(100, 9999)} {random.choice(['Main', 'Oak', 'Elm', 'Pine', 'Maple'])} {random.choice(['St', 'Ave', 'Blvd', 'Rd'])}",  # addr1
        random.choice(
            ["", f"Apt {random.randint(1, 500)}", f"Unit {random.choice(['A', 'B', 'C'])}"]
        ),  # addr2
        f"{random.randint(1, 99):03d}" if random.random() < 0.7 else "",  # county_code
        random.choice([f"{city} County", ""]) if random.random() < 0.7 else "",  # county_name
        city,
        state,
        f"{random.randint(10000, 99999)}",  # zip
        random.choice(ACTIVE_STATUS),  # active_status
        random.choice(RELATIONSHIPS),  # relationship_code
        "true" if is_medicaid else "false",  # medicaid_indicator
        "true" if is_dual else "false",  # dual_eligibility_indicator
        coverage_start.isoformat() if is_dual else "1900-01-01",  # dual_begin_date
        coverage_end.isoformat() if is_dual else "1900-01-01",  # dual_end_date
        coverage_start.isoformat(),  # coverage_effective_date
        coverage_end.isoformat(),  # coverage_end_date
        pcp_start.isoformat(),  # provider_pcp_begin_date
        plan_start.isoformat(),  # health_plan_begin_date
        enrollment.isoformat() if random.random() < 0.8 else "",  # enrollment_date
        lob,  # line_of_business
        product_code,  # product
        product_name,  # product_description
        str(pbp_id),  # plan_benenfit_package_id (typo in catalog!)
        pbp_name,  # plan_benefit_package_name
        str(random.randint(1, 50)) if random.random() < 0.6 else "",  # segment_id
        coverage_start.isoformat(),  # member_product_begin_date
        f"MCO-{random.randint(10000, 99999)}",  # mco_contract_number
        f"PRV{random.randint(100000, 999999)}",  # attributed_provider_id
        f"{random.randint(1000000000, 9999999999)}",  # atrributed_provider_npi (typo!)
        f"{random.uniform(50.0, 5000.0):.4f}",  # contract_id (decimal)
        f"PBP-{pbp_id}",  # plan_benefit_package_id (alphanum)
        refresh.isoformat(),  # refresh_date
    ]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=100, help="Number of rows (default 100)")
    parser.add_argument(
        "--out",
        type=Path,
        default=ROOT / "data" / "generated" / "membership_bronze_sample.psv",
        help="Output path",
    )
    args = parser.parse_args()

    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter="|", quoting=csv.QUOTE_MINIMAL)
        w.writerow(HEADERS)
        for i in range(args.rows):
            w.writerow(generate_row(i))

    print(f"Wrote {args.rows} rows → {args.out}")
    print(f"Columns: {len(HEADERS)} (matches BRONZE_AETNA.RAW_MEMBERSHIP business columns)")
    print(f"Size: {args.out.stat().st_size:,} bytes")


if __name__ == "__main__":
    main()
