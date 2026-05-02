"""Generate a 100-record pipe-delimited membership sample file.

Used as a hands-on input for the Data Contract Architect demo.

The column headers are intentionally messy / non-FHIR-aligned (e.g.,
``MbrID`` instead of ``member_id``, ``BrthDt`` instead of ``birthDate``)
so the AI agent has interesting RENAME deviations to flag against
the FHIR R4 anchor.

Run from inside the control_tower container:

    docker exec datalink-control-tower python3 \\
        /opt/datalink/scripts/generate_membership_sample.py

Output: ``/opt/datalink/data/generated/membership_sample_100.psv``
        (bind-mounted to host at ``data/generated/``)

Synthetic data — no PHI. Names + addresses are random combinations
of common US first/last names and city placeholders. All phone
numbers use the 555 prefix (reserved for fictional use).
"""

from __future__ import annotations

import os
import random
from datetime import date, timedelta
from pathlib import Path

random.seed(42)  # deterministic output

# Intentionally non-FHIR column names — gives the agent RENAME work.
HEADERS = [
    "MbrID",
    "SubsID",
    "RelType",
    "FrstNm",
    "MdlNm",
    "LstNm",
    "BrthDt",
    "Gnr",
    "AddrL1",
    "AddrL2",
    "CityNm",
    "StAbr",
    "ZpCd",
    "HmPh",
    "EmlAdr",
    "EligStrt",
    "EligEnd",
    "GrpID",
    "PlnID",
    "CovLine",
    "CovLvl",
    "PCPNPI",
    "MStat",
    "RaceCd",
    "PrLng",
]

FIRST_NAMES = [
    "John",
    "Mary",
    "Robert",
    "Patricia",
    "James",
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
    "Christopher",
    "Nancy",
    "Daniel",
    "Lisa",
    "Matthew",
    "Margaret",
    "Anthony",
    "Betty",
    "Mark",
    "Sandra",
    "Donald",
    "Ashley",
    "Steven",
    "Kimberly",
    "Paul",
    "Emily",
    "Andrew",
    "Donna",
    "Joshua",
    "Michelle",
    "Kenneth",
    "Carol",
    "Kevin",
    "Amanda",
    "Brian",
    "Melissa",
    "George",
    "Deborah",
    "Edward",
    "Stephanie",
]
MIDDLE_NAMES = [
    "",
    "A",
    "B",
    "C",
    "J",
    "L",
    "M",
    "R",
    "T",
    "William",
    "Marie",
    "Anne",
    "James",
    "Lee",
    "Rose",
    "",
    "",
    "",
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
    "Lee",
    "Perez",
    "Thompson",
    "White",
    "Harris",
    "Sanchez",
    "Clark",
    "Ramirez",
    "Lewis",
    "Robinson",
    "Walker",
    "Young",
    "Allen",
    "King",
    "Wright",
    "Scott",
    "Torres",
    "Nguyen",
    "Hill",
    "Flores",
    "Green",
    "Adams",
    "Nelson",
    "Baker",
    "Hall",
    "Rivera",
    "Campbell",
    "Mitchell",
    "Carter",
    "Roberts",
]

# (city, state_abbr, zip)
CITIES = [
    ("Pittsburgh", "PA", "15201"),
    ("Philadelphia", "PA", "19103"),
    ("New York", "NY", "10001"),
    ("Brooklyn", "NY", "11201"),
    ("Buffalo", "NY", "14201"),
    ("Chicago", "IL", "60601"),
    ("Houston", "TX", "77001"),
    ("Dallas", "TX", "75201"),
    ("Austin", "TX", "73301"),
    ("Los Angeles", "CA", "90001"),
    ("San Diego", "CA", "92101"),
    ("San Francisco", "CA", "94102"),
    ("Phoenix", "AZ", "85001"),
    ("Tucson", "AZ", "85701"),
    ("Denver", "CO", "80201"),
    ("Seattle", "WA", "98101"),
    ("Portland", "OR", "97201"),
    ("Boston", "MA", "02101"),
    ("Atlanta", "GA", "30301"),
    ("Miami", "FL", "33101"),
    ("Orlando", "FL", "32801"),
    ("Tampa", "FL", "33601"),
    ("Cleveland", "OH", "44101"),
    ("Columbus", "OH", "43201"),
    ("Detroit", "MI", "48201"),
    ("Minneapolis", "MN", "55401"),
    ("St. Louis", "MO", "63101"),
    ("Kansas City", "MO", "64101"),
    ("Charlotte", "NC", "28201"),
    ("Raleigh", "NC", "27601"),
]
STREETS = [
    "Main St",
    "Oak Ave",
    "Maple Dr",
    "Cedar Ln",
    "Pine St",
    "Elm Rd",
    "Park Blvd",
    "Washington St",
    "Lincoln Ave",
    "Jefferson Dr",
    "Madison Ave",
    "Adams St",
    "Highland Ave",
    "Sunset Blvd",
    "Riverside Dr",
    "Hillcrest Ave",
]
APT_TYPES = ["Apt", "Unit", "Ste", "#", ""]

PLAN_IDS = [
    "HMO-GOLD-2026",
    "PPO-PLAT-2026",
    "HMO-SLVR-2026",
    "PPO-GOLD-2026",
    "HDHP-BRZ-2026",
    "HMO-CHOICE-2026",
]
GROUP_IDS = [
    "EMP-AETNA-001",
    "EMP-AETNA-002",
    "EMP-AETNA-FED",
    "EMP-AETNA-COM",
    "EMP-AETNA-MED",
]
COV_LINES = ["MED", "DEN", "VIS", "RX"]
COV_LEVELS = ["EE", "EE+SP", "EE+CH", "FAM", "EE+1"]
GENDERS = ["M", "F", "M", "F", "M", "F", "M", "F", "U"]
REL_TYPES = ["S", "S", "S", "S", "P", "C", "C", "C", "O"]
M_STATS = ["M", "S", "D", "W", "U", "U"]
RACE_CODES = ["2106-3", "2054-5", "2028-9", "2076-8", "2131-1", "UNK"]
LANGUAGES = ["en", "en", "en", "es", "en", "zh", "vi", "ko", "en"]


def random_dob() -> date:
    days = random.randint(365, 365 * 90)
    return date.today() - timedelta(days=days)


def random_eligibility_dates() -> tuple[str, str]:
    start_year = random.choice([2024, 2025, 2026])
    start = date(start_year, random.randint(1, 12), random.randint(1, 28))
    if random.random() < 0.7:
        end = date(start.year + 1, 12, 31)
        end_str = end.strftime("%Y%m%d")
    else:
        end_str = ""
    return start.strftime("%Y%m%d"), end_str


def gen_npi() -> str:
    return str(random.randint(1000000000, 1999999999))


def main() -> None:
    rows: list[str] = ["|".join(HEADERS)]
    for i in range(1, 101):
        rel = random.choice(REL_TYPES)
        mbr_id = f"MBR{i:06d}"
        subs_id = mbr_id if rel == "S" or i == 1 else f"MBR{random.randint(1, max(1, i - 1)):06d}"
        first = random.choice(FIRST_NAMES)
        middle = random.choice(MIDDLE_NAMES)
        last = random.choice(LAST_NAMES)
        dob = random_dob()
        gender = random.choice(GENDERS)
        city, st_abr, zp = random.choice(CITIES)
        addr1 = f"{random.randint(100, 9999)} {random.choice(STREETS)}"
        if random.random() < 0.3:
            apt = random.choice(APT_TYPES)
            addr2 = f"{apt} {random.randint(1, 999)}" if apt else ""
        else:
            addr2 = ""
        phone = f"{random.randint(200, 999)}-555-{random.randint(1000, 9999)}"
        email = f"{first.lower()}.{last.lower()}@example.org" if random.random() < 0.85 else ""
        elig_start, elig_end = random_eligibility_dates()
        grp = random.choice(GROUP_IDS)
        pln = random.choice(PLAN_IDS)
        cov_line = random.choice(COV_LINES)
        cov_lvl = random.choice(COV_LEVELS)
        pcp_npi = gen_npi() if random.random() < 0.85 else ""
        m_stat = random.choice(M_STATS)
        race = random.choice(RACE_CODES)
        lang = random.choice(LANGUAGES)

        rows.append(
            "|".join(
                [
                    mbr_id,
                    subs_id,
                    rel,
                    first,
                    middle,
                    last,
                    dob.strftime("%Y%m%d"),
                    gender,
                    addr1,
                    addr2,
                    city,
                    st_abr,
                    zp,
                    phone,
                    email,
                    elig_start,
                    elig_end,
                    grp,
                    pln,
                    cov_line,
                    cov_lvl,
                    pcp_npi,
                    m_stat,
                    race,
                    lang,
                ]
            )
        )

    out = Path("/opt/datalink/data/generated/membership_sample_100.psv")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(rows) + "\n", encoding="utf-8")

    # chown to host UID so the operator can copy / git stage it
    import contextlib

    with contextlib.suppress(PermissionError, OSError):
        os.chown(str(out), 1000, 1000)

    print(f"Wrote {out}")
    print(f"  Bytes : {out.stat().st_size}")
    print(f"  Rows  : {len(rows) - 1} data + 1 header")
    print(f"  Cols  : {len(HEADERS)}")
    print()
    print("First 3 rows:")
    for r in rows[:4]:
        truncated = r[:130] + ("..." if len(r) > 130 else "")
        print(f"  {truncated}")


if __name__ == "__main__":
    main()
