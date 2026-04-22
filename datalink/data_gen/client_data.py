"""Per-client synthetic data generator — Phase 6.

Produces volume-realistic, deterministic-per-client CSV files matching the
Bronze DDL schemas (RAW_CLAIMS / RAW_MEMBERSHIP / RAW_PROVIDER).

Design
------
* Deterministic: `client_id` seeds the RNG via hashlib(client_id). Re-runs
  for the same client produce BYTE-IDENTICAL files. Re-runs for different
  clients produce different data.
* Volume-accurate: defaults match production-realistic payer volumes:
      Membership:    500_000 rows / client
      Provider:      100_000 rows / client
      Claims:      1_000_000 rows / client
* Referentially consistent within a client: every claim references a
  member_id and provider_npi that EXIST in that client's membership +
  provider files.
* Cross-client uniqueness: member_ids / NPIs / claim_ids are prefixed
  with the client tag, so AETNA's members are disjoint from CARESOURCE's.
* Realistic-enough values: CPT codes + ICD-10 codes pulled from CMS
  synthetic sets; NPIs are valid 10-digit numerics; plan codes map to
  real payer product lines.

Usage
-----
CLI:
    python -m datalink.data_gen.client_data --client aetna
    python -m datalink.data_gen.client_data --all --out data/generated
    python -m datalink.data_gen.client_data --client aetna --claims 100000  # dev

Library:
    from datalink.data_gen.client_data import generate_for_client
    paths = generate_for_client("aetna", Path("data/generated/aetna"))

Output
------
    data/generated/{client}/
        claims_{CLIENT}.csv
        membership_{CLIENT}.csv
        provider_{CLIENT}.csv

The `task_bronze_ingest` orchestration task renames these to
`{source}_{CLIENT}_{YYYYMMDD_HHMMSS}.csv` when uploading to SFTP, so the
audit column `_source_file` captures both client AND upload time without
bloating the generated-file cache.
"""

from __future__ import annotations

import argparse
import hashlib
import sys
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

# ----------------------------------------------------------------------------
# Reference data — small in-memory tables that the random picker draws from.
# ----------------------------------------------------------------------------

# A representative CMS CPT sample covering E/M, surgery, radiology, path/lab,
# medicine. Real payer ops sees ~80% of claims in a narrow CPT band.
CPT_CODES = [
    # E/M — office visits (very high volume)
    "99213",
    "99214",
    "99215",
    "99203",
    "99204",
    "99205",
    # E/M — inpatient
    "99221",
    "99222",
    "99223",
    "99231",
    "99232",
    "99233",
    # Preventive
    "99395",
    "99396",
    "99397",
    "99385",
    "99386",
    "99387",
    # Surgery
    "27447",
    "27130",
    "29881",
    "29827",
    "43239",
    "45378",
    # Radiology
    "70551",
    "71045",
    "72148",
    "73721",
    "74177",
    # Pathology / Lab
    "80053",
    "85025",
    "80061",
    "83036",
    "82947",
    "84443",
    # Medicine / injections
    "90658",
    "90471",
    "90834",
    "90837",
    "96372",
    # Emergency
    "99283",
    "99284",
    "99285",
    # HCPCS (alpha prefix)
    "G0008",
    "G0439",
    "G0202",
    "J1100",
    "J2941",
]

ICD10_PRIMARY = [
    # Diabetes
    "E11.9",
    "E10.9",
    "E11.65",
    "E11.22",
    # Hypertension
    "I10",
    "I11.9",
    "I12.9",
    # Heart disease
    "I25.10",
    "I48.91",
    "I50.9",
    "I25.2",
    # Respiratory
    "J44.9",
    "J45.909",
    "J18.9",
    "J06.9",
    # Renal
    "N18.3",
    "N18.6",
    "N18.9",
    # Mental health
    "F32.9",
    "F41.9",
    "F33.0",
    "F43.10",
    # Musculoskeletal
    "M25.511",
    "M54.5",
    "M17.11",
    "M79.3",
    # Preventive / encounter
    "Z00.00",
    "Z23",
    "Z79.4",
    "Z51.11",
    # Infections
    "B34.9",
    "J22",
    "A09",
    # Pregnancy / women's health
    "O09.00",
    "Z30.09",
    "N91.2",
]

ICD10_SECONDARY = [*ICD10_PRIMARY[:20], ""]  # 20 codes or blank

CLAIM_STATUS = ["SUBMITTED", "APPROVED", "DENIED", "PENDED"]
# Weights: most claims approve; 10% submitted, 5% denied, 5% pended.
CLAIM_STATUS_WEIGHTS = [0.10, 0.80, 0.05, 0.05]

GENDERS = ["F", "M", "U"]
GENDER_WEIGHTS = [0.52, 0.47, 0.01]

COVERAGE_TYPES = ["HMO", "PPO", "DUAL_ELIGIBLE", "MEDICAID", "MEDICARE_ADV", "CHIP"]

US_STATES = [
    "AL",
    "AK",
    "AZ",
    "AR",
    "CA",
    "CO",
    "CT",
    "DE",
    "FL",
    "GA",
    "HI",
    "ID",
    "IL",
    "IN",
    "IA",
    "KS",
    "KY",
    "LA",
    "ME",
    "MD",
    "MA",
    "MI",
    "MN",
    "MS",
    "MO",
    "MT",
    "NE",
    "NV",
    "NH",
    "NJ",
    "NM",
    "NY",
    "NC",
    "ND",
    "OH",
    "OK",
    "OR",
    "PA",
    "RI",
    "SC",
    "SD",
    "TN",
    "TX",
    "UT",
    "VT",
    "VA",
    "WA",
    "WV",
    "WI",
    "WY",
]

NETWORK_STATUS = ["IN_NETWORK", "OUT_OF_NETWORK", "TIER_1", "TIER_2", "TIER_3"]

SPECIALTY_CODES = [
    "207Q00000X",  # Family medicine
    "207R00000X",  # Internal medicine
    "208D00000X",  # Pediatrics
    "207T00000X",  # Surgery
    "207X00000X",  # Orthopaedic surgery
    "2084N0400X",  # Neurology
    "208000000X",  # Pediatrics general
    "2085R0202X",  # Diagnostic radiology
    "282N00000X",  # General acute care hospital
    "261QM0850X",  # Mental health clinic
    "261QU0200X",  # Urgent care clinic
    "208M00000X",  # Hospitalist
    "208600000X",  # Surgery specialty
    "207RC0000X",  # Cardiovascular
    "2083X0100X",  # Geriatric medicine
]

# Entity type: 1 = individual provider, 2 = organization. ~80% individual.
ENTITY_TYPES = ["1", "2"]
ENTITY_WEIGHTS = [0.80, 0.20]

PLAN_ID_TEMPLATES = [
    "{CLIENT}-HMO-{year}",
    "{CLIENT}-PPO-{year}",
    "{CLIENT}-MA-{year}",
    "{CLIENT}-MEDICAID-{year}",
    "{CLIENT}-CHIP-{year}",
    "{CLIENT}-DUAL-{year}",
]

FIRST_NAMES = [
    "JAMES",
    "MARY",
    "JOHN",
    "PATRICIA",
    "ROBERT",
    "JENNIFER",
    "MICHAEL",
    "LINDA",
    "WILLIAM",
    "ELIZABETH",
    "DAVID",
    "BARBARA",
    "RICHARD",
    "SUSAN",
    "JOSEPH",
    "JESSICA",
    "THOMAS",
    "SARAH",
    "CHARLES",
    "KAREN",
    "CHRISTOPHER",
    "NANCY",
    "DANIEL",
    "LISA",
    "MATTHEW",
    "MARGARET",
    "ANTHONY",
    "BETTY",
    "MARK",
    "SANDRA",
]

LAST_NAMES = [
    "SMITH",
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
    "ANDERSON",
    "THOMAS",
    "TAYLOR",
    "MOORE",
    "JACKSON",
    "MARTIN",
    "LEE",
    "PEREZ",
    "THOMPSON",
    "WHITE",
    "HARRIS",
    "SANCHEZ",
    "CLARK",
    "RAMIREZ",
    "LEWIS",
    "ROBINSON",
]

MD_SUFFIXES = ["MD", "DO", "NP", "PA", "RN"]


# ----------------------------------------------------------------------------
# Seeding
# ----------------------------------------------------------------------------


def _seed_for(client_id: str) -> int:
    """Derive a 32-bit RNG seed from the client_id so the data is
    deterministic per client but different across clients."""
    h = hashlib.sha256(client_id.lower().encode()).digest()
    return int.from_bytes(h[:4], "big") % (2**31 - 1)


# ----------------------------------------------------------------------------
# Generators
# ----------------------------------------------------------------------------


def generate_providers(client_id: str, n: int, rng: np.random.Generator) -> pd.DataFrame:
    """Generate n provider rows. NPIs start at 1_000_000_000 and are
    10-digit unique within a client."""
    upper = client_id.upper()
    # NPI base varies per client so AETNA's NPIs are disjoint from CARESOURCE's.
    # Seed offset = first 6 digits of the seed, shifted into the 10-digit NPI space.
    npi_base = 1_000_000_000 + (_seed_for(client_id) % 8_000_000_000)
    npis = [str(npi_base + i) for i in range(n)]

    first_idx = rng.integers(0, len(FIRST_NAMES), n)
    last_idx = rng.integers(0, len(LAST_NAMES), n)
    suffix_idx = rng.integers(0, len(MD_SUFFIXES), n)

    entity_idx = rng.choice(len(ENTITY_TYPES), n, p=ENTITY_WEIGHTS)
    specialty_idx = rng.integers(0, len(SPECIALTY_CODES), n)
    network_idx = rng.integers(0, len(NETWORK_STATUS), n)
    state_idx = rng.integers(0, len(US_STATES), n)

    # TIN: 10-digit tax id (2-digit prefix + 7 digit serial) — loose but
    # plausible.
    tins = [
        f"{rng.integers(10, 99):02d}-{rng.integers(1_000_000, 9_999_999):07d}" for _ in range(n)
    ]

    names = [
        f"FICTIONAL_{FIRST_NAMES[f]}_{LAST_NAMES[ln]}_{MD_SUFFIXES[s]}"
        for f, ln, s in zip(first_idx, last_idx, suffix_idx, strict=True)
    ]
    addresses = [f"{rng.integers(100, 9999)} {upper}_MEDICAL_DR" for _ in range(n)]
    cities = [f"{upper}_CITY_{i % 50}" for i in range(n)]

    return pd.DataFrame(
        {
            "npi": npis,
            "provider_name": names,
            "entity_type": [ENTITY_TYPES[i] for i in entity_idx],
            "specialty_code": [SPECIALTY_CODES[i] for i in specialty_idx],
            "tin": tins,
            "network_status": [NETWORK_STATUS[i] for i in network_idx],
            "address_line1": addresses,
            "city": cities,
            "state": [US_STATES[i] for i in state_idx],
            "license_state": [US_STATES[i] for i in state_idx],
        }
    )


def generate_membership(client_id: str, n: int, rng: np.random.Generator) -> pd.DataFrame:
    """Generate n membership rows with client-prefixed member_ids."""
    upper = client_id.upper()
    member_ids = [f"{upper}-MBR-{i:08d}" for i in range(n)]
    subscriber_ids = [f"{upper}-SUB-{i:08d}" for i in range(n)]

    # Birthdates: uniform across 1940-2020.
    dob_offset_days = rng.integers(0, (date(2020, 12, 31) - date(1940, 1, 1)).days, n)
    dobs = [date(1940, 1, 1) + timedelta(days=int(d)) for d in dob_offset_days]

    gender_idx = rng.choice(len(GENDERS), n, p=GENDER_WEIGHTS)

    # Plans: mix of 6 plan templates across 3 years.
    year_choice = rng.integers(0, 3, n)  # 2024, 2025, 2026
    template_choice = rng.integers(0, len(PLAN_ID_TEMPLATES), n)
    plan_ids = [
        PLAN_ID_TEMPLATES[t].format(CLIENT=upper, year=2024 + int(y))
        for t, y in zip(template_choice, year_choice, strict=True)
    ]

    group_ids = [f"GRP-{rng.integers(10000, 99999):05d}" for _ in range(n)]

    # Effective dates in last 3 years; termination NULL for ~85%, otherwise
    # after effective.
    eff_offset = rng.integers(0, 365 * 3, n)
    eff_dates = [date(2023, 1, 1) + timedelta(days=int(d)) for d in eff_offset]
    term_flip = rng.random(n) < 0.15
    term_span = rng.integers(30, 730, n)  # 1 month to 2 years after effective
    term_dates: list[str | date] = []
    for i in range(n):
        if term_flip[i]:
            term_dates.append(eff_dates[i] + timedelta(days=int(term_span[i])))
        else:
            term_dates.append("")

    coverage_idx = rng.integers(0, len(COVERAGE_TYPES), n)
    state_idx = rng.integers(0, len(US_STATES), n)

    return pd.DataFrame(
        {
            "member_id": member_ids,
            "subscriber_id": subscriber_ids,
            "dob": [d.isoformat() for d in dobs],
            "gender": [GENDERS[i] for i in gender_idx],
            "plan_id": plan_ids,
            "group_id": group_ids,
            "effective_date": [d.isoformat() for d in eff_dates],
            "termination_date": [
                d.isoformat() if hasattr(d, "isoformat") else d for d in term_dates
            ],
            "coverage_type": [COVERAGE_TYPES[i] for i in coverage_idx],
            "state": [US_STATES[i] for i in state_idx],
        }
    )


def generate_claims(
    client_id: str,
    n: int,
    member_ids: np.ndarray,
    plan_ids: np.ndarray,
    provider_npis: np.ndarray,
    rng: np.random.Generator,
) -> pd.DataFrame:
    """Generate n claim rows referencing the given member + provider arrays.

    Referential integrity: every claim's member_id + provider_npi exist in
    the companion membership / provider files. plan_id is drawn from the
    member's plan so claims tie back to an enrollment row.
    """
    upper = client_id.upper()
    claim_ids = [f"{upper}-CLM-{i:010d}" for i in range(n)]

    # Pick member index once per claim — same index for member_id + plan_id
    # so the claim's plan matches that member's enrollment.
    mem_idx = rng.integers(0, len(member_ids), n)
    prov_idx = rng.integers(0, len(provider_npis), n)

    cpt_idx = rng.integers(0, len(CPT_CODES), n)
    icd_idx = rng.integers(0, len(ICD10_PRIMARY), n)
    icd2_idx = rng.integers(0, len(ICD10_SECONDARY), n)

    # Service dates in last 2 years
    svc_offset = rng.integers(0, 365 * 2, n)
    svc_dates = [date(2024, 1, 1) + timedelta(days=int(d)) for d in svc_offset]

    # Billed amounts: log-normal so mode ~200, tail to 50k.
    billed_raw = np.clip(rng.lognormal(mean=5.5, sigma=1.2, size=n), 10, 100_000)
    billed = np.round(billed_raw, 2)

    status_idx = rng.choice(len(CLAIM_STATUS), n, p=CLAIM_STATUS_WEIGHTS)

    # ~40% of claims have prior_auth_ref (Medallion doc §3).
    has_auth = rng.random(n) < 0.40
    auth_refs = [f"PA-{upper}-{i:08d}" if has_auth[i] else "" for i in range(n)]

    return pd.DataFrame(
        {
            "claim_id": claim_ids,
            "member_id": member_ids[mem_idx],
            "provider_npi": provider_npis[prov_idx],
            "cpt_code": [CPT_CODES[i] for i in cpt_idx],
            "icd10_primary": [ICD10_PRIMARY[i] for i in icd_idx],
            "icd10_secondary": [ICD10_SECONDARY[i] for i in icd2_idx],
            "service_date": [d.isoformat() for d in svc_dates],
            "billed_amount": [f"{v:.2f}" for v in billed],
            "claim_status": [CLAIM_STATUS[i] for i in status_idx],
            "plan_id": plan_ids[mem_idx],
            "prior_auth_ref": auth_refs,
        }
    )


# ----------------------------------------------------------------------------
# Orchestration
# ----------------------------------------------------------------------------


def generate_for_client(
    client_id: str,
    output_dir: Path,
    n_claims: int = 1_000_000,
    n_members: int = 500_000,
    n_providers: int = 100_000,
    force: bool = False,
) -> dict[str, Path]:
    """Generate the 3 CSVs for one client. Returns source_type → path dict.

    Idempotent: if the 3 files already exist in `output_dir` and `force=False`,
    skips generation and returns the existing paths.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    upper = client_id.upper()
    paths = {
        "PROVIDER": output_dir / f"provider_{upper}.csv",
        "MEMBERSHIP": output_dir / f"membership_{upper}.csv",
        "CLAIMS": output_dir / f"claims_{upper}.csv",
    }
    if not force and all(p.exists() and p.stat().st_size > 0 for p in paths.values()):
        return paths

    seed = _seed_for(client_id)
    rng = np.random.default_rng(seed)

    # Provider first — claims reference NPIs.
    print(f"[{client_id}] generating {n_providers:,} provider rows ...", file=sys.stderr)
    prov_df = generate_providers(client_id, n_providers, rng)
    prov_df.to_csv(paths["PROVIDER"], index=False)
    print(
        f"[{client_id}]   -> {paths['PROVIDER'].name} ({paths['PROVIDER'].stat().st_size / 1e6:.1f} MB)",
        file=sys.stderr,
    )

    print(f"[{client_id}] generating {n_members:,} membership rows ...", file=sys.stderr)
    mem_df = generate_membership(client_id, n_members, rng)
    mem_df.to_csv(paths["MEMBERSHIP"], index=False)
    print(
        f"[{client_id}]   -> {paths['MEMBERSHIP'].name} ({paths['MEMBERSHIP'].stat().st_size / 1e6:.1f} MB)",
        file=sys.stderr,
    )

    print(f"[{client_id}] generating {n_claims:,} claim rows ...", file=sys.stderr)
    claim_df = generate_claims(
        client_id,
        n_claims,
        member_ids=mem_df["member_id"].to_numpy(),
        plan_ids=mem_df["plan_id"].to_numpy(),
        provider_npis=prov_df["npi"].to_numpy(),
        rng=rng,
    )
    claim_df.to_csv(paths["CLAIMS"], index=False)
    print(
        f"[{client_id}]   -> {paths['CLAIMS'].name} ({paths['CLAIMS'].stat().st_size / 1e6:.1f} MB)",
        file=sys.stderr,
    )

    return paths


ALL_CLIENTS = ("default", "aetna", "caresource", "affinity", "coaccess", "dhmp", "hcsc")


def generate_all_clients(
    output_root: Path,
    n_claims: int = 1_000_000,
    n_members: int = 500_000,
    n_providers: int = 100_000,
    force: bool = False,
) -> dict[str, dict[str, Path]]:
    results: dict[str, dict[str, Path]] = {}
    for client in ALL_CLIENTS:
        results[client] = generate_for_client(
            client,
            output_root / client,
            n_claims=n_claims,
            n_members=n_members,
            n_providers=n_providers,
            force=force,
        )
    return results


# ----------------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="generate_client_data",
        description="Generate deterministic per-client synthetic Bronze CSVs.",
    )
    ap.add_argument("--client", help="Single client id to generate for (e.g. aetna)")
    ap.add_argument("--all", action="store_true", help="Generate for all 7 clients")
    ap.add_argument(
        "--out",
        default="data/generated",
        help="Output root directory (default: data/generated)",
    )
    ap.add_argument("--claims", type=int, default=1_000_000, help="Claim rows per client")
    ap.add_argument("--members", type=int, default=500_000, help="Membership rows per client")
    ap.add_argument("--providers", type=int, default=100_000, help="Provider rows per client")
    ap.add_argument("--force", action="store_true", help="Regenerate even if files exist")
    args = ap.parse_args(argv)

    out_root = Path(args.out)

    if args.all:
        generate_all_clients(
            out_root,
            n_claims=args.claims,
            n_members=args.members,
            n_providers=args.providers,
            force=args.force,
        )
        return 0
    if args.client:
        generate_for_client(
            args.client,
            out_root / args.client,
            n_claims=args.claims,
            n_members=args.members,
            n_providers=args.providers,
            force=args.force,
        )
        return 0
    ap.error("--client <id> or --all is required")
    return 2  # type: ignore[unreachable]  # argparse.error() raises but mypy doesn't know


if __name__ == "__main__":
    sys.exit(main())
