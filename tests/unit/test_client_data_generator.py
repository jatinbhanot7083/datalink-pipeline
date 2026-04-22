"""Tests for scripts.generate_client_data — Phase 6 per-client data generator.

Validates:
  * Determinism: same client produces byte-identical output across runs
  * Cross-client uniqueness: AETNA's IDs never collide with CARESOURCE's
  * Schema correctness: CSV columns match the Bronze DDL exactly
  * Volume knobs: --claims / --members / --providers honored
  * Referential integrity: every claim's member_id + npi exist in the
    companion membership + provider files
  * Idempotency: second generate_for_client() call skips work
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import pandas as pd
import pytest

from datalink.data_gen.client_data import (
    ALL_CLIENTS,
    generate_for_client,
)

# Use small volumes for tests so they run in <5s.
_N_CLAIMS = 500
_N_MEMBERS = 200
_N_PROVIDERS = 50


@pytest.fixture
def tiny_aetna(tmp_path: Path) -> dict[str, Path]:
    return generate_for_client(
        "aetna",
        tmp_path / "aetna",
        n_claims=_N_CLAIMS,
        n_members=_N_MEMBERS,
        n_providers=_N_PROVIDERS,
    )


@pytest.fixture
def tiny_caresource(tmp_path: Path) -> dict[str, Path]:
    return generate_for_client(
        "caresource",
        tmp_path / "caresource",
        n_claims=_N_CLAIMS,
        n_members=_N_MEMBERS,
        n_providers=_N_PROVIDERS,
    )


# ---------- volume ----------


@pytest.mark.unit
def test_generator_produces_exact_row_counts(tiny_aetna: dict[str, Path]) -> None:
    assert len(pd.read_csv(tiny_aetna["CLAIMS"])) == _N_CLAIMS
    assert len(pd.read_csv(tiny_aetna["MEMBERSHIP"])) == _N_MEMBERS
    assert len(pd.read_csv(tiny_aetna["PROVIDER"])) == _N_PROVIDERS


# ---------- schema ----------


_CLAIMS_COLS = [
    "claim_id",
    "member_id",
    "provider_npi",
    "cpt_code",
    "icd10_primary",
    "icd10_secondary",
    "service_date",
    "billed_amount",
    "claim_status",
    "plan_id",
    "prior_auth_ref",
]
_MEMBERSHIP_COLS = [
    "member_id",
    "subscriber_id",
    "dob",
    "gender",
    "plan_id",
    "group_id",
    "effective_date",
    "termination_date",
    "coverage_type",
    "state",
]
_PROVIDER_COLS = [
    "npi",
    "provider_name",
    "entity_type",
    "specialty_code",
    "tin",
    "network_status",
    "address_line1",
    "city",
    "state",
    "license_state",
]


@pytest.mark.unit
def test_claim_columns_match_bronze_ddl(tiny_aetna: dict[str, Path]) -> None:
    df = pd.read_csv(tiny_aetna["CLAIMS"])
    assert list(df.columns) == _CLAIMS_COLS


@pytest.mark.unit
def test_membership_columns_match_bronze_ddl(tiny_aetna: dict[str, Path]) -> None:
    df = pd.read_csv(tiny_aetna["MEMBERSHIP"])
    assert list(df.columns) == _MEMBERSHIP_COLS


@pytest.mark.unit
def test_provider_columns_match_bronze_ddl(tiny_aetna: dict[str, Path]) -> None:
    df = pd.read_csv(tiny_aetna["PROVIDER"])
    assert list(df.columns) == _PROVIDER_COLS


# ---------- determinism ----------


@pytest.mark.unit
def test_same_client_same_seed_same_bytes(tmp_path: Path) -> None:
    """Regenerating for the same client must produce identical hashes."""
    p1 = generate_for_client(
        "aetna",
        tmp_path / "run1" / "aetna",
        n_claims=_N_CLAIMS,
        n_members=_N_MEMBERS,
        n_providers=_N_PROVIDERS,
    )
    p2 = generate_for_client(
        "aetna",
        tmp_path / "run2" / "aetna",
        n_claims=_N_CLAIMS,
        n_members=_N_MEMBERS,
        n_providers=_N_PROVIDERS,
    )
    for key in ("CLAIMS", "MEMBERSHIP", "PROVIDER"):
        h1 = hashlib.sha256(p1[key].read_bytes()).hexdigest()
        h2 = hashlib.sha256(p2[key].read_bytes()).hexdigest()
        assert h1 == h2, f"{key}: client 'aetna' not deterministic across runs"


# ---------- cross-client uniqueness ----------


@pytest.mark.unit
def test_members_are_disjoint_across_clients(
    tiny_aetna: dict[str, Path], tiny_caresource: dict[str, Path]
) -> None:
    a = set(pd.read_csv(tiny_aetna["MEMBERSHIP"])["member_id"])
    c = set(pd.read_csv(tiny_caresource["MEMBERSHIP"])["member_id"])
    assert not (a & c), "member_ids must be disjoint across clients"


@pytest.mark.unit
def test_npis_are_disjoint_across_clients(
    tiny_aetna: dict[str, Path], tiny_caresource: dict[str, Path]
) -> None:
    a = set(pd.read_csv(tiny_aetna["PROVIDER"])["npi"].astype(str))
    c = set(pd.read_csv(tiny_caresource["PROVIDER"])["npi"].astype(str))
    assert not (a & c), "NPIs must be disjoint across clients"


@pytest.mark.unit
def test_claim_ids_carry_client_prefix(tiny_aetna: dict[str, Path]) -> None:
    df = pd.read_csv(tiny_aetna["CLAIMS"])
    assert df["claim_id"].str.startswith("AETNA-CLM-").all()


# ---------- referential integrity within a client ----------


@pytest.mark.unit
def test_every_claim_member_id_exists_in_membership(tiny_aetna: dict[str, Path]) -> None:
    claims = pd.read_csv(tiny_aetna["CLAIMS"])
    members = pd.read_csv(tiny_aetna["MEMBERSHIP"])
    orphan = set(claims["member_id"]) - set(members["member_id"])
    assert not orphan, f"{len(orphan)} claims reference non-existent members"


@pytest.mark.unit
def test_every_claim_npi_exists_in_provider(tiny_aetna: dict[str, Path]) -> None:
    claims = pd.read_csv(tiny_aetna["CLAIMS"])
    providers = pd.read_csv(tiny_aetna["PROVIDER"])
    orphan = set(claims["provider_npi"].astype(str)) - set(providers["npi"].astype(str))
    assert not orphan, f"{len(orphan)} claims reference non-existent providers"


# ---------- idempotency ----------


@pytest.mark.unit
def test_generator_skips_on_reinvoke_without_force(tmp_path: Path) -> None:
    """Second call with files already present must not regenerate."""
    paths = generate_for_client(
        "aetna",
        tmp_path / "aetna",
        n_claims=_N_CLAIMS,
        n_members=_N_MEMBERS,
        n_providers=_N_PROVIDERS,
    )
    mtimes_before = {k: v.stat().st_mtime_ns for k, v in paths.items()}

    # Second call with force=False — must NOT touch the files.
    generate_for_client(
        "aetna",
        tmp_path / "aetna",
        n_claims=_N_CLAIMS,
        n_members=_N_MEMBERS,
        n_providers=_N_PROVIDERS,
    )
    mtimes_after = {k: v.stat().st_mtime_ns for k, v in paths.items()}
    assert mtimes_before == mtimes_after, "idempotent call must not rewrite existing files"


# ---------- all-clients catalogue ----------


@pytest.mark.unit
def test_all_7_clients_enumerated() -> None:
    """Production payer list must match baseline_seeder's REAL_CLIENTS set."""
    from datalink.quality.baseline_seeder import REAL_CLIENTS

    assert set(ALL_CLIENTS) == {"default", *REAL_CLIENTS}
