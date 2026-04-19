"""Pydantic row-schema models for Bronze tables.

Used to validate CSV rows before they hit the warehouse. Conservative type
coercion only (stripping whitespace, normalizing dates) — anything that
looks semantically wrong is REJECTED, not auto-corrected.

The 4 metadata columns (_load_dt, _source_file, _batch_id, _record_source)
are NOT in these schemas — they're appended by the ingest layer, not the
source file.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field


class ClaimRow(BaseModel):
    """One row of RAW_CLAIMS — 11 source fields."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    claim_id: str = Field(min_length=1)
    member_id: str = Field(min_length=1)
    provider_npi: str
    cpt_code: str
    icd10_primary: str
    icd10_secondary: str | None = None
    service_date: date
    billed_amount: Decimal
    claim_status: str
    plan_id: str
    prior_auth_ref: str | None = None


class MembershipRow(BaseModel):
    """One row of RAW_MEMBERSHIP — 10 source fields."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    member_id: str = Field(min_length=1)
    subscriber_id: str
    dob: date
    gender: str = Field(min_length=1, max_length=1)
    plan_id: str = Field(min_length=1)
    group_id: str
    effective_date: date
    termination_date: date | None = None
    coverage_type: str
    state: str = Field(min_length=2, max_length=2)


class ProviderRow(BaseModel):
    """One row of RAW_PROVIDER — 10 source fields."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    npi: str = Field(min_length=10, max_length=10)
    provider_name: str
    entity_type: str = Field(pattern="^[12]$")  # 1=Individual, 2=Organization
    specialty_code: str
    tin: str
    network_status: str
    address_line1: str
    city: str
    state: str = Field(min_length=2, max_length=2)
    license_state: str = Field(min_length=2, max_length=2)
