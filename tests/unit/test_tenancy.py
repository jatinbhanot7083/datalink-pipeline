"""Tests for datalink.tenancy — multi-tenant schema routing (Phase 6)."""

from __future__ import annotations

import pytest

from datalink.tenancy import (
    DEFAULT_CLIENT,
    Layer,
    ensure_tenant_schemas,
    normalize_client_id,
    schema_for,
    schemas_for_client,
)

# ---------- normalize_client_id ----------


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("default", "default"),
        ("acme", "acme"),
        ("ACME", "acme"),
        ("acme_health", "acme_health"),
        ("  ACME_Health  ", "acme_health"),
        (None, DEFAULT_CLIENT),
        ("", DEFAULT_CLIENT),
        ("   ", DEFAULT_CLIENT),
    ],
)
def test_normalize_client_id_happy_path(raw: str | None, expected: str) -> None:
    assert normalize_client_id(raw) == expected


@pytest.mark.parametrize(
    "bad",
    [
        "1acme",  # must start with letter
        "acme-health",  # dashes rejected
        "acme health",  # spaces rejected
        "acme.health",  # dots rejected
        "a" * 64,  # too long
        "_acme",  # leading underscore rejected
        "acme;DROP TABLE X",  # sql injection attempt
    ],
)
def test_normalize_client_id_rejects_invalid(bad: str) -> None:
    with pytest.raises(ValueError, match="invalid client_id"):
        normalize_client_id(bad)


# ---------- schema_for ----------


def test_schema_for_default_client_no_prefix() -> None:
    """Default client uses Phase-5.x unsuffixed schema names (backward compat)."""
    assert schema_for("default", Layer.BRONZE) == "BRONZE"
    assert schema_for("default", Layer.SILVER_DV) == "SILVER_silver"
    assert schema_for("default", Layer.GOLD_UM) == "SILVER_gold_um"


def test_schema_for_tenant_client_suffixed() -> None:
    """Non-default clients append _UPPER_CLIENT to the layer base."""
    assert schema_for("acme_health", Layer.BRONZE) == "BRONZE_ACME_HEALTH"
    assert schema_for("acme_health", Layer.SILVER_DV) == "SILVER_silver_ACME_HEALTH"
    assert schema_for("acme_health", Layer.GOLD_UM) == "SILVER_gold_um_ACME_HEALTH"


def test_schema_for_control_never_suffixed() -> None:
    """CONTROL is cross-tenant metadata — never gets a client suffix."""
    assert schema_for("default", Layer.CONTROL) == "CONTROL"
    assert schema_for("acme_health", Layer.CONTROL) == "CONTROL"
    assert schema_for("any_tenant", Layer.CONTROL) == "CONTROL"


def test_schema_for_normalizes_case() -> None:
    """Upper-case input is lower-cased for the normalized client id but
    the emitted schema suffix is upper-case (SQL identifier convention)."""
    assert schema_for("ACME", Layer.BRONZE) == "BRONZE_ACME"
    assert schema_for("Acme_Health", Layer.BRONZE) == "BRONZE_ACME_HEALTH"


def test_schema_for_empty_client_falls_back_to_default() -> None:
    assert schema_for("", Layer.BRONZE) == "BRONZE"


# ---------- schemas_for_client ----------


def test_schemas_for_client_returns_all_three_layers() -> None:
    triple = schemas_for_client("acme")
    assert set(triple) == {Layer.BRONZE, Layer.SILVER_DV, Layer.GOLD_UM}
    assert triple[Layer.BRONZE] == "BRONZE_ACME"
    assert triple[Layer.SILVER_DV] == "SILVER_silver_ACME"
    assert triple[Layer.GOLD_UM] == "SILVER_gold_um_ACME"


def test_schemas_for_client_excludes_control() -> None:
    """CONTROL is cross-tenant — NEVER in a client's owned schema set."""
    assert Layer.CONTROL not in schemas_for_client("acme")
    assert Layer.CONTROL not in schemas_for_client("default")


# ---------- ensure_tenant_schemas ----------


class _FakeWarehouse:
    """Minimal fake — records create-schema calls for assertion."""

    def __init__(self) -> None:
        self.created: list[str] = []

    def create_schema_if_not_exists(self, schema: str) -> None:
        self.created.append(schema)

    def execute(self, sql: str) -> None:
        self.created.append(sql)


def test_ensure_tenant_schemas_creates_three_for_default() -> None:
    wh = _FakeWarehouse()
    created = ensure_tenant_schemas(wh, "default")
    assert set(created) == {"BRONZE", "SILVER_silver", "SILVER_gold_um"}
    assert set(wh.created) == {"BRONZE", "SILVER_silver", "SILVER_gold_um"}


def test_ensure_tenant_schemas_creates_three_for_tenant() -> None:
    wh = _FakeWarehouse()
    created = ensure_tenant_schemas(wh, "acme_health")
    expected = {"BRONZE_ACME_HEALTH", "SILVER_silver_ACME_HEALTH", "SILVER_gold_um_ACME_HEALTH"}
    assert set(created) == expected
    assert set(wh.created) == expected


def test_ensure_tenant_schemas_uses_sql_fallback_when_no_helper() -> None:
    """Adapters without create_schema_if_not_exists must still work."""

    class _PlainWarehouse:
        def __init__(self) -> None:
            self.calls: list[str] = []

        def execute(self, sql: str) -> None:
            self.calls.append(sql)

    wh = _PlainWarehouse()
    ensure_tenant_schemas(wh, "default")
    for stmt in wh.calls:
        assert stmt.startswith("CREATE SCHEMA IF NOT EXISTS ")
