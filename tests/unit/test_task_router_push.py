"""Tests for datalink.orchestration.tasks.task_router_push — schema-suffix bug.

Regression: without the client-aware source_schema, multi-tenant runs
silently pushed 0 rows to SQL Server / Postgres because the router
defaulted to `SILVER_gold_um` but dbt writes to `SILVER_gold_um_{CLIENT}`.
"""

from __future__ import annotations

from typing import Any, ClassVar
from unittest.mock import patch

import pytest


class _FakeResult:
    """Minimal PushResult shape that task_router_push consumes."""

    all_green: bool = True
    targets_requested: ClassVar[list[str]] = []
    targets_skipped: ClassVar[list[str]] = []
    per_target: ClassVar[list[Any]] = []


def _make_ctx(client_id: str) -> Any:
    """TaskContext stub — only fields task_router_push touches."""
    from datalink.orchestration.tasks import TaskContext

    return TaskContext(pipeline_id="gold_um_push", run_id="test", client_id=client_id)


@pytest.mark.unit
def test_router_push_passes_client_suffixed_schema_for_non_default() -> None:
    from datalink.orchestration.tasks import task_router_push

    captured: dict[str, Any] = {}

    def _fake_push(adapters: Any, settings: Any, source_schema: str) -> _FakeResult:
        captured["source_schema"] = source_schema
        return _FakeResult()

    with patch("datalink.pipeline.router.push_gold_um_to_operational", side_effect=_fake_push):
        result = task_router_push(_make_ctx("aetna"))

    assert captured["source_schema"] == "SILVER_gold_um_AETNA"
    assert result["client_id"] == "aetna"
    assert result["source_schema"] == "SILVER_gold_um_AETNA"


@pytest.mark.unit
def test_router_push_uses_bare_schema_for_default_client() -> None:
    from datalink.orchestration.tasks import task_router_push

    captured: dict[str, Any] = {}

    def _fake_push(adapters: Any, settings: Any, source_schema: str) -> _FakeResult:
        captured["source_schema"] = source_schema
        return _FakeResult()

    with patch("datalink.pipeline.router.push_gold_um_to_operational", side_effect=_fake_push):
        task_router_push(_make_ctx("default"))

    assert captured["source_schema"] == "SILVER_gold_um"


@pytest.mark.unit
def test_router_push_uppercases_mixed_case_client() -> None:
    from datalink.orchestration.tasks import task_router_push

    captured: dict[str, Any] = {}

    def _fake_push(adapters: Any, settings: Any, source_schema: str) -> _FakeResult:
        captured["source_schema"] = source_schema
        return _FakeResult()

    with patch("datalink.pipeline.router.push_gold_um_to_operational", side_effect=_fake_push):
        task_router_push(_make_ctx("CareSource"))

    assert captured["source_schema"] == "SILVER_gold_um_CARESOURCE"
