from __future__ import annotations

from types import SimpleNamespace

import pytest

from xbbg import blp
from xbbg.services import Operation, Service


def _elem(name: str, *, children: list[SimpleNamespace] | None = None) -> SimpleNamespace:
    return SimpleNamespace(
        name=name,
        description=f"{name} description",
        data_type="String",
        type_name="String",
        is_array=False,
        is_optional=False,
        enum_values=[],
        children=children or [],
    )


@pytest.mark.asyncio
async def test_abops_uses_schema_operation_listing(monkeypatch):
    async def fake_list(service_uri: str) -> list[str]:
        assert service_uri == Service.REFDATA.value
        return ["ReferenceDataRequest", "HistoricalDataRequest"]

    monkeypatch.setattr("xbbg.schema.alist_operations", fake_list)

    ops = await blp.abops()

    assert ops == ["ReferenceDataRequest", "HistoricalDataRequest"]


@pytest.mark.asyncio
async def test_abschema_returns_operation_shape(monkeypatch):
    async def fake_get_operation(service_uri: str, operation_name: str) -> SimpleNamespace:
        assert service_uri == Service.REFDATA.value
        assert operation_name == Operation.REFERENCE_DATA.value
        return SimpleNamespace(
            name="ReferenceDataRequest",
            description="Reference data",
            request=_elem("request", children=[_elem("securities")]),
            responses=[_elem("response")],
        )

    monkeypatch.setattr("xbbg.schema.aget_operation", fake_get_operation)

    schema = await blp.abschema(operation=Operation.REFERENCE_DATA)

    assert schema["name"] == "ReferenceDataRequest"
    assert schema["request"]["children"][0]["name"] == "securities"
    assert schema["responses"][0]["name"] == "response"


@pytest.mark.asyncio
async def test_abschema_returns_service_shape(monkeypatch):
    async def fake_get_schema(service_uri: str) -> SimpleNamespace:
        assert service_uri == Service.REFDATA.value
        return SimpleNamespace(
            service=service_uri,
            description="Reference service",
            operations=[
                SimpleNamespace(
                    name="ReferenceDataRequest",
                    description="Reference data",
                    request=_elem("request"),
                    responses=[_elem("response")],
                )
            ],
            cached_at="2026-03-20T00:00:00Z",
        )

    monkeypatch.setattr("xbbg.schema.aget_schema", fake_get_schema)

    schema = await blp.abschema()

    assert schema["service"] == Service.REFDATA.value
    assert schema["operations"][0]["name"] == "ReferenceDataRequest"
    assert schema["cached_at"] == "2026-03-20T00:00:00Z"
