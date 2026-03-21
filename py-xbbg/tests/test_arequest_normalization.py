"""Tests for arequest normalization and request-plumbing helpers."""

from __future__ import annotations

import pyarrow as pa
import pytest

from xbbg.services import Operation, Service


def _sample_batch() -> pa.RecordBatch:
    return pa.record_batch(
        [
            pa.array(["IBM US Equity"]),
            pa.array(["PX_LAST"]),
            pa.array(["123.45"]),
        ],
        names=["ticker", "field", "value"],
    )


@pytest.mark.asyncio
async def test_arequest_routes_bql_overrides_to_elements(monkeypatch):
    """BQL-style requests should pass overrides as generic request elements."""
    from xbbg import blp

    captured: dict[str, object] = {}

    class FakeEngine:
        async def request(self, params_dict):
            captured.update(params_dict)
            return _sample_batch()

    monkeypatch.setattr(blp, "_get_engine", lambda: FakeEngine())

    result = await blp.arequest(
        service=Service.BQLSVC,
        operation=Operation.BQL_SEND_QUERY,
        elements=[("showTotals", True)],
        overrides={"group": "General"},
    )

    assert captured["elements"] == [("showTotals", "true"), ("group", "General")]
    assert "overrides" not in captured
    assert len(result) == 1


@pytest.mark.asyncio
async def test_arequest_keeps_refdata_overrides_separate(monkeypatch):
    """Reference-data requests should keep elements and overrides on their native lanes."""
    from xbbg import blp

    captured: dict[str, object] = {}

    class FakeEngine:
        async def request(self, params_dict):
            captured.update(params_dict)
            return _sample_batch()

    monkeypatch.setattr(blp, "_get_engine", lambda: FakeEngine())

    result = await blp.arequest(
        service=Service.REFDATA,
        operation=Operation.REFERENCE_DATA,
        securities=["IBM US Equity"],
        fields=["PX_LAST"],
        elements=[("returnEids", True)],
        overrides={"EQY_FUND_CRNCY": "USD"},
    )

    assert captured["elements"] == [("returnEids", "true")]
    assert captured["overrides"] == [("EQY_FUND_CRNCY", "USD")]
    assert len(result) == 1
