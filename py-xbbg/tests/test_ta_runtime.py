from __future__ import annotations

import pyarrow as pa
import pytest

import xbbg.blp as blp


@pytest.mark.asyncio
async def test_abta_combines_single_security_requests(monkeypatch):
    captured: list[dict[str, object]] = []

    class FakeParams:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def to_dict(self):
            return self.kwargs

    class FakeEngine:
        async def request(self, params_dict):
            captured.append(params_dict)
            ticker = params_dict["elements"][0][1]
            return pa.record_batch(
                [
                    pa.array([ticker]),
                    pa.array(["RSI"]),
                    pa.array(["55.5"]),
                ],
                names=["ticker", "field", "value"],
            )

    monkeypatch.setattr(blp, "_get_engine", lambda: FakeEngine())
    monkeypatch.setattr(blp, "_request_params_cls", lambda: FakeParams)
    monkeypatch.setattr(blp, "_convert_backend", lambda df, _backend: df)

    result = await blp.abta(["AAPL US Equity", "MSFT US Equity"], "rsi")

    assert len(captured) == 2
    assert all(item["service"] == blp.Service.TASVC for item in captured)
    assert all(item["operation"] == blp.Operation.STUDY_REQUEST for item in captured)
    assert len(result) == 2
