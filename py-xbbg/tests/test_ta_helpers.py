from __future__ import annotations

from xbbg.blp import _build_study_request, _get_study_attr_name, ta_studies, ta_study_params


def test_get_study_attr_name_supports_aliases():
    assert _get_study_attr_name("sma") == "smavgStudyAttributes"
    assert _get_study_attr_name("fear greed") == "fgStudyAttributes"
    assert _get_study_attr_name("custom") == "customStudyAttributes"


def test_build_study_request_uses_defaults_and_normalizes_dates():
    result = _build_study_request(
        "AAPL US Equity",
        "rsi",
        start_date="2024-01-01",
        end_date="2024/01/31",
    )

    assert ("priceSource.securityName", "AAPL US Equity") in result
    assert ("priceSource.dataRange.historical.startDate", "20240101") in result
    assert ("priceSource.dataRange.historical.endDate", "20240131") in result
    assert ("studyAttributes.rsiStudyAttributes.period", "14") in result


def test_build_study_request_intraday_uses_trade_event_and_interval():
    result = _build_study_request(
        "AAPL US Equity",
        "macd",
        periodicity="INTRADAY",
        interval=30,
    )

    assert ("priceSource.dataRange.intraday.eventType", "TRADE") in result
    assert ("priceSource.dataRange.intraday.interval", "30") in result


def test_ta_studies_and_params_are_public_helpers():
    studies = ta_studies()
    assert "rsi" in studies
    assert ta_study_params("boll")["period"] == 20
