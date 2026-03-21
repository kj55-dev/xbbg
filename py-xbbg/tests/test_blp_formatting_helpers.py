from __future__ import annotations

import pandas as pd
import pytest

from xbbg._services_gen import Format
from xbbg.blp import (
    _apply_wide_pivot_bdh,
    _apply_wide_pivot_bdp,
    _fmt_date,
    _handle_deprecated_wide_format,
    _normalize_fields,
    _normalize_tickers,
)


def test_normalize_tickers_accepts_str_and_sequence():
    assert _normalize_tickers("AAPL US Equity") == ["AAPL US Equity"]
    assert _normalize_tickers(["AAPL US Equity", "MSFT US Equity"]) == [
        "AAPL US Equity",
        "MSFT US Equity",
    ]


def test_normalize_fields_defaults_to_px_last():
    assert _normalize_fields(None) == ["PX_LAST"]
    assert _normalize_fields("PX_OPEN") == ["PX_OPEN"]


def test_fmt_date_handles_common_input_formats():
    assert _fmt_date("2024-01-15") == "20240115"
    assert _fmt_date("20240115") == "20240115"
    assert _fmt_date("2024/01/15") == "20240115"
    assert _fmt_date("not-a-date") == "not-a-date"


def test_handle_deprecated_wide_format_warns_and_requests_post_pivot():
    with pytest.warns(DeprecationWarning):
        fmt, want_wide = _handle_deprecated_wide_format(Format.WIDE, "ticker", stacklevel=1)

    assert fmt is None
    assert want_wide is True


def test_apply_wide_pivot_bdp_shapes_dataframe():
    df = pd.DataFrame(
        {
            "ticker": ["AAPL US Equity", "AAPL US Equity"],
            "field": ["PX_LAST", "PX_OPEN"],
            "value": [150.0, 148.0],
        }
    )

    result = _apply_wide_pivot_bdp(df)

    assert set(result.columns) == {"PX_LAST", "PX_OPEN"}
    assert result.index.tolist() == ["AAPL US Equity"]


def test_apply_wide_pivot_bdh_builds_datetime_index_and_multi_columns():
    df = pd.DataFrame(
        {
            "ticker": ["AAPL US Equity", "AAPL US Equity"],
            "date": ["2024-01-01", "2024-01-02"],
            "field": ["PX_LAST", "PX_LAST"],
            "value": [150.0, 151.0],
        }
    )

    result = _apply_wide_pivot_bdh(df)

    assert result.index.name is None
    assert str(result.index.dtype).startswith("datetime64")
    assert ("AAPL US Equity", "PX_LAST") in result.columns
