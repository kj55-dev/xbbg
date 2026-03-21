"""Pure formatting and pivot helpers used by the public Bloomberg API."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import TYPE_CHECKING
import warnings

from xbbg._services_gen import Format

if TYPE_CHECKING:
    import pandas as pd


def _normalize_tickers(tickers: str | Sequence[str]) -> list[str]:
    if isinstance(tickers, str):
        return [tickers]
    return list(tickers)


def _normalize_fields(fields: str | Sequence[str] | None) -> list[str]:
    if fields is None:
        return ["PX_LAST"]
    if isinstance(fields, str):
        return [fields]
    return list(fields)


def _fmt_date(dt: str | None, fmt: str = "%Y%m%d") -> str:
    if dt is None:
        return datetime.now().strftime(fmt)
    if isinstance(dt, str):
        if dt.lower() == "today":
            return datetime.now().strftime(fmt)
        try:
            return datetime.fromisoformat(dt).strftime(fmt)
        except (ValueError, TypeError):
            for parse_fmt in ("%Y-%m-%d", "%Y%m%d", "%Y/%m/%d"):
                try:
                    return datetime.strptime(dt, parse_fmt).strftime(fmt)
                except ValueError:
                    continue
            return dt
    return dt.strftime(fmt)


def _handle_deprecated_wide_format(
    format: Format | str | None,
    pivot_index: str | list[str],
    stacklevel: int = 3,
) -> tuple[Format | None, bool]:
    fmt = Format(format) if isinstance(format, str) else format
    want_wide = fmt == Format.WIDE if fmt else False

    if want_wide:
        index_str = str(pivot_index) if isinstance(pivot_index, list) else f"'{pivot_index}'"
        warnings.warn(
            f"Format.WIDE is deprecated and will be removed in v2.0. "
            f"Use format=Format.LONG (default) and then call "
            f"df.pivot(on='field', index={index_str}, values='value') "
            f"to convert to wide format.",
            DeprecationWarning,
            stacklevel=stacklevel,
        )
        fmt = None

    return fmt, want_wide


def _apply_wide_pivot_bdp(df) -> pd.DataFrame:
    if hasattr(df, "to_pandas"):
        pdf = df.to_pandas()
    else:
        pdf = df

    result = pdf.pivot(index="ticker", columns="field", values="value")
    result.columns.name = None
    return result


def _apply_wide_pivot_bdh(df) -> pd.DataFrame:
    import pandas as pd

    if hasattr(df, "to_pandas"):
        pdf = df.to_pandas()
    else:
        pdf = df

    pivoted = pdf.pivot_table(
        index="date",
        columns=["ticker", "field"],
        values="value",
        aggfunc="first",
    )
    pivoted.index = pd.to_datetime(pivoted.index)
    pivoted.index.name = None
    return pivoted
