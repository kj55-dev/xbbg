"""Backwards-compatible market resolvers re-export."""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from xbbg.ext.futures import active_cdx, active_futures, cdx_ticker, fut_ticker

    MONTH_CODE_MAP: dict[str, str]

__all__ = [
    "MONTH_CODE_MAP",
    "fut_ticker",
    "active_futures",
    "cdx_ticker",
    "active_cdx",
]

_FUTURES_EXPORTS = {
    "fut_ticker": "fut_ticker",
    "active_futures": "active_futures",
    "cdx_ticker": "cdx_ticker",
    "active_cdx": "active_cdx",
}


def __getattr__(name: str):
    """Lazily resolve exports that need the native extension."""
    if name == "MONTH_CODE_MAP":
        from xbbg import _core

        return _core.ext_get_futures_months()

    attr_name = _FUTURES_EXPORTS.get(name)
    if attr_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    module = importlib.import_module("xbbg.ext.futures")
    return getattr(module, attr_name)


def __dir__() -> list[str]:
    """Expose public exports for tab completion."""
    return list(__all__)
