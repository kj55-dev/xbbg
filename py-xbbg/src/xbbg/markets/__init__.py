"""Market information utilities.

This package re-exports market helpers lazily so pure-Python helpers and
simple dataclasses do not force native market-session imports at package load.
"""

from __future__ import annotations

import importlib

__all__ = [
    "SessionWindows",
    "derive_sessions",
    "get_session_windows",
    "exch_info",
    "market_info",
    "market_timing",
    "ccy_pair",
    "convert_session_times_to_utc",
    "set_exchange_override",
    "get_exchange_override",
    "clear_exchange_override",
    "list_exchange_overrides",
    "has_override",
    "ExchangeInfo",
    "fetch_exchange_info",
    "afetch_exchange_info",
]

_ATTR_TO_MODULE = {
    "ExchangeInfo": "bloomberg",
    "afetch_exchange_info": "bloomberg",
    "fetch_exchange_info": "bloomberg",
    "ccy_pair": "info",
    "convert_session_times_to_utc": "info",
    "exch_info": "info",
    "market_info": "info",
    "market_timing": "info",
    "clear_exchange_override": "overrides",
    "get_exchange_override": "overrides",
    "has_override": "overrides",
    "list_exchange_overrides": "overrides",
    "set_exchange_override": "overrides",
    "SessionWindows": "sessions",
    "derive_sessions": "sessions",
    "get_session_windows": "sessions",
}


def __getattr__(name: str):
    """Resolve market exports lazily to avoid import-time native coupling."""
    module_name = _ATTR_TO_MODULE.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    module = importlib.import_module(f"xbbg.markets.{module_name}")
    return getattr(module, name)


def __dir__() -> list[str]:
    """Expose public exports for tab completion."""
    return list(__all__)
