from __future__ import annotations

import importlib
import sys

import pytest


@pytest.fixture
def missing_native_extension(monkeypatch):
    """Import market helpers while simulating a missing native extension."""
    import xbbg

    def _missing_core():
        raise ImportError("synthetic missing native extension")

    monkeypatch.setattr(xbbg, "_core_module", None, raising=False)
    monkeypatch.setattr(xbbg, "_importing_core", False, raising=False)
    monkeypatch.setattr(xbbg, "_import_core", _missing_core)

    for name in tuple(sys.modules):
        if name == "xbbg._core" or name.startswith("xbbg.markets"):
            sys.modules.pop(name, None)

    yield


def test_markets_package_import_is_lazy_without_native_extension(missing_native_extension):
    """Importing xbbg.markets should not eagerly pull in native-dependent modules."""
    markets = importlib.import_module("xbbg.markets")

    assert "xbbg.markets.sessions" not in sys.modules
    assert "xbbg.markets.resolvers" not in sys.modules

    assert markets.ExchangeInfo.__name__ == "ExchangeInfo"
    assert callable(markets.set_exchange_override)

    assert markets.SessionWindows.__name__ == "SessionWindows"
    assert "xbbg.markets.sessions" in sys.modules


def test_markets_sessions_import_survives_missing_native_extension(missing_native_extension):
    """The SessionWindows dataclass should stay usable without the Rust extension."""
    sessions = importlib.import_module("xbbg.markets.sessions")

    windows = sessions.SessionWindows(
        day=("09:30", "16:00"),
        post=("16:00", "20:00"),
    )

    assert windows.to_dict() == {
        "day": ("09:30", "16:00"),
        "post": ("16:00", "20:00"),
    }


def test_markets_resolvers_module_import_is_lazy_without_native_extension(missing_native_extension):
    """Importing the resolvers module should defer native futures helpers until access time."""
    resolvers = importlib.import_module("xbbg.markets.resolvers")

    assert "xbbg.ext.futures" not in sys.modules
    assert resolvers.__all__ == [
        "MONTH_CODE_MAP",
        "fut_ticker",
        "active_futures",
        "cdx_ticker",
        "active_cdx",
    ]
