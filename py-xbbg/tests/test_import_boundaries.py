"""Regression tests for pure-Python import boundaries.

These tests guard against eager ``xbbg._core`` imports leaking into modules
that should remain usable for offline validation and enum access.
"""

from __future__ import annotations

import importlib
import sys

import pytest


def _clear_xbbg_modules() -> None:
    for name in list(sys.modules):
        if name == "xbbg" or name.startswith("xbbg."):
            sys.modules.pop(name, None)


def test_pure_python_modules_import_without_loading_native_core():
    """Pure-Python entry points should not import ``xbbg._core`` eagerly."""
    _clear_xbbg_modules()

    services = importlib.import_module("xbbg.services")
    backend = importlib.import_module("xbbg.backend")
    fixed_income = importlib.import_module("xbbg.ext.fixed_income")
    markets = importlib.import_module("xbbg.markets")

    assert "xbbg._core" not in sys.modules
    assert backend.Backend.PANDAS == "pandas"
    assert fixed_income.YieldType.YTM == 1
    assert services.RequestParams is not None
    assert "get_exchange_override" in dir(markets)
    assert "xbbg._core" not in sys.modules


def test_request_params_validate_with_python_fallback_error():
    """Request validation should work even when the native extension is absent."""
    _clear_xbbg_modules()

    services = importlib.import_module("xbbg.services")
    exceptions = importlib.import_module("xbbg.exceptions")

    params = services.RequestParams(
        service=services.Service.REFDATA,
        operation=services.Operation.REFERENCE_DATA,
    )

    with pytest.raises(exceptions.BlpValidationError, match="securities is required"):
        params.validate()
