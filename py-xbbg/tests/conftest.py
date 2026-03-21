"""Pytest configuration for xbbg tests."""

from __future__ import annotations

import importlib
import os
from pathlib import Path
import sys

import pytest

# Ensure the py-xbbg/src package is in path
pkg_root = os.path.dirname(os.path.dirname(__file__))
python_src = os.path.join(pkg_root, "src")
if python_src not in sys.path:
    sys.path.insert(0, python_src)


def _default_blpapi_root() -> str | None:
    """Find the first vendored Bloomberg SDK root for local test runs."""
    vendor_root = Path(pkg_root).parent / "vendor" / "blpapi-sdk"
    if not vendor_root.is_dir():
        return None

    for candidate in sorted(vendor_root.iterdir(), reverse=True):
        if (candidate / "include").is_dir() and (candidate / "lib").is_dir():
            return str(candidate)
    return None


if "BLPAPI_ROOT" not in os.environ and (detected_root := _default_blpapi_root()):
    os.environ["BLPAPI_ROOT"] = detected_root


def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line(
        "markers",
        "integration: mark test as integration test (requires Bloomberg connection)",
    )
    config.addinivalue_line(
        "markers",
        "slow: mark test as slow running",
    )
    config.addinivalue_line(
        "markers",
        "live: mark test as requiring a live Bloomberg Terminal or B-PIPE connection",
    )


def pytest_collection_modifyitems(config, items):
    """Skip tests that require runtimes not present in the current environment."""
    live_enabled = os.environ.get("XBBG_LIVE_TESTS") == "1" or os.environ.get("XBBG_INTEGRATION_TESTS") == "1"
    native_core_available = _native_core_available()

    skip_live = pytest.mark.skip(
        reason="Live Bloomberg tests require a Bloomberg Terminal/B-PIPE session (set XBBG_LIVE_TESTS=1 to enable)",
    )
    skip_native = pytest.mark.skip(reason="xbbg._core is unavailable in this environment")
    for item in items:
        if "live" in item.keywords and not live_enabled:
            item.add_marker(skip_live)
        if not native_core_available and "test_exceptions.py::TestRustCoreExceptions" in item.nodeid:
            item.add_marker(skip_native)


def _native_core_available() -> bool:
    """Return whether the xbbg native extension imports successfully."""
    try:
        importlib.import_module("xbbg._core")
    except ImportError:
        return False
    return True


@pytest.fixture
def sample_tickers():
    """Fixture providing sample ticker symbols."""
    return ["AAPL US Equity", "MSFT US Equity", "IBM US Equity"]


@pytest.fixture
def sample_fields():
    """Fixture providing sample field names."""
    return ["PX_LAST", "VOLUME", "NAME"]
