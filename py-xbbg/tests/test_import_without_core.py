from __future__ import annotations

import importlib
import sys
from types import SimpleNamespace

import pytest


@pytest.fixture
def pure_python_modules(monkeypatch):
    """Import Python-only modules while simulating a missing native extension."""
    import xbbg

    def _missing_core():
        raise ImportError("synthetic missing native extension")

    monkeypatch.setattr(xbbg, "_core_module", None, raising=False)
    monkeypatch.setattr(xbbg, "_importing_core", False, raising=False)
    monkeypatch.setattr(xbbg, "_import_core", _missing_core)

    for name in ("xbbg._core", "xbbg.exceptions", "xbbg.services", "xbbg.backend"):
        sys.modules.pop(name, None)

    exceptions = importlib.import_module("xbbg.exceptions")
    services = importlib.import_module("xbbg.services")
    backend = importlib.import_module("xbbg.backend")
    yield exceptions, services, backend


def test_backend_import_survives_missing_native_extension(pure_python_modules):
    """Pure-Python backend helpers should not require the Rust extension."""
    _exceptions, _services, backend = pure_python_modules
    assert backend.Backend.PANDAS.value == "pandas"
    assert backend.Backend.NARWHALS_LAZY.value == "narwhals_lazy"


def test_request_validation_works_with_fallback_exceptions(pure_python_modules):
    """RequestParams validation should still raise the public validation type."""
    exceptions, services, _backend = pure_python_modules

    params = services.RequestParams(
        service=services.Service.REFDATA,
        operation=services.Operation.REFERENCE_DATA,
        fields=["PX_LAST"],
    )

    with pytest.raises(exceptions.BlpValidationError, match="securities is required"):
        params.validate()


def test_fallback_exceptions_keep_legacy_metadata_helpers(pure_python_modules):
    """Fallback exception classes should preserve back-compat attributes/helpers."""
    exceptions, _services, _backend = pure_python_modules

    error = exceptions.BlpRequestError(
        "request failed",
        service="//blp/refdata",
        operation="ReferenceDataRequest",
        request_id="req-123",
        code=42,
    )
    assert error.service == "//blp/refdata"
    assert error.operation == "ReferenceDataRequest"
    assert error.request_id == "req-123"
    assert error.code == 42

    parsed = exceptions.BlpValidationError.from_rust_error(
        "Unknown element 'securitiez' (did you mean 'securities'?)",
    )
    assert parsed.element == "securitiez"
    assert parsed.suggestion == "securities"


def test_ext_reexports_lazy_symbols_without_native_extension(monkeypatch):
    """xbbg.ext should stay importable and lazily expose enum-only symbols."""
    import xbbg

    def _missing_core():
        raise ImportError("synthetic missing native extension")

    monkeypatch.setattr(xbbg, "_core_module", None, raising=False)
    monkeypatch.setattr(xbbg, "_importing_core", False, raising=False)
    monkeypatch.setattr(xbbg, "_import_core", _missing_core)

    for name in ("xbbg._core", "xbbg.ext", "xbbg.ext.fixed_income"):
        sys.modules.pop(name, None)

    ext = importlib.import_module("xbbg.ext")
    assert ext.YieldType.YTM == 1


def test_fixed_income_import_stays_pure_until_native_helpers_are_used():
    """Importing enum-only fixed-income helpers should not eagerly load ``xbbg._core``."""
    sys.modules.pop("xbbg._core", None)
    sys.modules.pop("xbbg.ext.fixed_income", None)

    module = importlib.import_module("xbbg.ext.fixed_income")

    assert module.YieldType.YTM == 1
    assert "xbbg._core" not in sys.modules


def test_markets_sessions_import_stays_pure_without_native_extension():
    """SessionWindows should stay importable without touching the native module."""
    sys.modules.pop("xbbg._core", None)
    sys.modules.pop("xbbg.markets.sessions", None)

    module = importlib.import_module("xbbg.markets.sessions")
    windows = module.SessionWindows(day=("09:30", "16:00"), pre=("04:00", "09:30"))

    assert windows.to_dict() == {"day": ("09:30", "16:00"), "pre": ("04:00", "09:30")}
    assert "xbbg._core" not in sys.modules


def test_fallback_exception_identity_survives_reload(pure_python_modules):
    """Reloading pure-Python modules should preserve public exception identity."""
    exceptions, services, _backend = pure_python_modules

    for name in ("xbbg.exceptions", "xbbg.services"):
        sys.modules.pop(name, None)

    reloaded_exceptions = importlib.import_module("xbbg.exceptions")
    reloaded_services = importlib.import_module("xbbg.services")

    assert reloaded_exceptions.BlpValidationError is exceptions.BlpValidationError
    assert reloaded_services.RequestParams is not services.RequestParams

    params = services.RequestParams(
        service=services.Service.REFDATA,
        operation=services.Operation.RAW_REQUEST,
    )
    with pytest.raises(reloaded_exceptions.BlpValidationError):
        params.validate()


def test_rebinding_core_exceptions_reapplies_python_compatibility(pure_python_modules):
    """Synthetic core rebinding should preserve Python-side compatibility helpers."""
    exceptions, _services, _backend = pure_python_modules

    class FakeBlpError(Exception):
        pass

    class FakeBlpSessionError(FakeBlpError):
        pass

    class FakeBlpRequestError(FakeBlpError):
        pass

    class FakeBlpSecurityError(FakeBlpRequestError):
        pass

    class FakeBlpFieldError(FakeBlpRequestError):
        pass

    class FakeBlpValidationError(FakeBlpError):
        pass

    class FakeBlpTimeoutError(FakeBlpError):
        pass

    class FakeBlpInternalError(FakeBlpError):
        pass

    fake_core = SimpleNamespace(
        BlpError=FakeBlpError,
        BlpSessionError=FakeBlpSessionError,
        BlpRequestError=FakeBlpRequestError,
        BlpSecurityError=FakeBlpSecurityError,
        BlpFieldError=FakeBlpFieldError,
        BlpValidationError=FakeBlpValidationError,
        BlpTimeoutError=FakeBlpTimeoutError,
        BlpInternalError=FakeBlpInternalError,
    )

    exceptions._bind_core_exceptions(fake_core)

    error = exceptions.BlpRequestError(
        "request failed",
        service="//blp/refdata",
        operation="ReferenceDataRequest",
        request_id="req-123",
        code=42,
    )
    parsed = exceptions.BlpValidationError.from_rust_error(
        "Unknown element 'securitiez' (did you mean 'securities'?)",
    )

    assert isinstance(error, FakeBlpRequestError)
    assert error.service == "//blp/refdata"
    assert error.operation == "ReferenceDataRequest"
    assert error.request_id == "req-123"
    assert error.code == 42
    assert parsed.element == "securitiez"
    assert parsed.suggestion == "securities"
    assert issubclass(exceptions.BlpBPipeError, FakeBlpError)
