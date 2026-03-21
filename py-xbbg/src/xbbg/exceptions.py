"""Bloomberg API exceptions.

Expose a stable Python exception hierarchy even when the native extension
isn't currently importable. When ``xbbg._core`` becomes available, this module
re-binds its public exception names to the canonical Rust classes.
"""

from __future__ import annotations

import sys
from types import ModuleType


class _FallbackBlpError(Exception):
    """Fallback base exception used before the native module is available."""


class _FallbackBlpSessionError(_FallbackBlpError):
    """Fallback session error."""


class _FallbackBlpRequestError(_FallbackBlpError):
    """Fallback request error."""


class _FallbackBlpSecurityError(_FallbackBlpRequestError):
    """Fallback security error."""


class _FallbackBlpFieldError(_FallbackBlpRequestError):
    """Fallback field error."""


class _FallbackBlpValidationError(_FallbackBlpError):
    """Fallback validation error."""


class _FallbackBlpTimeoutError(_FallbackBlpError):
    """Fallback timeout error."""


class _FallbackBlpInternalError(_FallbackBlpError):
    """Fallback internal error."""


def _get_fallback_exception_types() -> dict[str, type[Exception]]:
    """Return a stable fallback hierarchy reused across module reloads."""
    package = sys.modules.get("xbbg")
    cached = getattr(package, "_fallback_exception_types", None) if package is not None else None
    if cached is not None:
        return cached

    fallback = {
        "BlpError": _FallbackBlpError,
        "BlpSessionError": _FallbackBlpSessionError,
        "BlpRequestError": _FallbackBlpRequestError,
        "BlpSecurityError": _FallbackBlpSecurityError,
        "BlpFieldError": _FallbackBlpFieldError,
        "BlpValidationError": _FallbackBlpValidationError,
        "BlpTimeoutError": _FallbackBlpTimeoutError,
        "BlpInternalError": _FallbackBlpInternalError,
    }
    if package is not None:
        package._fallback_exception_types = fallback
    return fallback


_FALLBACK_EXCEPTION_TYPES = _get_fallback_exception_types()

BlpError = _FALLBACK_EXCEPTION_TYPES["BlpError"]
BlpSessionError = _FALLBACK_EXCEPTION_TYPES["BlpSessionError"]
BlpRequestError = _FALLBACK_EXCEPTION_TYPES["BlpRequestError"]
BlpSecurityError = _FALLBACK_EXCEPTION_TYPES["BlpSecurityError"]
BlpFieldError = _FALLBACK_EXCEPTION_TYPES["BlpFieldError"]
BlpValidationError = _FALLBACK_EXCEPTION_TYPES["BlpValidationError"]
BlpTimeoutError = _FALLBACK_EXCEPTION_TYPES["BlpTimeoutError"]
BlpInternalError = _FALLBACK_EXCEPTION_TYPES["BlpInternalError"]


def _init_request_error(
    self,
    message: str,
    *,
    service: str | None = None,
    operation: str | None = None,
    request_id: str | None = None,
    code: int | None = None,
) -> None:
    """Back-compat init for request-derived errors with context attributes."""
    Exception.__init__(self, message)
    self.service = service
    self.operation = operation
    self.request_id = request_id
    self.code = code


def _init_validation_error(
    self,
    message: str,
    *,
    element: str | None = None,
    suggestion: str | None = None,
    valid_values: list[str] | None = None,
) -> None:
    """Back-compat init for validation errors with parsed metadata."""
    Exception.__init__(self, message)
    self.element = element
    self.suggestion = suggestion
    self.valid_values = valid_values


def _parse_validation_error(message: str) -> tuple[str | None, str | None]:
    """Extract ``(element, suggestion)`` from Rust validation error text."""
    suggestion = None
    if "(did you mean '" in message:
        start = message.find("(did you mean '") + len("(did you mean '")
        end = message.find("'?)", start)
        if end > start:
            suggestion = message[start:end]

    element = None
    if "Unknown element '" in message:
        start = message.find("Unknown element '") + len("Unknown element '")
        end = message.find("'", start)
        if end > start:
            element = message[start:end]
    elif "Invalid enum value" in message and "for '" in message:
        start = message.find("for '") + len("for '")
        end = message.find("'", start)
        if end > start:
            element = message[start:end]

    return element, suggestion


def _from_rust_error(cls, message: str):
    """Back-compat helper for constructing ``BlpValidationError`` from text."""
    element, suggestion = _parse_validation_error(message)
    err = cls(message)
    if element is not None:
        err.element = element
    if suggestion is not None:
        err.suggestion = suggestion
    return err


def _make_bpipe_error(base_cls: type[Exception]) -> type[Exception]:
    class _BlpBPipeError(base_cls):
        """B-PIPE license required for this operation.

        Raised when attempting to use features that require Bloomberg B-PIPE
        license but only a standard Terminal connection is available.

        B-PIPE features include:
            - Level 2 market depth data (depth/adepth)
            - Option and futures chains (chains/achains)
        """

    _BlpBPipeError.__name__ = "BlpBPipeError"
    _BlpBPipeError.__qualname__ = "BlpBPipeError"
    _BlpBPipeError.__module__ = __name__
    return _BlpBPipeError


def _bind_core_exceptions(core_module: ModuleType) -> None:
    """Rebind public exception names to the canonical Rust exception classes."""
    global BlpError, BlpSessionError, BlpRequestError, BlpSecurityError
    global BlpFieldError, BlpValidationError, BlpTimeoutError, BlpInternalError
    global BlpBPipeError

    BlpError = core_module.BlpError
    BlpSessionError = core_module.BlpSessionError
    BlpRequestError = core_module.BlpRequestError
    BlpSecurityError = core_module.BlpSecurityError
    BlpFieldError = core_module.BlpFieldError
    BlpValidationError = core_module.BlpValidationError
    BlpTimeoutError = core_module.BlpTimeoutError
    BlpInternalError = core_module.BlpInternalError

    BlpRequestError.__init__ = _init_request_error
    BlpSecurityError.__init__ = _init_request_error
    BlpFieldError.__init__ = _init_request_error
    BlpValidationError.__init__ = _init_validation_error
    BlpValidationError.from_rust_error = classmethod(_from_rust_error)

    BlpBPipeError = _make_bpipe_error(BlpError)


BlpRequestError.__init__ = _init_request_error
BlpSecurityError.__init__ = _init_request_error
BlpFieldError.__init__ = _init_request_error
BlpValidationError.__init__ = _init_validation_error
BlpBPipeError = _make_bpipe_error(BlpError)
BlpValidationError.from_rust_error = classmethod(_from_rust_error)

try:
    from . import _core as _initial_core
except ImportError:
    _initial_core = None

if _initial_core is not None:
    _bind_core_exceptions(_initial_core)


__all__ = [
    "BlpError",
    "BlpSessionError",
    "BlpRequestError",
    "BlpSecurityError",
    "BlpFieldError",
    "BlpValidationError",
    "BlpTimeoutError",
    "BlpInternalError",
    "BlpBPipeError",
]
