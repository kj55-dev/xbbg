"""Bloomberg API exceptions.

Expose a stable Python exception hierarchy even when the native extension
isn't currently importable. When ``xbbg._core`` becomes available, this module
re-binds its public exception names to the canonical Rust classes.
"""

from __future__ import annotations

import sys
from types import ModuleType
from typing import Any, cast


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
    if isinstance(cached, dict):
        return cast("dict[str, type[Exception]]", cached)

    fallback: dict[str, type[Exception]] = {
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
        cast("Any", package)._fallback_exception_types = fallback
    return fallback


_FALLBACK_EXCEPTION_TYPES = _get_fallback_exception_types()

BlpError: type[Exception] = _FALLBACK_EXCEPTION_TYPES["BlpError"]
BlpSessionError: type[Exception] = _FALLBACK_EXCEPTION_TYPES["BlpSessionError"]
BlpRequestError: type[Exception] = _FALLBACK_EXCEPTION_TYPES["BlpRequestError"]
BlpSecurityError: type[Exception] = _FALLBACK_EXCEPTION_TYPES["BlpSecurityError"]
BlpFieldError: type[Exception] = _FALLBACK_EXCEPTION_TYPES["BlpFieldError"]
BlpValidationError: type[Exception] = _FALLBACK_EXCEPTION_TYPES["BlpValidationError"]
BlpTimeoutError: type[Exception] = _FALLBACK_EXCEPTION_TYPES["BlpTimeoutError"]
BlpInternalError: type[Exception] = _FALLBACK_EXCEPTION_TYPES["BlpInternalError"]
BlpBPipeError: type[Exception]


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


def _from_rust_error(cls: type[Exception], message: str) -> Exception:
    """Back-compat helper for constructing ``BlpValidationError`` from text."""
    element, suggestion = _parse_validation_error(message)
    err = cast("Any", cls(message))
    if element is not None:
        err.element = element
    if suggestion is not None:
        err.suggestion = suggestion
    return err


def _make_bpipe_error(base_cls: type[Exception]) -> type[Exception]:
    return cast(
        "type[Exception]",
        type(
            "BlpBPipeError",
            (cast("Any", base_cls),),
            {
                "__doc__": (
                    "B-PIPE license required for this operation.\n\n"
                    "Raised when attempting to use features that require Bloomberg "
                    "B-PIPE license but only a standard Terminal connection is available.\n\n"
                    "B-PIPE features include:\n"
                    "    - Level 2 market depth data (depth/adepth)\n"
                    "    - Option and futures chains (chains/achains)"
                ),
                "__module__": __name__,
                "__qualname__": "BlpBPipeError",
            },
        ),
    )


def _set_request_error_init(exc_type: type[Exception]) -> None:
    cast("Any", exc_type).__init__ = _init_request_error


def _set_validation_error_compatibility(exc_type: type[Exception]) -> None:
    compat_exc = cast("Any", exc_type)
    compat_exc.__init__ = _init_validation_error
    compat_exc.from_rust_error = classmethod(_from_rust_error)


def _apply_exception_compatibility() -> None:
    """Attach Python-side compatibility helpers to the current public types."""
    global BlpBPipeError

    for exc_type in (BlpRequestError, BlpSecurityError, BlpFieldError):
        _set_request_error_init(exc_type)

    _set_validation_error_compatibility(BlpValidationError)
    BlpBPipeError = _make_bpipe_error(BlpError)


def _bind_core_exceptions(core_module: ModuleType) -> None:
    """Rebind public exception names to the canonical Rust exception classes."""
    global BlpError, BlpSessionError, BlpRequestError, BlpSecurityError
    global BlpFieldError, BlpValidationError, BlpTimeoutError, BlpInternalError

    BlpError = core_module.BlpError
    BlpSessionError = core_module.BlpSessionError
    BlpRequestError = core_module.BlpRequestError
    BlpSecurityError = core_module.BlpSecurityError
    BlpFieldError = core_module.BlpFieldError
    BlpValidationError = core_module.BlpValidationError
    BlpTimeoutError = core_module.BlpTimeoutError
    BlpInternalError = core_module.BlpInternalError

    _apply_exception_compatibility()


_apply_exception_compatibility()

_initial_core: ModuleType | None = None
try:
    from . import _core as _loaded_core
except ImportError:
    pass
else:
    _initial_core = _loaded_core

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
