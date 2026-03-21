from __future__ import annotations

class BlpError(Exception): ...
class BlpSessionError(BlpError): ...

class BlpRequestError(BlpError):
    service: str | None
    operation: str | None
    request_id: str | None
    code: int | None

    def __init__(
        self,
        message: str,
        *,
        service: str | None = ...,
        operation: str | None = ...,
        request_id: str | None = ...,
        code: int | None = ...,
    ) -> None: ...

class BlpSecurityError(BlpRequestError): ...
class BlpFieldError(BlpRequestError): ...

class BlpValidationError(BlpError):
    element: str | None
    suggestion: str | None
    valid_values: list[str] | None

    def __init__(
        self,
        message: str,
        *,
        element: str | None = ...,
        suggestion: str | None = ...,
        valid_values: list[str] | None = ...,
    ) -> None: ...
    @classmethod
    def from_rust_error(cls, message: str) -> BlpValidationError: ...

class BlpTimeoutError(BlpError): ...
class BlpInternalError(BlpError): ...
class BlpBPipeError(BlpError): ...

def _bind_core_exceptions(core_module: object) -> None: ...
