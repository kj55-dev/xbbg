"""Internal request middleware state and execution helpers."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
import inspect
import time
from typing import Any, TypeAlias

import pyarrow as pa

DataFrameResult: TypeAlias = Any
RequestHandler: TypeAlias = Callable[["RequestContext"], Awaitable[DataFrameResult]]
RequestMiddleware: TypeAlias = Callable[
    ["RequestContext", RequestHandler],
    DataFrameResult | Awaitable[DataFrameResult],
]


@dataclass(slots=True)
class RequestContext:
    """Mutable context object passed through the request middleware chain."""

    request_id: str
    params: Any
    params_dict: dict[str, Any]
    backend: Any
    securities: list[str]
    fields: list[str]
    metadata: dict[str, Any] = field(default_factory=dict)
    started_at: float = field(default_factory=time.perf_counter)
    elapsed_ms: float | None = None
    batch: pa.RecordBatch | None = None
    table: pa.Table | None = None
    frame: DataFrameResult | None = None
    error: Exception | None = None


_request_middleware: list[RequestMiddleware] = []


async def _await_request_value(value: DataFrameResult | Awaitable[DataFrameResult]) -> DataFrameResult:
    if inspect.isawaitable(value):
        return await value
    return value


def add_middleware(middleware: RequestMiddleware) -> RequestMiddleware:
    """Register a request middleware callable."""
    _request_middleware.append(middleware)
    return middleware


def remove_middleware(middleware: RequestMiddleware) -> None:
    """Remove a previously registered middleware callable."""
    _request_middleware.remove(middleware)


def clear_middleware() -> None:
    """Remove all registered middleware."""
    _request_middleware.clear()


def get_middleware() -> tuple[RequestMiddleware, ...]:
    """Return the currently registered middleware chain."""
    return tuple(_request_middleware)


def set_middleware(middleware: Sequence[RequestMiddleware]) -> None:
    """Replace the current middleware chain."""
    _request_middleware[:] = list(middleware)


async def run_request_middleware(
    context: RequestContext,
    terminal: RequestHandler,
) -> DataFrameResult:
    """Run the registered request middleware chain."""

    async def invoke(index: int, current_context: RequestContext) -> DataFrameResult:
        if index >= len(_request_middleware):
            return await terminal(current_context)

        middleware = _request_middleware[index]

        async def call_next(next_context: RequestContext) -> DataFrameResult:
            return await invoke(index + 1, next_context)

        try:
            return await _await_request_value(middleware(current_context, call_next))
        except Exception as exc:
            current_context.error = exc
            raise

    return await invoke(0, context)
