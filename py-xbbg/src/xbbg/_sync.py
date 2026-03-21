"""Shared helpers for exposing async APIs as synchronous wrappers."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Coroutine
import functools
import inspect
from typing import Any, ParamSpec, Protocol, TypeVar, cast

_P = ParamSpec("_P")
_T = TypeVar("_T")


class _WrappedCallable(Protocol[_P, _T]):
    """Callable protocol that also exposes function-style metadata."""

    __name__: str
    __qualname__: str
    __module__: str
    __signature__: inspect.Signature

    def __call__(self, *args: _P.args, **kwargs: _P.kwargs) -> _T: ...


async def _await_value(awaitable: Awaitable[_T]) -> _T:
    """Bridge a generic awaitable into a concrete coroutine."""
    return await awaitable


def _run_sync(awaitable: Awaitable[_T]) -> _T:
    """Run an awaitable to completion in a fresh event loop."""
    if inspect.iscoroutine(awaitable):
        return asyncio.run(cast("Coroutine[Any, Any, _T]", awaitable))
    return asyncio.run(_await_value(awaitable))


def _syncify(async_func: Callable[_P, Awaitable[_T]]) -> _WrappedCallable[_P, _T]:
    """Create a synchronous wrapper for an async function."""

    @functools.wraps(async_func)
    def wrapper(*args: _P.args, **kwargs: _P.kwargs) -> _T:
        return _run_sync(async_func(*args, **kwargs))

    return cast("_WrappedCallable[_P, _T]", wrapper)


def _build_sync_wrapper(
    sync_name: str,
    async_func: Callable[..., Awaitable[_T]],
    *,
    template: Callable[..., object] | None = None,
    module_name: str | None = None,
) -> _WrappedCallable[..., _T]:
    """Create a named sync wrapper that preserves the template signature."""
    template_func = template if template is not None else async_func

    @functools.wraps(template_func)
    def wrapped(*args: object, **kwargs: object) -> _T:
        return _run_sync(async_func(*args, **kwargs))

    wrapped_any = cast("Any", wrapped)
    wrapped_any.__name__ = sync_name
    wrapped_any.__qualname__ = sync_name
    wrapped_any.__signature__ = inspect.signature(template_func)
    if module_name is not None:
        wrapped_any.__module__ = module_name
    return cast("_WrappedCallable[..., _T]", wrapped)
