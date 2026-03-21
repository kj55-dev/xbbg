"""Shared helpers for exposing async APIs as synchronous wrappers."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
import functools
import inspect
from typing import ParamSpec, TypeVar

_P = ParamSpec("_P")
_T = TypeVar("_T")


def _run_sync(awaitable: Awaitable[_T]) -> _T:
    """Run an awaitable to completion in a fresh event loop."""
    return asyncio.run(awaitable)


def _syncify(async_func: Callable[_P, Awaitable[_T]]) -> Callable[_P, _T]:
    """Create a synchronous wrapper for an async function."""

    @functools.wraps(async_func)
    def wrapper(*args: _P.args, **kwargs: _P.kwargs) -> _T:
        return _run_sync(async_func(*args, **kwargs))

    return wrapper


def _build_sync_wrapper(
    sync_name: str,
    async_func: Callable[..., Awaitable[_T]],
    *,
    template: Callable[..., object] | None = None,
    module_name: str | None = None,
) -> Callable[..., _T]:
    """Create a named sync wrapper that preserves the template signature."""
    template_func = template if template is not None else async_func

    @functools.wraps(template_func)
    def wrapped(*args: object, **kwargs: object) -> _T:
        return _run_sync(async_func(*args, **kwargs))

    wrapped.__name__ = sync_name
    wrapped.__qualname__ = sync_name
    wrapped.__signature__ = inspect.signature(template_func)
    if module_name is not None:
        wrapped.__module__ = module_name
    return wrapped
