from __future__ import annotations

import inspect

from xbbg._sync import _build_sync_wrapper, _run_sync, _syncify


async def _answer(value: int) -> int:
    return value


def test_run_sync_executes_awaitables():
    assert _run_sync(_answer(42)) == 42


def test_syncify_wraps_async_functions():
    async def plus(left: int, right: int = 0) -> int:
        return left + right

    wrapped = _syncify(plus)

    assert wrapped(2, right=3) == 5
    assert wrapped.__name__ == plus.__name__


def test_build_sync_wrapper_preserves_signature_and_metadata():
    async def plus(left: int, right: int = 0) -> int:
        return left + right

    wrapped = _build_sync_wrapper("plus_sync", plus, module_name="xbbg.blp")

    assert wrapped(2, right=3) == 5
    assert wrapped.__name__ == "plus_sync"
    assert wrapped.__qualname__ == "plus_sync"
    assert wrapped.__module__ == "xbbg.blp"
    assert str(inspect.signature(wrapped)) == str(inspect.signature(plus))
