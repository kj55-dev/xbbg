"""Internal runtime helpers for async and sync streaming iteration."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import narwhals.stable.v1 as nw
import pyarrow as pa


async def iter_subscription(
    subscribe_factory,
    *,
    tickers,
    fields,
    raw: bool,
    backend,
    callback: Callable[[pa.RecordBatch | nw.DataFrame | dict[str, Any]], None] | None,
    tick_mode: bool,
    flush_threshold: int | None,
    stream_capacity: int | None,
    overflow_policy: str | None,
    recovery_policy: str | None,
    logger,
):
    """Yield batches from an async subscription with optional callback handling."""
    async with await subscribe_factory(
        tickers,
        fields,
        raw=raw,
        backend=backend,
        tick_mode=tick_mode,
        flush_threshold=flush_threshold,
        stream_capacity=stream_capacity,
        overflow_policy=overflow_policy,
        recovery_policy=recovery_policy,
    ) as sub:
        async for batch in sub:
            if callback is not None:
                try:
                    callback(batch)
                except Exception as exc:
                    logger.warning("callback raised exception: %s", exc, exc_info=True)
            yield batch


def sync_stream_from_async(
    astream_factory,
    run_sync,
    *,
    tickers,
    fields,
    raw: bool,
    backend,
    callback,
    tick_mode: bool,
    flush_threshold: int | None,
    stream_capacity: int | None,
    overflow_policy: str | None,
):
    """Run the async stream in a background thread and yield items synchronously."""
    import queue
    import threading

    q: queue.Queue = queue.Queue()
    stop_event = threading.Event()

    async def run_stream():
        try:
            async for batch in astream_factory(
                tickers,
                fields,
                raw=raw,
                backend=backend,
                callback=callback,
                tick_mode=tick_mode,
                flush_threshold=flush_threshold,
                stream_capacity=stream_capacity,
                overflow_policy=overflow_policy,
            ):
                if stop_event.is_set():
                    break
                q.put(batch)
        except Exception as exc:
            q.put(exc)
        finally:
            q.put(None)

    def thread_target():
        run_sync(run_stream())

    thread = threading.Thread(target=thread_target, daemon=True)
    thread.start()

    try:
        while True:
            item = q.get()
            if item is None:
                break
            if isinstance(item, Exception):
                raise item
            yield item
    finally:
        stop_event.set()
        thread.join(timeout=1.0)
