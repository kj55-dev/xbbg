"""Internal helpers for streaming subscription setup and validation."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any
import warnings


def validate_streaming_request(
    *,
    flush_threshold: int | None,
    stream_capacity: int | None,
    overflow_policy: str | None,
    recovery_policy: str | None,
    tick_mode: bool,
) -> tuple[int | None, str | None, str | None]:
    """Validate and normalize public streaming options."""
    if flush_threshold is not None and flush_threshold < 1:
        raise ValueError("flush_threshold must be >= 1")
    if stream_capacity is not None and stream_capacity < 1:
        raise ValueError("stream_capacity must be >= 1")
    if overflow_policy is not None and overflow_policy not in ("drop_newest", "drop_oldest", "block"):
        raise ValueError(
            f"overflow_policy must be one of 'drop_newest', 'drop_oldest', 'block', got {overflow_policy!r}"
        )
    if recovery_policy is not None and recovery_policy not in ("none", "resubscribe"):
        raise ValueError(f"recovery_policy must be one of 'none', 'resubscribe', got {recovery_policy!r}")
    if overflow_policy == "drop_oldest":
        warnings.warn(
            "overflow_policy='drop_oldest' currently behaves as 'drop_newest' for performance-safe bounded streaming",
            stacklevel=3,
        )
    if tick_mode and flush_threshold is not None and flush_threshold > 1:
        warnings.warn(
            f"tick_mode=True forces flush_threshold=1, ignoring flush_threshold={flush_threshold}",
            stacklevel=3,
        )
        flush_threshold = 1
    return flush_threshold, overflow_policy, recovery_policy


async def subscribe_with_runtime_options(
    engine: Any,
    *,
    ticker_list: list[str],
    field_list: list[str],
    service: str | None,
    options: Sequence[str] | None,
    flush_threshold: int | None,
    stream_capacity: int | None,
    overflow_policy: str | None,
    recovery_policy: str | None,
):
    """Subscribe using either the simple or extended engine API."""
    if (
        service is not None
        or options is not None
        or flush_threshold is not None
        or stream_capacity is not None
        or overflow_policy is not None
        or recovery_policy is not None
    ):
        opt_kwargs = {
            key: value
            for key, value in {
                "flush_threshold": flush_threshold,
                "stream_capacity": stream_capacity,
                "overflow_policy": overflow_policy,
                "recovery_policy": recovery_policy,
            }.items()
            if value is not None
        }
        return await engine.subscribe_with_options(
            service or "//blp/mktdata",
            ticker_list,
            field_list,
            list(options or []),
            **opt_kwargs,
        )

    return await engine.subscribe(ticker_list, field_list)
