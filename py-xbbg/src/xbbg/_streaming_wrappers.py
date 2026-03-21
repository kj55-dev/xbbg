"""Pure Python wrappers around the lower-level streaming objects."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import logging
from typing import Any, TypeAlias

import narwhals.stable.v1 as nw
import pyarrow as pa

from ._request_formatting import _normalize_tickers
from .backend import Backend, convert_backend

logger = logging.getLogger(__name__)

TickValue: TypeAlias = float | int | str | bool | datetime | None


@dataclass
class Tick:
    """Single tick data point from a subscription."""

    ticker: str
    field: str
    value: TickValue
    timestamp: datetime


class Subscription:
    """Subscription handle with async iteration and dynamic control."""

    def __init__(self, py_sub, raw: bool, backend: Backend | None, tick_mode: bool = False):
        self._sub = py_sub
        self._raw = raw
        self._backend = backend
        self._tick_mode = tick_mode

    def __aiter__(self):
        return self

    async def __anext__(self) -> pa.RecordBatch | nw.DataFrame | dict[str, Any]:
        batch = await self._sub.__anext__()

        if self._tick_mode:
            return {field.name: batch.column(i)[0].as_py() for i, field in enumerate(batch.schema)}

        if self._raw:
            return batch

        table = pa.Table.from_batches([batch])
        nw_df = nw.from_native(table)
        return convert_backend(nw_df, self._backend)

    async def add(self, tickers: str | list[str]) -> None:
        ticker_list = _normalize_tickers(tickers)
        logger.debug("subscription add: %s", ticker_list)
        await self._sub.add(ticker_list)

    async def remove(self, tickers: str | list[str]) -> None:
        ticker_list = _normalize_tickers(tickers)
        logger.debug("subscription remove: %s", ticker_list)
        await self._sub.remove(ticker_list)

    @property
    def tickers(self) -> list[str]:
        return self._sub.tickers

    @property
    def failed_tickers(self) -> list[str]:
        return self._sub.failed_tickers

    @property
    def failures(self) -> list[dict[str, str]]:
        return [{"ticker": ticker, "reason": reason, "kind": kind} for ticker, reason, kind in self._sub.failures]

    @property
    def topic_states(self) -> dict[str, dict[str, int | str]]:
        return {
            ticker: {"state": state, "last_change_us": last_change_us}
            for ticker, state, last_change_us in self._sub.topic_states
        }

    @property
    def session_status(self) -> dict[str, int | str]:
        return dict(self._sub.session_status)

    @property
    def admin_status(self) -> dict[str, int | bool | None]:
        return dict(self._sub.admin_status)

    @property
    def service_status(self) -> dict[str, dict[str, int | bool]]:
        return {
            service: {"up": up, "last_change_us": last_change_us}
            for service, up, last_change_us in self._sub.service_status
        }

    @property
    def events(self) -> list[dict[str, str | int | None]]:
        return [
            {
                "at_us": at_us,
                "category": category,
                "level": level,
                "message_type": message_type,
                "topic": topic,
                "detail": detail,
            }
            for at_us, category, level, message_type, topic, detail in self._sub.events
        ]

    @property
    def status(self) -> dict[str, Any]:
        return {
            "active": self.is_active,
            "all_failed": self.all_failed,
            "tickers": self.tickers,
            "failed_tickers": self.failed_tickers,
            "topic_states": self.topic_states,
            "session": self.session_status,
            "admin": self.admin_status,
            "services": self.service_status,
        }

    @property
    def fields(self) -> list[str]:
        return self._sub.fields

    @property
    def is_active(self) -> bool:
        return self._sub.is_active

    @property
    def all_failed(self) -> bool:
        return self._sub.all_failed

    @property
    def stats(self) -> dict:
        return self._sub.stats

    async def unsubscribe(self, drain: bool = False) -> list[pa.RecordBatch] | None:
        logger.debug("unsubscribe: drain=%s", drain)
        return await self._sub.unsubscribe(drain)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.unsubscribe()

    def __repr__(self) -> str:
        return repr(self._sub)
