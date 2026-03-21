"""Helper functions for Bloomberg Quote Request (BQR) support."""

from __future__ import annotations

from datetime import datetime, timedelta
import re
from typing import Any

import narwhals.stable.v1 as nw
import pyarrow as pa


def parse_date_offset(offset: str, reference: datetime) -> datetime:
    """Parse date offset string like '-2d', '-1w', '-1m', '-3h'."""
    offset = offset.strip().lower()
    match = re.match(r"^(-?\d+)([dwmh])$", offset)
    if not match:
        raise ValueError(f"Invalid date offset format: {offset}. Use format like '-2d', '-1w', '-1m', '-3h'")

    value = int(match.group(1))
    unit = match.group(2)

    if unit == "d":
        return reference + timedelta(days=value)
    if unit == "w":
        return reference + timedelta(weeks=value)
    if unit == "m":
        return reference + timedelta(days=value * 30)
    if unit == "h":
        return reference + timedelta(hours=value)
    raise ValueError(f"Unknown time unit: {unit}")


def reshape_bqr_generic(table: pa.Table, ticker: str) -> nw.DataFrame:
    """Reshape generic extractor output into structured BQR rows."""
    if "path" not in table.column_names:
        return nw.from_native(pa.table({"ticker": [], "time": [], "type": [], "value": [], "size": []}))

    paths = table["path"].to_pylist()
    value_strs = table["value_str"].to_pylist() if "value_str" in table.column_names else [None] * len(paths)
    value_nums = table["value_num"].to_pylist() if "value_num" in table.column_names else [None] * len(paths)

    pattern = re.compile(r"tickData\[(\d+)\]\.(\w+)")

    tick_values: list[tuple[str, str, Any]] = []
    all_fields: set[str] = set()

    for row_idx, path in enumerate(paths):
        if not isinstance(path, str):
            continue
        match = pattern.search(path)
        if not match:
            continue

        idx, field = match.group(1), match.group(2)
        all_fields.add(field)

        value_str = value_strs[row_idx]
        value_num = value_nums[row_idx]
        value = value_str if value_str not in (None, "") else value_num
        tick_values.append((idx, field, value))

    if not tick_values:
        return nw.from_native(pa.table({"ticker": [], "time": [], "type": [], "value": [], "size": []}))

    records_by_idx: dict[str, dict[str, Any]] = {}
    for idx, field, value in tick_values:
        if idx not in records_by_idx:
            record: dict[str, Any] = {"ticker": ticker}
            for name in all_fields:
                record[name] = None
            records_by_idx[idx] = record
        records_by_idx[idx][field] = value

    records = list(records_by_idx.values())
    result = pa.Table.from_pylist(records)

    cols = result.column_names
    priority = ["ticker", "time", "type", "value", "size"]
    ordered = [col for col in priority if col in cols]
    ordered += [col for col in cols if col not in priority]
    result = result.select(ordered)

    return nw.from_native(result)
