"""Schema-driven request element/override routing helpers."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any
import warnings

from xbbg._services_gen import Operation, Service

_VALID_ELEMENTS_CACHE: dict[tuple[str, str], set[str]] = {}


async def get_valid_elements(
    service: str,
    operation: str,
    *,
    get_engine: Callable[[], Any],
    logger,
) -> set[str]:
    """Get valid request element names from schema cache."""
    cache_key = (service, operation)
    if cache_key in _VALID_ELEMENTS_CACHE:
        return _VALID_ELEMENTS_CACHE[cache_key]

    try:
        engine = get_engine()
        elements = await engine.list_valid_elements(service, operation)
        valid = set(elements) if elements else set()
        _VALID_ELEMENTS_CACHE[cache_key] = valid
        return valid
    except Exception:
        logger.debug("Schema lookup failed for %s/%s, using empty set", service, operation, exc_info=True)
        return set()


async def route_kwargs(
    service: str | Service,
    operation: str | Operation,
    kwargs: dict,
    *,
    get_engine: Callable[[], Any],
    logger,
) -> tuple[list[tuple[str, Any]], list[tuple[str, str]]]:
    """Route kwargs to request elements or Bloomberg overrides using schema introspection."""
    svc = service.value if isinstance(service, Service) else service
    op = operation.value if isinstance(operation, Operation) else operation

    valid_elements = await get_valid_elements(
        svc,
        op,
        get_engine=get_engine,
        logger=logger,
    )

    elements: list[tuple[str, Any]] = []
    overrides: list[tuple[str, str]] = []

    if "overrides" in kwargs:
        ovrd = kwargs.pop("overrides")
        if isinstance(ovrd, dict):
            overrides.extend((key, str(value)) for key, value in ovrd.items())
        elif isinstance(ovrd, list):
            overrides.extend((str(key), str(value)) for key, value in ovrd)

    for key in list(kwargs.keys()):
        value = kwargs.pop(key)

        if key in valid_elements:
            elements.append((key, value))
        elif key.isupper() or (len(key) > 2 and key[0].isupper() and "_" in key):
            overrides.append((key, str(value)))
        elif valid_elements:
            warnings.warn(
                f"Unknown parameter '{key}' for {op} - passing to Bloomberg. "
                f"Valid elements: {sorted(valid_elements)[:10]}{'...' if len(valid_elements) > 10 else ''}",
                stacklevel=4,
            )
            elements.append((key, value))
        else:
            elements.append((key, value))

    return elements, overrides
