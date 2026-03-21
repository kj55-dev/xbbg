"""High-level Bloomberg data API: reference, historical, intraday.

This module provides the xbbg-compatible API using the Rust backend,
with support for multiple DataFrame backends via narwhals.

API Design:
- Async-first: Core implementation uses async/await (abdp, abdh, etc.)
- Sync wrappers: Convenience functions (bdp, bdh, etc.) wrap async with asyncio.run()
- Generic API: arequest() and request() for power users and arbitrary Bloomberg requests
- Users can use either style based on their needs
"""

from __future__ import annotations

import atexit
from collections.abc import Callable, Sequence
import contextvars
import logging
import time
from typing import Any, TypeAlias

import narwhals.stable.v1 as nw
from narwhals.typing import IntoFrame
import pyarrow as pa

from xbbg._services_gen import (
    ExtractorHint,
    Format,
    Operation,
    OutputMode,
    Service,
)

from . import _request_formatting, _streaming_wrappers
from ._bqr_helpers import parse_date_offset as _parse_date_offset, reshape_bqr_generic as _reshape_bqr_generic
from ._engine_runtime import (
    atexit_cleanup as _atexit_cleanup_impl,
    configure as _configure_impl,
    get_engine as _get_engine_impl,
    is_connected as _is_connected_impl,
    normalize_config_kwargs as _normalize_config_kwargs_impl,
    reset as _reset_impl,
    shutdown as _shutdown_impl,
)
from ._exports import BLP_MODULE_EXPORTS
from ._generated_endpoints import (
    _EndpointPlan,
    _execute_generated_endpoint as _execute_generated_endpoint_impl,
    _GeneratedEndpointSpec,
    _install_generated_endpoints,
)
from ._generated_plans import (
    build_abcurves_plan as _build_abcurves_plan_impl,
    build_abdh_plan as _build_abdh_plan_impl,
    build_abdib_plan as _build_abdib_plan_impl,
    build_abdp_plan as _build_abdp_plan_impl,
    build_abds_plan as _build_abds_plan_impl,
    build_abdtick_plan as _build_abdtick_plan_impl,
    build_abeqs_plan as _build_abeqs_plan_impl,
    build_abflds_plan as _build_abflds_plan_impl,
    build_abgovts_plan as _build_abgovts_plan_impl,
    build_ablkp_plan as _build_ablkp_plan_impl,
    build_abport_plan as _build_abport_plan_impl,
    build_abql_plan as _build_abql_plan_impl,
    build_abqr_plan as _build_abqr_plan_impl,
    build_absrch_plan as _build_absrch_plan_impl,
)
from ._request_formatting import _normalize_fields, _normalize_tickers
from ._request_middleware import (
    RequestContext,
    add_middleware as _add_middleware,
    clear_middleware as _clear_middleware,
    get_middleware as _get_middleware,
    remove_middleware as _remove_middleware,
    run_request_middleware as _run_request_middleware,
    set_middleware as _set_middleware,
)
from ._schema_api import get_schema as _get_schema_impl, list_operations as _list_operations_impl
from ._schema_routing import (
    get_valid_elements as _aget_valid_elements_impl,
    route_kwargs as _aroute_kwargs_impl,
)
from ._streaming_control import (
    subscribe_with_runtime_options as _subscribe_with_runtime_options,
    validate_streaming_request as _validate_streaming_request,
)
from ._streaming_runtime import (
    iter_subscription as _iter_subscription,
    sync_stream_from_async as _sync_stream_from_async,
)
from ._sync import _build_sync_wrapper, _run_sync
from ._ta_helpers import (
    build_study_request as _build_study_request,
    get_study_attr_name as _get_study_attr_name_impl,
    ta_studies as _ta_studies,
    ta_study_params as _ta_study_params,
)
from ._ta_runtime import generate_ta_stubs as _generate_ta_stubs, run_abta as _run_abta
from .backend import (
    Backend,
    convert_backend as _convert_backend,
    get_backend,
    resolve_backend as _resolve_backend,
    set_backend as _set_backend,
)

set_backend = _set_backend
add_middleware = _add_middleware
remove_middleware = _remove_middleware
clear_middleware = _clear_middleware
get_middleware = _get_middleware
set_middleware = _set_middleware
Subscription = _streaming_wrappers.Subscription
Tick = _streaming_wrappers.Tick
_apply_wide_pivot_bdh = _request_formatting._apply_wide_pivot_bdh
_apply_wide_pivot_bdp = _request_formatting._apply_wide_pivot_bdp
_fmt_date = _request_formatting._fmt_date
_handle_deprecated_wide_format = _request_formatting._handle_deprecated_wide_format
_get_study_attr_name = _get_study_attr_name_impl
ta_studies = _ta_studies
ta_study_params = _ta_study_params

# Type alias for backend conversion return types
# Covers: nw.DataFrame, nw.LazyFrame (narwhals wrappers) + IntoFrame (all native types)
DataFrameResult: TypeAlias = nw.DataFrame | nw.LazyFrame | IntoFrame

logger = logging.getLogger(__name__)


__all__ = list(BLP_MODULE_EXPORTS)


# Generated sync wrappers are installed dynamically by _install_generated_endpoints().
# Define placeholders so static analysis recognizes these exported names.
(
    bdp,
    bdh,
    bds,
    bdib,
    bdtick,
    bql,
    bsrch,
    bqr,
    bflds,
    beqs,
    blkp,
    bport,
    bcurves,
    bgovts,
) = (None,) * 14


# Engine configuration (set before first use)
_config = None  # PyEngineConfig instance or None

# Lazy-load the engine to avoid import errors when the Rust module isn't built
_engine = None

# Scoped engine for multi-engine routing (async-safe via contextvars)
_active_engine: contextvars.ContextVar[Engine | None] = contextvars.ContextVar("_active_engine", default=None)


class Engine:
    """Non-global Bloomberg engine for multi-source routing.

    Use as a context manager to scope all ``blp.*`` calls to this engine:

        engine = blp.Engine(host="bpipe.firm.com", auth_method="app", app_name="myapp")
        with engine:
            df = blp.bdp(...)  # uses this engine, not the global

    Or pass directly to individual calls:

        df = blp.bdp(..., engine=engine)

    The global ``configure()`` + ``blp.bdp()`` API is unaffected.
    """

    def __init__(self, **kwargs: Any) -> None:
        from . import _core

        normalized = _normalize_config_kwargs(kwargs)
        config = _core.PyEngineConfig(**normalized)
        self._py_engine = _core.PyEngine.with_config(config)
        self._token: contextvars.Token | None = None

    def __enter__(self) -> Engine:
        self._token = _active_engine.set(self)
        return self

    def __exit__(self, *exc: Any) -> None:
        if self._token is not None:
            _active_engine.reset(self._token)
            self._token = None

    async def __aenter__(self) -> Engine:
        self._token = _active_engine.set(self)
        return self

    async def __aexit__(self, *exc: Any) -> None:
        if self._token is not None:
            _active_engine.reset(self._token)
            self._token = None

    def shutdown(self) -> None:
        self._py_engine.signal_shutdown()


def _request_params_cls():
    """Load RequestParams lazily so pure-Python helpers stay importable."""
    from xbbg.services import RequestParams

    return RequestParams


RequestParams = _request_params_cls()


def _build_request_context(
    service: str | Service,
    operation: str | Operation,
    *,
    request_operation: str | Operation | None = None,
    securities: str | Sequence[str] | None = None,
    security: str | None = None,
    fields: str | Sequence[str] | None = None,
    overrides: dict[str, Any] | Sequence[tuple[str, str]] | None = None,
    elements: Sequence[tuple[str, Any]] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    start_datetime: str | None = None,
    end_datetime: str | None = None,
    event_type: str | None = None,
    event_types: Sequence[str] | None = None,
    interval: int | None = None,
    options: dict[str, Any] | Sequence[tuple[str, str]] | None = None,
    field_types: dict[str, str] | None = None,
    output: OutputMode | str = OutputMode.ARROW,
    extractor: ExtractorHint | str | None = None,
    format: Format | str | None = None,
    include_security_errors: bool = False,
    validate_fields: bool | None = None,
    backend: Backend | str | None = None,
) -> RequestContext:
    """Normalize request inputs, validate params, and build a request context."""
    securities_list = _normalize_tickers(securities) if securities is not None else None
    fields_list = _normalize_fields(fields) if fields is not None else None

    overrides_list: list[tuple[str, str]] | None = None
    elements_list: list[tuple[str, Any]] | None = None

    if elements is not None:
        elements_list = [(str(k), str(v).lower() if isinstance(v, bool) else str(v)) for k, v in elements]

    if overrides is not None:
        override_tuples: list[tuple[str, str]] = (
            [(str(k), str(v)) for k, v in overrides.items()] if isinstance(overrides, dict) else list(overrides)
        )
        service_str = service.value if isinstance(service, Service) else service
        if service_str in (Service.BQLSVC.value, Service.EXRSVC.value):
            if elements_list:
                elements_list.extend(override_tuples)
            else:
                elements_list = override_tuples
        else:
            overrides_list = override_tuples

    options_list: list[tuple[str, str]] | None = None
    if options is not None:
        options_list = [(str(k), str(v)) for k, v in options.items()] if isinstance(options, dict) else list(options)

    extractor_hint: ExtractorHint | None = None
    if extractor is not None:
        extractor_hint = ExtractorHint(extractor) if isinstance(extractor, str) else extractor

    format_hint: Format | None = None
    if format is not None:
        format_hint = Format(format) if isinstance(format, str) else format

    params = _request_params_cls()(
        service=service,
        operation=operation,
        request_operation=request_operation,
        securities=securities_list,
        security=security,
        fields=fields_list,
        overrides=overrides_list,
        elements=elements_list,
        start_date=start_date,
        end_date=end_date,
        start_datetime=start_datetime,
        end_datetime=end_datetime,
        event_type=event_type,
        event_types=list(event_types) if event_types else None,
        interval=interval,
        options=options_list,
        field_types=field_types,
        output=OutputMode(output) if isinstance(output, str) else output,
        extractor=extractor_hint,
        format=format_hint,
        include_security_errors=include_security_errors,
        validate_fields=validate_fields,
    )
    params.validate()

    return RequestContext(
        request_id=f"req-{time.time_ns()}",
        params=params,
        params_dict=params.to_dict(),
        backend=backend,
        securities=list(securities_list or []),
        fields=list(fields_list or []),
    )


# =============================================================================
# Engine Lifecycle Management
# =============================================================================


def _atexit_cleanup() -> None:
    """Release engine reference during interpreter shutdown.

    This is called automatically by atexit. The Rust Drop chain handles
    actual cleanup (signaling worker threads to stop).

    Non-blocking: just releases the reference, doesn't wait for threads.
    """
    global _engine
    _engine = _atexit_cleanup_impl(_engine, logger=logger)


# Register cleanup handler
atexit.register(_atexit_cleanup)


def shutdown() -> None:
    """Signal the Bloomberg engine to shutdown.

    Signals all worker threads to stop. They will terminate when they
    finish their current work or see the shutdown signal.

    This is called automatically during Python interpreter shutdown.
    You usually don't need to call this directly.

    Example::

        import xbbg

        df = xbbg.bdp("AAPL US Equity", "PX_LAST")

        # Explicit shutdown (optional - happens automatically on exit)
        xbbg.shutdown()
    """
    global _engine
    _engine = _shutdown_impl(_engine)


def reset() -> None:
    """Reset the engine to allow reconfiguration.

    Shuts down the current engine (if any) and clears configuration.
    The next Bloomberg request will create a fresh engine.

    Example::

        import xbbg

        # Initial usage
        df = xbbg.bdp("AAPL US Equity", "PX_LAST")

        # Need different config? Reset first
        xbbg.reset()
        xbbg.configure(port=9999)
        df = xbbg.bdp("AAPL US Equity", "PX_LAST")  # Uses new config
    """
    global _engine, _config
    _engine, _config = _reset_impl(_engine)


def is_connected() -> bool:
    """Check if the Bloomberg engine is initialized.

    Returns True if the engine exists. Note that this doesn't guarantee
    Bloomberg is still connected - a request might still fail if the
    connection was lost.

    Example::

        import xbbg

        print(xbbg.is_connected())  # False - not initialized yet

        df = xbbg.bdp("AAPL US Equity", "PX_LAST")

        print(xbbg.is_connected())  # True - engine created
    """
    return _is_connected_impl(_engine)


def _normalize_config_kwargs(kwargs: dict[str, Any]) -> dict[str, Any]:
    return _normalize_config_kwargs_impl(kwargs)


def configure(
    config=None,
    **kwargs,
) -> None:
    """Configure the xbbg engine before first use.

    This function must be called before any Bloomberg request is made.
    If called after the engine has started, a RuntimeError is raised.

    Can be called with an EngineConfig object, keyword arguments, or both
    (kwargs override config fields). All defaults come from Rust.

    Legacy connection-style aliases are also accepted and normalized here:
    `server` / `server_host` -> `host`, `server_port` -> `port`,
    `max_attempt` -> `num_start_attempts`, and `auto_restart` ->
    `auto_restart_on_disconnection`.

    See ``EngineConfig()`` for available fields and their defaults::

        >>> from xbbg import EngineConfig
        >>> EngineConfig()
        EngineConfig(host='localhost', port=8194, request_pool_size=2,
                     subscription_pool_size=1, ...)

    Args:
        config: An EngineConfig object with all settings.
        **kwargs: Override individual fields (host, port, request_pool_size,
            subscription_pool_size, field_cache_path, auth_method, app_name,
            user_id, ip_address, token, etc.). Legacy aliases like
            `server_host`, `server_port`, `max_attempt`, and `auto_restart`
            are also supported.

    Raises:
        RuntimeError: If called after the engine has already started.
        NotImplementedError: If unsupported session-only options such as
            `sess` or `tls_options` are provided.

    Example::

        import xbbg

        # Option 1: Using keyword arguments (most common)
        xbbg.configure(request_pool_size=4, subscription_pool_size=2)

        # Option 2: Using EngineConfig object
        from xbbg import EngineConfig

        xbbg.configure(EngineConfig(request_pool_size=4))

        # Option 3: EngineConfig + overrides
        cfg = EngineConfig(request_pool_size=4)
        xbbg.configure(cfg, subscription_pool_size=2)

        # Option 4: Legacy-style auth/server aliases also work
        xbbg.configure(
            auth_method="manual",
            app_name="my-app",
            user_id="123456",
            ip_address="10.0.0.1",
            server_host="bpipe-host",
            server_port=8195,
            max_attempt=5,
            auto_restart=False,
        )

        # Option 5: Custom field cache location
        xbbg.configure(field_cache_path="/data/bloomberg/field_cache.json")
    """
    global _config, _engine

    def _import_core():
        from . import _core

        return _core

    _config = _configure_impl(config=config, kwargs=kwargs, engine=_engine, import_core=_import_core, logger=logger)


def _get_engine(*, engine: Engine | None = None):
    """Get the active engine: explicit arg > contextvar scope > global singleton."""
    global _engine

    def _import_core():
        from . import _core

        return _core

    resolved, _engine = _get_engine_impl(
        explicit_engine=engine,
        scoped_engine=_active_engine.get(),
        global_engine=_engine,
        config=_config,
        import_core=_import_core,
        logger=logger,
    )
    return resolved


async def _aget_valid_elements(service: str, operation: str) -> set[str]:
    """Get valid request element names from schema cache (async)."""
    return await _aget_valid_elements_impl(service, operation, get_engine=_get_engine, logger=logger)


async def _aroute_kwargs(
    service: str | Service,
    operation: str | Operation,
    kwargs: dict,
) -> tuple[list[tuple[str, Any]], list[tuple[str, str]]]:
    """Route kwargs to elements or overrides using schema introspection (async)."""
    return await _aroute_kwargs_impl(service, operation, kwargs, get_engine=_get_engine, logger=logger)


async def _execute_request_terminal(context: RequestContext) -> DataFrameResult:
    engine = _get_engine()
    started = time.perf_counter()

    try:
        batch = await engine.request(context.params_dict)
    except Exception as exc:
        context.elapsed_ms = (time.perf_counter() - started) * 1000
        context.error = exc
        raise

    context.batch = batch
    context.elapsed_ms = (time.perf_counter() - started) * 1000

    logger.info(
        "bloomberg %s.%s: %d rows in %.1fms | securities=%s fields=%s",
        context.params.service,
        context.params.operation,
        batch.num_rows,
        context.elapsed_ms,
        context.securities or None,
        context.fields or None,
    )

    context.table = pa.Table.from_batches([batch])
    nw_df = nw.from_native(context.table)
    frame = _convert_backend(nw_df, context.backend)
    if frame is None:
        raise RuntimeError("backend conversion returned no frame")
    context.frame = frame
    return frame


# =============================================================================
# Generic API - Power Users
# =============================================================================


async def arequest(
    service: str | Service,
    operation: str | Operation,
    *,
    request_operation: str | Operation | None = None,
    securities: str | Sequence[str] | None = None,
    security: str | None = None,
    fields: str | Sequence[str] | None = None,
    overrides: dict[str, Any] | Sequence[tuple[str, str]] | None = None,
    elements: Sequence[tuple[str, Any]] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    start_datetime: str | None = None,
    end_datetime: str | None = None,
    event_type: str | None = None,
    event_types: Sequence[str] | None = None,
    interval: int | None = None,
    options: dict[str, Any] | Sequence[tuple[str, str]] | None = None,
    field_types: dict[str, str] | None = None,
    output: OutputMode | str = OutputMode.ARROW,
    extractor: ExtractorHint | str | None = None,
    format: Format | str | None = None,
    include_security_errors: bool = False,
    validate_fields: bool | None = None,
    backend: Backend | str | None = None,
):
    """Async generic Bloomberg request.

    This is the low-level API for power users who need to:
    - Send requests to arbitrary Bloomberg services
    - Use operations not covered by the typed convenience functions
    - Get raw JSON responses for debugging

    For common use cases, prefer the typed functions: abdp, abdh, abds, abdib, abdtick.

    Args:
        service: Bloomberg service URI (e.g., Service.REFDATA or "//blp/refdata").
        operation: Request operation name (e.g., Operation.REFERENCE_DATA).
        request_operation: Actual Bloomberg operation name when using
            ``Operation.RAW_REQUEST`` as the low-level escape hatch.
        securities: List of security identifiers (for multi-security requests).
        security: Single security identifier (for intraday requests).
        fields: List of field names to retrieve.
        overrides: Field overrides as dict or list of (name, value) tuples.
        elements: Additional request elements as list of (name, value) tuples.
            Used for schema-driven parameters like intervalHasSeconds, periodicitySelection.
        start_date: Start date for historical requests (YYYYMMDD format).
        end_date: End date for historical requests (YYYYMMDD format).
        start_datetime: Start datetime for intraday requests (ISO format).
        end_datetime: End datetime for intraday requests (ISO format).
        event_type: Event type for intraday bars (TRADE, BID, ASK, etc.).
        interval: Bar interval in minutes for intraday bars.
        options: Additional Bloomberg options as dict or list of (key, value) tuples.
        field_types: Manual type overrides for fields (for future type resolution).
        output: Output format: OutputMode.ARROW (default) or OutputMode.JSON.
        extractor: Override the auto-detected extractor. Use ExtractorHint.BULK for
            bulk data fields. If None, auto-detected from operation.
        format: Output format hint for result structure.
        include_security_errors: Include ``__SECURITY_ERROR__`` rows for
            failed securities on ReferenceData requests.
        validate_fields: Optional per-request override for field validation.
            ``True`` forces strict validation, ``False`` disables it, and
            ``None`` follows engine-level validation mode.
        backend: DataFrame backend to return. If None, uses global default.

    Returns:
        DataFrame/Table in the requested format.

    Example::

        # Query field metadata (//blp/apiflds service)
        df = await arequest(
            Service.APIFLDS,
            Operation.FIELD_INFO,
            fields=["PX_LAST", "VOLUME"],
        )

        # Get raw JSON for debugging
        json_table = await arequest(
            Service.REFDATA,
            Operation.REFERENCE_DATA,
            securities=["AAPL US Equity"],
            fields=["PX_LAST"],
            output=OutputMode.JSON,
        )

        # Custom Bloomberg request (power user)
        df = await arequest(
            "//blp/refdata",
            "ReferenceDataRequest",
            securities=["AAPL US Equity"],
            fields=["PX_LAST"],
        )

        # Raw request marker with explicit Bloomberg operation
        df = await arequest(
            Service.REFDATA,
            Operation.RAW_REQUEST,
            request_operation=Operation.REFERENCE_DATA,
            extractor=ExtractorHint.REFDATA,
            securities=["AAPL US Equity"],
            fields=["PX_LAST"],
        )
    """
    context = _build_request_context(
        service=service,
        operation=operation,
        request_operation=request_operation,
        securities=securities,
        security=security,
        fields=fields,
        overrides=overrides,
        elements=elements,
        start_date=start_date,
        end_date=end_date,
        start_datetime=start_datetime,
        end_datetime=end_datetime,
        event_type=event_type,
        event_types=event_types,
        interval=interval,
        options=options,
        field_types=field_types,
        output=output,
        extractor=extractor,
        format=format,
        include_security_errors=include_security_errors,
        validate_fields=validate_fields,
        backend=backend,
    )

    try:
        return await _run_request_middleware(context, _execute_request_terminal)
    except Exception as exc:
        context.error = exc
        raise


# =============================================================================
# Async API - Typed Convenience Functions
# =============================================================================


async def abdp(
    tickers: str | Sequence[str],
    flds: str | Sequence[str] | None = None,
    *,
    backend: Backend | str | None = None,
    format: Format | str | None = None,
    field_types: dict[str, str] | None = None,
    include_security_errors: bool = False,
    validate_fields: bool | None = None,
    **kwargs,
):
    """Async Bloomberg reference data (BDP).

    Args:
        tickers: Single ticker or list of tickers.
        flds: Single field or list of fields to query.
        backend: DataFrame backend to return. If None, uses global default.
            Supports lazy backends: 'polars_lazy', 'narwhals_lazy', 'duckdb'.
        format: Output format. Options:
            - Format.LONG (default): ticker, field, value (strings)
            - Format.LONG_TYPED: ticker, field, value_f64, value_i64, etc.
            - Format.LONG_WITH_METADATA: ticker, field, value, dtype
            - Format.WIDE: Pivoted format (DEPRECATED, use df.pivot() instead)
        field_types: Manual type overrides for fields (e.g., {'VOLUME': 'int64'}).
            If None, types are auto-resolved from Bloomberg field metadata.
        include_security_errors: Include ``__SECURITY_ERROR__`` rows for
            securities that Bloomberg rejected.
        validate_fields: Optional per-request override for field validation.
            ``True`` forces strict validation, ``False`` disables it, and
            ``None`` follows engine-level validation mode.
        **kwargs: Bloomberg overrides and infrastructure options.

    Returns:
        DataFrame in long format with columns: ticker, field, value.
        For lazy backends, returns LazyFrame that must be collected.

    Example::

        # Async usage
        df = await abdp("AAPL US Equity", ["PX_LAST", "VOLUME"])

        # Concurrent requests
        dfs = await asyncio.gather(
            abdp("AAPL US Equity", "PX_LAST"),
            abdp("MSFT US Equity", "PX_LAST"),
        )
    """
    return await _execute_generated_endpoint(_GENERATED_ENDPOINT_SPECS["abdp"], locals())


async def abdh(
    tickers: str | Sequence[str],
    flds: str | Sequence[str] | None = None,
    start_date: str | None = None,
    end_date: str = "today",
    *,
    backend: Backend | str | None = None,
    format: Format | str | None = None,
    field_types: dict[str, str] | None = None,
    validate_fields: bool | None = None,
    **kwargs,
):
    """Async Bloomberg historical data (BDH).

    Args:
        tickers: Single ticker or list of tickers.
        flds: Single field or list of fields. Defaults to ['PX_LAST'].
        start_date: Start date. Defaults to 8 weeks before end_date.
        end_date: End date. Defaults to 'today'.
        backend: DataFrame backend to return. If None, uses global default.
            Supports lazy backends: 'polars_lazy', 'narwhals_lazy', 'duckdb'.
        format: Output format. Options:
            - Format.LONG (default): ticker, date, field, value (strings)
            - Format.LONG_TYPED: ticker, date, field, value_f64, value_i64, etc.
            - Format.LONG_WITH_METADATA: ticker, date, field, value, dtype
            - Format.WIDE: Pivoted format (DEPRECATED, use df.pivot() instead)
        field_types: Manual type overrides for fields (e.g., {'VOLUME': 'int64'}).
            If None, types are auto-resolved from Bloomberg field metadata.
        validate_fields: Optional per-request override for field validation.
            ``True`` forces strict validation, ``False`` disables it, and
            ``None`` follows engine-level validation mode.
        **kwargs: Additional overrides and infrastructure options.
            adjust: Adjustment type ('all', 'dvd', 'split', '-', None).

    Returns:
        DataFrame in long format with columns: ticker, date, field, value.
        For lazy backends, returns LazyFrame that must be collected.

    Example::

        # Async usage
        df = await abdh("AAPL US Equity", "PX_LAST", start_date="2024-01-01")

        # Concurrent requests
        dfs = await asyncio.gather(
            abdh("AAPL US Equity", "PX_LAST"),
            abdh("MSFT US Equity", "PX_LAST"),
        )
    """
    return await _execute_generated_endpoint(_GENERATED_ENDPOINT_SPECS["abdh"], locals())


async def abds(
    tickers: str | Sequence[str],
    flds: str,
    *,
    backend: Backend | str | None = None,
    validate_fields: bool | None = None,
    **kwargs,
):
    """Async Bloomberg bulk data (BDS).

    Args:
        tickers: Single ticker or list of tickers.
        flds: Single field name (bulk fields return multiple rows).
        backend: DataFrame backend to return. If None, uses global default.
        validate_fields: Optional per-request override for field validation.
            ``True`` forces strict validation, ``False`` disables it, and
            ``None`` follows engine-level validation mode.
        **kwargs: Bloomberg overrides and infrastructure options.

    Returns:
        DataFrame with bulk data, multiple rows per ticker.

    Example::

        df = await abds("AAPL US Equity", "DVD_Hist_All")
        df = await abds("SPX Index", "INDX_MEMBERS", backend="polars")
    """
    return await _execute_generated_endpoint(_GENERATED_ENDPOINT_SPECS["abds"], locals())


async def abdib(
    ticker: str,
    dt: str | None = None,
    session: str = "allday",
    typ: str = "TRADE",
    *,
    start_datetime: str | None = None,
    end_datetime: str | None = None,
    interval: int = 1,
    backend: Backend | str | None = None,
    **kwargs,
):
    """Async Bloomberg intraday bar data (BDIB).

    Args:
        ticker: Ticker name.
        dt: Date to download (for single-day requests).
        session: Trading session name. Ignored when start_datetime/end_datetime provided.
        typ: Event type (TRADE, BID, ASK, etc.).
        start_datetime: Explicit start datetime for multi-day requests.
        end_datetime: Explicit end datetime for multi-day requests.
        interval: Bar interval in minutes (default: 1), or seconds if intervalHasSeconds=True.
        backend: DataFrame backend to return. If None, uses global default.
        **kwargs: Additional Bloomberg options (e.g., intervalHasSeconds, gapFillInitialBar).

    Returns:
        DataFrame with intraday bar data.

    Example::

        # 1-minute bars (default)
        df = await abdib("AAPL US Equity", dt="2024-12-01")

        # 5-minute bars with explicit datetime range
        df = await abdib(
            "AAPL US Equity",
            start_datetime="2024-12-01 09:30",
            end_datetime="2024-12-01 16:00",
            interval=5,
        )

        # 10-second bars
        df = await abdib("AAPL US Equity", dt="2024-12-01", interval=10, intervalHasSeconds=True)
    """
    return await _execute_generated_endpoint(_GENERATED_ENDPOINT_SPECS["abdib"], locals())


async def abdtick(
    ticker: str,
    start_datetime: str,
    end_datetime: str,
    *,
    event_types: Sequence[str] | None = None,
    backend: Backend | str | None = None,
    **kwargs,
):
    """Async Bloomberg tick data (BDTICK).

    Args:
        ticker: Ticker name.
        start_datetime: Start datetime.
        end_datetime: End datetime.
        event_types: Event types to retrieve. Defaults to ["TRADE"].
            Options: TRADE, BID, ASK, BID_BEST, ASK_BEST, MID_PRICE, AT_TRADE, BEST_BID, BEST_ASK.
        backend: DataFrame backend to return. If None, uses global default.
        **kwargs: Additional options.

    Returns:
        DataFrame with tick data.

    Example::

        df = await abdtick("AAPL US Equity", "2024-12-01 09:30", "2024-12-01 10:00")
        df = await abdtick(
            "AAPL US Equity", "2024-12-01 09:30", "2024-12-01 10:00", event_types=["TRADE", "BID", "ASK"]
        )
        df = await abdtick("AAPL US Equity", "2024-12-01 09:30", "2024-12-01 10:00", backend="polars")
    """
    return await _execute_generated_endpoint(_GENERATED_ENDPOINT_SPECS["abdtick"], locals())


# =============================================================================
# Sync API - Convenience Wrappers
# =============================================================================


_GENERATED_ENDPOINT_SPECS: dict[str, _GeneratedEndpointSpec] = {}


async def _execute_generated_endpoint(spec: _GeneratedEndpointSpec, call_args: dict[str, Any]) -> DataFrameResult:
    """Execute a generated endpoint using local request/conversion helpers."""
    return await _execute_generated_endpoint_impl(
        spec,
        call_args,
        arequest_func=arequest,
        convert_backend_func=_convert_backend,
    )


# Generated endpoint sync wrappers are installed via _install_generated_endpoints().


# =============================================================================
# Streaming API - Real-time Market Data
# =============================================================================


async def asubscribe(
    tickers: str | list[str],
    fields: str | list[str],
    *,
    raw: bool = False,
    backend: Backend | str | None = None,
    service: str | None = None,
    options: list[str] | None = None,
    tick_mode: bool = False,
    flush_threshold: int | None = None,
    stream_capacity: int | None = None,
    overflow_policy: str | None = None,
    recovery_policy: str | None = None,
) -> Subscription:
    """Create an async subscription to real-time market data.

    This is the low-level subscription API with full control over
    the subscription lifecycle, including dynamic add/remove.

    Args:
        tickers: Securities to subscribe to
        fields: Fields to subscribe to (e.g., 'LAST_PRICE', 'BID', 'ASK')
        raw: If True, yield raw Arrow RecordBatches for max performance
        backend: DataFrame backend for batch conversion (ignored if raw=True)
        service: Bloomberg service (e.g., '//blp/mktdata'). If provided, uses subscribe_with_options
        options: List of subscription options. If provided, uses subscribe_with_options
        tick_mode: If True, convert batches to dicts (implies raw=True)
        flush_threshold: Batch flush threshold (validation only in Wave 1)
        stream_capacity: Stream channel capacity (validation only in Wave 1)
        overflow_policy: Overflow policy for stream (validation only in Wave 1)
        recovery_policy: Optional reconnect policy: None/"none" or "resubscribe"

    Returns:
        Subscription handle for iteration and control

    Example::

        # Basic usage
        sub = await xbbg.asubscribe(["AAPL US Equity"], ["LAST_PRICE", "BID"])
        async for batch in sub:
            print(batch)
        await sub.unsubscribe()

        # With context manager
        async with xbbg.asubscribe(["AAPL US Equity"], ["LAST_PRICE"]) as sub:
            count = 0
            async for batch in sub:
                print(batch)
                count += 1
                if count >= 10:
                    break

        # Dynamic add/remove
        sub = await xbbg.asubscribe(["AAPL US Equity"], ["LAST_PRICE"])
        async for batch in sub:
            if should_add_msft:
                await sub.add(["MSFT US Equity"])
            if should_remove_aapl:
                await sub.remove(["AAPL US Equity"])

        # Tick mode (dict conversion)
        sub = await xbbg.asubscribe(["AAPL US Equity"], ["LAST_PRICE"], tick_mode=True)
        async for tick_dict in sub:
            print(tick_dict)  # {'ticker': 'AAPL US Equity', 'LAST_PRICE': 150.25, ...}
    """
    flush_threshold, overflow_policy, recovery_policy = _validate_streaming_request(
        flush_threshold=flush_threshold,
        stream_capacity=stream_capacity,
        overflow_policy=overflow_policy,
        recovery_policy=recovery_policy,
        tick_mode=tick_mode,
    )

    ticker_list = _normalize_tickers(tickers)
    field_list = _normalize_fields(fields)

    effective_backend = _resolve_backend(backend)

    engine = _get_engine()
    logger.debug("subscribe: tickers=%s fields=%s", ticker_list, field_list)

    py_sub = await _subscribe_with_runtime_options(
        engine,
        ticker_list=ticker_list,
        field_list=field_list,
        service=service,
        options=options,
        flush_threshold=flush_threshold,
        stream_capacity=stream_capacity,
        overflow_policy=overflow_policy,
        recovery_policy=recovery_policy,
    )

    return Subscription(py_sub, raw=raw or tick_mode, backend=effective_backend, tick_mode=tick_mode)


async def astream(
    tickers: str | list[str],
    fields: str | list[str],
    *,
    raw: bool = False,
    backend: Backend | str | None = None,
    callback: Callable[[pa.RecordBatch | nw.DataFrame | dict[str, Any]], None] | None = None,
    tick_mode: bool = False,
    flush_threshold: int | None = None,
    stream_capacity: int | None = None,
    overflow_policy: str | None = None,
    recovery_policy: str | None = None,
):
    """High-level async streaming - simple iteration.

    This is the simple API for streaming data. For dynamic add/remove,
    use asubscribe() instead.

    Args:
        tickers: Securities to subscribe to
        fields: Fields to subscribe to
        raw: If True, yield raw Arrow RecordBatches
        backend: DataFrame backend for batch conversion
        callback: Optional callback function to invoke on each batch
        tick_mode: If True, convert batches to dicts

    Yields:
        Batches of market data (RecordBatch, DataFrame, or dict)

    Example::

        async for batch in xbbg.astream(["AAPL US Equity"], ["LAST_PRICE"]):
            print(batch)
            if done:
                break


        # With callback
        def on_batch(batch):
            print(f"Got batch: {batch}")


        async for _ in xbbg.astream(["AAPL US Equity"], ["LAST_PRICE"], callback=on_batch):
            pass
    """
    async for batch in _iter_subscription(
        asubscribe,
        tickers=tickers,
        fields=fields,
        raw=raw,
        backend=backend,
        callback=callback,
        tick_mode=tick_mode,
        flush_threshold=flush_threshold,
        stream_capacity=stream_capacity,
        overflow_policy=overflow_policy,
        recovery_policy=recovery_policy,
        logger=logger,
    ):
        yield batch


def stream(
    tickers: str | list[str],
    fields: str | list[str],
    *,
    raw: bool = False,
    backend: Backend | str | None = None,
    callback: Callable[[pa.RecordBatch | nw.DataFrame | dict[str, Any]], None] | None = None,
    tick_mode: bool = False,
    flush_threshold: int | None = None,
    stream_capacity: int | None = None,
    overflow_policy: str | None = None,
):
    """High-level sync streaming using a background thread.

    Note: This is a generator that runs the async stream in a background
    thread. Use astream() for async contexts.

    Args:
        tickers: Securities to subscribe to
        fields: Fields to subscribe to
        raw: If True, yield raw Arrow RecordBatches
        backend: DataFrame backend for batch conversion
        callback: Optional callback function to invoke on each batch
        tick_mode: If True, convert batches to dicts

    Yields:
        Batches of market data

    Example::

        for batch in xbbg.stream(["AAPL US Equity"], ["LAST_PRICE"]):
            print(batch)
            if done:
                break
    """
    yield from _sync_stream_from_async(
        astream,
        _run_sync,
        tickers=tickers,
        fields=fields,
        raw=raw,
        backend=backend,
        callback=callback,
        tick_mode=tick_mode,
        flush_threshold=flush_threshold,
        stream_capacity=stream_capacity,
        overflow_policy=overflow_policy,
    )


# =============================================================================
# VWAP Streaming API - Real-time Volume Weighted Average Price
# =============================================================================


async def avwap(
    tickers: str | list[str],
    fields: str | list[str] | None = None,
    *,
    start_time: str | None = None,
    end_time: str | None = None,
    raw: bool = False,
    backend: Backend | str | None = None,
) -> Subscription:
    """Subscribe to real-time VWAP data (//blp/mktvwap).

    Provides streaming Volume Weighted Average Price calculations.

    Args:
        tickers: Securities to subscribe to
        fields: Fields to subscribe to (default: RT_PX_VWAP, RT_VWAP_VOLUME)
        start_time: VWAP calculation start time (e.g., "09:30")
        end_time: VWAP calculation end time (e.g., "16:00")
        raw: If True, yield raw Arrow RecordBatches for max performance
        backend: DataFrame backend for batch conversion (ignored if raw=True)

    Returns:
        Subscription handle for iteration and control

    Example::

        # Basic usage - subscribe to VWAP
        sub = await xbbg.avwap(["AAPL US Equity"])
        async for batch in sub:
            print(batch)
        await sub.unsubscribe()

        # With custom time window
        sub = await xbbg.avwap(["AAPL US Equity", "MSFT US Equity"], start_time="09:30", end_time="16:00")

        # With specific fields
        sub = await xbbg.avwap("AAPL US Equity", ["RT_PX_VWAP", "RT_VWAP_VOLUME", "RT_VWAP_TURNOVER"])
    """
    ticker_list = _normalize_tickers(tickers)

    # Default fields if not provided
    if fields is None:
        field_list = ["RT_PX_VWAP", "RT_VWAP_VOLUME"]
    else:
        field_list = _normalize_fields(fields)

    # Build subscription options
    options: list[str] = []
    if start_time:
        options.append(f"VWAP_START_TIME={start_time}")
    if end_time:
        options.append(f"VWAP_END_TIME={end_time}")

    effective_backend = _resolve_backend(backend)

    engine = _get_engine()
    py_sub = await engine.subscribe_with_options(
        Service.MKTVWAP.value,
        ticker_list,
        field_list,
        options if options else None,
    )

    return Subscription(py_sub, raw=raw, backend=effective_backend)


# =============================================================================
# MKTBAR API - Real-time Streaming OHLC Bars
# =============================================================================


async def amktbar(
    tickers: str | list[str],
    *,
    interval: int = 1,
    start_time: str | None = None,
    end_time: str | None = None,
    raw: bool = False,
    backend: Backend | str | None = None,
) -> Subscription:
    """Subscribe to real-time streaming OHLC bars.

    Like bdib but streaming instead of historical. Provides real-time
    bar updates as they form during the trading day.

    Args:
        tickers: Security identifier(s).
        interval: Bar interval in minutes (default: 1).
        start_time: Optional start time in HH:MM format.
        end_time: Optional end time in HH:MM format.
        raw: If True, return raw pyarrow RecordBatch (default: False).
        backend: DataFrame backend to return. If None, uses global default.

    Returns:
        Subscription object for async iteration.

    Example::

        # Subscribe to 5-minute bars
        async with await amktbar("AAPL US Equity", interval=5) as sub:
            async for batch in sub:
                print(batch)

        # Multiple securities
        sub = await amktbar(["AAPL US Equity", "MSFT US Equity"], interval=1)
        async for batch in sub:
            print(batch)
    """
    logger.debug("amktbar: tickers=%s interval=%d", tickers, interval)

    # Normalize inputs
    ticker_list = _normalize_tickers(tickers)
    effective_backend = _resolve_backend(backend)

    # Build subscription options
    options: list[str] = [f"interval={interval}"]
    if start_time:
        options.append(f"START_TIME={start_time}")
    if end_time:
        options.append(f"END_TIME={end_time}")

    # Get engine and subscribe
    engine = _get_engine()
    py_sub = await engine.subscribe_with_options(
        Service.MKTBAR.value,
        ticker_list,
        ["OPEN", "HIGH", "LOW", "CLOSE", "VOLUME", "NUM_TRADES"],
        options if options else None,
    )

    return Subscription(py_sub, raw=raw, backend=effective_backend)


# =============================================================================
# MKTDEPTH API - Level 2 Market Depth (B-PIPE Only)
# =============================================================================


async def adepth(
    tickers: str | list[str],
    *,
    raw: bool = False,
    backend: Backend | str | None = None,
) -> Subscription:
    """Subscribe to Level 2 market depth / order book data.

    .. warning::
        **Requires Bloomberg B-PIPE license.** This feature is not available
        with standard Terminal connections.

    Provides real-time order book updates with bid/ask prices and sizes
    at multiple levels.

    Args:
        tickers: Security identifier(s).
        raw: If True, return raw pyarrow RecordBatch (default: False).
        backend: DataFrame backend to return. If None, uses global default.

    Returns:
        Subscription object for async iteration.

    Raises:
        BlpBPipeError: If B-PIPE license is not available.

    Example::

        # Subscribe to market depth
        async with await adepth("AAPL US Equity") as sub:
            async for batch in sub:
                print(batch)  # Order book updates
    """
    from xbbg.exceptions import BlpBPipeError

    logger.debug("adepth: tickers=%s", tickers)

    # Normalize inputs
    ticker_list = _normalize_tickers(tickers)
    effective_backend = _resolve_backend(backend)

    # Get engine and subscribe
    engine = _get_engine()
    try:
        py_sub = await engine.subscribe_with_options(
            Service.MKTDEPTH.value,
            ticker_list,
            [],  # Fields are implicit for market depth
            None,
        )
    except Exception as e:
        # Check for B-PIPE related errors
        if "MKTDEPTHDATA" in str(e).upper() or "SERVICE" in str(e).upper():
            raise BlpBPipeError("Level 2 market depth requires Bloomberg B-PIPE license.") from e
        raise

    return Subscription(py_sub, raw=raw, backend=effective_backend)


# =============================================================================
# MKTLIST API - Option/Futures Chains (B-PIPE Only)
# =============================================================================


async def achains(
    underlying: str,
    *,
    chain_type: str = "OPTIONS",
    raw: bool = False,
    backend: Backend | str | None = None,
) -> Subscription:
    """Subscribe to option or futures chain updates.

    .. warning::
        **Requires Bloomberg B-PIPE license.** This feature is not available
        with standard Terminal connections.

    Provides real-time updates for option chains or futures chains
    on a given underlying security.

    Args:
        underlying: Underlying security identifier.
        chain_type: Type of chain - "OPTIONS" or "FUTURES" (default: "OPTIONS").
        raw: If True, return raw pyarrow RecordBatch (default: False).
        backend: DataFrame backend to return. If None, uses global default.

    Returns:
        Subscription object for async iteration.

    Raises:
        BlpBPipeError: If B-PIPE license is not available.

    Example::

        # Subscribe to option chain
        async with await achains("AAPL US Equity") as sub:
            async for batch in sub:
                print(batch)  # Option chain updates

        # Subscribe to futures chain
        sub = await achains("ES1 Index", chain_type="FUTURES")
    """
    from xbbg.exceptions import BlpBPipeError

    logger.debug("achains: underlying=%s chain_type=%s", underlying, chain_type)

    effective_backend = _resolve_backend(backend)

    # Build subscription options
    options: list[str] = [f"chainType={chain_type}"]

    # Get engine and subscribe
    engine = _get_engine()
    try:
        py_sub = await engine.subscribe_with_options(
            Service.MKTLIST.value,
            [underlying],
            [],  # Fields depend on chain type
            options,
        )
    except Exception as e:
        # Check for B-PIPE related errors
        if "MKTLIST" in str(e).upper() or "SERVICE" in str(e).upper():
            raise BlpBPipeError("Option/futures chains require Bloomberg B-PIPE license.") from e
        raise

    return Subscription(py_sub, raw=raw, backend=effective_backend)


# =============================================================================
# Technical Analysis API - Bloomberg Technical Analysis Service
# =============================================================================


async def abta(
    tickers: str | list[str],
    study: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    periodicity: str = "DAILY",
    interval: int | None = None,
    **study_params,
) -> DataFrameResult:
    """Get technical analysis study data (async).

    Uses Bloomberg //blp/tasvc service to calculate technical indicators.

    Args:
        tickers: Security or list of securities
        study: Study type (e.g., 'sma', 'rsi', 'macd', 'boll', 'atr')
        start_date: Start date (YYYYMMDD format)
        end_date: End date (YYYYMMDD format)
        periodicity: Data periodicity ('DAILY', 'WEEKLY', 'MONTHLY', 'INTRADAY')
        interval: Intraday interval in minutes (only for periodicity='INTRADAY')
        **study_params: Study-specific parameters (e.g., period=20 for SMA period)

    Returns:
        DataFrame with study results

    Available Studies:
        Moving Averages: sma, ema, wma, vma, tma
        Oscillators: rsi, macd, mao, momentum, roc
        Bands: boll (Bollinger), keltner, mae
        Trend: dmi/adx, stoch, trender, parabolic/sar
        Volume: chko, ado, vat
        Volatility: atr, hurst
        Other: ichimoku, pivot, williams

    Example::

        # Simple Moving Average with 20-day period
        df = await xbbg.abta("AAPL US Equity", "sma", period=20)

        # RSI with 14-day period
        df = await xbbg.abta("AAPL US Equity", "rsi", period=14)

        # MACD with custom parameters
        df = await xbbg.abta("AAPL US Equity", "macd", maPeriod1=12, maPeriod2=26, sigPeriod=9)

        # Bollinger Bands with 20-day period and 2 std devs
        df = await xbbg.abta("AAPL US Equity", "boll", period=20, upperBand=2.0, lowerBand=2.0)

        # Intraday RSI with 60-minute bars
        df = await xbbg.abta("AAPL US Equity", "rsi", periodicity="INTRADAY", interval=60)

        # Multiple securities (sends concurrent requests)
        df = await xbbg.abta(["AAPL US Equity", "MSFT US Equity"], "rsi")
    """
    return await _run_abta(
        tickers=tickers,
        study=study,
        start_date=start_date,
        end_date=end_date,
        periodicity=periodicity,
        interval=interval,
        study_params=study_params,
        normalize_tickers=_normalize_tickers,
        get_engine=_get_engine,
        request_params_cls=_request_params_cls,
        build_study_request=_build_study_request,
        convert_backend=_convert_backend,
        get_backend=get_backend,
        Service=Service,
        Operation=Operation,
        ExtractorHint=ExtractorHint,
    )


generate_ta_stubs = _generate_ta_stubs


# =============================================================================
# BQL API - Bloomberg Query Language
# =============================================================================


async def abql(
    expression: str,
    *,
    backend: Backend | str | None = None,
) -> DataFrameResult:
    """Async Bloomberg Query Language (BQL) request.

    BQL is Bloomberg's powerful query language for financial analytics.
    It allows you to query data across universes of securities with
    complex filters, calculations, and time series operations.

    Args:
        expression: BQL expression string.
        backend: DataFrame backend to return. If None, uses global default.

    Returns:
        DataFrame with columns: id, <field1>, <field2>, ...
        Where 'id' is the security identifier from the BQL universe.

    Example::

        # Get price for a single security
        df = await abql("get(px_last) for('AAPL US Equity')")

        # Get multiple fields
        df = await abql("get(px_last, volume) for('AAPL US Equity')")

        # Holdings of an ETF
        df = await abql("get(id_isin, weights) for(holdings('SPY US Equity'))")

        # Index members
        df = await abql("get(px_last) for(members('SPX Index'))")

        # With filters
        df = await abql("get(px_last, pe_ratio) for(members('SPX Index')) with(pe_ratio > 20)")

        # Time series
        df = await abql("get(px_last) for('AAPL US Equity') with(dates=range(-5d, 0d))")
    """
    return await _execute_generated_endpoint(_GENERATED_ENDPOINT_SPECS["abql"], locals())


# =============================================================================
# BSRCH API - Bloomberg Search
# =============================================================================


async def absrch(
    domain: str,
    *,
    backend: Backend | str | None = None,
    **kwargs,
) -> DataFrameResult:
    """Async Bloomberg Search (BSRCH) request.

    BSRCH executes saved Bloomberg searches and returns matching securities.

    Args:
        domain: The saved search domain/name (e.g., "FI:SOVR", "COMDTY:PRECIOUS").
        backend: DataFrame backend to return. If None, uses global default.
        **kwargs: Additional search parameters passed as request elements.

    Returns:
        DataFrame with columns from the saved search results.

    Example::

        # Sovereign bonds
        df = await absrch("FI:SOVR")

        # With additional parameters
        df = await absrch("COMDTY:WEATHER", LOCATION="NYC", MODEL="GFS")
    """
    return await _execute_generated_endpoint(_GENERATED_ENDPOINT_SPECS["absrch"], locals())


async def abqr(
    ticker: str,
    date_offset: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    *,
    event_types: Sequence[str] | None = None,
    include_broker_codes: bool = False,
    include_spread_price: bool = False,
    include_yield: bool = False,
    include_condition_codes: bool = False,
    include_exchange_codes: bool = False,
    backend: Backend | str | None = None,
    **kwargs,
) -> DataFrameResult:
    """Async Bloomberg Quote Request (BQR).

    Retrieves dealer quote data using IntradayTickRequest with BID/ASK events.
    Emulates the Excel =BQR() function.

    Args:
        ticker: Security identifier. Supports Bloomberg tickers with pricing
            source qualifiers (e.g., 'IBM US Equity@MSG1', '/isin/US037833FB15@MSG1').
        date_offset: Date offset from now (e.g., '-2d', '-1w', '-3h').
            Mutually exclusive with start_date/end_date.
        start_date: Start date (e.g., '2024-01-15'). Defaults to 2 days ago.
        end_date: End date (e.g., '2024-01-17'). Defaults to today.
        event_types: Event types to retrieve. Defaults to ['BID', 'ASK'].
        include_broker_codes: Include broker/dealer codes (default False).
        include_spread_price: Include spread price for bonds (default False).
        include_yield: Include yield data for bonds (default False).
        include_condition_codes: Include trade condition codes (default False).
        include_exchange_codes: Include exchange codes (default False).
        backend: DataFrame backend to return. If None, uses global default.
        **kwargs: Additional options.

    Returns:
        DataFrame with columns: ticker, time, type, value, size,
        plus optional brokerBuyCode, brokerSellCode, spreadPrice, etc.

    Example::

        # With date offset (like Excel BQR)
        df = await abqr("IBM US Equity@MSG1", date_offset="-2d")

        # Bond with broker codes and spread
        df = await abqr(
            "US037833FB15@MSG1 Corp",
            date_offset="-2d",
            include_broker_codes=True,
            include_spread_price=True,
        )

        # With explicit date range
        df = await abqr(
            "XYZ 4.5 01/15/30@MSG1 Corp",
            start_date="2024-01-15",
            end_date="2024-01-17",
        )

        # Trade events only
        df = await abqr(
            "XYZ 4.5 01/15/30@MSG1 Corp",
            date_offset="-1d",
            event_types=["TRADE"],
        )
    """
    return await _execute_generated_endpoint(_GENERATED_ENDPOINT_SPECS["abqr"], locals())


async def abflds(
    fields: str | list[str] | None = None,
    *,
    search_spec: str | None = None,
    backend: Backend | str | None = None,
    **kwargs,
) -> DataFrameResult:
    """Async Bloomberg field metadata lookup (BFLDS).

    Unified field function: get metadata for specific fields, or search by keyword.

    Args:
        fields: Single field or list of fields to get metadata for.
            Mutually exclusive with search_spec.
        search_spec: Search term to find fields by name/description.
            Mutually exclusive with fields.
        backend: DataFrame backend to return. If None, uses global default.
        **kwargs: Infrastructure options (e.g., port, server).

    Returns:
        DataFrame with field information or search results.

    Raises:
        ValueError: If neither fields nor search_spec is provided, or both are provided.

    Example::

        # Get info for specific fields
        df = await abflds(fields=["PX_LAST", "VOLUME"])

        # Search for fields by keyword
        df = await abflds(search_spec="vwap")
    """
    return await _execute_generated_endpoint(_GENERATED_ENDPOINT_SPECS["abflds"], locals())


# =============================================================================
# BEQS API - Bloomberg Equity Screening
# =============================================================================


async def abeqs(
    screen: str,
    *,
    asof: str | None = None,
    screen_type: str = "PRIVATE",
    group: str = "General",
    backend: Backend | str | None = None,
    **kwargs,
) -> DataFrameResult:
    """Async Bloomberg Equity Screening (BEQS) request.

    Execute a saved Bloomberg equity screen and return matching securities.

    Args:
        screen: Screen name as saved in Bloomberg.
        asof: As-of date for the screen (YYYYMMDD format).
        screen_type: Screen type - "PRIVATE" (custom) or "GLOBAL" (Bloomberg).
        group: Group name if screen is organized into groups.
        backend: DataFrame backend to return. If None, uses global default.
        **kwargs: Additional request parameters.

    Returns:
        DataFrame with columns from the screen results (security, fieldData, etc.).

    Example::

        # Run a private screen
        df = await abeqs("MyScreen")

        # Run with as-of date
        df = await abeqs("MyScreen", asof="20240101")

        # Run a Bloomberg global screen
        df = await abeqs("TOP_DECL_DVD", screen_type="GLOBAL")
    """
    return await _execute_generated_endpoint(_GENERATED_ENDPOINT_SPECS["abeqs"], locals())


# =============================================================================
# BLKP API - Bloomberg Security Lookup
# =============================================================================


async def ablkp(
    query: str,
    *,
    yellowkey: str = "YK_FILTER_NONE",
    language: str = "LANG_OVERRIDE_NONE",
    max_results: int = 20,
    backend: Backend | str | None = None,
    **kwargs,
) -> DataFrameResult:
    """Async Bloomberg security lookup (BLKP) request.

    Search for securities by company name or partial ticker.

    Args:
        query: Search query (company name or partial ticker).
        yellowkey: Asset class filter. Common values:
            - "YK_FILTER_NONE" (default, all asset classes)
            - "YK_FILTER_EQTY" (equities only)
            - "YK_FILTER_CORP" (corporate bonds)
            - "YK_FILTER_GOVT" (government bonds)
            - "YK_FILTER_INDX" (indices)
            - "YK_FILTER_CURR" (currencies)
            - "YK_FILTER_CMDT" (commodities)
        language: Language override for results.
        max_results: Maximum number of results (default: 20, max: 1000).
        backend: DataFrame backend to return. If None, uses global default.
        **kwargs: Additional request parameters.

    Returns:
        DataFrame with columns: security, description, and other result fields.

    Example::

        # Search for Apple
        df = await ablkp("Apple")

        # Search for equities only
        df = await ablkp("NVDA", yellowkey="YK_FILTER_EQTY")

        # Get more results
        df = await ablkp("Microsoft", max_results=50)
    """
    return await _execute_generated_endpoint(_GENERATED_ENDPOINT_SPECS["ablkp"], locals())


# =============================================================================
# BPORT API - Bloomberg Portfolio Data
# =============================================================================


async def abport(
    portfolio: str,
    fields: str | Sequence[str],
    *,
    backend: Backend | str | None = None,
    **kwargs,
) -> DataFrameResult:
    """Async Bloomberg portfolio data (BPORT) request.

    Get portfolio holdings and related data using PortfolioDataRequest.

    Args:
        portfolio: Portfolio identifier/name.
        fields: Field name or list of fields (e.g., "PORTFOLIO_MWEIGHT").
        backend: DataFrame backend to return. If None, uses global default.
        **kwargs: Additional request parameters/overrides.

    Returns:
        DataFrame with portfolio data.

    Example::

        # Get portfolio weights
        df = await abport("MY_PORTFOLIO", "PORTFOLIO_MWEIGHT")

        # Get multiple fields
        df = await abport("MY_PORTFOLIO", ["PORTFOLIO_MWEIGHT", "PORTFOLIO_POSITION"])
    """
    return await _execute_generated_endpoint(_GENERATED_ENDPOINT_SPECS["abport"], locals())


# =============================================================================
# BCURVES API - Bloomberg Yield Curve List
# =============================================================================


async def abcurves(
    *,
    country: str | None = None,
    currency: str | None = None,
    curve_type: str | None = None,
    subtype: str | None = None,
    curveid: str | None = None,
    bbgid: str | None = None,
    backend: Backend | str | None = None,
    **kwargs,
) -> DataFrameResult:
    """Async Bloomberg yield curve list (BCURVES) request.

    Search for yield curves by country, currency, type, or other filters.

    Args:
        country: Country code filter (e.g., "US", "GB", "DE").
        currency: Currency code filter (e.g., "USD", "EUR", "GBP").
        curve_type: Curve type filter (e.g., "GOVERNMENT", "CORPORATE").
        subtype: Curve subtype filter.
        curveid: Specific curve ID to look up.
        bbgid: Bloomberg Global ID filter.
        backend: DataFrame backend to return. If None, uses global default.
        **kwargs: Additional request parameters.

    Returns:
        DataFrame with yield curve information.

    Example::

        # List US yield curves
        df = await abcurves(country="US")

        # List USD government curves
        df = await abcurves(currency="USD", curve_type="GOVERNMENT")

        # Look up specific curve
        df = await abcurves(curveid="YCSW0023 Index")
    """
    return await _execute_generated_endpoint(_GENERATED_ENDPOINT_SPECS["abcurves"], locals())


# =============================================================================
# BGOVTS API - Bloomberg Government Securities List
# =============================================================================


async def abgovts(
    query: str | None = None,
    *,
    partial_match: bool = True,
    backend: Backend | str | None = None,
    **kwargs,
) -> DataFrameResult:
    """Async Bloomberg government securities list (BGOVTS) request.

    Search for government securities by ticker or name.

    Args:
        query: Search query (ticker or partial name).
        partial_match: If True, match partial ticker names (default: True).
        backend: DataFrame backend to return. If None, uses global default.
        **kwargs: Additional request parameters.

    Returns:
        DataFrame with government securities information.

    Example::

        # Search for US Treasury securities
        df = await abgovts("T")

        # Search for German government bonds
        df = await abgovts("DBR")

        # Exact match only
        df = await abgovts("T 2.5 05/15/24", partial_match=False)
    """
    return await _execute_generated_endpoint(_GENERATED_ENDPOINT_SPECS["abgovts"], locals())


async def _build_abdp_plan(args: dict[str, Any]) -> _EndpointPlan:
    return await _build_abdp_plan_impl(args, aroute_kwargs=_aroute_kwargs, get_engine=_get_engine)


async def _build_abdh_plan(args: dict[str, Any]) -> _EndpointPlan:
    return await _build_abdh_plan_impl(args, aroute_kwargs=_aroute_kwargs, get_engine=_get_engine)


async def _build_abds_plan(args: dict[str, Any]) -> _EndpointPlan:
    return await _build_abds_plan_impl(args, aroute_kwargs=_aroute_kwargs)


async def _build_abdib_plan(args: dict[str, Any]) -> _EndpointPlan:
    return await _build_abdib_plan_impl(args, aroute_kwargs=_aroute_kwargs)


async def _build_abdtick_plan(args: dict[str, Any]) -> _EndpointPlan:
    return await _build_abdtick_plan_impl(args, aroute_kwargs=_aroute_kwargs)


def _build_abql_plan(args: dict[str, Any]) -> _EndpointPlan:
    return _build_abql_plan_impl(args)


def _build_abqr_plan(args: dict[str, Any]) -> _EndpointPlan:
    return _build_abqr_plan_impl(
        args,
        parse_date_offset=_parse_date_offset,
        reshape_bqr_generic=_reshape_bqr_generic,
        convert_backend=_convert_backend,
        logger=logger,
    )


def _build_absrch_plan(args: dict[str, Any]) -> _EndpointPlan:
    return _build_absrch_plan_impl(args)


async def _build_abeqs_plan(args: dict[str, Any]) -> _EndpointPlan:
    return await _build_abeqs_plan_impl(args, aroute_kwargs=_aroute_kwargs)


async def _build_ablkp_plan(args: dict[str, Any]) -> _EndpointPlan:
    return await _build_ablkp_plan_impl(args, aroute_kwargs=_aroute_kwargs)


async def _build_abport_plan(args: dict[str, Any]) -> _EndpointPlan:
    return await _build_abport_plan_impl(args, aroute_kwargs=_aroute_kwargs)


async def _build_abcurves_plan(args: dict[str, Any]) -> _EndpointPlan:
    return await _build_abcurves_plan_impl(args, aroute_kwargs=_aroute_kwargs)


async def _build_abgovts_plan(args: dict[str, Any]) -> _EndpointPlan:
    return await _build_abgovts_plan_impl(args, aroute_kwargs=_aroute_kwargs)


def _build_abflds_plan(args: dict[str, Any]) -> _EndpointPlan:
    return _build_abflds_plan_impl(args)


_GENERATED_ENDPOINT_SPECS.update(
    {
        "abdp": _GeneratedEndpointSpec(
            async_name="abdp",
            sync_name="bdp",
            service=Service.REFDATA,
            operation=Operation.REFERENCE_DATA,
            builder=_build_abdp_plan,
        ),
        "abdh": _GeneratedEndpointSpec(
            async_name="abdh",
            sync_name="bdh",
            service=Service.REFDATA,
            operation=Operation.HISTORICAL_DATA,
            builder=_build_abdh_plan,
        ),
        "abds": _GeneratedEndpointSpec(
            async_name="abds",
            sync_name="bds",
            service=Service.REFDATA,
            operation=Operation.REFERENCE_DATA,
            builder=_build_abds_plan,
            extractor=ExtractorHint.BULK,
        ),
        "abdib": _GeneratedEndpointSpec(
            async_name="abdib",
            sync_name="bdib",
            service=Service.REFDATA,
            operation=Operation.INTRADAY_BAR,
            builder=_build_abdib_plan,
        ),
        "abdtick": _GeneratedEndpointSpec(
            async_name="abdtick",
            sync_name="bdtick",
            service=Service.REFDATA,
            operation=Operation.INTRADAY_TICK,
            builder=_build_abdtick_plan,
        ),
        "abql": _GeneratedEndpointSpec(
            async_name="abql",
            sync_name="bql",
            service=Service.BQLSVC,
            operation=Operation.BQL_SEND_QUERY,
            builder=_build_abql_plan,
            extractor=ExtractorHint.BQL,
        ),
        "abqr": _GeneratedEndpointSpec(
            async_name="abqr",
            sync_name="bqr",
            service=Service.REFDATA,
            operation=Operation.INTRADAY_TICK,
            builder=_build_abqr_plan,
        ),
        "absrch": _GeneratedEndpointSpec(
            async_name="absrch",
            sync_name="bsrch",
            service=Service.EXRSVC,
            operation=Operation.EXCEL_GET_GRID,
            builder=_build_absrch_plan,
            extractor=ExtractorHint.BSRCH,
        ),
        "abeqs": _GeneratedEndpointSpec(
            async_name="abeqs",
            sync_name="beqs",
            service=Service.REFDATA,
            operation=Operation.BEQS,
            builder=_build_abeqs_plan,
            extractor=ExtractorHint.GENERIC,
        ),
        "ablkp": _GeneratedEndpointSpec(
            async_name="ablkp",
            sync_name="blkp",
            service=Service.INSTRUMENTS,
            operation=Operation.INSTRUMENT_LIST,
            builder=_build_ablkp_plan,
            extractor=ExtractorHint.GENERIC,
        ),
        "abport": _GeneratedEndpointSpec(
            async_name="abport",
            sync_name="bport",
            service=Service.REFDATA,
            operation=Operation.PORTFOLIO_DATA,
            builder=_build_abport_plan,
        ),
        "abcurves": _GeneratedEndpointSpec(
            async_name="abcurves",
            sync_name="bcurves",
            service=Service.INSTRUMENTS,
            operation=Operation.CURVE_LIST,
            builder=_build_abcurves_plan,
            extractor=ExtractorHint.GENERIC,
        ),
        "abgovts": _GeneratedEndpointSpec(
            async_name="abgovts",
            sync_name="bgovts",
            service=Service.INSTRUMENTS,
            operation=Operation.GOVT_LIST,
            builder=_build_abgovts_plan,
            extractor=ExtractorHint.GENERIC,
        ),
        "abflds": _GeneratedEndpointSpec(
            async_name="abflds",
            sync_name="bflds",
            service=Service.APIFLDS,
            operation=Operation.FIELD_INFO,
            builder=_build_abflds_plan,
        ),
    }
)

_install_generated_endpoints(
    _GENERATED_ENDPOINT_SPECS,
    module_globals=globals(),
    execute_generated_endpoint_func=_execute_generated_endpoint,
    module_name=__name__,
)

# Backward-compatible aliases
abfld = abflds
bfld = bflds


async def afieldInfo(
    fields: str | list[str],
    *,
    backend: Backend | str | None = None,
    **kwargs,
) -> DataFrameResult:
    """Get metadata about Bloomberg fields (async).

    Convenience wrapper around abflds(fields=...).

    Args:
        fields: Single field or list of fields to get metadata for.
        backend: DataFrame backend to return. If None, uses global default.
        **kwargs: Infrastructure options.

    Returns:
        DataFrame with field information.

    Example::

        df = await afieldInfo(["PX_LAST", "VOLUME"])
    """
    return await abflds(fields=fields, backend=backend, **kwargs)


async def afieldSearch(
    searchterm: str,
    *,
    backend: Backend | str | None = None,
    **kwargs,
) -> DataFrameResult:
    """Search for Bloomberg fields by keyword (async).

    Convenience wrapper around abflds(search_spec=...).

    Args:
        searchterm: Search term to find fields by name/description.
        backend: DataFrame backend to return. If None, uses global default.
        **kwargs: Infrastructure options.

    Returns:
        DataFrame with search results.

    Example::

        df = await afieldSearch("vwap")
    """
    return await abflds(search_spec=searchterm, backend=backend, **kwargs)


# ─── Schema Introspection API ────────────────────────────────────────────────


async def abops(service: str | Service = Service.REFDATA) -> list[str]:
    """List available operations for a Bloomberg service (async).

    Args:
        service: Service URI or Service enum (default: //blp/refdata)

    Returns:
        List of operation names.

    Example::

        >>> ops = await abops()
        >>> print(ops)
        ['ReferenceDataRequest', 'HistoricalDataRequest', ...]

        >>> ops = await abops("//blp/instruments")
        >>> print(ops)
        ['InstrumentListRequest', ...]
    """
    return await _list_operations_impl(service)


async def abschema(
    service: str | Service = Service.REFDATA,
    operation: str | Operation | None = None,
) -> dict:
    """Get Bloomberg service or operation schema (async).

    Returns introspected schema with element definitions, types, and enum values.
    Schemas are cached locally (~/.xbbg/schemas/) for fast subsequent access.

    Args:
        service: Service URI or Service enum (default: //blp/refdata)
        operation: Optional operation name. If None, returns full service schema.

    Returns:
        Dictionary with schema information:
        - If operation is None: Full service schema with all operations
        - If operation is specified: Just that operation's request/response schema

    Example::

        >>> # Get full service schema
        >>> schema = await abschema()
        >>> print(schema['operations'][0]['name'])
        'ReferenceDataRequest'

        >>> # Get specific operation schema
        >>> op_schema = await abschema(operation="ReferenceDataRequest")
        >>> print(op_schema['request']['children'][0]['name'])
        'securities'

        >>> # Get enum values for an element
        >>> op = await abschema(operation="HistoricalDataRequest")
        >>> for child in op['request']['children']:
        ...     if child.get('enum_values'):
        ...         print(f"{child['name']}: {child['enum_values']}")
    """
    return await _get_schema_impl(service, operation)


def _install_manual_sync_wrappers() -> None:
    for sync_name, async_func in (
        ("request", arequest),
        ("subscribe", asubscribe),
        ("vwap", avwap),
        ("mktbar", amktbar),
        ("depth", adepth),
        ("chains", achains),
        ("bta", abta),
        ("fieldInfo", afieldInfo),
        ("fieldSearch", afieldSearch),
        ("bops", abops),
        ("bschema", abschema),
    ):
        globals()[sync_name] = _build_sync_wrapper(sync_name, async_func, module_name=__name__)


_install_manual_sync_wrappers()
