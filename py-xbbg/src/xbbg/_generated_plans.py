from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from xbbg._services_gen import ExtractorHint, Operation, Service

from ._generated_endpoints import _EndpointPlan
from ._request_formatting import (
    _apply_wide_pivot_bdh,
    _apply_wide_pivot_bdp,
    _fmt_date,
    _handle_deprecated_wide_format,
    _normalize_fields,
    _normalize_tickers,
)


async def build_abdp_plan(args: dict[str, Any], *, aroute_kwargs, get_engine) -> _EndpointPlan:
    ticker_list = _normalize_tickers(args["tickers"])
    field_list = _normalize_fields(args["flds"])
    kwargs = dict(args.get("kwargs", {}))

    elements, overrides = await aroute_kwargs(Service.REFDATA, Operation.REFERENCE_DATA, kwargs)

    resolved_types = await get_engine().resolve_field_types(
        field_list,
        args.get("field_types"),
        "string",
    )

    fmt, want_wide = _handle_deprecated_wide_format(args.get("format"), pivot_index="ticker")

    return _EndpointPlan(
        request_kwargs={
            "securities": ticker_list,
            "fields": field_list,
            "overrides": overrides if overrides else None,
            "elements": elements if elements else None,
            "format": fmt,
            "field_types": resolved_types,
            "include_security_errors": args.get("include_security_errors", False),
            "validate_fields": args.get("validate_fields"),
        },
        backend=args.get("backend"),
        postprocess=_apply_wide_pivot_bdp if want_wide else None,
    )


async def build_abdh_plan(args: dict[str, Any], *, aroute_kwargs, get_engine) -> _EndpointPlan:
    ticker_list = _normalize_tickers(args["tickers"])
    field_list = _normalize_fields(args["flds"])
    kwargs = dict(args.get("kwargs", {}))

    fmt, want_wide = _handle_deprecated_wide_format(args.get("format"), pivot_index=["ticker", "date"])

    end_value = args.get("end_date", "today")
    start_value = args.get("start_date")

    e_dt = _fmt_date(end_value)
    if start_value is None:
        end_dt_parsed = datetime.strptime(e_dt, "%Y%m%d")
        s_dt = (end_dt_parsed - timedelta(weeks=8)).strftime("%Y%m%d")
    else:
        s_dt = _fmt_date(start_value)

    options: list[tuple[str, str]] = []
    adjust = kwargs.pop("adjust", None)
    if adjust == "all":
        options.extend(
            [
                ("adjustmentSplit", "true"),
                ("adjustmentNormal", "true"),
                ("adjustmentAbnormal", "true"),
            ]
        )
    elif adjust == "dvd":
        options.extend(
            [
                ("adjustmentNormal", "true"),
                ("adjustmentAbnormal", "true"),
            ]
        )
    elif adjust == "split":
        options.append(("adjustmentSplit", "true"))

    elements, overrides = await aroute_kwargs(Service.REFDATA, Operation.HISTORICAL_DATA, kwargs)

    resolved_types = await get_engine().resolve_field_types(
        field_list,
        args.get("field_types"),
        "float64",
    )

    return _EndpointPlan(
        request_kwargs={
            "securities": ticker_list,
            "fields": field_list,
            "start_date": s_dt,
            "end_date": e_dt,
            "overrides": overrides if overrides else None,
            "elements": elements if elements else None,
            "options": options if options else None,
            "field_types": resolved_types,
            "format": fmt,
            "validate_fields": args.get("validate_fields"),
        },
        backend=args.get("backend"),
        postprocess=_apply_wide_pivot_bdh if want_wide else None,
    )


async def build_abds_plan(args: dict[str, Any], *, aroute_kwargs) -> _EndpointPlan:
    ticker_list = _normalize_tickers(args["tickers"])
    kwargs = dict(args.get("kwargs", {}))
    elements, overrides = await aroute_kwargs(Service.REFDATA, Operation.REFERENCE_DATA, kwargs)

    return _EndpointPlan(
        request_kwargs={
            "securities": ticker_list,
            "fields": [args["flds"]],
            "overrides": overrides if overrides else None,
            "elements": elements if elements else None,
            "validate_fields": args.get("validate_fields"),
        },
        backend=args.get("backend"),
    )


async def build_abdib_plan(args: dict[str, Any], *, aroute_kwargs) -> _EndpointPlan:
    kwargs = dict(args.get("kwargs", {}))
    start_dt = args.get("start_datetime")
    end_dt = args.get("end_datetime")
    dt_value = args.get("dt")

    if start_dt is not None and end_dt is not None:
        s_dt = datetime.fromisoformat(start_dt.replace(" ", "T")).isoformat()
        e_dt = datetime.fromisoformat(end_dt.replace(" ", "T")).isoformat()
    elif dt_value is not None:
        cur_dt = datetime.fromisoformat(dt_value.replace(" ", "T")).strftime("%Y-%m-%d")
        s_dt = f"{cur_dt}T00:00:00"
        e_dt = f"{cur_dt}T23:59:59"
    else:
        raise ValueError("Either dt or both start_datetime and end_datetime must be provided")

    elements, _ = await aroute_kwargs(Service.REFDATA, Operation.INTRADAY_BAR, kwargs)

    return _EndpointPlan(
        request_kwargs={
            "security": args["ticker"],
            "event_type": args["typ"],
            "interval": args["interval"],
            "start_datetime": s_dt,
            "end_datetime": e_dt,
            "elements": elements if elements else None,
        },
        backend=args.get("backend"),
    )


async def build_abdtick_plan(args: dict[str, Any], *, aroute_kwargs) -> _EndpointPlan:
    kwargs = dict(args.get("kwargs", {}))
    s_dt = datetime.fromisoformat(args["start_datetime"].replace(" ", "T")).isoformat()
    e_dt = datetime.fromisoformat(args["end_datetime"].replace(" ", "T")).isoformat()
    event_types = list(args.get("event_types") or ["TRADE"])
    elements, _ = await aroute_kwargs(Service.REFDATA, Operation.INTRADAY_TICK, kwargs)

    return _EndpointPlan(
        request_kwargs={
            "security": args["ticker"],
            "start_datetime": s_dt,
            "end_datetime": e_dt,
            "event_types": event_types,
            "elements": elements if elements else None,
        },
        backend=args.get("backend"),
    )


def build_abql_plan(args: dict[str, Any]) -> _EndpointPlan:
    return _EndpointPlan(request_kwargs={"overrides": {"expression": args["expression"]}}, backend=args.get("backend"))


def build_abqr_plan(
    args: dict[str, Any], *, parse_date_offset, reshape_bqr_generic, convert_backend, logger
) -> _EndpointPlan:
    event_types = list(args.get("event_types") or ["BID", "ASK"])
    now = datetime.now()
    time_fmt = "%Y-%m-%dT%H:%M:%S"

    date_offset = args.get("date_offset")
    start_date = args.get("start_date")
    end_date = args.get("end_date")

    if date_offset:
        start_dt = parse_date_offset(date_offset, now)
        s_dt = start_dt.strftime(time_fmt)
        e_dt = now.strftime(time_fmt)
    elif start_date is not None:
        s_dt = _fmt_date(start_date, "%Y-%m-%d") + "T00:00:00"
        e_dt = _fmt_date(end_date, "%Y-%m-%d") + "T23:59:59" if end_date is not None else now.strftime(time_fmt)
    else:
        s_dt = (now - timedelta(days=2)).strftime(time_fmt)
        e_dt = now.strftime(time_fmt)

    elements: list[tuple[str, Any]] = []
    if args.get("include_broker_codes"):
        elements.append(("includeBrokerCodes", "true"))
    if args.get("include_spread_price"):
        elements.append(("includeSpreadPrice", "true"))
    if args.get("include_yield"):
        elements.append(("includeYield", "true"))
    if args.get("include_condition_codes"):
        elements.append(("includeConditionCodes", "true"))
    if args.get("include_exchange_codes"):
        elements.append(("includeExchangeCodes", "true"))

    has_extras = bool(elements)
    ticker = args["ticker"]
    backend = args.get("backend")

    logger.debug("abqr: ticker=%s start=%s end=%s events=%s", ticker, s_dt, e_dt, event_types)

    def postprocess(nw_df: Any):
        logger.debug("abqr: received %d rows", len(nw_df))
        result = nw_df
        if has_extras:
            table = result.to_arrow()
            if "path" in table.column_names:
                result = reshape_bqr_generic(table, ticker)
        return convert_backend(result, backend)

    return _EndpointPlan(
        request_kwargs={
            "security": ticker,
            "start_datetime": s_dt,
            "end_datetime": e_dt,
            "event_types": event_types,
            "elements": elements if elements else None,
        },
        backend=backend,
        postprocess=postprocess,
    )


def build_absrch_plan(args: dict[str, Any]) -> _EndpointPlan:
    kwargs = dict(args.get("kwargs", {}))
    overrides: dict[str, str] = {"Domain": args["domain"]}
    for key, value in kwargs.items():
        overrides[key] = str(value)
    return _EndpointPlan(request_kwargs={"overrides": overrides}, backend=args.get("backend"))


async def build_abeqs_plan(args: dict[str, Any], *, aroute_kwargs) -> _EndpointPlan:
    kwargs = dict(args.get("kwargs", {}))
    routed_elements, overrides = await aroute_kwargs(Service.REFDATA, Operation.BEQS, kwargs)
    elements: list[tuple[str, Any]] = [
        ("screenName", args["screen"]),
        ("screenType", args["screen_type"]),
        ("Group", args["group"]),
    ]
    if args.get("asof"):
        elements.append(("asOfDate", _fmt_date(args["asof"])))
    elements.extend(routed_elements)
    return _EndpointPlan(
        request_kwargs={"elements": elements, "overrides": overrides if overrides else None},
        backend=args.get("backend"),
    )


async def build_ablkp_plan(args: dict[str, Any], *, aroute_kwargs) -> _EndpointPlan:
    kwargs = dict(args.get("kwargs", {}))
    routed_elements, _ = await aroute_kwargs(Service.INSTRUMENTS, Operation.INSTRUMENT_LIST, kwargs)
    elements: list[tuple[str, Any]] = [
        ("query", args["query"]),
        ("yellowKeyFilter", args["yellowkey"]),
        ("languageOverride", args["language"]),
        ("maxResults", args["max_results"]),
    ]
    elements.extend(routed_elements)
    return _EndpointPlan(request_kwargs={"elements": elements}, backend=args.get("backend"))


async def build_abport_plan(args: dict[str, Any], *, aroute_kwargs) -> _EndpointPlan:
    kwargs = dict(args.get("kwargs", {}))
    field_list = _normalize_fields(args["fields"])
    elements, overrides = await aroute_kwargs(Service.REFDATA, Operation.PORTFOLIO_DATA, kwargs)
    return _EndpointPlan(
        request_kwargs={
            "securities": [args["portfolio"]],
            "fields": field_list,
            "elements": elements if elements else None,
            "overrides": overrides if overrides else None,
        },
        backend=args.get("backend"),
    )


async def build_abcurves_plan(args: dict[str, Any], *, aroute_kwargs) -> _EndpointPlan:
    kwargs = dict(args.get("kwargs", {}))
    routed_elements, _ = await aroute_kwargs(Service.INSTRUMENTS, Operation.CURVE_LIST, kwargs)
    elements: list[tuple[str, Any]] = []
    if args.get("country") is not None:
        elements.append(("countryCode", args["country"]))
    if args.get("currency") is not None:
        elements.append(("currencyCode", args["currency"]))
    if args.get("curve_type") is not None:
        elements.append(("type", args["curve_type"]))
    if args.get("subtype") is not None:
        elements.append(("subtype", args["subtype"]))
    if args.get("curveid") is not None:
        elements.append(("curveid", args["curveid"]))
    if args.get("bbgid") is not None:
        elements.append(("bbgid", args["bbgid"]))
    elements.extend(routed_elements)
    return _EndpointPlan(request_kwargs={"elements": elements if elements else None}, backend=args.get("backend"))


async def build_abgovts_plan(args: dict[str, Any], *, aroute_kwargs) -> _EndpointPlan:
    kwargs = dict(args.get("kwargs", {}))
    routed_elements, _ = await aroute_kwargs(Service.INSTRUMENTS, Operation.GOVT_LIST, kwargs)
    elements: list[tuple[str, Any]] = []
    if args.get("query") is not None:
        elements.append(("ticker", args["query"]))
    elements.append(("partialMatch", args["partial_match"]))
    elements.extend(routed_elements)
    return _EndpointPlan(request_kwargs={"elements": elements if elements else None}, backend=args.get("backend"))


def build_abflds_plan(args: dict[str, Any]) -> _EndpointPlan:
    fields = args.get("fields")
    search_spec = args.get("search_spec")
    if fields is not None and search_spec is not None:
        raise ValueError("Cannot specify both 'fields' and 'search_spec'")
    if fields is None and search_spec is None:
        raise ValueError("Must specify either 'fields' or 'search_spec'")
    if fields is not None:
        return _EndpointPlan(
            request_kwargs={"fields": _normalize_fields(fields)},
            backend=args.get("backend"),
            service=Service.APIFLDS,
            operation=Operation.FIELD_INFO,
        )
    return _EndpointPlan(
        request_kwargs={"fields": [search_spec]},
        backend=args.get("backend"),
        service=Service.APIFLDS,
        operation=Operation.FIELD_SEARCH,
        extractor=ExtractorHint.FIELD_INFO,
    )
