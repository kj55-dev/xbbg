"""Helper tables and request builders for Bloomberg technical analysis studies."""

from __future__ import annotations

from typing import Any

_TA_STUDIES: dict[str, str] = {
    "smavg": "smavgStudyAttributes",
    "sma": "smavgStudyAttributes",
    "emavg": "emavgStudyAttributes",
    "ema": "emavgStudyAttributes",
    "wmavg": "wmavgStudyAttributes",
    "wma": "wmavgStudyAttributes",
    "vmavg": "vmavgStudyAttributes",
    "vma": "vmavgStudyAttributes",
    "tmavg": "tmavgStudyAttributes",
    "tma": "tmavgStudyAttributes",
    "ipmavg": "ipmavgStudyAttributes",
    "rsi": "rsiStudyAttributes",
    "macd": "macdStudyAttributes",
    "mao": "maoStudyAttributes",
    "momentum": "momentumStudyAttributes",
    "mom": "momentumStudyAttributes",
    "roc": "rocStudyAttributes",
    "boll": "bollStudyAttributes",
    "bb": "bollStudyAttributes",
    "kltn": "kltnStudyAttributes",
    "keltner": "kltnStudyAttributes",
    "mae": "maeStudyAttributes",
    "te": "teStudyAttributes",
    "al": "alStudyAttributes",
    "dmi": "dmiStudyAttributes",
    "adx": "dmiStudyAttributes",
    "tas": "tasStudyAttributes",
    "stoch": "tasStudyAttributes",
    "trender": "trenderStudyAttributes",
    "ptps": "ptpsStudyAttributes",
    "parabolic": "ptpsStudyAttributes",
    "sar": "ptpsStudyAttributes",
    "chko": "chkoStudyAttributes",
    "ado": "adoStudyAttributes",
    "vat": "vatStudyAttributes",
    "tvat": "tvatStudyAttributes",
    "atr": "atrStudyAttributes",
    "hurst": "hurstStudyAttributes",
    "fg": "fgStudyAttributes",
    "fear_greed": "fgStudyAttributes",
    "goc": "gocStudyAttributes",
    "ichimoku": "gocStudyAttributes",
    "cmci": "cmciStudyAttributes",
    "wlpr": "wlprStudyAttributes",
    "williams": "wlprStudyAttributes",
    "maxmin": "maxminStudyAttributes",
    "rex": "rexStudyAttributes",
    "etd": "etdStudyAttributes",
    "pd": "pdStudyAttributes",
    "rv": "rvStudyAttributes",
    "pivot": "pivotStudyAttributes",
    "or": "orStudyAttributes",
    "pcr": "pcrStudyAttributes",
    "bs": "bsStudyAttributes",
}

_TA_DEFAULTS: dict[str, dict[str, Any]] = {
    "smavgStudyAttributes": {"period": 20, "priceSourceClose": "PX_LAST"},
    "emavgStudyAttributes": {"period": 20, "priceSourceClose": "PX_LAST"},
    "wmavgStudyAttributes": {"period": 20, "priceSourceClose": "PX_LAST"},
    "vmavgStudyAttributes": {"period": 20, "priceSourceClose": "PX_LAST"},
    "tmavgStudyAttributes": {"period": 20, "priceSourceClose": "PX_LAST"},
    "rsiStudyAttributes": {"period": 14, "priceSourceClose": "PX_LAST"},
    "macdStudyAttributes": {
        "maPeriod1": 12,
        "maPeriod2": 26,
        "sigPeriod": 9,
        "priceSourceClose": "PX_LAST",
    },
    "bollStudyAttributes": {
        "period": 20,
        "upperBand": 2.0,
        "lowerBand": 2.0,
        "priceSourceClose": "PX_LAST",
    },
    "dmiStudyAttributes": {
        "period": 14,
        "priceSourceHigh": "PX_HIGH",
        "priceSourceLow": "PX_LOW",
        "priceSourceClose": "PX_LAST",
    },
    "atrStudyAttributes": {
        "maType": "Simple",
        "period": 14,
        "priceSourceHigh": "PX_HIGH",
        "priceSourceLow": "PX_LOW",
        "priceSourceClose": "PX_LAST",
    },
    "tasStudyAttributes": {
        "periodK": 14,
        "periodD": 3,
        "periodDS": 3,
        "periodDSS": 3,
        "priceSourceHigh": "PX_HIGH",
        "priceSourceLow": "PX_LOW",
        "priceSourceClose": "PX_LAST",
    },
}


def get_study_attr_name(study: str) -> str:
    study_lower = study.lower().replace("-", "_").replace(" ", "_")
    if study_lower in _TA_STUDIES:
        return _TA_STUDIES[study_lower]
    if study_lower.endswith("studyattributes"):
        return study_lower
    return f"{study_lower}StudyAttributes"


def build_study_request(
    ticker: str,
    study: str,
    start_date: str | None = None,
    end_date: str | None = None,
    periodicity: str = "DAILY",
    interval: int | None = None,
    **study_params,
) -> list[tuple[str, str]]:
    attr_name = get_study_attr_name(study)
    defaults = _TA_DEFAULTS.get(attr_name, {})
    params = {**defaults, **study_params}

    elements: list[tuple[str, str]] = []

    def _norm_date(d: str | None) -> str | None:
        return d.replace("-", "").replace("/", "") if d else None

    sd = _norm_date(start_date)
    ed = _norm_date(end_date)

    elements.append(("priceSource.securityName", ticker))

    if periodicity.upper() in ("DAILY", "WEEKLY", "MONTHLY"):
        prefix = "priceSource.dataRange.historical"
        if sd:
            elements.append((f"{prefix}.startDate", sd))
        if ed:
            elements.append((f"{prefix}.endDate", ed))
        elements.append((f"{prefix}.periodicitySelection", periodicity.upper()))
    else:
        prefix = "priceSource.dataRange.intraday"
        if sd:
            elements.append((f"{prefix}.startDate", sd))
        if ed:
            elements.append((f"{prefix}.endDate", ed))
        elements.append((f"{prefix}.eventType", "TRADE"))
        elements.append((f"{prefix}.interval", str(interval or 60)))

    sa_prefix = f"studyAttributes.{attr_name}"
    for key, value in params.items():
        elements.append((f"{sa_prefix}.{key}", str(value)))

    return elements


def ta_studies() -> list[str]:
    seen = set()
    result = []
    for name in _TA_STUDIES:
        if name not in seen:
            seen.add(name)
            result.append(name)
    return sorted(result)


def ta_study_params(study: str) -> dict[str, Any]:
    attr_name = get_study_attr_name(study)
    return _TA_DEFAULTS.get(attr_name, {})
