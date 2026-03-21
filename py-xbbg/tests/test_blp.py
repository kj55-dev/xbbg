from __future__ import annotations

import importlib
import inspect


def _blp_module():
    return importlib.import_module("xbbg.blp")


def test_blp_exports_match_declared_contract():
    blp = _blp_module()

    for name in (
        "Backend",
        "arequest",
        "request",
        "abdp",
        "bdp",
        "asubscribe",
        "subscribe",
        "Tick",
        "Subscription",
        "bops",
        "bschema",
    ):
        assert name in blp.__all__


def test_generated_sync_wrappers_are_installed():
    blp = _blp_module()

    for sync_name, async_name in (
        ("bdp", "abdp"),
        ("bdh", "abdh"),
        ("bds", "abds"),
        ("bdib", "abdib"),
        ("bdtick", "abdtick"),
        ("bql", "abql"),
        ("bqr", "abqr"),
        ("bflds", "abflds"),
        ("beqs", "abeqs"),
        ("blkp", "ablkp"),
        ("bport", "abport"),
        ("bcurves", "abcurves"),
        ("bgovts", "abgovts"),
    ):
        sync_func = getattr(blp, sync_name)
        async_func = getattr(blp, async_name)

        assert callable(sync_func)
        assert callable(async_func)
        assert sync_func.__name__ == sync_name
        assert sync_func.__module__ == "xbbg.blp"
        assert str(inspect.signature(sync_func)) == str(inspect.signature(async_func))


def test_manual_sync_wrappers_are_installed():
    blp = _blp_module()

    for sync_name, async_name in (
        ("request", "arequest"),
        ("subscribe", "asubscribe"),
        ("vwap", "avwap"),
        ("mktbar", "amktbar"),
        ("depth", "adepth"),
        ("chains", "achains"),
        ("bta", "abta"),
        ("fieldInfo", "afieldInfo"),
        ("fieldSearch", "afieldSearch"),
        ("bops", "abops"),
        ("bschema", "abschema"),
    ):
        sync_func = getattr(blp, sync_name)
        async_func = getattr(blp, async_name)

        assert callable(sync_func)
        assert callable(async_func)
        assert sync_func.__name__ == sync_name
        assert sync_func.__module__ == "xbbg.blp"
        assert str(inspect.signature(sync_func)) == str(inspect.signature(async_func))


def test_blp_core_public_symbols_are_resolvable():
    blp = _blp_module()

    for name in (
        "Backend",
        "RequestContext",
        "Service",
        "Operation",
        "OutputMode",
        "RequestParams",
        "ExtractorHint",
        "Tick",
        "Subscription",
        "ta_studies",
        "ta_study_params",
    ):
        assert getattr(blp, name) is not None
