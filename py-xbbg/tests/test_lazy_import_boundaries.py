from __future__ import annotations


def test_ext_package_exposes_yield_type_without_native_sdk():
    """Pure enums should stay importable without loading the native extension."""
    from xbbg import ext

    assert ext.YieldType.YTM == 1
    assert ext.YieldType.YTAL == 9


def test_markets_package_exposes_session_windows_without_native_sdk():
    """Pure market dataclasses should stay importable without loading the native extension."""
    from xbbg import markets

    assert markets.SessionWindows(day=("09:30", "16:00")).to_dict() == {"day": ("09:30", "16:00")}
