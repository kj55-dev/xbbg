from __future__ import annotations

import importlib
from pathlib import Path

from xbbg import _sdk


def _touch(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("", encoding="utf-8")


def test_find_sdk_lib_supports_macos_darwin_layout(monkeypatch, tmp_path):
    monkeypatch.setattr("sys.platform", "darwin")
    dylib = tmp_path / "Darwin" / "libblpapi3.dylib"
    _touch(dylib)

    assert _sdk._find_sdk_lib(tmp_path) == dylib


def test_find_sdk_lib_supports_versioned_vendor_root(monkeypatch, tmp_path):
    monkeypatch.setattr("sys.platform", "darwin")
    dylib = tmp_path / "3.26.1.1" / "lib" / "libblpapi3.dylib"
    _touch(dylib)

    assert _sdk._find_sdk_lib(tmp_path) == dylib


def test_default_blpapi_root_accepts_macos_vendor_layout(monkeypatch, tmp_path):
    test_conftest = importlib.import_module("conftest")
    pkg_root = tmp_path / "py-xbbg"
    vendor_root = tmp_path / "vendor" / "blpapi-sdk" / "3.26.1.1"
    (vendor_root / "include").mkdir(parents=True)
    (vendor_root / "Darwin").mkdir(parents=True)

    monkeypatch.setattr(test_conftest, "pkg_root", str(pkg_root))

    assert test_conftest._default_blpapi_root() == str(vendor_root)
