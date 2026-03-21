from __future__ import annotations

import importlib


def test_imports():
    """Importing xbbg should either load _core or show the SDK guidance."""
    import xbbg

    assert xbbg is not None
    module_vars = vars(xbbg)

    try:
        core = module_vars.get("_core")
        if core is None:
            core = object.__getattribute__(xbbg, "__getattr__")("_core")
    except ImportError as exc:
        assert "Bloomberg C++ SDK shared library" in str(exc)
    else:
        assert core is not None
        assert hasattr(core, "__version__")


def test_import_core_wraps_macos_loader_errors(monkeypatch):
    """macOS dylib failures should include the SDK guidance block."""
    import xbbg

    xbbg_importlib = vars(xbbg)["importlib"]
    original_import_module = xbbg_importlib.import_module

    def fake_import_module(name: str):
        if name == "xbbg._core":
            raise ImportError("dlopen(/tmp/xbbg/_core.so, 0x0002): Library not loaded: @rpath/libblpapi3_64.so")
        return original_import_module(name)

    monkeypatch.setattr(xbbg_importlib, "import_module", fake_import_module)
    monkeypatch.setattr(xbbg, "_core_module", None)
    monkeypatch.setattr(xbbg, "_importing_core", False)

    try:
        vars(xbbg)["_import_core"]()
    except ImportError as exc:
        assert "Bloomberg C++ SDK shared library" in str(exc)
    else:
        raise AssertionError("Expected _import_core() to raise ImportError")


def test_package_level_exports_resolve_to_blp_symbols():
    import xbbg

    blp = importlib.import_module("xbbg.blp")

    for name in (
        "Engine",
        "Backend",
        "request",
        "bdp",
        "subscribe",
        "Service",
        "Operation",
        "RequestParams",
    ):
        assert getattr(xbbg, name) is getattr(blp, name)
