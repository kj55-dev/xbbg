"""SDK detection and management utilities for Bloomberg libraries."""

from __future__ import annotations

from pathlib import Path

_sdk_info: dict | None = None
_manual_sdk_path: Path | None = None


def _get_lib_version(_lib_path: Path) -> str | None:
    """Get the version of the linked Bloomberg C SDK at runtime."""
    try:
        from . import _core

        major, minor, patch, build = _core.sdk_version()
        return f"{major}.{minor}.{patch}.{build}"
    except Exception:
        return None


def _find_sdk_lib(sdk_path: Path) -> Path | None:
    """Find the blpapi DLL/SO in an SDK directory."""
    import sys

    if sys.platform == "win32":
        candidates = ["blpapi3_64.dll", "blpapi3_32.dll", "lib/blpapi3_64.dll", "lib/blpapi3_32.dll"]
    else:  # Linux
        candidates = ["libblpapi3_64.so", "libblpapi3.so", "lib/libblpapi3_64.so", "lib/libblpapi3.so"]

    for candidate in candidates:
        full_path = sdk_path / candidate
        if full_path.is_file():
            return full_path

    if sdk_path.is_dir():
        for child in sorted(sdk_path.iterdir(), reverse=True):
            if not child.is_dir():
                continue
            for candidate in candidates:
                full_path = child / candidate
                if full_path.is_file():
                    return full_path

    return None


def _iter_sdk_search_paths() -> list[Path]:
    """Return candidate SDK roots in priority order."""
    import os
    import sys

    paths: list[Path] = []

    if _manual_sdk_path is not None:
        paths.append(_manual_sdk_path)

    try:
        import blpapi

        blpapi_file = getattr(blpapi, "__file__", None)
        if blpapi_file:
            paths.append(Path(blpapi_file).parent)
    except ImportError:
        pass

    if sys.platform == "win32":
        dapi_paths = [
            Path(r"C:\blp\DAPI"),
            Path(os.path.expandvars(r"%LOCALAPPDATA%\Bloomberg\DAPI")),
        ]
    else:
        dapi_paths = [
            Path.home() / "blp" / "DAPI",
            Path("/opt/bloomberg/DAPI"),
        ]

    for dapi_path in dapi_paths:
        if dapi_path.is_dir():
            paths.append(dapi_path)
            break

    if blpapi_root := os.environ.get("BLPAPI_ROOT"):
        sdk_path = Path(blpapi_root)
        if sdk_path.is_dir():
            paths.append(sdk_path)

    deduped: list[Path] = []
    seen: set[Path] = set()
    for path in paths:
        try:
            resolved = path.resolve()
        except OSError:
            resolved = path
        if resolved in seen:
            continue
        seen.add(resolved)
        deduped.append(path)
    return deduped


def get_sdk_info() -> dict:
    """Detect all available Bloomberg SDK sources and versions.

    Returns a dict with:
        - sources: list of all detected SDK sources
        - active: the source that will be used (first available)

    Each source entry contains:
        - name: "blpapi_python", "dapi", or "sdk_env"
        - version: version string if detectable
        - path: Path to the SDK

    Example:
        >>> import xbbg
        >>> xbbg.get_sdk_info()
        {'sources': [{'name': 'blpapi_python', 'version': '3.25.11.1', ...}], 'active': 'blpapi_python'}
    """
    global _sdk_info
    if _sdk_info is not None:
        return _sdk_info

    sources: list[dict] = []

    # Check 0: Manually set SDK path (highest priority)
    if _manual_sdk_path is not None:
        manual_version = None
        lib_path = _find_sdk_lib(_manual_sdk_path)
        if lib_path:
            manual_version = _get_lib_version(lib_path)
        sources.append(
            {
                "name": "manual",
                "version": manual_version,
                "path": _manual_sdk_path,
            }
        )

    for sdk_path in _iter_sdk_search_paths():
        source_name = "sdk_env"
        if _manual_sdk_path is not None and sdk_path == _manual_sdk_path:
            source_name = "manual"
        else:
            try:
                import blpapi

                blpapi_file = getattr(blpapi, "__file__", None)
                if blpapi_file and sdk_path == Path(blpapi_file).parent:
                    source_name = "blpapi_python"
            except ImportError:
                pass
            if source_name == "sdk_env" and sdk_path.name == "DAPI":
                source_name = "dapi"

        if any(existing["path"] == sdk_path for existing in sources):
            continue

        sdk_version = None
        lib_path = _find_sdk_lib(sdk_path)
        if lib_path:
            sdk_version = _get_lib_version(lib_path)
        sources.append(
            {
                "name": source_name,
                "version": sdk_version,
                "path": sdk_path,
            }
        )

    runtime_version = None
    try:
        from . import _core

        major, minor, patch, build = _core.sdk_version()
        runtime_version = f"{major}.{minor}.{patch}.{build}"
    except Exception:
        pass

    info = {
        "sources": sources,
        "active": sources[0]["name"] if sources else None,
        "runtime_version": runtime_version,
    }
    _sdk_info = info
    return info


def set_sdk_path(path: str | Path) -> None:
    """Manually set the Bloomberg SDK path.

    This takes precedence over all auto-detected sources (blpapi_python, dapi, sdk_env).
    The path should point to a directory containing the Bloomberg SDK shared library.

    Args:
        path: Path to the SDK directory (e.g., "C:/blpapi_cpp_3.25.11.1" or Path object)

    Example:
        >>> import xbbg
        >>> xbbg.set_sdk_path("C:/custom/blpapi")
        >>> xbbg.get_sdk_info()["active"]
        'manual'
    """
    from pathlib import Path as PathClass

    global _manual_sdk_path, _sdk_info

    sdk_path = PathClass(path) if isinstance(path, str) else path
    if not sdk_path.is_dir():
        raise ValueError(f"SDK path does not exist or is not a directory: {sdk_path}")

    lib_path = _find_sdk_lib(sdk_path)
    if not lib_path:
        raise ValueError(f"Could not find Bloomberg SDK library in: {sdk_path}")

    _manual_sdk_path = sdk_path
    _sdk_info = None  # Clear cached info to refresh on next get_sdk_info() call


def clear_sdk_path() -> None:
    """Clear the manually set SDK path and revert to auto-detection.

    Example:
        >>> import xbbg
        >>> xbbg.set_sdk_path("C:/custom/blpapi")
        >>> xbbg.clear_sdk_path()  # Back to auto-detection
    """
    global _manual_sdk_path, _sdk_info
    _manual_sdk_path = None
    _sdk_info = None  # Clear cached info to refresh on next get_sdk_info() call


def _add_sdk_to_dll_search_path() -> None:
    """Add all detected SDK library paths to Windows DLL search path.

    This must be called before importing the native extension (_core).
    Checks all SDK sources: manual path, blpapi package, DAPI, BLPAPI_ROOT.

    All operations are wrapped in try/except to handle permission errors
    gracefully (e.g., no admin access, restricted folders).
    """
    import os

    added_dirs: set[str] = set()

    def try_add_dir(sdk_path: Path | None) -> None:
        """Try to add SDK library directory to DLL search path. Silently fails on errors."""
        if sdk_path is None:
            return
        try:
            lib_path = _find_sdk_lib(sdk_path)
            if lib_path:
                lib_dir = str(lib_path.parent)
                if lib_dir not in added_dirs:
                    os.add_dll_directory(lib_dir)
                    added_dirs.add(lib_dir)
        except (OSError, PermissionError, ValueError):
            pass  # Can't access directory or add to DLL search path

    for sdk_path in _iter_sdk_search_paths():
        try_add_dir(sdk_path)


def _prepare_sdk_runtime() -> None:
    """Make SDK shared libraries visible before importing the native extension."""
    import ctypes
    import sys

    if sys.platform == "win32":
        _add_sdk_to_dll_search_path()
        return

    mode = getattr(ctypes, "RTLD_GLOBAL", None)
    for sdk_path in _iter_sdk_search_paths():
        lib_path = _find_sdk_lib(sdk_path)
        if lib_path is None:
            continue
        try:
            if mode is None:
                ctypes.CDLL(str(lib_path))
            else:
                ctypes.CDLL(str(lib_path), mode=mode)
            return
        except OSError:
            continue
