from __future__ import annotations

from collections.abc import Callable
from typing import Any


def atexit_cleanup(engine: Any, *, logger) -> Any | None:
    if engine is None:
        return None

    try:
        engine.signal_shutdown()
    except Exception:
        logger.debug("Exception during atexit cleanup (ignored)", exc_info=True)
    return None


def shutdown(engine: Any) -> Any | None:
    if engine is not None:
        engine.signal_shutdown()
    return None


def reset(engine: Any) -> tuple[Any | None, Any | None]:
    return shutdown(engine), None


def is_connected(engine: Any) -> bool:
    return engine is not None


def normalize_config_kwargs(kwargs: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(kwargs)

    unsupported = {name for name in ("sess", "tls_options") if name in normalized}
    if unsupported:
        unsupported_list = ", ".join(sorted(unsupported))
        raise NotImplementedError(
            f"xbbg.configure() does not currently support {unsupported_list}. Use engine/session configuration instead."
        )

    if "server" in normalized:
        server = normalized.pop("server")
        if "server_host" not in normalized and "host" not in normalized:
            normalized["host"] = server
    if "server_host" in normalized:
        normalized["host"] = normalized.pop("server_host")
    if "server_port" in normalized:
        normalized["port"] = normalized.pop("server_port")

    if "max_attempt" in normalized:
        max_attempt = normalized.pop("max_attempt")
        normalized.setdefault("num_start_attempts", max_attempt)
    if "auto_restart" in normalized:
        auto_restart = normalized.pop("auto_restart")
        normalized.setdefault("auto_restart_on_disconnection", auto_restart)
    if "max_recovery" in normalized:
        max_recovery = normalized.pop("max_recovery")
        normalized.setdefault("max_recovery_attempts", max_recovery)
    if "retry_max" in normalized:
        retry_max = normalized.pop("retry_max")
        normalized.setdefault("retry_max_retries", retry_max)
    if "retry_delay" in normalized:
        retry_delay = normalized.pop("retry_delay")
        normalized.setdefault("retry_initial_delay_ms", retry_delay)
    if "retry_backoff" in normalized:
        retry_backoff = normalized.pop("retry_backoff")
        normalized.setdefault("retry_backoff_factor", retry_backoff)

    return normalized


def configure(
    *,
    config: Any,
    kwargs: dict[str, Any],
    engine: Any,
    import_core: Callable[[], Any],
    logger,
) -> Any:
    normalized = normalize_config_kwargs(kwargs)

    if (num_start_attempts := normalized.get("num_start_attempts")) is not None and num_start_attempts < 1:
        raise ValueError("num_start_attempts must be at least 1")

    if engine is not None:
        raise RuntimeError(
            "Cannot configure after engine has started. Call xbbg.configure() before any Bloomberg request."
        )

    if config is not None:
        configured = config
        for key, value in normalized.items():
            setattr(configured, key, value)
    else:
        configured = import_core().PyEngineConfig(**normalized)

    logger.info("Engine configured: %s", configured)
    return configured


def get_engine(
    *,
    explicit_engine: Any,
    scoped_engine: Any,
    global_engine: Any,
    config: Any,
    import_core: Callable[[], Any],
    logger,
) -> tuple[Any, Any]:
    if explicit_engine is not None:
        return explicit_engine._py_engine, global_engine

    if scoped_engine is not None:
        return scoped_engine._py_engine, global_engine

    engine = global_engine
    if engine is None:
        core = import_core()
        if config is not None:
            logger.debug("Creating PyEngine with config: %s", config)
            engine = core.PyEngine.with_config(config)
        else:
            logger.debug("Creating PyEngine with default config")
            engine = core.PyEngine()
        logger.info("PyEngine connected to Bloomberg")
    return engine, engine
