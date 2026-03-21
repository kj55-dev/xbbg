"""Internal helpers for generated async/sync Bloomberg endpoint wrappers."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping, MutableMapping
from dataclasses import dataclass
import inspect
from typing import Any, cast

from ._sync import _build_sync_wrapper


@dataclass(frozen=True)
class _EndpointPlan:
    request_kwargs: dict[str, Any]
    backend: Any
    postprocess: Callable[[Any], Any] | None = None
    service: Any = None
    operation: Any = None
    extractor: Any = None


@dataclass(frozen=True)
class _GeneratedEndpointSpec:
    async_name: str
    sync_name: str
    service: Any
    operation: Any
    builder: Callable[[dict[str, Any]], Awaitable[_EndpointPlan] | _EndpointPlan]
    extractor: Any = None


def _strip_signature_annotations(func: Callable[..., Any]) -> str:
    """Return a signature string with annotations removed for exec-generated wrappers."""
    signature = inspect.signature(func)
    stripped_params = [param.replace(annotation=inspect._empty) for param in signature.parameters.values()]
    stripped = signature.replace(parameters=stripped_params, return_annotation=inspect._empty)
    return str(stripped)


async def _execute_generated_endpoint(
    spec: _GeneratedEndpointSpec,
    call_args: dict[str, Any],
    *,
    arequest_func: Callable[..., Awaitable[Any]],
    convert_backend_func: Callable[[Any, Any], Any],
) -> Any:
    """Execute a generated endpoint using the provided request/conversion callbacks."""
    plan_or_awaitable = spec.builder(call_args)
    if inspect.isawaitable(plan_or_awaitable):
        plan = await cast("Awaitable[_EndpointPlan]", plan_or_awaitable)
    else:
        plan = plan_or_awaitable

    request_kwargs = dict(plan.request_kwargs)
    if plan.extractor is not None:
        request_kwargs["extractor"] = plan.extractor
    elif spec.extractor is not None and "extractor" not in request_kwargs:
        request_kwargs["extractor"] = spec.extractor

    service = plan.service if plan.service is not None else spec.service
    operation = plan.operation if plan.operation is not None else spec.operation

    nw_df = await arequest_func(
        service=service,
        operation=operation,
        backend=None,
        **request_kwargs,
    )

    if plan.postprocess is not None:
        return plan.postprocess(nw_df)

    return convert_backend_func(nw_df, plan.backend)


def _build_generated_async(
    spec: _GeneratedEndpointSpec,
    async_template: Callable[..., Any],
    *,
    registry: Mapping[str, _GeneratedEndpointSpec],
    execute_generated_endpoint_func: Callable[[Any, dict[str, Any]], Awaitable[Any]],
    module_name: str,
) -> Callable[..., Any]:
    """Build an async wrapper that dispatches through the generated-endpoint registry."""
    signature_text = _strip_signature_annotations(async_template)
    source = (
        f"async def {spec.async_name}{signature_text}:\n"
        f"    return await _execute_generated_endpoint(_GENERATED_ENDPOINT_SPECS[{spec.async_name!r}], locals())"
    )
    globals_ns = {
        "__builtins__": __builtins__,
        "_execute_generated_endpoint": execute_generated_endpoint_func,
        "_GENERATED_ENDPOINT_SPECS": registry,
    }
    locals_ns: dict[str, Any] = {}
    exec(source, globals_ns, locals_ns)
    generated = locals_ns[spec.async_name]
    generated.__doc__ = async_template.__doc__
    generated.__annotations__ = dict(getattr(async_template, "__annotations__", {}))
    generated.__module__ = module_name
    generated.__qualname__ = spec.async_name
    return generated


def _install_generated_endpoint(
    spec: _GeneratedEndpointSpec,
    *,
    registry: Mapping[str, _GeneratedEndpointSpec],
    module_globals: MutableMapping[str, Any],
    execute_generated_endpoint_func: Callable[[Any, dict[str, Any]], Awaitable[Any]],
    module_name: str,
) -> None:
    """Install one generated async wrapper and its sync twin into module globals."""
    async_template = module_globals[spec.async_name]
    generated_async = _build_generated_async(
        spec,
        async_template,
        registry=registry,
        execute_generated_endpoint_func=execute_generated_endpoint_func,
        module_name=module_name,
    )
    module_globals[spec.async_name] = generated_async
    module_globals[spec.sync_name] = _build_sync_wrapper(
        spec.sync_name,
        generated_async,
        template=async_template,
        module_name=module_name,
    )


def _install_generated_endpoints(
    registry: Mapping[str, _GeneratedEndpointSpec],
    *,
    module_globals: MutableMapping[str, Any],
    execute_generated_endpoint_func: Callable[[Any, dict[str, Any]], Awaitable[Any]],
    module_name: str,
) -> None:
    """Install all generated async/sync endpoint wrappers into module globals."""
    for spec in registry.values():
        _install_generated_endpoint(
            spec,
            registry=registry,
            module_globals=module_globals,
            execute_generated_endpoint_func=execute_generated_endpoint_func,
            module_name=module_name,
        )
