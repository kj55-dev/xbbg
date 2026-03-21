from __future__ import annotations

from typing import Any

from xbbg._services_gen import Operation, Service


def element_to_dict(elem: Any) -> dict[str, Any]:
    return {
        "name": elem.name,
        "description": elem.description,
        "data_type": elem.data_type,
        "type_name": elem.type_name,
        "is_array": elem.is_array,
        "is_optional": elem.is_optional,
        "enum_values": elem.enum_values,
        "children": [element_to_dict(child) for child in elem.children],
    }


async def list_operations(service: str | Service = Service.REFDATA) -> list[str]:
    from . import schema

    service_uri = service.value if isinstance(service, Service) else service
    return await schema.alist_operations(service_uri)


async def get_schema(
    service: str | Service = Service.REFDATA,
    operation: str | Operation | None = None,
) -> dict[str, Any]:
    from . import schema

    service_uri = service.value if isinstance(service, Service) else service

    if operation is not None:
        op_name = operation.value if isinstance(operation, Operation) else operation
        op_schema = await schema.aget_operation(service_uri, op_name)
        return {
            "name": op_schema.name,
            "description": op_schema.description,
            "request": element_to_dict(op_schema.request),
            "responses": [element_to_dict(response) for response in op_schema.responses],
        }

    svc_schema = await schema.aget_schema(service_uri)
    return {
        "service": svc_schema.service,
        "description": svc_schema.description,
        "operations": [
            {
                "name": op.name,
                "description": op.description,
                "request": element_to_dict(op.request),
                "responses": [element_to_dict(response) for response in op.responses],
            }
            for op in svc_schema.operations
        ],
        "cached_at": svc_schema.cached_at,
    }
