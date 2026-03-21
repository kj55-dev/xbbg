"""Runtime helpers for technical-analysis requests and stub generation."""

from __future__ import annotations

from typing import Any
import warnings

import narwhals.stable.v1 as nw
import pyarrow as pa

from ._sync import _run_sync
from ._ta_helpers import _TA_DEFAULTS, _TA_STUDIES


async def run_abta(
    *,
    tickers,
    study: str,
    start_date: str | None,
    end_date: str | None,
    periodicity: str,
    interval: int | None,
    study_params: dict[str, Any],
    normalize_tickers,
    get_engine,
    request_params_cls,
    build_study_request,
    convert_backend,
    get_backend,
    Service,
    Operation,
    ExtractorHint,
):
    """Run TASVC requests for one or more securities and merge results."""
    ticker_list = normalize_tickers(tickers)
    engine = get_engine()

    async def fetch_single(ticker: str) -> pa.RecordBatch | Exception:
        study_elements = build_study_request(
            ticker,
            study,
            start_date=start_date,
            end_date=end_date,
            periodicity=periodicity,
            interval=interval,
            **study_params,
        )
        params = request_params_cls()(
            service=Service.TASVC,
            operation=Operation.STUDY_REQUEST,
            extractor=ExtractorHint.GENERIC,
            elements=study_elements,
        )
        return await engine.request(params.to_dict())

    results = await __import__("asyncio").gather(
        *[fetch_single(ticker) for ticker in ticker_list],
        return_exceptions=True,
    )

    batches: list[pa.RecordBatch] = []
    for ticker, result in zip(ticker_list, results, strict=True):
        if isinstance(result, Exception):
            warnings.warn(f"Failed to fetch TA data for {ticker}: {result}", stacklevel=2)
        else:
            batches.append(result)

    if not batches:
        raise RuntimeError("All TA requests failed")

    table = pa.concat_tables([pa.Table.from_batches([batch]) for batch in batches])
    return convert_backend(nw.from_native(table), get_backend())


def generate_ta_stubs(output_dir: str | None = None) -> str:
    """Generate technical-analysis TypedDict stubs from the TASVC schema."""
    from pathlib import Path

    from .schema import aget_schema, configure_ide_stubs

    schema = _run_sync(aget_schema("//blp/tasvc"))
    op = schema.get_operation("studyRequest")
    if not op:
        raise RuntimeError("Could not find studyRequest operation in tasvc schema")

    study_attrs = None
    for child in op.request.children:
        if child.name == "studyAttributes":
            study_attrs = child
            break

    if not study_attrs:
        raise RuntimeError("Could not find studyAttributes in schema")

    lines = [
        '"""',
        "Bloomberg Technical Analysis Study Type Stubs",
        "",
        "Auto-generated from //blp/tasvc schema.",
        "DO NOT EDIT - regenerate using xbbg.generate_ta_stubs()",
        '"""',
        "",
        "from __future__ import annotations",
        "",
        "import sys",
        "if sys.version_info >= (3, 11):",
        "    from typing import Literal, NotRequired, TypedDict",
        "else:",
        "    from typing import Literal",
        "    from typing_extensions import NotRequired, TypedDict",
        "",
    ]

    attr_to_friendly = {value: key for key, value in _TA_STUDIES.items()}
    type_map = {
        "Bool": "bool",
        "Int32": "int",
        "Int64": "int",
        "Float32": "float",
        "Float64": "float",
        "String": "str",
        "Enumeration": "str",
    }

    for study_child in study_attrs.children:
        attr_name = study_child.name
        friendly = attr_to_friendly.get(attr_name, attr_name.replace("StudyAttributes", ""))
        class_name = friendly.upper() + "Params"
        if class_name.startswith("_"):
            class_name = class_name[1:]

        lines.append(f"class {class_name}(TypedDict, total=False):")
        lines.append(f'    """Parameters for {friendly} study."""')

        if not study_child.children:
            lines.append("    pass")
        else:
            for param in study_child.children:
                param_name = param.name
                if param.enum_values:
                    values_str = ", ".join(f'"{value}"' for value in param.enum_values)
                    param_type = f"Literal[{values_str}]"
                else:
                    param_type = type_map.get(param.data_type, "str")

                defaults = _TA_DEFAULTS.get(attr_name, {})
                default_val = defaults.get(param_name)
                if default_val is not None:
                    lines.append(f"    {param_name}: NotRequired[{param_type}]  # default: {default_val}")
                else:
                    lines.append(f"    {param_name}: NotRequired[{param_type}]")

        lines.append("")

    study_names = sorted(set(_TA_STUDIES.keys()))
    lines.append("# All available study names")
    lines.append(f"StudyName = Literal[{', '.join(repr(study) for study in study_names)}]")
    lines.append("")

    output_path = Path.home() / ".xbbg" / "stubs" if output_dir is None else Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    stub_path = output_path / "ta_studies.pyi"
    stub_path.write_text("\n".join(lines))

    py_path = output_path / "ta_studies.py"
    py_path.write_text("\n".join(lines))

    ide_msg = configure_ide_stubs(output_path)
    print(ide_msg)

    return str(stub_path)
