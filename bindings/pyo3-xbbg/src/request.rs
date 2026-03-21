use std::collections::HashMap;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use xbbg_async::engine::{ExtractorType, RequestParams};

fn missing_required_field(field: &str) -> PyErr {
    PyRuntimeError::new_err(format!("missing required field: {}", field))
}

fn required_item<T>(dict: &Bound<'_, PyDict>, key: &str) -> PyResult<T>
where
    for<'py> T: FromPyObject<'py>,
{
    dict.get_item(key)?
        .ok_or_else(|| missing_required_field(key))?
        .extract()
}

fn optional_item<T>(dict: &Bound<'_, PyDict>, key: &str) -> PyResult<Option<T>>
where
    for<'py> T: FromPyObject<'py>,
{
    dict.get_item(key)?.map(|value| value.extract()).transpose()
}

/// Convert a Python dictionary to Rust RequestParams.
pub(crate) fn dict_to_request_params(dict: &Bound<'_, PyDict>) -> PyResult<RequestParams> {
    // Required fields
    let service: String = required_item(dict, "service")?;

    let operation: String = required_item(dict, "operation")?;

    let (extractor, extractor_set) = match dict.get_item("extractor")? {
        Some(value) => {
            let extractor_str: String = value.extract()?;
            let extractor = ExtractorType::parse(&extractor_str).ok_or_else(|| {
                PyRuntimeError::new_err(format!("invalid extractor type: {}", extractor_str))
            })?;
            (extractor, true)
        }
        None => (ExtractorType::default(), false),
    };

    let request_operation: Option<String> = optional_item(dict, "request_operation")?;

    // Optional fields
    let securities: Option<Vec<String>> = optional_item(dict, "securities")?;

    let security: Option<String> = optional_item(dict, "security")?;

    let fields: Option<Vec<String>> = optional_item(dict, "fields")?;

    let overrides: Option<Vec<(String, String)>> = optional_item(dict, "overrides")?;

    let elements: Option<Vec<(String, String)>> = optional_item(dict, "elements")?;

    let kwargs: Option<HashMap<String, String>> = optional_item(dict, "kwargs")?;

    let start_date: Option<String> = optional_item(dict, "start_date")?;

    let end_date: Option<String> = optional_item(dict, "end_date")?;

    let start_datetime: Option<String> = optional_item(dict, "start_datetime")?;

    let end_datetime: Option<String> = optional_item(dict, "end_datetime")?;

    let event_type: Option<String> = optional_item(dict, "event_type")?;

    let event_types: Option<Vec<String>> = optional_item(dict, "event_types")?;

    let interval: Option<u32> = optional_item(dict, "interval")?;

    let options: Option<Vec<(String, String)>> = optional_item(dict, "options")?;

    let field_types: Option<HashMap<String, String>> = optional_item(dict, "field_types")?;

    let include_security_errors: bool =
        optional_item(dict, "include_security_errors")?.unwrap_or(false);

    let validate_fields: Option<bool> = optional_item(dict, "validate_fields")?;

    let search_spec: Option<String> = optional_item(dict, "search_spec")?;

    let field_ids: Option<Vec<String>> = optional_item(dict, "field_ids")?;

    let format: Option<String> = optional_item(dict, "format")?;

    Ok(RequestParams {
        service,
        operation,
        request_operation,
        extractor,
        extractor_set,
        securities,
        security,
        fields,
        overrides,
        elements,
        kwargs,
        start_date,
        end_date,
        start_datetime,
        end_datetime,
        event_type,
        event_types,
        interval,
        options,
        field_types,
        include_security_errors,
        validate_fields,
        search_spec,
        field_ids,
        format,
    })
}
