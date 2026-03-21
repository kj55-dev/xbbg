use pyo3::prelude::*;

use xbbg_async::BlpAsyncError;
use xbbg_core::{errors::CorrelationContext, BlpError};

// =============================================================================
// Python Exception Hierarchy (mirrors py-xbbg/src/xbbg/exceptions.py)
// =============================================================================

pyo3::create_exception!(xbbg._core, BlpErrorBase, pyo3::exceptions::PyException);
pyo3::create_exception!(xbbg._core, BlpSessionError, BlpErrorBase);
pyo3::create_exception!(xbbg._core, BlpRequestError, BlpErrorBase);
pyo3::create_exception!(xbbg._core, BlpSecurityError, BlpRequestError);
pyo3::create_exception!(xbbg._core, BlpFieldError, BlpRequestError);
pyo3::create_exception!(xbbg._core, BlpValidationError, BlpErrorBase);
pyo3::create_exception!(xbbg._core, BlpTimeoutError, BlpErrorBase);
pyo3::create_exception!(xbbg._core, BlpInternalError, BlpErrorBase);

pub(crate) fn register_exceptions(py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("BlpError", py.get_type::<BlpErrorBase>())?;
    module.add("BlpSessionError", py.get_type::<BlpSessionError>())?;
    module.add("BlpRequestError", py.get_type::<BlpRequestError>())?;
    module.add("BlpSecurityError", py.get_type::<BlpSecurityError>())?;
    module.add("BlpFieldError", py.get_type::<BlpFieldError>())?;
    module.add("BlpValidationError", py.get_type::<BlpValidationError>())?;
    module.add("BlpTimeoutError", py.get_type::<BlpTimeoutError>())?;
    module.add("BlpInternalError", py.get_type::<BlpInternalError>())?;
    Ok(())
}

fn py_error<E>(msg: impl Into<String>) -> PyErr
where
    E: pyo3::PyTypeInfo,
{
    PyErr::new::<E, _>(msg.into())
}

fn session_error(msg: impl Into<String>) -> PyErr {
    py_error::<BlpSessionError>(msg)
}

fn request_error(msg: impl Into<String>) -> PyErr {
    py_error::<BlpRequestError>(msg)
}

fn validation_error(msg: impl Into<String>) -> PyErr {
    py_error::<BlpValidationError>(msg)
}

fn timeout_error() -> PyErr {
    py_error::<BlpTimeoutError>("Request timed out")
}

fn internal_error(msg: impl Into<String>) -> PyErr {
    py_error::<BlpInternalError>(msg)
}

/// Convert BlpError to appropriate Python exception.
///
/// Maps each BlpError variant to the corresponding Python exception class,
/// preserving all structured error context (service, operation, cid, etc.).
pub(crate) fn blp_error_to_pyerr(e: BlpError) -> PyErr {
    match e {
        BlpError::SessionStart { source, label } => session_error(format_error_msg(
            "Session start failed",
            label.as_deref(),
            source.as_deref(),
        )),
        BlpError::OpenService {
            service,
            source,
            label,
        } => session_error(format!(
            "Failed to open service '{}': {}",
            service,
            format_error_msg("", label.as_deref(), source.as_deref())
        )),
        BlpError::RequestFailure {
            service,
            operation,
            cid,
            label,
            request_id,
            source,
        } => request_error(request_failure_message(
            &service,
            operation.as_deref(),
            cid.as_ref(),
            label.as_deref(),
            request_id.as_deref(),
            source.as_deref(),
        )),
        BlpError::InvalidArgument { detail } => {
            validation_error(format!("Invalid argument: {}", detail))
        }
        BlpError::Timeout => timeout_error(),
        BlpError::TemplateTerminated { cid } => {
            request_error(template_terminated_message(cid.as_ref()))
        }
        BlpError::SubscriptionFailure { cid, label } => {
            request_error(subscription_failure_message(cid.as_ref(), label.as_deref()))
        }
        BlpError::Internal { detail } => internal_error(format!("Internal error: {}", detail)),
        BlpError::SchemaOperationNotFound { service, operation } => {
            validation_error(format!("Operation not found: {}::{}", service, operation))
        }
        BlpError::SchemaElementNotFound { parent, name } => {
            validation_error(format!("Schema element not found: {}.{}", parent, name))
        }
        BlpError::SchemaTypeMismatch {
            element,
            expected,
            found,
        } => validation_error(format!(
            "Schema type mismatch at {}: expected {:?}, found {:?}",
            element, expected, found
        )),
        BlpError::SchemaUnsupported { element, detail } => validation_error(format!(
            "Unsupported schema construct at {}: {}",
            element, detail
        )),
        BlpError::Validation { message, errors } => {
            // Build detailed error message with suggestions
            let details: Vec<String> = errors
                .iter()
                .map(|e| {
                    if let Some(ref suggestion) = e.suggestion {
                        format!("{} (did you mean '{}'?)", e, suggestion)
                    } else {
                        e.to_string()
                    }
                })
                .collect();
            let msg = if details.is_empty() {
                message
            } else {
                format!("{}: {}", message, details.join("; "))
            };
            validation_error(msg)
        }
    }
}

/// Convert BlpAsyncError to appropriate Python exception.
pub(crate) fn blp_async_error_to_pyerr(e: BlpAsyncError) -> PyErr {
    match e {
        // Route structured BlpError through the full exception mapper
        BlpAsyncError::Blp(blp_err) | BlpAsyncError::BlpError(blp_err) => {
            blp_error_to_pyerr(blp_err)
        }

        BlpAsyncError::Internal(msg) => internal_error(msg),

        BlpAsyncError::ConfigError { detail } => {
            validation_error(format!("Configuration error: {}", detail))
        }
        BlpAsyncError::ChannelClosed => internal_error("Channel closed unexpectedly"),
        BlpAsyncError::StreamFull => internal_error("Stream buffer full - consumer too slow"),
        BlpAsyncError::Cancelled => request_error("Request was cancelled"),
        BlpAsyncError::Timeout => timeout_error(),
        BlpAsyncError::SessionLost {
            worker_id,
            in_flight_count,
        } => session_error(format!(
            "session lost on worker {} ({} in-flight requests failed)",
            worker_id, in_flight_count,
        )),
        BlpAsyncError::AllWorkersDown { pool_size } => session_error(format!(
            "all {} request workers are dead — no healthy worker available",
            pool_size,
        )),
    }
}

fn request_failure_message(
    service: &str,
    operation: Option<&str>,
    cid: Option<&CorrelationContext>,
    label: Option<&str>,
    request_id: Option<&str>,
    source: Option<&(dyn std::error::Error + Send + Sync)>,
) -> String {
    let mut msg = format!("Request failed on {}", service);
    if let Some(op) = operation {
        msg.push_str(&format!("::{}", op));
    }
    if let Some(c) = cid {
        msg.push_str(&format!(" (cid={})", c));
    }
    if let Some(rid) = request_id {
        msg.push_str(&format!(" [request_id={}]", rid));
    }
    if let Some(l) = label {
        msg.push_str(&format!(" - {}", l));
    }
    if let Some(s) = source {
        msg.push_str(&format!(": {}", s));
    }
    msg
}

fn template_terminated_message(cid: Option<&CorrelationContext>) -> String {
    match cid {
        Some(c) => format!("Request template terminated (cid={})", c),
        None => "Request template terminated".to_string(),
    }
}

fn subscription_failure_message(cid: Option<&CorrelationContext>, label: Option<&str>) -> String {
    let mut msg = "Subscription failed".to_string();
    if let Some(c) = cid {
        msg.push_str(&format!(" (cid={})", c));
    }
    if let Some(l) = label {
        msg.push_str(&format!(": {}", l));
    }
    msg
}

/// Helper to format error messages with optional label and source.
fn format_error_msg(
    base: &str,
    label: Option<&str>,
    source: Option<&(dyn std::error::Error + Send + Sync)>,
) -> String {
    let mut msg = base.to_string();
    if let Some(l) = label {
        if !msg.is_empty() {
            msg.push_str(": ");
        }
        msg.push_str(l);
    }
    if let Some(s) = source {
        if !msg.is_empty() {
            msg.push_str(" - ");
        }
        msg.push_str(&s.to_string());
    }
    if msg.is_empty() {
        "Unknown error".to_string()
    } else {
        msg
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;

    #[test]
    fn request_failure_maps_to_request_exception_with_full_context() {
        Python::initialize();

        let err = blp_error_to_pyerr(BlpError::RequestFailure {
            service: "refdata".to_string(),
            operation: Some("bdp".to_string()),
            cid: Some(CorrelationContext::Tag("cid-7".to_string())),
            label: Some("daily run".to_string()),
            request_id: Some("req-1".to_string()),
            source: Some(Box::new(io::Error::new(io::ErrorKind::Other, "boom"))),
        });

        assert_eq!(
            err.to_string(),
            "BlpRequestError: Request failed on refdata::bdp (cid=cid-7) [request_id=req-1] - daily run: boom"
        );
    }

    #[test]
    fn async_wrapped_core_errors_share_the_core_mapper() {
        Python::initialize();

        let wrapped = blp_async_error_to_pyerr(BlpAsyncError::Blp(BlpError::Timeout));
        let explicit = blp_async_error_to_pyerr(BlpAsyncError::BlpError(BlpError::Timeout));

        assert_eq!(wrapped.to_string(), "BlpTimeoutError: Request timed out");
        assert_eq!(explicit.to_string(), "BlpTimeoutError: Request timed out");
    }
}
