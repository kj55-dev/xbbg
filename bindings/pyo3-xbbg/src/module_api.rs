use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3_stub_gen::derive::*;

use crate::config::PyEngineConfig;
use crate::errors::register_exceptions;
use crate::subscription::PySubscription;
use crate::{ext, markets, recipes, PyEngine};

fn package_version() -> &'static str {
    let git_version = env!("VERGEN_GIT_DESCRIBE");
    git_version.strip_prefix('v').unwrap_or(git_version)
}

fn register_py_functions(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(version, module)?)?;
    module.add_function(wrap_pyfunction!(sdk_version, module)?)?;
    module.add_function(wrap_pyfunction!(set_log_level, module)?)?;
    module.add_function(wrap_pyfunction!(get_log_level, module)?)?;
    module.add_function(wrap_pyfunction!(enable_sdk_logging, module)?)?;
    Ok(())
}

fn register_py_classes(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyEngine>()?;
    module.add_class::<PyEngineConfig>()?;
    module.add_class::<PySubscription>()?;
    Ok(())
}

/// Convert Arrow RecordBatch to PyArrow RecordBatch using zero-copy FFI.
///
/// Uses Arrow's C Data Interface via ToPyArrow for zero-copy conversion.
pub(crate) fn record_batch_to_pyarrow(
    py: Python<'_>,
    batch: arrow::record_batch::RecordBatch,
) -> PyResult<Py<PyAny>> {
    use arrow::pyarrow::ToPyArrow;

    batch
        .to_pyarrow(py)
        .map(|b| b.unbind())
        .map_err(|e| PyRuntimeError::new_err(format!("Arrow FFI conversion failed: {e}")))
}

#[gen_stub_pyfunction]
#[pyfunction]
pub(crate) fn version() -> String {
    xbbg_core::version().to_string()
}

#[gen_stub_pyfunction]
#[pyfunction]
pub(crate) fn sdk_version() -> (i32, i32, i32, i32) {
    xbbg_core::sdk_version()
}

/// Set the Rust log level.
#[gen_stub_pyfunction]
#[pyfunction]
pub(crate) fn set_log_level(level: &str) -> PyResult<()> {
    let lvl = xbbg_log::parse_level(level).ok_or_else(|| {
        PyValueError::new_err(format!(
            "Invalid log level '{}'. Expected: trace, debug, info, warn, error",
            level
        ))
    })?;
    xbbg_log::set_level(lvl);
    Ok(())
}

/// Get the current Rust log level as a string.
#[gen_stub_pyfunction]
#[pyfunction]
pub(crate) fn get_log_level() -> &'static str {
    match xbbg_log::current_level() {
        xbbg_log::Level::TRACE => "trace",
        xbbg_log::Level::DEBUG => "debug",
        xbbg_log::Level::INFO => "info",
        xbbg_log::Level::WARN => "warn",
        xbbg_log::Level::ERROR => "error",
    }
}

#[gen_stub_pyfunction]
#[pyfunction]
pub(crate) fn enable_sdk_logging(level: &str) -> PyResult<()> {
    let lvl: xbbg_async::sdk_logging::SdkLogLevel = level
        .parse()
        .map_err(|e: String| PyValueError::new_err(e))?;
    xbbg_async::sdk_logging::register_sdk_logging(lvl);
    Ok(())
}

pub(crate) fn register_module(py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("__version__", package_version())?;
    register_py_functions(module)?;
    register_py_classes(module)?;
    register_exceptions(py, module)?;
    ext::register_ext_module(module)?;
    markets::register(module)?;
    recipes::register_recipes_module(module)?;
    Ok(())
}
