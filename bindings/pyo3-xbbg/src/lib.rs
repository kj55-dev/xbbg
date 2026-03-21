//! PyO3 bindings for xbbg Bloomberg engine.
//!
//! This module provides Python bindings for the Rust xbbg Engine,
//! exposing a generic `request()` method that accepts parameters from Python.
//!
//! # GIL Handling
//!
//! The async API releases the GIL during Bloomberg SDK operations:
//! - `future_into_py` schedules work on tokio (no GIL held)
//! - GIL is only acquired via `Python::attach()` for final Arrow conversion
//! - `py.detach()` releases GIL during blocking `Engine::start()`
//!
//! # Exception Mapping
//!
//! Rust errors are mapped to Python exceptions:
//! - `BlpError::SessionStart` → `BlpSessionError`
//! - `BlpError::OpenService` → `BlpSessionError`
//! - `BlpError::RequestFailure` → `BlpRequestError`
//! - `BlpError::Timeout` → `BlpTimeoutError`
//! - `BlpError::InvalidArgument` → `BlpValidationError`
//! - Other errors → `BlpInternalError`
//!
//! # Logging
//!
//! Rust tracing events are output to stderr via a non-blocking writer.
//! The log level is controlled from Python without any GIL acquisition:
//!
//! ```python
//! import xbbg
//! xbbg.set_log_level("debug")   # sets atomic level, no GIL on log path
//! xbbg.set_log_level("warn")    # default — quiet for end users
//! ```
//!
//! For per-crate control, set `RUST_LOG` before importing xbbg:
//!
//! ```bash
//! RUST_LOG=xbbg_core=trace,xbbg_async=debug python my_script.py
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use chrono::NaiveDate;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3_async_runtimes::tokio::future_into_py;
use pyo3_stub_gen::{define_stub_info_gatherer, derive::*};
use xbbg_log::{debug, info, warn};

#[cfg(test)]
use xbbg_async::engine::state::SubscriptionMetrics;
use xbbg_async::engine::{Engine, EngineConfig, SubscriptionRecoveryPolicy};
use xbbg_async::OverflowPolicy;
#[cfg(test)]
use xbbg_core::AuthConfig;
use xbbg_ext::{ExchangeInfo, MarketInfo, MarketTiming};

mod config;
mod errors;
mod ext;
mod markets;
mod module_api;
mod recipes;
mod request;
mod subscription;

#[cfg(test)]
use config::build_auth_config;
use config::PyEngineConfig;
use errors::{blp_async_error_to_pyerr, blp_error_to_pyerr};
use module_api::record_batch_to_pyarrow;
use request::dict_to_request_params;
use subscription::py_subscription_from_stream;
#[cfg(test)]
use subscription::{subscription_metrics_totals, SubscriptionMetricsMap};

fn exchange_info_to_py(py: Python<'_>, info: &ExchangeInfo) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    dict.set_item("ticker", &info.ticker)?;
    dict.set_item("mic", info.mic.clone())?;
    dict.set_item("exch_code", info.exch_code.clone())?;
    dict.set_item("timezone", &info.timezone)?;
    dict.set_item("utc_offset", info.utc_offset)?;
    dict.set_item("source", info.source.as_str())?;
    dict.set_item("day", info.sessions.day.clone())?;
    dict.set_item("allday", info.sessions.allday.clone())?;
    dict.set_item("pre", info.sessions.pre.clone())?;
    dict.set_item("post", info.sessions.post.clone())?;
    dict.set_item("am", info.sessions.am.clone())?;
    dict.set_item("pm", info.sessions.pm.clone())?;
    Ok(dict.into_any().unbind())
}

fn market_info_to_py(py: Python<'_>, info: &MarketInfo) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    dict.set_item("exch", info.exch.clone())?;
    dict.set_item("tz", info.tz.clone())?;
    dict.set_item("freq", info.freq.clone())?;
    dict.set_item("is_fut", info.is_fut)?;
    Ok(dict.into_any().unbind())
}

/// Python wrapper for the xbbg Engine.
#[gen_stub_pyclass]
#[pyclass]
pub(crate) struct PyEngine {
    engine: Arc<Engine>,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyEngine {
    /// Create a new Engine with optional host/port configuration.
    ///
    /// This blocks while connecting to Bloomberg. GIL is released during connection.
    /// For more configuration options, use `Engine.with_config()`.
    #[new]
    #[pyo3(signature = (host="localhost", port=8194))]
    fn new(py: Python<'_>, host: &str, port: u16) -> PyResult<Self> {
        info!(
            host = host,
            port = port,
            "PyEngine: connecting to Bloomberg"
        );

        let config = EngineConfig {
            server_host: host.to_string(),
            server_port: port,
            ..Default::default()
        };

        Self::start_engine(py, config)
    }

    /// Create a new Engine with full configuration.
    ///
    /// This blocks while connecting to Bloomberg. GIL is released during connection.
    ///
    /// Example:
    /// ```python
    /// config = EngineConfig(
    ///     host="localhost",
    ///     port=8194,
    ///     request_pool_size=4,
    ///     subscription_pool_size=8,
    ///     overflow_policy="drop_newest",
    /// )
    /// engine = Engine.with_config(config)
    /// ```
    #[staticmethod]
    fn with_config(py: Python<'_>, config: &PyEngineConfig) -> PyResult<Self> {
        info!(
            host = %config.host,
            port = config.port,
            request_pool_size = config.request_pool_size,
            subscription_pool_size = config.subscription_pool_size,
            "PyEngine: connecting with custom config"
        );

        let rust_config: EngineConfig = config.try_into()?;

        Self::start_engine(py, rust_config)
    }

    // =========================================================================
    // Generic Request API
    // =========================================================================

    /// Generic async Bloomberg request.
    ///
    /// Accepts a dictionary of parameters and returns a PyArrow RecordBatch.
    ///
    /// Required keys:
    /// - service: Bloomberg service URI (e.g., "//blp/refdata")
    /// - operation: Request operation name (e.g., "ReferenceDataRequest")
    ///   Use "" / Operation.RAW_REQUEST together with request_operation for raw mode.
    ///
    /// Optional keys:
    /// - extractor: Extractor type hint (e.g., "refdata", "histdata", "intraday_bar")
    ///   If omitted, Rust resolves a default from `operation`.
    /// - request_operation: Actual Bloomberg operation name when operation=""
    ///
    /// Optional keys (depend on request type):
    /// - securities: List of security identifiers
    /// - security: Single security identifier
    /// - fields: List of field names
    /// - overrides: List of (name, value) tuples
    /// - start_date, end_date: For historical requests
    /// - start_datetime, end_datetime: For intraday requests
    /// - event_type: For intraday bars (TRADE, BID, ASK)
    /// - interval: Bar interval in minutes
    /// - options: Additional Bloomberg options
    #[pyo3(signature = (params))]
    fn request<'py>(
        &self,
        py: Python<'py>,
        params: &Bound<'py, PyDict>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.engine.clone();

        // Extract and convert params to Rust struct
        let rust_params = dict_to_request_params(params)?;

        debug!(
            service = %rust_params.service,
            operation = %rust_params.operation,
            extractor = ?rust_params.extractor,
            securities = ?rust_params.securities,
            fields = ?rust_params.fields,
            "PyEngine: sending request"
        );

        future_into_py(py, async move {
            let batch = engine.request(rust_params).await.map_err(|e| {
                warn!(error = %e, "PyEngine: request failed");
                blp_async_error_to_pyerr(e)
            })?;

            debug!(num_rows = batch.num_rows(), "PyEngine: request completed");

            Python::attach(|py| record_batch_to_pyarrow(py, batch))
        })
    }

    /// Resolve exchange metadata using override -> cache -> Bloomberg waterfall.
    fn resolve_exchange<'py>(
        &self,
        py: Python<'py>,
        ticker: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.engine.clone();
        future_into_py(py, async move {
            let info = engine.resolve_exchange(&ticker).await;
            Python::attach(|py| exchange_info_to_py(py, &info))
        })
    }

    /// Fetch market-level metadata (exchange, timezone, futures cycle info).
    fn fetch_market_info<'py>(
        &self,
        py: Python<'py>,
        ticker: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.engine.clone();
        future_into_py(py, async move {
            let info = engine
                .fetch_market_info(&ticker)
                .await
                .map_err(blp_async_error_to_pyerr)?;
            Python::attach(|py| market_info_to_py(py, &info))
        })
    }

    /// Resolve market timing (BOD/EOD/FINISHED) for a ticker/date.
    #[pyo3(signature = (ticker, date, timing="EOD", tz=None))]
    fn market_timing<'py>(
        &self,
        py: Python<'py>,
        ticker: String,
        date: String,
        timing: &str,
        tz: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.engine.clone();
        let timing = MarketTiming::parse(timing)
            .ok_or_else(|| PyValueError::new_err("timing must be one of: BOD, EOD, FINISHED"))?;
        let date = NaiveDate::parse_from_str(&date, "%Y-%m-%d")
            .map_err(|_| PyValueError::new_err("date must be YYYY-MM-DD"))?;

        future_into_py(py, async move {
            let value = engine
                .resolve_market_timing(&ticker, date, timing, tz.as_deref())
                .await
                .map_err(blp_async_error_to_pyerr)?;
            Python::attach(|py| Ok(value.into_pyobject(py)?.into_any().unbind()))
        })
    }

    /// Invalidate exchange cache (one ticker or all entries).
    #[pyo3(signature = (ticker=None))]
    fn invalidate_exchange_cache(&self, ticker: Option<String>) -> PyResult<()> {
        self.engine
            .invalidate_exchange_cache(ticker.as_deref())
            .map_err(PyRuntimeError::new_err)
    }

    /// Persist exchange cache to disk.
    fn save_exchange_cache(&self, py: Python<'_>) -> PyResult<()> {
        let engine = self.engine.clone();
        py.detach(move || engine.save_exchange_cache())
            .map_err(PyRuntimeError::new_err)
    }

    // =========================================================================
    // Field Type Resolution API
    // =========================================================================

    /// Resolve field types for a list of fields.
    #[pyo3(signature = (fields, overrides=None, default_type="string"))]
    fn resolve_field_types<'py>(
        &self,
        py: Python<'py>,
        fields: Vec<String>,
        overrides: Option<HashMap<String, String>>,
        default_type: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.engine.clone();
        let default = default_type.to_string();

        future_into_py(py, async move {
            let resolved = engine
                .resolve_field_types(&fields, overrides.as_ref(), &default)
                .await
                .map_err(blp_async_error_to_pyerr)?;

            Python::attach(|py| {
                let dict = PyDict::new(py);
                for (k, v) in resolved {
                    dict.set_item(k, v)?;
                }
                Ok(dict.into_any().unbind())
            })
        })
    }

    /// Get field info from cache.
    fn get_field_info(&self, field: &str) -> Option<HashMap<String, String>> {
        self.engine.get_field_info(field).map(|info| {
            let mut map = HashMap::new();
            map.insert("field_id".to_string(), info.field_id);
            map.insert("arrow_type".to_string(), info.arrow_type);
            map.insert("description".to_string(), info.description);
            map.insert("category".to_string(), info.category);
            map
        })
    }

    /// Clear the field type cache.
    fn clear_field_cache(&self) {
        self.engine.clear_field_cache();
    }

    /// Save the field type cache to disk.
    fn save_field_cache(&self, py: Python<'_>) -> PyResult<()> {
        let engine = self.engine.clone();
        py.detach(move || engine.save_field_cache())
            .map_err(PyRuntimeError::new_err)
    }

    /// Get field cache statistics including the active cache path.
    fn field_cache_stats(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let (entry_count, cache_path) = self.engine.field_cache_stats();
        let dict = PyDict::new(py);
        dict.set_item("entry_count", entry_count)?;
        dict.set_item("cache_path", cache_path.to_string_lossy().into_owned())?;
        Ok(dict.into())
    }

    /// Validate Bloomberg field names.
    ///
    /// Queries Bloomberg's field info service to check if the given fields exist.
    /// Returns a list of invalid field names (fields that Bloomberg doesn't recognize).
    ///
    /// Example:
    ///     invalid = await engine.validate_fields(["PX_LAST", "INVALID_FIELD"])
    ///     # invalid = ["INVALID_FIELD"]
    fn validate_fields<'py>(
        &self,
        py: Python<'py>,
        fields: Vec<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.engine.clone();

        future_into_py(py, async move {
            let invalid = engine
                .validate_fields(&fields)
                .await
                .map_err(blp_async_error_to_pyerr)?;

            Python::attach(|py| Ok(invalid.into_pyobject(py)?.into_any().unbind()))
        })
    }

    // =========================================================================
    // Schema Cache API
    // =========================================================================

    /// Get service schema (from cache or introspect).
    ///
    /// Returns a dictionary with schema information including operations.
    /// First checks disk cache; if not cached, introspects the service.
    #[pyo3(signature = (service))]
    fn get_schema<'py>(&self, py: Python<'py>, service: String) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.engine.clone();

        future_into_py(py, async move {
            let schema = engine
                .get_schema(&service)
                .await
                .map_err(blp_async_error_to_pyerr)?;

            // Convert to JSON string for Python (dereference Arc)
            let json = serde_json::to_string(&*schema)
                .map_err(|e| PyRuntimeError::new_err(format!("serialize schema: {e}")))?;

            Python::attach(|py| Ok(json.into_pyobject(py)?.into_any().unbind()))
        })
    }

    /// Get a specific operation schema.
    ///
    /// Returns operation details including request/response element definitions.
    #[pyo3(signature = (service, operation))]
    fn get_operation<'py>(
        &self,
        py: Python<'py>,
        service: String,
        operation: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.engine.clone();

        future_into_py(py, async move {
            let op = engine
                .get_operation(&service, &operation)
                .await
                .map_err(blp_async_error_to_pyerr)?;

            let json = serde_json::to_string(&op)
                .map_err(|e| PyRuntimeError::new_err(format!("serialize operation: {e}")))?;

            Python::attach(|py| Ok(json.into_pyobject(py)?.into_any().unbind()))
        })
    }

    /// List all operations for a service.
    #[pyo3(signature = (service))]
    fn list_operations<'py>(
        &self,
        py: Python<'py>,
        service: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.engine.clone();

        future_into_py(py, async move {
            let ops = engine
                .list_operations(&service)
                .await
                .map_err(blp_async_error_to_pyerr)?;

            Python::attach(|py| {
                let list = pyo3::types::PyList::new(py, ops)?;
                Ok(list.into_any().unbind())
            })
        })
    }

    /// Get cached schema without introspection.
    ///
    /// Returns None if the schema is not cached.
    fn get_cached_schema(&self, service: &str) -> Option<String> {
        self.engine
            .get_cached_schema(service)
            .and_then(|s| serde_json::to_string(&*s).ok())
    }

    /// Invalidate a cached schema.
    fn invalidate_schema(&self, service: &str) {
        self.engine.invalidate_schema(service);
    }

    /// Clear all cached schemas.
    fn clear_schema_cache(&self) {
        self.engine.clear_schema_cache();
    }

    /// List all cached service URIs.
    fn list_cached_schemas(&self) -> Vec<String> {
        self.engine.list_cached_schemas()
    }

    // =========================================================================
    // Schema Validation API
    // =========================================================================

    /// Get valid enum values for an element.
    ///
    /// Returns a list of valid enum values, or None if the element is not an enum.
    #[pyo3(signature = (service, operation, element))]
    fn get_enum_values<'py>(
        &self,
        py: Python<'py>,
        service: String,
        operation: String,
        element: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.engine.clone();

        future_into_py(py, async move {
            let values = engine
                .get_enum_values(&service, &operation, &element)
                .await
                .map_err(blp_async_error_to_pyerr)?;

            Python::attach(|py| match values {
                Some(v) => {
                    let list = pyo3::types::PyList::new(py, v)?;
                    Ok(list.into_any().unbind())
                }
                None => Ok(py.None()),
            })
        })
    }

    /// List all valid element names for an operation.
    #[pyo3(signature = (service, operation))]
    fn list_valid_elements<'py>(
        &self,
        py: Python<'py>,
        service: String,
        operation: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.engine.clone();

        future_into_py(py, async move {
            let elements = engine
                .list_valid_elements(&service, &operation)
                .await
                .map_err(blp_async_error_to_pyerr)?;

            Python::attach(|py| match elements {
                Some(v) => {
                    let list = pyo3::types::PyList::new(py, v)?;
                    Ok(list.into_any().unbind())
                }
                None => Ok(py.None()),
            })
        })
    }

    // =========================================================================
    // Subscription API
    // =========================================================================

    /// Subscribe to real-time market data.
    ///
    /// Returns a PySubscription that supports async iteration and dynamic add/remove.
    /// GIL is released during async operations; iteration and add/remove use separate
    /// locks to avoid contention.
    ///
    /// Example:
    /// ```python
    /// sub = await engine.subscribe(['AAPL US Equity'], ['LAST_PRICE', 'BID', 'ASK'])
    /// async for batch in sub:
    ///     print(batch)
    /// await sub.unsubscribe()
    /// ```
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (tickers, fields, flush_threshold=None, overflow_policy=None, stream_capacity=None, recovery_policy=None))]
    fn subscribe<'py>(
        &self,
        py: Python<'py>,
        tickers: Vec<String>,
        fields: Vec<String>,
        flush_threshold: Option<usize>,
        overflow_policy: Option<String>,
        stream_capacity: Option<usize>,
        recovery_policy: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.engine.clone();
        let tickers_clone = tickers.clone();
        let fields_clone = fields.clone();

        let op = overflow_policy.as_deref().map(|s| match s {
            "drop_oldest" => OverflowPolicy::DropOldest,
            "block" => OverflowPolicy::Block,
            _ => OverflowPolicy::DropNewest,
        });
        let recovery = recovery_policy.as_deref().map(|s| match s {
            "resubscribe" => SubscriptionRecoveryPolicy::Resubscribe,
            _ => SubscriptionRecoveryPolicy::None,
        });

        debug!(
            tickers = ?tickers,
            fields = ?fields,
            "PyEngine: creating subscription"
        );

        future_into_py(py, async move {
            let stream = engine
                .subscribe_with_options(
                    "//blp/mktdata".to_string(),
                    tickers_clone.clone(),
                    fields_clone.clone(),
                    vec![],
                    stream_capacity,
                    flush_threshold,
                    op,
                    recovery,
                )
                .await
                .map_err(blp_async_error_to_pyerr)?;

            debug!("PyEngine: subscription created");

            Python::attach(|py| {
                py_subscription_from_stream(py, stream, fields_clone, stream_capacity)
            })
        })
    }

    /// Subscribe to real-time data with custom service and options.
    ///
    /// This is the generic subscription method for services like //blp/mktvwap.
    ///
    /// Args:
    ///     service: Bloomberg service URI (e.g., "//blp/mktvwap")
    ///     tickers: List of securities to subscribe to
    ///     fields: List of fields to subscribe to
    ///     options: List of subscription options (e.g., ["VWAP_START_TIME=09:30"])
    ///
    /// Example:
    /// ```python
    /// sub = await engine.subscribe_with_options(
    ///     '//blp/mktvwap',
    ///     ['AAPL US Equity'],
    ///     ['RT_PX_VWAP', 'RT_VWAP_VOLUME'],
    ///     ['VWAP_START_TIME=09:30', 'VWAP_END_TIME=16:00']
    /// )
    /// async for batch in sub:
    ///     print(batch)
    /// ```
    #[pyo3(signature = (service, tickers, fields, options=None, flush_threshold=None, overflow_policy=None, stream_capacity=None, recovery_policy=None))]
    #[allow(clippy::too_many_arguments)]
    fn subscribe_with_options<'py>(
        &self,
        py: Python<'py>,
        service: String,
        tickers: Vec<String>,
        fields: Vec<String>,
        options: Option<Vec<String>>,
        flush_threshold: Option<usize>,
        overflow_policy: Option<String>,
        stream_capacity: Option<usize>,
        recovery_policy: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.engine.clone();
        let tickers_clone = tickers.clone();
        let fields_clone = fields.clone();
        let options_clone = options.clone().unwrap_or_default();
        let service_clone = service.clone();

        let op = overflow_policy.as_deref().map(|s| match s {
            "drop_oldest" => OverflowPolicy::DropOldest,
            "block" => OverflowPolicy::Block,
            _ => OverflowPolicy::DropNewest,
        });
        let recovery = recovery_policy.as_deref().map(|s| match s {
            "resubscribe" => SubscriptionRecoveryPolicy::Resubscribe,
            _ => SubscriptionRecoveryPolicy::None,
        });

        debug!(
            service = %service,
            tickers = ?tickers,
            fields = ?fields,
            options = ?options,
            "PyEngine: creating subscription with options"
        );

        future_into_py(py, async move {
            let stream = engine
                .subscribe_with_options(
                    service_clone.clone(),
                    tickers_clone.clone(),
                    fields_clone.clone(),
                    options_clone.clone(),
                    stream_capacity,
                    flush_threshold,
                    op,
                    recovery,
                )
                .await
                .map_err(blp_async_error_to_pyerr)?;

            debug!("PyEngine: subscription with options created");

            Python::attach(|py| {
                py_subscription_from_stream(py, stream, fields_clone, stream_capacity)
            })
        })
    }

    // =========================================================================
    // Lifecycle Management
    // =========================================================================

    /// Signal engine shutdown (non-blocking).
    ///
    /// Signals all worker threads to stop. They will terminate when they
    /// finish their current work or see the shutdown signal.
    ///
    /// This is called automatically during Python interpreter shutdown via atexit.
    /// You usually don't need to call this directly.
    fn signal_shutdown(&self) {
        info!("PyEngine: signal_shutdown called");
        self.engine.signal_shutdown();
    }

    fn worker_health(&self) -> PyResult<Vec<(usize, String)>> {
        // TODO: replace with self.engine.request_pool_health() once Engine exposes it.
        Ok(Vec::new())
    }

    /// Check if engine is available.
    ///
    /// Returns True if the engine exists. Note that this doesn't guarantee
    /// Bloomberg is still connected - a request might still fail.
    fn is_available(&self) -> bool {
        // Engine exists if we have it
        true
    }
}

impl PyEngine {
    /// Shared helper: release GIL and start Engine on a blocking thread.
    #[allow(clippy::result_large_err)]
    fn start_engine(py: Python<'_>, config: EngineConfig) -> PyResult<Self> {
        // Release GIL during blocking Engine::start().
        // Engine::start() creates Bloomberg sessions and waits for them to connect,
        // which can take seconds — must not hold GIL during this.
        let engine = py.detach(|| Engine::start(config)).map_err(|e| {
            warn!(error = %e, "PyEngine: connection failed");
            blp_async_error_to_pyerr(e)
        })?;

        info!("PyEngine: connected successfully");

        Ok(Self {
            engine: Arc::new(engine),
        })
    }
}

#[pymodule]
#[pyo3(name = "_core")]
fn _core(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    xbbg_log::init();

    info!("xbbg._core module initialized");
    module_api::register_module(_py, m)?;
    Ok(())
}

// =============================================================================
// Logging control — Python-facing functions
// =============================================================================

define_stub_info_gatherer!(stub_info);

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicU64};

    fn metrics(
        messages_received: u64,
        dropped_batches: u64,
        batches_sent: u64,
        slow_consumer: bool,
    ) -> Arc<SubscriptionMetrics> {
        Arc::new(SubscriptionMetrics {
            messages_received: Arc::new(AtomicU64::new(messages_received)),
            dropped_batches: Arc::new(AtomicU64::new(dropped_batches)),
            batches_sent: Arc::new(AtomicU64::new(batches_sent)),
            slow_consumer: Arc::new(AtomicBool::new(slow_consumer)),
            data_loss_events: Arc::new(AtomicU64::new(0)),
            last_message_us: Arc::new(AtomicU64::new(0)),
            last_data_loss_us: Arc::new(AtomicU64::new(0)),
        })
    }

    #[test]
    fn subscription_metrics_totals_only_counts_active_entries() {
        let mut metrics_map = SubscriptionMetricsMap::new();
        metrics_map.insert(10, metrics(5, 1, 4, false));
        metrics_map.insert(11, metrics(7, 2, 6, true));

        metrics_map.remove(&10);

        assert_eq!(
            subscription_metrics_totals(&metrics_map),
            (7, 2, 6, true, 0, 0, 0)
        );
    }

    #[test]
    fn py_engine_config_defaults_include_auth_defaults() {
        let config = PyEngineConfig::new(None).expect("default config");
        assert_eq!(config.auth_method, None);
        assert_eq!(config.num_start_attempts, 3);
        assert!(config.auto_restart_on_disconnection);
    }

    #[test]
    fn py_engine_config_maps_manual_auth_to_engine_config() {
        Python::initialize();
        Python::attach(|py| {
            let kwargs = PyDict::new(py);
            kwargs
                .set_item("auth_method", "manual")
                .expect("auth_method");
            kwargs.set_item("app_name", "my-app").expect("app_name");
            kwargs.set_item("user_id", "123456").expect("user_id");
            kwargs
                .set_item("ip_address", "10.0.0.1")
                .expect("ip_address");

            let config = PyEngineConfig::new(Some(&kwargs)).expect("manual auth config");
            let engine_config: EngineConfig = (&config).try_into().expect("engine config");

            assert_eq!(
                engine_config.auth,
                Some(AuthConfig::Manual {
                    app_name: "my-app".to_string(),
                    user_id: "123456".to_string(),
                    ip_address: "10.0.0.1".to_string(),
                })
            );
        });
    }

    #[test]
    fn py_engine_config_rejects_missing_auth_fields() {
        Python::initialize();
        Python::attach(|py| {
            let kwargs = PyDict::new(py);
            kwargs.set_item("auth_method", "app").expect("auth_method");

            let config = PyEngineConfig::new(Some(&kwargs)).expect("partial auth config");
            let err = match EngineConfig::try_from(&config) {
                Ok(_) => panic!("missing app_name should fail"),
                Err(err) => err,
            };
            assert!(err.to_string().contains("app_name is required"));
        });
    }

    #[test]
    fn build_auth_config_supports_all_auth_methods() {
        let mut config = PyEngineConfig::new(None).expect("default config");

        config.auth_method = Some("user".to_string());
        assert_eq!(
            build_auth_config(&config).expect("user auth"),
            Some(AuthConfig::User)
        );

        config.auth_method = Some("app".to_string());
        config.app_name = Some("app-name".to_string());
        assert_eq!(
            build_auth_config(&config).expect("app auth"),
            Some(AuthConfig::App {
                app_name: "app-name".to_string(),
            })
        );

        config.auth_method = Some("userapp".to_string());
        assert_eq!(
            build_auth_config(&config).expect("userapp auth"),
            Some(AuthConfig::UserApp {
                app_name: "app-name".to_string(),
            })
        );

        config.auth_method = Some("dir".to_string());
        config.dir_property = Some("mail=jane@example.com".to_string());
        assert_eq!(
            build_auth_config(&config).expect("dir auth"),
            Some(AuthConfig::Directory {
                property_name: "mail=jane@example.com".to_string(),
            })
        );

        config.auth_method = Some("token".to_string());
        config.token = Some("tok-123".to_string());
        assert_eq!(
            build_auth_config(&config).expect("token auth"),
            Some(AuthConfig::Token {
                token: "tok-123".to_string(),
            })
        );
    }
}
