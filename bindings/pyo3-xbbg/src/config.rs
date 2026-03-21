use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3_stub_gen::derive::*;

use xbbg_async::engine::{EngineConfig, RetryPolicy};
use xbbg_async::{OverflowPolicy, ValidationMode};
use xbbg_core::AuthConfig;

/// Python configuration for the xbbg Engine.
///
/// All settings have sensible defaults - you only need to specify what you want to change.
///
/// The defaults are derived from `EngineConfig::default()` in xbbg-async, so they
/// stay in sync automatically.
#[gen_stub_pyclass]
#[pyclass]
#[derive(Clone)]
pub struct PyEngineConfig {
    /// Bloomberg server host (default: "localhost")
    #[pyo3(get, set)]
    pub host: String,
    /// Bloomberg server port (default: 8194)
    #[pyo3(get, set)]
    pub port: u16,
    /// Multiple servers for failover: list of (host, port) tuples. Overrides host/port when set.
    #[pyo3(get, set)]
    pub servers: Vec<(String, u16)>,
    #[pyo3(get, set)]
    pub zfp_remote: Option<String>,
    /// Number of pre-warmed request workers (default: 2)
    #[pyo3(get, set)]
    pub request_pool_size: usize,
    /// Number of pre-warmed subscription sessions (default: 1)
    #[pyo3(get, set)]
    pub subscription_pool_size: usize,
    /// Validation mode: "disabled" (default), "strict", or "lenient"
    #[pyo3(get, set)]
    pub validation_mode: String,
    /// Number of ticks to buffer before flushing to Python (default: 1)
    #[pyo3(get, set)]
    pub subscription_flush_threshold: usize,
    /// Bloomberg SDK event queue size (default: 10000)
    #[pyo3(get, set)]
    pub max_event_queue_size: usize,
    /// Internal command channel capacity (default: 256)
    #[pyo3(get, set)]
    pub command_queue_size: usize,
    /// Subscription stream backpressure capacity (default: 256)
    #[pyo3(get, set)]
    pub subscription_stream_capacity: usize,
    /// Overflow policy for slow consumers: "drop_newest" (default), "drop_oldest", "block"
    #[pyo3(get, set)]
    pub overflow_policy: String,
    /// Services to pre-warm on startup (default: ["//blp/refdata", "//blp/apiflds"])
    #[pyo3(get, set)]
    pub warmup_services: Vec<String>,
    /// Custom path for field cache JSON file (default: ~/.xbbg/field_cache.json)
    /// Set to None to use the default path.
    #[pyo3(get, set)]
    pub field_cache_path: Option<String>,
    /// Optional auth method: "user", "app", "userapp", "dir", "manual", or "token".
    #[pyo3(get, set)]
    pub auth_method: Option<String>,
    /// Bloomberg application name for app/userapp/manual auth.
    #[pyo3(get, set)]
    pub app_name: Option<String>,
    /// Active Directory property for dir auth.
    #[pyo3(get, set)]
    pub dir_property: Option<String>,
    /// Manual Bloomberg user id for manual auth.
    #[pyo3(get, set)]
    pub user_id: Option<String>,
    /// Manual Bloomberg ip address for manual auth.
    #[pyo3(get, set)]
    pub ip_address: Option<String>,
    #[pyo3(get, set)]
    pub token: Option<String>,
    #[pyo3(get, set)]
    pub tls_client_credentials: Option<String>,
    #[pyo3(get, set)]
    pub tls_client_credentials_password: Option<String>,
    #[pyo3(get, set)]
    pub tls_trust_material: Option<String>,
    #[pyo3(get, set)]
    pub tls_handshake_timeout_ms: Option<i32>,
    #[pyo3(get, set)]
    pub tls_crl_fetch_timeout_ms: Option<i32>,
    #[pyo3(get, set)]
    pub num_start_attempts: usize,
    /// Whether Bloomberg should auto-restart the session on disconnect (default: True).
    #[pyo3(get, set)]
    pub auto_restart_on_disconnection: bool,
    #[pyo3(get, set)]
    pub max_recovery_attempts: usize,
    #[pyo3(get, set)]
    pub recovery_timeout_ms: u64,
    #[pyo3(get, set)]
    pub retry_max_retries: u32,
    #[pyo3(get, set)]
    pub retry_initial_delay_ms: u64,
    #[pyo3(get, set)]
    pub retry_backoff_factor: f64,
    #[pyo3(get, set)]
    pub retry_max_delay_ms: u64,
    #[pyo3(get, set)]
    pub health_check_interval_ms: u64,
    #[pyo3(get, set)]
    pub sdk_log_level: String,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyEngineConfig {
    /// Create a new configuration with defaults.
    ///
    /// All defaults are derived from the Rust EngineConfig to stay in sync.
    #[new]
    #[pyo3(signature = (**kwargs))]
    pub(crate) fn new(kwargs: Option<&Bound<'_, PyDict>>) -> PyResult<Self> {
        let defaults = EngineConfig::default();
        let mut config = Self {
            host: defaults.server_host,
            port: defaults.server_port,
            servers: Vec::new(),
            zfp_remote: None,
            request_pool_size: defaults.request_pool_size,
            subscription_pool_size: defaults.subscription_pool_size,
            validation_mode: defaults.validation_mode.to_string(),
            subscription_flush_threshold: defaults.subscription_flush_threshold,
            max_event_queue_size: defaults.max_event_queue_size,
            command_queue_size: defaults.command_queue_size,
            subscription_stream_capacity: defaults.subscription_stream_capacity,
            overflow_policy: defaults.overflow_policy.to_string(),
            warmup_services: defaults.warmup_services,
            field_cache_path: None,
            auth_method: None,
            app_name: None,
            dir_property: None,
            user_id: None,
            ip_address: None,
            token: None,
            tls_client_credentials: None,
            tls_client_credentials_password: None,
            tls_trust_material: None,
            tls_handshake_timeout_ms: None,
            tls_crl_fetch_timeout_ms: None,
            num_start_attempts: defaults.num_start_attempts,
            auto_restart_on_disconnection: defaults.auto_restart_on_disconnection,
            max_recovery_attempts: 3,
            recovery_timeout_ms: 30_000,
            retry_max_retries: 0,
            retry_initial_delay_ms: 1000,
            retry_backoff_factor: 2.0,
            retry_max_delay_ms: 30_000,
            health_check_interval_ms: 30_000,
            sdk_log_level: "off".to_string(),
        };

        if let Some(kw) = kwargs {
            apply_kw(kw, "host", &mut config.host)?;
            apply_kw(kw, "port", &mut config.port)?;
            apply_kw(kw, "servers", &mut config.servers)?;
            apply_kw(kw, "zfp_remote", &mut config.zfp_remote)?;
            apply_kw(kw, "request_pool_size", &mut config.request_pool_size)?;
            apply_kw(
                kw,
                "subscription_pool_size",
                &mut config.subscription_pool_size,
            )?;
            apply_kw(kw, "validation_mode", &mut config.validation_mode)?;
            apply_kw(
                kw,
                "subscription_flush_threshold",
                &mut config.subscription_flush_threshold,
            )?;
            apply_kw(kw, "max_event_queue_size", &mut config.max_event_queue_size)?;
            apply_kw(kw, "command_queue_size", &mut config.command_queue_size)?;
            apply_kw(
                kw,
                "subscription_stream_capacity",
                &mut config.subscription_stream_capacity,
            )?;
            apply_kw(kw, "overflow_policy", &mut config.overflow_policy)?;
            apply_kw(kw, "warmup_services", &mut config.warmup_services)?;
            apply_kw(kw, "field_cache_path", &mut config.field_cache_path)?;
            apply_kw(kw, "auth_method", &mut config.auth_method)?;
            apply_kw(kw, "app_name", &mut config.app_name)?;
            apply_kw(kw, "dir_property", &mut config.dir_property)?;
            apply_kw(kw, "user_id", &mut config.user_id)?;
            apply_kw(kw, "ip_address", &mut config.ip_address)?;
            apply_kw(kw, "token", &mut config.token)?;
            apply_kw(
                kw,
                "tls_client_credentials",
                &mut config.tls_client_credentials,
            )?;
            apply_kw(
                kw,
                "tls_client_credentials_password",
                &mut config.tls_client_credentials_password,
            )?;
            apply_kw(kw, "tls_trust_material", &mut config.tls_trust_material)?;
            apply_kw(
                kw,
                "tls_handshake_timeout_ms",
                &mut config.tls_handshake_timeout_ms,
            )?;
            apply_kw(
                kw,
                "tls_crl_fetch_timeout_ms",
                &mut config.tls_crl_fetch_timeout_ms,
            )?;
            apply_kw(kw, "num_start_attempts", &mut config.num_start_attempts)?;
            apply_kw(
                kw,
                "auto_restart_on_disconnection",
                &mut config.auto_restart_on_disconnection,
            )?;
            apply_kw(
                kw,
                "max_recovery_attempts",
                &mut config.max_recovery_attempts,
            )?;
            apply_kw(kw, "recovery_timeout_ms", &mut config.recovery_timeout_ms)?;
            apply_kw(kw, "retry_max_retries", &mut config.retry_max_retries)?;
            apply_kw(
                kw,
                "retry_initial_delay_ms",
                &mut config.retry_initial_delay_ms,
            )?;
            apply_kw(kw, "retry_backoff_factor", &mut config.retry_backoff_factor)?;
            apply_kw(kw, "retry_max_delay_ms", &mut config.retry_max_delay_ms)?;
            apply_kw(
                kw,
                "health_check_interval_ms",
                &mut config.health_check_interval_ms,
            )?;
            apply_kw(kw, "sdk_log_level", &mut config.sdk_log_level)?;
        }

        Ok(config)
    }

    fn __repr__(&self) -> String {
        let fcp_display = self.field_cache_path.as_deref().unwrap_or("default");
        let auth_method = self.auth_method.as_deref().unwrap_or("none");
        format!(
            "EngineConfig(host='{}', port={}, request_pool_size={}, subscription_pool_size={}, \
             validation_mode='{}', overflow_policy='{}', auth_method='{}', field_cache_path='{}', warmup_services={:?})",
            self.host,
            self.port,
            self.request_pool_size,
            self.subscription_pool_size,
            self.validation_mode,
            self.overflow_policy,
            auth_method,
            fcp_display,
            self.warmup_services
        )
    }
}

fn apply_kw<'py, T>(kw: &Bound<'py, PyDict>, key: &str, target: &mut T) -> PyResult<()>
where
    T: FromPyObject<'py>,
{
    if let Some(value) = kw.get_item(key)? {
        *target = value.extract()?;
    }
    Ok(())
}
fn require_auth_value(value: &Option<String>, field: &str, method: &str) -> PyResult<String> {
    value
        .clone()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            PyValueError::new_err(format!("{field} is required for auth_method='{method}'"))
        })
}

pub(crate) fn build_auth_config(py_config: &PyEngineConfig) -> PyResult<Option<AuthConfig>> {
    let method = match py_config.auth_method.as_deref() {
        None => {
            if py_config.app_name.is_some()
                || py_config.dir_property.is_some()
                || py_config.user_id.is_some()
                || py_config.ip_address.is_some()
                || py_config.token.is_some()
            {
                return Err(PyValueError::new_err(
                    "auth_method is required when auth-specific fields are provided",
                ));
            }
            return Ok(None);
        }
        Some(method) => method.trim().to_ascii_lowercase(),
    };

    let auth = match method.as_str() {
        "" | "none" => None,
        "user" => Some(AuthConfig::User),
        "app" => Some(AuthConfig::App {
            app_name: require_auth_value(&py_config.app_name, "app_name", &method)?,
        }),
        "userapp" => Some(AuthConfig::UserApp {
            app_name: require_auth_value(&py_config.app_name, "app_name", &method)?,
        }),
        "dir" | "directory" => Some(AuthConfig::Directory {
            property_name: require_auth_value(&py_config.dir_property, "dir_property", &method)?,
        }),
        "manual" => Some(AuthConfig::Manual {
            app_name: require_auth_value(&py_config.app_name, "app_name", &method)?,
            user_id: require_auth_value(&py_config.user_id, "user_id", &method)?,
            ip_address: require_auth_value(&py_config.ip_address, "ip_address", &method)?,
        }),
        "token" => Some(AuthConfig::Token {
            token: require_auth_value(&py_config.token, "token", &method)?,
        }),
        other => {
            return Err(PyValueError::new_err(format!(
                "Invalid auth_method: {other}. Must be one of ['none', 'user', 'app', 'userapp', 'dir', 'manual', 'token']",
            )));
        }
    };

    Ok(auth)
}

impl TryFrom<&PyEngineConfig> for EngineConfig {
    type Error = PyErr;

    fn try_from(py_config: &PyEngineConfig) -> Result<Self, Self::Error> {
        let validation_mode: ValidationMode = py_config
            .validation_mode
            .parse()
            .map_err(|e: String| pyo3::exceptions::PyValueError::new_err(e))?;

        let overflow_policy: OverflowPolicy = py_config
            .overflow_policy
            .parse()
            .map_err(|e: String| pyo3::exceptions::PyValueError::new_err(e))?;

        let auth = build_auth_config(py_config)?;

        Ok(EngineConfig {
            server_host: py_config.host.clone(),
            server_port: py_config.port,
            servers: py_config.servers.clone(),
            zfp_remote: py_config
                .zfp_remote
                .as_deref()
                .map(|s| s.parse())
                .transpose()
                .map_err(|e: String| pyo3::exceptions::PyValueError::new_err(e))?,
            request_pool_size: py_config.request_pool_size,
            subscription_pool_size: py_config.subscription_pool_size,
            validation_mode,
            subscription_flush_threshold: py_config.subscription_flush_threshold,
            max_event_queue_size: py_config.max_event_queue_size,
            command_queue_size: py_config.command_queue_size,
            subscription_stream_capacity: py_config.subscription_stream_capacity,
            overflow_policy,
            warmup_services: py_config.warmup_services.clone(),
            field_cache_path: py_config
                .field_cache_path
                .as_ref()
                .map(std::path::PathBuf::from),
            auth,
            tls_client_credentials: py_config.tls_client_credentials.clone(),
            tls_client_credentials_password: py_config.tls_client_credentials_password.clone(),
            tls_trust_material: py_config.tls_trust_material.clone(),
            tls_handshake_timeout_ms: py_config.tls_handshake_timeout_ms,
            tls_crl_fetch_timeout_ms: py_config.tls_crl_fetch_timeout_ms,
            num_start_attempts: py_config.num_start_attempts,
            auto_restart_on_disconnection: py_config.auto_restart_on_disconnection,
            max_recovery_attempts: py_config.max_recovery_attempts,
            recovery_timeout_ms: py_config.recovery_timeout_ms,
            retry_policy: RetryPolicy {
                max_retries: py_config.retry_max_retries,
                initial_delay_ms: py_config.retry_initial_delay_ms,
                backoff_factor: py_config.retry_backoff_factor,
                max_delay_ms: py_config.retry_max_delay_ms,
            },
            health_check_interval_ms: py_config.health_check_interval_ms,
            sdk_log_level: py_config
                .sdk_log_level
                .parse()
                .map_err(|e: String| pyo3::exceptions::PyValueError::new_err(e))?,
        })
    }
}
