use xbbg_core::AuthConfig;

use super::status::{OverflowPolicy, RetryPolicy};

/// Validation mode for request validation.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ValidationMode {
    /// Error on invalid fields/requests
    Strict,
    /// Warn but still send request
    Lenient,
    /// Skip validation entirely (default)
    #[default]
    Disabled,
}

impl std::str::FromStr for ValidationMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "strict" => Ok(Self::Strict),
            "lenient" => Ok(Self::Lenient),
            "disabled" | "off" | "none" => Ok(Self::Disabled),
            _ => Err(format!(
                "unknown validation mode '{}': expected strict, lenient, or disabled",
                s
            )),
        }
    }
}

impl std::fmt::Display for ValidationMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Strict => write!(f, "strict"),
            Self::Lenient => write!(f, "lenient"),
            Self::Disabled => write!(f, "disabled"),
        }
    }
}

/// Configuration for the Engine.
#[derive(Clone)]
pub struct EngineConfig {
    /// Server host for single-server mode (e.g., "localhost").
    pub server_host: String,
    /// Server port for single-server mode (e.g., 8194).
    pub server_port: u16,
    /// Multiple servers for failover. When non-empty, overrides server_host/server_port.
    /// SDK tries servers in order — index 0 first, then index 1, etc.
    pub servers: Vec<(String, u16)>,
    /// ZFP over leased lines. When set, overrides host/port/servers.
    pub zfp_remote: Option<xbbg_core::zfp::ZfpRemote>,
    /// Max event queue size (Bloomberg SDK setting)
    pub max_event_queue_size: usize,
    /// Command channel capacity (backpressure)
    pub command_queue_size: usize,
    /// Subscription flush threshold (rows before auto-flush)
    pub subscription_flush_threshold: usize,
    /// Subscription stream capacity (backpressure)
    pub subscription_stream_capacity: usize,
    /// Overflow policy for slow consumers
    pub overflow_policy: OverflowPolicy,
    /// Number of request workers (default: 2)
    pub request_pool_size: usize,
    /// Number of subscription sessions (default: 4)
    pub subscription_pool_size: usize,
    /// Services to pre-warm on request workers
    pub warmup_services: Vec<String>,
    /// Validation mode for requests (default: Strict)
    pub validation_mode: ValidationMode,
    /// Custom path for the field cache JSON file (default: ~/.xbbg/field_cache.json)
    pub field_cache_path: Option<std::path::PathBuf>,
    /// Structured Bloomberg session auth configuration.
    pub auth: Option<AuthConfig>,
    /// TLS client credentials file path (PKCS#12).
    pub tls_client_credentials: Option<String>,
    /// TLS client credentials password.
    pub tls_client_credentials_password: Option<String>,
    /// TLS trust material file path (PKCS#7).
    pub tls_trust_material: Option<String>,
    /// TLS handshake timeout in milliseconds.
    pub tls_handshake_timeout_ms: Option<i32>,
    /// CRL fetch timeout in milliseconds.
    pub tls_crl_fetch_timeout_ms: Option<i32>,
    /// Number of times the SDK will attempt to connect before giving up.
    pub num_start_attempts: usize,
    /// Whether the SDK should auto-restart the session after disconnection.
    pub auto_restart_on_disconnection: bool,
    /// Max attempts to recover subscriptions after reconnect (default: 3).
    pub max_recovery_attempts: usize,
    /// Timeout in ms for the full recovery sequence (default: 30000).
    pub recovery_timeout_ms: u64,
    /// Retry policy for transient request failures (default: no retry).
    pub retry_policy: RetryPolicy,
    /// Interval in ms between worker health checks (default: 30000).
    pub health_check_interval_ms: u64,
    /// Bloomberg SDK internal log level. Bridges SDK logs into xbbg tracing.
    /// Must be set before first session starts. Default: Off.
    pub sdk_log_level: crate::sdk_logging::SdkLogLevel,
}
impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            server_host: "localhost".to_string(),
            server_port: 8194,
            servers: Vec::new(),
            zfp_remote: None,
            max_event_queue_size: 10_000,
            command_queue_size: 256,
            subscription_flush_threshold: 1,
            subscription_stream_capacity: 256,
            overflow_policy: OverflowPolicy::default(),
            request_pool_size: 2,
            subscription_pool_size: 1,
            warmup_services: vec![
                crate::services::Service::RefData.to_string(),
                crate::services::Service::ApiFlds.to_string(),
            ],
            validation_mode: ValidationMode::default(),
            field_cache_path: None,
            auth: None,
            tls_client_credentials: None,
            tls_client_credentials_password: None,
            tls_trust_material: None,
            tls_handshake_timeout_ms: None,
            tls_crl_fetch_timeout_ms: None,
            num_start_attempts: 3,
            auto_restart_on_disconnection: true,
            max_recovery_attempts: 3,
            recovery_timeout_ms: 30_000,
            retry_policy: RetryPolicy::default(),
            health_check_interval_ms: 30_000,
            sdk_log_level: crate::sdk_logging::SdkLogLevel::Off,
        }
    }
}
