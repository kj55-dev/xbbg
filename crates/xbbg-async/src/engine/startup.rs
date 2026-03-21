use std::sync::Arc;

use xbbg_core::session::Session;
use xbbg_core::{apply_session_identity_options, AuthConfig, BlpError, SessionOptions};

use super::{EngineConfig, RequestWorkerPool, SubscriptionSessionPool};
use crate::errors::BlpAsyncError;

const SESSION_STARTUP_TIMEOUT_MS: u32 = 30_000;

fn configure_session_options(
    options: &mut SessionOptions,
    config: &EngineConfig,
    record_subscription_receive_times: bool,
) -> Result<(), BlpError> {
    let fallback = vec![(config.server_host.clone(), config.server_port)];
    let servers = if config.servers.is_empty() {
        &fallback
    } else {
        &config.servers
    };
    for (index, (host, port)) in servers.iter().enumerate() {
        options.set_server_address(host, *port, index)?;
    }
    options.set_num_start_attempts(config.num_start_attempts)?;
    options.set_auto_restart_on_disconnection(config.auto_restart_on_disconnection);
    options.set_max_event_queue_size(config.max_event_queue_size);
    let _ = options.set_bandwidth_save_mode_disabled(true);

    if record_subscription_receive_times {
        options.set_record_subscription_receive_times(true);
    }

    if let Some(auth_config) = config.auth.as_ref() {
        let _ = apply_session_identity_options(options, auth_config)?;
    }

    if let (Some(creds), Some(trust)) = (
        config.tls_client_credentials.as_ref(),
        config.tls_trust_material.as_ref(),
    ) {
        let password = config
            .tls_client_credentials_password
            .as_deref()
            .unwrap_or("");
        let mut tls = xbbg_core::tls::TlsOptions::from_files(creds, password, trust)?;
        if let Some(ms) = config.tls_handshake_timeout_ms {
            tls.set_tls_handshake_timeout_ms(ms);
        }
        if let Some(ms) = config.tls_crl_fetch_timeout_ms {
            tls.set_crl_fetch_timeout_ms(ms);
        }
        options.set_tls_options(&tls);
    }

    Ok(())
}

fn build_tls_options(config: &EngineConfig) -> Result<xbbg_core::tls::TlsOptions, BlpError> {
    let creds =
        config
            .tls_client_credentials
            .as_deref()
            .ok_or_else(|| BlpError::InvalidArgument {
                detail: "ZFP requires tls_client_credentials".into(),
            })?;
    let trust = config
        .tls_trust_material
        .as_deref()
        .ok_or_else(|| BlpError::InvalidArgument {
            detail: "ZFP requires tls_trust_material".into(),
        })?;
    let password = config
        .tls_client_credentials_password
        .as_deref()
        .unwrap_or("");
    let mut tls = xbbg_core::tls::TlsOptions::from_files(creds, password, trust)?;
    if let Some(ms) = config.tls_handshake_timeout_ms {
        tls.set_tls_handshake_timeout_ms(ms);
    }
    if let Some(ms) = config.tls_crl_fetch_timeout_ms {
        tls.set_crl_fetch_timeout_ms(ms);
    }
    Ok(tls)
}

fn attach_auth_context(error: BlpError, auth: Option<&AuthConfig>) -> BlpError {
    let Some(auth) = auth else {
        return error;
    };

    match error {
        BlpError::SessionStart { source, label } => {
            let label = match label {
                Some(existing) => {
                    Some(format!("auth_method={} - {}", auth.method_name(), existing))
                }
                None => Some(format!("auth_method={}", auth.method_name())),
            };
            BlpError::SessionStart { source, label }
        }
        other => other,
    }
}

pub(crate) fn start_configured_session(
    config: &EngineConfig,
    record_subscription_receive_times: bool,
) -> Result<Session, BlpError> {
    let mut options = SessionOptions::new()?;

    if let Some(ref zfp_remote) = config.zfp_remote {
        let tls = build_tls_options(config)?;
        xbbg_core::zfp::configure_zfp_options(&mut options, &tls, *zfp_remote)?;
    }

    configure_session_options(&mut options, config, record_subscription_receive_times)?;

    let session = Session::new(&options)?;
    session
        .start_and_wait(SESSION_STARTUP_TIMEOUT_MS)
        .map_err(|err| attach_auth_context(err, config.auth.as_ref()))?;
    Ok(session)
}

pub(crate) fn build_engine_components(
    config: Arc<EngineConfig>,
) -> Result<
    (
        RequestWorkerPool,
        Arc<SubscriptionSessionPool>,
        Arc<tokio::runtime::Runtime>,
    ),
    BlpAsyncError,
> {
    crate::sdk_logging::register_sdk_logging(config.sdk_log_level);
    crate::field_cache::init_global_resolver(config.field_cache_path.clone());

    let runtime = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|e| BlpAsyncError::Internal(format!("tokio runtime: {e}")))?,
    );

    xbbg_log::info!(
        request_pool_size = config.request_pool_size,
        subscription_pool_size = config.subscription_pool_size,
        "starting Engine with worker pools"
    );

    let request_pool = RequestWorkerPool::new(config.request_pool_size, config.clone())?;
    let subscription_pool = Arc::new(SubscriptionSessionPool::new(
        config.subscription_pool_size,
        config.clone(),
    )?);

    Ok((request_pool, subscription_pool, runtime))
}
