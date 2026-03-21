use std::collections::HashMap;
use std::sync::{OnceLock, RwLock, RwLockReadGuard, RwLockWriteGuard};

use chrono::Utc;

use crate::{ExtError, Result};

use super::exchange::{ExchangeInfo, ExchangeInfoSource, OverridePatch};

static REGISTRY: OnceLock<RwLock<HashMap<String, OverridePatch>>> = OnceLock::new();

fn registry() -> &'static RwLock<HashMap<String, OverridePatch>> {
    REGISTRY.get_or_init(|| RwLock::new(HashMap::new()))
}

fn registry_read() -> Result<RwLockReadGuard<'static, HashMap<String, OverridePatch>>> {
    registry()
        .read()
        .map_err(|_| ExtError::Internal("override registry poisoned".to_string()))
}

fn registry_write() -> Result<RwLockWriteGuard<'static, HashMap<String, OverridePatch>>> {
    registry()
        .write()
        .map_err(|_| ExtError::Internal("override registry poisoned".to_string()))
}

fn normalize_ticker(ticker: &str) -> Result<String> {
    let normalized = ticker.trim();
    if normalized.is_empty() {
        return Err(ExtError::InvalidInput("ticker cannot be empty".to_string()));
    }
    Ok(normalized.to_string())
}

fn merge_override_patch(existing: &mut OverridePatch, patch: &OverridePatch) {
    if let Some(timezone) = &patch.timezone {
        existing.timezone = Some(timezone.clone());
    }
    if let Some(mic) = &patch.mic {
        existing.mic = Some(mic.clone());
    }
    if let Some(exch_code) = &patch.exch_code {
        existing.exch_code = Some(exch_code.clone());
    }
    if let Some(sessions) = &patch.sessions {
        existing.sessions = Some(sessions.clone());
    }
}

fn materialize_override_info(ticker: String, patch: &OverridePatch) -> ExchangeInfo {
    let mut info = ExchangeInfo::fallback(ticker).with_source(ExchangeInfoSource::Override);
    patch.apply_to(&mut info);
    info.cached_at = Some(Utc::now());
    info
}

/// Set or merge a runtime override patch for a ticker.
pub fn set_exchange_override(ticker: &str, patch: OverridePatch) -> Result<()> {
    if patch.is_empty() {
        return Err(ExtError::InvalidInput(
            "override patch must include at least one field".to_string(),
        ));
    }
    let key = normalize_ticker(ticker)?;
    let mut guard = registry_write()?;
    guard
        .entry(key)
        .and_modify(|existing| merge_override_patch(existing, &patch))
        .or_insert(patch);
    Ok(())
}

/// Get a raw override patch for merge workflows.
pub fn get_exchange_override_patch(ticker: &str) -> Result<Option<OverridePatch>> {
    let key = ticker.trim();
    if key.is_empty() {
        return Ok(None);
    }
    Ok(registry_read()?.get(key).cloned())
}

/// Get a materialized exchange override object.
pub fn get_exchange_override(ticker: &str) -> Result<Option<ExchangeInfo>> {
    let Some(patch) = get_exchange_override_patch(ticker)? else {
        return Ok(None);
    };
    Ok(Some(materialize_override_info(ticker.to_string(), &patch)))
}

/// Remove a single override if `ticker` is provided; clear all otherwise.
pub fn clear_exchange_override(ticker: Option<&str>) -> Result<()> {
    let mut guard = registry_write()?;
    match ticker {
        Some(t) if !t.trim().is_empty() => {
            guard.remove(t.trim());
        }
        _ => guard.clear(),
    }
    Ok(())
}

/// Return all active overrides as materialized exchange info objects.
pub fn list_exchange_overrides() -> Result<HashMap<String, ExchangeInfo>> {
    Ok(registry_read()?
        .iter()
        .map(|(ticker, patch)| {
            (
                ticker.clone(),
                materialize_override_info(ticker.clone(), patch),
            )
        })
        .collect())
}

pub fn has_exchange_override(ticker: &str) -> Result<bool> {
    let key = ticker.trim();
    if key.is_empty() {
        return Ok(false);
    }
    Ok(registry_read()?.contains_key(key))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::markets::sessions::SessionWindows;
    use std::sync::{Mutex, MutexGuard, OnceLock};

    fn test_registry_guard() -> MutexGuard<'static, ()> {
        static TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        TEST_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap()
    }

    #[test]
    fn test_override_lifecycle() {
        let _guard = test_registry_guard();
        clear_exchange_override(None).unwrap();

        set_exchange_override(
            "AAPL US Equity",
            OverridePatch {
                timezone: Some("America/New_York".to_string()),
                ..OverridePatch::default()
            },
        )
        .unwrap();

        assert!(has_exchange_override("AAPL US Equity").unwrap());
        let info = get_exchange_override("AAPL US Equity").unwrap().unwrap();
        assert_eq!(info.timezone, "America/New_York");
        assert_eq!(info.source, ExchangeInfoSource::Override);

        set_exchange_override(
            "AAPL US Equity",
            OverridePatch {
                sessions: Some(SessionWindows {
                    day: Some(("09:30".to_string(), "16:00".to_string())),
                    ..SessionWindows::default()
                }),
                ..OverridePatch::default()
            },
        )
        .unwrap();

        let info = get_exchange_override("AAPL US Equity").unwrap().unwrap();
        assert_eq!(info.timezone, "America/New_York");
        assert_eq!(
            info.sessions.day,
            Some(("09:30".to_string(), "16:00".to_string()))
        );

        clear_exchange_override(Some("AAPL US Equity")).unwrap();
        assert!(!has_exchange_override("AAPL US Equity").unwrap());
    }

    #[test]
    fn test_override_list_materialization() {
        let _guard = test_registry_guard();
        clear_exchange_override(None).unwrap();

        set_exchange_override(
            "MSFT US Equity",
            OverridePatch {
                mic: Some("XNAS".to_string()),
                ..OverridePatch::default()
            },
        )
        .unwrap();

        let overrides = list_exchange_overrides().unwrap();
        let info = overrides.get("MSFT US Equity").unwrap();

        assert_eq!(info.ticker, "MSFT US Equity");
        assert_eq!(info.mic, Some("XNAS".to_string()));
        assert_eq!(info.source, ExchangeInfoSource::Override);
        assert!(info.cached_at.is_some());
    }
}
