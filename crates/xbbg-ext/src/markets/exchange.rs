use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::sessions::SessionWindows;

/// Source of exchange metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExchangeInfoSource {
    Override,
    Cache,
    Bloomberg,
    Inferred,
    Fallback,
}

impl ExchangeInfoSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Override => "override",
            Self::Cache => "cache",
            Self::Bloomberg => "bloomberg",
            Self::Inferred => "inferred",
            Self::Fallback => "fallback",
        }
    }
}

/// Canonical exchange metadata used across resolution layers.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExchangeInfo {
    pub ticker: String,
    pub mic: Option<String>,
    pub exch_code: Option<String>,
    pub timezone: String,
    pub utc_offset: Option<f64>,
    pub sessions: SessionWindows,
    pub source: ExchangeInfoSource,
    pub cached_at: Option<DateTime<Utc>>,
}

impl ExchangeInfo {
    pub fn fallback(ticker: impl Into<String>) -> Self {
        Self {
            ticker: ticker.into(),
            mic: None,
            exch_code: None,
            timezone: "UTC".to_string(),
            utc_offset: None,
            sessions: SessionWindows::default(),
            source: ExchangeInfoSource::Fallback,
            cached_at: None,
        }
    }

    pub fn with_source(mut self, source: ExchangeInfoSource) -> Self {
        self.source = source;
        self
    }

    pub fn as_cache_hit(self) -> Self {
        self.with_source(ExchangeInfoSource::Cache)
    }
}

/// Partial exchange update used by runtime overrides.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct OverridePatch {
    pub timezone: Option<String>,
    pub mic: Option<String>,
    pub exch_code: Option<String>,
    pub sessions: Option<SessionWindows>,
}

impl OverridePatch {
    pub fn is_empty(&self) -> bool {
        self.timezone.is_none()
            && self.mic.is_none()
            && self.exch_code.is_none()
            && self.sessions.is_none()
    }

    pub fn apply_to(&self, info: &mut ExchangeInfo) {
        assign_if_some(&mut info.timezone, &self.timezone);
        assign_option_if_some(&mut info.mic, &self.mic);
        assign_option_if_some(&mut info.exch_code, &self.exch_code);
        assign_if_some(&mut info.sessions, &self.sessions);
    }
}

fn assign_if_some<T: Clone>(target: &mut T, value: &Option<T>) {
    if let Some(value) = value {
        *target = value.clone();
    }
}

fn assign_option_if_some<T: Clone>(target: &mut Option<T>, value: &Option<T>) {
    if let Some(value) = value {
        *target = Some(value.clone());
    }
}

/// Market-level metadata used by higher-level APIs.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct MarketInfo {
    pub exch: Option<String>,
    pub tz: Option<String>,
    pub freq: Option<String>,
    pub is_fut: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::markets::sessions::SessionWindows;

    #[test]
    fn fallback_populates_the_expected_baseline() {
        let info = ExchangeInfo::fallback("AAPL US Equity");

        assert_eq!(info.ticker, "AAPL US Equity");
        assert_eq!(info.mic, None);
        assert_eq!(info.exch_code, None);
        assert_eq!(info.timezone, "UTC");
        assert_eq!(info.utc_offset, None);
        assert_eq!(info.sessions, SessionWindows::default());
        assert_eq!(info.source, ExchangeInfoSource::Fallback);
        assert_eq!(info.cached_at, None);
    }

    #[test]
    fn source_helpers_only_change_the_source() {
        let info = ExchangeInfo::fallback("AAPL US Equity");
        let bloomberg = info.clone().with_source(ExchangeInfoSource::Bloomberg);
        let cache_hit = bloomberg.clone().as_cache_hit();

        assert_eq!(bloomberg.ticker, info.ticker);
        assert_eq!(bloomberg.timezone, info.timezone);
        assert_eq!(bloomberg.sessions, info.sessions);
        assert_eq!(bloomberg.source, ExchangeInfoSource::Bloomberg);

        assert_eq!(cache_hit.ticker, info.ticker);
        assert_eq!(cache_hit.timezone, info.timezone);
        assert_eq!(cache_hit.sessions, info.sessions);
        assert_eq!(cache_hit.source, ExchangeInfoSource::Cache);
    }

    #[test]
    fn override_patch_applies_only_present_fields() {
        let mut info =
            ExchangeInfo::fallback("AAPL US Equity").with_source(ExchangeInfoSource::Inferred);
        info.mic = Some("XNAS".to_string());
        info.exch_code = Some("US".to_string());
        info.utc_offset = Some(-5.0);

        let sessions = SessionWindows {
            day: Some(("09:30".to_string(), "16:00".to_string())),
            pre: Some(("04:00".to_string(), "09:30".to_string())),
            ..SessionWindows::default()
        };
        let patch = OverridePatch {
            timezone: Some("America/New_York".to_string()),
            exch_code: Some("XNYS".to_string()),
            sessions: Some(sessions.clone()),
            ..OverridePatch::default()
        };

        patch.apply_to(&mut info);

        assert_eq!(info.timezone, "America/New_York");
        assert_eq!(info.mic, Some("XNAS".to_string()));
        assert_eq!(info.exch_code, Some("XNYS".to_string()));
        assert_eq!(info.utc_offset, Some(-5.0));
        assert_eq!(info.sessions, sessions);
        assert_eq!(info.source, ExchangeInfoSource::Inferred);
    }
}
