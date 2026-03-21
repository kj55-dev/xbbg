use std::collections::HashMap;

use super::{parse_operation_lossless, ExtractorType};
use crate::errors::BlpAsyncError;
use crate::services::Operation;

/// Generic request parameters from Python.
///
/// This unified struct holds all possible Bloomberg request parameters.
/// Not all fields are used for all request types.
#[derive(Clone, Debug, Default)]
pub struct RequestParams {
    /// Bloomberg service URI (e.g., "//blp/refdata")
    pub service: String,
    /// Request operation name (e.g., "ReferenceDataRequest")
    pub operation: String,
    /// Actual Bloomberg operation name when using the RawRequest marker.
    pub request_operation: Option<String>,
    /// Extractor type hint for Arrow conversion
    pub extractor: ExtractorType,
    /// Whether extractor was explicitly provided by the caller.
    pub extractor_set: bool,
    /// Multiple securities (for bdp/bdh)
    pub securities: Option<Vec<String>>,
    /// Single security (for intraday)
    pub security: Option<String>,
    /// Fields to retrieve
    pub fields: Option<Vec<String>>,
    /// Field overrides (for standard Bloomberg override format)
    pub overrides: Option<Vec<(String, String)>>,
    /// Generic request elements (for BQL expression, bsrch domain, etc.)
    pub elements: Option<Vec<(String, String)>>,
    /// Raw kwargs to route into elements/overrides using schema-driven logic.
    pub kwargs: Option<HashMap<String, String>>,
    /// Start date (YYYYMMDD for bdh)
    pub start_date: Option<String>,
    /// End date (YYYYMMDD for bdh)
    pub end_date: Option<String>,
    /// Start datetime (ISO for intraday)
    pub start_datetime: Option<String>,
    /// End datetime (ISO for intraday)
    pub end_datetime: Option<String>,
    /// Event type (TRADE, BID, ASK for intraday bars - singular)
    pub event_type: Option<String>,
    /// Event types (TRADE, BID, ASK for intraday ticks - array)
    pub event_types: Option<Vec<String>>,
    /// Bar interval in minutes (for bdib)
    pub interval: Option<u32>,
    /// Additional Bloomberg options
    pub options: Option<Vec<(String, String)>>,
    /// Manual field type overrides (for future type resolution)
    pub field_types: Option<HashMap<String, String>>,
    /// Include security error rows in RefData long output when present.
    pub include_security_errors: bool,
    /// Optional per-request field validation override.
    ///
    /// - Some(true): force strict field validation for this request
    /// - Some(false): disable field validation for this request
    /// - None: follow engine-level validation_mode
    pub validate_fields: Option<bool>,
    /// Search spec for FieldSearchRequest (//blp/apiflds)
    pub search_spec: Option<String>,
    /// Field IDs for FieldInfoRequest (//blp/apiflds)
    pub field_ids: Option<Vec<String>>,
    /// Output format (long, long_typed, long_metadata, wide)
    pub format: Option<String>,
}

impl RequestParams {
    pub(crate) fn is_raw_request(&self) -> bool {
        matches!(
            parse_operation_lossless(&self.operation),
            Operation::RawRequest
        )
    }

    pub(crate) fn effective_operation(&self) -> &str {
        if self.is_raw_request() {
            self.request_operation.as_deref().unwrap_or_default()
        } else {
            &self.operation
        }
    }

    /// Apply default values derived from operation semantics.
    pub fn with_defaults(mut self) -> Self {
        if !self.extractor_set {
            let operation = parse_operation_lossless(&self.operation);
            self.extractor = operation.default_extractor();
        }
        self
    }

    /// Validate request parameters for known Bloomberg operations.
    pub fn validate(&self) -> Result<(), BlpAsyncError> {
        if self.service.is_empty() {
            return Err(BlpAsyncError::ConfigError {
                detail: "service is required".to_string(),
            });
        }

        let operation = parse_operation_lossless(&self.operation);
        if matches!(operation, Operation::RawRequest) {
            if self
                .request_operation
                .as_ref()
                .is_none_or(|operation| operation.is_empty())
            {
                return Err(BlpAsyncError::ConfigError {
                    detail: "request_operation is required for RawRequest".to_string(),
                });
            }
        } else if self.operation.is_empty() {
            return Err(BlpAsyncError::ConfigError {
                detail: "operation is required".to_string(),
            });
        }

        match operation {
            Operation::ReferenceData => self.validate_reference_data(),
            Operation::HistoricalData => self.validate_historical_data(),
            Operation::IntradayBar => self.validate_intraday_bar(),
            Operation::IntradayTick => self.validate_intraday_tick(),
            Operation::FieldInfo | Operation::FieldSearch => {
                self.validate_field_request(&operation)
            }
            // Unknown/custom operations run in power-user mode.
            Operation::Beqs
            | Operation::PortfolioData
            | Operation::InstrumentList
            | Operation::CurveList
            | Operation::GovtList
            | Operation::BqlSendQuery
            | Operation::ExcelGetGrid
            | Operation::StudyRequest
            | Operation::RawRequest
            | Operation::Custom(_) => Ok(()),
        }
    }

    fn validate_reference_data(&self) -> Result<(), BlpAsyncError> {
        if !self.has_securities() {
            return Err(BlpAsyncError::ConfigError {
                detail: "securities is required for ReferenceDataRequest".to_string(),
            });
        }

        if !self.has_fields() {
            return Err(BlpAsyncError::ConfigError {
                detail: "fields is required for ReferenceDataRequest".to_string(),
            });
        }

        Ok(())
    }

    fn validate_historical_data(&self) -> Result<(), BlpAsyncError> {
        if !self.has_securities() {
            return Err(BlpAsyncError::ConfigError {
                detail: "securities is required for HistoricalDataRequest".to_string(),
            });
        }

        if !self.has_fields() {
            return Err(BlpAsyncError::ConfigError {
                detail: "fields is required for HistoricalDataRequest".to_string(),
            });
        }

        if !self.has_start_date() {
            return Err(BlpAsyncError::ConfigError {
                detail: "start_date is required for HistoricalDataRequest".to_string(),
            });
        }

        if !self.has_end_date() {
            return Err(BlpAsyncError::ConfigError {
                detail: "end_date is required for HistoricalDataRequest".to_string(),
            });
        }

        Ok(())
    }

    fn validate_intraday_bar(&self) -> Result<(), BlpAsyncError> {
        if !self.has_security() {
            return Err(BlpAsyncError::ConfigError {
                detail: "security is required for IntradayBarRequest".to_string(),
            });
        }

        if !self.has_event_type() {
            return Err(BlpAsyncError::ConfigError {
                detail: "event_type is required for IntradayBarRequest".to_string(),
            });
        }

        if self.interval.is_none() {
            return Err(BlpAsyncError::ConfigError {
                detail: "interval is required for IntradayBarRequest".to_string(),
            });
        }

        if !self.has_start_datetime() {
            return Err(BlpAsyncError::ConfigError {
                detail: "start_datetime is required for IntradayBarRequest".to_string(),
            });
        }

        if !self.has_end_datetime() {
            return Err(BlpAsyncError::ConfigError {
                detail: "end_datetime is required for IntradayBarRequest".to_string(),
            });
        }

        Ok(())
    }

    fn validate_intraday_tick(&self) -> Result<(), BlpAsyncError> {
        if !self.has_security() {
            return Err(BlpAsyncError::ConfigError {
                detail: "security is required for IntradayTickRequest".to_string(),
            });
        }

        if !self.has_start_datetime() {
            return Err(BlpAsyncError::ConfigError {
                detail: "start_datetime is required for IntradayTickRequest".to_string(),
            });
        }

        if !self.has_end_datetime() {
            return Err(BlpAsyncError::ConfigError {
                detail: "end_datetime is required for IntradayTickRequest".to_string(),
            });
        }

        Ok(())
    }

    fn validate_field_request(&self, operation: &Operation) -> Result<(), BlpAsyncError> {
        let has_fields = self.has_fields();

        match operation {
            Operation::FieldInfo => {
                let has_field_ids = self.field_ids.as_ref().is_some_and(|ids| !ids.is_empty());
                if !has_fields && !has_field_ids {
                    return Err(BlpAsyncError::ConfigError {
                        detail: "fields is required for field metadata requests".to_string(),
                    });
                }
            }
            Operation::FieldSearch => {
                let has_search_spec = self.search_spec.as_ref().is_some_and(|s| !s.is_empty());
                if !has_fields && !has_search_spec {
                    return Err(BlpAsyncError::ConfigError {
                        detail: "fields is required for field metadata requests".to_string(),
                    });
                }
            }
            _ => {}
        }

        Ok(())
    }

    fn has_securities(&self) -> bool {
        self.securities
            .as_ref()
            .is_some_and(|values| !values.is_empty())
    }

    fn has_security(&self) -> bool {
        self.security
            .as_ref()
            .is_some_and(|value| !value.is_empty())
    }

    fn has_fields(&self) -> bool {
        self.fields
            .as_ref()
            .is_some_and(|values| !values.is_empty())
    }

    fn has_start_date(&self) -> bool {
        self.start_date
            .as_ref()
            .is_some_and(|value| !value.is_empty())
    }

    fn has_end_date(&self) -> bool {
        self.end_date
            .as_ref()
            .is_some_and(|value| !value.is_empty())
    }

    fn has_start_datetime(&self) -> bool {
        self.start_datetime
            .as_ref()
            .is_some_and(|value| !value.is_empty())
    }

    fn has_end_datetime(&self) -> bool {
        self.end_datetime
            .as_ref()
            .is_some_and(|value| !value.is_empty())
    }

    fn has_event_type(&self) -> bool {
        self.event_type
            .as_ref()
            .is_some_and(|value| !value.is_empty())
    }
}

pub(crate) fn merge_raw_kwargs_into_elements(
    params: &mut RequestParams,
    kwargs: HashMap<String, String>,
) {
    if kwargs.is_empty() {
        return;
    }

    let mut keys: Vec<String> = kwargs.keys().cloned().collect();
    keys.sort();

    let elements = params.elements.get_or_insert_with(Vec::new);
    for key in keys {
        if let Some(value) = kwargs.get(&key) {
            elements.push((key, value.clone()));
        }
    }
}
