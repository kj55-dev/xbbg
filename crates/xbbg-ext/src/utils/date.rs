//! Date parsing and formatting utilities.
//!
//! Provides fast date parsing with support for multiple common formats.

use chrono::NaiveDate;

use crate::error::{ExtError, Result};

/// Supported date formats for parsing.
const DEFAULT_DATE_FORMAT: &str = "%Y%m%d";
const ISO_DATE_FORMAT: &str = "%Y-%m-%d";
const ISO_DATETIME_FORMAT: &str = "%Y-%m-%dT%H:%M:%S";

const DATE_FORMATS: &[&str] = &[
    ISO_DATE_FORMAT,     // 2024-01-15
    DEFAULT_DATE_FORMAT, // 20240115
    "%Y/%m/%d",          // 2024/01/15
    "%d-%m-%Y",          // 15-01-2024
    "%d/%m/%Y",          // 15/01/2024
];

fn parse_with_format(s: &str, fmt: &str) -> Option<NaiveDate> {
    NaiveDate::parse_from_str(s, fmt).ok()
}

fn local_naive_datetime() -> chrono::NaiveDateTime {
    chrono::Local::now().naive_local()
}

fn yesterday_local_date() -> chrono::NaiveDate {
    local_naive_datetime().date() - chrono::Duration::days(1)
}

fn fast_date_format(s: &str) -> Option<&'static str> {
    match s.len() {
        8 => Some(DEFAULT_DATE_FORMAT),
        10 => match s.as_bytes().get(4) {
            Some(b'-') => Some(ISO_DATE_FORMAT),
            Some(b'/') => Some("%Y/%m/%d"),
            _ => match s.as_bytes().get(2) {
                Some(b'-') => Some("%d-%m-%Y"),
                Some(b'/') => Some("%d/%m/%Y"),
                _ => None,
            },
        },
        _ => None,
    }
}

/// Parse a date string into a `NaiveDate`.
///
/// Supports multiple formats:
/// - `YYYY-MM-DD` (ISO 8601)
/// - `YYYYMMDD` (Bloomberg compact)
/// - `YYYY/MM/DD`
/// - `DD-MM-YYYY`
/// - `DD/MM/YYYY`
///
/// # Examples
///
/// ```
/// use xbbg_ext::utils::date::parse_date;
///
/// let d1 = parse_date("2024-01-15").unwrap();
/// let d2 = parse_date("20240115").unwrap();
/// let d3 = parse_date("2024/01/15").unwrap();
///
/// assert_eq!(d1, d2);
/// assert_eq!(d2, d3);
/// ```
///
/// # Errors
///
/// Returns `ExtError::DateParse` if the string doesn't match any supported format.
pub fn parse_date(s: &str) -> Result<NaiveDate> {
    let s = s.trim();

    if let Some(fmt) = fast_date_format(s) {
        if let Some(d) = parse_with_format(s, fmt) {
            return Ok(d);
        }
    }

    // Fallback: try all formats
    for fmt in DATE_FORMATS {
        if let Some(d) = parse_with_format(s, fmt) {
            return Ok(d);
        }
    }

    Err(ExtError::DateParse(s.to_string()))
}

/// Format a date to a string.
///
/// # Arguments
///
/// * `date` - The date to format
/// * `fmt` - Optional format string (default: `%Y%m%d` for Bloomberg)
///
/// # Examples
///
/// ```
/// use chrono::NaiveDate;
/// use xbbg_ext::utils::date::fmt_date;
///
/// let d = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
///
/// assert_eq!(fmt_date(d, None), "20240115");
/// assert_eq!(fmt_date(d, Some("%Y-%m-%d")), "2024-01-15");
/// ```
pub fn fmt_date(date: NaiveDate, fmt: Option<&str>) -> String {
    date.format(fmt.unwrap_or(DEFAULT_DATE_FORMAT)).to_string()
}

/// Try to parse a date, returning None if parsing fails.
///
/// Useful when date parsing is optional or for filter operations.
pub fn try_parse_date(s: &str) -> Option<NaiveDate> {
    parse_date(s).ok()
}

/// Compute default date range for turnover queries.
///
/// Returns `(start_date, end_date)` as ISO-8601 strings.
/// * `end_date` defaults to yesterday if not provided.
/// * `start_date` defaults to 30 days before `end_date` if not provided.
///
/// # Examples
///
/// ```
/// use xbbg_ext::utils::date::default_turnover_dates;
///
/// let (start, end) = default_turnover_dates(None, None);
/// assert_eq!(start.len(), 10); // "YYYY-MM-DD"
/// assert_eq!(end.len(), 10);
///
/// let (start2, end2) = default_turnover_dates(None, Some("2024-06-15"));
/// assert_eq!(end2, "2024-06-15");
/// assert_eq!(start2, "2024-05-16");
/// ```
pub fn default_turnover_dates(
    start_date: Option<&str>,
    end_date: Option<&str>,
) -> (String, String) {
    let end = match end_date {
        Some(s) => try_parse_date(s).unwrap_or_else(yesterday_local_date),
        None => yesterday_local_date(),
    };

    let start = match start_date {
        Some(s) => try_parse_date(s).unwrap_or(end - chrono::Duration::days(30)),
        None => end - chrono::Duration::days(30),
    };

    (
        fmt_date(start, Some(ISO_DATE_FORMAT)),
        fmt_date(end, Some(ISO_DATE_FORMAT)),
    )
}

/// Compute default datetime range for BQR (quote request) queries.
///
/// Returns `(start_datetime, end_datetime)` as ISO-8601 datetime strings.
/// * `end_datetime` defaults to now if not provided.
/// * `start_datetime` defaults to 1 hour before `end_datetime` if not provided.
///
/// Input datetimes support both `YYYY-MM-DD HH:MM` and `YYYY-MM-DDTHH:MM` formats.
///
/// # Examples
///
/// ```
/// use xbbg_ext::utils::date::default_bqr_datetimes;
///
/// let (start, end) = default_bqr_datetimes(None, None);
/// assert!(start.contains('T'));
/// assert!(end.contains('T'));
///
/// let (start2, end2) = default_bqr_datetimes(
///     Some("2024-01-15 09:00"),
///     Some("2024-01-15 10:00"),
/// );
/// assert_eq!(start2, "2024-01-15T09:00:00");
/// assert_eq!(end2, "2024-01-15T10:00:00");
/// ```
pub fn default_bqr_datetimes(
    start_datetime: Option<&str>,
    end_datetime: Option<&str>,
) -> (String, String) {
    let end_str = match end_datetime {
        Some(s) => normalize_datetime_str(s),
        None => local_naive_datetime()
            .format(ISO_DATETIME_FORMAT)
            .to_string(),
    };

    let start_str = match start_datetime {
        Some(s) => normalize_datetime_str(s),
        None => {
            // Parse end_str back to compute 1 hour before
            let end_dt = chrono::NaiveDateTime::parse_from_str(&end_str, ISO_DATETIME_FORMAT)
                .unwrap_or_else(|_| local_naive_datetime());
            let start_dt = end_dt - chrono::Duration::hours(1);
            start_dt.format(ISO_DATETIME_FORMAT).to_string()
        }
    };

    (start_str, end_str)
}

/// Normalize a datetime string to ISO-8601 format `YYYY-MM-DDTHH:MM:SS`.
///
/// Handles:
/// * `YYYY-MM-DD HH:MM` → `YYYY-MM-DDTHH:MM:00`
/// * `YYYY-MM-DDTHH:MM` → `YYYY-MM-DDTHH:MM:00`
/// * Already complete strings pass through.
fn normalize_datetime_str(s: &str) -> String {
    let s = s.replace(' ', "T");
    if s.len() == 16 && s.contains('T') {
        // YYYY-MM-DDTHH:MM → add :00
        format!("{}:00", s)
    } else {
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Datelike;

    #[test]
    fn test_parse_iso() {
        let d = parse_date("2024-01-15").unwrap();
        assert_eq!(d.year(), 2024);
        assert_eq!(d.month(), 1);
        assert_eq!(d.day(), 15);
    }

    #[test]
    fn test_parse_compact() {
        let d = parse_date("20240115").unwrap();
        assert_eq!(d.year(), 2024);
        assert_eq!(d.month(), 1);
        assert_eq!(d.day(), 15);
    }

    #[test]
    fn test_parse_slash() {
        let d = parse_date("2024/01/15").unwrap();
        assert_eq!(d.year(), 2024);
        assert_eq!(d.month(), 1);
        assert_eq!(d.day(), 15);
    }

    #[test]
    fn test_parse_euro_dash() {
        let d = parse_date("15-01-2024").unwrap();
        assert_eq!(d.year(), 2024);
        assert_eq!(d.month(), 1);
        assert_eq!(d.day(), 15);
    }

    #[test]
    fn test_parse_euro_slash() {
        let d = parse_date("15/01/2024").unwrap();
        assert_eq!(d.year(), 2024);
        assert_eq!(d.month(), 1);
        assert_eq!(d.day(), 15);
    }

    #[test]
    fn test_parse_invalid() {
        assert!(parse_date("not-a-date").is_err());
        assert!(parse_date("").is_err());
        assert!(parse_date("2024").is_err());
    }

    #[test]
    fn test_parse_with_whitespace() {
        let d = parse_date("  2024-01-15  ").unwrap();
        assert_eq!(d.year(), 2024);
    }

    #[test]
    fn test_fmt_date_default() {
        let d = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        assert_eq!(fmt_date(d, None), "20240115");
    }

    #[test]
    fn test_fmt_date_custom() {
        let d = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        assert_eq!(fmt_date(d, Some("%Y-%m-%d")), "2024-01-15");
    }

    #[test]
    fn test_all_formats_same_result() {
        let dates = [
            "2024-01-15",
            "20240115",
            "2024/01/15",
            "15-01-2024",
            "15/01/2024",
        ];

        let expected = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        for s in dates {
            assert_eq!(parse_date(s).unwrap(), expected, "Failed for: {}", s);
        }
    }
}
