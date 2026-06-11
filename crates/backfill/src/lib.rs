//! REST history backfill (A5): months of funding/OI/kline history are
//! downloadable today and are the fastest route to validating strategy
//! economics — research must not queue behind live capture.
//!
//! Backfill output is *derived, refetchable* data, so it bypasses the WAL
//! (which records live observations) and writes Parquet directly to
//! `data/backfill/<venue>/<dataset>/<YYYY-MM>.parquet`, month-partitioned,
//! published atomically (tmp + rename; the final file is the completion
//! marker, the wal-sweep idiom). The current month lands as
//! `<YYYY-MM>.partial.parquet`, refreshed each run and replaced by the
//! final file once the month closes. Schemas are identical to the live
//! tables (`recorder::tables`); provenance = the `source` column (0 = REST)
//! plus the backfill root; `local_ts` = fetch time, visibly late vs
//! `venue_ts`.

use chrono::{Datelike, NaiveDate, Utc};
use rust_decimal::Decimal;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::time::Duration;

pub mod binance;
pub mod bybit;
pub mod funding;
pub mod klines;
pub mod oi;
pub mod reconcile;

#[derive(Debug)]
pub enum BackfillError {
    Http(String),
    Parse(String),
    Io(std::io::Error),
    Table(String),
    Invalid(String),
}

impl std::fmt::Display for BackfillError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BackfillError::Http(e) => write!(f, "http: {e}"),
            BackfillError::Parse(e) => write!(f, "parse: {e}"),
            BackfillError::Io(e) => write!(f, "io: {e}"),
            BackfillError::Table(e) => write!(f, "table: {e}"),
            BackfillError::Invalid(e) => write!(f, "invalid: {e}"),
        }
    }
}

impl std::error::Error for BackfillError {}

impl From<std::io::Error> for BackfillError {
    fn from(e: std::io::Error) -> Self {
        BackfillError::Io(e)
    }
}

pub fn now_nanos() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

pub fn ms_to_nanos(ms: u64) -> u64 {
    ms * 1_000_000
}

/// Minimum gap between REST requests. Backfill is a batch job sharing the
/// IP budget with live capture pollers — pace conservatively (~4 req/s) and
/// let venue weight headers be the audit trail.
pub struct Pacer {
    min_gap: Duration,
    next: Option<tokio::time::Instant>,
}

impl Pacer {
    pub fn new(min_gap: Duration) -> Self {
        Self {
            min_gap,
            next: None,
        }
    }

    pub async fn wait(&mut self) {
        if let Some(next) = self.next {
            tokio::time::sleep_until(next).await;
        }
        self.next = Some(tokio::time::Instant::now() + self.min_gap);
    }
}

impl Default for Pacer {
    fn default() -> Self {
        Self::new(Duration::from_millis(250))
    }
}

/// One calendar month, the backfill partition unit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Month {
    pub year: i32,
    pub month: u32,
}

impl Month {
    pub fn parse(s: &str) -> Result<Self, BackfillError> {
        let (y, m) = s
            .split_once('-')
            .ok_or_else(|| BackfillError::Invalid(format!("month {s:?}: want YYYY-MM")))?;
        let year: i32 = y
            .parse()
            .map_err(|_| BackfillError::Invalid(format!("month {s:?}: bad year")))?;
        let month: u32 = m
            .parse()
            .map_err(|_| BackfillError::Invalid(format!("month {s:?}: bad month")))?;
        if !(1..=12).contains(&month) {
            return Err(BackfillError::Invalid(format!("month {s:?}: 01..=12")));
        }
        Ok(Self { year, month })
    }

    pub fn of_ms(ms: u64) -> Self {
        let date = chrono::DateTime::from_timestamp_millis(ms as i64)
            .unwrap_or_default()
            .date_naive();
        Self {
            year: date.year(),
            month: date.month(),
        }
    }

    pub fn current() -> Self {
        let today = Utc::now().date_naive();
        Self {
            year: today.year(),
            month: today.month(),
        }
    }

    pub fn next(self) -> Self {
        if self.month == 12 {
            Self {
                year: self.year + 1,
                month: 1,
            }
        } else {
            Self {
                year: self.year,
                month: self.month + 1,
            }
        }
    }

    /// UTC [start, end) in epoch ms.
    pub fn bounds_ms(self) -> (u64, u64) {
        let start = NaiveDate::from_ymd_opt(self.year, self.month, 1)
            .expect("valid month")
            .and_hms_opt(0, 0, 0)
            .expect("midnight exists")
            .and_utc()
            .timestamp_millis() as u64;
        let n = self.next();
        let end = NaiveDate::from_ymd_opt(n.year, n.month, 1)
            .expect("valid month")
            .and_hms_opt(0, 0, 0)
            .expect("midnight exists")
            .and_utc()
            .timestamp_millis() as u64;
        (start, end)
    }

    /// All months from `self` through `last`, inclusive.
    pub fn through(self, last: Month) -> Vec<Month> {
        let mut out = Vec::new();
        let mut m = self;
        while m <= last {
            out.push(m);
            m = m.next();
        }
        out
    }
}

impl std::fmt::Display for Month {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:04}-{:02}", self.year, self.month)
    }
}

/// One realized funding settlement as venues report it over REST.
#[derive(Debug, Clone)]
pub struct FundingPoint {
    /// Venue-raw symbol, verbatim case (lowercased at write time).
    pub symbol: String,
    pub funding_time_ms: u64,
    pub rate: Decimal,
}

/// A venue's perp, as listed by its instruments endpoint.
#[derive(Debug, Clone)]
pub struct PerpMeta {
    /// Venue-raw symbol, verbatim case.
    pub symbol: String,
    pub onboard_ms: Option<u64>,
    pub funding_interval_ns: Option<u64>,
}

/// REST funding-history source for one venue. Implementations own their
/// paging and pacing; `fetch_funding` returns *all* points in [start, end),
/// already deduplicated. Sized for exactly two impls (Binance, Bybit) —
/// abstractions follow second implementations.
pub trait FundingHistorySource {
    fn venue(&self) -> &'static str;

    /// Whether one venue-wide chronological pass covers all symbols
    /// (Binance); otherwise the driver iterates `list_perps` (Bybit).
    fn venue_wide_history(&self) -> bool;

    /// List perps and, as a side effect, dump the raw instruments response
    /// to `meta_dir/<venue>/<date>-….json` if not already present — the
    /// symbology/SCD input for venues without a live capture process.
    fn list_perps(
        &self,
        meta_dir: &Path,
    ) -> impl Future<Output = Result<Vec<PerpMeta>, BackfillError>> + Send;

    /// All settlements in [start_ms, end_ms) for `symbol` (or every symbol
    /// when `None` and `venue_wide_history()`).
    fn fetch_funding(
        &self,
        symbol: Option<&str>,
        start_ms: u64,
        end_ms: u64,
    ) -> impl Future<Output = Result<Vec<FundingPoint>, BackfillError>> + Send;
}

/// Atomically publish `tmp` as `final_path` (same directory rename).
pub fn publish(tmp: &Path, final_path: &Path) -> Result<(), BackfillError> {
    std::fs::rename(tmp, final_path)?;
    Ok(())
}

/// `<dataset dir>/.tmp-<file>` — crash leftovers are cleaned on the next run.
pub fn tmp_path(final_path: &Path) -> PathBuf {
    let dir = final_path.parent().unwrap_or_else(|| Path::new("."));
    let name = final_path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    dir.join(format!(".tmp-{name}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn month_parse_display_roundtrip() {
        let m = Month::parse("2023-07").unwrap();
        assert_eq!(
            m,
            Month {
                year: 2023,
                month: 7
            }
        );
        assert_eq!(m.to_string(), "2023-07");
        assert!(Month::parse("2023-13").is_err());
        assert!(Month::parse("202307").is_err());
        assert!(Month::parse("y-07").is_err());
    }

    #[test]
    fn month_bounds_and_iteration() {
        let m = Month::parse("2023-12").unwrap();
        let (start, end) = m.bounds_ms();
        assert_eq!(start, 1_701_388_800_000); // 2023-12-01T00:00:00Z
        assert_eq!(end, 1_704_067_200_000); // 2024-01-01T00:00:00Z

        let months = Month::parse("2023-11").unwrap().through(m.next());
        let strs: Vec<String> = months.iter().map(|m| m.to_string()).collect();
        assert_eq!(strs, ["2023-11", "2023-12", "2024-01"]);

        assert_eq!(Month::of_ms(start), m);
        assert_eq!(Month::of_ms(end - 1), m);
        assert_eq!(Month::of_ms(end), m.next());
    }

    #[test]
    fn tmp_path_stays_in_dataset_dir() {
        let f = Path::new("/data/backfill/binance/funding/2024-01.parquet");
        assert_eq!(
            tmp_path(f),
            Path::new("/data/backfill/binance/funding/.tmp-2024-01.parquet")
        );
    }
}
