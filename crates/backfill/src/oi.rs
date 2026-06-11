//! Open-interest history backfill — the perishable dataset:
//! `/futures/data/openInterestHist` retains only ~30 days at 5 m grain, and
//! windows past the edge fail with code -1130 (live-verified 2026-06-11), so
//! every week of delay permanently loses history. A daily timer runs this
//! until the live OI poller has accumulated ≥ 30 days of coverage.
//!
//! Day-partitioned (`<YYYY-MM-DD>.parquet`): one closed UTC day is one
//! request per perp (288 five-minute points < the 500 limit), so the daily
//! incremental run is ~2× the perp count in requests. Today refreshes as
//! `.partial`.

use crate::{now_nanos, publish, tmp_path, BackfillError, PerpMeta};
use chrono::{NaiveDate, Utc};
use recorder::tables::OpenInterestTable;
use rust_decimal::Decimal;
use std::future::Future;
use std::path::{Path, PathBuf};
use venue_core::SourceId;

/// One `openInterestHist` point (5 m grain).
#[derive(Debug, Clone)]
pub struct OiPoint {
    pub ts_ms: u64,
    pub sum_open_interest: Decimal,
    pub sum_open_interest_value: Decimal,
}

/// Venue OI-history capability; Binance-only today, kept as a trait for the
/// driver's tests.
pub trait OiHistSource {
    fn venue(&self) -> &'static str;

    fn list_perps(
        &self,
        meta_dir: &Path,
    ) -> impl Future<Output = Result<Vec<PerpMeta>, BackfillError>> + Send;

    /// 5 m points in [start_ms, end_ms); a window past the venue's retention
    /// returns `Ok(vec![])`, not an error.
    fn fetch_oi_hist(
        &self,
        symbol: &str,
        start_ms: u64,
        end_ms: u64,
    ) -> impl Future<Output = Result<Vec<OiPoint>, BackfillError>> + Send;
}

pub struct OiHistCfg {
    pub out_root: PathBuf,
    pub meta_root: PathBuf,
    /// How far back to attempt; the venue edge (~30 d) truncates gracefully.
    pub days_back: u32,
}

impl Default for OiHistCfg {
    fn default() -> Self {
        Self {
            out_root: PathBuf::from("data/backfill"),
            meta_root: PathBuf::from("data/meta"),
            days_back: 32,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum DayOutcome {
    Published {
        rows: usize,
    },
    Partial {
        rows: usize,
    },
    AlreadyPublished,
    /// Nothing returned for any perp — out of venue retention (or pre-data).
    Empty,
}

pub async fn run<S: OiHistSource>(
    src: &S,
    cfg: &OiHistCfg,
) -> Result<Vec<(NaiveDate, DayOutcome)>, BackfillError> {
    let perps = src.list_perps(&cfg.meta_root).await?;
    tracing::info!(venue = src.venue(), perps = perps.len(), "perps listed");

    let dir = cfg.out_root.join(src.venue()).join("oi_hist");
    std::fs::create_dir_all(&dir)?;
    for entry in std::fs::read_dir(&dir)?.flatten() {
        if entry.file_name().to_string_lossy().starts_with(".tmp-") {
            std::fs::remove_file(entry.path())?;
        }
    }

    let today = Utc::now().date_naive();
    let first = today - chrono::Days::new(cfg.days_back as u64);
    let now_ms = now_nanos() / 1_000_000;
    let mut outcomes = Vec::new();

    let mut day = first;
    while day <= today {
        let closed = day < today;
        let final_path = dir.join(format!("{day}.parquet"));
        let partial_path = dir.join(format!("{day}.partial.parquet"));
        if closed && final_path.exists() {
            outcomes.push((day, DayOutcome::AlreadyPublished));
            day = day.succ_opt().expect("no calendar overflow");
            continue;
        }

        let start_ms = day
            .and_hms_opt(0, 0, 0)
            .expect("midnight exists")
            .and_utc()
            .timestamp_millis() as u64;
        let end_ms = (start_ms + 86_400_000).min(now_ms);

        let target = if closed { &final_path } else { &partial_path };
        let tmp = tmp_path(target);
        let file_name = tmp
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        let mut table = OpenInterestTable::with_file_name(&dir, &file_name);
        let local_ts = now_nanos();
        let mut rows = 0usize;
        for perp in &perps {
            if perp.onboard_ms.is_some_and(|on| on >= end_ms) {
                continue;
            }
            let points = src.fetch_oi_hist(&perp.symbol, start_ms, end_ms).await?;
            let symbol = perp.symbol.to_lowercase();
            for p in points {
                table
                    .push_row(
                        &symbol,
                        Some(p.ts_ms * 1_000_000),
                        local_ts,
                        SourceId::REST,
                        &p.sum_open_interest,
                        Some(&p.sum_open_interest_value),
                    )
                    .map_err(|e| BackfillError::Table(e.to_string()))?;
                rows += 1;
            }
        }
        let written = table
            .finish()
            .map_err(|e| BackfillError::Table(e.to_string()))?;
        debug_assert_eq!(written, rows);

        if rows == 0 {
            // Lazy writer created no file; nothing to publish.
            outcomes.push((day, DayOutcome::Empty));
            tracing::info!(venue = src.venue(), %day, "no OI history (out of retention?)");
        } else {
            publish(&tmp, target)?;
            if closed && partial_path.exists() {
                std::fs::remove_file(&partial_path)?;
            }
            tracing::info!(
                venue = src.venue(),
                %day,
                rows,
                path = %target.display(),
                "OI day published"
            );
            outcomes.push((
                day,
                if closed {
                    DayOutcome::Published { rows }
                } else {
                    DayOutcome::Partial { rows }
                },
            ));
        }
        day = day.succ_opt().expect("no calendar overflow");
    }

    Ok(outcomes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use std::sync::Mutex;

    /// Retention boundary `edge_ms`: windows entirely before it are "out of
    /// retention" (empty, like the venue's -1130). Counts fetches.
    struct MockOi {
        edge_ms: u64,
        fetches: Mutex<u32>,
    }

    impl OiHistSource for MockOi {
        fn venue(&self) -> &'static str {
            "mockx"
        }

        async fn list_perps(&self, _meta: &Path) -> Result<Vec<PerpMeta>, BackfillError> {
            Ok(vec![PerpMeta {
                symbol: "BTCUSDT".into(),
                onboard_ms: Some(0),
                funding_interval_ns: None,
            }])
        }

        async fn fetch_oi_hist(
            &self,
            _symbol: &str,
            start_ms: u64,
            end_ms: u64,
        ) -> Result<Vec<OiPoint>, BackfillError> {
            *self.fetches.lock().unwrap() += 1;
            if end_ms <= self.edge_ms {
                return Ok(Vec::new());
            }
            // Two points per in-retention day; clamp to the edge.
            Ok([start_ms, start_ms + 300_000]
                .into_iter()
                .filter(|ts| *ts >= self.edge_ms && *ts < end_ms)
                .map(|ts| OiPoint {
                    ts_ms: ts,
                    sum_open_interest: dec!(100.5),
                    sum_open_interest_value: dec!(6000000),
                })
                .collect())
        }
    }

    fn day_start_ms(date: NaiveDate) -> u64 {
        date.and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp_millis() as u64
    }

    #[tokio::test]
    async fn out_of_retention_days_empty_in_retention_published_today_partial() {
        let tmp = tempfile::tempdir().unwrap();
        let today = Utc::now().date_naive();
        // Edge: 3 days ago — older days must come back Empty, file-less.
        let edge = day_start_ms(today - chrono::Days::new(3));
        let src = MockOi {
            edge_ms: edge,
            fetches: Mutex::new(0),
        };
        let cfg = OiHistCfg {
            out_root: tmp.path().join("backfill"),
            meta_root: tmp.path().join("meta"),
            days_back: 6,
        };
        let outcomes = run(&src, &cfg).await.unwrap();
        assert_eq!(outcomes.len(), 7);

        let dir = tmp.path().join("backfill/mockx/oi_hist");
        for (day, outcome) in &outcomes {
            if *day == today {
                assert_eq!(*outcome, DayOutcome::Partial { rows: 2 });
                assert!(dir.join(format!("{day}.partial.parquet")).exists());
            } else if day_start_ms(*day) >= edge {
                assert_eq!(*outcome, DayOutcome::Published { rows: 2 }, "{day}");
                assert!(dir.join(format!("{day}.parquet")).exists());
            } else {
                assert_eq!(*outcome, DayOutcome::Empty, "{day}");
                assert!(!dir.join(format!("{day}.parquet")).exists());
            }
        }

        // Idempotency: published days skipped; empty + today refetched.
        let before = *src.fetches.lock().unwrap();
        let again = run(&src, &cfg).await.unwrap();
        let refetched = *src.fetches.lock().unwrap() - before;
        assert_eq!(refetched, 4, "3 empty + 1 today");
        assert!(again
            .iter()
            .filter(|(d, _)| *d != today && day_start_ms(*d) >= edge)
            .all(|(_, o)| *o == DayOutcome::AlreadyPublished));
    }
}
