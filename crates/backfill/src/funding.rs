//! Month-partitioned funding-history driver, venue-generic over
//! [`FundingHistorySource`]. A published month file is its own completion
//! marker; the current month is a `.partial` refreshed per run and replaced
//! by the final file on the first run after the month closes.

use crate::{
    now_nanos, publish, tmp_path, BackfillError, FundingHistorySource, FundingPoint, Month,
};
use recorder::tables::FundingRealizedTable;
use std::path::{Path, PathBuf};
use venue_core::SourceId;

pub struct FundingBackfillCfg {
    /// `data/backfill` — datasets land under `<root>/<venue>/funding/`.
    pub out_root: PathBuf,
    /// `data/meta` — raw instruments dumps from `list_perps`.
    pub meta_root: PathBuf,
    /// First month to fetch; default = the earliest onboard month the venue
    /// reports (only currently-listed symbols carry onboard info, so pass an
    /// explicit `--from` to reach further back for delisted ones).
    pub from: Option<Month>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum MonthOutcome {
    /// Closed month written and atomically published.
    Published { rows: usize },
    /// Current month refreshed as `<m>.partial.parquet`.
    Partial { rows: usize },
    /// Closed month already has its marker file; not refetched.
    AlreadyPublished,
    /// Closed month fetched empty (pre-listing); no file written, refetched
    /// on the next run — one cheap request, not worth a marker format.
    Empty,
}

pub async fn run<S: FundingHistorySource>(
    src: &S,
    cfg: &FundingBackfillCfg,
) -> Result<Vec<(Month, MonthOutcome)>, BackfillError> {
    let perps = src.list_perps(&cfg.meta_root).await?;
    tracing::info!(venue = src.venue(), perps = perps.len(), "perps listed");

    let from = match cfg.from {
        Some(m) => m,
        None => perps
            .iter()
            .filter_map(|p| p.onboard_ms)
            .min()
            .map(Month::of_ms)
            .ok_or_else(|| {
                BackfillError::Invalid("venue reports no onboard dates; pass --from YYYY-MM".into())
            })?,
    };

    let dir = cfg.out_root.join(src.venue()).join("funding");
    std::fs::create_dir_all(&dir)?;
    clean_stale_tmps(&dir)?;

    let current = Month::current();
    if from > current {
        return Err(BackfillError::Invalid(format!(
            "--from {from} is in the future"
        )));
    }
    let now_ms = now_nanos() / 1_000_000;
    let mut outcomes = Vec::new();

    for m in from.through(current) {
        let final_path = dir.join(format!("{m}.parquet"));
        let partial_path = dir.join(format!("{m}.partial.parquet"));
        let closed = m < current;

        if closed && final_path.exists() {
            outcomes.push((m, MonthOutcome::AlreadyPublished));
            continue;
        }

        let (start_ms, end_ms) = m.bounds_ms();
        let end_eff = end_ms.min(now_ms);
        let mut rows = if src.venue_wide_history() {
            src.fetch_funding(None, start_ms, end_eff).await?
        } else {
            let mut all = Vec::new();
            for perp in &perps {
                // Skip symbols that listed after this month; their windows
                // would be guaranteed-empty requests.
                if perp.onboard_ms.is_some_and(|on| on >= end_eff) {
                    continue;
                }
                all.extend(
                    src.fetch_funding(Some(&perp.symbol), start_ms, end_eff)
                        .await?,
                );
            }
            all
        };

        if rows.is_empty() {
            if !closed {
                outcomes.push((m, MonthOutcome::Partial { rows: 0 }));
            } else {
                outcomes.push((m, MonthOutcome::Empty));
            }
            tracing::info!(venue = src.venue(), month = %m, "no settlements in window");
            continue;
        }
        // Venue-wide pages arrive time-ordered; per-symbol concat does not.
        // One sort keeps row groups time-clustered either way.
        rows.sort_by(|a, b| {
            (a.funding_time_ms, a.symbol.as_str()).cmp(&(b.funding_time_ms, b.symbol.as_str()))
        });

        let target = if closed { &final_path } else { &partial_path };
        let tmp = tmp_path(target);
        let written = write_month(&dir, &tmp, &rows)?;
        publish(&tmp, target)?;
        if closed && partial_path.exists() {
            // Superseded by the final file.
            std::fs::remove_file(&partial_path)?;
        }
        tracing::info!(
            venue = src.venue(),
            month = %m,
            rows = written,
            path = %target.display(),
            "funding month published"
        );
        outcomes.push((
            m,
            if closed {
                MonthOutcome::Published { rows: written }
            } else {
                MonthOutcome::Partial { rows: written }
            },
        ));
    }

    Ok(outcomes)
}

fn write_month(dir: &Path, tmp: &Path, rows: &[FundingPoint]) -> Result<usize, BackfillError> {
    let file_name = tmp
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    let mut table = FundingRealizedTable::with_file_name(dir, &file_name);
    let local_ts = now_nanos();
    for row in rows {
        let ft_ns = row.funding_time_ms * 1_000_000;
        table
            .push_row(
                &row.symbol.to_lowercase(),
                Some(ft_ns),
                local_ts,
                SourceId::REST,
                &row.rate,
                ft_ns,
                // Today's interval would be wrong for symbols whose cadence
                // changed; research derives the realized interval from
                // consecutive settlements instead. Live rows stamp it.
                None,
            )
            .map_err(|e| BackfillError::Table(e.to_string()))?;
    }
    table
        .finish()
        .map_err(|e| BackfillError::Table(e.to_string()))
}

fn clean_stale_tmps(dir: &Path) -> Result<(), BackfillError> {
    for entry in std::fs::read_dir(dir)?.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with(".tmp-") {
            tracing::warn!(file = %name, "removing stale tmp (crashed run?)");
            std::fs::remove_file(entry.path())?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PerpMeta;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use rust_decimal_macros::dec;
    use std::sync::Mutex;

    /// Canned source: rows in 2024-01 (closed) and one at the start of the
    /// current month. Counts fetches to prove idempotency.
    struct MockSource {
        rows: Vec<FundingPoint>,
        fetches: Mutex<Vec<(u64, u64)>>,
    }

    impl MockSource {
        fn new() -> Self {
            let (cur_start, _) = Month::current().bounds_ms();
            let jan = Month::parse("2024-01").unwrap().bounds_ms().0;
            Self {
                rows: vec![
                    FundingPoint {
                        symbol: "BTCUSDT".into(),
                        funding_time_ms: jan + 8 * 3_600_000,
                        rate: dec!(0.0001),
                    },
                    FundingPoint {
                        symbol: "ETHUSDT".into(),
                        funding_time_ms: jan + 8 * 3_600_000,
                        rate: dec!(-0.0002),
                    },
                    FundingPoint {
                        symbol: "BTCUSDT".into(),
                        funding_time_ms: cur_start,
                        rate: dec!(0.0003),
                    },
                ],
                fetches: Mutex::new(Vec::new()),
            }
        }
    }

    impl FundingHistorySource for MockSource {
        fn venue(&self) -> &'static str {
            "mockx"
        }

        fn venue_wide_history(&self) -> bool {
            true
        }

        async fn list_perps(&self, _meta_dir: &Path) -> Result<Vec<PerpMeta>, BackfillError> {
            Ok(vec![PerpMeta {
                symbol: "BTCUSDT".into(),
                onboard_ms: Some(Month::parse("2024-01").unwrap().bounds_ms().0),
                funding_interval_ns: None,
            }])
        }

        async fn fetch_funding(
            &self,
            _symbol: Option<&str>,
            start_ms: u64,
            end_ms: u64,
        ) -> Result<Vec<FundingPoint>, BackfillError> {
            self.fetches.lock().unwrap().push((start_ms, end_ms));
            Ok(self
                .rows
                .iter()
                .filter(|r| r.funding_time_ms >= start_ms && r.funding_time_ms < end_ms)
                .cloned()
                .collect())
        }
    }

    fn cfg(tmp: &Path) -> FundingBackfillCfg {
        FundingBackfillCfg {
            out_root: tmp.join("backfill"),
            meta_root: tmp.join("meta"),
            from: None, // derives 2024-01 from onboard
        }
    }

    #[tokio::test]
    async fn publishes_closed_months_and_partials_current() {
        let tmp = tempfile::tempdir().unwrap();
        let src = MockSource::new();
        let outcomes = run(&src, &cfg(tmp.path())).await.unwrap();

        let dir = tmp.path().join("backfill/mockx/funding");
        assert_eq!(
            outcomes[0],
            (
                Month::parse("2024-01").unwrap(),
                MonthOutcome::Published { rows: 2 }
            )
        );
        assert!(dir.join("2024-01.parquet").exists());

        let (current, outcome) = outcomes.last().unwrap();
        assert_eq!(*current, Month::current());
        assert_eq!(*outcome, MonthOutcome::Partial { rows: 1 });
        assert!(dir.join(format!("{current}.partial.parquet")).exists());
        assert!(!dir.join(format!("{current}.parquet")).exists());

        // In-between months were fetched, found empty, left file-less.
        assert!(outcomes[1..outcomes.len() - 1]
            .iter()
            .all(|(_, o)| *o == MonthOutcome::Empty));
        let no_tmps = std::fs::read_dir(&dir)
            .unwrap()
            .flatten()
            .all(|e| !e.file_name().to_string_lossy().starts_with(".tmp-"));
        assert!(no_tmps);
    }

    #[tokio::test]
    async fn published_months_are_not_refetched() {
        let tmp = tempfile::tempdir().unwrap();
        let src = MockSource::new();
        run(&src, &cfg(tmp.path())).await.unwrap();
        let first_fetches = src.fetches.lock().unwrap().len();

        let outcomes = run(&src, &cfg(tmp.path())).await.unwrap();
        assert_eq!(outcomes[0].1, MonthOutcome::AlreadyPublished);
        let jan_start = Month::parse("2024-01").unwrap().bounds_ms().0;
        let second_run: Vec<(u64, u64)> = src.fetches.lock().unwrap()[first_fetches..].to_vec();
        assert!(
            second_run.iter().all(|(s, _)| *s > jan_start),
            "published 2024-01 must not be refetched: {second_run:?}"
        );
        // Current month refreshed again.
        assert_eq!(
            outcomes.last().unwrap().1,
            MonthOutcome::Partial { rows: 1 }
        );
    }

    #[tokio::test]
    async fn backfill_schema_matches_live_table_schema() {
        let tmp = tempfile::tempdir().unwrap();
        let src = MockSource::new();
        run(&src, &cfg(tmp.path())).await.unwrap();

        // Live-shaped reference file via the same public writer the WAL
        // converter uses (guards against the backfill path growing its own
        // diverging writer).
        let live_dir = tmp.path().join("live");
        std::fs::create_dir_all(&live_dir).unwrap();
        let mut live = FundingRealizedTable::new(&live_dir);
        live.push_row(
            "btcusdt",
            Some(1),
            2,
            SourceId(1),
            &dec!(0.0001),
            1,
            Some(28_800_000_000_000),
        )
        .unwrap();
        live.finish().unwrap();

        let schema_of = |p: &Path| {
            let f = std::fs::File::open(p).unwrap();
            ParquetRecordBatchReaderBuilder::try_new(f)
                .unwrap()
                .schema()
                .clone()
        };
        let backfilled = schema_of(&tmp.path().join("backfill/mockx/funding/2024-01.parquet"));
        let live = schema_of(&live_dir.join("funding_rate_realized.parquet"));
        assert_eq!(
            backfilled, live,
            "backfill and live schemas must be identical"
        );
    }
}
