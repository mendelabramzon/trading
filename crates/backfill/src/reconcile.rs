//! Daily funding reconciliation (A5): captured `FundingRateRealized` events
//! vs an independent REST refetch, per UTC day.
//!
//! Honest caveat, stated once: the live realized-funding poller is itself
//! REST-sourced (the WS markPrice family is dead), so this does not compare
//! two independent observation channels — it verifies *pipeline completeness
//! end-to-end* (poller uptime → WAL → conversion → publish) against a fresh
//! refetch. `rate_mismatches` becomes load-bearing if a WS source revives.
//!
//! Captured set for day D = the published `funding_rate_realized.parquet`
//! of D (publish marker required, else `blocked`) + D+1's published parquet
//! when present + a scan of D+1's WAL (late capture: a settlement at 23:59
//! is discovered by a poll after midnight and lands in D+1's file). Dedup
//! by (instrument, funding_time).

use crate::{BackfillError, FundingHistorySource};
use chrono::NaiveDate;
use num_traits::ToPrimitive;
use serde::Serialize;
use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use venue_core::{MarketPayload, Payload};

/// f64 round-trip slack for 8-decimal funding rates; anything larger is a
/// real disagreement.
const RATE_EPS: f64 = 1e-10;

/// Bound the green-day scan; the exit criterion needs 14.
const MAX_GREEN_SCAN_DAYS: u32 = 400;

pub struct ReconcileCfg {
    pub data_dir: PathBuf,
    /// UTC day to reconcile (default: yesterday).
    pub date: NaiveDate,
    /// Re-evaluate and overwrite an existing report.
    pub force: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReconcileStatus {
    Pass,
    Fail,
    /// Inputs unavailable (day not converted / QA failed); nothing to judge.
    Blocked,
}

#[derive(Debug, Serialize)]
pub struct FundingMiss {
    pub instrument: String,
    pub funding_time_ns: u64,
    pub rest_rate: f64,
}

#[derive(Debug, Serialize)]
pub struct FundingExtra {
    pub instrument: String,
    pub funding_time_ns: u64,
    pub captured_rate: f64,
}

#[derive(Debug, Serialize)]
pub struct RateMismatch {
    pub instrument: String,
    pub funding_time_ns: u64,
    pub captured_rate: f64,
    pub rest_rate: f64,
}

#[derive(Debug, Serialize)]
pub struct ReconcileReport {
    pub schema_version: u32,
    pub venue: String,
    pub date: String,
    pub generated_at: String,
    pub status: ReconcileStatus,
    /// Present only for `blocked`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blocked_reason: Option<String>,
    pub expected_events: usize,
    pub captured_events: usize,
    pub coverage_pct: f64,
    pub missing: Vec<FundingMiss>,
    pub extra: Vec<FundingExtra>,
    pub rate_mismatches: Vec<RateMismatch>,
    /// Consecutive pass days ending at `date`, this report included — the
    /// Phase-2 exit criterion reads `>= 14` off the latest report.
    pub consecutive_green_days: u32,
}

#[derive(Debug)]
pub struct Outcome {
    pub status: ReconcileStatus,
    pub report_path: PathBuf,
    /// True when an existing report was honored instead of re-evaluating.
    pub skipped_existing: bool,
}

type Key = (String, u64);

pub async fn run<S: FundingHistorySource>(
    src: &S,
    cfg: &ReconcileCfg,
) -> Result<Outcome, BackfillError> {
    let venue = src.venue();
    let reports_dir = cfg.data_dir.join("meta/reconciliation").join(venue);
    let report_path = reports_dir.join(format!("{}.json", cfg.date));

    if report_path.exists() && !cfg.force {
        let status = read_report_status(&report_path)?;
        tracing::info!(%venue, date = %cfg.date, ?status, "report exists; honoring it (use --force to redo)");
        return Ok(Outcome {
            status,
            report_path,
            skipped_existing: true,
        });
    }

    let start_ns = day_start_ns(cfg.date);
    let end_ns = day_start_ns(cfg.date.succ_opt().expect("no calendar overflow"));

    // Expected: independent venue-wide refetch for the day.
    let expected_points = src
        .fetch_funding(None, start_ns / 1_000_000, end_ns / 1_000_000)
        .await?;
    let mut expected: HashMap<Key, f64> = HashMap::new();
    for p in expected_points {
        let rate = p.rate.to_f64().unwrap_or(f64::NAN);
        expected.insert(
            (p.symbol.to_lowercase(), p.funding_time_ms * 1_000_000),
            rate,
        );
    }

    // Captured: published parquet of D (required), plus D+1 spillover.
    let day_dir = cfg
        .data_dir
        .join("parquet")
        .join(venue)
        .join(cfg.date.to_string());
    if let Some(reason) = publish_gate(&day_dir) {
        let report = blocked_report(venue, cfg.date, expected.len(), reason.clone());
        write_report(&report_path, &report)?;
        tracing::warn!(%venue, date = %cfg.date, reason, "reconciliation blocked");
        return Ok(Outcome {
            status: ReconcileStatus::Blocked,
            report_path,
            skipped_existing: false,
        });
    }

    let mut captured: HashMap<Key, f64> = HashMap::new();
    collect_parquet(
        &day_dir.join("funding_rate_realized.parquet"),
        start_ns,
        end_ns,
        &mut captured,
    )?;

    let next_date = cfg.date.succ_opt().expect("no calendar overflow");
    let next_dir = cfg
        .data_dir
        .join("parquet")
        .join(venue)
        .join(next_date.to_string());
    if publish_gate(&next_dir).is_none() {
        collect_parquet(
            &next_dir.join("funding_rate_realized.parquet"),
            start_ns,
            end_ns,
            &mut captured,
        )?;
    }
    let next_wal = cfg
        .data_dir
        .join("wal")
        .join(venue)
        .join(format!("{next_date}.wal"));
    if next_wal.exists() {
        collect_wal(&next_wal, start_ns, end_ns, &mut captured)?;
    }

    let report = build_report(venue, cfg.date, &expected, &captured, &reports_dir);
    write_report(&report_path, &report)?;
    tracing::info!(
        %venue,
        date = %cfg.date,
        status = ?report.status,
        expected = report.expected_events,
        captured = report.captured_events,
        coverage_pct = report.coverage_pct,
        missing = report.missing.len(),
        extra = report.extra.len(),
        mismatches = report.rate_mismatches.len(),
        green_days = report.consecutive_green_days,
        "reconciliation evaluated"
    );
    Ok(Outcome {
        status: report.status,
        report_path,
        skipped_existing: false,
    })
}

fn day_start_ns(date: NaiveDate) -> u64 {
    date.and_hms_opt(0, 0, 0)
        .expect("midnight exists")
        .and_utc()
        .timestamp_nanos_opt()
        .expect("in range") as u64
}

/// `None` when the day directory is published with a passing QA report;
/// otherwise the blocking reason.
fn publish_gate(day_dir: &Path) -> Option<String> {
    let marker = day_dir.join(recorder::sweep::QA_REPORT_FILE);
    let raw = match std::fs::read(&marker) {
        Ok(raw) => raw,
        Err(_) => return Some(format!("not published: {} missing", marker.display())),
    };
    match serde_json::from_slice::<serde_json::Value>(&raw) {
        Ok(v) if v["status"] == "pass" => None,
        Ok(v) => Some(format!(
            "QA status {:?} for {}",
            v["status"],
            day_dir.display()
        )),
        Err(e) => Some(format!("unreadable QA report: {e}")),
    }
}

fn collect_parquet(
    path: &Path,
    start_ns: u64,
    end_ns: u64,
    captured: &mut HashMap<Key, f64>,
) -> Result<(), BackfillError> {
    use arrow::array::{Array, Float64Array, StringArray, TimestampNanosecondArray};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let file = match File::open(path) {
        Ok(f) => f,
        // A published day with zero realized rows legitimately has no file
        // (lazy writers); the comparison decides whether that's a miss.
        Err(_) => return Ok(()),
    };
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| BackfillError::Table(e.to_string()))?
        .build()
        .map_err(|e| BackfillError::Table(e.to_string()))?;
    for batch in reader {
        let batch = batch.map_err(|e| BackfillError::Table(e.to_string()))?;
        let schema = batch.schema();
        let col = |name: &str| {
            schema
                .index_of(name)
                .map_err(|e| BackfillError::Table(e.to_string()))
        };
        let instruments = batch
            .column(col("instrument")?)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| BackfillError::Table("instrument column type".into()))?;
        let times = batch
            .column(col("funding_time")?)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .ok_or_else(|| BackfillError::Table("funding_time column type".into()))?;
        let rates = batch
            .column(col("rate")?)
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| BackfillError::Table("rate column type".into()))?;
        for i in 0..batch.num_rows() {
            let ft = times.value(i) as u64;
            if ft < start_ns || ft >= end_ns {
                continue;
            }
            let rate = if rates.is_null(i) {
                f64::NAN
            } else {
                rates.value(i)
            };
            captured.insert((instruments.value(i).to_string(), ft), rate);
        }
    }
    Ok(())
}

fn collect_wal(
    path: &Path,
    start_ns: u64,
    end_ns: u64,
    captured: &mut HashMap<Key, f64>,
) -> Result<(), BackfillError> {
    let file = File::open(path)?;
    let mut reader = wire::FrameReader::new(BufReader::new(file));
    loop {
        match reader.next_event() {
            Ok(Some(event)) => {
                let Payload::Market(MarketPayload::FundingRateRealized {
                    rate, funding_time, ..
                }) = &event.payload
                else {
                    continue;
                };
                if *funding_time < start_ns || *funding_time >= end_ns {
                    continue;
                }
                let Some(instrument) = &event.instrument else {
                    continue;
                };
                captured.insert(
                    (instrument.value.to_string(), *funding_time),
                    rate.to_f64().unwrap_or(f64::NAN),
                );
            }
            Ok(None) => break,
            // An open WAL being written right now can have a torn tail;
            // FrameReader resyncs, and a hard error here must not fail the
            // whole reconciliation — the day's own data came from parquet.
            Err(e) => {
                tracing::warn!(path = %path.display(), error = %e, "WAL scan stopped early");
                break;
            }
        }
    }
    Ok(())
}

fn build_report(
    venue: &str,
    date: NaiveDate,
    expected: &HashMap<Key, f64>,
    captured: &HashMap<Key, f64>,
    reports_dir: &Path,
) -> ReconcileReport {
    let mut missing = Vec::new();
    let mut rate_mismatches = Vec::new();
    for (key, rest_rate) in expected {
        match captured.get(key) {
            None => missing.push(FundingMiss {
                instrument: key.0.clone(),
                funding_time_ns: key.1,
                rest_rate: *rest_rate,
            }),
            Some(captured_rate) if (captured_rate - rest_rate).abs() > RATE_EPS => {
                rate_mismatches.push(RateMismatch {
                    instrument: key.0.clone(),
                    funding_time_ns: key.1,
                    captured_rate: *captured_rate,
                    rest_rate: *rest_rate,
                });
            }
            Some(_) => {}
        }
    }
    let mut extra = Vec::new();
    for (key, captured_rate) in captured {
        if !expected.contains_key(key) {
            extra.push(FundingExtra {
                instrument: key.0.clone(),
                funding_time_ns: key.1,
                captured_rate: *captured_rate,
            });
        }
    }
    missing.sort_by(|a, b| {
        (a.funding_time_ns, &a.instrument).cmp(&(b.funding_time_ns, &b.instrument))
    });
    extra.sort_by(|a, b| {
        (a.funding_time_ns, &a.instrument).cmp(&(b.funding_time_ns, &b.instrument))
    });

    let covered = expected.len() - missing.len();
    let coverage_pct = if expected.is_empty() {
        100.0
    } else {
        covered as f64 * 100.0 / expected.len() as f64
    };
    let status = if missing.is_empty() && extra.is_empty() && rate_mismatches.is_empty() {
        ReconcileStatus::Pass
    } else {
        ReconcileStatus::Fail
    };

    let prior_green = consecutive_green_before(reports_dir, date);
    ReconcileReport {
        schema_version: 1,
        venue: venue.to_string(),
        date: date.to_string(),
        generated_at: chrono::Utc::now().to_rfc3339(),
        status,
        blocked_reason: None,
        expected_events: expected.len(),
        captured_events: captured.len(),
        coverage_pct,
        missing,
        extra,
        rate_mismatches,
        consecutive_green_days: if status == ReconcileStatus::Pass {
            prior_green + 1
        } else {
            0
        },
    }
}

fn blocked_report(
    venue: &str,
    date: NaiveDate,
    expected_events: usize,
    reason: String,
) -> ReconcileReport {
    ReconcileReport {
        schema_version: 1,
        venue: venue.to_string(),
        date: date.to_string(),
        generated_at: chrono::Utc::now().to_rfc3339(),
        status: ReconcileStatus::Blocked,
        blocked_reason: Some(reason),
        expected_events,
        captured_events: 0,
        coverage_pct: 0.0,
        missing: Vec::new(),
        extra: Vec::new(),
        rate_mismatches: Vec::new(),
        consecutive_green_days: 0,
    }
}

/// Consecutive pass days strictly before `date`, walking backwards through
/// prior report files; a gap (missing report) breaks the streak.
fn consecutive_green_before(reports_dir: &Path, date: NaiveDate) -> u32 {
    let mut count = 0;
    let mut d = date;
    for _ in 0..MAX_GREEN_SCAN_DAYS {
        let Some(prev) = d.pred_opt() else { break };
        d = prev;
        let path = reports_dir.join(format!("{d}.json"));
        match read_report_status(&path) {
            Ok(ReconcileStatus::Pass) => count += 1,
            _ => break,
        }
    }
    count
}

fn read_report_status(path: &Path) -> Result<ReconcileStatus, BackfillError> {
    let raw = std::fs::read(path)?;
    let v: serde_json::Value =
        serde_json::from_slice(&raw).map_err(|e| BackfillError::Parse(e.to_string()))?;
    match v["status"].as_str() {
        Some("pass") => Ok(ReconcileStatus::Pass),
        Some("fail") => Ok(ReconcileStatus::Fail),
        Some("blocked") => Ok(ReconcileStatus::Blocked),
        other => Err(BackfillError::Parse(format!(
            "unknown report status {other:?} in {}",
            path.display()
        ))),
    }
}

fn write_report(path: &Path, report: &ReconcileReport) -> Result<(), BackfillError> {
    let dir = path.parent().expect("report path has a parent");
    std::fs::create_dir_all(dir)?;
    let json =
        serde_json::to_vec_pretty(report).map_err(|e| BackfillError::Parse(e.to_string()))?;
    let part = path.with_extension("json.part");
    std::fs::write(&part, json)?;
    std::fs::rename(&part, path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{FundingPoint, PerpMeta};
    use recorder::tables::FundingRealizedTable;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use std::sync::Mutex;
    use venue_core::{Event, InstrumentId, SourceId, VenueId};

    const DAY: &str = "2026-06-09";
    const DAY_START_MS: u64 = 1_780_963_200_000; // 2026-06-09T00:00:00Z

    struct MockRest {
        points: Mutex<Vec<FundingPoint>>,
    }

    impl MockRest {
        fn new(points: Vec<FundingPoint>) -> Self {
            Self {
                points: Mutex::new(points),
            }
        }
    }

    impl FundingHistorySource for MockRest {
        fn venue(&self) -> &'static str {
            "binance"
        }

        fn venue_wide_history(&self) -> bool {
            true
        }

        async fn list_perps(&self, _meta: &Path) -> Result<Vec<PerpMeta>, BackfillError> {
            Ok(Vec::new())
        }

        async fn fetch_funding(
            &self,
            _symbol: Option<&str>,
            start_ms: u64,
            end_ms: u64,
        ) -> Result<Vec<FundingPoint>, BackfillError> {
            Ok(self
                .points
                .lock()
                .unwrap()
                .iter()
                .filter(|p| p.funding_time_ms >= start_ms && p.funding_time_ms < end_ms)
                .cloned()
                .collect())
        }
    }

    fn point(symbol: &str, offset_h: u64, rate: Decimal) -> FundingPoint {
        FundingPoint {
            symbol: symbol.into(),
            funding_time_ms: DAY_START_MS + offset_h * 3_600_000,
            rate,
        }
    }

    fn cfg(tmp: &Path) -> ReconcileCfg {
        ReconcileCfg {
            data_dir: tmp.to_path_buf(),
            date: DAY.parse().unwrap(),
            force: false,
        }
    }

    /// Publish a day dir with a passing marker and the given captured rows.
    fn publish_day(data_dir: &Path, date: &str, rows: &[FundingPoint]) {
        let dir = data_dir.join("parquet/binance").join(date);
        std::fs::create_dir_all(&dir).unwrap();
        if !rows.is_empty() {
            let mut t = FundingRealizedTable::new(&dir);
            for r in rows {
                let ns = r.funding_time_ms * 1_000_000;
                t.push_row(
                    &r.symbol.to_lowercase(),
                    Some(ns),
                    ns + 5_000_000_000,
                    SourceId::REST,
                    &r.rate,
                    ns,
                    None,
                )
                .unwrap();
            }
            t.finish().unwrap();
        }
        std::fs::write(
            dir.join(recorder::sweep::QA_REPORT_FILE),
            br#"{"status":"pass"}"#,
        )
        .unwrap();
    }

    fn write_next_day_wal(data_dir: &Path, rows: &[FundingPoint]) {
        let dir = data_dir.join("wal/binance");
        std::fs::create_dir_all(&dir).unwrap();
        let mut buf = Vec::new();
        for r in rows {
            let ns = r.funding_time_ms * 1_000_000;
            let event = Event {
                venue: VenueId {
                    value: "binance".into(),
                },
                instrument: Some(InstrumentId {
                    value: r.symbol.to_lowercase().into(),
                }),
                venue_ts: Some(ns),
                local_ts: ns + 300_000_000_000,
                source: SourceId::REST,
                provenance: None,
                payload: Payload::Market(MarketPayload::FundingRateRealized {
                    rate: r.rate,
                    funding_time: ns,
                    interval: None,
                }),
            };
            wire::encode_frame(&event, &mut buf).unwrap();
        }
        std::fs::write(dir.join("2026-06-10.wal"), &buf).unwrap();
    }

    fn report_json(outcome: &Outcome) -> serde_json::Value {
        serde_json::from_slice(&std::fs::read(&outcome.report_path).unwrap()).unwrap()
    }

    #[tokio::test]
    async fn full_coverage_passes_with_green_day() {
        let tmp = tempfile::tempdir().unwrap();
        let pts = vec![
            point("BTCUSDT", 8, dec!(0.0001)),
            point("ETHUSDT", 8, dec!(-0.0002)),
        ];
        publish_day(tmp.path(), DAY, &pts);
        let out = run(&MockRest::new(pts), &cfg(tmp.path())).await.unwrap();
        assert_eq!(out.status, ReconcileStatus::Pass);
        let json = report_json(&out);
        assert_eq!(json["coverage_pct"], 100.0);
        assert_eq!(json["consecutive_green_days"], 1);
        assert_eq!(json["expected_events"], 2);
    }

    #[tokio::test]
    async fn missing_extra_and_mismatch_fail() {
        let tmp = tempfile::tempdir().unwrap();
        let expected = vec![
            point("BTCUSDT", 8, dec!(0.0001)),
            point("ETHUSDT", 8, dec!(-0.0002)),
            point("SOLUSDT", 8, dec!(0.0003)),
        ];
        // Captured: ETH missing, SOL wrong rate, plus a phantom XRP row.
        let captured = vec![
            point("BTCUSDT", 8, dec!(0.0001)),
            point("SOLUSDT", 8, dec!(0.00031)),
            point("XRPUSDT", 8, dec!(0.0009)),
        ];
        publish_day(tmp.path(), DAY, &captured);
        let out = run(&MockRest::new(expected), &cfg(tmp.path()))
            .await
            .unwrap();
        assert_eq!(out.status, ReconcileStatus::Fail);
        let json = report_json(&out);
        assert_eq!(json["missing"][0]["instrument"], "ethusdt");
        assert_eq!(json["extra"][0]["instrument"], "xrpusdt");
        assert_eq!(json["rate_mismatches"][0]["instrument"], "solusdt");
        assert_eq!(json["consecutive_green_days"], 0);
        let covered = json["coverage_pct"].as_f64().unwrap();
        assert!((covered - 66.666).abs() < 0.01, "{covered}");
    }

    #[tokio::test]
    async fn late_capture_in_next_day_wal_counts() {
        let tmp = tempfile::tempdir().unwrap();
        let on_time = point("BTCUSDT", 8, dec!(0.0001));
        // Settles 23:00 on D, discovered after midnight → D+1 WAL only.
        let late = point("ETHUSDT", 23, dec!(-0.0002));
        publish_day(tmp.path(), DAY, std::slice::from_ref(&on_time));
        write_next_day_wal(tmp.path(), std::slice::from_ref(&late));
        let out = run(&MockRest::new(vec![on_time, late]), &cfg(tmp.path()))
            .await
            .unwrap();
        assert_eq!(out.status, ReconcileStatus::Pass, "{:?}", report_json(&out));
    }

    #[tokio::test]
    async fn unpublished_day_blocks() {
        let tmp = tempfile::tempdir().unwrap();
        let pts = vec![point("BTCUSDT", 8, dec!(0.0001))];
        // No publish_day: marker missing.
        let out = run(&MockRest::new(pts), &cfg(tmp.path())).await.unwrap();
        assert_eq!(out.status, ReconcileStatus::Blocked);
        let json = report_json(&out);
        assert!(json["blocked_reason"]
            .as_str()
            .unwrap()
            .contains("not published"));
        assert_eq!(json["consecutive_green_days"], 0);
    }

    #[tokio::test]
    async fn green_days_accumulate_and_break_on_fail() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("meta/reconciliation/binance");
        std::fs::create_dir_all(&dir).unwrap();
        // D-1, D-2 pass; D-3 fail → streak before D is 2, with D = 3.
        std::fs::write(dir.join("2026-06-08.json"), br#"{"status":"pass"}"#).unwrap();
        std::fs::write(dir.join("2026-06-07.json"), br#"{"status":"pass"}"#).unwrap();
        std::fs::write(dir.join("2026-06-06.json"), br#"{"status":"fail"}"#).unwrap();

        let pts = vec![point("BTCUSDT", 8, dec!(0.0001))];
        publish_day(tmp.path(), DAY, &pts);
        let out = run(&MockRest::new(pts), &cfg(tmp.path())).await.unwrap();
        assert_eq!(report_json(&out)["consecutive_green_days"], 3);
    }

    #[tokio::test]
    async fn existing_report_is_honored_unless_forced() {
        let tmp = tempfile::tempdir().unwrap();
        let pts = vec![point("BTCUSDT", 8, dec!(0.0001))];
        publish_day(tmp.path(), DAY, &pts);
        let src = MockRest::new(pts.clone());
        let first = run(&src, &cfg(tmp.path())).await.unwrap();
        assert!(!first.skipped_existing);

        // Mutate REST truth; without --force the verdict must not change.
        src.points
            .lock()
            .unwrap()
            .push(point("ETHUSDT", 8, dec!(1)));
        let second = run(&src, &cfg(tmp.path())).await.unwrap();
        assert!(second.skipped_existing);
        assert_eq!(second.status, ReconcileStatus::Pass);

        let forced = run(
            &src,
            &ReconcileCfg {
                force: true,
                ..cfg(tmp.path())
            },
        )
        .await
        .unwrap();
        assert!(!forced.skipped_existing);
        assert_eq!(forced.status, ReconcileStatus::Fail);
    }
}
