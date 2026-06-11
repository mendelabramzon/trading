//! Conversion automation (P6): find closed WAL files, convert each to
//! Parquet, run QA, publish atomically.
//!
//! Completion marker = `qa_report.json` inside the final output directory.
//! Conversion + QA write into `out/<venue>/.tmp-<date>/`, the report lands
//! last, then one atomic `fs::rename` publishes `out/<venue>/<date>/`. A
//! crash mid-conversion leaves only a `.tmp-*` dir (cleaned next sweep); a
//! final dir *without* the marker is treated as partial and re-converted —
//! Parquet is derived data, the WAL stays truth.

use crate::parquet_converter::convert_wal;
use crate::qa::{qa_wal, QaReport, QaStatus};
use chrono::NaiveDate;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

/// A WAL younger than this is left alone even if its date is closed: the
/// writer legitimately reopens a rotated date for a few seconds when a
/// backlog crosses midnight.
const FRESH_GUARD: Duration = Duration::from_secs(600);

pub const QA_REPORT_FILE: &str = "qa_report.json";

#[derive(Debug)]
pub struct SweepOutcome {
    pub venue: String,
    pub date: NaiveDate,
    pub wal_path: PathBuf,
    pub result: SweepResult,
}

#[derive(Debug)]
pub enum SweepResult {
    Converted {
        status: QaStatus,
        report_path: PathBuf,
    },
    /// Conversion/QA/publish failed in a way that produced no report; the
    /// file is retried on the next sweep.
    Failed(String),
    /// Closed date but recently written (midnight-backlog guard).
    SkippedFresh,
}

/// Convert every closed (`date < as_of`), unconverted WAL under
/// `wal_root/<venue>/<date>.wal`. Already-published days (marker present) and
/// open days are skipped silently — the sweep is idempotent and cheap to run
/// hourly. Per-file failures never abort the sweep.
pub fn sweep(wal_root: &Path, out_root: &Path, as_of: NaiveDate) -> Vec<SweepOutcome> {
    let mut outcomes = Vec::new();
    let venues = match fs::read_dir(wal_root) {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(root = %wal_root.display(), error = %e, "sweep: cannot read WAL root");
            return outcomes;
        }
    };

    for venue_dir in venues.flatten() {
        if !venue_dir.path().is_dir() {
            continue;
        }
        let venue = venue_dir.file_name().to_string_lossy().into_owned();
        let Ok(files) = fs::read_dir(venue_dir.path()) else {
            continue;
        };
        let mut wals: Vec<(NaiveDate, PathBuf)> = files
            .flatten()
            .filter_map(|f| {
                let path = f.path();
                if path.extension().is_none_or(|e| e != "wal") {
                    return None;
                }
                let stem = path.file_stem()?.to_string_lossy().into_owned();
                let date = NaiveDate::parse_from_str(&stem, "%Y-%m-%d").ok()?;
                Some((date, path))
            })
            .collect();
        wals.sort();

        for (date, wal_path) in wals {
            if date >= as_of {
                continue; // open day
            }
            let final_dir = out_root.join(&venue).join(date.to_string());
            if final_dir.join(QA_REPORT_FILE).exists() {
                continue; // already published
            }
            let result = convert_one(&wal_path, out_root, &venue, date, &final_dir);
            if let SweepResult::Converted {
                status,
                report_path,
            } = &result
            {
                tracing::info!(
                    venue,
                    %date,
                    ?status,
                    report = %report_path.display(),
                    "sweep: converted"
                );
            } else {
                tracing::warn!(venue, %date, ?result, "sweep: not converted");
            }
            outcomes.push(SweepOutcome {
                venue: venue.clone(),
                date,
                wal_path,
                result,
            });
        }
    }
    outcomes
}

fn convert_one(
    wal_path: &Path,
    out_root: &Path,
    venue: &str,
    date: NaiveDate,
    final_dir: &Path,
) -> SweepResult {
    match fs::metadata(wal_path).and_then(|m| m.modified()) {
        Ok(mtime) => {
            let age = SystemTime::now().duration_since(mtime).unwrap_or_default();
            if age < FRESH_GUARD {
                return SweepResult::SkippedFresh;
            }
        }
        Err(e) => return SweepResult::Failed(format!("stat failed: {e}")),
    }

    let venue_out = out_root.join(venue);
    let tmp_dir = venue_out.join(format!(".tmp-{date}"));
    let publish = || -> Result<(QaReport, PathBuf), String> {
        if tmp_dir.exists() {
            fs::remove_dir_all(&tmp_dir).map_err(|e| format!("clean stale tmp: {e}"))?;
        }
        if final_dir.exists() {
            // Marker-less leftovers (crash or pre-automation manual run):
            // derived data, regenerate.
            fs::remove_dir_all(final_dir).map_err(|e| format!("clean partial output: {e}"))?;
        }
        fs::create_dir_all(&tmp_dir).map_err(|e| format!("create tmp dir: {e}"))?;

        let conversion = convert_wal(wal_path, &tmp_dir);
        let mut report = qa_wal(wal_path, venue, &date.to_string())
            .map_err(|e| format!("qa pass failed: {e}"))?;
        if let Err(e) = conversion {
            report.set_conversion_error(e.to_string());
        }

        let json = serde_json::to_vec_pretty(&report).map_err(|e| format!("serialize: {e}"))?;
        let part = tmp_dir.join(format!("{QA_REPORT_FILE}.part"));
        fs::write(&part, json).map_err(|e| format!("write report: {e}"))?;
        fs::rename(&part, tmp_dir.join(QA_REPORT_FILE))
            .map_err(|e| format!("finalize report: {e}"))?;
        fs::rename(&tmp_dir, final_dir).map_err(|e| format!("publish: {e}"))?;
        Ok((report, final_dir.join(QA_REPORT_FILE)))
    };

    match publish() {
        Ok((report, report_path)) => SweepResult::Converted {
            status: report.status,
            report_path,
        },
        Err(e) => SweepResult::Failed(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use std::fs::FileTimes;
    use venue_core::{
        Event, InstrumentId, Level, MarketPayload, Nanos, Payload, SourceId, VenueId,
    };

    const T0: Nanos = 1_700_000_000_000_000_000;
    const SEC: Nanos = 1_000_000_000;

    fn event(local_ts: Nanos, payload: Payload) -> Event {
        Event {
            venue: VenueId {
                value: "binance".into(),
            },
            instrument: Some(InstrumentId {
                value: "btcusdt".into(),
            }),
            venue_ts: Some(local_ts - 1_000_000),
            local_ts,
            source: SourceId(1),
            provenance: None,
            payload,
        }
    }

    fn passing_events() -> Vec<Event> {
        vec![
            event(
                T0,
                Payload::Market(MarketPayload::BookSnapshot {
                    bids: vec![Level {
                        price: dec!(1),
                        qty: dec!(1),
                    }],
                    asks: vec![],
                    last_update_id: 100,
                }),
            ),
            event(
                T0 + SEC,
                Payload::Market(MarketPayload::BookUpdate {
                    // At least one level: the converter emits book-update rows
                    // per level, and the lazy writer skips empty tables.
                    bids: vec![Level {
                        price: dec!(1),
                        qty: dec!(2),
                    }],
                    asks: vec![],
                    first_update_id: 95,
                    final_update_id: 105,
                    prev_final_update_id: None,
                    event_time: Some(T0 + SEC),
                }),
            ),
        ]
    }

    fn write_wal(path: &Path, events: &[Event], age: Duration) {
        let mut bytes = Vec::new();
        let mut buf = Vec::new();
        for e in events {
            buf.clear();
            wire::encode_frame(e, &mut buf).unwrap();
            bytes.extend_from_slice(&buf);
        }
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, bytes).unwrap();
        let f = fs::File::options().write(true).open(path).unwrap();
        f.set_times(FileTimes::new().set_modified(SystemTime::now() - age))
            .unwrap();
    }

    fn roots(tmp: &Path) -> (PathBuf, PathBuf) {
        (tmp.join("wal"), tmp.join("parquet"))
    }

    const OLD: Duration = Duration::from_secs(3600);

    #[test]
    fn closed_day_converted_with_marker_then_skipped() {
        let tmp = tempfile::tempdir().unwrap();
        let (wal_root, out_root) = roots(tmp.path());
        let as_of = NaiveDate::from_ymd_opt(2026, 6, 11).unwrap();
        write_wal(
            &wal_root.join("binance/2026-06-10.wal"),
            &passing_events(),
            OLD,
        );

        let outcomes = sweep(&wal_root, &out_root, as_of);
        assert_eq!(outcomes.len(), 1);
        assert!(matches!(
            outcomes[0].result,
            SweepResult::Converted {
                status: QaStatus::Pass,
                ..
            }
        ));
        let final_dir = out_root.join("binance/2026-06-10");
        let listing: Vec<_> = fs::read_dir(&final_dir)
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        assert!(final_dir.join(QA_REPORT_FILE).exists(), "{listing:?}");
        assert!(
            final_dir.join("book_update.parquet").exists(),
            "{listing:?}"
        );
        assert!(!out_root.join("binance/.tmp-2026-06-10").exists());

        // Second run: marker short-circuits, nothing to do.
        assert!(sweep(&wal_root, &out_root, as_of).is_empty());
    }

    #[test]
    fn open_day_not_touched() {
        let tmp = tempfile::tempdir().unwrap();
        let (wal_root, out_root) = roots(tmp.path());
        let as_of = NaiveDate::from_ymd_opt(2026, 6, 11).unwrap();
        write_wal(
            &wal_root.join("binance/2026-06-11.wal"),
            &passing_events(),
            OLD,
        );
        assert!(sweep(&wal_root, &out_root, as_of).is_empty());
        assert!(!out_root.join("binance/2026-06-11").exists());
    }

    #[test]
    fn fresh_mtime_guard_skips() {
        let tmp = tempfile::tempdir().unwrap();
        let (wal_root, out_root) = roots(tmp.path());
        let as_of = NaiveDate::from_ymd_opt(2026, 6, 11).unwrap();
        write_wal(
            &wal_root.join("binance/2026-06-10.wal"),
            &passing_events(),
            Duration::ZERO,
        );
        let outcomes = sweep(&wal_root, &out_root, as_of);
        assert_eq!(outcomes.len(), 1);
        assert!(matches!(outcomes[0].result, SweepResult::SkippedFresh));
        assert!(!out_root.join("binance/2026-06-10").exists());
    }

    #[test]
    fn stale_tmp_and_markerless_output_recovered() {
        let tmp = tempfile::tempdir().unwrap();
        let (wal_root, out_root) = roots(tmp.path());
        let as_of = NaiveDate::from_ymd_opt(2026, 6, 11).unwrap();
        write_wal(
            &wal_root.join("binance/2026-06-10.wal"),
            &passing_events(),
            OLD,
        );
        // Crash leftovers: a stale tmp dir and a marker-less final dir.
        fs::create_dir_all(out_root.join("binance/.tmp-2026-06-10")).unwrap();
        fs::write(
            out_root.join("binance/.tmp-2026-06-10/junk"),
            b"crash leftovers",
        )
        .unwrap();
        fs::create_dir_all(out_root.join("binance/2026-06-10")).unwrap();
        fs::write(
            out_root.join("binance/2026-06-10/orphan.parquet"),
            b"partial",
        )
        .unwrap();

        let outcomes = sweep(&wal_root, &out_root, as_of);
        assert_eq!(outcomes.len(), 1);
        assert!(matches!(
            outcomes[0].result,
            SweepResult::Converted {
                status: QaStatus::Pass,
                ..
            }
        ));
        let final_dir = out_root.join("binance/2026-06-10");
        assert!(final_dir.join(QA_REPORT_FILE).exists());
        assert!(!final_dir.join("orphan.parquet").exists());
        assert!(!out_root.join("binance/.tmp-2026-06-10").exists());
    }

    #[test]
    fn corrupt_wal_publishes_failing_report() {
        let tmp = tempfile::tempdir().unwrap();
        let (wal_root, out_root) = roots(tmp.path());
        let as_of = NaiveDate::from_ymd_opt(2026, 6, 11).unwrap();
        let wal = wal_root.join("binance/2026-06-10.wal");
        write_wal(&wal, &passing_events(), OLD);
        // Overwhelm the file with garbage (way past the 1% gate), keep mtime old.
        let mut bytes = fs::read(&wal).unwrap();
        bytes.extend(vec![0x55u8; bytes.len() * 200]);
        fs::write(&wal, bytes).unwrap();
        let f = fs::File::options().write(true).open(&wal).unwrap();
        f.set_times(FileTimes::new().set_modified(SystemTime::now() - OLD))
            .unwrap();

        let outcomes = sweep(&wal_root, &out_root, as_of);
        assert_eq!(outcomes.len(), 1);
        match &outcomes[0].result {
            SweepResult::Converted {
                status,
                report_path,
            } => {
                assert_eq!(*status, QaStatus::Fail);
                let report: serde_json::Value =
                    serde_json::from_slice(&fs::read(report_path).unwrap()).unwrap();
                assert_eq!(report["status"], "fail");
                assert_eq!(report["conversion"]["ok"], false);
            }
            other => panic!("expected published failing report, got {other:?}"),
        }
        // Published (marker present): the corrupt day is not retried forever.
        assert!(sweep(&wal_root, &out_root, as_of).is_empty());
    }

    #[test]
    fn non_date_and_non_wal_files_ignored() {
        let tmp = tempfile::tempdir().unwrap();
        let (wal_root, out_root) = roots(tmp.path());
        let as_of = NaiveDate::from_ymd_opt(2026, 6, 11).unwrap();
        fs::create_dir_all(wal_root.join("binance")).unwrap();
        fs::write(wal_root.join("binance/notes.txt"), b"x").unwrap();
        fs::write(wal_root.join("binance/garbage.wal"), b"x").unwrap();
        assert!(sweep(&wal_root, &out_root, as_of).is_empty());
    }
}
