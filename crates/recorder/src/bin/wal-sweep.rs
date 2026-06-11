//! Conversion automation entrypoint (P6): convert closed WALs to Parquet and
//! emit the daily QA report. Idempotent (completion marker = `qa_report.json`)
//! — run hourly from a systemd timer, or manually.
//!
//! Usage: `wal-sweep <wal_root> <out_root> [--as-of YYYY-MM-DD]`
//!
//! `--as-of` overrides "today" (UTC): everything strictly before it is
//! treated as closed. Useful for tests and for force-sweeping a still-open
//! day (`--as-of tomorrow`).
//!
//! Exit codes: 0 = all converted days pass QA (or nothing to do); 1 = at
//! least one day failed QA or could not be converted — the timer unit goes
//! red in journald.

use chrono::{NaiveDate, Utc};
use recorder::sweep::{sweep, SweepResult};
use std::path::PathBuf;
use std::process::ExitCode;

fn main() -> ExitCode {
    let usage = "usage: wal-sweep <wal_root> <out_root> [--as-of YYYY-MM-DD]";
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (mut wal_root, mut out_root, mut as_of) = (None, None, None);
    let mut it = args.iter();
    while let Some(arg) = it.next() {
        if arg == "--as-of" {
            let Some(v) = it.next() else {
                eprintln!("{usage}");
                return ExitCode::from(2);
            };
            match NaiveDate::parse_from_str(v, "%Y-%m-%d") {
                Ok(d) => as_of = Some(d),
                Err(e) => {
                    eprintln!("--as-of: {e}");
                    return ExitCode::from(2);
                }
            }
        } else if wal_root.is_none() {
            wal_root = Some(PathBuf::from(arg));
        } else if out_root.is_none() {
            out_root = Some(PathBuf::from(arg));
        } else {
            eprintln!("{usage}");
            return ExitCode::from(2);
        }
    }
    let (Some(wal_root), Some(out_root)) = (wal_root, out_root) else {
        eprintln!("{usage}");
        return ExitCode::from(2);
    };
    let as_of = as_of.unwrap_or_else(|| Utc::now().date_naive());

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let outcomes = sweep(&wal_root, &out_root, as_of);
    let mut failed = 0u32;
    for o in &outcomes {
        match &o.result {
            SweepResult::Converted {
                status: recorder::QaStatus::Pass,
                report_path,
            } => println!("{}/{}: PASS ({})", o.venue, o.date, report_path.display()),
            SweepResult::Converted {
                status: recorder::QaStatus::Fail,
                report_path,
            } => {
                failed += 1;
                println!("{}/{}: FAIL ({})", o.venue, o.date, report_path.display());
            }
            SweepResult::Failed(e) => {
                failed += 1;
                println!("{}/{}: ERROR {e}", o.venue, o.date);
            }
            SweepResult::SkippedFresh => {
                println!("{}/{}: skipped (recently written)", o.venue, o.date);
            }
        }
    }
    println!(
        "swept {} file(s) as of {as_of}, {failed} problem(s)",
        outcomes.len()
    );
    if failed > 0 {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}
