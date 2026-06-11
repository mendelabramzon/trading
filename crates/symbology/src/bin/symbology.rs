//! Reference-data build CLI: canonical mapping + instruments SCD + fee
//! schedules, one `build` run for the daily timer. Each product builds
//! independently — one failure does not block the others, but any failure
//! exits 1 so the timer goes red.
//!
//! Exit codes: 0 = all built, 1 = any build failure, 2 = usage.

use std::path::PathBuf;
use std::process::ExitCode;
use symbology::build::{build, BuildCfg};
use symbology::{fees, scd};

const USAGE: &str = "usage: symbology build [--data-dir data] \
     [--overrides configs/symbology-overrides.toml] [--fees-dir configs/fees]";

fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let argv: Vec<String> = std::env::args().skip(1).collect();
    let Some((cmd, rest)) = argv.split_first() else {
        eprintln!("{USAGE}");
        return ExitCode::from(2);
    };
    if cmd != "build" {
        eprintln!("unknown subcommand {cmd:?}\n{USAGE}");
        return ExitCode::from(2);
    }

    let mut data_dir = PathBuf::from("data");
    let mut overrides = PathBuf::from("configs/symbology-overrides.toml");
    let mut fees_dir = PathBuf::from("configs/fees");
    let mut it = rest.iter();
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--data-dir" => match it.next() {
                Some(v) => data_dir = PathBuf::from(v),
                None => {
                    eprintln!("--data-dir needs a path\n{USAGE}");
                    return ExitCode::from(2);
                }
            },
            "--overrides" => match it.next() {
                Some(v) => overrides = PathBuf::from(v),
                None => {
                    eprintln!("--overrides needs a path\n{USAGE}");
                    return ExitCode::from(2);
                }
            },
            "--fees-dir" => match it.next() {
                Some(v) => fees_dir = PathBuf::from(v),
                None => {
                    eprintln!("--fees-dir needs a path\n{USAGE}");
                    return ExitCode::from(2);
                }
            },
            other => {
                eprintln!("unknown argument {other:?}\n{USAGE}");
                return ExitCode::from(2);
            }
        }
    }

    let mut failed = false;
    match build(&BuildCfg {
        data_dir: data_dir.clone(),
        overrides_path: overrides,
    }) {
        Ok(summary) => tracing::info!(
            matched = summary.matched_canonicals,
            path = %summary.mapping_path.display(),
            "mapping built"
        ),
        Err(e) => {
            tracing::error!(error = %e, "mapping build failed");
            failed = true;
        }
    }
    match scd::build_scd(&data_dir) {
        Ok(summary) => tracing::info!(
            days = summary.days,
            rows = summary.rows,
            path = %summary.path.display(),
            "instruments SCD built"
        ),
        Err(e) => {
            tracing::error!(error = %e, "instruments SCD build failed");
            failed = true;
        }
    }
    match fees::build_fees(&fees_dir, &data_dir) {
        Ok(summary) => tracing::info!(venues = ?summary.venues, "fee schedules built"),
        Err(e) => {
            tracing::error!(error = %e, "fee schedule build failed");
            failed = true;
        }
    }

    if failed {
        ExitCode::from(1)
    } else {
        tracing::info!("symbology build complete");
        ExitCode::SUCCESS
    }
}
