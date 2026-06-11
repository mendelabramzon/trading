//! REST history backfill CLI (A5). Subcommands mirror the datasets:
//!
//!   backfill funding --venue <binance> [--from YYYY-MM] [--data-dir data]
//!   backfill oi-hist   … (perishable ~30-day OI history; separate step)
//!   backfill klines    … (price context; separate step)
//!   backfill reconcile … (daily coverage check; separate step)
//!
//! Exit codes follow wal-sweep: 0 = success, 1 = runtime failure,
//! 2 = usage/config error.

use backfill::binance::BinanceSource;
use backfill::funding::{run, FundingBackfillCfg, MonthOutcome};
use backfill::reconcile::{self, ReconcileCfg, ReconcileStatus};
use backfill::Month;
use chrono::{NaiveDate, Utc};
use std::path::PathBuf;
use std::process::ExitCode;

const USAGE: &str = "usage: backfill <funding|oi-hist|klines|reconcile> [--venue binance] \
     [--from YYYY-MM] [--date YYYY-MM-DD] [--data-dir data] [--force]";

struct Args {
    venue: String,
    from: Option<Month>,
    date: Option<NaiveDate>,
    data_dir: PathBuf,
    force: bool,
}

fn parse_args(args: &[String]) -> Result<Args, String> {
    let mut venue = "binance".to_string();
    let mut from = None;
    let mut date = None;
    let mut data_dir = PathBuf::from("data");
    let mut force = false;
    let mut it = args.iter();
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--venue" => {
                venue = it.next().ok_or("--venue needs a value")?.clone();
            }
            "--from" => {
                let raw = it.next().ok_or("--from needs YYYY-MM")?;
                from = Some(Month::parse(raw).map_err(|e| e.to_string())?);
            }
            "--date" => {
                let raw = it.next().ok_or("--date needs YYYY-MM-DD")?;
                date = Some(raw.parse().map_err(|e| format!("--date: {e}"))?);
            }
            "--data-dir" => {
                data_dir = PathBuf::from(it.next().ok_or("--data-dir needs a path")?);
            }
            "--force" => force = true,
            other => return Err(format!("unknown argument {other:?}")),
        }
    }
    Ok(Args {
        venue,
        from,
        date,
        data_dir,
        force,
    })
}

#[tokio::main]
async fn main() -> ExitCode {
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
    let args = match parse_args(rest) {
        Ok(a) => a,
        Err(e) => {
            eprintln!("{e}\n{USAGE}");
            return ExitCode::from(2);
        }
    };

    match cmd.as_str() {
        "funding" => funding(args).await,
        "reconcile" => reconcile_cmd(args).await,
        "oi-hist" => oi_hist(args).await,
        "klines" => klines_cmd(args).await,
        other => {
            eprintln!("unknown subcommand {other:?}\n{USAGE}");
            ExitCode::from(2)
        }
    }
}

async fn oi_hist(args: Args) -> ExitCode {
    if args.venue != "binance" {
        eprintln!("unknown venue {:?}; supported: binance", args.venue);
        return ExitCode::from(2);
    }
    let cfg = backfill::oi::OiHistCfg {
        out_root: args.data_dir.join("backfill"),
        meta_root: args.data_dir.join("meta"),
        ..Default::default()
    };
    match backfill::oi::run(&BinanceSource::new(), &cfg).await {
        Ok(outcomes) => {
            let published = outcomes
                .iter()
                .filter(|(_, o)| matches!(o, backfill::oi::DayOutcome::Published { .. }))
                .count();
            tracing::info!(
                days = outcomes.len(),
                published,
                "OI history backfill complete"
            );
            ExitCode::SUCCESS
        }
        Err(e) => {
            tracing::error!(error = %e, "OI history backfill failed");
            ExitCode::from(1)
        }
    }
}

async fn klines_cmd(args: Args) -> ExitCode {
    if args.venue != "binance" {
        eprintln!("unknown venue {:?}; supported: binance", args.venue);
        return ExitCode::from(2);
    }
    let Some(from) = args.from else {
        // Full-universe kline history is hours of paced requests; make the
        // range an explicit choice.
        eprintln!("klines requires --from YYYY-MM");
        return ExitCode::from(2);
    };
    let cfg = backfill::klines::KlinesCfg {
        out_root: args.data_dir.join("backfill"),
        meta_root: args.data_dir.join("meta"),
        from,
    };
    match backfill::klines::run(&BinanceSource::new(), &cfg).await {
        Ok(outcomes) => {
            tracing::info!(months = outcomes.len(), "kline backfill complete");
            ExitCode::SUCCESS
        }
        Err(e) => {
            tracing::error!(error = %e, "kline backfill failed");
            ExitCode::from(1)
        }
    }
}

/// Exit-code contract (timer goes red on non-zero): 0 = pass,
/// 1 = fail/blocked, 2 = usage.
async fn reconcile_cmd(args: Args) -> ExitCode {
    let cfg = ReconcileCfg {
        data_dir: args.data_dir,
        date: args.date.unwrap_or_else(|| {
            Utc::now()
                .date_naive()
                .pred_opt()
                .expect("no calendar overflow")
        }),
        force: args.force,
    };
    let outcome = match args.venue.as_str() {
        "binance" => reconcile::run(&BinanceSource::new(), &cfg).await,
        other => {
            eprintln!("unknown venue {other:?}; supported: binance");
            return ExitCode::from(2);
        }
    };
    match outcome {
        Ok(o) => {
            tracing::info!(status = ?o.status, report = %o.report_path.display(), "reconciliation done");
            match o.status {
                ReconcileStatus::Pass => ExitCode::SUCCESS,
                ReconcileStatus::Fail | ReconcileStatus::Blocked => ExitCode::from(1),
            }
        }
        Err(e) => {
            tracing::error!(error = %e, "reconciliation errored");
            ExitCode::from(1)
        }
    }
}

async fn funding(args: Args) -> ExitCode {
    let cfg = FundingBackfillCfg {
        out_root: args.data_dir.join("backfill"),
        meta_root: args.data_dir.join("meta"),
        from: args.from,
    };
    let outcomes = match args.venue.as_str() {
        "binance" => run(&BinanceSource::new(), &cfg).await,
        "bybit" => run(&backfill::bybit::BybitSource::new(), &cfg).await,
        other => {
            eprintln!("unknown venue {other:?}; supported: binance, bybit");
            return ExitCode::from(2);
        }
    };
    match outcomes {
        Ok(outcomes) => {
            let (mut published, mut partial, mut skipped, mut empty, mut rows) = (0, 0, 0, 0, 0);
            for (_, o) in &outcomes {
                match o {
                    MonthOutcome::Published { rows: r } => {
                        published += 1;
                        rows += r;
                    }
                    MonthOutcome::Partial { rows: r } => {
                        partial += 1;
                        rows += r;
                    }
                    MonthOutcome::AlreadyPublished => skipped += 1,
                    MonthOutcome::Empty => empty += 1,
                }
            }
            tracing::info!(
                published,
                partial,
                already_published = skipped,
                empty,
                rows,
                "funding backfill complete"
            );
            ExitCode::SUCCESS
        }
        Err(e) => {
            tracing::error!(error = %e, "funding backfill failed");
            ExitCode::from(1)
        }
    }
}
