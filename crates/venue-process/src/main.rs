//! Supervised capture entrypoint (improvement_plan step 11): config → adapter
//! → WAL, with startup retry (N8), heartbeat (P5d), daily exchangeInfo dump
//! (P5a), and graceful SIGTERM/SIGINT shutdown. Replaces the `smoke` example
//! as the way capture runs.
//!
//! Exit codes: 0 = clean signal shutdown; 1 = fatal runtime error (the N2
//! WAL-fatality policy exits from inside `recorder`); 2 = config/usage error
//! (systemd units set `RestartPreventExitStatus=2` so a bad config does not
//! restart-flap).

mod heartbeat;

use chrono::{NaiveDate, Utc};
use recorder::{RawWalWriter, StatsSink, WalWriter};
use std::collections::HashSet;
use std::io::IsTerminal;
use std::path::Path;
use std::process::ExitCode;
use std::time::Duration;
use tokio::signal::unix::{signal, Signal, SignalKind};
use tracing::{error, info, warn};
use venue_adapter::{EventSink, RawFrameSink, VenueAdapter};
use venue_binance::{BinanceAdapter, ExponentialBackoff};
use venue_core::{Instrument, RawFrame};

/// `fetch_instruments_raw` uses a client with no HTTP timeout; unbounded, a
/// hung connection would wedge the main loop (and signal handling with it).
const EXCHANGE_INFO_TIMEOUT: Duration = Duration::from_secs(30);
const MAIN_TICK: Duration = Duration::from_secs(60);

/// Raw tee that may be disabled by config while keeping the adapter type
/// uniform.
#[derive(Clone)]
struct OptRawSink(Option<recorder::RawWalSink>);

impl RawFrameSink for OptRawSink {
    fn send_raw(&self, frame: RawFrame) {
        if let Some(sink) = &self.0 {
            sink.send_raw(frame);
        }
    }
}

struct Signals {
    term: Signal,
    int: Signal,
}

impl Signals {
    fn new() -> std::io::Result<Self> {
        Ok(Self {
            term: signal(SignalKind::terminate())?,
            int: signal(SignalKind::interrupt())?,
        })
    }

    async fn recv(&mut self) -> &'static str {
        tokio::select! {
            _ = self.term.recv() => "SIGTERM",
            _ = self.int.recv() => "SIGINT",
        }
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let Some(path) = std::env::args().nth(1) else {
        eprintln!("usage: venue-process <config.toml>");
        return ExitCode::from(2);
    };
    let cfg = match config::load(Path::new(&path)) {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::from(2);
        }
    };

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(&cfg.logging.filter)),
        )
        .with_ansi(std::io::stdout().is_terminal())
        .init();

    run(cfg).await
}

async fn run(cfg: config::Config) -> ExitCode {
    let venue = cfg.venue.id.clone();
    info!(venue, data_dir = %cfg.paths.data_dir.display(), "starting capture");

    let mut signals = match Signals::new() {
        Ok(s) => s,
        Err(e) => {
            error!(error = %e, "failed to install signal handlers");
            return ExitCode::from(2);
        }
    };

    // WAL owns durability on its own thread (A2); the raw tee captures venue
    // frames verbatim (R2) when enabled.
    let wal = WalWriter::new(cfg.paths.wal_dir());
    let raw_writer = cfg
        .capture
        .raw_tee
        .then(|| RawWalWriter::new(cfg.paths.raw_dir(), &venue));
    let raw_sink = OptRawSink(raw_writer.as_ref().map(|w| w.sink()));

    let sink = StatsSink::new(wal.sink());
    let capture_stats = sink.stats();
    let mut adapter = BinanceAdapter::new(sink).with_raw_tee(raw_sink);

    // P5a dump + loud validation: a typo'd symbol would otherwise be acked by
    // Binance with result:null and capture exactly nothing.
    let mut last_dump_date: Option<NaiveDate> = None;
    match dump_exchange_info(&adapter, &cfg, &venue).await {
        Some(instruments) => {
            last_dump_date = Some(Utc::now().date_naive());
            let known: HashSet<String> = instruments
                .iter()
                .map(|i| i.id.value.to_lowercase())
                .collect();
            let unknown: Vec<String> = cfg
                .explicit_instruments()
                .into_iter()
                .filter(|id| !known.contains(id))
                .collect();
            if !unknown.is_empty() {
                error!(
                    ?unknown,
                    "configured instruments not listed in venue exchangeInfo"
                );
                return ExitCode::from(2);
            }
        }
        None => warn!("exchangeInfo unavailable at startup; instrument validation skipped"),
    }

    // Startup retry (N8): a failed subscribe leaves earlier stream chunks
    // live, so roll the partial pool back with disconnect() and retry the
    // whole subscription with capped jittered backoff — forever; an unattended
    // capture box must ride out long network outages.
    let subs = cfg.subscriptions();
    let mut backoff = ExponentialBackoff::new();
    loop {
        let attempt = async {
            adapter.connect().await?;
            adapter.subscribe(subs.clone()).await
        };
        match attempt.await {
            Ok(()) => break,
            Err(e) => {
                warn!(error = %e, "startup subscribe failed; rolling back partial subscriptions");
                adapter.disconnect().await.ok();
                let delay = backoff.next_delay();
                info!(delay_ms = delay.as_millis() as u64, "startup retry");
                tokio::select! {
                    _ = tokio::time::sleep(delay) => {}
                    sig = signals.recv() => {
                        info!(signal = sig, "shutdown requested during startup retry");
                        return shutdown(adapter, wal, raw_writer, None).await;
                    }
                }
            }
        }
    }

    let heartbeat = heartbeat::spawn(
        venue.clone(),
        capture_stats,
        wal.stats(),
        raw_writer.as_ref().map(|w| w.stats()),
        Duration::from_secs(cfg.capture.heartbeat_secs),
    );
    info!("subscribed; capture running — SIGTERM/SIGINT to stop");

    let mut tick = tokio::time::interval(MAIN_TICK);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            sig = signals.recv() => {
                info!(signal = sig, "shutdown requested");
                break;
            }
            _ = tick.tick() => {
                // P5a is daily; also retries a failed startup dump every tick.
                let today = Utc::now().date_naive();
                if last_dump_date != Some(today)
                    && dump_exchange_info(&adapter, &cfg, &venue).await.is_some()
                {
                    last_dump_date = Some(today);
                }
            }
        }
    }

    shutdown(adapter, wal, raw_writer, Some(heartbeat)).await
}

/// Fetch + persist the raw exchangeInfo body (P5a): reference fields the
/// parser drops today stay recoverable from `data/meta/`.
async fn dump_exchange_info<S: EventSink, R: RawFrameSink>(
    adapter: &BinanceAdapter<S, R>,
    cfg: &config::Config,
    venue: &str,
) -> Option<Vec<Instrument>> {
    match tokio::time::timeout(EXCHANGE_INFO_TIMEOUT, adapter.fetch_instruments_raw()).await {
        Ok(Ok((raw_json, instruments))) => {
            let dir = cfg.paths.meta_dir().join(venue);
            let date = Utc::now().format("%Y-%m-%d");
            let path = dir.join(format!("{date}-exchangeInfo.json"));
            let written =
                std::fs::create_dir_all(&dir).and_then(|()| std::fs::write(&path, &raw_json));
            match written {
                Ok(()) => info!(
                    instruments = instruments.len(),
                    path = %path.display(),
                    "exchangeInfo dumped"
                ),
                // Meta writes are not WAL writes: warn, don't exit — if the
                // disk is actually full the WAL thread exits the process (N2).
                Err(e) => warn!(error = %e, "exchangeInfo dump write failed"),
            }
            Some(instruments)
        }
        Ok(Err(e)) => {
            warn!(error = %e, "exchangeInfo fetch failed");
            None
        }
        Err(_) => {
            warn!(
                timeout_s = EXCHANGE_INFO_TIMEOUT.as_secs(),
                "exchangeInfo fetch timed out"
            );
            None
        }
    }
}

/// Shutdown contract (order is load-bearing): disconnect joins the connection
/// tasks and snapshot fetcher, dropping their sink clones; the adapter drop
/// releases the last ones; only then can the writer Drop impls join their
/// threads (final flush + fsync) without hanging.
async fn shutdown<S: EventSink, R: RawFrameSink>(
    mut adapter: BinanceAdapter<S, R>,
    wal: WalWriter,
    raw_writer: Option<RawWalWriter>,
    heartbeat: Option<tokio::task::JoinHandle<()>>,
) -> ExitCode {
    if let Some(task) = heartbeat {
        task.abort();
    }
    adapter.disconnect().await.ok();
    info!("adapter disconnected");
    drop(adapter);
    drop(wal);
    drop(raw_writer);
    info!("WAL flushed; exiting cleanly");
    ExitCode::SUCCESS
}
