//! Live capture smoke test.
//!
//! Usage: `smoke [max_events] [timeout_secs]`
//! - no args: run until ctrl+c (long-running capture);
//! - `smoke 100000 180`: exit after 100k recorded events or 180 s, whichever
//!   comes first — the bounded form used for acceptance runs.

use recorder::{RawWalWriter, WalWriter};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};
use venue_adapter::{DataType, EventSink, EventSinkError, Scope, Subscription, VenueAdapter};
use venue_core::{Event, InstrumentId};

/// Forwards to the inner sink and counts accepted events so the bounded run
/// can stop after N messages.
#[derive(Clone)]
struct CountingSink<S> {
    inner: S,
    count: Arc<AtomicU64>,
}

impl<S: EventSink> EventSink for CountingSink<S> {
    async fn send(&self, event: Event) -> Result<(), EventSinkError> {
        self.inner.send(event).await?;
        self.count.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    async fn send_batch(&self, events: Vec<Event>) -> Result<(), EventSinkError> {
        let n = events.len() as u64;
        self.inner.send_batch(events).await?;
        self.count.fetch_add(n, Ordering::Relaxed);
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("venue_binance=debug".parse().unwrap())
                .add_directive("recorder=info".parse().unwrap()),
        )
        .init();

    let max_events: Option<u64> = std::env::args()
        .nth(1)
        .map(|a| a.parse().expect("max_events"));
    let timeout_secs: Option<u64> = std::env::args()
        .nth(2)
        .map(|a| a.parse().expect("timeout_secs"));

    // WAL writer owns durability on its own thread; the adapter writes
    // through a WalSink clone (record-at-the-edge, A2). The raw tee captures
    // venue frames verbatim (R2) — default ON during bring-up.
    let wal = WalWriter::new(PathBuf::from("data/wal"));
    let raw = RawWalWriter::new(PathBuf::from("data/raw"), "binance");
    let count = Arc::new(AtomicU64::new(0));
    let sink = CountingSink {
        inner: wal.sink(),
        count: count.clone(),
    };
    let mut adapter = venue_binance::BinanceAdapter::new(sink).with_raw_tee(raw.sink());

    // P5a: persist the raw exchangeInfo body — reference fields the parser
    // drops today stay recoverable from data/meta/.
    match adapter.fetch_instruments_raw().await {
        Ok((raw_json, instruments)) => {
            let dir = PathBuf::from("data/meta/binance");
            std::fs::create_dir_all(&dir).expect("create meta dir");
            let date = chrono::Utc::now().format("%Y-%m-%d");
            let path = dir.join(format!("{date}-exchangeInfo.json"));
            std::fs::write(&path, &raw_json).expect("write exchangeInfo dump");
            info!(instruments = instruments.len(), path = %path.display(), "exchangeInfo dumped");
        }
        Err(e) => warn!(error = %e, "exchangeInfo dump failed"),
    }

    adapter.connect().await.expect("connect failed");

    let subs = vec![
        Subscription {
            scope: Scope::Instruments(vec![InstrumentId {
                value: "btcusdt".into(),
            }]),
            data: vec![
                DataType::BookTicker,
                DataType::Trade,
                DataType::BookDepth,
                DataType::Liquidation,
            ],
        },
        Subscription {
            scope: Scope::Instruments(vec![InstrumentId {
                value: "ethusdt".into(),
            }]),
            data: vec![DataType::BookTicker, DataType::BookDepth],
        },
    ];

    adapter.subscribe(subs).await.expect("subscribe failed");
    info!(
        ?max_events,
        ?timeout_secs,
        "subscribed, recording to WAL — ctrl+c to stop"
    );

    let deadline = timeout_secs.map(|s| tokio::time::Instant::now() + Duration::from_secs(s));
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("ctrl+c received, shutting down");
                break;
            }
            _ = tokio::time::sleep(Duration::from_secs(1)) => {
                let recorded = count.load(Ordering::Relaxed);
                if max_events.is_some_and(|m| recorded >= m) {
                    info!(recorded, "reached max events, shutting down");
                    break;
                }
                if deadline.is_some_and(|d| tokio::time::Instant::now() >= d) {
                    info!(recorded, "reached timeout, shutting down");
                    break;
                }
            }
        }
    }

    // Shutdown contract (sink clones must die before their writers or the
    // Drop join hangs): disconnect → drop(adapter) → drop(wal/raw).
    adapter.disconnect().await.ok();
    info!("adapter disconnected");
    drop(adapter);
    drop(wal);
    drop(raw);
    info!(
        recorded = count.load(Ordering::Relaxed),
        "WAL flushed, exiting"
    );
}
