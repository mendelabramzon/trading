use venue_adapter::{VenueAdapter, Subscription, DataType};
use venue_core::{Event, InstrumentId};
use tokio::sync::mpsc;
use recorder::WalWriter;
use std::path::PathBuf;
use tracing::info;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("venue_binance=debug".parse().unwrap())
                .add_directive("recorder=info".parse().unwrap()),
        )
        .init();

    let (tx, mut rx) = mpsc::channel::<Event>(1000);
    let mut adapter = venue_binance::BinanceAdapter::new(tx);

    // start the WAL writer on its own thread
    let wal = WalWriter::new(PathBuf::from("data/wal"));

    adapter.connect().await.expect("connect failed");

    let subs = vec![
        Subscription {
            instrument: InstrumentId { value: "btcusdt".into() },
            data_type: vec![DataType::BookTicker, DataType::Trade],
        },
        Subscription {
            instrument: InstrumentId { value: "ethusdt".into() },
            data_type: vec![DataType::BookTicker],
        },
    ];

    adapter.subscribe(subs).await.expect("subscribe failed");
    info!("subscribed, receiving events");

    // Use select to handle both events and ctrl+c
    loop {
        tokio::select! {
            event = rx.recv() => {
                match event {
                    Some(event) => wal.send(&event),
                    None => break, // all senders dropped
                }
            }
            _ = tokio::signal::ctrl_c() => {
                info!("ctrl+c received, shutting down");
                break;
            }
        }
    }

    // Disconnect WebSocket connections
    adapter.disconnect().await.ok();
    info!("adapter disconnected");

    // wal is dropped here — Drop impl flushes and joins the writer thread
    drop(wal);
    info!("WAL flushed, exiting");
}
