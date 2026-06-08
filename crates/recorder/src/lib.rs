use chrono::Utc;
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};
use venue_core::Event;

pub mod parquet_converter;

pub struct WalWriter {
    tx: Option<mpsc::SyncSender<Event>>,
    handle: Option<thread::JoinHandle<()>>,
}

impl WalWriter {
    pub fn new(base_dir: PathBuf) -> Self {
        let (tx, rx) = mpsc::sync_channel::<Event>(10_000);

        let handle = thread::spawn(move || {
            Self::run(base_dir, rx);
        });

        Self {
            tx: Some(tx),
            handle: Some(handle),
        }
    }

    pub fn send(&self, event: &Event) {
        if let Some(tx) = &self.tx {
            if tx.send(event.clone()).is_err() {
                tracing::warn!("WAL event dropped: channel full or closed");
            }
        }
    }

    fn run(base_dir: PathBuf, rx: mpsc::Receiver<Event>) {
        const FSYNC_INTERVAL: Duration = Duration::from_secs(1);

        let mut writers: HashMap<String, BufWriter<File>> = HashMap::new();
        let mut buf = Vec::with_capacity(4096);
        let mut last_fsync = Instant::now();

        loop {
            let remaining = FSYNC_INTERVAL.saturating_sub(last_fsync.elapsed());
            let event = match rx.recv_timeout(remaining) {
                Ok(event) => Some(event),
                Err(mpsc::RecvTimeoutError::Timeout) => None,
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
            };

            if let Some(event) = event {
                // derive date from event timestamp, falling back to wall clock
                let venue: &str = &event.venue.value;
                let date = event
                    .venue_ts
                    .or(event.local_ts)
                    .and_then(|nanos| {
                        chrono::DateTime::from_timestamp(
                            (nanos / 1_000_000_000) as i64,
                            (nanos % 1_000_000_000) as u32,
                        )
                    })
                    .map(|dt| dt.format("%Y-%m-%d").to_string())
                    .unwrap_or_else(|| Utc::now().format("%Y-%m-%d").to_string());
                let key = format!("{venue}/{date}");

                // get or create the writer for this key
                let writer = writers.entry(key.clone()).or_insert_with(|| {
                    let dir = base_dir.join(venue);
                    fs::create_dir_all(&dir).expect("failed to create WAL dir");
                    let path = dir.join(format!("{date}.wal"));
                    tracing::info!(path = %path.display(), "opening WAL file");
                    let file = OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(&path)
                        .expect("failed to open WAL file");
                    BufWriter::new(file)
                });

                // encode and write
                buf.clear();
                if let Err(e) = wire::encode(&event, &mut buf) {
                    tracing::warn!(error = ?e, "wire encode failed, event dropped");
                } else if let Err(e) = writer.write_all(&buf) {
                    tracing::warn!(error = %e, "WAL write failed");
                }
            }

            // periodic fsync
            if last_fsync.elapsed() >= FSYNC_INTERVAL {
                Self::fsync_all(&mut writers);
                last_fsync = Instant::now();
            }
        }

        // Channel disconnected — flush and sync everything.
        Self::fsync_all(&mut writers);
        tracing::info!("WAL writer thread exiting cleanly");
    }

    fn fsync_all(writers: &mut HashMap<String, BufWriter<File>>) {
        for (key, writer) in writers.iter_mut() {
            if let Err(e) = writer.flush() {
                tracing::warn!(key, error = %e, "failed to flush WAL");
            }
            if let Err(e) = writer.get_ref().sync_data() {
                tracing::warn!(key, error = %e, "failed to fsync WAL");
            }
        }
    }
}

impl Drop for WalWriter {
    fn drop(&mut self) {
        // Drop the sender first — this causes rx.recv() to return Err,
        // which breaks the loop and triggers flush.
        drop(self.tx.take());

        // Wait for the thread to finish flushing.
        if let Some(handle) = self.handle.take() {
            if handle.join().is_err() {
                tracing::warn!("WAL writer thread panicked");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use std::io::Read as _;
    use venue_core::*;

    #[test]
    fn test_wal_write_read() {
        let tmp = tempfile::tempdir().unwrap();
        let base_dir = tmp.path().to_path_buf();

        let events: Vec<Event> = (0..5)
            .map(|i| Event {
                venue: VenueId {
                    value: "test_venue".into(),
                },
                instrument: Some(InstrumentId {
                    value: "btcusdt".into(),
                }),
                // Use a fixed timestamp so the WAL file date is deterministic
                venue_ts: Some(1_700_000_000_000_000_000 + i * 1_000_000_000),
                local_ts: Some(1_700_000_000_100_000_000 + i * 1_000_000_000),
                payload: Payload::MarketData(MarketDataPayload::BookTicker {
                    best_bid: Level {
                        price: dec!(50000) + rust_decimal::Decimal::from(i),
                        qty: dec!(1.0),
                    },
                    best_ask: Level {
                        price: dec!(50001) + rust_decimal::Decimal::from(i),
                        qty: dec!(2.0),
                    },
                }),
                sequence: Some(i as u64),
            })
            .collect();

        // Write events through WalWriter, then drop to flush
        {
            let writer = WalWriter::new(base_dir.clone());
            for event in &events {
                writer.send(event);
            }
            // drop triggers flush + fsync
        }

        // Find the WAL file
        let wal_dir = base_dir.join("test_venue");
        let wal_files: Vec<_> = fs::read_dir(&wal_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map(|x| x == "wal").unwrap_or(false))
            .collect();
        assert_eq!(wal_files.len(), 1, "expected exactly one WAL file");

        // Read back and decode
        let wal_path = wal_files[0].path();
        let mut file = File::open(&wal_path).unwrap();
        let mut data = Vec::new();
        file.read_to_end(&mut data).unwrap();

        let mut offset = 0;
        let mut decoded_events = Vec::new();
        while offset < data.len() {
            let (event, consumed) = wire::decode(&data[offset..]).unwrap();
            decoded_events.push(event);
            offset += consumed;
        }

        assert_eq!(decoded_events.len(), 5);

        for (i, event) in decoded_events.iter().enumerate() {
            assert_eq!(event.venue.value.as_ref(), "test_venue");
            assert_eq!(event.instrument.as_ref().unwrap().value.as_ref(), "btcusdt");
            assert_eq!(event.sequence, Some(i as u64));

            match &event.payload {
                Payload::MarketData(MarketDataPayload::BookTicker { best_bid, best_ask }) => {
                    assert_eq!(
                        best_bid.price,
                        dec!(50000) + rust_decimal::Decimal::from(i as u64)
                    );
                    assert_eq!(
                        best_ask.price,
                        dec!(50001) + rust_decimal::Decimal::from(i as u64)
                    );
                }
                other => panic!("unexpected payload at index {i}: {other:?}"),
            }
        }
    }
}
