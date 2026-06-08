use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::mpsc;
use std::thread;
use venue_core::Event;
use chrono::Utc;

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
            if let Err(_) = tx.send(event.clone()) {
                tracing::warn!("WAL event dropped: channel full or closed");
            }
        }
    }

    fn run(base_dir: PathBuf, rx: mpsc::Receiver<Event>) {
        let mut writers: HashMap<String, BufWriter<File>> = HashMap::new();
        let mut buf = Vec::with_capacity(4096);

        while let Ok(event) = rx.recv() {
            // build the key: "binance/2026-06-05"
            let venue = &event.venue.value;
            let date = Utc::now().format("%Y-%m-%d").to_string();
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
            } else {
                if let Err(e) = writer.write_all(&buf) {
                    tracing::warn!(error = %e, "WAL write failed");
                }
            }
        }

        // rx.recv() returned Err — all senders dropped. Flush everything.
        for (key, mut writer) in writers.drain() {
            if let Err(e) = writer.flush() {
                tracing::warn!(key, error = %e, "failed to flush WAL on shutdown");
            }
        }
        tracing::info!("WAL writer thread exiting cleanly");
    }
}

impl Drop for WalWriter {
    fn drop(&mut self) {
        // Drop the sender first — this causes rx.recv() to return Err,
        // which breaks the loop and triggers flush.
        drop(self.tx.take());

        // Wait for the thread to finish flushing.
        if let Some(handle) = self.handle.take() {
            if let Err(_) = handle.join() {
                tracing::warn!("WAL writer thread panicked");
            }
        }
    }
}
