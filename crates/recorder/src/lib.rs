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
    tx: mpsc::SyncSender<Event>,
    handle: Option<thread::JoinHandle<()>>,
}

impl WalWriter {
    pub fn new(base_dir: PathBuf) -> Self {
        let (tx, rx) = mpsc::sync_channel::<Event>(10_000);

        let handle = thread::spawn(move || {
            Self::run(base_dir, rx);
        });

        Self {
            tx,
            handle: Some(handle),
        }
    }

    pub fn send(&self, event: &Event) {
        let _ = self.tx.send(event.clone());
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
                let file = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&path)
                    .expect("failed to open WAL file");
                BufWriter::new(file)
            });

            // encode and write
            buf.clear();
            if wire::encode(&event, &mut buf).is_ok() {
                let _ = writer.write_all(&buf);
            }
        }
    }
}