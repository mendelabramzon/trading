use chrono::{NaiveDate, Utc};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};
use venue_adapter::{EventSink, EventSinkError, RawFrameSink};
use venue_core::{Event, Nanos, RawFrame};

pub mod parquet_converter;

const FSYNC_INTERVAL: Duration = Duration::from_secs(1);

/// Capture channel capacity. Sized in time, not events: ~2 s of headroom at a
/// 50k events/s burst (N6) so a slow fsync cannot immediately backpressure the
/// venue connection.
const CHANNEL_CAPACITY: usize = 100_000;

/// WAL-failure fatality policy (N2): a capture process that cannot persist is
/// worse than a dead one — it looks healthy while recording nothing. Any I/O
/// error on the WAL path exits the process so the supervisor restarts it.
fn fatal_io(context: &str, err: &dyn std::fmt::Display) -> ! {
    tracing::error!(%err, context, "fatal WAL I/O error; exiting for supervisor restart");
    std::process::exit(1);
}

fn nanos_to_date(nanos: Nanos) -> NaiveDate {
    chrono::DateTime::from_timestamp(
        (nanos / 1_000_000_000) as i64,
        (nanos % 1_000_000_000) as u32,
    )
    .map(|dt| dt.date_naive())
    .unwrap_or_else(|| Utc::now().date_naive())
}

/// Shared writer loop for `.wal` (events) and `.rawwal` (raw frames): per
/// venue/day append files, 1 s fsync cadence, midnight rotation, fatal-exit on
/// I/O errors. `route` extracts the (venue, local_ts) routing key from a
/// record; records are framed by `wire::encode_frame`.
fn run_wal_loop<T, F>(base_dir: PathBuf, extension: &str, rx: mpsc::Receiver<T>, route: F)
where
    T: serde::Serialize,
    F: Fn(&T) -> (String, Nanos),
{
    let mut writers: HashMap<(String, NaiveDate), BufWriter<File>> = HashMap::new();
    let mut buf = Vec::with_capacity(4096);
    let mut last_fsync = Instant::now();

    loop {
        let remaining = FSYNC_INTERVAL.saturating_sub(last_fsync.elapsed());
        let record = match rx.recv_timeout(remaining) {
            Ok(record) => Some(record),
            Err(mpsc::RecvTimeoutError::Timeout) => None,
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        };

        if let Some(record) = record {
            // File date comes from local_ts (capture truth): files are
            // arrival-ordered and venue clocks must not pick the file.
            let (venue, local_ts) = route(&record);
            let date = nanos_to_date(local_ts);
            let key = (venue, date);

            let writer = writers.entry(key).or_insert_with_key(|(venue, date)| {
                if *date < Utc::now().date_naive() {
                    tracing::warn!(%venue, %date, "late record reopens an already-rotated date");
                }
                let dir = base_dir.join(venue);
                if let Err(e) = fs::create_dir_all(&dir) {
                    fatal_io("create WAL dir", &e);
                }
                let path = dir.join(format!("{date}.{extension}"));
                tracing::info!(path = %path.display(), "opening WAL file");
                match OpenOptions::new().create(true).append(true).open(&path) {
                    Ok(file) => BufWriter::new(file),
                    Err(e) => fatal_io("open WAL file", &e),
                }
            });

            buf.clear();
            // Encode errors are data bugs, not I/O failures: log and drop the
            // record rather than killing capture.
            if let Err(e) = wire::encode_frame(&record, &mut buf) {
                tracing::warn!(error = ?e, "wire encode failed, record dropped");
            } else if let Err(e) = writer.write_all(&buf) {
                fatal_io("write WAL frame", &e);
            }
        }

        if last_fsync.elapsed() >= FSYNC_INTERVAL {
            fsync_all(&mut writers);
            rotate(&mut writers);
            last_fsync = Instant::now();
        }
    }

    // Channel disconnected — flush and sync everything.
    fsync_all(&mut writers);
    tracing::info!("WAL writer thread exiting cleanly");
}

fn fsync_all(writers: &mut HashMap<(String, NaiveDate), BufWriter<File>>) {
    for ((venue, date), writer) in writers.iter_mut() {
        if let Err(e) = writer.flush() {
            fatal_io(&format!("flush WAL {venue}/{date}"), &e);
        }
        if let Err(e) = writer.get_ref().sync_data() {
            fatal_io(&format!("fsync WAL {venue}/{date}"), &e);
        }
    }
}

/// Midnight rotation: writers for past dates are flushed (by the fsync that
/// just ran) and dropped, closing the file handle. Their files are complete
/// and ready for conversion.
fn rotate(writers: &mut HashMap<(String, NaiveDate), BufWriter<File>>) {
    let today = Utc::now().date_naive();
    writers.retain(|(venue, date), _| {
        let keep = *date >= today;
        if !keep {
            tracing::info!(%venue, %date, "rotated WAL file (day complete)");
        }
        keep
    });
}

/// Durable event log: one dedicated OS thread owning all file I/O. Events are
/// routed to `{base_dir}/{venue}/{date}.wal` by their capture date.
pub struct WalWriter {
    tx: Option<mpsc::SyncSender<Event>>,
    handle: Option<thread::JoinHandle<()>>,
}

impl WalWriter {
    pub fn new(base_dir: PathBuf) -> Self {
        let (tx, rx) = mpsc::sync_channel::<Event>(CHANNEL_CAPACITY);

        let handle = thread::spawn(move || {
            run_wal_loop(base_dir, "wal", rx, |event: &Event| {
                (event.venue.value.to_string(), event.local_ts)
            });
        });

        Self {
            tx: Some(tx),
            handle: Some(handle),
        }
    }

    pub fn send(&self, event: &Event) {
        if let Some(tx) = &self.tx {
            if tx.send(event.clone()).is_err() {
                tracing::warn!("WAL event dropped: writer thread gone");
            }
        }
    }

    /// An `EventSink` handle feeding this writer. Clones share the channel.
    ///
    /// Shutdown contract: every `WalSink` clone (adapters!) must be dropped
    /// before the `WalWriter`, or `Drop` blocks forever waiting for the
    /// writer thread. Order: disconnect adapter → drop adapter → drop writer.
    pub fn sink(&self) -> WalSink {
        WalSink {
            tx: self
                .tx
                .clone()
                .expect("WalWriter::sink called after shutdown"),
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

/// Lossless `EventSink` into the WAL thread. `send` never drops: the fast
/// path is a non-blocking `try_send`; a full channel falls back to a blocking
/// send inside `block_in_place`.
///
/// Requires the multi-thread tokio runtime (`block_in_place` panics on the
/// current-thread flavor).
#[derive(Clone)]
pub struct WalSink {
    tx: mpsc::SyncSender<Event>,
}

impl WalSink {
    fn send_sync(&self, event: Event) -> Result<(), EventSinkError> {
        match self.tx.try_send(event) {
            Ok(()) => Ok(()),
            Err(mpsc::TrySendError::Full(event)) => {
                tracing::warn!("WAL channel full; backpressuring capture");
                tokio::task::block_in_place(|| self.tx.send(event))
                    .map_err(|_| EventSinkError::Closed)
            }
            Err(mpsc::TrySendError::Disconnected(_)) => Err(EventSinkError::Closed),
        }
    }
}

impl EventSink for WalSink {
    async fn send(&self, event: Event) -> Result<(), EventSinkError> {
        self.send_sync(event)
    }

    /// All events enter the channel with no await points between them, so a
    /// multi-event venue message stays contiguous in the WAL.
    async fn send_batch(&self, events: Vec<Event>) -> Result<(), EventSinkError> {
        for event in events {
            self.send_sync(event)?;
        }
        Ok(())
    }
}

/// Raw-frame capture tier (R2): tees raw venue frames to
/// `{base_dir}/{venue}/{date}.rawwal` so a parser defect means "re-run
/// normalization", not permanent loss. One writer per venue process.
pub struct RawWalWriter {
    tx: Option<mpsc::SyncSender<RawFrame>>,
    handle: Option<thread::JoinHandle<()>>,
}

impl RawWalWriter {
    pub fn new(base_dir: PathBuf, venue: &str) -> Self {
        let venue = venue.to_string();
        let (tx, rx) = mpsc::sync_channel::<RawFrame>(CHANNEL_CAPACITY);

        let handle = thread::spawn(move || {
            run_wal_loop(base_dir, "rawwal", rx, move |frame: &RawFrame| {
                (venue.clone(), frame.local_ts)
            });
        });

        Self {
            tx: Some(tx),
            handle: Some(handle),
        }
    }

    /// A `RawFrameSink` handle for adapters. Same shutdown contract as
    /// `WalSink`: drop all clones before dropping the writer.
    pub fn sink(&self) -> RawWalSink {
        RawWalSink {
            tx: self
                .tx
                .clone()
                .expect("RawWalWriter::sink called after shutdown"),
        }
    }
}

impl Drop for RawWalWriter {
    fn drop(&mut self) {
        drop(self.tx.take());
        if let Some(handle) = self.handle.take() {
            if handle.join().is_err() {
                tracing::warn!("raw WAL writer thread panicked");
            }
        }
    }
}

/// Best-effort tee: unlike the normalized WAL (lossless, blocking), the raw
/// tier drops frames when its channel is full rather than stall the venue
/// read loop — the normalized WAL remains the source of truth.
#[derive(Clone)]
pub struct RawWalSink {
    tx: mpsc::SyncSender<RawFrame>,
}

impl RawFrameSink for RawWalSink {
    fn send_raw(&self, frame: RawFrame) {
        if let Err(mpsc::TrySendError::Full(_)) = self.tx.try_send(frame) {
            tracing::warn!("raw WAL channel full; raw frame dropped (normalized WAL unaffected)");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use std::io::Read as _;
    use venue_core::*;

    fn make_event(i: u64, local_ts: Nanos) -> Event {
        Event {
            venue: VenueId {
                value: "test_venue".into(),
            },
            instrument: Some(InstrumentId {
                value: "btcusdt".into(),
            }),
            venue_ts: Some(local_ts - 100_000_000),
            local_ts,
            source: SourceId(1),
            provenance: None,
            payload: Payload::Market(MarketPayload::BookTicker {
                best_bid: Level {
                    price: dec!(50000) + rust_decimal::Decimal::from(i),
                    qty: dec!(1.0),
                },
                best_ask: Level {
                    price: dec!(50001) + rust_decimal::Decimal::from(i),
                    qty: dec!(2.0),
                },
                update_id: 1000 + i,
            }),
        }
    }

    #[test]
    fn test_wal_write_read() {
        let tmp = tempfile::tempdir().unwrap();
        let base_dir = tmp.path().to_path_buf();

        // Fixed timestamp so the WAL file date is deterministic.
        let events: Vec<Event> = (0..5)
            .map(|i| make_event(i, 1_700_000_000_100_000_000 + i * 1_000_000_000))
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
            assert_eq!(event.source, SourceId(1));

            match &event.payload {
                Payload::Market(MarketPayload::BookTicker {
                    best_bid,
                    best_ask,
                    update_id,
                }) => {
                    assert_eq!(*update_id, 1000 + i as u64);
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

    #[test]
    fn test_wal_routes_by_local_ts_date() {
        let tmp = tempfile::tempdir().unwrap();
        let base_dir = tmp.path().to_path_buf();

        // Two events on different UTC days.
        let day1 = 1_700_000_000_000_000_000u64; // 2023-11-14
        let day2 = day1 + 86_400_000_000_000; // 2023-11-15
        {
            let writer = WalWriter::new(base_dir.clone());
            writer.send(&make_event(0, day1));
            writer.send(&make_event(1, day2));
        }

        let wal_dir = base_dir.join("test_venue");
        let mut names: Vec<_> = fs::read_dir(&wal_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        assert_eq!(names, ["2023-11-14.wal", "2023-11-15.wal"]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_wal_sink_is_lossless_event_sink() {
        let tmp = tempfile::tempdir().unwrap();
        let base_dir = tmp.path().to_path_buf();

        {
            let writer = WalWriter::new(base_dir.clone());
            let sink = writer.sink();
            sink.send(make_event(0, 1_700_000_000_000_000_000))
                .await
                .unwrap();
            sink.send_batch(vec![
                make_event(1, 1_700_000_000_000_000_001),
                make_event(2, 1_700_000_000_000_000_002),
            ])
            .await
            .unwrap();
            // Shutdown contract: sink clones dropped before the writer.
            drop(sink);
        }

        let wal_path = base_dir.join("test_venue").join("2023-11-14.wal");
        let mut reader = wire::FrameReader::new(File::open(wal_path).unwrap());
        let mut count = 0;
        while reader.next_event().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 3);
    }

    #[test]
    fn test_raw_wal_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let base_dir = tmp.path().to_path_buf();

        let frames = vec![
            RawFrame {
                local_ts: 1_700_000_000_000_000_000,
                source: SourceId(1),
                bytes: br#"{"e":"bookTicker","s":"BTCUSDT"}"#.to_vec(),
            },
            RawFrame {
                local_ts: 1_700_000_000_000_000_001,
                source: SourceId(2),
                bytes: vec![0xDE, 0xAD, 0xBE, 0xEF],
            },
        ];

        {
            let writer = RawWalWriter::new(base_dir.clone(), "test_venue");
            let sink = writer.sink();
            for f in &frames {
                sink.send_raw(f.clone());
            }
            drop(sink);
        }

        let path = base_dir.join("test_venue").join("2023-11-14.rawwal");
        let mut reader = wire::FrameReader::new(File::open(path).unwrap());
        let mut decoded: Vec<RawFrame> = Vec::new();
        while let Some(f) = reader.next_frame::<RawFrame>().unwrap() {
            decoded.push(f);
        }
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].bytes, frames[0].bytes);
        assert_eq!(decoded[0].source, SourceId(1));
        assert_eq!(decoded[1].bytes, frames[1].bytes);
    }
}
