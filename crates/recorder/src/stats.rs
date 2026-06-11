//! Capture-side observability counters (P5d).
//!
//! Lock-free atomics shared via `Arc`: sinks and writer threads update them on
//! the hot path with `Relaxed` ordering (they are counters, not
//! synchronization), and the heartbeat task reads them once a minute.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use venue_adapter::{EventSink, EventSinkError};
use venue_core::{ControlPayload, Event, MarketPayload, Payload};

pub(crate) fn unix_now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

/// Counters for one WAL writer thread (normalized or raw). Queue depth is
/// derived from two monotonic totals so racing increments can never wrap a
/// gauge below zero.
#[derive(Debug, Default)]
pub struct WriterStats {
    enqueued: AtomicU64,
    dequeued: AtomicU64,
    written_frames: AtomicU64,
    last_fsync_unix_ns: AtomicU64,
    /// Raw tee only: frames dropped because the channel was full.
    dropped_frames: AtomicU64,
}

impl WriterStats {
    pub(crate) fn record_enqueued(&self) {
        self.enqueued.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_dequeued(&self) {
        self.dequeued.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_written(&self) {
        self.written_frames.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_fsync(&self) {
        self.last_fsync_unix_ns
            .store(unix_now_ns(), Ordering::Relaxed);
    }

    pub(crate) fn record_dropped(&self) {
        self.dropped_frames.fetch_add(1, Ordering::Relaxed);
    }

    /// Records currently sitting in the channel (approximate under load).
    pub fn queue_depth(&self) -> u64 {
        self.enqueued
            .load(Ordering::Relaxed)
            .saturating_sub(self.dequeued.load(Ordering::Relaxed))
    }

    /// Frames successfully written to the file (encode failures excluded).
    pub fn written(&self) -> u64 {
        self.written_frames.load(Ordering::Relaxed)
    }

    /// Raw-tee frames dropped on a full channel.
    pub fn dropped(&self) -> u64 {
        self.dropped_frames.load(Ordering::Relaxed)
    }

    /// Time since the last fsync; `None` until the first one. The writer
    /// fsyncs on a 1 s cadence, so a growing age means the thread is stuck.
    pub fn fsync_age(&self) -> Option<Duration> {
        let ns = self.last_fsync_unix_ns.load(Ordering::Relaxed);
        (ns != 0).then(|| Duration::from_nanos(unix_now_ns().saturating_sub(ns)))
    }
}

/// Event classification for stats and QA. `name()` strings match the
/// vocabulary used by the Parquet converter and the QA report.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EventKind {
    BookTicker,
    BookSnapshot,
    BookUpdate,
    Trades,
    MarkPrice,
    IndexPrice,
    FundingPrediction,
    FundingRealized,
    OpenInterest,
    Liquidation,
    Reference,
    Chain,
    Account,
    Control,
}

impl EventKind {
    pub const COUNT: usize = 14;

    pub const ALL: [EventKind; Self::COUNT] = [
        EventKind::BookTicker,
        EventKind::BookSnapshot,
        EventKind::BookUpdate,
        EventKind::Trades,
        EventKind::MarkPrice,
        EventKind::IndexPrice,
        EventKind::FundingPrediction,
        EventKind::FundingRealized,
        EventKind::OpenInterest,
        EventKind::Liquidation,
        EventKind::Reference,
        EventKind::Chain,
        EventKind::Account,
        EventKind::Control,
    ];

    pub fn of(payload: &Payload) -> Self {
        match payload {
            Payload::Market(m) => match m {
                MarketPayload::BookTicker { .. } => EventKind::BookTicker,
                MarketPayload::BookSnapshot { .. } => EventKind::BookSnapshot,
                MarketPayload::BookUpdate { .. } => EventKind::BookUpdate,
                MarketPayload::Trades { .. } => EventKind::Trades,
                MarketPayload::MarkPrice { .. } => EventKind::MarkPrice,
                MarketPayload::IndexPrice { .. } => EventKind::IndexPrice,
                MarketPayload::FundingRatePrediction { .. } => EventKind::FundingPrediction,
                MarketPayload::FundingRateRealized { .. } => EventKind::FundingRealized,
                MarketPayload::OpenInterest { .. } => EventKind::OpenInterest,
                MarketPayload::Liquidation { .. } => EventKind::Liquidation,
            },
            Payload::Reference(_) => EventKind::Reference,
            Payload::Chain(_) => EventKind::Chain,
            Payload::Account(_) => EventKind::Account,
            Payload::Control(_) => EventKind::Control,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            EventKind::BookTicker => "book_ticker",
            EventKind::BookSnapshot => "book_snapshot",
            EventKind::BookUpdate => "book_update",
            EventKind::Trades => "trades",
            EventKind::MarkPrice => "mark_price",
            EventKind::IndexPrice => "index_price",
            EventKind::FundingPrediction => "funding_prediction",
            EventKind::FundingRealized => "funding_realized",
            EventKind::OpenInterest => "open_interest",
            EventKind::Liquidation => "liquidation",
            EventKind::Reference => "reference",
            EventKind::Chain => "chain",
            EventKind::Account => "account",
            EventKind::Control => "control",
        }
    }

    fn index(self) -> usize {
        self as usize
    }
}

/// Per-kind event counters fed by a [`StatsSink`].
#[derive(Debug, Default)]
pub struct CaptureStats {
    total: AtomicU64,
    by_kind: [AtomicU64; EventKind::COUNT],
    last_event_unix_ns: [AtomicU64; EventKind::COUNT],
    conn_down: AtomicU64,
}

/// Point-in-time copy for rate computation (delta vs the previous beat).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CaptureSnapshot {
    pub total: u64,
    pub by_kind: [u64; EventKind::COUNT],
    pub conn_down: u64,
}

impl CaptureStats {
    fn record(&self, kind: EventKind, count: u64, now_ns: u64, conn_down: u64) {
        self.total.fetch_add(count, Ordering::Relaxed);
        self.by_kind[kind.index()].fetch_add(count, Ordering::Relaxed);
        self.last_event_unix_ns[kind.index()].store(now_ns, Ordering::Relaxed);
        if conn_down > 0 {
            self.conn_down.fetch_add(conn_down, Ordering::Relaxed);
        }
    }

    pub fn total(&self) -> u64 {
        self.total.load(Ordering::Relaxed)
    }

    pub fn count(&self, kind: EventKind) -> u64 {
        self.by_kind[kind.index()].load(Ordering::Relaxed)
    }

    /// Reconnect detector: ConnDown control events seen so far.
    pub fn conn_down(&self) -> u64 {
        self.conn_down.load(Ordering::Relaxed)
    }

    /// Age of the most recent event of this kind; `None` if never seen.
    pub fn staleness(&self, kind: EventKind) -> Option<Duration> {
        let ns = self.last_event_unix_ns[kind.index()].load(Ordering::Relaxed);
        (ns != 0).then(|| Duration::from_nanos(unix_now_ns().saturating_sub(ns)))
    }

    pub fn snapshot(&self) -> CaptureSnapshot {
        let mut by_kind = [0u64; EventKind::COUNT];
        for (slot, counter) in by_kind.iter_mut().zip(&self.by_kind) {
            *slot = counter.load(Ordering::Relaxed);
        }
        CaptureSnapshot {
            total: self.total.load(Ordering::Relaxed),
            by_kind,
            conn_down: self.conn_down.load(Ordering::Relaxed),
        }
    }
}

/// Forwards to the inner sink and counts accepted events per kind — the
/// heartbeat's data source. Counts only after the inner sink accepts, so the
/// numbers reflect what actually reached the WAL channel.
#[derive(Clone)]
pub struct StatsSink<S> {
    inner: S,
    stats: Arc<CaptureStats>,
}

impl<S> StatsSink<S> {
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            stats: Arc::new(CaptureStats::default()),
        }
    }

    pub fn stats(&self) -> Arc<CaptureStats> {
        Arc::clone(&self.stats)
    }
}

fn is_conn_down(event: &Event) -> bool {
    matches!(
        &event.payload,
        Payload::Control(ControlPayload::ConnDown { .. })
    )
}

impl<S: EventSink> EventSink for StatsSink<S> {
    async fn send(&self, event: Event) -> Result<(), EventSinkError> {
        let kind = EventKind::of(&event.payload);
        let conn_down = u64::from(is_conn_down(&event));
        self.inner.send(event).await?;
        self.stats.record(kind, 1, unix_now_ns(), conn_down);
        Ok(())
    }

    async fn send_batch(&self, events: Vec<Event>) -> Result<(), EventSinkError> {
        let mut by_kind = [0u64; EventKind::COUNT];
        let mut conn_down = 0u64;
        for event in &events {
            by_kind[EventKind::of(&event.payload).index()] += 1;
            conn_down += u64::from(is_conn_down(event));
        }
        self.inner.send_batch(events).await?;
        let now = unix_now_ns();
        for (kind, &count) in EventKind::ALL.iter().zip(&by_kind) {
            if count > 0 {
                self.stats.record(*kind, count, now, 0);
            }
        }
        if conn_down > 0 {
            self.stats.conn_down.fetch_add(conn_down, Ordering::Relaxed);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use venue_core::{Level, SourceId, VenueId};

    fn event(payload: Payload) -> Event {
        Event {
            venue: VenueId { value: "t".into() },
            instrument: None,
            venue_ts: None,
            local_ts: 1_700_000_000_000_000_000,
            source: SourceId(1),
            provenance: None,
            payload,
        }
    }

    fn book_ticker() -> Event {
        event(Payload::Market(MarketPayload::BookTicker {
            best_bid: Level {
                price: dec!(1),
                qty: dec!(1),
            },
            best_ask: Level {
                price: dec!(2),
                qty: dec!(1),
            },
            update_id: 1,
        }))
    }

    fn conn_down() -> Event {
        event(Payload::Control(ControlPayload::ConnDown {
            label: "ws-1".into(),
            reason: "test".into(),
        }))
    }

    #[tokio::test]
    async fn stats_sink_counts_per_kind_and_conn_down() {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Event>(16);
        let sink = StatsSink::new(tx);
        let stats = sink.stats();

        sink.send(book_ticker()).await.unwrap();
        sink.send(book_ticker()).await.unwrap();
        sink.send(conn_down()).await.unwrap();
        sink.send_batch(vec![book_ticker(), conn_down()])
            .await
            .unwrap();

        assert_eq!(stats.total(), 5);
        assert_eq!(stats.count(EventKind::BookTicker), 3);
        assert_eq!(stats.count(EventKind::Control), 2);
        assert_eq!(stats.count(EventKind::Trades), 0);
        assert_eq!(stats.conn_down(), 2);
        assert!(stats.staleness(EventKind::BookTicker).is_some());
        assert!(stats.staleness(EventKind::Trades).is_none());

        // All five events reached the inner sink.
        let mut forwarded = 0;
        while rx.try_recv().is_ok() {
            forwarded += 1;
        }
        assert_eq!(forwarded, 5);
    }

    #[tokio::test]
    async fn stats_sink_does_not_count_rejected_events() {
        let (tx, rx) = tokio::sync::mpsc::channel::<Event>(1);
        drop(rx); // inner sink is closed
        let sink = StatsSink::new(tx);
        let stats = sink.stats();

        assert!(sink.send(book_ticker()).await.is_err());
        assert_eq!(stats.total(), 0);
    }

    #[test]
    fn event_kind_snapshot_roundtrip() {
        let stats = CaptureStats::default();
        stats.record(EventKind::Trades, 3, unix_now_ns(), 0);
        let snap = stats.snapshot();
        assert_eq!(snap.total, 3);
        assert_eq!(snap.by_kind[EventKind::Trades as usize], 3);
        assert_eq!(EventKind::of(&conn_down().payload), EventKind::Control);
        assert_eq!(EventKind::Trades.name(), "trades");
    }
}
