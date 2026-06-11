//! Per-file capture QA (the daily QA report).
//!
//! One streaming pass over a WAL, O(1) per event, bounded memory: per-kind
//! counts, per-instrument coverage, depth-chain validation (pu == previous u),
//! snapshot splice checks (`U <= lastUpdateId <= u`), duplicate/regression
//! detection on venue sequence ids, and latency histograms. Chain breaks and
//! trade-id regressions are split into *explained* (a ConnDown was seen for
//! the source that fed the instrument — reconnects legitimately lose venue
//! messages) and *unexplained* (data loss with a healthy connection — a QA
//! failure).
//!
//! The JSON report (`schema_version` 1) is additive-evolution: consumers must
//! ignore unknown fields. Phase 3 folds these rows into the manifest (R10).

use crate::stats::EventKind;
use serde::Serialize;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use venue_core::{ControlPayload, Event, MarketPayload, Nanos, Payload, SourceId};

/// Fail when corruption exceeds this fraction of file bytes (mirrors the
/// converter's gate, P1).
const MAX_SKIPPED_RATIO: f64 = 0.01;
/// A snapshot still unspliced this close to EOF is "pending", not a failure:
/// its splicing diff lands in the next file.
const PENDING_AT_EOF_NS: Nanos = 60 * 1_000_000_000;
/// Control-timeline cap: a flapping day must not balloon the report.
const TIMELINE_CAP: usize = 500;

#[derive(Debug)]
pub enum QaError {
    Io(std::io::Error),
}

impl std::fmt::Display for QaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QaError::Io(e) => write!(f, "QA read failed: {e}"),
        }
    }
}

impl std::error::Error for QaError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum QaStatus {
    Pass,
    Fail,
}

#[derive(Debug, Serialize)]
pub struct QaReport {
    pub schema_version: u32,
    pub venue: String,
    pub date: String,
    pub wal_file: String,
    pub wal_bytes: u64,
    pub generated_at: String,
    pub status: QaStatus,
    pub failures: Vec<String>,
    pub conversion: ConversionStatus,
    pub frames: FrameStats,
    pub events: EventTotals,
    pub instruments: BTreeMap<String, InstrumentCoverage>,
    pub depth: BTreeMap<String, DepthQa>,
    pub dups: BTreeMap<String, DupStats>,
    pub control: ControlCounts,
    pub latency_us: LatencyStats,
}

impl QaReport {
    /// Fold a conversion outcome into the report (the sweep converts first,
    /// then runs QA; a refused conversion must fail the day loudly).
    pub fn set_conversion_error(&mut self, error: String) {
        self.failures.push(format!("conversion failed: {error}"));
        self.conversion = ConversionStatus {
            ok: false,
            error: Some(error),
        };
        self.status = QaStatus::Fail;
    }
}

#[derive(Debug, Serialize)]
pub struct ConversionStatus {
    pub ok: bool,
    pub error: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct FrameStats {
    pub frames_ok: u64,
    pub skipped_bytes: u64,
    pub resyncs: u64,
    pub undecodable_frames: u64,
    pub truncated_tail: bool,
    pub skipped_ratio: f64,
}

#[derive(Debug, Serialize)]
pub struct EventTotals {
    pub total: u64,
    pub by_kind: BTreeMap<&'static str, u64>,
}

#[derive(Debug, Default, Serialize)]
pub struct InstrumentCoverage {
    pub events: u64,
    pub first_local_ts: Nanos,
    pub last_local_ts: Nanos,
    pub by_kind: BTreeMap<&'static str, u64>,
}

#[derive(Debug, Default, Serialize)]
pub struct DepthQa {
    pub updates: u64,
    pub snapshots: u64,
    pub chain_breaks_explained: u64,
    pub chain_breaks_unexplained: u64,
    pub book_update_overlap: u64,
    pub unspliced_snapshots: u64,
    pub snapshots_pending_at_eof: u64,
    pub snapshots_abandoned_by_reconnect: u64,
    pub missing_snapshot: bool,
}

#[derive(Debug, Default, Serialize)]
pub struct DupStats {
    pub trade_id_regressions_explained: u64,
    pub trade_id_regressions_unexplained: u64,
    pub book_ticker_update_id_regressions: u64,
}

#[derive(Debug, Default, Serialize)]
pub struct ControlCounts {
    pub conn_up: u64,
    pub conn_down: u64,
    pub gap_events: u64,
    pub gap_dropped_total: u64,
    pub sub_ack_ok: u64,
    pub sub_ack_failed: u64,
    pub timeline: Vec<String>,
    pub timeline_truncated: bool,
}

#[derive(Debug, Serialize)]
pub struct LatencyStats {
    /// Depth `E − T` (event time minus transaction time), venue-side queueing.
    pub depth_e_minus_t: Option<HistogramSummary>,
    /// `local_ts − venue_ts` per kind: capture-path latency + clock offset.
    pub local_minus_venue: BTreeMap<&'static str, HistogramSummary>,
}

/// Fixed-width latency histogram: 1 ms buckets over [−1 s, +10 s] plus
/// under/overflow tails and exact min/max. ~88 KB; safe for day-scale files
/// where collecting raw deltas is not.
pub struct Histogram {
    buckets: Vec<u64>,
    underflow: u64,
    overflow: u64,
    count: u64,
    min_ns: i64,
    max_ns: i64,
}

const BUCKET_NS: i64 = 1_000_000; // 1 ms
const LO_NS: i64 = -1_000_000_000; // −1 s
const HI_NS: i64 = 10_000_000_000; // +10 s
const N_BUCKETS: usize = ((HI_NS - LO_NS) / BUCKET_NS) as usize;

impl Default for Histogram {
    fn default() -> Self {
        Self {
            buckets: vec![0; N_BUCKETS],
            underflow: 0,
            overflow: 0,
            count: 0,
            min_ns: i64::MAX,
            max_ns: i64::MIN,
        }
    }
}

impl Histogram {
    pub fn record(&mut self, delta_ns: i64) {
        self.count += 1;
        self.min_ns = self.min_ns.min(delta_ns);
        self.max_ns = self.max_ns.max(delta_ns);
        if delta_ns < LO_NS {
            self.underflow += 1;
        } else if delta_ns >= HI_NS {
            self.overflow += 1;
        } else {
            self.buckets[((delta_ns - LO_NS) / BUCKET_NS) as usize] += 1;
        }
    }

    /// Percentile resolved to a bucket's upper edge (1 ms resolution); tails
    /// resolve to the exact min/max.
    pub fn percentile_us(&self, p: f64) -> Option<i64> {
        if self.count == 0 {
            return None;
        }
        let rank = ((p * self.count as f64).ceil() as u64).clamp(1, self.count);
        let mut seen = self.underflow;
        if rank <= seen {
            return Some(self.min_ns / 1_000);
        }
        for (i, n) in self.buckets.iter().enumerate() {
            seen += n;
            if rank <= seen {
                return Some((LO_NS + (i as i64 + 1) * BUCKET_NS) / 1_000);
            }
        }
        Some(self.max_ns / 1_000)
    }

    pub fn summary(&self) -> Option<HistogramSummary> {
        (self.count > 0).then(|| HistogramSummary {
            count: self.count,
            p50_us: self.percentile_us(0.50).unwrap_or_default(),
            p95_us: self.percentile_us(0.95).unwrap_or_default(),
            p99_us: self.percentile_us(0.99).unwrap_or_default(),
            min_us: self.min_ns / 1_000,
            max_us: self.max_ns / 1_000,
        })
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct HistogramSummary {
    pub count: u64,
    pub p50_us: i64,
    pub p95_us: i64,
    pub p99_us: i64,
    pub min_us: i64,
    pub max_us: i64,
}

#[derive(Default)]
struct SnapState {
    last_update_id: u64,
    local_ts: Nanos,
    spliced: bool,
    /// The connection feeding this instrument died while the snapshot was
    /// still awaiting its covering update: the splice window was cut short,
    /// not the data path — excused, like reconnect chain breaks (the fetcher
    /// re-snapshots on reconnect).
    abandoned_by_reconnect: bool,
}

#[derive(Default)]
struct InstState {
    coverage: InstrumentCoverage,
    // depth
    updates: u64,
    last_final: Option<u64>,
    last_update_source: Option<SourceId>,
    depth_reconnect_pending: bool,
    chain_breaks_explained: u64,
    chain_breaks_unexplained: u64,
    book_update_overlap: u64,
    snapshots: Vec<SnapState>,
    // trades / book ticker sequence ids
    last_trade_id: Option<u64>,
    last_trade_source: Option<SourceId>,
    trade_reconnect_pending: bool,
    trade_regr_explained: u64,
    trade_regr_unexplained: u64,
    last_ticker_update_id: Option<u64>,
    ticker_regressions: u64,
}

/// Streaming QA accumulator; `push` is O(1) per event.
#[derive(Default)]
struct Accumulator {
    total: u64,
    by_kind: [u64; EventKind::COUNT],
    instruments: BTreeMap<String, InstState>,
    control: ControlCounts,
    timeline_dropped: bool,
    e_minus_t: Histogram,
    local_minus_venue: [Option<Box<Histogram>>; EventKind::COUNT],
    max_local_ts: Nanos,
}

impl Accumulator {
    fn push(&mut self, event: &Event) {
        let kind = EventKind::of(&event.payload);
        self.total += 1;
        self.by_kind[kind as usize] += 1;
        self.max_local_ts = self.max_local_ts.max(event.local_ts);

        if let Some(venue_ts) = event.venue_ts {
            self.local_minus_venue[kind as usize]
                .get_or_insert_with(Default::default)
                .record(event.local_ts as i64 - venue_ts as i64);
        }

        if let Payload::Control(c) = &event.payload {
            self.push_control(event, c);
            return;
        }

        let Some(instrument) = &event.instrument else {
            return; // malformed market event; converter counts these (N3)
        };
        let st = self
            .instruments
            .entry(instrument.value.to_string())
            .or_default();
        st.coverage.events += 1;
        *st.coverage.by_kind.entry(kind.name()).or_default() += 1;
        if st.coverage.first_local_ts == 0 {
            st.coverage.first_local_ts = event.local_ts;
        }
        st.coverage.last_local_ts = event.local_ts;

        match &event.payload {
            Payload::Market(MarketPayload::BookUpdate {
                first_update_id,
                final_update_id,
                prev_final_update_id,
                event_time,
                ..
            }) => {
                st.updates += 1;
                if let (Some(pu), Some(prev_u)) = (prev_final_update_id, st.last_final) {
                    if *pu != prev_u {
                        if st.depth_reconnect_pending {
                            st.chain_breaks_explained += 1;
                        } else {
                            st.chain_breaks_unexplained += 1;
                        }
                    } else if *first_update_id <= prev_u {
                        st.book_update_overlap += 1;
                    }
                }
                st.last_final = Some(*final_update_id);
                st.last_update_source = Some(event.source);
                st.depth_reconnect_pending = false;

                if let (Some(et), Some(vt)) = (event_time, event.venue_ts) {
                    self.e_minus_t.record(*et as i64 - vt as i64);
                }
                // Documented splice contract (payloads.rs): the update that
                // continues a snapshot satisfies `U <= lastUpdateId + 1 <= u`
                // — including the perfectly-contiguous `U == lastUpdateId+1`.
                for snap in st.snapshots.iter_mut().filter(|s| !s.spliced) {
                    let next_needed = snap.last_update_id + 1;
                    if *first_update_id <= next_needed && next_needed <= *final_update_id {
                        snap.spliced = true;
                    }
                }
            }
            Payload::Market(MarketPayload::BookSnapshot { last_update_id, .. }) => {
                st.snapshots.push(SnapState {
                    last_update_id: *last_update_id,
                    local_ts: event.local_ts,
                    ..SnapState::default()
                });
            }
            Payload::Market(MarketPayload::Trades { trades }) => {
                for trade in trades {
                    // Binance per-fill ids are numeric and monotonic per
                    // symbol; non-numeric ids (other venues) skip dup QA.
                    let Ok(id) = trade.id.parse::<u64>() else {
                        continue;
                    };
                    if let Some(last) = st.last_trade_id {
                        if id <= last {
                            if st.trade_reconnect_pending {
                                st.trade_regr_explained += 1;
                            } else {
                                st.trade_regr_unexplained += 1;
                            }
                        }
                    }
                    st.last_trade_id = Some(id);
                }
                st.last_trade_source = Some(event.source);
                st.trade_reconnect_pending = false;
            }
            Payload::Market(MarketPayload::BookTicker { update_id, .. }) => {
                if let Some(last) = st.last_ticker_update_id {
                    if *update_id <= last {
                        st.ticker_regressions += 1;
                    }
                }
                st.last_ticker_update_id = Some(*update_id);
            }
            _ => {}
        }
    }

    fn push_control(&mut self, event: &Event, c: &ControlPayload) {
        let desc = match c {
            ControlPayload::ConnUp { label } => {
                self.control.conn_up += 1;
                format!("ConnUp {label}")
            }
            ControlPayload::ConnDown { label, reason } => {
                self.control.conn_down += 1;
                // Reconnects legitimately lose venue messages: excuse the
                // next sequence break on every instrument this source fed.
                for st in self.instruments.values_mut() {
                    if st.last_update_source == Some(event.source) {
                        st.depth_reconnect_pending = true;
                        for snap in st.snapshots.iter_mut().filter(|s| !s.spliced) {
                            snap.abandoned_by_reconnect = true;
                        }
                    }
                    if st.last_trade_source == Some(event.source) {
                        st.trade_reconnect_pending = true;
                    }
                }
                format!("ConnDown {label}: {reason}")
            }
            ControlPayload::Gap { reason, dropped } => {
                self.control.gap_events += 1;
                self.control.gap_dropped_total += dropped;
                format!("Gap {reason}: {dropped}")
            }
            ControlPayload::SubAck { request_id, ok, .. } => {
                if *ok {
                    self.control.sub_ack_ok += 1;
                } else {
                    self.control.sub_ack_failed += 1;
                }
                format!("SubAck id={request_id} ok={ok}")
            }
            other => format!("{other:?}"),
        };
        if self.control.timeline.len() < TIMELINE_CAP {
            self.control
                .timeline
                .push(format!("local_ts={} {desc}", event.local_ts));
        } else {
            self.timeline_dropped = true;
        }
    }

    fn finish(
        mut self,
        venue: &str,
        date: &str,
        wal_path: &Path,
        wal_bytes: u64,
        frames: FrameStats,
        mut failures: Vec<String>,
    ) -> QaReport {
        let mut by_kind = BTreeMap::new();
        for kind in EventKind::ALL {
            if self.by_kind[kind as usize] > 0 {
                by_kind.insert(kind.name(), self.by_kind[kind as usize]);
            }
        }

        let mut instruments = BTreeMap::new();
        let mut depth = BTreeMap::new();
        let mut dups = BTreeMap::new();
        for (id, st) in std::mem::take(&mut self.instruments) {
            if st.updates > 0 || !st.snapshots.is_empty() {
                let mut d = DepthQa {
                    updates: st.updates,
                    snapshots: st.snapshots.len() as u64,
                    chain_breaks_explained: st.chain_breaks_explained,
                    chain_breaks_unexplained: st.chain_breaks_unexplained,
                    book_update_overlap: st.book_update_overlap,
                    missing_snapshot: st.updates > 0 && st.snapshots.is_empty(),
                    ..DepthQa::default()
                };
                for snap in &st.snapshots {
                    if !snap.spliced {
                        if snap.abandoned_by_reconnect {
                            d.snapshots_abandoned_by_reconnect += 1;
                        } else if snap.local_ts + PENDING_AT_EOF_NS >= self.max_local_ts {
                            d.snapshots_pending_at_eof += 1;
                        } else {
                            d.unspliced_snapshots += 1;
                        }
                    }
                }
                if d.chain_breaks_unexplained > 0 {
                    failures.push(format!(
                        "{id}: {} unexplained depth chain break(s)",
                        d.chain_breaks_unexplained
                    ));
                }
                if d.unspliced_snapshots > 0 {
                    failures.push(format!(
                        "{id}: {} snapshot(s) never spliceable against the update stream",
                        d.unspliced_snapshots
                    ));
                }
                if d.missing_snapshot {
                    failures.push(format!("{id}: depth updates but no REST snapshot all file"));
                }
                depth.insert(id.clone(), d);
            }
            if st.trade_regr_explained + st.trade_regr_unexplained + st.ticker_regressions > 0 {
                // Report-only in v1: promote to gating once baselines are
                // clean for a while.
                dups.insert(
                    id.clone(),
                    DupStats {
                        trade_id_regressions_explained: st.trade_regr_explained,
                        trade_id_regressions_unexplained: st.trade_regr_unexplained,
                        book_ticker_update_id_regressions: st.ticker_regressions,
                    },
                );
            }
            instruments.insert(id, st.coverage);
        }

        if frames.skipped_ratio > MAX_SKIPPED_RATIO {
            failures.push(format!(
                "corrupt bytes ratio {:.4} exceeds {MAX_SKIPPED_RATIO}",
                frames.skipped_ratio
            ));
        }
        if self.total == 0 {
            failures.push("file contains zero decodable events".into());
        }

        let mut local_minus_venue = BTreeMap::new();
        for kind in EventKind::ALL {
            if let Some(h) = &self.local_minus_venue[kind as usize] {
                if let Some(s) = h.summary() {
                    local_minus_venue.insert(kind.name(), s);
                }
            }
        }

        let mut control = self.control;
        control.timeline_truncated = self.timeline_dropped;

        let status = if failures.is_empty() {
            QaStatus::Pass
        } else {
            QaStatus::Fail
        };
        QaReport {
            schema_version: 1,
            venue: venue.to_string(),
            date: date.to_string(),
            wal_file: wal_path.display().to_string(),
            wal_bytes,
            generated_at: chrono::Utc::now().to_rfc3339(),
            status,
            failures,
            conversion: ConversionStatus {
                ok: true,
                error: None,
            },
            frames,
            events: EventTotals {
                total: self.total,
                by_kind,
            },
            instruments,
            depth,
            dups,
            control,
            latency_us: LatencyStats {
                depth_e_minus_t: self.e_minus_t.summary(),
                local_minus_venue,
            },
        }
    }
}

/// Run QA over one WAL file. Mid-file fatal decode errors (e.g. a sticky
/// BadVersion) do not abort: the report carries them as failures.
pub fn qa_wal(wal_path: &Path, venue: &str, date: &str) -> Result<QaReport, QaError> {
    let wal_bytes = std::fs::metadata(wal_path).map_err(QaError::Io)?.len();
    let file = File::open(wal_path).map_err(QaError::Io)?;
    let mut reader = wire::FrameReader::new(BufReader::new(file));

    let mut acc = Accumulator::default();
    let mut failures = Vec::new();
    loop {
        match reader.next_event() {
            Ok(Some(event)) => acc.push(&event),
            Ok(None) => break,
            Err(e) => {
                failures.push(format!(
                    "fatal decode error after {} events: {e}",
                    acc.total
                ));
                break;
            }
        }
    }

    let s = reader.stats().clone();
    let frames = FrameStats {
        frames_ok: s.frames_ok,
        skipped_bytes: s.skipped_bytes,
        resyncs: s.resyncs,
        undecodable_frames: s.undecodable_frames,
        truncated_tail: s.truncated_tail,
        skipped_ratio: s.skipped_bytes as f64 / (wal_bytes.max(1)) as f64,
    };
    Ok(acc.finish(venue, date, wal_path, wal_bytes, frames, failures))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use std::sync::Arc;
    use venue_core::{InstrumentId, Level, Trade, VenueId};

    fn write_wal(dir: &Path, name: &str, events: &[Event]) -> std::path::PathBuf {
        let mut bytes = Vec::new();
        let mut buf = Vec::new();
        for e in events {
            buf.clear();
            wire::encode_frame(e, &mut buf).unwrap();
            bytes.extend_from_slice(&buf);
        }
        let path = dir.join(name);
        std::fs::write(&path, bytes).unwrap();
        path
    }

    fn event(instrument: &str, local_ts: Nanos, source: u16, payload: Payload) -> Event {
        Event {
            venue: VenueId {
                value: "binance".into(),
            },
            instrument: (!instrument.is_empty()).then(|| InstrumentId {
                value: instrument.into(),
            }),
            venue_ts: Some(local_ts.saturating_sub(50_000_000)),
            local_ts,
            source: SourceId(source),
            provenance: None,
            payload,
        }
    }

    fn book_update(
        inst: &str,
        ts: Nanos,
        source: u16,
        first: u64,
        last: u64,
        pu: Option<u64>,
    ) -> Event {
        event(
            inst,
            ts,
            source,
            Payload::Market(MarketPayload::BookUpdate {
                bids: vec![],
                asks: vec![],
                first_update_id: first,
                final_update_id: last,
                prev_final_update_id: pu,
                event_time: Some(ts.saturating_sub(40_000_000)),
            }),
        )
    }

    fn snapshot(inst: &str, ts: Nanos, last_update_id: u64) -> Event {
        event(
            inst,
            ts,
            0,
            Payload::Market(MarketPayload::BookSnapshot {
                bids: vec![Level {
                    price: dec!(1),
                    qty: dec!(1),
                }],
                asks: vec![],
                last_update_id,
            }),
        )
    }

    fn trades(inst: &str, ts: Nanos, source: u16, ids: &[&str]) -> Event {
        event(
            inst,
            ts,
            source,
            Payload::Market(MarketPayload::Trades {
                trades: ids
                    .iter()
                    .map(|id| Trade {
                        id: Arc::from(*id),
                        price: dec!(100),
                        qty: dec!(1),
                        aggressor_side: venue_core::AggressorSide::Buy,
                        kind: None,
                    })
                    .collect(),
            }),
        )
    }

    fn conn_down(ts: Nanos, source: u16) -> Event {
        event(
            "",
            ts,
            source,
            Payload::Control(ControlPayload::ConnDown {
                label: "ws-1".into(),
                reason: "test".into(),
            }),
        )
    }

    const T0: Nanos = 1_700_000_000_000_000_000;
    const SEC: Nanos = 1_000_000_000;

    #[test]
    fn clean_chain_passes() {
        let tmp = tempfile::tempdir().unwrap();
        let events = vec![
            snapshot("btcusdt", T0, 100),
            book_update("btcusdt", T0 + SEC, 1, 95, 105, None),
            book_update("btcusdt", T0 + 2 * SEC, 1, 106, 110, Some(105)),
            book_update("btcusdt", T0 + 3 * SEC, 1, 111, 115, Some(110)),
        ];
        let path = write_wal(tmp.path(), "a.wal", &events);
        let r = qa_wal(&path, "binance", "2026-06-10").unwrap();
        assert_eq!(r.status, QaStatus::Pass, "failures: {:?}", r.failures);
        assert_eq!(r.events.total, 4);
        let d = &r.depth["btcusdt"];
        assert_eq!(d.updates, 3);
        assert_eq!(d.chain_breaks_unexplained, 0);
        assert_eq!(d.unspliced_snapshots, 0);
        assert!(!d.missing_snapshot);
        assert!(r.latency_us.depth_e_minus_t.is_some());
        assert!(r.latency_us.local_minus_venue.contains_key("book_update"));
    }

    #[test]
    fn chain_break_without_conn_down_fails() {
        let tmp = tempfile::tempdir().unwrap();
        let events = vec![
            snapshot("btcusdt", T0, 100),
            book_update("btcusdt", T0 + SEC, 1, 95, 105, None),
            book_update("btcusdt", T0 + 2 * SEC, 1, 200, 210, Some(199)), // pu != 105
        ];
        let path = write_wal(tmp.path(), "a.wal", &events);
        let r = qa_wal(&path, "binance", "2026-06-10").unwrap();
        assert_eq!(r.status, QaStatus::Fail);
        assert_eq!(r.depth["btcusdt"].chain_breaks_unexplained, 1);
        assert!(r.failures.iter().any(|f| f.contains("chain break")));
    }

    #[test]
    fn chain_break_after_conn_down_is_explained() {
        let tmp = tempfile::tempdir().unwrap();
        let events = vec![
            snapshot("btcusdt", T0, 100),
            book_update("btcusdt", T0 + SEC, 1, 95, 105, None),
            conn_down(T0 + 2 * SEC, 1),
            book_update("btcusdt", T0 + 3 * SEC, 1, 200, 210, Some(199)),
            // snapshot after reconnect splices the new chain
            snapshot("btcusdt", T0 + 4 * SEC, 205),
            book_update("btcusdt", T0 + 5 * SEC, 1, 203, 211, Some(210)),
        ];
        let path = write_wal(tmp.path(), "a.wal", &events);
        let r = qa_wal(&path, "binance", "2026-06-10").unwrap();
        let d = &r.depth["btcusdt"];
        assert_eq!(d.chain_breaks_explained, 1);
        // The second break (pu=210 vs prev 210 — none) is clean.
        assert_eq!(d.chain_breaks_unexplained, 0);
        assert_eq!(r.status, QaStatus::Pass, "failures: {:?}", r.failures);
        assert_eq!(r.control.conn_down, 1);
    }

    #[test]
    fn conn_down_on_other_source_does_not_excuse() {
        let tmp = tempfile::tempdir().unwrap();
        let events = vec![
            snapshot("btcusdt", T0, 100),
            book_update("btcusdt", T0 + SEC, 1, 95, 105, None),
            conn_down(T0 + 2 * SEC, 7), // different connection
            book_update("btcusdt", T0 + 3 * SEC, 1, 200, 210, Some(199)),
        ];
        let path = write_wal(tmp.path(), "a.wal", &events);
        let r = qa_wal(&path, "binance", "2026-06-10").unwrap();
        assert_eq!(r.depth["btcusdt"].chain_breaks_unexplained, 1);
        assert_eq!(r.status, QaStatus::Fail);
    }

    #[test]
    fn splice_boundaries_and_unspliced() {
        let tmp = tempfile::tempdir().unwrap();
        // Contract: `U <= lastUpdateId + 1 <= u`. Covers the mid-range case,
        // both boundaries, and the perfectly-contiguous next update
        // (U == lastUpdateId + 1); a stale snapshot below every update range
        // (long before EOF) stays unspliced.
        let events = vec![
            snapshot("a", T0, 50),        // stale: never covered
            snapshot("a", T0 + SEC, 100), // 100+1 ∈ [100, 105]
            book_update("a", T0 + 2 * SEC, 1, 100, 105, None),
            snapshot("a", T0 + 3 * SEC, 110), // contiguous: U == 111
            book_update("a", T0 + 4 * SEC, 1, 106, 110, Some(105)), // u == 110 < 111: no
            // push EOF far past the stale snapshot's pending window
            book_update("a", T0 + 200 * SEC, 1, 111, 112, Some(110)), // 111 <= 111 <= 112: yes
        ];
        let path = write_wal(tmp.path(), "a.wal", &events);
        let r = qa_wal(&path, "binance", "2026-06-10").unwrap();
        let d = &r.depth["a"];
        assert_eq!(d.snapshots, 3);
        assert_eq!(d.unspliced_snapshots, 1); // only the stale one
        assert_eq!(d.snapshots_pending_at_eof, 0);
        assert_eq!(r.status, QaStatus::Fail);
    }

    #[test]
    fn snapshot_near_eof_is_pending_not_failure() {
        let tmp = tempfile::tempdir().unwrap();
        let events = vec![
            book_update("a", T0, 1, 95, 105, None),
            snapshot("a", T0 + 30 * SEC, 500), // 30 s before EOF: splice lands tomorrow
            book_update("a", T0 + 31 * SEC, 1, 106, 110, Some(105)),
        ];
        let path = write_wal(tmp.path(), "a.wal", &events);
        let r = qa_wal(&path, "binance", "2026-06-10").unwrap();
        let d = &r.depth["a"];
        assert_eq!(d.snapshots_pending_at_eof, 1);
        assert_eq!(d.unspliced_snapshots, 0);
        assert_eq!(r.status, QaStatus::Pass, "failures: {:?}", r.failures);
    }

    #[test]
    fn snapshot_cut_off_by_disconnect_is_abandoned_not_failed() {
        let tmp = tempfile::tempdir().unwrap();
        let events = vec![
            book_update("a", T0, 1, 95, 105, None),
            snapshot("a", T0 + SEC, 500), // awaiting update 501
            conn_down(T0 + 2 * SEC, 1),   // connection dies first
            // session 2, fresh chain + fresh snapshot, far past pending window
            book_update("a", T0 + 200 * SEC, 1, 700, 710, Some(699)),
            snapshot("a", T0 + 201 * SEC, 705),
            book_update("a", T0 + 202 * SEC, 1, 706, 711, Some(710)),
        ];
        let path = write_wal(tmp.path(), "a.wal", &events);
        let r = qa_wal(&path, "binance", "2026-06-10").unwrap();
        let d = &r.depth["a"];
        assert_eq!(d.snapshots_abandoned_by_reconnect, 1);
        assert_eq!(d.unspliced_snapshots, 0);
        assert_eq!(d.chain_breaks_explained, 1);
        assert_eq!(d.chain_breaks_unexplained, 0);
        assert_eq!(r.status, QaStatus::Pass, "failures: {:?}", r.failures);
    }

    #[test]
    fn updates_without_any_snapshot_fail() {
        let tmp = tempfile::tempdir().unwrap();
        let events = vec![
            book_update("a", T0, 1, 95, 105, None),
            book_update("a", T0 + SEC, 1, 106, 110, Some(105)),
        ];
        let path = write_wal(tmp.path(), "a.wal", &events);
        let r = qa_wal(&path, "binance", "2026-06-10").unwrap();
        assert!(r.depth["a"].missing_snapshot);
        assert_eq!(r.status, QaStatus::Fail);
    }

    #[test]
    fn trade_id_regression_buckets() {
        let tmp = tempfile::tempdir().unwrap();
        let events = vec![
            snapshot("a", T0, 1),
            trades("a", T0 + SEC, 1, &["10", "11"]),
            trades("a", T0 + 2 * SEC, 1, &["11"]), // dup, no reconnect: unexplained
            conn_down(T0 + 3 * SEC, 1),
            trades("a", T0 + 4 * SEC, 1, &["9"]), // after ConnDown: explained
            trades("a", T0 + 5 * SEC, 1, &["abc"]), // non-numeric: ignored
        ];
        let path = write_wal(tmp.path(), "a.wal", &events);
        let r = qa_wal(&path, "binance", "2026-06-10").unwrap();
        let d = &r.dups["a"];
        assert_eq!(d.trade_id_regressions_unexplained, 1);
        assert_eq!(d.trade_id_regressions_explained, 1);
        // Report-only: regressions alone do not fail the day.
        assert_eq!(r.status, QaStatus::Pass, "failures: {:?}", r.failures);
    }

    #[test]
    fn zero_events_fail() {
        let tmp = tempfile::tempdir().unwrap();
        let path = write_wal(tmp.path(), "a.wal", &[]);
        let r = qa_wal(&path, "binance", "2026-06-10").unwrap();
        assert_eq!(r.status, QaStatus::Fail);
        assert!(r.failures.iter().any(|f| f.contains("zero")));
    }

    #[test]
    fn corrupt_beyond_ratio_fails_with_frame_stats() {
        let tmp = tempfile::tempdir().unwrap();
        let path = write_wal(
            tmp.path(),
            "a.wal",
            &[
                snapshot("a", T0, 1),
                book_update("a", T0 + SEC, 1, 1, 2, None),
            ],
        );
        // Append garbage > 1% of total size.
        let mut bytes = std::fs::read(&path).unwrap();
        let garbage = vec![0xAAu8; bytes.len()];
        bytes.extend_from_slice(&garbage);
        std::fs::write(&path, bytes).unwrap();

        let r = qa_wal(&path, "binance", "2026-06-10").unwrap();
        assert!(r.frames.skipped_bytes > 0);
        assert!(r.frames.skipped_ratio > MAX_SKIPPED_RATIO);
        assert_eq!(r.status, QaStatus::Fail);
        assert!(r.failures.iter().any(|f| f.contains("corrupt")));
    }

    #[test]
    fn conversion_error_folds_into_status() {
        let tmp = tempfile::tempdir().unwrap();
        let path = write_wal(
            tmp.path(),
            "a.wal",
            &[
                snapshot("a", T0, 100),
                book_update("a", T0 + SEC, 1, 95, 105, None),
            ],
        );
        let mut r = qa_wal(&path, "binance", "2026-06-10").unwrap();
        assert_eq!(r.status, QaStatus::Pass);
        r.set_conversion_error("disk full".into());
        assert_eq!(r.status, QaStatus::Fail);
        assert!(!r.conversion.ok);
    }

    #[test]
    fn histogram_percentiles() {
        let mut h = Histogram::default();
        for ms in 1..=100i64 {
            h.record(ms * 1_000_000);
        }
        let s = h.summary().unwrap();
        assert_eq!(s.count, 100);
        // Bucket upper edge: a sample at exactly k ms lands in [k, k+1) and
        // reports k+1 — conservative by ≤ 1 ms, never under.
        assert_eq!(s.p50_us, 51_000);
        assert_eq!(s.p95_us, 96_000);
        assert_eq!(s.p99_us, 100_000);
        assert_eq!(s.min_us, 1_000);
        assert_eq!(s.max_us, 100_000);

        // Negative deltas and tails.
        let mut h = Histogram::default();
        h.record(-2 * SEC as i64); // underflow
        h.record(-500_000_000);
        h.record(20 * SEC as i64); // overflow
        let s = h.summary().unwrap();
        assert_eq!(s.min_us, -2_000_000);
        assert_eq!(s.max_us, 20_000_000);
        assert_eq!(h.percentile_us(0.01).unwrap(), -2_000_000); // exact min in tail
        assert_eq!(h.percentile_us(1.0).unwrap(), 20_000_000);
    }

    #[test]
    fn report_serializes_to_json() {
        let tmp = tempfile::tempdir().unwrap();
        let path = write_wal(
            tmp.path(),
            "a.wal",
            &[
                snapshot("btcusdt", T0, 100),
                book_update("btcusdt", T0 + SEC, 1, 95, 105, None),
            ],
        );
        let r = qa_wal(&path, "binance", "2026-06-10").unwrap();
        let json = serde_json::to_string_pretty(&r).unwrap();
        assert!(json.contains("\"schema_version\": 1"));
        assert!(json.contains("\"status\": \"pass\""));
        assert!(json.contains("\"book_update\""));
    }
}
