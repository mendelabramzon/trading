//! P5d heartbeat: once a minute, one log line that makes silent partial death
//! visible — per-kind event rates, WAL queue depth, fsync age, raw-tee drops,
//! reconnect count, per-kind staleness. Logs-only in Phase 1: the heartbeat
//! informs, the reader judges (rare kinds like liquidation are legitimately
//! stale for hours on quiet markets; funding_realized up to its interval +
//! poll cadence). Per-kind staleness is also the per-poller detector: every
//! REST poller owns distinct kinds (mark/index/funding_prediction = premium
//! index, funding_realized, open_interest), while SourceId can't tell REST
//! sources apart (all 0 by the frozen wire-v1 convention).

use recorder::{CaptureStats, EventKind, WriterStats};
use std::fmt::Write as _;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;

pub fn spawn(
    venue: String,
    capture: Arc<CaptureStats>,
    wal: Arc<WriterStats>,
    raw: Option<Arc<WriterStats>>,
    interval: Duration,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let start = Instant::now();
        let mut prev = capture.snapshot();
        let mut prev_at = Instant::now();
        let mut tick = tokio::time::interval(interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // The first tick fires immediately; consume it so every logged beat
        // covers a full interval.
        tick.tick().await;
        loop {
            tick.tick().await;
            let snap = capture.snapshot();
            let dt = prev_at.elapsed().as_secs_f64().max(0.001);

            let mut eps = String::new();
            let mut staleness = String::new();
            for kind in EventKind::ALL {
                let i = kind as usize;
                let delta = snap.by_kind[i].saturating_sub(prev.by_kind[i]);
                if delta > 0 {
                    let _ = write!(eps, "{}={:.0} ", kind.name(), delta as f64 / dt);
                }
                if snap.by_kind[i] > 0 {
                    if let Some(age) = capture.staleness(kind) {
                        let _ = write!(staleness, "{}={}s ", kind.name(), age.as_secs());
                    }
                }
            }
            if eps.is_empty() {
                eps.push_str("idle");
            }

            tracing::info!(
                venue = %venue,
                up_s = start.elapsed().as_secs(),
                total = snap.total,
                eps = eps.trim_end(),
                wal_depth = wal.queue_depth(),
                wal_written = wal.written(),
                fsync_age_ms = wal.fsync_age().map(|d| d.as_millis() as u64),
                raw_depth = raw.as_ref().map(|r| r.queue_depth()),
                raw_dropped = raw.as_ref().map(|r| r.dropped()),
                reconnects = snap.conn_down,
                staleness_s = staleness.trim_end(),
                "heartbeat"
            );

            prev = snap;
            prev_at = Instant::now();
        }
    })
}
