//! The unit of ingestion composition (R11): a venue process hosts N
//! `IngestSource`s — WS pool connections, REST pollers, chain watchers —
//! sharing one sink and one WAL. The trait is the contract; `SourceSet` is
//! the supervision harness (spawn, cancel, bounded join).

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use venue_core::SourceId;

/// How long `SourceSet::shutdown` waits for sources to observe cancellation
/// before aborting them (the same budget WsPool gives its connections).
const JOIN_TIMEOUT: Duration = Duration::from_secs(3);

/// One run-until-cancelled event producer inside a venue process (R11).
///
/// Contract:
/// - Emit into the sink captured at construction, stamping `source_id()` on
///   every event.
/// - Emit `ControlPayload::ConnUp { label }` on first success and on
///   recovery, `ConnDown { label, reason }` on entering a failed state —
///   silence must be diagnosable from the recorded stream alone (A7).
/// - Transient errors are handled inside (retry/backoff); returning before
///   cancellation means the source is permanently done.
/// - Return promptly once `cancel` fires; `SourceSet::shutdown` aborts
///   stragglers after a bounded wait.
pub trait IngestSource: Send + 'static {
    /// Stable label for heartbeat lines and control events
    /// (e.g. `"poller-open-interest"`).
    fn label(&self) -> Arc<str>;

    /// The per-process source registry id stamped on emitted events
    /// (convention: 0 = REST, 1+ = WS connections).
    fn source_id(&self) -> SourceId;

    /// Run until `cancel` fires. The boxed future keeps the trait
    /// dyn-compatible so one process can hold heterogeneous sources.
    fn run(self: Box<Self>, cancel: CancellationToken) -> Pin<Box<dyn Future<Output = ()> + Send>>;
}

/// Owns the spawned source tasks of one venue process: one root cancel
/// token, bounded-join shutdown, per-label diagnostics.
#[derive(Default)]
pub struct SourceSet {
    cancel: CancellationToken,
    tasks: Vec<(Arc<str>, JoinHandle<()>)>,
}

impl SourceSet {
    pub fn new() -> Self {
        Self::default()
    }

    /// Spawn a source under this set's cancellation scope.
    pub fn spawn(&mut self, source: Box<dyn IngestSource>) {
        let label = source.label();
        let cancel = self.cancel.child_token();
        tracing::info!(label = %label, source = source.source_id().0, "ingest source starting");
        let handle = tokio::spawn(source.run(cancel));
        self.tasks.push((label, handle));
    }

    pub fn len(&self) -> usize {
        self.tasks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    /// Cancel every source and join against one shared deadline; stragglers
    /// are aborted — a zombie source holding a sink clone would otherwise
    /// keep the WAL channel open past process teardown. The set is reusable
    /// afterwards (the startup-retry path re-subscribes after a rollback).
    pub async fn shutdown(&mut self) {
        if self.tasks.is_empty() {
            return;
        }
        self.cancel.cancel();
        let deadline = tokio::time::Instant::now() + JOIN_TIMEOUT;
        for (label, mut handle) in self.tasks.drain(..) {
            match tokio::time::timeout_at(deadline, &mut handle).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    tracing::warn!(label = %label, error = %e, "ingest source task failed")
                }
                Err(_) => {
                    tracing::warn!(label = %label, "ingest source ignored cancel; aborting");
                    handle.abort();
                }
            }
        }
        self.cancel = CancellationToken::new();
    }
}

impl Drop for SourceSet {
    /// Dropping without `shutdown` at least signals cancel so tasks release
    /// their sink clones; it cannot join (no await in drop).
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    struct TickSource {
        ticks: Arc<AtomicU64>,
    }

    impl IngestSource for TickSource {
        fn label(&self) -> Arc<str> {
            "tick".into()
        }

        fn source_id(&self) -> SourceId {
            SourceId(0)
        }

        fn run(
            self: Box<Self>,
            cancel: CancellationToken,
        ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
            Box::pin(async move {
                loop {
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_millis(10)) => {
                            self.ticks.fetch_add(1, Ordering::Relaxed);
                        }
                        _ = cancel.cancelled() => return,
                    }
                }
            })
        }
    }

    /// Ignores its cancel token: shutdown must abort it within the join
    /// budget instead of hanging.
    struct StubbornSource;

    impl IngestSource for StubbornSource {
        fn label(&self) -> Arc<str> {
            "stubborn".into()
        }

        fn source_id(&self) -> SourceId {
            SourceId(9)
        }

        fn run(
            self: Box<Self>,
            _cancel: CancellationToken,
        ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
            Box::pin(async move {
                loop {
                    tokio::time::sleep(Duration::from_secs(3600)).await;
                }
            })
        }
    }

    #[tokio::test(start_paused = true)]
    async fn tick_source_runs_and_stops_on_cancel() {
        let ticks = Arc::new(AtomicU64::new(0));
        let mut set = SourceSet::new();
        set.spawn(Box::new(TickSource {
            ticks: ticks.clone(),
        }));
        assert_eq!(set.len(), 1);

        tokio::time::sleep(Duration::from_millis(105)).await;
        assert!(
            ticks.load(Ordering::Relaxed) >= 10,
            "source ticked while running"
        );

        set.shutdown().await;
        assert!(set.is_empty());
        let after = ticks.load(Ordering::Relaxed);
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            ticks.load(Ordering::Relaxed),
            after,
            "no ticks after shutdown"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_aborts_source_that_ignores_cancel() {
        let mut set = SourceSet::new();
        set.spawn(Box::new(StubbornSource));
        // Must return once the join deadline passes instead of hanging on
        // the never-cancelling task (paused clock auto-advances).
        set.shutdown().await;
        assert!(set.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn set_is_reusable_after_shutdown() {
        let ticks = Arc::new(AtomicU64::new(0));
        let mut set = SourceSet::new();
        set.spawn(Box::new(TickSource {
            ticks: ticks.clone(),
        }));
        set.shutdown().await;

        // Respawned sources must run under a fresh token, not the cancelled
        // one (the N8 retry path: disconnect rollback, then re-subscribe).
        let ticks2 = Arc::new(AtomicU64::new(0));
        set.spawn(Box::new(TickSource {
            ticks: ticks2.clone(),
        }));
        tokio::time::sleep(Duration::from_millis(55)).await;
        assert!(ticks2.load(Ordering::Relaxed) >= 5, "respawned source runs");
        set.shutdown().await;
    }
}
