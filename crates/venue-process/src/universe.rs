//! Universe manager (A11/R4): periodically fetch *all* venue symbols, diff
//! against persisted state, record lifecycle transitions as
//! `ReferencePayload` events (through the normal sink → WAL → reference
//! parquet), feed the OI poller's watch channel, and report newly TRADING
//! perps for optional auto-subscription.
//!
//! State persists at `data/meta/<venue>/universe.json` so restarts diff
//! against the last observation instead of re-emitting a full-universe
//! burst; a missing file produces that baseline burst exactly once, by
//! design. Diffing compares the *normalized* `Instrument` (typed fields),
//! so venue-side JSON noise like filter reordering cannot fake a change
//! (V10).

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::watch;
use venue_adapter::EventSink;
use venue_binance::Universe;
use venue_core::{
    Event, Instrument, InstrumentClass, InstrumentId, LifecycleState, Nanos, Payload,
    ReferencePayload, SourceId, VenueId,
};

fn now_nanos() -> Nanos {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

/// What one diff produced; the caller decides about auto-subscription.
#[derive(Debug, Default)]
pub struct UniverseChanges {
    pub added: Vec<InstrumentId>,
    pub changed: usize,
    pub delisted: usize,
    /// True only for the very first observation (no prior state on disk).
    pub baseline: bool,
}

pub struct Manager {
    venue_id: VenueId,
    state_path: PathBuf,
    /// Lowercase symbol → last observed instrument. BTreeMap: deterministic
    /// serialization and diff order.
    prev: Option<BTreeMap<String, Instrument>>,
    universe_tx: watch::Sender<Universe>,
}

impl Manager {
    pub fn new(
        venue_id: VenueId,
        state_path: PathBuf,
        universe_tx: watch::Sender<Universe>,
    ) -> Self {
        Self {
            venue_id,
            state_path,
            prev: None,
            universe_tx,
        }
    }

    fn load_state(&self) -> Option<BTreeMap<String, Instrument>> {
        let raw = std::fs::read(&self.state_path).ok()?;
        match serde_json::from_slice::<Vec<Instrument>>(&raw) {
            Ok(list) => Some(
                list.into_iter()
                    .map(|i| (i.id.value.to_lowercase(), i))
                    .collect(),
            ),
            Err(e) => {
                tracing::warn!(error = %e, path = %self.state_path.display(),
                    "universe state unreadable; treating as missing (baseline burst follows)");
                None
            }
        }
    }

    fn persist_state(&self, state: &BTreeMap<String, Instrument>) {
        let list: Vec<&Instrument> = state.values().collect();
        let write = || -> std::io::Result<()> {
            if let Some(dir) = self.state_path.parent() {
                std::fs::create_dir_all(dir)?;
            }
            let part = self.state_path.with_extension("json.part");
            std::fs::write(&part, serde_json::to_vec(&list).unwrap_or_default())?;
            std::fs::rename(&part, &self.state_path)
        };
        if let Err(e) = write() {
            // Meta-tier write: warn, don't exit (N2 applies to the WAL only).
            // The cost of losing it is one duplicate Reference burst.
            tracing::warn!(error = %e, "universe state write failed");
        }
    }

    fn reference_event(&self, instrument_id: &str, payload: ReferencePayload) -> Event {
        Event {
            venue: self.venue_id.clone(),
            instrument: Some(InstrumentId {
                value: instrument_id.into(),
            }),
            venue_ts: None,
            local_ts: now_nanos(),
            source: SourceId::REST,
            provenance: None,
            payload: Payload::Reference(payload),
        }
    }

    /// Diff one full-universe observation against the previous one, emit the
    /// transitions, update the watch channel, persist. Returns what changed
    /// so the caller can auto-subscribe.
    pub async fn apply<S: EventSink>(
        &mut self,
        instruments: Vec<Instrument>,
        sink: &S,
    ) -> UniverseChanges {
        let new: BTreeMap<String, Instrument> = instruments
            .into_iter()
            .map(|i| (i.id.value.to_lowercase(), i))
            .collect();
        let prev = match self.prev.take() {
            Some(prev) => Some(prev),
            None => self.load_state(),
        };
        let baseline = prev.is_none();
        let prev = prev.unwrap_or_default();

        let mut changes = UniverseChanges {
            baseline,
            ..Default::default()
        };
        let mut events: Vec<Event> = Vec::new();
        for (key, instrument) in &new {
            match prev.get(key) {
                None => {
                    changes.added.push(instrument.id.clone());
                    events.push(self.reference_event(
                        key,
                        ReferencePayload::InstrumentAdded {
                            instrument: instrument.clone(),
                        },
                    ));
                }
                Some(old) if old != instrument => {
                    changes.changed += 1;
                    events.push(self.reference_event(
                        key,
                        ReferencePayload::InstrumentChanged {
                            instrument: instrument.clone(),
                        },
                    ));
                }
                Some(_) => {}
            }
        }
        for key in prev.keys() {
            if !new.contains_key(key) {
                changes.delisted += 1;
                events.push(self.reference_event(
                    key,
                    ReferencePayload::InstrumentDelisted {
                        instrument_id: InstrumentId {
                            value: key.as_str().into(),
                        },
                    },
                ));
            }
        }

        if !events.is_empty() {
            tracing::info!(
                added = changes.added.len(),
                changed = changes.changed,
                delisted = changes.delisted,
                baseline,
                "universe diff recorded"
            );
            if let Err(e) = sink.send_batch(events).await {
                tracing::warn!(error = ?e, "reference events dropped (sink closing?)");
            }
        }

        let perps: Vec<Arc<str>> = new
            .values()
            .filter(|i| {
                matches!(i.class, InstrumentClass::Perp) && i.lifecycle == LifecycleState::Trading
            })
            .map(|i| i.id.value.clone())
            .collect();
        if self.universe_tx.borrow().as_ref() != &perps {
            tracing::info!(perps = perps.len(), "perp universe updated");
            self.universe_tx.send(Arc::new(perps)).ok();
        }

        self.persist_state(&new);
        self.prev = Some(new);
        changes
    }

    /// Newly TRADING perps worth auto-subscribing (the caller filters by
    /// config policy). Baseline bursts never auto-subscribe — the configured
    /// subscriptions already cover the steady state.
    pub fn auto_subscribe_candidates(&self, changes: &UniverseChanges) -> Vec<InstrumentId> {
        if changes.baseline {
            return Vec::new();
        }
        let Some(state) = &self.prev else {
            return Vec::new();
        };
        changes
            .added
            .iter()
            .filter(|id| {
                state.get(&id.value.to_lowercase()).is_some_and(|i| {
                    matches!(i.class, InstrumentClass::Perp)
                        && i.lifecycle == LifecycleState::Trading
                })
            })
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;
    use tokio::sync::mpsc;

    fn instrument(symbol: &str, lifecycle: LifecycleState, tick: &str) -> Instrument {
        Instrument {
            id: InstrumentId {
                value: symbol.into(),
            },
            class: InstrumentClass::Perp,
            base: venue_core::Asset(symbol.trim_end_matches("usdt").to_uppercase().into()),
            quote: venue_core::Asset("USDT".into()),
            tick_size: tick.parse::<Decimal>().ok(),
            lot_size: None,
            min_notional: None,
            contract_multiplier: None,
            settle_ccy: Some(venue_core::Asset("USDT".into())),
            linearity: Some(venue_core::Linearity::Linear),
            funding_interval: Some(8 * 3600 * 1_000_000_000),
            lifecycle,
        }
    }

    struct Harness {
        manager: Manager,
        rx: mpsc::Receiver<Event>,
        sink: mpsc::Sender<Event>,
        _universe_rx: watch::Receiver<Universe>,
        universe_tx_probe: watch::Receiver<Universe>,
    }

    fn harness(tmp: &std::path::Path) -> Harness {
        let (tx, rx) = mpsc::channel(1024);
        let (utx, urx) = watch::channel::<Universe>(Arc::new(Vec::new()));
        let probe = urx.clone();
        Harness {
            manager: Manager::new(
                VenueId {
                    value: "binance".into(),
                },
                tmp.join("meta/binance/universe.json"),
                utx,
            ),
            rx,
            sink: tx,
            _universe_rx: urx,
            universe_tx_probe: probe,
        }
    }

    fn drain(rx: &mut mpsc::Receiver<Event>) -> Vec<Event> {
        let mut out = Vec::new();
        while let Ok(e) = rx.try_recv() {
            out.push(e);
        }
        out
    }

    #[tokio::test]
    async fn baseline_then_add_change_delist_disappear() {
        let tmp = tempfile::tempdir().unwrap();
        let mut h = harness(tmp.path());

        // Baseline: missing state file → full burst, no auto-subscribe.
        let changes = h
            .manager
            .apply(
                vec![
                    instrument("btcusdt", LifecycleState::Trading, "0.1"),
                    instrument("ethusdt", LifecycleState::Trading, "0.01"),
                ],
                &h.sink,
            )
            .await;
        assert!(changes.baseline);
        assert_eq!(changes.added.len(), 2);
        assert_eq!(drain(&mut h.rx).len(), 2);
        assert!(h.manager.auto_subscribe_candidates(&changes).is_empty());
        assert_eq!(h.universe_tx_probe.borrow().len(), 2);

        // Add a new TRADING perp, change ETH's tick, halt BTC, drop nothing.
        let changes = h
            .manager
            .apply(
                vec![
                    instrument("btcusdt", LifecycleState::Halted, "0.1"),
                    instrument("ethusdt", LifecycleState::Trading, "0.001"),
                    instrument("newusdt", LifecycleState::Trading, "0.0001"),
                ],
                &h.sink,
            )
            .await;
        assert!(!changes.baseline);
        assert_eq!(changes.added.len(), 1);
        assert_eq!(changes.changed, 2, "lifecycle + tick changes");
        assert_eq!(changes.delisted, 0);
        let auto = h.manager.auto_subscribe_candidates(&changes);
        assert_eq!(auto.len(), 1);
        assert_eq!(auto[0].value.as_ref(), "newusdt");
        // Halted BTC leaves the OI universe.
        let perps: Vec<String> = h
            .universe_tx_probe
            .borrow()
            .iter()
            .map(|s| s.to_string())
            .collect();
        assert_eq!(perps, ["ethusdt", "newusdt"]);
        let events = drain(&mut h.rx);
        assert_eq!(events.len(), 3);
        assert!(events.iter().all(|e| e.source == SourceId::REST));

        // Disappearance from the response = delisted.
        let changes = h
            .manager
            .apply(
                vec![
                    instrument("btcusdt", LifecycleState::Halted, "0.1"),
                    instrument("ethusdt", LifecycleState::Trading, "0.001"),
                ],
                &h.sink,
            )
            .await;
        assert_eq!(changes.delisted, 1);
        let events = drain(&mut h.rx);
        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0].payload,
            Payload::Reference(ReferencePayload::InstrumentDelisted { instrument_id })
                if instrument_id.value.as_ref() == "newusdt"
        ));
    }

    #[tokio::test]
    async fn restart_with_state_file_emits_no_burst() {
        let tmp = tempfile::tempdir().unwrap();
        let universe = vec![
            instrument("btcusdt", LifecycleState::Trading, "0.1"),
            instrument("ethusdt", LifecycleState::Trading, "0.01"),
        ];
        {
            let mut h = harness(tmp.path());
            h.manager.apply(universe.clone(), &h.sink).await;
            assert_eq!(drain(&mut h.rx).len(), 2, "baseline burst");
        }
        // "Restart": fresh manager, same state path, identical universe.
        let mut h = harness(tmp.path());
        let changes = h.manager.apply(universe, &h.sink).await;
        assert!(!changes.baseline, "state file found");
        assert_eq!(changes.added.len(), 0);
        assert_eq!(changes.changed, 0);
        assert!(drain(&mut h.rx).is_empty(), "no re-emitted burst");
    }
}
