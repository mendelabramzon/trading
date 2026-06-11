//! Canonical symbology (A3): the versioned mapping
//! `(venue, venue-raw instrument) ↔ CanonicalInstrumentId`, built from venue
//! instrument dumps plus curated overrides, written as a queryable dataset
//! (`data/meta/symbology/mapping.parquet`), and loadable point-in-time via
//! [`Registry`]. Events stay keyed venue-raw; joins are one lookup.
//!
//! Cross-venue *event* identity (prediction markets) is research-layer by
//! decision and stays out of here. Multiplier-prefix bases (`1000PEPE`) are
//! kept verbatim — they match across the current venues.

use std::collections::HashMap;
use std::path::Path;
use venue_core::{Asset, CanonicalInstrumentId, InstrumentClass, InstrumentId, Nanos, VenueId};

pub mod build;
pub mod fees;
pub mod scd;

#[derive(Debug)]
pub enum SymbologyError {
    Io(std::io::Error),
    Parse(String),
    Invalid(String),
}

impl std::fmt::Display for SymbologyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SymbologyError::Io(e) => write!(f, "io: {e}"),
            SymbologyError::Parse(e) => write!(f, "parse: {e}"),
            SymbologyError::Invalid(e) => write!(f, "invalid: {e}"),
        }
    }
}

impl std::error::Error for SymbologyError {}

impl From<std::io::Error> for SymbologyError {
    fn from(e: std::io::Error) -> Self {
        SymbologyError::Io(e)
    }
}

/// Stable string form of a canonical id, the registry's join key
/// (`BASE-QUOTE-CLASS-SETTLE`, e.g. `BTC-USDT-perp-USDT`).
pub fn canonical_key(c: &CanonicalInstrumentId) -> String {
    format!(
        "{}-{}-{}-{}",
        c.base.0,
        c.quote.0,
        class_str(&c.class),
        c.settle.0
    )
}

pub fn class_str(class: &InstrumentClass) -> &'static str {
    match class {
        InstrumentClass::Spot => "spot",
        InstrumentClass::Perp => "perp",
        InstrumentClass::Future { .. } => "future",
        InstrumentClass::PredictionOutcome => "prediction_outcome",
        InstrumentClass::Pool => "pool",
    }
}

pub fn class_from_str(s: &str) -> Result<InstrumentClass, SymbologyError> {
    match s {
        "spot" => Ok(InstrumentClass::Spot),
        "perp" => Ok(InstrumentClass::Perp),
        // Dated futures carry expiries the mapping does not encode yet;
        // they are excluded at build time (the funding universe is perps).
        other => Err(SymbologyError::Parse(format!(
            "unsupported class {other:?} in mapping"
        ))),
    }
}

/// One validity window of one venue instrument's canonical assignment.
#[derive(Debug, Clone)]
pub struct MappingRow {
    pub venue: String,
    /// Venue-raw key, lowercase (the capture convention).
    pub instrument: String,
    pub canonical: CanonicalInstrumentId,
    pub valid_from: Nanos,
    /// `None` = still valid.
    pub valid_to: Option<Nanos>,
    /// "derived" | "override".
    pub origin: String,
}

impl MappingRow {
    fn covers(&self, at: Nanos) -> bool {
        at >= self.valid_from && self.valid_to.is_none_or(|to| at < to)
    }
}

/// Point-in-time lookup over the built mapping, both directions.
pub struct Registry {
    by_venue_instrument: HashMap<(String, String), Vec<MappingRow>>,
    by_canonical_venue: HashMap<(String, String), Vec<MappingRow>>,
}

impl Registry {
    pub fn from_rows(rows: Vec<MappingRow>) -> Self {
        let mut by_venue_instrument: HashMap<(String, String), Vec<MappingRow>> = HashMap::new();
        let mut by_canonical_venue: HashMap<(String, String), Vec<MappingRow>> = HashMap::new();
        for row in rows {
            by_venue_instrument
                .entry((row.venue.clone(), row.instrument.clone()))
                .or_default()
                .push(row.clone());
            by_canonical_venue
                .entry((canonical_key(&row.canonical), row.venue.clone()))
                .or_default()
                .push(row);
        }
        Self {
            by_venue_instrument,
            by_canonical_venue,
        }
    }

    pub fn load(path: &Path) -> Result<Self, SymbologyError> {
        Ok(Self::from_rows(build::read_mapping(path)?))
    }

    pub fn len(&self) -> usize {
        self.by_venue_instrument.values().map(Vec::len).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.by_venue_instrument.is_empty()
    }

    /// The canonical id of a venue-raw instrument at time `at`.
    pub fn canonical(
        &self,
        venue: &VenueId,
        instrument: &InstrumentId,
        at: Nanos,
    ) -> Option<&CanonicalInstrumentId> {
        self.by_venue_instrument
            .get(&(venue.value.to_string(), instrument.value.to_lowercase()))?
            .iter()
            .find(|r| r.covers(at))
            .map(|r| &r.canonical)
    }

    /// The venue-raw instrument carrying `canonical` on `venue` at `at`.
    pub fn venue_instrument(
        &self,
        canonical: &CanonicalInstrumentId,
        venue: &VenueId,
        at: Nanos,
    ) -> Option<InstrumentId> {
        self.by_canonical_venue
            .get(&(canonical_key(canonical), venue.value.to_string()))?
            .iter()
            .find(|r| r.covers(at))
            .map(|r| InstrumentId {
                value: r.instrument.clone().into(),
            })
    }

    /// Canonical keys listed on every one of the given venues at `at` —
    /// the cross-venue join universe.
    pub fn matched_keys(&self, venues: &[&str], at: Nanos) -> Vec<String> {
        let mut per_key: HashMap<&str, usize> = HashMap::new();
        for ((key, venue), rows) in &self.by_canonical_venue {
            if venues.contains(&venue.as_str()) && rows.iter().any(|r| r.covers(at)) {
                *per_key.entry(key.as_str()).or_default() += 1;
            }
        }
        let mut keys: Vec<String> = per_key
            .into_iter()
            .filter(|(_, n)| *n == venues.len())
            .map(|(k, _)| k.to_string())
            .collect();
        keys.sort();
        keys
    }
}

pub(crate) fn make_canonical(base: &str, quote: &str, settle: &str) -> CanonicalInstrumentId {
    CanonicalInstrumentId {
        base: Asset(base.into()),
        quote: Asset(quote.into()),
        class: InstrumentClass::Perp,
        settle: Asset(settle.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(venue: &str, inst: &str, base: &str, from: Nanos, to: Option<Nanos>) -> MappingRow {
        MappingRow {
            venue: venue.into(),
            instrument: inst.into(),
            canonical: make_canonical(base, "USDT", "USDT"),
            valid_from: from,
            valid_to: to,
            origin: "derived".into(),
        }
    }

    #[test]
    fn point_in_time_lookup_honors_validity_windows() {
        // btcusdt renames its canonical base at t=100 (synthetic but the
        // exact shape an instrument re-cut produces).
        let registry = Registry::from_rows(vec![
            row("binance", "oldusdt", "OLD", 0, Some(100)),
            row("binance", "oldusdt", "NEW", 100, None),
            row("bybit", "newusdt", "NEW", 50, None),
        ]);
        let venue = VenueId {
            value: "binance".into(),
        };
        let inst = InstrumentId {
            value: "oldusdt".into(),
        };
        assert_eq!(
            registry
                .canonical(&venue, &inst, 50)
                .unwrap()
                .base
                .0
                .as_ref(),
            "OLD"
        );
        assert_eq!(
            registry
                .canonical(&venue, &inst, 150)
                .unwrap()
                .base
                .0
                .as_ref(),
            "NEW"
        );
        assert!(
            registry.canonical(&venue, &inst, u64::MAX).is_some(),
            "open window"
        );

        // Reverse: who carries NEW on bybit at t=200?
        let c = make_canonical("NEW", "USDT", "USDT");
        let bybit = VenueId {
            value: "bybit".into(),
        };
        assert_eq!(
            registry
                .venue_instrument(&c, &bybit, 200)
                .unwrap()
                .value
                .as_ref(),
            "newusdt"
        );
        assert!(
            registry.venue_instrument(&c, &bybit, 10).is_none(),
            "before listing"
        );

        // Cross-venue match at t=200: NEW exists on both.
        assert_eq!(
            registry.matched_keys(&["binance", "bybit"], 200),
            ["NEW-USDT-perp-USDT"]
        );
        // At t=60 binance still maps oldusdt→OLD, bybit has NEW: no match.
        assert!(registry.matched_keys(&["binance", "bybit"], 60).is_empty());
    }

    #[test]
    fn canonical_key_is_stable() {
        assert_eq!(
            canonical_key(&make_canonical("1000PEPE", "USDT", "USDT")),
            "1000PEPE-USDT-perp-USDT"
        );
    }
}
