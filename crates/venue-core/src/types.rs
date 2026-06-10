use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

// WIRE FREEZE (wire v1): every Serialize type here is encoded positionally by
// rmp-serde — field order and arity are load-bearing. Do not reorder, insert,
// or remove fields on serialized types without bumping `wire::WIRE_VERSION`.
// Enum variant NAMES are load-bearing too; adding variants is the only
// additive change old readers tolerate.

//System Types
// ------------------------------------------------------------

// Nanos
pub type Nanos = u64;

/// Which connection/poller/watcher inside a venue process produced an
/// observation. Per-process registry convention: 0 = REST, 1+ = WS
/// connections in spawn order.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub struct SourceId(pub u16);

impl SourceId {
    pub const REST: SourceId = SourceId(0);
}

/// A raw venue frame exactly as received, captured before parsing (the raw
/// tee). Written to `.rawwal` files with the same wire framing as events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawFrame {
    pub local_ts: Nanos,
    pub source: SourceId,
    #[serde(with = "serde_bytes")]
    pub bytes: Vec<u8>,
}

/// On-chain observation context. Reserved for chain ingestion; populated by
/// chain watchers, `None` for every CEX event. For Solana, `block` carries the
/// slot.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Provenance {
    pub block: u64,
    pub tx_index: Option<u32>,
    pub log_index: Option<u32>,
    pub finality: Finality,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum Finality {
    Pending,
    Safe,
    Finalized,
}

// Market Data Types
// ------------------------------------------------------------

// Aggressor Side
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum AggressorSide {
    Buy,
    Sell,
}

// Level
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Level {
    pub price: Decimal,
    pub qty: Decimal,
}

/// One trade. `id` is the venue-raw trade id kept as a string: Binance ids are
/// numeric, Bybit's are hex, Polymarket's and on-chain ids are hashes — a u64
/// breaks on the second venue.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Trade {
    pub id: Arc<str>,
    pub price: Decimal,
    pub qty: Decimal,
    pub aggressor_side: AggressorSide,
    /// Venue-raw fill type where provided (Binance `@trade` `X`: MARKET vs
    /// liquidation/ADL fills). `None` when the venue doesn't distinguish.
    pub kind: Option<Arc<str>>,
}

// Instrument Types
// ------------------------------------------------------------

// Instrument Id (venue-raw key, e.g. the lowercased Binance symbol)
#[derive(Debug, Clone, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub struct InstrumentId {
    pub value: Arc<str>,
}

/// Canonical asset symbol ("BTC", "USDT").
#[derive(Debug, Clone, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub struct Asset(pub Arc<str>);

/// Instrument class across all target venue families.
#[derive(Debug, Clone, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub enum InstrumentClass {
    Spot,
    Perp,
    /// Dated future; `expiry` is the delivery time (epoch nanos) when known.
    Future {
        expiry: Option<Nanos>,
    },
    /// One outcome token of a prediction market.
    PredictionOutcome,
    /// An AMM pool.
    Pool,
}

/// Lifecycle state of an instrument at observation time.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub enum LifecycleState {
    PendingTrading,
    Trading,
    Halted,
    Delisted,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub enum Linearity {
    Linear,
    Inverse,
}

/// Cross-venue canonical instrument identity (A3). Events stay keyed by
/// venue-raw `InstrumentId`; the symbology layer maps `(VenueId, InstrumentId)
/// ↔ CanonicalInstrumentId` with validity intervals.
#[derive(Debug, Clone, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub struct CanonicalInstrumentId {
    pub base: Asset,
    pub quote: Asset,
    pub class: InstrumentClass,
    pub settle: Asset,
}

/// Venue instrument reference data. Embedded in `ReferencePayload` events, so
/// this struct is part of the frozen wire format.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Instrument {
    pub id: InstrumentId,
    pub class: InstrumentClass,
    pub base: Asset,
    pub quote: Asset,
    pub tick_size: Option<Decimal>,
    pub lot_size: Option<Decimal>,
    pub min_notional: Option<Decimal>,
    /// Contract value multiplier; `None` means 1 (plain linear contract).
    pub contract_multiplier: Option<Decimal>,
    pub settle_ccy: Option<Asset>,
    pub linearity: Option<Linearity>,
    /// Funding interval (nanos) for perps, when known.
    pub funding_interval: Option<Nanos>,
    pub lifecycle: LifecycleState,
}

// Venue Types
// ------------------------------------------------------------

// Venue Id
#[derive(Debug, Clone, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub struct VenueId {
    pub value: Arc<str>,
}
