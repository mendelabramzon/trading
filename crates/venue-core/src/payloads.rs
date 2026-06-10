use crate::types::{AggressorSide, Instrument, InstrumentId, Level, Nanos, Trade};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

// WIRE FREEZE (wire v1): positional struct fields — never reorder/insert;
// variant names load-bearing — never rename; new variants are the only
// additive change. See `wire::WIRE_VERSION`.

/// Domain-namespaced payload (R1). Consumers filter coarsely on the domain;
/// each domain evolves (additively) without touching the others.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Payload {
    Market(MarketPayload),
    Reference(ReferencePayload),
    Chain(ChainPayload),
    Account(AccountPayload),
    Control(ControlPayload),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum MarketPayload {
    BookTicker {
        best_bid: Level,
        best_ask: Level,
        /// Venue book-sequence id of this top-of-book change (Binance `u`).
        update_id: u64,
    },
    BookSnapshot {
        bids: Vec<Level>,
        asks: Vec<Level>,
        /// Book version this snapshot represents; updates splice against it
        /// via `first_update_id <= last_update_id + 1 <= final_update_id`.
        last_update_id: u64,
    },
    BookUpdate {
        bids: Vec<Level>,
        asks: Vec<Level>,
        /// Binance `U`.
        first_update_id: u64,
        /// Binance `u`.
        final_update_id: u64,
        /// Binance `pu`: `final_update_id` of the previous update on this
        /// stream. A break in the chain (outside reconnects) means lost data.
        prev_final_update_id: Option<u64>,
        /// Venue event time `E` (`venue_ts` carries transaction time `T`);
        /// kept for E−T latency QA.
        event_time: Option<Nanos>,
    },
    Trades {
        trades: Vec<Trade>,
    },
    MarkPrice {
        price: Decimal,
    },
    IndexPrice {
        price: Decimal,
    },
    FundingRatePrediction {
        rate: Decimal,
        next_funding_time: Nanos,
        /// Funding interval (nanos): the same rate means an 8x different
        /// annualized cost at 1h vs 8h. `None` only when venue metadata was
        /// unavailable at capture time.
        interval: Option<Nanos>,
        premium_index: Option<Decimal>,
        clamp_min: Option<Decimal>,
        clamp_max: Option<Decimal>,
    },
    FundingRateRealized {
        rate: Decimal,
        funding_time: Nanos,
        interval: Option<Nanos>,
    },
    /// No live producer yet: Binance serves OI over REST only; captured by the
    /// Phase-2 poller.
    OpenInterest {
        open_interest: Decimal,
        open_interest_value: Option<Decimal>,
    },
    /// Forced liquidation order (Binance `forceOrder`). `side` is the side of
    /// the liquidation order itself (Sell = a long was liquidated).
    Liquidation {
        side: AggressorSide,
        price: Decimal,
        qty: Decimal,
        filled_qty: Option<Decimal>,
        avg_price: Option<Decimal>,
        /// Venue-raw order status (e.g. "FILLED").
        order_status: Option<Arc<str>>,
    },
}

/// Instrument lifecycle and resolution events (R4/A11). Producers arrive with
/// the Phase-2 universe manager; the schema is frozen now because these events
/// are unrecoverable if not captured.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ReferencePayload {
    InstrumentAdded {
        instrument: Instrument,
    },
    InstrumentChanged {
        instrument: Instrument,
    },
    InstrumentDelisted {
        instrument_id: InstrumentId,
    },
    /// Prediction-market resolution; `outcome` is the venue-raw winning
    /// outcome identifier.
    MarketResolved {
        outcome: Arc<str>,
    },
}

/// Reserved namespace for on-chain payloads (R3). Populated in Phase 5;
/// reserving the variant now keeps the `Payload` wire tag stable.
#[non_exhaustive]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ChainPayload {}

/// Reserved namespace for private account/execution payloads (A13): orders,
/// fills, positions, funding payments. Populated in Phase 6. Private events
/// never transit the shared bus.
#[non_exhaustive]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum AccountPayload {}

/// Capture/transport truth (A7): connection state, gaps, snapshot brackets,
/// subscription acks, reorgs. Recorded in the WAL and replayed like market
/// data — backtests must see the same discontinuities live consumers saw.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ControlPayload {
    ConnUp {
        label: Arc<str>,
    },
    ConnDown {
        label: Arc<str>,
        reason: Arc<str>,
    },
    /// Emitted by a lossy hop (e.g. the future bus) when events were dropped.
    Gap {
        reason: Arc<str>,
        dropped: u64,
    },
    SnapshotBegin,
    SnapshotEnd,
    SubAck {
        request_id: u64,
        ok: bool,
        detail: Option<Arc<str>>,
    },
    /// Chain reorganization observed from `from_block` (R3).
    Reorg {
        from_block: u64,
    },
}
