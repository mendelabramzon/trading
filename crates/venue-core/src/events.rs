use serde::{Deserialize, Serialize};

use crate::{InstrumentId, Nanos, Payload, Provenance, SourceId, VenueId};

/// Envelope v2 (wire v1). Field order is frozen — rmp-serde encodes structs
/// positionally; see the wire-freeze note in `types.rs`.
///
/// Timestamp contract (D7): `venue_ts` is the venue *transaction* time where
/// the venue provides one (trades, book ticker, depth); event time is the
/// documented fallback for streams without one (Binance markPriceUpdate).
/// `local_ts` is capture-host receive time and is always present — it is the
/// replay merge clock for cross-venue runs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub venue: VenueId,
    /// `None` only for venue-scoped events (e.g. `Control::ConnUp`).
    pub instrument: Option<InstrumentId>,
    pub venue_ts: Option<Nanos>,
    pub local_ts: Nanos,
    /// Which connection/poller produced this observation (R9).
    pub source: SourceId,
    /// On-chain context; `None` for every CEX event (R3).
    pub provenance: Option<Provenance>,
    pub payload: Payload,
}
