use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

//System Types
// ------------------------------------------------------------

// Nanos
pub type Nanos = u64;

// Sequence
pub type Sequence = u64;

// Market Data Types
// ------------------------------------------------------------

// Aggressor Side
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AggressorSide {
    Buy,
    Sell,
}

// Level
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Level {
    pub price: Decimal,
    pub qty: Decimal,
}

// Trade
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trade {
    pub price: Decimal,
    pub qty: Decimal,
    pub aggressor_side: AggressorSide,
}

// Instrument Types
// ------------------------------------------------------------

// Instrument Id
#[derive(Debug, Clone, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub struct InstrumentId {
    pub value: Arc<str>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum InstrumentKind {
    Spot,
    Perp,
}

// Instrument
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Instrument {
    pub id: InstrumentId,
    pub kind: InstrumentKind,
    pub base: String,
    pub quote: String,
}

// Venue Types
// ------------------------------------------------------------

// Venue Id
#[derive(Debug, Clone, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub struct VenueId {
    pub value: Arc<str>,
}

// Venue
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Venue {
    pub id: VenueId,
    pub name: String,
    pub instruments: Vec<InstrumentId>,
}
