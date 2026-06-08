use crate::types::{Level, Nanos, Trade};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]

pub enum Payload {
    MarketData(MarketDataPayload),
    Error(ErrorPayload),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MarketDataPayload {
    BookTicker {
        best_bid: Level,
        best_ask: Level,
    },
    BookSnapshot {
        bids: Vec<Level>,
        asks: Vec<Level>,
    },
    BookUpdate {
        bids: Vec<Level>,
        asks: Vec<Level>,
    },
    FundingRatePrediction {
        rate: Decimal,
        next_funding_time: Nanos,
    },
    FundingRateRealized {
        rate: Decimal,
        funding_time: Nanos,
    },
    MarkPrice {
        price: Decimal,
    },
    IndexPrice {
        price: Decimal,
    },
    Trades {
        trades: Vec<Trade>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorPayload {
    pub code: String,
    pub message: String,
}
