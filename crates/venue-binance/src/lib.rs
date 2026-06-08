use async_trait::async_trait;
use futures_util::stream::{SplitSink, SplitStream};
use rust_decimal::Decimal;
use serde::Deserialize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio_tungstenite::{tungstenite::Message, MaybeTlsStream, WebSocketStream};
use venue_adapter::*;
use venue_core::*;
mod ws_pool;

const BASE_REST_URL: &str = "https://fapi.binance.com";
pub(crate) const BASE_WS_URL: &str = "wss://fstream.binance.com/ws";
const MAX_STREAMS_PER_CONN: usize = 200;

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;
type WsWriter = SplitSink<WsStream, Message>;
type WsReader = SplitStream<WsStream>;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExchangeInfoResponse {
    symbols: Vec<SymbolInfo>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SymbolInfo {
    symbol: String,
    contract_type: String,
    base_asset: String,
    quote_asset: String,
    status: String,
}

#[derive(Deserialize)]
#[serde(tag = "e")]
enum BinanceWsMessage {
    #[serde(rename = "bookTicker")]
    BookTicker(BookTickerMsg),
    #[serde(rename = "aggTrade")]
    AggTrade(AggTradeMsg),
    #[serde(rename = "depthUpdate")]
    DepthUpdate(DepthUpdateMsg),
    #[serde(rename = "markPriceUpdate")]
    MarkPriceUpdate(MarkPriceUpdateMsg),
}

#[derive(Deserialize)]
struct BookTickerMsg {
    s: String,
    b: Decimal, // best bid price
    #[serde(rename = "B")]
    bq: Decimal, // best bid qty
    a: Decimal, // best ask price
    #[serde(rename = "A")]
    aq: Decimal, // best ask qty
    #[serde(rename = "T")]
    time: u64, // transaction time (ms)
}

#[derive(Deserialize)]
struct AggTradeMsg {
    s: String,
    p: Decimal, // price
    q: Decimal, // quantity
    #[serde(rename = "T")]
    time: u64, // trade time (ms)
    m: bool,    // buyer is maker?
}

#[derive(Deserialize)]
struct DepthUpdateMsg {
    s: String,
    #[serde(rename = "E")]
    event_time: u64,
    b: Vec<(Decimal, Decimal)>, // bid levels [price, qty]
    a: Vec<(Decimal, Decimal)>, // ask levels [price, qty]
}

#[derive(Deserialize)]
struct MarkPriceUpdateMsg {
    s: String,
    #[serde(rename = "E")]
    event_time: u64,
    p: Decimal, // mark price
    i: Decimal, // index price
    r: Decimal, // funding rate
    #[serde(rename = "T")]
    next_funding_time: u64,
}

pub struct BinanceAdapter<S: EventSink> {
    venue_id: VenueId,
    sink: S,
    pool: ws_pool::WsPool,
    next_id: u64,
    sequence: Arc<AtomicU64>,
}

impl<S: EventSink> BinanceAdapter<S> {
    pub fn new(sink: S) -> Self {
        Self {
            venue_id: VenueId {
                value: "binance".into(),
            },
            sink,
            pool: ws_pool::WsPool::new(MAX_STREAMS_PER_CONN),
            next_id: 0,
            sequence: Arc::new(AtomicU64::new(0)),
        }
    }
}

#[async_trait]
impl<S: EventSink> VenueAdapter<S> for BinanceAdapter<S> {
    fn venue_id(&self) -> &VenueId {
        &self.venue_id
    }

    async fn fetch_instruments(&self) -> Result<Vec<Instrument>, VenueError> {
        let url = format!("{}/fapi/v1/exchangeInfo", BASE_REST_URL);
        let resp: ExchangeInfoResponse = reqwest::get(&url)
            .await
            .map_err(|e| VenueError::RequestFailed(e.to_string()))?
            .json()
            .await
            .map_err(|e| VenueError::RequestFailed(e.to_string()))?;

        let instruments = resp
            .symbols
            .into_iter()
            .filter(|s| s.status == "TRADING")
            .map(|s| {
                let kind = match s.contract_type.as_str() {
                    "PERPETUAL" => InstrumentKind::Perp,
                    _ => InstrumentKind::Spot, // simplification for now
                };
                Instrument {
                    id: InstrumentId {
                        value: s.symbol.to_lowercase().into(),
                    },
                    kind,
                    base: s.base_asset,
                    quote: s.quote_asset,
                }
            })
            .collect();

        Ok(instruments)
    }

    async fn connect(&mut self) -> Result<(), VenueError> {
        Ok(())
    }

    async fn subscribe(&mut self, subscriptions: Vec<Subscription>) -> Result<(), VenueError> {
        // build deduplicated streams
        let mut streams = std::collections::HashSet::new();
        for sub in &subscriptions {
            let symbol = &sub.instrument.value;
            for dt in &sub.data_type {
                let stream = match dt {
                    DataType::BookTicker => format!("{symbol}@bookTicker"),
                    DataType::Trade => format!("{symbol}@aggTrade"),
                    DataType::BookDepth => format!("{symbol}@depth@100ms"),
                    DataType::FundingRate | DataType::MarkPrice | DataType::IndexPrice => {
                        format!("{symbol}@markPrice@1s")
                    }
                };
                streams.insert(stream);
            }
        }

        let streams: Vec<String> = streams.into_iter().collect();

        // delegate to pool
        self.pool
            .subscribe(
                streams,
                &self.sink,
                &self.venue_id,
                &mut self.next_id,
                &self.sequence,
            )
            .await
    }

    async fn disconnect(&mut self) -> Result<(), VenueError> {
        self.pool.disconnect().await
    }
}

pub(crate) async fn handle_message<S: EventSink>(
    text: &str,
    venue_id: &VenueId,
    sink: &S,
    seq: &AtomicU64,
) {
    let msg = match serde_json::from_str::<BinanceWsMessage>(text) {
        Ok(m) => m,
        Err(e) => {
            tracing::debug!(error = %e, "ignoring unparseable WS message");
            return;
        }
    };

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;

    match msg {
        BinanceWsMessage::BookTicker(msg) => {
            let instrument: Arc<str> = msg.s.to_lowercase().into();
            if let Err(e) = sink
                .send(Event {
                    venue: venue_id.clone(),
                    instrument: Some(InstrumentId {
                        value: instrument.clone(),
                    }),
                    venue_ts: Some(msg.time * 1_000_000),
                    local_ts: Some(now),
                    payload: Payload::MarketData(MarketDataPayload::BookTicker {
                        best_bid: Level {
                            price: msg.b,
                            qty: msg.bq,
                        },
                        best_ask: Level {
                            price: msg.a,
                            qty: msg.aq,
                        },
                    }),
                    sequence: Some(seq.fetch_add(1, Ordering::Relaxed)),
                })
                .await
            {
                tracing::warn!(error = ?e, %instrument, "sink.send failed, event dropped");
            }
        }

        BinanceWsMessage::AggTrade(msg) => {
            let side = if msg.m {
                AggressorSide::Sell
            } else {
                AggressorSide::Buy
            };
            let instrument: Arc<str> = msg.s.to_lowercase().into();

            if let Err(e) = sink
                .send(Event {
                    venue: venue_id.clone(),
                    instrument: Some(InstrumentId {
                        value: instrument.clone(),
                    }),
                    venue_ts: Some(msg.time * 1_000_000),
                    local_ts: Some(now),
                    payload: Payload::MarketData(MarketDataPayload::Trades {
                        trades: vec![Trade {
                            price: msg.p,
                            qty: msg.q,
                            aggressor_side: side,
                        }],
                    }),
                    sequence: Some(seq.fetch_add(1, Ordering::Relaxed)),
                })
                .await
            {
                tracing::warn!(error = ?e, %instrument, "sink.send failed, event dropped");
            }
        }

        BinanceWsMessage::DepthUpdate(msg) => {
            let instrument: Arc<str> = msg.s.to_lowercase().into();
            let bids = msg
                .b
                .into_iter()
                .map(|(p, q)| Level { price: p, qty: q })
                .collect();
            let asks = msg
                .a
                .into_iter()
                .map(|(p, q)| Level { price: p, qty: q })
                .collect();

            if let Err(e) = sink
                .send(Event {
                    venue: venue_id.clone(),
                    instrument: Some(InstrumentId {
                        value: instrument.clone(),
                    }),
                    venue_ts: Some(msg.event_time * 1_000_000),
                    local_ts: Some(now),
                    payload: Payload::MarketData(MarketDataPayload::BookUpdate { bids, asks }),
                    sequence: Some(seq.fetch_add(1, Ordering::Relaxed)),
                })
                .await
            {
                tracing::warn!(error = ?e, %instrument, "sink.send failed, event dropped");
            }
        }

        BinanceWsMessage::MarkPriceUpdate(msg) => {
            let instrument: Arc<str> = msg.s.to_lowercase().into();
            let venue_ts = Some(msg.event_time * 1_000_000);

            if let Err(e) = sink
                .send(Event {
                    venue: venue_id.clone(),
                    instrument: Some(InstrumentId {
                        value: instrument.clone(),
                    }),
                    venue_ts,
                    local_ts: Some(now),
                    payload: Payload::MarketData(MarketDataPayload::MarkPrice { price: msg.p }),
                    sequence: Some(seq.fetch_add(1, Ordering::Relaxed)),
                })
                .await
            {
                tracing::warn!(error = ?e, %instrument, "sink.send failed, event dropped");
            }

            if let Err(e) = sink
                .send(Event {
                    venue: venue_id.clone(),
                    instrument: Some(InstrumentId {
                        value: instrument.clone(),
                    }),
                    venue_ts,
                    local_ts: Some(now),
                    payload: Payload::MarketData(MarketDataPayload::IndexPrice { price: msg.i }),
                    sequence: Some(seq.fetch_add(1, Ordering::Relaxed)),
                })
                .await
            {
                tracing::warn!(error = ?e, %instrument, "sink.send failed, event dropped");
            }

            if let Err(e) = sink
                .send(Event {
                    venue: venue_id.clone(),
                    instrument: Some(InstrumentId {
                        value: instrument.clone(),
                    }),
                    venue_ts,
                    local_ts: Some(now),
                    payload: Payload::MarketData(MarketDataPayload::FundingRatePrediction {
                        rate: msg.r,
                        next_funding_time: msg.next_funding_time * 1_000_000,
                    }),
                    sequence: Some(seq.fetch_add(1, Ordering::Relaxed)),
                })
                .await
            {
                tracing::warn!(error = ?e, %instrument, "sink.send failed, event dropped");
            }
        }
    }
}
