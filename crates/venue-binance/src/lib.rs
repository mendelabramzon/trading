use futures_util::stream::{SplitSink, SplitStream};
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::net::TcpStream;
use tokio_tungstenite::{tungstenite::Message, MaybeTlsStream, WebSocketStream};
use venue_adapter::*;
use venue_core::*;
mod rest;
mod ws_pool;

use rest::FundingMap;
pub use ws_pool::ExponentialBackoff;

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
    #[serde(default)]
    margin_asset: Option<String>,
    status: String,
    /// Delivery time in ms. Perpetuals carry a year-2100 sentinel here.
    #[serde(default)]
    delivery_date: Option<u64>,
    #[serde(default)]
    filters: Vec<serde_json::Value>,
}

/// Pull one decimal field out of the exchangeInfo `filters` array by
/// `filterType`. A typed enum over Binance's ~10 filter kinds would be
/// brittle; a Value scan survives venue-side additions.
fn filter_decimal(filters: &[serde_json::Value], filter_type: &str, key: &str) -> Option<Decimal> {
    filters
        .iter()
        .find(|f| f.get("filterType").and_then(|v| v.as_str()) == Some(filter_type))
        .and_then(|f| f.get(key))
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse().ok())
}

#[derive(Deserialize)]
#[serde(tag = "e")]
enum BinanceWsMessage {
    #[serde(rename = "bookTicker")]
    BookTicker(BookTickerMsg),
    #[serde(rename = "trade")]
    Trade(TradeMsg),
    #[serde(rename = "aggTrade")]
    AggTrade(AggTradeMsg),
    #[serde(rename = "depthUpdate")]
    DepthUpdate(DepthUpdateMsg),
    #[serde(rename = "markPriceUpdate")]
    MarkPriceUpdate(MarkPriceUpdateMsg),
    #[serde(rename = "forceOrder")]
    ForceOrder(ForceOrderMsg),
}

#[derive(Deserialize)]
struct BookTickerMsg {
    s: String,
    u: u64,     // order book update id
    b: Decimal, // best bid price
    #[serde(rename = "B")]
    bq: Decimal, // best bid qty
    a: Decimal, // best ask price
    #[serde(rename = "A")]
    aq: Decimal, // best ask qty
    #[serde(rename = "T")]
    time: u64, // transaction time (ms)
}

/// Per-fill trade stream (`<symbol>@trade`). Live-verified 2026-06-10: the
/// fapi `@aggTrade` stream no longer emits; `@trade` is the venue's trade
/// feed and additionally carries the fill type `X`.
#[derive(Deserialize)]
struct TradeMsg {
    s: String,
    t: u64,     // trade id
    p: Decimal, // price
    q: Decimal, // quantity
    #[serde(rename = "T")]
    time: u64, // trade time (ms)
    m: bool,    // buyer is maker?
    #[serde(rename = "X", default)]
    kind: Option<String>, // fill type: MARKET, liquidation/ADL variants
}

/// Kept for venues/endpoints that still emit aggregated trades; not currently
/// subscribed (see `TradeMsg`).
#[derive(Deserialize)]
struct AggTradeMsg {
    s: String,
    a: u64,     // aggregate trade id
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
    event_time: u64, // ms
    #[serde(rename = "T")]
    transaction_time: u64, // ms
    #[serde(rename = "U")]
    first_update_id: u64,
    #[serde(rename = "u")]
    final_update_id: u64,
    #[serde(rename = "pu")]
    prev_final_update_id: Option<u64>,
    b: Vec<(Decimal, Decimal)>, // bid levels [price, qty]
    a: Vec<(Decimal, Decimal)>, // ask levels [price, qty]
}

#[derive(Deserialize)]
struct MarkPriceUpdateMsg {
    s: String,
    #[serde(rename = "E")]
    event_time: u64, // ms — this stream has no transaction time (its T is next funding)
    p: Decimal, // mark price
    i: Decimal, // index price
    r: Decimal, // funding rate
    #[serde(rename = "T")]
    next_funding_time: u64, // ms
}

#[derive(Deserialize)]
struct ForceOrderMsg {
    o: ForceOrderDetail,
}

#[derive(Deserialize)]
struct ForceOrderDetail {
    s: String,
    #[serde(rename = "S")]
    side: String, // BUY | SELL — side of the liquidation order
    q: Decimal, // original quantity
    p: Decimal, // price
    #[serde(default)]
    ap: Option<Decimal>, // average price
    #[serde(rename = "X", default)]
    status: Option<String>,
    #[serde(default)]
    z: Option<Decimal>, // cumulative filled quantity
    #[serde(rename = "T")]
    time: u64, // trade time (ms)
}

pub struct BinanceAdapter<S: EventSink, R: RawFrameSink = ()> {
    venue_id: VenueId,
    sink: S,
    raw: R,
    pool: ws_pool::WsPool,
    next_id: u64,
    snapshot_fetcher: Option<(
        tokio_util::sync::CancellationToken,
        tokio::task::JoinHandle<()>,
    )>,
}

impl<S: EventSink> BinanceAdapter<S> {
    pub fn new(sink: S) -> Self {
        Self {
            venue_id: VenueId {
                value: "binance".into(),
            },
            sink,
            raw: (),
            pool: ws_pool::WsPool::new(MAX_STREAMS_PER_CONN),
            next_id: 0,
            snapshot_fetcher: None,
        }
    }
}

impl<S: EventSink, R: RawFrameSink> BinanceAdapter<S, R> {
    /// Attach a raw-frame tee (R2): every WS text frame is captured verbatim
    /// before parsing. Default-on for venues in bring-up.
    pub fn with_raw_tee<R2: RawFrameSink>(self, raw: R2) -> BinanceAdapter<S, R2> {
        BinanceAdapter {
            venue_id: self.venue_id,
            sink: self.sink,
            raw,
            pool: self.pool,
            next_id: self.next_id,
            snapshot_fetcher: self.snapshot_fetcher,
        }
    }

    /// `fetch_instruments` plus the raw exchangeInfo body (P5a): callers
    /// persist the raw JSON to `data/meta/` so reference data the parser
    /// drops today stays recoverable. File I/O deliberately stays out of the
    /// adapter; the venue process owns the dump location.
    pub async fn fetch_instruments_raw(&self) -> Result<(String, Vec<Instrument>), VenueError> {
        let url = format!("{}/fapi/v1/exchangeInfo", BASE_REST_URL);
        let text = reqwest::get(&url)
            .await
            .map_err(|e| VenueError::RequestFailed(e.to_string()))?
            .text()
            .await
            .map_err(|e| VenueError::RequestFailed(e.to_string()))?;
        let resp: ExchangeInfoResponse = serde_json::from_str(&text)
            .map_err(|e| VenueError::RequestFailed(format!("exchangeInfo parse: {e}")))?;

        let funding = rest::fetch_funding_info().await.unwrap_or_else(|e| {
            tracing::warn!(error = %e, "fundingInfo fetch failed; using 8h defaults");
            FundingMap::default()
        });

        let instruments = resp
            .symbols
            .into_iter()
            .filter(|s| s.status == "TRADING")
            .map(|s| symbol_to_instrument(s, &funding))
            .collect();

        Ok((text, instruments))
    }
}

impl<S: EventSink, R: RawFrameSink> VenueAdapter<S> for BinanceAdapter<S, R> {
    fn venue_id(&self) -> &VenueId {
        &self.venue_id
    }

    async fn fetch_instruments(&self) -> Result<Vec<Instrument>, VenueError> {
        self.fetch_instruments_raw()
            .await
            .map(|(_, instruments)| instruments)
    }

    async fn connect(&mut self) -> Result<(), VenueError> {
        Ok(())
    }

    async fn subscribe(&mut self, subscriptions: Vec<Subscription>) -> Result<(), VenueError> {
        // Build deduplicated streams (e.g. FundingRate/MarkPrice/IndexPrice all
        // ride one @markPrice stream).
        let mut streams = std::collections::HashSet::new();
        for sub in &subscriptions {
            match &sub.scope {
                Scope::Instruments(ids) => {
                    for id in ids {
                        let symbol = &id.value;
                        for dt in &sub.data {
                            match dt {
                                DataType::BookTicker => {
                                    streams.insert(format!("{symbol}@bookTicker"));
                                }
                                DataType::Trade => {
                                    // Live-verified: fapi @aggTrade is silent;
                                    // @trade is the working per-fill stream.
                                    streams.insert(format!("{symbol}@trade"));
                                }
                                DataType::BookDepth => {
                                    streams.insert(format!("{symbol}@depth@100ms"));
                                }
                                DataType::FundingRate
                                | DataType::MarkPrice
                                | DataType::IndexPrice => {
                                    streams.insert(format!("{symbol}@markPrice@1s"));
                                }
                                DataType::Liquidation => {
                                    streams.insert(format!("{symbol}@forceOrder"));
                                }
                                DataType::OpenInterest => {
                                    tracing::warn!(
                                        %symbol,
                                        "OpenInterest is REST-only on Binance; \
                                         captured by the Phase-2 poller — skipped"
                                    );
                                }
                            }
                        }
                    }
                }
                Scope::All => {
                    for dt in &sub.data {
                        match dt {
                            DataType::BookTicker => {
                                streams.insert("!bookTicker".to_string());
                            }
                            DataType::FundingRate | DataType::MarkPrice | DataType::IndexPrice => {
                                streams.insert("!markPrice@arr@1s".to_string());
                            }
                            DataType::Liquidation => {
                                streams.insert("!forceOrder@arr".to_string());
                            }
                            other => {
                                tracing::warn!(
                                    ?other,
                                    "no venue-wide Binance stream for this data type — skipped"
                                );
                            }
                        }
                    }
                }
                Scope::Class(class) => {
                    tracing::warn!(
                        ?class,
                        "Scope::Class expansion requires the universe manager (Phase 2) — skipped"
                    );
                }
            }
        }

        let streams: Vec<String> = streams.into_iter().collect();

        // Funding metadata (A4): per-symbol intervals/clamps stamped onto
        // FundingRatePrediction events; symbols not listed use the 8h default.
        let funding = rest::fetch_funding_info().await.unwrap_or_else(|e| {
            tracing::warn!(error = %e, "fundingInfo fetch failed; using 8h defaults");
            FundingMap::default()
        });

        // Depth snapshots (Bug 1): one fetcher task per adapter, triggered by
        // the first depthUpdate per symbol per connection session.
        let snapshot_tx = if streams.iter().any(|s| s.contains("@depth")) {
            let cancel = tokio_util::sync::CancellationToken::new();
            let (tx, handle) = rest::spawn_snapshot_fetcher(
                self.sink.clone(),
                self.venue_id.clone(),
                cancel.clone(),
            );
            self.snapshot_fetcher = Some((cancel, handle));
            Some(tx)
        } else {
            None
        };

        // delegate to pool
        self.pool
            .subscribe(
                streams,
                &self.sink,
                &self.raw,
                &self.venue_id,
                &mut self.next_id,
                funding,
                snapshot_tx,
            )
            .await
    }

    async fn disconnect(&mut self) -> Result<(), VenueError> {
        self.pool.disconnect().await?;
        if let Some((cancel, handle)) = self.snapshot_fetcher.take() {
            cancel.cancel();
            let _ = handle.await;
        }
        Ok(())
    }
}

fn symbol_to_instrument(s: SymbolInfo, funding: &FundingMap) -> Instrument {
    let class = match s.contract_type.as_str() {
        "PERPETUAL" => InstrumentClass::Perp,
        // USD-M futures are never spot: everything non-perpetual here is a
        // dated future (CURRENT_QUARTER / NEXT_QUARTER / settling states).
        _ => InstrumentClass::Future {
            expiry: s.delivery_date.map(ms_to_nanos),
        },
    };
    let funding_interval = match class {
        InstrumentClass::Perp => Some(
            funding
                .get(&s.symbol.to_lowercase())
                .map_or(rest::DEFAULT_FUNDING_INTERVAL_NS, |m| m.interval),
        ),
        _ => None,
    };
    let lifecycle = match s.status.as_str() {
        "TRADING" => LifecycleState::Trading,
        "PENDING_TRADING" => LifecycleState::PendingTrading,
        "DELIVERED" | "CLOSE" => LifecycleState::Delisted,
        _ => LifecycleState::Halted,
    };
    let linearity = s.margin_asset.as_deref().map(|m| {
        if m == s.quote_asset {
            Linearity::Linear
        } else {
            Linearity::Inverse
        }
    });

    Instrument {
        id: InstrumentId {
            value: s.symbol.to_lowercase().into(),
        },
        class,
        base: Asset(s.base_asset.into()),
        quote: Asset(s.quote_asset.clone().into()),
        tick_size: filter_decimal(&s.filters, "PRICE_FILTER", "tickSize"),
        lot_size: filter_decimal(&s.filters, "LOT_SIZE", "stepSize"),
        min_notional: filter_decimal(&s.filters, "MIN_NOTIONAL", "notional"),
        contract_multiplier: None, // USD-M linear contracts: multiplier 1
        settle_ccy: s.margin_asset.map(|m| Asset(m.into())),
        linearity,
        funding_interval,
        lifecycle,
    }
}

#[inline]
pub(crate) fn ms_to_nanos(ms: u64) -> Nanos {
    ms * 1_000_000
}

pub(crate) fn now_nanos() -> Nanos {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

/// What the parser made of one WS text frame. Non-data frames go to the
/// pool's reply watcher (SUBSCRIBE acks, venue error frames); a depth update
/// reports its symbol so the connection can trigger the initial REST snapshot.
pub(crate) struct ParseOutcome {
    pub is_data: bool,
    pub depth_symbol: Option<std::sync::Arc<str>>,
}

pub(crate) async fn handle_message<S: EventSink>(
    text: &str,
    venue_id: &VenueId,
    sink: &S,
    source: SourceId,
    funding: &FundingMap,
) -> ParseOutcome {
    // Venue-wide streams (!markPrice@arr) deliver arrays of messages.
    if text.starts_with('[') {
        match serde_json::from_str::<Vec<BinanceWsMessage>>(text) {
            Ok(msgs) => {
                let mut depth_symbol = None;
                for msg in msgs {
                    depth_symbol = handle_parsed(msg, venue_id, sink, source, funding)
                        .await
                        .or(depth_symbol);
                }
                return ParseOutcome {
                    is_data: true,
                    depth_symbol,
                };
            }
            Err(e) => {
                tracing::trace!(error = %e, "WS array frame is not data");
                return ParseOutcome {
                    is_data: false,
                    depth_symbol: None,
                };
            }
        }
    }

    match serde_json::from_str::<BinanceWsMessage>(text) {
        Ok(msg) => {
            let depth_symbol = handle_parsed(msg, venue_id, sink, source, funding).await;
            ParseOutcome {
                is_data: true,
                depth_symbol,
            }
        }
        Err(e) => {
            tracing::trace!(error = %e, "WS frame is not data");
            ParseOutcome {
                is_data: false,
                depth_symbol: None,
            }
        }
    }
}

async fn handle_parsed<S: EventSink>(
    msg: BinanceWsMessage,
    venue_id: &VenueId,
    sink: &S,
    source: SourceId,
    funding: &FundingMap,
) -> Option<std::sync::Arc<str>> {
    let now = now_nanos();

    let make_event = |symbol: &str, venue_ts_ms: u64, payload: Payload| Event {
        venue: venue_id.clone(),
        instrument: Some(InstrumentId {
            value: symbol.to_lowercase().into(),
        }),
        venue_ts: Some(ms_to_nanos(venue_ts_ms)),
        local_ts: now,
        source,
        provenance: None,
        payload,
    };

    match msg {
        BinanceWsMessage::BookTicker(msg) => {
            let event = make_event(
                &msg.s,
                msg.time,
                Payload::Market(MarketPayload::BookTicker {
                    best_bid: Level {
                        price: msg.b,
                        qty: msg.bq,
                    },
                    best_ask: Level {
                        price: msg.a,
                        qty: msg.aq,
                    },
                    update_id: msg.u,
                }),
            );
            if let Err(e) = sink.send(event).await {
                tracing::warn!(error = ?e, symbol = %msg.s, "sink.send failed, event dropped");
            }
            None
        }

        BinanceWsMessage::Trade(msg) => {
            let side = if msg.m {
                AggressorSide::Sell
            } else {
                AggressorSide::Buy
            };
            let event = make_event(
                &msg.s,
                msg.time,
                Payload::Market(MarketPayload::Trades {
                    trades: vec![Trade {
                        id: msg.t.to_string().into(),
                        price: msg.p,
                        qty: msg.q,
                        aggressor_side: side,
                        kind: msg.kind.map(Into::into),
                    }],
                }),
            );
            if let Err(e) = sink.send(event).await {
                tracing::warn!(error = ?e, symbol = %msg.s, "sink.send failed, event dropped");
            }
            None
        }

        BinanceWsMessage::AggTrade(msg) => {
            let side = if msg.m {
                AggressorSide::Sell
            } else {
                AggressorSide::Buy
            };
            let event = make_event(
                &msg.s,
                msg.time,
                Payload::Market(MarketPayload::Trades {
                    trades: vec![Trade {
                        id: msg.a.to_string().into(),
                        price: msg.p,
                        qty: msg.q,
                        aggressor_side: side,
                        kind: None, // aggregated fills carry no type
                    }],
                }),
            );
            if let Err(e) = sink.send(event).await {
                tracing::warn!(error = ?e, symbol = %msg.s, "sink.send failed, event dropped");
            }
            None
        }

        BinanceWsMessage::DepthUpdate(msg) => {
            let symbol: std::sync::Arc<str> = msg.s.to_lowercase().into();
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

            // D7 contract: venue_ts = transaction time T; event time E kept in
            // the payload for E−T latency QA.
            let event = Event {
                venue: venue_id.clone(),
                instrument: Some(InstrumentId {
                    value: symbol.clone(),
                }),
                venue_ts: Some(ms_to_nanos(msg.transaction_time)),
                local_ts: now,
                source,
                provenance: None,
                payload: Payload::Market(MarketPayload::BookUpdate {
                    bids,
                    asks,
                    first_update_id: msg.first_update_id,
                    final_update_id: msg.final_update_id,
                    prev_final_update_id: msg.prev_final_update_id,
                    event_time: Some(ms_to_nanos(msg.event_time)),
                }),
            };
            if let Err(e) = sink.send(event).await {
                tracing::warn!(error = ?e, symbol = %symbol, "sink.send failed, event dropped");
            }
            Some(symbol)
        }

        BinanceWsMessage::MarkPriceUpdate(msg) => {
            // This stream has no transaction time; venue_ts = event time E is
            // the documented exception to the D7 contract.
            let meta = funding.get(&msg.s.to_lowercase());
            let events = vec![
                make_event(
                    &msg.s,
                    msg.event_time,
                    Payload::Market(MarketPayload::MarkPrice { price: msg.p }),
                ),
                make_event(
                    &msg.s,
                    msg.event_time,
                    Payload::Market(MarketPayload::IndexPrice { price: msg.i }),
                ),
                make_event(
                    &msg.s,
                    msg.event_time,
                    Payload::Market(MarketPayload::FundingRatePrediction {
                        rate: msg.r,
                        next_funding_time: ms_to_nanos(msg.next_funding_time),
                        // fundingInfo lists only non-default symbols; absent
                        // means the venue-wide 8h default (A4).
                        interval: Some(
                            meta.map_or(rest::DEFAULT_FUNDING_INTERVAL_NS, |m| m.interval),
                        ),
                        premium_index: None, // separate stream; not captured yet
                        clamp_min: meta.and_then(|m| m.floor),
                        clamp_max: meta.and_then(|m| m.cap),
                    }),
                ),
            ];
            if let Err(e) = sink.send_batch(events).await {
                tracing::warn!(error = ?e, symbol = %msg.s, "sink.send_batch failed, events dropped");
            }
            None
        }

        BinanceWsMessage::ForceOrder(msg) => {
            let o = msg.o;
            let side = if o.side == "SELL" {
                AggressorSide::Sell
            } else {
                AggressorSide::Buy
            };
            let event = make_event(
                &o.s,
                o.time,
                Payload::Market(MarketPayload::Liquidation {
                    side,
                    price: o.p,
                    qty: o.q,
                    filled_qty: o.z,
                    avg_price: o.ap,
                    order_status: o.status.map(Into::into),
                }),
            );
            if let Err(e) = sink.send(event).await {
                tracing::warn!(error = ?e, symbol = %o.s, "sink.send failed, event dropped");
            }
            None
        }
    }
}

/// Parser fixture tests (P4): literal Binance JSON in, full `Event` out.
/// Every field the venue sends and we keep is asserted here — this is the
/// guard against the D1/D7/N1/A4 failure class (parser silently dropping
/// unrecoverable data).
#[cfg(test)]
mod parse_fixtures {
    use super::*;
    use rust_decimal_macros::dec;
    use tokio::sync::mpsc;

    const SRC: SourceId = SourceId(3);

    async fn parse_with(text: &str, funding: &FundingMap) -> (Vec<Event>, ParseOutcome) {
        let venue_id = VenueId {
            value: "binance".into(),
        };
        let (tx, mut rx) = mpsc::channel::<Event>(64);
        let outcome = handle_message(text, &venue_id, &tx, SRC, funding).await;
        drop(tx);
        let mut out = Vec::new();
        while let Ok(e) = rx.try_recv() {
            out.push(e);
        }
        (out, outcome)
    }

    async fn parse(text: &str) -> Vec<Event> {
        parse_with(text, &FundingMap::default()).await.0
    }

    #[tokio::test]
    async fn parses_book_ticker() {
        let events = parse(
            r#"{"e":"bookTicker","u":400900217,"E":1568014460893,"T":1568014460891,"s":"BNBUSDT","b":"25.35190000","B":"31.21000000","a":"25.36520000","A":"40.66000000"}"#,
        )
        .await;
        assert_eq!(events.len(), 1);
        let e = &events[0];
        assert_eq!(e.venue.value.as_ref(), "binance");
        assert_eq!(e.instrument.as_ref().unwrap().value.as_ref(), "bnbusdt");
        // venue_ts = transaction time T, not event time E (D7).
        assert_eq!(e.venue_ts, Some(1_568_014_460_891_000_000));
        assert!(e.local_ts > 0);
        assert_eq!(e.source, SRC);
        assert_eq!(e.provenance, None);
        match &e.payload {
            Payload::Market(MarketPayload::BookTicker {
                best_bid,
                best_ask,
                update_id,
            }) => {
                assert_eq!(*update_id, 400900217);
                assert_eq!(best_bid.price, dec!(25.35190000));
                assert_eq!(best_bid.qty, dec!(31.21000000));
                assert_eq!(best_ask.price, dec!(25.36520000));
                assert_eq!(best_ask.qty, dec!(40.66000000));
            }
            other => panic!("unexpected payload: {other:?}"),
        }
    }

    #[tokio::test]
    async fn parses_trade() {
        // Frame shape captured live 2026-06-10 from <symbol>@trade.
        let events = parse(
            r#"{"e":"trade","E":1781093004918,"T":1781093004918,"s":"BTCUSDT","t":7773119483,"p":"61038.40","q":"0.003","X":"MARKET","m":false}"#,
        )
        .await;
        assert_eq!(events.len(), 1);
        let e = &events[0];
        assert_eq!(e.instrument.as_ref().unwrap().value.as_ref(), "btcusdt");
        assert_eq!(e.venue_ts, Some(1_781_093_004_918_000_000));
        match &e.payload {
            Payload::Market(MarketPayload::Trades { trades }) => {
                assert_eq!(trades.len(), 1);
                assert_eq!(trades[0].id.as_ref(), "7773119483");
                assert_eq!(trades[0].price, dec!(61038.40));
                assert_eq!(trades[0].qty, dec!(0.003));
                // m = false: buyer is taker, so the aggressor bought.
                assert_eq!(trades[0].aggressor_side, AggressorSide::Buy);
                assert_eq!(trades[0].kind.as_deref(), Some("MARKET"));
            }
            other => panic!("unexpected payload: {other:?}"),
        }
    }

    #[tokio::test]
    async fn parses_agg_trade_fallback() {
        let events = parse(
            r#"{"e":"aggTrade","E":123456789,"s":"BTCUSDT","a":5933014,"p":"0.001","q":"100","f":100,"l":105,"T":123456785,"m":true}"#,
        )
        .await;
        assert_eq!(events.len(), 1);
        let e = &events[0];
        assert_eq!(e.instrument.as_ref().unwrap().value.as_ref(), "btcusdt");
        assert_eq!(e.venue_ts, Some(123_456_785_000_000));
        match &e.payload {
            Payload::Market(MarketPayload::Trades { trades }) => {
                assert_eq!(trades.len(), 1);
                assert_eq!(trades[0].id.as_ref(), "5933014");
                assert_eq!(trades[0].price, dec!(0.001));
                assert_eq!(trades[0].qty, dec!(100));
                // m = true: buyer is maker, so the aggressor sold.
                assert_eq!(trades[0].aggressor_side, AggressorSide::Sell);
                assert_eq!(trades[0].kind, None);
            }
            other => panic!("unexpected payload: {other:?}"),
        }
    }

    #[tokio::test]
    async fn parses_depth_update_with_chain_ids() {
        let events = parse(
            r#"{"e":"depthUpdate","E":123456789,"T":123456788,"s":"BTCUSDT","U":157,"u":160,"pu":149,"b":[["0.0024","10"]],"a":[["0.0026","100"]]}"#,
        )
        .await;
        assert_eq!(events.len(), 1);
        let e = &events[0];
        // venue_ts = T (transaction), E preserved in the payload (D7).
        assert_eq!(e.venue_ts, Some(123_456_788_000_000));
        match &e.payload {
            Payload::Market(MarketPayload::BookUpdate {
                bids,
                asks,
                first_update_id,
                final_update_id,
                prev_final_update_id,
                event_time,
            }) => {
                assert_eq!(*first_update_id, 157);
                assert_eq!(*final_update_id, 160);
                assert_eq!(*prev_final_update_id, Some(149));
                assert_eq!(*event_time, Some(123_456_789_000_000));
                assert_eq!(bids.len(), 1);
                assert_eq!(bids[0].price, dec!(0.0024));
                assert_eq!(asks[0].qty, dec!(100));
            }
            other => panic!("unexpected payload: {other:?}"),
        }
    }

    #[tokio::test]
    async fn mark_price_fans_out_three_events_in_order() {
        let events = parse(
            r#"{"e":"markPriceUpdate","E":1562305380000,"s":"BTCUSDT","p":"11794.15000000","i":"11784.62659091","P":"11784.25641265","r":"0.00038167","T":1562306400000}"#,
        )
        .await;
        assert_eq!(events.len(), 3);
        // This stream has no transaction time; venue_ts = E (documented D7 exception).
        for e in &events {
            assert_eq!(e.venue_ts, Some(1_562_305_380_000_000_000));
            assert_eq!(e.instrument.as_ref().unwrap().value.as_ref(), "btcusdt");
        }
        assert!(matches!(
            &events[0].payload,
            Payload::Market(MarketPayload::MarkPrice { price }) if *price == dec!(11794.15)
        ));
        assert!(matches!(
            &events[1].payload,
            Payload::Market(MarketPayload::IndexPrice { price }) if *price == dec!(11784.62659091)
        ));
        match &events[2].payload {
            Payload::Market(MarketPayload::FundingRatePrediction {
                rate,
                next_funding_time,
                interval,
                clamp_min,
                clamp_max,
                ..
            }) => {
                assert_eq!(*rate, dec!(0.00038167));
                assert_eq!(*next_funding_time, 1_562_306_400_000_000_000);
                // Not in the funding map → venue default 8h, no known clamps.
                assert_eq!(*interval, Some(8 * 3600 * 1_000_000_000));
                assert_eq!(*clamp_min, None);
                assert_eq!(*clamp_max, None);
            }
            other => panic!("unexpected payload: {other:?}"),
        }
    }

    #[tokio::test]
    async fn funding_map_overrides_interval_and_clamps() {
        let mut map = std::collections::HashMap::new();
        map.insert(
            "btcusdt".to_string(),
            rest::FundingMeta {
                interval: 4 * 3600 * 1_000_000_000,
                cap: Some(dec!(0.02)),
                floor: Some(dec!(-0.02)),
            },
        );
        let funding: FundingMap = std::sync::Arc::new(map);
        let (events, _) = parse_with(
            r#"{"e":"markPriceUpdate","E":1562305380000,"s":"BTCUSDT","p":"1","i":"1","P":"1","r":"0.0001","T":1562306400000}"#,
            &funding,
        )
        .await;
        match &events[2].payload {
            Payload::Market(MarketPayload::FundingRatePrediction {
                interval,
                clamp_min,
                clamp_max,
                ..
            }) => {
                assert_eq!(*interval, Some(4 * 3600 * 1_000_000_000));
                assert_eq!(*clamp_min, Some(dec!(-0.02)));
                assert_eq!(*clamp_max, Some(dec!(0.02)));
            }
            other => panic!("unexpected payload: {other:?}"),
        }
    }

    #[tokio::test]
    async fn depth_outcome_reports_symbol_for_snapshot_trigger() {
        let (_, outcome) = parse_with(
            r#"{"e":"depthUpdate","E":2,"T":1,"s":"BTCUSDT","U":157,"u":160,"pu":149,"b":[],"a":[]}"#,
            &FundingMap::default(),
        )
        .await;
        assert!(outcome.is_data);
        assert_eq!(outcome.depth_symbol.as_deref(), Some("btcusdt"));

        let (_, outcome) = parse_with(
            r#"{"e":"aggTrade","E":2,"s":"BTCUSDT","a":1,"p":"1","q":"1","T":1,"m":false}"#,
            &FundingMap::default(),
        )
        .await;
        assert!(outcome.is_data);
        assert_eq!(outcome.depth_symbol, None);

        let (_, outcome) = parse_with(r#"{"result":null,"id":1}"#, &FundingMap::default()).await;
        assert!(!outcome.is_data);
    }

    #[tokio::test]
    async fn parses_force_order_liquidation() {
        let events = parse(
            r#"{"e":"forceOrder","E":1568014460893,"o":{"s":"BTCUSDT","S":"SELL","o":"LIMIT","f":"IOC","q":"0.014","p":"9910","ap":"9910","X":"FILLED","l":"0.014","z":"0.014","T":1568014460891}}"#,
        )
        .await;
        assert_eq!(events.len(), 1);
        let e = &events[0];
        assert_eq!(e.venue_ts, Some(1_568_014_460_891_000_000));
        match &e.payload {
            Payload::Market(MarketPayload::Liquidation {
                side,
                price,
                qty,
                filled_qty,
                avg_price,
                order_status,
            }) => {
                assert_eq!(*side, AggressorSide::Sell);
                assert_eq!(*price, dec!(9910));
                assert_eq!(*qty, dec!(0.014));
                assert_eq!(*filled_qty, Some(dec!(0.014)));
                assert_eq!(*avg_price, Some(dec!(9910)));
                assert_eq!(order_status.as_deref(), Some("FILLED"));
            }
            other => panic!("unexpected payload: {other:?}"),
        }
    }

    #[tokio::test]
    async fn parses_venue_wide_array_messages() {
        let events = parse(
            r#"[{"e":"markPriceUpdate","E":1562305380000,"s":"BTCUSDT","p":"11794.15","i":"11784.62","P":"11784.25","r":"0.0001","T":1562306400000},{"e":"markPriceUpdate","E":1562305380000,"s":"ETHUSDT","p":"294.1","i":"294.0","P":"294.0","r":"0.0002","T":1562306400000}]"#,
        )
        .await;
        assert_eq!(
            events.len(),
            6,
            "two markPrice messages fan out to 3 events each"
        );
        assert_eq!(
            events[0].instrument.as_ref().unwrap().value.as_ref(),
            "btcusdt"
        );
        assert_eq!(
            events[3].instrument.as_ref().unwrap().value.as_ref(),
            "ethusdt"
        );
    }

    #[tokio::test]
    async fn ignores_acks_and_error_frames_without_panicking() {
        // SUBSCRIBE ack and venue error frames are not data; the pool-level
        // reply watcher handles them. The parser must just skip them.
        assert!(parse(r#"{"result":null,"id":1}"#).await.is_empty());
        assert!(
            parse(r#"{"error":{"code":2,"msg":"Invalid request"},"id":1}"#)
                .await
                .is_empty()
        );
        assert!(parse("not json at all").await.is_empty());
    }
}
