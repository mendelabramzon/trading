use venue_core::*;
use venue_adapter::*;
use async_trait::async_trait;
use serde::Deserialize;
use rust_decimal::Decimal;
use tokio::net::TcpStream;
use tokio_tungstenite::{tungstenite::Message, MaybeTlsStream,WebSocketStream};
use futures_util::{stream::SplitSink};
mod ws_pool;

const BASE_REST_URL: &str = "https://fapi.binance.com";
const BASE_WS_URL: &str = "wss://fstream.binance.com/ws";
const MAX_STREAMS_PER_CONN: usize = 200;


type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;
type WsWriter = SplitSink<WsStream, Message>;

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
  struct WsEventType {
      e: String,
  }

  #[derive(Deserialize)]
  struct BookTickerMsg {
      s: String,
      b: Decimal,          // best bid price
      #[serde(rename = "B")]
      bq: Decimal,         // best bid qty
      a: Decimal,          // best ask price
      #[serde(rename = "A")]
      aq: Decimal,         // best ask qty
      #[serde(rename = "T")]
      time: u64,           // transaction time (ms)
  }

  #[derive(Deserialize)]
  struct AggTradeMsg {
      s: String,
      p: Decimal,          // price
      q: Decimal,          // quantity
      #[serde(rename = "T")]
      time: u64,           // trade time (ms)
      m: bool,             // buyer is maker?
  }

  #[derive(Deserialize)]
  struct DepthUpdateMsg {
      s: String,
      #[serde(rename = "E")]
      event_time: u64,
      b: Vec<(Decimal, Decimal)>,  // bid levels [price, qty]
      a: Vec<(Decimal, Decimal)>,  // ask levels [price, qty]
  }

  #[derive(Deserialize)]
  struct MarkPriceUpdateMsg {
      s: String,
      #[serde(rename = "E")]
      event_time: u64,
      p: Decimal,          // mark price
      i: Decimal,          // index price
      r: Decimal,          // funding rate
      #[serde(rename = "T")]
      next_funding_time: u64,
  }

  pub struct BinanceAdapter<S: EventSink> {
      venue_id: VenueId,
      sink: S,
      pool: ws_pool::WsPool,
      next_id: u64,
  }

  impl<S: EventSink> BinanceAdapter<S> {
      pub fn new(sink: S) -> Self {
          Self { 
            venue_id: VenueId { value: "binance".to_string() }, 
            sink,
            pool: ws_pool::WsPool::new(MAX_STREAMS_PER_CONN),
            next_id: 0,
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

          let instruments = resp.symbols
              .into_iter()
              .filter(|s| s.status == "TRADING")
              .map(|s| {
                  let kind = match s.contract_type.as_str() {
                      "PERPETUAL" => InstrumentKind::Perp,
                      _ => InstrumentKind::Spot, // simplification for now
                  };
                  Instrument {
                      id: InstrumentId { value: s.symbol.to_lowercase() },
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


/**   What the function should do

  1. Convert each (InstrumentId, Vec<DataType>) pair into Binance stream names
  2. Deduplicate — multiple DataTypes may produce the same stream name
  3. Build the JSON subscribe message
  4. Send it over self.ws_writer using SinkExt::send() (that's where the unused
  SinkExt import gets used)
  5. Increment self.next_id for the message id field (that's where next_id gets
  used)
  */


  async fn subscribe(&mut self, subscriptions: Vec<Subscription>) -> Result<(),
  VenueError> {
      // build deduplicated streams (same as before)
      let mut streams = std::collections::HashSet::new();
      for sub in &subscriptions {
          let symbol = &sub.instrument.value;
          for dt in &sub.data_type {
              let stream = match dt {
                  DataType::BookTicker  => format!("{symbol}@bookTicker"),
                  DataType::Trade       => format!("{symbol}@aggTrade"),
                  DataType::BookDepth   => format!("{symbol}@depth@100ms"),
                  DataType::FundingRate |
                  DataType::MarkPrice   |
                  DataType::IndexPrice  => format!("{symbol}@markPrice@1s"),
              };
              streams.insert(stream);
          }
      }

      let streams: Vec<String> = streams.into_iter().collect();

      // delegate to pool
      self.pool.subscribe(streams, &self.sink, &self.venue_id, &mut
  self.next_id).await
  }

  async fn disconnect(&mut self) -> Result<(), VenueError> {
    self.pool.disconnect().await
}
  }


// handle message function

  async fn handle_message<S: EventSink> (text: &str, venue_id: &VenueId, sink: &S) {
        
    // Extract event type
    
    let event_type: WsEventType = match serde_json::from_str(text) {
            Ok(e) => e,
            Err(_) => return,
        };

    let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;

    // match on event type, deserealize and send

    match event_type.e.as_str() {

    // BookTicker message schema
        "bookTicker" => {
        let msg: BookTickerMsg = match serde_json::from_str(text) {
            Ok(m) => m,
            Err(_) => return,
        };

        let _ = sink.send(Event {
            venue: venue_id.clone(),
            instrument: Some(InstrumentId { value: msg.s.to_lowercase()}),
            venue_ts: Some(msg.time * 1_000_000), //ms -> ns
            local_ts: Some(now),
            payload: Payload::MarketData(MarketDataPayload::BookTicker {
                best_bid: Level {price: msg.b, qty: msg.bq },
                best_ask: Level {price: msg.a, qty: msg.aq },
            }),
            sequence: None,

        }).await;
       },


       "aggTrade" => {
        let msg: AggTradeMsg = match serde_json::from_str(text) {
            Ok(m) => m,
            Err(_) => return,
        };

        let side = if msg.m { AggressorSide::Sell } else { AggressorSide::Buy };

        let _ = sink.send(Event{
            venue: venue_id.clone(),
            instrument: Some(InstrumentId { value: msg.s.to_lowercase()}),
            venue_ts: Some(msg.time * 1_000_000), //ms -> ns
            local_ts: Some(now),
            payload: Payload::MarketData(MarketDataPayload::Trades {
                trades: vec![Trade {price: msg.p, qty: msg.q, aggressor_side: side}],
            }),
            sequence: None,
        }).await;
       },



       "depthUpdate" => {
        let msg: DepthUpdateMsg = match serde_json::from_str(text) {
            Ok(m) => m,
            Err(_) => return,
        };
  
        let bids = msg.b.into_iter().map(|(p, q)| Level { price: p, qty: q
    }).collect();
        let asks = msg.a.into_iter().map(|(p, q)| Level { price: p, qty: q
    }).collect();
  
        let _ = sink.send(Event {
            venue: venue_id.clone(),
            instrument: Some(InstrumentId { value: msg.s.to_lowercase() }),
            venue_ts: Some(msg.event_time * 1_000_000),
            local_ts: Some(now),
            payload: Payload::MarketData(MarketDataPayload::BookUpdate { bids, asks
    }),
            sequence: None,
        }).await;
    },



    "markPriceUpdate" => {
        let msg: MarkPriceUpdateMsg = match serde_json::from_str(text) {
            Ok(m) => m,
            Err(_) => return,
        };
  
        let instrument = InstrumentId { value: msg.s.to_lowercase() };
        let venue_ts = Some(msg.event_time * 1_000_000);
  
        let _ = sink.send(Event {
            venue: venue_id.clone(),
            instrument: Some(instrument.clone()),
            venue_ts,
            local_ts: Some(now),
            payload: Payload::MarketData(MarketDataPayload::MarkPrice { price: msg.p
     }),
            sequence: None,
        }).await;
  
        let _ = sink.send(Event {
            venue: venue_id.clone(),
            instrument: Some(instrument.clone()),
            venue_ts,
            local_ts: Some(now),
            payload: Payload::MarketData(MarketDataPayload::IndexPrice { price:
    msg.i }),
            sequence: None,
        }).await;
  
        let _ = sink.send(Event {
            venue: venue_id.clone(),
            instrument: Some(instrument),
            venue_ts,
            local_ts: Some(now),
            payload: Payload::MarketData(MarketDataPayload::FundingRatePrediction {
                rate: msg.r,
                next_funding_time: msg.next_funding_time * 1_000_000,
            }),
            sequence: None,
        }).await;
    },
  

    _ => {},


    }


  }