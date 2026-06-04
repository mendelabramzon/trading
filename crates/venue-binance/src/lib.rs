use venue_core::*;
use venue_adapter::*;
use async_trait::async_trait;
use serde::Deserialize;
use rust_decimal::Decimal;
use tokio::task::JoinHandle;
use tokio::net::TcpStream;
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream,WebSocketStream};
use futures_util::{SinkExt, StreamExt, stream::SplitSink};

const BASE_REST_URL: &str = "https://fapi.binance.com";
const BASE_WS_URL: &str = "wss://fstream.binance.com/ws/";

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
      ws_writer: Option<WsWriter>,
      read_handle: Option<JoinHandle<()>>,
      next_id: u64,
  }

  impl<S: EventSink> BinanceAdapter<S> {
      pub fn new(sink: S) -> Self {
          Self { 
            venue_id: VenueId { value: "binance".to_string() }, 
            sink,
            ws_writer: None,
            read_handle: None,
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

        /// open a websocket
          let (ws_stream, _) = connect_async(BASE_WS_URL)
          .await
          .map_err(|e| VenueError::ConnectionFailed(e.to_string()))?;
        
        /// split and store the writer
        let (ws_writer, mut reader) = ws_stream.split();
        self.ws_writer = Some(ws_writer);

        /// clone, spawn, store the handle
        let sink = self.sink.clone();
        let venue_id = self.venue_id.clone();

        self.read_handle = Some(tokio::spawn(async move {
            while let Some(msg) = reader.next().await {
                match msg {
                    Ok(Message::Text(text)) => {
                        handle_message(&text, &venue_id, &sink).await;
                    }
                    Ok(Message::Close(_)) | Err(_) => break,
                    _ => {}
                }
            }
        }));

        Ok(())
      }

      async fn subscribe(&mut self, subscriptions: Vec<Subscription>) ->
  Result<(), VenueError> {
          todo!()
      }

      async fn disconnect(&mut self) -> Result<(), VenueError> {
          todo!()
      }
  }

  async fn handle_message(text: &str, venue_id: &VenueId, sink: &EventSink) {
        
    /// Extract event type
    
    let event_type: WsEventType = match serde_json::from_str(text) {
            Ok(e) => e,
            Err(_) => return,
        };

    let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;

    /// match on event type, deserealize and send

    match event_type.e.as_str() {

    /// BookTicker message schema
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
                best_ask: Level {price: msg.a, qty: msg:aq },
            }),
            sequence: None,

        }).await;
       };




    }


  }