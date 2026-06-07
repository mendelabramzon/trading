use std::fs::{self, File};
  use std::path::Path;
  use std::sync::Arc;

  use arrow::array::{Float64Array, StringArray, UInt64Array};
  use arrow::datatypes::{DataType, Field, Schema};
  use arrow::record_batch::RecordBatch;
  use parquet::arrow::ArrowWriter;
  use num_traits::ToPrimitive;
  use venue_core::{Event, MarketDataPayload, Payload};

  pub fn convert_wal(wal_path: &Path, output_dir: &Path) -> Result<(), Box<dyn
  std::error::Error>> {
      // 1. Read and decode all events from the WAL file
      let data = fs::read(wal_path)?;
      let mut events = Vec::new();
      let mut offset = 0;
      while offset < data.len() {
          match wire::decode(&data[offset..]) {
              Ok((event, consumed)) => {
                  events.push(event);
                  offset += consumed;
              }
              Err(_) => break,
          }
      }

      // 2. Collect columns per data type
      let mut book_tickers = BookTickerColumns::new();
      let mut trades = TradeColumns::new();
      let mut mark_prices = SinglePriceColumns::new();
      let mut index_prices = SinglePriceColumns::new();
      let mut funding_rates = FundingRateColumns::new();

      for event in &events {
          let instrument = match &event.instrument {
              Some(id) => id.value.clone(),
              None => continue,
          };
          let venue_ts = event.venue_ts.unwrap_or(0);
          let local_ts = event.local_ts.unwrap_or(0);

          match &event.payload {
              Payload::MarketData(md) => match md {
                  MarketDataPayload::BookTicker { best_bid, best_ask } => {
                      book_tickers.push(
                          instrument, venue_ts, local_ts,
                          best_bid.price.to_f64().unwrap_or(0.0),
                          best_bid.qty.to_f64().unwrap_or(0.0),
                          best_ask.price.to_f64().unwrap_or(0.0),
                          best_ask.qty.to_f64().unwrap_or(0.0),
                      );
                  }
                  MarketDataPayload::Trades { trades: trade_list } => {
                      for trade in trade_list {
                          let side = match trade.aggressor_side {
                              venue_core::AggressorSide::Buy => "buy",
                              venue_core::AggressorSide::Sell => "sell",
                          };
                          trades.push(
                              instrument.clone(), venue_ts, local_ts,
                              trade.price.to_f64().unwrap_or(0.0),
                              trade.qty.to_f64().unwrap_or(0.0),
                              side,
                          );
                      }
                  }
                  MarketDataPayload::MarkPrice { price } => {
                      mark_prices.push(
                          instrument, venue_ts, local_ts,
                          price.to_f64().unwrap_or(0.0),
                      );
                  }
                  MarketDataPayload::IndexPrice { price } => {
                      index_prices.push(
                          instrument, venue_ts, local_ts,
                          price.to_f64().unwrap_or(0.0),
                      );
                  }
                  MarketDataPayload::FundingRatePrediction { rate,
  next_funding_time } => {
                      funding_rates.push(
                          instrument, venue_ts, local_ts,
                          rate.to_f64().unwrap_or(0.0),
                          *next_funding_time,
                      );
                  }
                  _ => {}
              },
              _ => {}
          }
      }

      // 3. Write each non-empty table to parquet
      fs::create_dir_all(output_dir)?;

      if !book_tickers.instrument.is_empty() {
          write_book_tickers(output_dir, &book_tickers)?;
      }
      if !trades.instrument.is_empty() {
          write_trades(output_dir, &trades)?;
      }
      if !mark_prices.instrument.is_empty() {
          write_single_price(output_dir, "mark_price.parquet", &mark_prices)?;
      }
      if !index_prices.instrument.is_empty() {
          write_single_price(output_dir, "index_price.parquet", &index_prices)?;
      }
      if !funding_rates.instrument.is_empty() {
          write_funding_rates(output_dir, &funding_rates)?;
      }

      Ok(())
  }

  // --- Column collectors ---

  struct BookTickerColumns {
      instrument: Vec<String>,
      venue_ts: Vec<u64>,
      local_ts: Vec<u64>,
      bid_price: Vec<f64>,
      bid_qty: Vec<f64>,
      ask_price: Vec<f64>,
      ask_qty: Vec<f64>,
  }

  impl BookTickerColumns {
      fn new() -> Self {
          Self {
              instrument: Vec::new(), venue_ts: Vec::new(), local_ts: Vec::new(),
              bid_price: Vec::new(), bid_qty: Vec::new(),
              ask_price: Vec::new(), ask_qty: Vec::new(),
          }
      }
      fn push(&mut self, inst: String, vts: u64, lts: u64, bp: f64, bq: f64, ap:
  f64, aq: f64) {
          self.instrument.push(inst);
          self.venue_ts.push(vts);
          self.local_ts.push(lts);
          self.bid_price.push(bp);
          self.bid_qty.push(bq);
          self.ask_price.push(ap);
          self.ask_qty.push(aq);
      }
  }

  struct TradeColumns {
      instrument: Vec<String>,
      venue_ts: Vec<u64>,
      local_ts: Vec<u64>,
      price: Vec<f64>,
      qty: Vec<f64>,
      side: Vec<String>,
  }

  impl TradeColumns {
      fn new() -> Self {
          Self {
              instrument: Vec::new(), venue_ts: Vec::new(), local_ts: Vec::new(),
              price: Vec::new(), qty: Vec::new(), side: Vec::new(),
          }
      }
      fn push(&mut self, inst: String, vts: u64, lts: u64, p: f64, q: f64, s:
  &str) {
          self.instrument.push(inst);
          self.venue_ts.push(vts);
          self.local_ts.push(lts);
          self.price.push(p);
          self.qty.push(q);
          self.side.push(s.to_string());
      }
  }

  struct SinglePriceColumns {
      instrument: Vec<String>,
      venue_ts: Vec<u64>,
      local_ts: Vec<u64>,
      price: Vec<f64>,
  }

  impl SinglePriceColumns {
      fn new() -> Self {
          Self {
              instrument: Vec::new(), venue_ts: Vec::new(), local_ts: Vec::new(),
              price: Vec::new(),
          }
      }
      fn push(&mut self, inst: String, vts: u64, lts: u64, p: f64) {
          self.instrument.push(inst);
          self.venue_ts.push(vts);
          self.local_ts.push(lts);
          self.price.push(p);
      }
  }

  struct FundingRateColumns {
      instrument: Vec<String>,
      venue_ts: Vec<u64>,
      local_ts: Vec<u64>,
      rate: Vec<f64>,
      next_funding_time: Vec<u64>,
  }

  impl FundingRateColumns {
      fn new() -> Self {
          Self {
              instrument: Vec::new(), venue_ts: Vec::new(), local_ts: Vec::new(),
              rate: Vec::new(), next_funding_time: Vec::new(),
          }
      }
      fn push(&mut self, inst: String, vts: u64, lts: u64, r: f64, nft: u64) {
          self.instrument.push(inst);
          self.venue_ts.push(vts);
          self.local_ts.push(lts);
          self.rate.push(r);
          self.next_funding_time.push(nft);
      }
  }

  // --- Parquet writers ---

  fn write_book_tickers(dir: &Path, cols: &BookTickerColumns) -> Result<(),
  Box<dyn std::error::Error>> {
      let schema = Arc::new(Schema::new(vec![
          Field::new("instrument", DataType::Utf8, false),
          Field::new("venue_ts", DataType::UInt64, false),
          Field::new("local_ts", DataType::UInt64, false),
          Field::new("bid_price", DataType::Float64, false),
          Field::new("bid_qty", DataType::Float64, false),
          Field::new("ask_price", DataType::Float64, false),
          Field::new("ask_qty", DataType::Float64, false),
      ]));

      let batch = RecordBatch::try_new(schema.clone(), vec![
          Arc::new(StringArray::from(cols.instrument.clone())),
          Arc::new(UInt64Array::from(cols.venue_ts.clone())),
          Arc::new(UInt64Array::from(cols.local_ts.clone())),
          Arc::new(Float64Array::from(cols.bid_price.clone())),
          Arc::new(Float64Array::from(cols.bid_qty.clone())),
          Arc::new(Float64Array::from(cols.ask_price.clone())),
          Arc::new(Float64Array::from(cols.ask_qty.clone())),
      ])?;

      let file = File::create(dir.join("book_ticker.parquet"))?;
      let mut writer = ArrowWriter::try_new(file, schema, None)?;
      writer.write(&batch)?;
      writer.close()?;
      Ok(())
  }

  fn write_trades(dir: &Path, cols: &TradeColumns) -> Result<(), Box<dyn
  std::error::Error>> {
      let schema = Arc::new(Schema::new(vec![
          Field::new("instrument", DataType::Utf8, false),
          Field::new("venue_ts", DataType::UInt64, false),
          Field::new("local_ts", DataType::UInt64, false),
          Field::new("price", DataType::Float64, false),
          Field::new("qty", DataType::Float64, false),
          Field::new("side", DataType::Utf8, false),
      ]));

      let batch = RecordBatch::try_new(schema.clone(), vec![
          Arc::new(StringArray::from(cols.instrument.clone())),
          Arc::new(UInt64Array::from(cols.venue_ts.clone())),
          Arc::new(UInt64Array::from(cols.local_ts.clone())),
          Arc::new(Float64Array::from(cols.price.clone())),
          Arc::new(Float64Array::from(cols.qty.clone())),
          Arc::new(StringArray::from(cols.side.clone())),
      ])?;

      let file = File::create(dir.join("trades.parquet"))?;
      let mut writer = ArrowWriter::try_new(file, schema, None)?;
      writer.write(&batch)?;
      writer.close()?;
      Ok(())
  }

  fn write_single_price(dir: &Path, filename: &str, cols: &SinglePriceColumns) ->
  Result<(), Box<dyn std::error::Error>> {
      let schema = Arc::new(Schema::new(vec![
          Field::new("instrument", DataType::Utf8, false),
          Field::new("venue_ts", DataType::UInt64, false),
          Field::new("local_ts", DataType::UInt64, false),
          Field::new("price", DataType::Float64, false),
      ]));

      let batch = RecordBatch::try_new(schema.clone(), vec![
          Arc::new(StringArray::from(cols.instrument.clone())),
          Arc::new(UInt64Array::from(cols.venue_ts.clone())),
          Arc::new(UInt64Array::from(cols.local_ts.clone())),
          Arc::new(Float64Array::from(cols.price.clone())),
      ])?;

      let file = File::create(dir.join(filename))?;
      let mut writer = ArrowWriter::try_new(file, schema, None)?;
      writer.write(&batch)?;
      writer.close()?;
      Ok(())
  }

  fn write_funding_rates(dir: &Path, cols: &FundingRateColumns) -> Result<(),
  Box<dyn std::error::Error>> {
      let schema = Arc::new(Schema::new(vec![
          Field::new("instrument", DataType::Utf8, false),
          Field::new("venue_ts", DataType::UInt64, false),
          Field::new("local_ts", DataType::UInt64, false),
          Field::new("rate", DataType::Float64, false),
          Field::new("next_funding_time", DataType::UInt64, false),
      ]));

      let batch = RecordBatch::try_new(schema.clone(), vec![
          Arc::new(StringArray::from(cols.instrument.clone())),
          Arc::new(UInt64Array::from(cols.venue_ts.clone())),
          Arc::new(UInt64Array::from(cols.local_ts.clone())),
          Arc::new(Float64Array::from(cols.rate.clone())),
          Arc::new(UInt64Array::from(cols.next_funding_time.clone())),
      ])?;

      let file = File::create(dir.join("funding_rate.parquet"))?;
      let mut writer = ArrowWriter::try_new(file, schema, None)?;
      writer.write(&batch)?;
      writer.close()?;
      Ok(())
  }
