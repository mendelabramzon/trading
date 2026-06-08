use std::fs::{self, File};
use std::io::{BufReader, Read};
use std::path::Path;
use std::sync::Arc;

use arrow::array::{Float64Array, StringArray, UInt32Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use num_traits::ToPrimitive;
use parquet::arrow::ArrowWriter;
use venue_core::{MarketDataPayload, Payload};

pub fn convert_wal(wal_path: &Path, output_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    // Stream events from the WAL file and collect columns inline
    let mut reader = BufReader::new(File::open(wal_path)?);
    let mut len_buf = [0u8; 4];

    let mut book_tickers = BookTickerColumns::new();
    let mut trades = TradeColumns::new();
    let mut mark_prices = SinglePriceColumns::new();
    let mut index_prices = SinglePriceColumns::new();
    let mut funding_rates = FundingRateColumns::new();
    let mut book_snapshots = BookDepthColumns::new();
    let mut book_updates = BookDepthColumns::new();
    let mut funding_realized = FundingRateColumns::new();

    loop {
        // read 4-byte length prefix
        match reader.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        }
        let len = u32::from_le_bytes(len_buf) as usize;

        // read exactly that many payload bytes
        let mut payload = vec![0u8; len];
        reader.read_exact(&mut payload)?;

        let event =
            wire::decode_payload(&payload).map_err(|e| format!("wire decode failed: {e:?}"))?;

        let instrument = match &event.instrument {
            Some(id) => id.value.to_string(),
            None => continue,
        };
        let venue_ts = event.venue_ts.unwrap_or(0);
        let local_ts = event.local_ts.unwrap_or(0);

        match &event.payload {
            Payload::MarketData(md) => match md {
                MarketDataPayload::BookTicker { best_bid, best_ask } => {
                    book_tickers.push(
                        instrument,
                        venue_ts,
                        local_ts,
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
                            instrument.clone(),
                            venue_ts,
                            local_ts,
                            trade.price.to_f64().unwrap_or(0.0),
                            trade.qty.to_f64().unwrap_or(0.0),
                            side,
                        );
                    }
                }
                MarketDataPayload::MarkPrice { price } => {
                    mark_prices.push(
                        instrument,
                        venue_ts,
                        local_ts,
                        price.to_f64().unwrap_or(0.0),
                    );
                }
                MarketDataPayload::IndexPrice { price } => {
                    index_prices.push(
                        instrument,
                        venue_ts,
                        local_ts,
                        price.to_f64().unwrap_or(0.0),
                    );
                }
                MarketDataPayload::FundingRatePrediction {
                    rate,
                    next_funding_time,
                } => {
                    funding_rates.push(
                        instrument,
                        venue_ts,
                        local_ts,
                        rate.to_f64().unwrap_or(0.0),
                        *next_funding_time,
                    );
                }
                MarketDataPayload::BookSnapshot { bids, asks } => {
                    book_snapshots.push_levels(&instrument, venue_ts, local_ts, bids, asks);
                }
                MarketDataPayload::BookUpdate { bids, asks } => {
                    book_updates.push_levels(&instrument, venue_ts, local_ts, bids, asks);
                }
                MarketDataPayload::FundingRateRealized { rate, funding_time } => {
                    funding_realized.push(
                        instrument,
                        venue_ts,
                        local_ts,
                        rate.to_f64().unwrap_or(0.0),
                        *funding_time,
                    );
                }
            },
            Payload::Error(_) => {}
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
        write_funding_rates(output_dir, "funding_rate.parquet", &funding_rates)?;
    }
    if !book_snapshots.instrument.is_empty() {
        write_book_depth(output_dir, "book_snapshot.parquet", &book_snapshots)?;
    }
    if !book_updates.instrument.is_empty() {
        write_book_depth(output_dir, "book_update.parquet", &book_updates)?;
    }
    if !funding_realized.instrument.is_empty() {
        write_funding_rates(
            output_dir,
            "funding_rate_realized.parquet",
            &funding_realized,
        )?;
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
            instrument: Vec::new(),
            venue_ts: Vec::new(),
            local_ts: Vec::new(),
            bid_price: Vec::new(),
            bid_qty: Vec::new(),
            ask_price: Vec::new(),
            ask_qty: Vec::new(),
        }
    }
    #[allow(clippy::too_many_arguments)]
    fn push(&mut self, inst: String, vts: u64, lts: u64, bp: f64, bq: f64, ap: f64, aq: f64) {
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
            instrument: Vec::new(),
            venue_ts: Vec::new(),
            local_ts: Vec::new(),
            price: Vec::new(),
            qty: Vec::new(),
            side: Vec::new(),
        }
    }
    fn push(&mut self, inst: String, vts: u64, lts: u64, p: f64, q: f64, s: &str) {
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
            instrument: Vec::new(),
            venue_ts: Vec::new(),
            local_ts: Vec::new(),
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
            instrument: Vec::new(),
            venue_ts: Vec::new(),
            local_ts: Vec::new(),
            rate: Vec::new(),
            next_funding_time: Vec::new(),
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

struct BookDepthColumns {
    instrument: Vec<String>,
    venue_ts: Vec<u64>,
    local_ts: Vec<u64>,
    side: Vec<String>,
    level_idx: Vec<u32>,
    price: Vec<f64>,
    qty: Vec<f64>,
}

impl BookDepthColumns {
    fn new() -> Self {
        Self {
            instrument: Vec::new(),
            venue_ts: Vec::new(),
            local_ts: Vec::new(),
            side: Vec::new(),
            level_idx: Vec::new(),
            price: Vec::new(),
            qty: Vec::new(),
        }
    }

    fn push_levels(
        &mut self,
        inst: &str,
        vts: u64,
        lts: u64,
        bids: &[venue_core::Level],
        asks: &[venue_core::Level],
    ) {
        for (i, level) in bids.iter().enumerate() {
            self.instrument.push(inst.to_string());
            self.venue_ts.push(vts);
            self.local_ts.push(lts);
            self.side.push("bid".to_string());
            self.level_idx.push(i as u32);
            self.price.push(level.price.to_f64().unwrap_or(0.0));
            self.qty.push(level.qty.to_f64().unwrap_or(0.0));
        }
        for (i, level) in asks.iter().enumerate() {
            self.instrument.push(inst.to_string());
            self.venue_ts.push(vts);
            self.local_ts.push(lts);
            self.side.push("ask".to_string());
            self.level_idx.push(i as u32);
            self.price.push(level.price.to_f64().unwrap_or(0.0));
            self.qty.push(level.qty.to_f64().unwrap_or(0.0));
        }
    }
}

// --- Parquet writers ---

fn write_book_tickers(
    dir: &Path,
    cols: &BookTickerColumns,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("instrument", DataType::Utf8, false),
        Field::new("venue_ts", DataType::UInt64, false),
        Field::new("local_ts", DataType::UInt64, false),
        Field::new("bid_price", DataType::Float64, false),
        Field::new("bid_qty", DataType::Float64, false),
        Field::new("ask_price", DataType::Float64, false),
        Field::new("ask_qty", DataType::Float64, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(cols.instrument.clone())),
            Arc::new(UInt64Array::from(cols.venue_ts.clone())),
            Arc::new(UInt64Array::from(cols.local_ts.clone())),
            Arc::new(Float64Array::from(cols.bid_price.clone())),
            Arc::new(Float64Array::from(cols.bid_qty.clone())),
            Arc::new(Float64Array::from(cols.ask_price.clone())),
            Arc::new(Float64Array::from(cols.ask_qty.clone())),
        ],
    )?;

    let file = File::create(dir.join("book_ticker.parquet"))?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

fn write_trades(dir: &Path, cols: &TradeColumns) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("instrument", DataType::Utf8, false),
        Field::new("venue_ts", DataType::UInt64, false),
        Field::new("local_ts", DataType::UInt64, false),
        Field::new("price", DataType::Float64, false),
        Field::new("qty", DataType::Float64, false),
        Field::new("side", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(cols.instrument.clone())),
            Arc::new(UInt64Array::from(cols.venue_ts.clone())),
            Arc::new(UInt64Array::from(cols.local_ts.clone())),
            Arc::new(Float64Array::from(cols.price.clone())),
            Arc::new(Float64Array::from(cols.qty.clone())),
            Arc::new(StringArray::from(cols.side.clone())),
        ],
    )?;

    let file = File::create(dir.join("trades.parquet"))?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

fn write_single_price(
    dir: &Path,
    filename: &str,
    cols: &SinglePriceColumns,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("instrument", DataType::Utf8, false),
        Field::new("venue_ts", DataType::UInt64, false),
        Field::new("local_ts", DataType::UInt64, false),
        Field::new("price", DataType::Float64, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(cols.instrument.clone())),
            Arc::new(UInt64Array::from(cols.venue_ts.clone())),
            Arc::new(UInt64Array::from(cols.local_ts.clone())),
            Arc::new(Float64Array::from(cols.price.clone())),
        ],
    )?;

    let file = File::create(dir.join(filename))?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

fn write_funding_rates(
    dir: &Path,
    filename: &str,
    cols: &FundingRateColumns,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("instrument", DataType::Utf8, false),
        Field::new("venue_ts", DataType::UInt64, false),
        Field::new("local_ts", DataType::UInt64, false),
        Field::new("rate", DataType::Float64, false),
        Field::new("next_funding_time", DataType::UInt64, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(cols.instrument.clone())),
            Arc::new(UInt64Array::from(cols.venue_ts.clone())),
            Arc::new(UInt64Array::from(cols.local_ts.clone())),
            Arc::new(Float64Array::from(cols.rate.clone())),
            Arc::new(UInt64Array::from(cols.next_funding_time.clone())),
        ],
    )?;

    let file = File::create(dir.join(filename))?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

fn write_book_depth(
    dir: &Path,
    filename: &str,
    cols: &BookDepthColumns,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("instrument", DataType::Utf8, false),
        Field::new("venue_ts", DataType::UInt64, false),
        Field::new("local_ts", DataType::UInt64, false),
        Field::new("side", DataType::Utf8, false),
        Field::new("level_idx", DataType::UInt32, false),
        Field::new("price", DataType::Float64, false),
        Field::new("qty", DataType::Float64, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(cols.instrument.clone())),
            Arc::new(UInt64Array::from(cols.venue_ts.clone())),
            Arc::new(UInt64Array::from(cols.local_ts.clone())),
            Arc::new(StringArray::from(cols.side.clone())),
            Arc::new(UInt32Array::from(cols.level_idx.clone())),
            Arc::new(Float64Array::from(cols.price.clone())),
            Arc::new(Float64Array::from(cols.qty.clone())),
        ],
    )?;

    let file = File::create(dir.join(filename))?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}
