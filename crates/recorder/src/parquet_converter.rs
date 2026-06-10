use std::fs::{self, File};
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{
    ArrayRef, Float64Array, StringArray, TimestampNanosecondArray, UInt16Array, UInt32Array,
    UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use num_traits::ToPrimitive;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use rust_decimal::Decimal;
use venue_core::{AggressorSide, ControlPayload, MarketPayload, Payload, SourceId};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

/// Rows buffered per table before a RecordBatch is flushed as one row group
/// (Bug 2: bounded memory instead of full-day buffering).
const BATCH_ROWS: usize = 500_000;

/// Conversion fails outright if more than this fraction of the WAL byte
/// stream had to be skipped as corrupt (P1): a file that damaged is an
/// operational incident, not something to silently log through.
const MAX_SKIPPED_RATIO: f64 = 0.01;

/// Decimal → f64 for analytics columns; conversion failure becomes a null
/// plus a warning, never a fabricated 0.0 (D5).
fn dec_opt(d: &Decimal, what: &str, instrument: &str) -> Option<f64> {
    let v = d.to_f64();
    if v.is_none() {
        tracing::warn!(%instrument, what, value = %d, "Decimal→f64 failed; writing null");
    }
    v
}

fn ts_field(name: &str, nullable: bool) -> Field {
    Field::new(
        name,
        DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
        nullable,
    )
}

fn ts_array_opt(vals: Vec<Option<i64>>) -> ArrayRef {
    Arc::new(TimestampNanosecondArray::from(vals).with_timezone("UTC"))
}

fn ts_array(vals: Vec<i64>) -> ArrayRef {
    Arc::new(TimestampNanosecondArray::from(vals).with_timezone("UTC"))
}

/// Lazily-opened zstd Parquet writer for one output table. The file is only
/// created once the first non-empty batch arrives, so absent data types leave
/// no empty files behind.
struct TableWriter {
    path: PathBuf,
    schema: Arc<Schema>,
    writer: Option<ArrowWriter<File>>,
    rows: usize,
}

impl TableWriter {
    fn new(output_dir: &Path, file_name: &str, schema: Schema) -> Self {
        Self {
            path: output_dir.join(file_name),
            schema: Arc::new(schema),
            writer: None,
            rows: 0,
        }
    }

    fn write_batch(&mut self, columns: Vec<ArrayRef>) -> Result<()> {
        let rows = columns.first().map_or(0, |c| c.len());
        if rows == 0 {
            return Ok(());
        }
        if self.writer.is_none() {
            let file = File::create(&self.path)?;
            let props = WriterProperties::builder()
                .set_compression(Compression::ZSTD(ZstdLevel::default()))
                .build();
            self.writer = Some(ArrowWriter::try_new(
                file,
                self.schema.clone(),
                Some(props),
            )?);
        }
        let batch = RecordBatch::try_new(self.schema.clone(), columns)?;
        self.writer.as_mut().unwrap().write(&batch)?;
        self.rows += rows;
        Ok(())
    }

    fn close(mut self) -> Result<usize> {
        if let Some(writer) = self.writer.take() {
            writer.close()?;
        }
        if self.rows > 0 {
            tracing::info!(path = %self.path.display(), rows = self.rows, "parquet written");
        }
        Ok(self.rows)
    }
}

fn aggressor_str(side: AggressorSide) -> &'static str {
    match side {
        AggressorSide::Buy => "buy",
        AggressorSide::Sell => "sell",
    }
}

/// Common per-event envelope columns shared by every market table.
#[derive(Default)]
struct EnvelopeCols {
    instrument: Vec<String>,
    venue_ts: Vec<Option<i64>>,
    local_ts: Vec<i64>,
    source: Vec<u16>,
}

impl EnvelopeCols {
    fn push(&mut self, instrument: &str, venue_ts: Option<u64>, local_ts: u64, source: SourceId) {
        self.instrument.push(instrument.to_string());
        self.venue_ts.push(venue_ts.map(|v| v as i64));
        self.local_ts.push(local_ts as i64);
        self.source.push(source.0);
    }

    fn len(&self) -> usize {
        self.instrument.len()
    }

    fn fields() -> Vec<Field> {
        vec![
            Field::new("instrument", DataType::Utf8, false),
            ts_field("venue_ts", true),
            ts_field("local_ts", false),
            Field::new("source", DataType::UInt16, false),
        ]
    }

    fn take_arrays(&mut self) -> Vec<ArrayRef> {
        vec![
            Arc::new(StringArray::from(std::mem::take(&mut self.instrument))),
            ts_array_opt(std::mem::take(&mut self.venue_ts)),
            ts_array(std::mem::take(&mut self.local_ts)),
            Arc::new(UInt16Array::from(std::mem::take(&mut self.source))),
        ]
    }
}

struct BookTickerTable {
    env: EnvelopeCols,
    update_id: Vec<u64>,
    bid_price: Vec<Option<f64>>,
    bid_qty: Vec<Option<f64>>,
    ask_price: Vec<Option<f64>>,
    ask_qty: Vec<Option<f64>>,
    writer: TableWriter,
}

impl BookTickerTable {
    fn new(dir: &Path) -> Self {
        let mut fields = EnvelopeCols::fields();
        fields.extend([
            Field::new("update_id", DataType::UInt64, false),
            Field::new("bid_price", DataType::Float64, true),
            Field::new("bid_qty", DataType::Float64, true),
            Field::new("ask_price", DataType::Float64, true),
            Field::new("ask_qty", DataType::Float64, true),
        ]);
        Self {
            env: EnvelopeCols::default(),
            update_id: Vec::new(),
            bid_price: Vec::new(),
            bid_qty: Vec::new(),
            ask_price: Vec::new(),
            ask_qty: Vec::new(),
            writer: TableWriter::new(dir, "book_ticker.parquet", Schema::new(fields)),
        }
    }

    fn flush(&mut self) -> Result<()> {
        let mut cols = self.env.take_arrays();
        cols.push(Arc::new(UInt64Array::from(std::mem::take(
            &mut self.update_id,
        ))));
        cols.push(Arc::new(Float64Array::from(std::mem::take(
            &mut self.bid_price,
        ))));
        cols.push(Arc::new(Float64Array::from(std::mem::take(
            &mut self.bid_qty,
        ))));
        cols.push(Arc::new(Float64Array::from(std::mem::take(
            &mut self.ask_price,
        ))));
        cols.push(Arc::new(Float64Array::from(std::mem::take(
            &mut self.ask_qty,
        ))));
        self.writer.write_batch(cols)
    }

    fn maybe_flush(&mut self) -> Result<()> {
        if self.env.len() >= BATCH_ROWS {
            self.flush()?;
        }
        Ok(())
    }

    fn finish(mut self) -> Result<usize> {
        self.flush()?;
        self.writer.close()
    }
}

struct TradeTable {
    env: EnvelopeCols,
    trade_id: Vec<String>,
    price: Vec<Option<f64>>,
    qty: Vec<Option<f64>>,
    side: Vec<&'static str>,
    kind: Vec<Option<String>>,
    writer: TableWriter,
}

impl TradeTable {
    fn new(dir: &Path) -> Self {
        let mut fields = EnvelopeCols::fields();
        fields.extend([
            Field::new("trade_id", DataType::Utf8, false),
            Field::new("price", DataType::Float64, true),
            Field::new("qty", DataType::Float64, true),
            Field::new("side", DataType::Utf8, false),
            Field::new("kind", DataType::Utf8, true),
        ]);
        Self {
            env: EnvelopeCols::default(),
            trade_id: Vec::new(),
            price: Vec::new(),
            qty: Vec::new(),
            side: Vec::new(),
            kind: Vec::new(),
            writer: TableWriter::new(dir, "trades.parquet", Schema::new(fields)),
        }
    }

    fn flush(&mut self) -> Result<()> {
        let mut cols = self.env.take_arrays();
        cols.push(Arc::new(StringArray::from(std::mem::take(
            &mut self.trade_id,
        ))));
        cols.push(Arc::new(Float64Array::from(std::mem::take(
            &mut self.price,
        ))));
        cols.push(Arc::new(Float64Array::from(std::mem::take(&mut self.qty))));
        cols.push(Arc::new(StringArray::from(std::mem::take(&mut self.side))));
        cols.push(Arc::new(StringArray::from(std::mem::take(&mut self.kind))));
        self.writer.write_batch(cols)
    }

    fn maybe_flush(&mut self) -> Result<()> {
        if self.env.len() >= BATCH_ROWS {
            self.flush()?;
        }
        Ok(())
    }

    fn finish(mut self) -> Result<usize> {
        self.flush()?;
        self.writer.close()
    }
}

struct SinglePriceTable {
    env: EnvelopeCols,
    price: Vec<Option<f64>>,
    writer: TableWriter,
}

impl SinglePriceTable {
    fn new(dir: &Path, file_name: &str) -> Self {
        let mut fields = EnvelopeCols::fields();
        fields.push(Field::new("price", DataType::Float64, true));
        Self {
            env: EnvelopeCols::default(),
            price: Vec::new(),
            writer: TableWriter::new(dir, file_name, Schema::new(fields)),
        }
    }

    fn flush(&mut self) -> Result<()> {
        let mut cols = self.env.take_arrays();
        cols.push(Arc::new(Float64Array::from(std::mem::take(
            &mut self.price,
        ))));
        self.writer.write_batch(cols)
    }

    fn maybe_flush(&mut self) -> Result<()> {
        if self.env.len() >= BATCH_ROWS {
            self.flush()?;
        }
        Ok(())
    }

    fn finish(mut self) -> Result<usize> {
        self.flush()?;
        self.writer.close()
    }
}

struct FundingPredictionTable {
    env: EnvelopeCols,
    rate: Vec<Option<f64>>,
    next_funding_time: Vec<i64>,
    interval_ns: Vec<Option<u64>>,
    premium_index: Vec<Option<f64>>,
    clamp_min: Vec<Option<f64>>,
    clamp_max: Vec<Option<f64>>,
    writer: TableWriter,
}

impl FundingPredictionTable {
    fn new(dir: &Path) -> Self {
        let mut fields = EnvelopeCols::fields();
        fields.extend([
            Field::new("rate", DataType::Float64, true),
            ts_field("next_funding_time", false),
            Field::new("interval_ns", DataType::UInt64, true),
            Field::new("premium_index", DataType::Float64, true),
            Field::new("clamp_min", DataType::Float64, true),
            Field::new("clamp_max", DataType::Float64, true),
        ]);
        Self {
            env: EnvelopeCols::default(),
            rate: Vec::new(),
            next_funding_time: Vec::new(),
            interval_ns: Vec::new(),
            premium_index: Vec::new(),
            clamp_min: Vec::new(),
            clamp_max: Vec::new(),
            writer: TableWriter::new(dir, "funding_rate.parquet", Schema::new(fields)),
        }
    }

    fn flush(&mut self) -> Result<()> {
        let mut cols = self.env.take_arrays();
        cols.push(Arc::new(Float64Array::from(std::mem::take(&mut self.rate))));
        cols.push(ts_array(std::mem::take(&mut self.next_funding_time)));
        cols.push(Arc::new(UInt64Array::from(std::mem::take(
            &mut self.interval_ns,
        ))));
        cols.push(Arc::new(Float64Array::from(std::mem::take(
            &mut self.premium_index,
        ))));
        cols.push(Arc::new(Float64Array::from(std::mem::take(
            &mut self.clamp_min,
        ))));
        cols.push(Arc::new(Float64Array::from(std::mem::take(
            &mut self.clamp_max,
        ))));
        self.writer.write_batch(cols)
    }

    fn maybe_flush(&mut self) -> Result<()> {
        if self.env.len() >= BATCH_ROWS {
            self.flush()?;
        }
        Ok(())
    }

    fn finish(mut self) -> Result<usize> {
        self.flush()?;
        self.writer.close()
    }
}

struct FundingRealizedTable {
    env: EnvelopeCols,
    rate: Vec<Option<f64>>,
    funding_time: Vec<i64>,
    interval_ns: Vec<Option<u64>>,
    writer: TableWriter,
}

impl FundingRealizedTable {
    fn new(dir: &Path) -> Self {
        let mut fields = EnvelopeCols::fields();
        fields.extend([
            Field::new("rate", DataType::Float64, true),
            ts_field("funding_time", false),
            Field::new("interval_ns", DataType::UInt64, true),
        ]);
        Self {
            env: EnvelopeCols::default(),
            rate: Vec::new(),
            funding_time: Vec::new(),
            interval_ns: Vec::new(),
            writer: TableWriter::new(dir, "funding_rate_realized.parquet", Schema::new(fields)),
        }
    }

    fn flush(&mut self) -> Result<()> {
        let mut cols = self.env.take_arrays();
        cols.push(Arc::new(Float64Array::from(std::mem::take(&mut self.rate))));
        cols.push(ts_array(std::mem::take(&mut self.funding_time)));
        cols.push(Arc::new(UInt64Array::from(std::mem::take(
            &mut self.interval_ns,
        ))));
        self.writer.write_batch(cols)
    }

    fn maybe_flush(&mut self) -> Result<()> {
        if self.env.len() >= BATCH_ROWS {
            self.flush()?;
        }
        Ok(())
    }

    fn finish(mut self) -> Result<usize> {
        self.flush()?;
        self.writer.close()
    }
}

/// Snapshot rows: one row per level. `level_idx` is meaningful here (rank in
/// the snapshot); update rows deliberately have no such column (D4).
struct BookSnapshotTable {
    env: EnvelopeCols,
    last_update_id: Vec<u64>,
    side: Vec<&'static str>,
    level_idx: Vec<u32>,
    price: Vec<Option<f64>>,
    qty: Vec<Option<f64>>,
    writer: TableWriter,
}

impl BookSnapshotTable {
    fn new(dir: &Path) -> Self {
        let mut fields = EnvelopeCols::fields();
        fields.extend([
            Field::new("last_update_id", DataType::UInt64, false),
            Field::new("side", DataType::Utf8, false),
            Field::new("level_idx", DataType::UInt32, false),
            Field::new("price", DataType::Float64, true),
            Field::new("qty", DataType::Float64, true),
        ]);
        Self {
            env: EnvelopeCols::default(),
            last_update_id: Vec::new(),
            side: Vec::new(),
            level_idx: Vec::new(),
            price: Vec::new(),
            qty: Vec::new(),
            writer: TableWriter::new(dir, "book_snapshot.parquet", Schema::new(fields)),
        }
    }

    fn flush(&mut self) -> Result<()> {
        let mut cols = self.env.take_arrays();
        cols.push(Arc::new(UInt64Array::from(std::mem::take(
            &mut self.last_update_id,
        ))));
        cols.push(Arc::new(StringArray::from(std::mem::take(&mut self.side))));
        cols.push(Arc::new(UInt32Array::from(std::mem::take(
            &mut self.level_idx,
        ))));
        cols.push(Arc::new(Float64Array::from(std::mem::take(
            &mut self.price,
        ))));
        cols.push(Arc::new(Float64Array::from(std::mem::take(&mut self.qty))));
        self.writer.write_batch(cols)
    }

    fn maybe_flush(&mut self) -> Result<()> {
        if self.env.len() >= BATCH_ROWS {
            self.flush()?;
        }
        Ok(())
    }

    fn finish(mut self) -> Result<usize> {
        self.flush()?;
        self.writer.close()
    }
}

/// Update rows: one row per changed level. No `level_idx` — diff entries have
/// no rank (D4); ordering/splicing runs on the update-id columns.
struct BookUpdateTable {
    env: EnvelopeCols,
    first_update_id: Vec<u64>,
    final_update_id: Vec<u64>,
    prev_final_update_id: Vec<Option<u64>>,
    event_time: Vec<Option<i64>>,
    side: Vec<&'static str>,
    price: Vec<Option<f64>>,
    qty: Vec<Option<f64>>,
    writer: TableWriter,
}

impl BookUpdateTable {
    fn new(dir: &Path) -> Self {
        let mut fields = EnvelopeCols::fields();
        fields.extend([
            Field::new("first_update_id", DataType::UInt64, false),
            Field::new("final_update_id", DataType::UInt64, false),
            Field::new("prev_final_update_id", DataType::UInt64, true),
            ts_field("event_time", true),
            Field::new("side", DataType::Utf8, false),
            Field::new("price", DataType::Float64, true),
            Field::new("qty", DataType::Float64, true),
        ]);
        Self {
            env: EnvelopeCols::default(),
            first_update_id: Vec::new(),
            final_update_id: Vec::new(),
            prev_final_update_id: Vec::new(),
            event_time: Vec::new(),
            side: Vec::new(),
            price: Vec::new(),
            qty: Vec::new(),
            writer: TableWriter::new(dir, "book_update.parquet", Schema::new(fields)),
        }
    }

    fn flush(&mut self) -> Result<()> {
        let mut cols = self.env.take_arrays();
        cols.push(Arc::new(UInt64Array::from(std::mem::take(
            &mut self.first_update_id,
        ))));
        cols.push(Arc::new(UInt64Array::from(std::mem::take(
            &mut self.final_update_id,
        ))));
        cols.push(Arc::new(UInt64Array::from(std::mem::take(
            &mut self.prev_final_update_id,
        ))));
        cols.push(ts_array_opt(std::mem::take(&mut self.event_time)));
        cols.push(Arc::new(StringArray::from(std::mem::take(&mut self.side))));
        cols.push(Arc::new(Float64Array::from(std::mem::take(
            &mut self.price,
        ))));
        cols.push(Arc::new(Float64Array::from(std::mem::take(&mut self.qty))));
        self.writer.write_batch(cols)
    }

    fn maybe_flush(&mut self) -> Result<()> {
        if self.env.len() >= BATCH_ROWS {
            self.flush()?;
        }
        Ok(())
    }

    fn finish(mut self) -> Result<usize> {
        self.flush()?;
        self.writer.close()
    }
}

struct LiquidationTable {
    env: EnvelopeCols,
    side: Vec<&'static str>,
    price: Vec<Option<f64>>,
    qty: Vec<Option<f64>>,
    filled_qty: Vec<Option<f64>>,
    avg_price: Vec<Option<f64>>,
    order_status: Vec<Option<String>>,
    writer: TableWriter,
}

impl LiquidationTable {
    fn new(dir: &Path) -> Self {
        let mut fields = EnvelopeCols::fields();
        fields.extend([
            Field::new("side", DataType::Utf8, false),
            Field::new("price", DataType::Float64, true),
            Field::new("qty", DataType::Float64, true),
            Field::new("filled_qty", DataType::Float64, true),
            Field::new("avg_price", DataType::Float64, true),
            Field::new("order_status", DataType::Utf8, true),
        ]);
        Self {
            env: EnvelopeCols::default(),
            side: Vec::new(),
            price: Vec::new(),
            qty: Vec::new(),
            filled_qty: Vec::new(),
            avg_price: Vec::new(),
            order_status: Vec::new(),
            writer: TableWriter::new(dir, "liquidation.parquet", Schema::new(fields)),
        }
    }

    fn flush(&mut self) -> Result<()> {
        let mut cols = self.env.take_arrays();
        cols.push(Arc::new(StringArray::from(std::mem::take(&mut self.side))));
        cols.push(Arc::new(Float64Array::from(std::mem::take(
            &mut self.price,
        ))));
        cols.push(Arc::new(Float64Array::from(std::mem::take(&mut self.qty))));
        cols.push(Arc::new(Float64Array::from(std::mem::take(
            &mut self.filled_qty,
        ))));
        cols.push(Arc::new(Float64Array::from(std::mem::take(
            &mut self.avg_price,
        ))));
        cols.push(Arc::new(StringArray::from(std::mem::take(
            &mut self.order_status,
        ))));
        self.writer.write_batch(cols)
    }

    fn maybe_flush(&mut self) -> Result<()> {
        if self.env.len() >= BATCH_ROWS {
            self.flush()?;
        }
        Ok(())
    }

    fn finish(mut self) -> Result<usize> {
        self.flush()?;
        self.writer.close()
    }
}

struct OpenInterestTable {
    env: EnvelopeCols,
    open_interest: Vec<Option<f64>>,
    open_interest_value: Vec<Option<f64>>,
    writer: TableWriter,
}

impl OpenInterestTable {
    fn new(dir: &Path) -> Self {
        let mut fields = EnvelopeCols::fields();
        fields.extend([
            Field::new("open_interest", DataType::Float64, true),
            Field::new("open_interest_value", DataType::Float64, true),
        ]);
        Self {
            env: EnvelopeCols::default(),
            open_interest: Vec::new(),
            open_interest_value: Vec::new(),
            writer: TableWriter::new(dir, "open_interest.parquet", Schema::new(fields)),
        }
    }

    fn flush(&mut self) -> Result<()> {
        let mut cols = self.env.take_arrays();
        cols.push(Arc::new(Float64Array::from(std::mem::take(
            &mut self.open_interest,
        ))));
        cols.push(Arc::new(Float64Array::from(std::mem::take(
            &mut self.open_interest_value,
        ))));
        self.writer.write_batch(cols)
    }

    fn maybe_flush(&mut self) -> Result<()> {
        if self.env.len() >= BATCH_ROWS {
            self.flush()?;
        }
        Ok(())
    }

    fn finish(mut self) -> Result<usize> {
        self.flush()?;
        self.writer.close()
    }
}

/// Control events keep their full payload as JSON in `detail`; `instrument`
/// is nullable because most control events are venue- or connection-scoped.
struct ControlTable {
    instrument: Vec<Option<String>>,
    venue_ts: Vec<Option<i64>>,
    local_ts: Vec<i64>,
    source: Vec<u16>,
    kind: Vec<&'static str>,
    detail: Vec<String>,
    writer: TableWriter,
}

impl ControlTable {
    fn new(dir: &Path) -> Self {
        let fields = vec![
            Field::new("instrument", DataType::Utf8, true),
            ts_field("venue_ts", true),
            ts_field("local_ts", false),
            Field::new("source", DataType::UInt16, false),
            Field::new("kind", DataType::Utf8, false),
            Field::new("detail", DataType::Utf8, false),
        ];
        Self {
            instrument: Vec::new(),
            venue_ts: Vec::new(),
            local_ts: Vec::new(),
            source: Vec::new(),
            kind: Vec::new(),
            detail: Vec::new(),
            writer: TableWriter::new(dir, "control.parquet", Schema::new(fields)),
        }
    }

    fn push(
        &mut self,
        instrument: Option<&str>,
        venue_ts: Option<u64>,
        local_ts: u64,
        source: SourceId,
        control: &ControlPayload,
    ) {
        let kind = match control {
            ControlPayload::ConnUp { .. } => "conn_up",
            ControlPayload::ConnDown { .. } => "conn_down",
            ControlPayload::Gap { .. } => "gap",
            ControlPayload::SnapshotBegin => "snapshot_begin",
            ControlPayload::SnapshotEnd => "snapshot_end",
            ControlPayload::SubAck { .. } => "sub_ack",
            ControlPayload::Reorg { .. } => "reorg",
        };
        self.instrument.push(instrument.map(str::to_string));
        self.venue_ts.push(venue_ts.map(|v| v as i64));
        self.local_ts.push(local_ts as i64);
        self.source.push(source.0);
        self.kind.push(kind);
        self.detail
            .push(serde_json::to_string(control).unwrap_or_else(|_| format!("{control:?}")));
    }

    fn flush(&mut self) -> Result<()> {
        let cols: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(std::mem::take(&mut self.instrument))),
            ts_array_opt(std::mem::take(&mut self.venue_ts)),
            ts_array(std::mem::take(&mut self.local_ts)),
            Arc::new(UInt16Array::from(std::mem::take(&mut self.source))),
            Arc::new(StringArray::from(std::mem::take(&mut self.kind))),
            Arc::new(StringArray::from(std::mem::take(&mut self.detail))),
        ];
        self.writer.write_batch(cols)
    }

    fn maybe_flush(&mut self) -> Result<()> {
        if self.instrument.len() >= BATCH_ROWS {
            self.flush()?;
        }
        Ok(())
    }

    fn finish(mut self) -> Result<usize> {
        self.flush()?;
        self.writer.close()
    }
}

pub fn convert_wal(wal_path: &Path, output_dir: &Path) -> Result<()> {
    let wal_len = fs::metadata(wal_path)?.len();
    let mut reader = wire::FrameReader::new(BufReader::new(File::open(wal_path)?));

    fs::create_dir_all(output_dir)?;

    let mut book_tickers = BookTickerTable::new(output_dir);
    let mut trades_t = TradeTable::new(output_dir);
    let mut mark_prices = SinglePriceTable::new(output_dir, "mark_price.parquet");
    let mut index_prices = SinglePriceTable::new(output_dir, "index_price.parquet");
    let mut funding_pred = FundingPredictionTable::new(output_dir);
    let mut funding_real = FundingRealizedTable::new(output_dir);
    let mut book_snapshots = BookSnapshotTable::new(output_dir);
    let mut book_updates = BookUpdateTable::new(output_dir);
    let mut liquidations = LiquidationTable::new(output_dir);
    let mut open_interest = OpenInterestTable::new(output_dir);
    let mut control = ControlTable::new(output_dir);

    let mut skipped_no_instrument = 0u64;
    let mut reference_events = 0u64;
    let mut chain_events = 0u64;
    let mut account_events = 0u64;

    while let Some(event) = reader.next_event()? {
        // Control events are routed even without an instrument; market events
        // without one are malformed and skipped with a count (N3).
        if let Payload::Control(c) = &event.payload {
            control.push(
                event.instrument.as_ref().map(|i| i.value.as_ref()),
                event.venue_ts,
                event.local_ts,
                event.source,
                c,
            );
            control.maybe_flush()?;
            continue;
        }

        let instrument = match &event.instrument {
            Some(id) => id.value.to_string(),
            None => {
                skipped_no_instrument += 1;
                tracing::warn!(payload = ?event.payload, "non-control event without instrument skipped");
                continue;
            }
        };
        let venue_ts = event.venue_ts;
        let local_ts = event.local_ts;
        let source = event.source;

        match &event.payload {
            Payload::Market(md) => match md {
                MarketPayload::BookTicker {
                    best_bid,
                    best_ask,
                    update_id,
                } => {
                    book_tickers
                        .env
                        .push(&instrument, venue_ts, local_ts, source);
                    book_tickers.update_id.push(*update_id);
                    book_tickers
                        .bid_price
                        .push(dec_opt(&best_bid.price, "bid_price", &instrument));
                    book_tickers
                        .bid_qty
                        .push(dec_opt(&best_bid.qty, "bid_qty", &instrument));
                    book_tickers
                        .ask_price
                        .push(dec_opt(&best_ask.price, "ask_price", &instrument));
                    book_tickers
                        .ask_qty
                        .push(dec_opt(&best_ask.qty, "ask_qty", &instrument));
                    book_tickers.maybe_flush()?;
                }
                MarketPayload::Trades { trades } => {
                    for trade in trades {
                        trades_t.env.push(&instrument, venue_ts, local_ts, source);
                        trades_t.trade_id.push(trade.id.to_string());
                        trades_t
                            .price
                            .push(dec_opt(&trade.price, "price", &instrument));
                        trades_t.qty.push(dec_opt(&trade.qty, "qty", &instrument));
                        trades_t.side.push(aggressor_str(trade.aggressor_side));
                        trades_t
                            .kind
                            .push(trade.kind.as_ref().map(|k| k.to_string()));
                    }
                    trades_t.maybe_flush()?;
                }
                MarketPayload::MarkPrice { price } => {
                    mark_prices
                        .env
                        .push(&instrument, venue_ts, local_ts, source);
                    mark_prices.price.push(dec_opt(price, "price", &instrument));
                    mark_prices.maybe_flush()?;
                }
                MarketPayload::IndexPrice { price } => {
                    index_prices
                        .env
                        .push(&instrument, venue_ts, local_ts, source);
                    index_prices
                        .price
                        .push(dec_opt(price, "price", &instrument));
                    index_prices.maybe_flush()?;
                }
                MarketPayload::FundingRatePrediction {
                    rate,
                    next_funding_time,
                    interval,
                    premium_index,
                    clamp_min,
                    clamp_max,
                } => {
                    funding_pred
                        .env
                        .push(&instrument, venue_ts, local_ts, source);
                    funding_pred.rate.push(dec_opt(rate, "rate", &instrument));
                    funding_pred
                        .next_funding_time
                        .push(*next_funding_time as i64);
                    funding_pred.interval_ns.push(*interval);
                    funding_pred.premium_index.push(
                        premium_index
                            .as_ref()
                            .and_then(|d| dec_opt(d, "premium_index", &instrument)),
                    );
                    funding_pred.clamp_min.push(
                        clamp_min
                            .as_ref()
                            .and_then(|d| dec_opt(d, "clamp_min", &instrument)),
                    );
                    funding_pred.clamp_max.push(
                        clamp_max
                            .as_ref()
                            .and_then(|d| dec_opt(d, "clamp_max", &instrument)),
                    );
                    funding_pred.maybe_flush()?;
                }
                MarketPayload::FundingRateRealized {
                    rate,
                    funding_time,
                    interval,
                } => {
                    funding_real
                        .env
                        .push(&instrument, venue_ts, local_ts, source);
                    funding_real.rate.push(dec_opt(rate, "rate", &instrument));
                    funding_real.funding_time.push(*funding_time as i64);
                    funding_real.interval_ns.push(*interval);
                    funding_real.maybe_flush()?;
                }
                MarketPayload::BookSnapshot {
                    bids,
                    asks,
                    last_update_id,
                } => {
                    for (side, levels) in [("bid", bids), ("ask", asks)] {
                        for (idx, level) in levels.iter().enumerate() {
                            book_snapshots
                                .env
                                .push(&instrument, venue_ts, local_ts, source);
                            book_snapshots.last_update_id.push(*last_update_id);
                            book_snapshots.side.push(side);
                            book_snapshots.level_idx.push(idx as u32);
                            book_snapshots
                                .price
                                .push(dec_opt(&level.price, "price", &instrument));
                            book_snapshots
                                .qty
                                .push(dec_opt(&level.qty, "qty", &instrument));
                        }
                    }
                    book_snapshots.maybe_flush()?;
                }
                MarketPayload::BookUpdate {
                    bids,
                    asks,
                    first_update_id,
                    final_update_id,
                    prev_final_update_id,
                    event_time,
                } => {
                    for (side, levels) in [("bid", bids), ("ask", asks)] {
                        for level in levels {
                            book_updates
                                .env
                                .push(&instrument, venue_ts, local_ts, source);
                            book_updates.first_update_id.push(*first_update_id);
                            book_updates.final_update_id.push(*final_update_id);
                            book_updates
                                .prev_final_update_id
                                .push(*prev_final_update_id);
                            book_updates.event_time.push(event_time.map(|v| v as i64));
                            book_updates.side.push(side);
                            book_updates
                                .price
                                .push(dec_opt(&level.price, "price", &instrument));
                            book_updates
                                .qty
                                .push(dec_opt(&level.qty, "qty", &instrument));
                        }
                    }
                    book_updates.maybe_flush()?;
                }
                MarketPayload::Liquidation {
                    side,
                    price,
                    qty,
                    filled_qty,
                    avg_price,
                    order_status,
                } => {
                    liquidations
                        .env
                        .push(&instrument, venue_ts, local_ts, source);
                    liquidations.side.push(aggressor_str(*side));
                    liquidations
                        .price
                        .push(dec_opt(price, "price", &instrument));
                    liquidations.qty.push(dec_opt(qty, "qty", &instrument));
                    liquidations.filled_qty.push(
                        filled_qty
                            .as_ref()
                            .and_then(|d| dec_opt(d, "filled_qty", &instrument)),
                    );
                    liquidations.avg_price.push(
                        avg_price
                            .as_ref()
                            .and_then(|d| dec_opt(d, "avg_price", &instrument)),
                    );
                    liquidations
                        .order_status
                        .push(order_status.as_ref().map(|s| s.to_string()));
                    liquidations.maybe_flush()?;
                }
                MarketPayload::OpenInterest {
                    open_interest: oi,
                    open_interest_value,
                } => {
                    open_interest
                        .env
                        .push(&instrument, venue_ts, local_ts, source);
                    open_interest
                        .open_interest
                        .push(dec_opt(oi, "open_interest", &instrument));
                    open_interest.open_interest_value.push(
                        open_interest_value
                            .as_ref()
                            .and_then(|d| dec_opt(d, "open_interest_value", &instrument)),
                    );
                    open_interest.maybe_flush()?;
                }
            },
            // No producers yet; counted so growth is visible in conversion logs.
            Payload::Reference(_) => reference_events += 1,
            Payload::Chain(_) => chain_events += 1,
            Payload::Account(_) => account_events += 1,
            Payload::Control(_) => unreachable!("control events routed above"),
        }
    }

    let stats = reader.stats().clone();
    if stats.resyncs > 0 || stats.undecodable_frames > 0 || stats.truncated_tail {
        tracing::warn!(
            frames_ok = stats.frames_ok,
            skipped_bytes = stats.skipped_bytes,
            resyncs = stats.resyncs,
            undecodable_frames = stats.undecodable_frames,
            truncated_tail = stats.truncated_tail,
            "WAL read recovered from damaged frames"
        );
    }
    if wal_len > 0 && (stats.skipped_bytes as f64 / wal_len as f64) > MAX_SKIPPED_RATIO {
        return Err(format!(
            "WAL too damaged to convert: {} of {} bytes skipped (> {:.0}% threshold)",
            stats.skipped_bytes,
            wal_len,
            MAX_SKIPPED_RATIO * 100.0
        )
        .into());
    }
    if skipped_no_instrument > 0 {
        tracing::warn!(skipped_no_instrument, "events without instrument skipped");
    }
    if reference_events + chain_events + account_events > 0 {
        tracing::info!(
            reference_events,
            chain_events,
            account_events,
            "non-market payloads counted but not yet converted"
        );
    }

    book_tickers.finish()?;
    trades_t.finish()?;
    mark_prices.finish()?;
    index_prices.finish()?;
    funding_pred.finish()?;
    funding_real.finish()?;
    book_snapshots.finish()?;
    book_updates.finish()?;
    liquidations.finish()?;
    open_interest.finish()?;
    control.finish()?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use rust_decimal_macros::dec;
    use venue_core::*;

    fn base_event(i: u64, payload: Payload) -> Event {
        Event {
            venue: VenueId {
                value: "test_venue".into(),
            },
            instrument: Some(InstrumentId {
                value: "btcusdt".into(),
            }),
            venue_ts: Some(1_700_000_000_000_000_000 + i * 1_000_000),
            local_ts: 1_700_000_000_100_000_000 + i * 1_000_000,
            source: SourceId(1),
            provenance: None,
            payload,
        }
    }

    fn make_events(n: u64) -> Vec<Event> {
        let mut events = Vec::new();
        for i in 0..n {
            events.push(base_event(
                i,
                Payload::Market(MarketPayload::BookTicker {
                    best_bid: Level {
                        price: dec!(50000),
                        qty: dec!(1),
                    },
                    best_ask: Level {
                        price: dec!(50001),
                        qty: dec!(2),
                    },
                    update_id: i,
                }),
            ));
        }
        events.push(base_event(
            n,
            Payload::Market(MarketPayload::BookUpdate {
                bids: vec![Level {
                    price: dec!(49999),
                    qty: dec!(3),
                }],
                asks: vec![],
                first_update_id: 157,
                final_update_id: 160,
                prev_final_update_id: Some(149),
                event_time: Some(1_700_000_000_000_000_111),
            }),
        ));
        events.push(base_event(
            n + 1,
            Payload::Market(MarketPayload::Trades {
                trades: vec![Trade {
                    id: "42".into(),
                    price: dec!(50000.5),
                    qty: dec!(0.25),
                    aggressor_side: AggressorSide::Buy,
                    kind: Some("MARKET".into()),
                }],
            }),
        ));
        let mut conn_up = base_event(
            n + 2,
            Payload::Control(ControlPayload::ConnUp {
                label: "ws-1".into(),
            }),
        );
        conn_up.instrument = None;
        conn_up.venue_ts = None;
        events.push(conn_up);
        events
    }

    fn write_wal(events: &[Event], path: &Path) {
        let mut buf = Vec::new();
        for e in events {
            wire::encode(e, &mut buf).unwrap();
        }
        fs::write(path, &buf).unwrap();
    }

    #[test]
    fn convert_roundtrip_schema_zstd_and_control_routing() {
        let tmp = tempfile::tempdir().unwrap();
        let wal_path = tmp.path().join("test.wal");
        let out_dir = tmp.path().join("out");
        write_wal(&make_events(10), &wal_path);

        convert_wal(&wal_path, &out_dir).unwrap();

        // book_ticker: schema, rows, zstd compression, UTC ns timestamps.
        let file = File::open(out_dir.join("book_ticker.parquet")).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let meta = builder.metadata().clone();
        assert_eq!(meta.file_metadata().num_rows(), 10);
        assert_eq!(
            meta.row_group(0).column(0).compression(),
            parquet::basic::Compression::ZSTD(Default::default())
        );
        let schema = builder.schema().clone();
        let names: Vec<_> = schema.fields().iter().map(|f| f.name().clone()).collect();
        assert_eq!(
            names,
            [
                "instrument",
                "venue_ts",
                "local_ts",
                "source",
                "update_id",
                "bid_price",
                "bid_qty",
                "ask_price",
                "ask_qty"
            ]
        );
        assert_eq!(
            schema.field(2).data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
        );
        assert!(schema.field(5).is_nullable(), "prices are nullable (D5)");
        let batches: Vec<_> = builder
            .build()
            .unwrap()
            .collect::<std::result::Result<_, _>>()
            .unwrap();
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 10);

        // book_update: id columns present, no level_idx (D4 split).
        let file = File::open(out_dir.join("book_update.parquet")).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let names: Vec<_> = builder
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        assert!(names.contains(&"first_update_id".to_string()));
        assert!(names.contains(&"prev_final_update_id".to_string()));
        assert!(names.contains(&"event_time".to_string()));
        assert!(
            !names.contains(&"level_idx".to_string()),
            "diff rows must not fabricate a level rank (D4)"
        );

        // trades: string trade id.
        let file = File::open(out_dir.join("trades.parquet")).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        assert_eq!(
            builder.schema().field(4).data_type(),
            &DataType::Utf8,
            "trade_id is a string (R6)"
        );

        // control: ConnUp routed despite instrument: None.
        let file = File::open(out_dir.join("control.parquet")).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        assert_eq!(builder.metadata().file_metadata().num_rows(), 1);

        // No empty files for absent types.
        assert!(!out_dir.join("liquidation.parquet").exists());
        assert!(!out_dir.join("open_interest.parquet").exists());
    }

    #[test]
    fn convert_recovers_past_one_corrupt_frame() {
        let tmp = tempfile::tempdir().unwrap();
        let wal_path = tmp.path().join("test.wal");
        let out_dir = tmp.path().join("out");

        // Enough events that one damaged frame stays under the 1% byte gate.
        let events = make_events(2000);
        let mut buf = Vec::new();
        for e in &events {
            wire::encode(e, &mut buf).unwrap();
        }
        // Corrupt one payload byte mid-file.
        let mid = buf.len() / 2;
        buf[mid] ^= 0xFF;
        fs::write(&wal_path, &buf).unwrap();

        convert_wal(&wal_path, &out_dir).unwrap();

        let file = File::open(out_dir.join("book_ticker.parquet")).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let rows = builder.metadata().file_metadata().num_rows();
        assert!(
            (1998..2000).contains(&rows),
            "all but the corrupted frame(s) survive, got {rows}"
        );
    }
}
