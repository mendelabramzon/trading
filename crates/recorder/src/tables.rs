//! Parquet table writers shared by the WAL converter and the `backfill`
//! crate: backfilled REST history must land with schemas identical to the
//! live tables so research unions them trivially (the `source` column and
//! the output root carry provenance, not the schema).

use std::fs::File;
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
use venue_core::{AggressorSide, ControlPayload, Nanos, ReferencePayload, SourceId};

pub type TableError = Box<dyn std::error::Error>;
type Result<T> = std::result::Result<T, TableError>;

/// Rows buffered per table before a RecordBatch is flushed as one row group
/// (Bug 2: bounded memory instead of full-day buffering).
pub const BATCH_ROWS: usize = 500_000;

/// Decimal → f64 for analytics columns; conversion failure becomes a null
/// plus a warning, never a fabricated 0.0 (D5).
pub fn dec_opt(d: &Decimal, what: &str, instrument: &str) -> Option<f64> {
    let v = d.to_f64();
    if v.is_none() {
        tracing::warn!(%instrument, what, value = %d, "Decimal→f64 failed; writing null");
    }
    v
}

pub fn ts_field(name: &str, nullable: bool) -> Field {
    Field::new(
        name,
        DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
        nullable,
    )
}

pub fn ts_array_opt(vals: Vec<Option<i64>>) -> ArrayRef {
    Arc::new(TimestampNanosecondArray::from(vals).with_timezone("UTC"))
}

pub fn ts_array(vals: Vec<i64>) -> ArrayRef {
    Arc::new(TimestampNanosecondArray::from(vals).with_timezone("UTC"))
}

pub(crate) fn aggressor_str(side: AggressorSide) -> &'static str {
    match side {
        AggressorSide::Buy => "buy",
        AggressorSide::Sell => "sell",
    }
}

/// Lazily-opened zstd Parquet writer for one output table. The file is only
/// created once the first non-empty batch arrives, so absent data types leave
/// no empty files behind.
pub struct TableWriter {
    path: PathBuf,
    schema: Arc<Schema>,
    writer: Option<ArrowWriter<File>>,
    rows: usize,
}

impl TableWriter {
    pub fn new(output_dir: &Path, file_name: &str, schema: Schema) -> Self {
        Self {
            path: output_dir.join(file_name),
            schema: Arc::new(schema),
            writer: None,
            rows: 0,
        }
    }

    pub fn write_batch(&mut self, columns: Vec<ArrayRef>) -> Result<()> {
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

    pub fn close(mut self) -> Result<usize> {
        if let Some(writer) = self.writer.take() {
            writer.close()?;
        }
        if self.rows > 0 {
            tracing::info!(path = %self.path.display(), rows = self.rows, "parquet written");
        }
        Ok(self.rows)
    }
}

/// Common per-event envelope columns shared by every market table.
#[derive(Default)]
pub struct EnvelopeCols {
    instrument: Vec<String>,
    venue_ts: Vec<Option<i64>>,
    local_ts: Vec<i64>,
    source: Vec<u16>,
}

impl EnvelopeCols {
    pub fn push(
        &mut self,
        instrument: &str,
        venue_ts: Option<u64>,
        local_ts: u64,
        source: SourceId,
    ) {
        self.instrument.push(instrument.to_string());
        self.venue_ts.push(venue_ts.map(|v| v as i64));
        self.local_ts.push(local_ts as i64);
        self.source.push(source.0);
    }

    pub fn len(&self) -> usize {
        self.instrument.len()
    }

    pub fn is_empty(&self) -> bool {
        self.instrument.is_empty()
    }

    pub fn fields() -> Vec<Field> {
        vec![
            Field::new("instrument", DataType::Utf8, false),
            ts_field("venue_ts", true),
            ts_field("local_ts", false),
            Field::new("source", DataType::UInt16, false),
        ]
    }

    pub fn take_arrays(&mut self) -> Vec<ArrayRef> {
        vec![
            Arc::new(StringArray::from(std::mem::take(&mut self.instrument))),
            ts_array_opt(std::mem::take(&mut self.venue_ts)),
            ts_array(std::mem::take(&mut self.local_ts)),
            Arc::new(UInt16Array::from(std::mem::take(&mut self.source))),
        ]
    }
}

pub(crate) struct BookTickerTable {
    pub(crate) env: EnvelopeCols,
    pub(crate) update_id: Vec<u64>,
    pub(crate) bid_price: Vec<Option<f64>>,
    pub(crate) bid_qty: Vec<Option<f64>>,
    pub(crate) ask_price: Vec<Option<f64>>,
    pub(crate) ask_qty: Vec<Option<f64>>,
    writer: TableWriter,
}

impl BookTickerTable {
    pub(crate) fn new(dir: &Path) -> Self {
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

    pub(crate) fn maybe_flush(&mut self) -> Result<()> {
        if self.env.len() >= BATCH_ROWS {
            self.flush()?;
        }
        Ok(())
    }

    pub(crate) fn finish(mut self) -> Result<usize> {
        self.flush()?;
        self.writer.close()
    }
}

pub(crate) struct TradeTable {
    pub(crate) env: EnvelopeCols,
    pub(crate) trade_id: Vec<String>,
    pub(crate) price: Vec<Option<f64>>,
    pub(crate) qty: Vec<Option<f64>>,
    pub(crate) side: Vec<&'static str>,
    pub(crate) kind: Vec<Option<String>>,
    writer: TableWriter,
}

impl TradeTable {
    pub(crate) fn new(dir: &Path) -> Self {
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

    pub(crate) fn maybe_flush(&mut self) -> Result<()> {
        if self.env.len() >= BATCH_ROWS {
            self.flush()?;
        }
        Ok(())
    }

    pub(crate) fn finish(mut self) -> Result<usize> {
        self.flush()?;
        self.writer.close()
    }
}

pub(crate) struct SinglePriceTable {
    pub(crate) env: EnvelopeCols,
    pub(crate) price: Vec<Option<f64>>,
    writer: TableWriter,
}

impl SinglePriceTable {
    pub(crate) fn new(dir: &Path, file_name: &str) -> Self {
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

    pub(crate) fn maybe_flush(&mut self) -> Result<()> {
        if self.env.len() >= BATCH_ROWS {
            self.flush()?;
        }
        Ok(())
    }

    pub(crate) fn finish(mut self) -> Result<usize> {
        self.flush()?;
        self.writer.close()
    }
}

pub(crate) struct FundingPredictionTable {
    pub(crate) env: EnvelopeCols,
    pub(crate) rate: Vec<Option<f64>>,
    pub(crate) next_funding_time: Vec<i64>,
    pub(crate) interval_ns: Vec<Option<u64>>,
    pub(crate) premium_index: Vec<Option<f64>>,
    pub(crate) clamp_min: Vec<Option<f64>>,
    pub(crate) clamp_max: Vec<Option<f64>>,
    writer: TableWriter,
}

impl FundingPredictionTable {
    pub(crate) fn new(dir: &Path) -> Self {
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

    pub(crate) fn maybe_flush(&mut self) -> Result<()> {
        if self.env.len() >= BATCH_ROWS {
            self.flush()?;
        }
        Ok(())
    }

    pub(crate) fn finish(mut self) -> Result<usize> {
        self.flush()?;
        self.writer.close()
    }
}

/// Realized funding settlements. Public: the backfill crate writes REST
/// history through the same schema (`source` 0 + the backfill root carry the
/// provenance).
pub struct FundingRealizedTable {
    env: EnvelopeCols,
    rate: Vec<Option<f64>>,
    funding_time: Vec<i64>,
    interval_ns: Vec<Option<u64>>,
    writer: TableWriter,
}

impl FundingRealizedTable {
    pub fn new(dir: &Path) -> Self {
        Self::with_file_name(dir, "funding_rate_realized.parquet")
    }

    pub fn with_file_name(dir: &Path, file_name: &str) -> Self {
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
            writer: TableWriter::new(dir, file_name, Schema::new(fields)),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn push_row(
        &mut self,
        instrument: &str,
        venue_ts: Option<Nanos>,
        local_ts: Nanos,
        source: SourceId,
        rate: &Decimal,
        funding_time: Nanos,
        interval_ns: Option<u64>,
    ) -> Result<()> {
        self.env.push(instrument, venue_ts, local_ts, source);
        self.rate.push(dec_opt(rate, "rate", instrument));
        self.funding_time.push(funding_time as i64);
        self.interval_ns.push(interval_ns);
        self.maybe_flush()
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

    pub fn maybe_flush(&mut self) -> Result<()> {
        if self.env.len() >= BATCH_ROWS {
            self.flush()?;
        }
        Ok(())
    }

    pub fn finish(mut self) -> Result<usize> {
        self.flush()?;
        self.writer.close()
    }
}

/// Snapshot rows: one row per level. `level_idx` is meaningful here (rank in
/// the snapshot); update rows deliberately have no such column (D4).
pub(crate) struct BookSnapshotTable {
    pub(crate) env: EnvelopeCols,
    pub(crate) last_update_id: Vec<u64>,
    pub(crate) side: Vec<&'static str>,
    pub(crate) level_idx: Vec<u32>,
    pub(crate) price: Vec<Option<f64>>,
    pub(crate) qty: Vec<Option<f64>>,
    writer: TableWriter,
}

impl BookSnapshotTable {
    pub(crate) fn new(dir: &Path) -> Self {
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

    pub(crate) fn maybe_flush(&mut self) -> Result<()> {
        if self.env.len() >= BATCH_ROWS {
            self.flush()?;
        }
        Ok(())
    }

    pub(crate) fn finish(mut self) -> Result<usize> {
        self.flush()?;
        self.writer.close()
    }
}

/// Update rows: one row per changed level. No `level_idx` — diff entries have
/// no rank (D4); ordering/splicing runs on the update-id columns.
pub(crate) struct BookUpdateTable {
    pub(crate) env: EnvelopeCols,
    pub(crate) first_update_id: Vec<u64>,
    pub(crate) final_update_id: Vec<u64>,
    pub(crate) prev_final_update_id: Vec<Option<u64>>,
    pub(crate) event_time: Vec<Option<i64>>,
    pub(crate) side: Vec<&'static str>,
    pub(crate) price: Vec<Option<f64>>,
    pub(crate) qty: Vec<Option<f64>>,
    writer: TableWriter,
}

impl BookUpdateTable {
    pub(crate) fn new(dir: &Path) -> Self {
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

    pub(crate) fn maybe_flush(&mut self) -> Result<()> {
        if self.env.len() >= BATCH_ROWS {
            self.flush()?;
        }
        Ok(())
    }

    pub(crate) fn finish(mut self) -> Result<usize> {
        self.flush()?;
        self.writer.close()
    }
}

pub(crate) struct LiquidationTable {
    pub(crate) env: EnvelopeCols,
    pub(crate) side: Vec<&'static str>,
    pub(crate) price: Vec<Option<f64>>,
    pub(crate) qty: Vec<Option<f64>>,
    pub(crate) filled_qty: Vec<Option<f64>>,
    pub(crate) avg_price: Vec<Option<f64>>,
    pub(crate) order_status: Vec<Option<String>>,
    writer: TableWriter,
}

impl LiquidationTable {
    pub(crate) fn new(dir: &Path) -> Self {
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

    pub(crate) fn maybe_flush(&mut self) -> Result<()> {
        if self.env.len() >= BATCH_ROWS {
            self.flush()?;
        }
        Ok(())
    }

    pub(crate) fn finish(mut self) -> Result<usize> {
        self.flush()?;
        self.writer.close()
    }
}

/// Open-interest observations. Public for the backfill crate (the
/// `openInterestHist` history carries the notional value column the live
/// endpoint lacks).
pub struct OpenInterestTable {
    env: EnvelopeCols,
    open_interest: Vec<Option<f64>>,
    open_interest_value: Vec<Option<f64>>,
    writer: TableWriter,
}

impl OpenInterestTable {
    pub fn new(dir: &Path) -> Self {
        Self::with_file_name(dir, "open_interest.parquet")
    }

    pub fn with_file_name(dir: &Path, file_name: &str) -> Self {
        let mut fields = EnvelopeCols::fields();
        fields.extend([
            Field::new("open_interest", DataType::Float64, true),
            Field::new("open_interest_value", DataType::Float64, true),
        ]);
        Self {
            env: EnvelopeCols::default(),
            open_interest: Vec::new(),
            open_interest_value: Vec::new(),
            writer: TableWriter::new(dir, file_name, Schema::new(fields)),
        }
    }

    pub fn push_row(
        &mut self,
        instrument: &str,
        venue_ts: Option<Nanos>,
        local_ts: Nanos,
        source: SourceId,
        open_interest: &Decimal,
        open_interest_value: Option<&Decimal>,
    ) -> Result<()> {
        self.env.push(instrument, venue_ts, local_ts, source);
        self.open_interest
            .push(dec_opt(open_interest, "open_interest", instrument));
        self.open_interest_value
            .push(open_interest_value.and_then(|d| dec_opt(d, "open_interest_value", instrument)));
        self.maybe_flush()
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

    pub fn maybe_flush(&mut self) -> Result<()> {
        if self.env.len() >= BATCH_ROWS {
            self.flush()?;
        }
        Ok(())
    }

    pub fn finish(mut self) -> Result<usize> {
        self.flush()?;
        self.writer.close()
    }
}

/// Control events keep their full payload as JSON in `detail`; `instrument`
/// is nullable because most control events are venue- or connection-scoped.
pub(crate) struct ControlTable {
    instrument: Vec<Option<String>>,
    venue_ts: Vec<Option<i64>>,
    local_ts: Vec<i64>,
    source: Vec<u16>,
    kind: Vec<&'static str>,
    detail: Vec<String>,
    writer: TableWriter,
}

impl ControlTable {
    pub(crate) fn new(dir: &Path) -> Self {
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

    pub(crate) fn push(
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

    pub(crate) fn maybe_flush(&mut self) -> Result<()> {
        if self.instrument.len() >= BATCH_ROWS {
            self.flush()?;
        }
        Ok(())
    }

    pub(crate) fn finish(mut self) -> Result<usize> {
        self.flush()?;
        self.writer.close()
    }
}

/// Reference (instrument lifecycle) events, ControlTable-shaped: typed `kind`
/// for filtering, full payload as JSON `detail` for forensics. Research joins
/// run against the instruments SCD, not this table.
pub(crate) struct ReferenceTable {
    instrument: Vec<Option<String>>,
    venue_ts: Vec<Option<i64>>,
    local_ts: Vec<i64>,
    source: Vec<u16>,
    kind: Vec<&'static str>,
    detail: Vec<String>,
    writer: TableWriter,
}

impl ReferenceTable {
    pub(crate) fn new(dir: &Path) -> Self {
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
            writer: TableWriter::new(dir, "reference.parquet", Schema::new(fields)),
        }
    }

    pub(crate) fn push(
        &mut self,
        instrument: Option<&str>,
        venue_ts: Option<u64>,
        local_ts: u64,
        source: SourceId,
        reference: &ReferencePayload,
    ) {
        let kind = match reference {
            ReferencePayload::InstrumentAdded { .. } => "instrument_added",
            ReferencePayload::InstrumentChanged { .. } => "instrument_changed",
            ReferencePayload::InstrumentDelisted { .. } => "instrument_delisted",
            ReferencePayload::MarketResolved { .. } => "market_resolved",
        };
        self.instrument.push(instrument.map(str::to_string));
        self.venue_ts.push(venue_ts.map(|v| v as i64));
        self.local_ts.push(local_ts as i64);
        self.source.push(source.0);
        self.kind.push(kind);
        self.detail
            .push(serde_json::to_string(reference).unwrap_or_else(|_| format!("{reference:?}")));
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

    pub(crate) fn maybe_flush(&mut self) -> Result<()> {
        if self.instrument.len() >= BATCH_ROWS {
            self.flush()?;
        }
        Ok(())
    }

    pub(crate) fn finish(mut self) -> Result<usize> {
        self.flush()?;
        self.writer.close()
    }
}
