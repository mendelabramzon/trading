//! 1 h kline backfill: price context for research (funding is a rate; spread
//! economics need a notional). Month-partitioned like funding; one request
//! per perp per month (≤ 744 hourly rows < the 1000 limit). Klines never
//! transit the WAL, so their table lives here, built from the shared
//! envelope/writer pieces so the lake stays schema-uniform.

use crate::{now_nanos, publish, tmp_path, BackfillError, Month, PerpMeta};
use arrow::array::{ArrayRef, Float64Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use recorder::tables::{dec_opt, ts_array, EnvelopeCols, TableWriter};
use rust_decimal::Decimal;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use venue_core::SourceId;

#[derive(Debug, Clone)]
pub struct Kline {
    pub open_time_ms: u64,
    pub close_time_ms: u64,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: Decimal,
    pub quote_volume: Decimal,
    pub trades: u64,
    pub taker_buy_volume: Decimal,
    pub taker_buy_quote_volume: Decimal,
}

pub trait KlineSource {
    fn venue(&self) -> &'static str;

    fn list_perps(
        &self,
        meta_dir: &Path,
    ) -> impl Future<Output = Result<Vec<PerpMeta>, BackfillError>> + Send;

    /// 1 h klines with open_time in [start_ms, end_ms).
    fn fetch_klines_1h(
        &self,
        symbol: &str,
        start_ms: u64,
        end_ms: u64,
    ) -> impl Future<Output = Result<Vec<Kline>, BackfillError>> + Send;
}

pub struct KlinesCfg {
    pub out_root: PathBuf,
    pub meta_root: PathBuf,
    /// Required: full-universe kline history is a deliberate, sized choice.
    pub from: Month,
}

/// `venue_ts` = open time (the bar's identity); close time is a column.
struct KlineTable {
    env: EnvelopeCols,
    close_time: Vec<i64>,
    open: Vec<Option<f64>>,
    high: Vec<Option<f64>>,
    low: Vec<Option<f64>>,
    close: Vec<Option<f64>>,
    volume: Vec<Option<f64>>,
    quote_volume: Vec<Option<f64>>,
    trades: Vec<u64>,
    taker_buy_volume: Vec<Option<f64>>,
    taker_buy_quote_volume: Vec<Option<f64>>,
    writer: TableWriter,
}

impl KlineTable {
    fn new(dir: &Path, file_name: &str) -> Self {
        let mut fields = EnvelopeCols::fields();
        fields.extend([
            recorder::tables::ts_field("close_time", false),
            Field::new("open", DataType::Float64, true),
            Field::new("high", DataType::Float64, true),
            Field::new("low", DataType::Float64, true),
            Field::new("close", DataType::Float64, true),
            Field::new("volume", DataType::Float64, true),
            Field::new("quote_volume", DataType::Float64, true),
            Field::new("trades", DataType::UInt64, false),
            Field::new("taker_buy_volume", DataType::Float64, true),
            Field::new("taker_buy_quote_volume", DataType::Float64, true),
        ]);
        Self {
            env: EnvelopeCols::default(),
            close_time: Vec::new(),
            open: Vec::new(),
            high: Vec::new(),
            low: Vec::new(),
            close: Vec::new(),
            volume: Vec::new(),
            quote_volume: Vec::new(),
            trades: Vec::new(),
            taker_buy_volume: Vec::new(),
            taker_buy_quote_volume: Vec::new(),
            writer: TableWriter::new(dir, file_name, Schema::new(fields)),
        }
    }

    fn push(&mut self, instrument: &str, local_ts: u64, k: &Kline) -> Result<(), BackfillError> {
        self.env.push(
            instrument,
            Some(k.open_time_ms * 1_000_000),
            local_ts,
            SourceId::REST,
        );
        self.close_time.push((k.close_time_ms * 1_000_000) as i64);
        self.open.push(dec_opt(&k.open, "open", instrument));
        self.high.push(dec_opt(&k.high, "high", instrument));
        self.low.push(dec_opt(&k.low, "low", instrument));
        self.close.push(dec_opt(&k.close, "close", instrument));
        self.volume.push(dec_opt(&k.volume, "volume", instrument));
        self.quote_volume
            .push(dec_opt(&k.quote_volume, "quote_volume", instrument));
        self.trades.push(k.trades);
        self.taker_buy_volume
            .push(dec_opt(&k.taker_buy_volume, "taker_buy_volume", instrument));
        self.taker_buy_quote_volume.push(dec_opt(
            &k.taker_buy_quote_volume,
            "taker_buy_quote_volume",
            instrument,
        ));
        if self.env.len() >= recorder::tables::BATCH_ROWS {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<(), BackfillError> {
        let mut cols = self.env.take_arrays();
        cols.push(ts_array(std::mem::take(&mut self.close_time)));
        for v in [
            &mut self.open,
            &mut self.high,
            &mut self.low,
            &mut self.close,
            &mut self.volume,
            &mut self.quote_volume,
        ] {
            cols.push(Arc::new(Float64Array::from(std::mem::take(v))) as ArrayRef);
        }
        cols.push(Arc::new(UInt64Array::from(std::mem::take(&mut self.trades))) as ArrayRef);
        for v in [&mut self.taker_buy_volume, &mut self.taker_buy_quote_volume] {
            cols.push(Arc::new(Float64Array::from(std::mem::take(v))) as ArrayRef);
        }
        self.writer
            .write_batch(cols)
            .map_err(|e| BackfillError::Table(e.to_string()))
    }

    fn finish(mut self) -> Result<usize, BackfillError> {
        self.flush()?;
        self.writer
            .close()
            .map_err(|e| BackfillError::Table(e.to_string()))
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum MonthOutcome {
    Published { rows: usize },
    Partial { rows: usize },
    AlreadyPublished,
    Empty,
}

pub async fn run<S: KlineSource>(
    src: &S,
    cfg: &KlinesCfg,
) -> Result<Vec<(Month, MonthOutcome)>, BackfillError> {
    let perps = src.list_perps(&cfg.meta_root).await?;
    tracing::info!(venue = src.venue(), perps = perps.len(), "perps listed");

    let dir = cfg.out_root.join(src.venue()).join("klines_1h");
    std::fs::create_dir_all(&dir)?;
    for entry in std::fs::read_dir(&dir)?.flatten() {
        if entry.file_name().to_string_lossy().starts_with(".tmp-") {
            std::fs::remove_file(entry.path())?;
        }
    }

    let current = Month::current();
    if cfg.from > current {
        return Err(BackfillError::Invalid(format!(
            "--from {} is in the future",
            cfg.from
        )));
    }
    let now_ms = now_nanos() / 1_000_000;
    let mut outcomes = Vec::new();

    for m in cfg.from.through(current) {
        let closed = m < current;
        let final_path = dir.join(format!("{m}.parquet"));
        let partial_path = dir.join(format!("{m}.partial.parquet"));
        if closed && final_path.exists() {
            outcomes.push((m, MonthOutcome::AlreadyPublished));
            continue;
        }
        let (start_ms, end_ms) = m.bounds_ms();
        let end_eff = end_ms.min(now_ms);

        let target = if closed { &final_path } else { &partial_path };
        let tmp = tmp_path(target);
        let file_name = tmp
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        let mut table = KlineTable::new(&dir, &file_name);
        let local_ts = now_nanos();
        let mut rows = 0usize;
        for perp in &perps {
            if perp.onboard_ms.is_some_and(|on| on >= end_eff) {
                continue;
            }
            let klines = src.fetch_klines_1h(&perp.symbol, start_ms, end_eff).await?;
            let symbol = perp.symbol.to_lowercase();
            for k in &klines {
                table.push(&symbol, local_ts, k)?;
                rows += 1;
            }
        }
        table.finish()?;

        if rows == 0 {
            outcomes.push((m, MonthOutcome::Empty));
            tracing::info!(venue = src.venue(), month = %m, "no klines in window");
            continue;
        }
        publish(&tmp, target)?;
        if closed && partial_path.exists() {
            std::fs::remove_file(&partial_path)?;
        }
        tracing::info!(
            venue = src.venue(),
            month = %m,
            rows,
            path = %target.display(),
            "kline month published"
        );
        outcomes.push((
            m,
            if closed {
                MonthOutcome::Published { rows }
            } else {
                MonthOutcome::Partial { rows }
            },
        ));
    }
    Ok(outcomes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use rust_decimal_macros::dec;

    struct MockKlines;

    impl KlineSource for MockKlines {
        fn venue(&self) -> &'static str {
            "mockx"
        }

        async fn list_perps(&self, _meta: &Path) -> Result<Vec<PerpMeta>, BackfillError> {
            Ok(vec![PerpMeta {
                symbol: "BTCUSDT".into(),
                onboard_ms: Some(0),
                funding_interval_ns: None,
            }])
        }

        async fn fetch_klines_1h(
            &self,
            _symbol: &str,
            start_ms: u64,
            _end_ms: u64,
        ) -> Result<Vec<Kline>, BackfillError> {
            Ok(vec![Kline {
                open_time_ms: start_ms,
                close_time_ms: start_ms + 3_599_999,
                open: dec!(100),
                high: dec!(110),
                low: dec!(90),
                close: dec!(105),
                volume: dec!(12.5),
                quote_volume: dec!(1300),
                trades: 42,
                taker_buy_volume: dec!(6),
                taker_buy_quote_volume: dec!(620),
            }])
        }
    }

    #[tokio::test]
    async fn months_publish_with_kline_schema() {
        let tmp = tempfile::tempdir().unwrap();
        let from = Month::current(); // single (current) month → partial
        let cfg = KlinesCfg {
            out_root: tmp.path().join("backfill"),
            meta_root: tmp.path().join("meta"),
            from,
        };
        let outcomes = run(&MockKlines, &cfg).await.unwrap();
        assert_eq!(outcomes.len(), 1);
        assert_eq!(outcomes[0].1, MonthOutcome::Partial { rows: 1 });

        let path = tmp
            .path()
            .join("backfill/mockx/klines_1h")
            .join(format!("{from}.partial.parquet"));
        let f = std::fs::File::open(&path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(f).unwrap();
        let names: Vec<_> = builder
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        assert_eq!(
            names,
            [
                "instrument",
                "venue_ts",
                "local_ts",
                "source",
                "close_time",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "quote_volume",
                "trades",
                "taker_buy_volume",
                "taker_buy_quote_volume"
            ]
        );
    }
}
