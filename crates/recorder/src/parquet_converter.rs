use std::fs::{self, File};
use std::io::BufReader;
use std::path::Path;

use venue_core::{MarketPayload, Payload};

use crate::tables::{
    aggressor_str, dec_opt, BookSnapshotTable, BookTickerTable, BookUpdateTable, ControlTable,
    FundingPredictionTable, FundingRealizedTable, LiquidationTable, OpenInterestTable,
    ReferenceTable, SinglePriceTable, TradeTable,
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

/// Conversion fails outright if more than this fraction of the WAL byte
/// stream had to be skipped as corrupt (P1): a file that damaged is an
/// operational incident, not something to silently log through.
const MAX_SKIPPED_RATIO: f64 = 0.01;

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
    let mut reference = ReferenceTable::new(output_dir);

    let mut skipped_no_instrument = 0u64;
    let mut chain_events = 0u64;
    let mut account_events = 0u64;

    while let Some(event) = reader.next_event()? {
        // Control and reference events are routed even without an instrument;
        // market events without one are malformed and skipped with a count (N3).
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
        if let Payload::Reference(r) = &event.payload {
            reference.push(
                event.instrument.as_ref().map(|i| i.value.as_ref()),
                event.venue_ts,
                event.local_ts,
                event.source,
                r,
            );
            reference.maybe_flush()?;
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
                    funding_real.push_row(
                        &instrument,
                        venue_ts,
                        local_ts,
                        source,
                        rate,
                        *funding_time,
                        *interval,
                    )?;
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
                    open_interest.push_row(
                        &instrument,
                        venue_ts,
                        local_ts,
                        source,
                        oi,
                        open_interest_value.as_ref(),
                    )?;
                }
            },
            // No producers yet; counted so growth is visible in conversion logs.
            Payload::Chain(_) => chain_events += 1,
            Payload::Account(_) => account_events += 1,
            Payload::Reference(_) | Payload::Control(_) => {
                unreachable!("reference/control events routed above")
            }
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
    if chain_events + account_events > 0 {
        tracing::info!(
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
    reference.finish()?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, TimeUnit};
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
        assert!(!out_dir.join("reference.parquet").exists());
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

    #[test]
    fn reference_events_convert_to_typed_rows() {
        let tmp = tempfile::tempdir().unwrap();
        let wal_path = tmp.path().join("test.wal");
        let out_dir = tmp.path().join("out");

        let instrument = || Instrument {
            id: InstrumentId {
                value: "newusdt".into(),
            },
            class: InstrumentClass::Perp,
            base: Asset("NEW".into()),
            quote: Asset("USDT".into()),
            tick_size: Some(dec!(0.001)),
            lot_size: Some(dec!(1)),
            min_notional: Some(dec!(5)),
            contract_multiplier: None,
            settle_ccy: Some(Asset("USDT".into())),
            linearity: Some(Linearity::Linear),
            funding_interval: Some(8 * 3600 * 1_000_000_000),
            lifecycle: LifecycleState::Trading,
        };
        let mut events = vec![
            base_event(
                0,
                Payload::Reference(ReferencePayload::InstrumentAdded {
                    instrument: instrument(),
                }),
            ),
            base_event(
                1,
                Payload::Reference(ReferencePayload::InstrumentChanged {
                    instrument: instrument(),
                }),
            ),
            base_event(
                2,
                Payload::Reference(ReferencePayload::InstrumentDelisted {
                    instrument_id: InstrumentId {
                        value: "newusdt".into(),
                    },
                }),
            ),
            base_event(
                3,
                Payload::Reference(ReferencePayload::MarketResolved {
                    outcome: "yes".into(),
                }),
            ),
        ];
        for e in &mut events {
            e.instrument = Some(InstrumentId {
                value: "newusdt".into(),
            });
        }
        write_wal(&events, &wal_path);

        convert_wal(&wal_path, &out_dir).unwrap();

        let file = File::open(out_dir.join("reference.parquet")).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        assert_eq!(builder.metadata().file_metadata().num_rows(), 4);
        let batch = builder.build().unwrap().next().unwrap().unwrap();
        let kinds: Vec<_> = batch
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .map(|v| v.unwrap().to_string())
            .collect();
        assert_eq!(
            kinds,
            [
                "instrument_added",
                "instrument_changed",
                "instrument_delisted",
                "market_resolved"
            ]
        );
        // detail keeps the full payload: the SCD-relevant fields survive.
        let detail = batch
            .column(5)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0);
        assert!(detail.contains("tick_size"), "{detail}");
    }
}
