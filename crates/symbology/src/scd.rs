//! Instruments SCD (A11): one row per (symbol, change interval) with
//! `valid_from`/`valid_to` at day resolution, rebuilt deterministically from
//! the accumulated daily raw dumps (`<date>-exchangeInfo.json` +
//! `<date>-fundingInfo.json` under `data/meta/binance/`). Point-in-time
//! joins answer "what was X's tick size / funding interval on date D".
//!
//! Intra-day changes are invisible to a daily-dump SCD by construction; the
//! live universe manager's `reference.parquet` records those from now on.
//! Binance-only today — the Bybit input is a different dump schema and no
//! research question needs it yet.

use crate::SymbologyError;
use arrow::array::{ArrayRef, Float64Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use chrono::NaiveDate;
use recorder::tables::{ts_array, ts_array_opt, ts_field, TableWriter};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub const SCD_FILE: &str = "binance.parquet";

/// Normalized per-symbol snapshot for one dump day; equality defines
/// "changed". Sizes as f64 (analytics tables convention).
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Snapshot {
    status: String,
    lifecycle: &'static str,
    class: &'static str,
    base: String,
    quote: String,
    settle: Option<String>,
    tick_size: Option<f64>,
    lot_size: Option<f64>,
    min_notional: Option<f64>,
    funding_interval_ns: Option<u64>,
    onboard_ms: Option<u64>,
}

#[derive(Debug)]
pub struct ScdRow {
    pub symbol: String,
    pub valid_from: u64,
    pub valid_to: Option<u64>,
    pub(crate) snap: Snapshot,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExchangeInfo {
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
    #[serde(default)]
    onboard_date: Option<u64>,
    #[serde(default)]
    filters: Vec<serde_json::Value>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct FundingInfoEntry {
    symbol: String,
    #[serde(default)]
    funding_interval_hours: Option<u64>,
}

fn filter_f64(filters: &[serde_json::Value], filter_type: &str, key: &str) -> Option<f64> {
    filters
        .iter()
        .find(|f| f.get("filterType").and_then(|v| v.as_str()) == Some(filter_type))
        .and_then(|f| f.get(key))
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse().ok())
}

fn lifecycle_str(status: &str) -> &'static str {
    match status {
        "TRADING" => "trading",
        "PENDING_TRADING" => "pending_trading",
        "DELIVERED" | "CLOSE" => "delisted",
        _ => "halted",
    }
}

const DEFAULT_FUNDING_INTERVAL_NS: u64 = 8 * 3600 * 1_000_000_000;

/// Parse one day's dumps into symbol → snapshot. `funding` is that day's
/// interval overrides (carried forward by the caller when a day lacks a
/// fundingInfo dump).
pub(crate) fn day_snapshots(
    exchange_info: &str,
    funding: &BTreeMap<String, u64>,
) -> Result<BTreeMap<String, Snapshot>, SymbologyError> {
    let info: ExchangeInfo =
        serde_json::from_str(exchange_info).map_err(|e| SymbologyError::Parse(e.to_string()))?;
    Ok(info
        .symbols
        .into_iter()
        .map(|s| {
            let is_perp = s.contract_type == "PERPETUAL";
            let snap = Snapshot {
                lifecycle: lifecycle_str(&s.status),
                status: s.status,
                class: if is_perp { "perp" } else { "future" },
                base: s.base_asset,
                quote: s.quote_asset,
                settle: s.margin_asset,
                tick_size: filter_f64(&s.filters, "PRICE_FILTER", "tickSize"),
                lot_size: filter_f64(&s.filters, "LOT_SIZE", "stepSize"),
                min_notional: filter_f64(&s.filters, "MIN_NOTIONAL", "notional"),
                funding_interval_ns: is_perp.then(|| {
                    funding
                        .get(&s.symbol)
                        .copied()
                        .unwrap_or(DEFAULT_FUNDING_INTERVAL_NS)
                }),
                onboard_ms: s.onboard_date,
            };
            (s.symbol.to_lowercase(), snap)
        })
        .collect())
}

fn parse_funding(body: &str) -> Result<BTreeMap<String, u64>, SymbologyError> {
    let entries: Vec<FundingInfoEntry> =
        serde_json::from_str(body).map_err(|e| SymbologyError::Parse(e.to_string()))?;
    Ok(entries
        .into_iter()
        .filter_map(|e| {
            e.funding_interval_hours
                .map(|h| (e.symbol, h * 3600 * 1_000_000_000))
        })
        .collect())
}

fn day_ns(date: NaiveDate) -> u64 {
    date.and_hms_opt(0, 0, 0)
        .expect("midnight exists")
        .and_utc()
        .timestamp_nanos_opt()
        .unwrap_or_default() as u64
}

/// Fold day snapshots (ascending) into SCD intervals: a change or
/// disappearance closes the open interval at that day's midnight.
pub(crate) fn fold_intervals(days: Vec<(NaiveDate, BTreeMap<String, Snapshot>)>) -> Vec<ScdRow> {
    let mut open: BTreeMap<String, ScdRow> = BTreeMap::new();
    let mut closed: Vec<ScdRow> = Vec::new();
    for (date, snapshots) in days {
        let ts = day_ns(date);
        // Disappearance = interval ends.
        let gone: Vec<String> = open
            .keys()
            .filter(|k| !snapshots.contains_key(*k))
            .cloned()
            .collect();
        for key in gone {
            let mut row = open.remove(&key).expect("key from open set");
            row.valid_to = Some(ts);
            closed.push(row);
        }
        for (symbol, snap) in snapshots {
            match open.get_mut(&symbol) {
                None => {
                    open.insert(
                        symbol.clone(),
                        ScdRow {
                            symbol,
                            valid_from: ts,
                            valid_to: None,
                            snap,
                        },
                    );
                }
                Some(row) if row.snap != snap => {
                    let mut finished = open.remove(&symbol).expect("present");
                    finished.valid_to = Some(ts);
                    closed.push(finished);
                    open.insert(
                        symbol.clone(),
                        ScdRow {
                            symbol,
                            valid_from: ts,
                            valid_to: None,
                            snap,
                        },
                    );
                }
                Some(_) => {}
            }
        }
    }
    closed.extend(open.into_values());
    closed.sort_by(|a, b| (&a.symbol, a.valid_from).cmp(&(&b.symbol, b.valid_from)));
    closed
}

/// Collect `<date><suffix>` dump bodies, ascending by date.
fn dated_dumps(dir: &Path, suffix: &str) -> Vec<(NaiveDate, PathBuf)> {
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return out;
    };
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        if let Some(date) = name
            .strip_suffix(suffix)
            .and_then(|d| d.parse::<NaiveDate>().ok())
        {
            out.push((date, entry.path()));
        }
    }
    out.sort();
    out
}

pub struct ScdSummary {
    pub days: usize,
    pub rows: usize,
    pub path: PathBuf,
}

/// Deterministic full rebuild from every dump under `data/meta/binance/`.
pub fn build_scd(data_dir: &Path) -> Result<ScdSummary, SymbologyError> {
    let venue_meta = data_dir.join("meta/binance");
    let exchange_dumps = dated_dumps(&venue_meta, "-exchangeInfo.json");
    if exchange_dumps.is_empty() {
        return Err(SymbologyError::Invalid(
            "no exchangeInfo dumps under data/meta/binance — nothing to fold".into(),
        ));
    }
    let funding_dumps: BTreeMap<NaiveDate, PathBuf> = dated_dumps(&venue_meta, "-fundingInfo.json")
        .into_iter()
        .collect();

    let mut funding: BTreeMap<String, u64> = BTreeMap::new();
    let mut days = Vec::new();
    for (date, path) in &exchange_dumps {
        if let Some(funding_path) = funding_dumps.get(date) {
            funding = parse_funding(&std::fs::read_to_string(funding_path)?)?;
        } // else: carry the previous day's intervals forward
        let body = std::fs::read_to_string(path)?;
        match day_snapshots(&body, &funding) {
            Ok(snaps) => days.push((*date, snaps)),
            // One corrupt dump must not sink the whole SCD; skip loudly.
            Err(e) => tracing::warn!(%date, error = %e, "unparseable exchangeInfo dump skipped"),
        }
    }
    let day_count = days.len();
    let rows = fold_intervals(days);

    let out_dir = data_dir.join("meta/instruments");
    std::fs::create_dir_all(&out_dir)?;
    write_scd(&out_dir, &rows)?;
    tracing::info!(days = day_count, rows = rows.len(), "instruments SCD built");
    Ok(ScdSummary {
        days: day_count,
        rows: rows.len(),
        path: out_dir.join(SCD_FILE),
    })
}

fn scd_schema() -> Schema {
    Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("status", DataType::Utf8, false),
        Field::new("lifecycle", DataType::Utf8, false),
        Field::new("class", DataType::Utf8, false),
        Field::new("base", DataType::Utf8, false),
        Field::new("quote", DataType::Utf8, false),
        Field::new("settle", DataType::Utf8, true),
        Field::new("tick_size", DataType::Float64, true),
        Field::new("lot_size", DataType::Float64, true),
        Field::new("min_notional", DataType::Float64, true),
        Field::new("funding_interval_ns", DataType::UInt64, true),
        ts_field("onboard_ts", true),
        ts_field("valid_from", false),
        ts_field("valid_to", true),
    ])
}

fn write_scd(out_dir: &Path, rows: &[ScdRow]) -> Result<(), SymbologyError> {
    let tmp_name = format!(".tmp-{SCD_FILE}");
    let mut writer = TableWriter::new(out_dir, &tmp_name, scd_schema());
    let cols: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(
            rows.iter().map(|r| r.symbol.as_str()).collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|r| r.snap.status.as_str())
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter().map(|r| r.snap.lifecycle).collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter().map(|r| r.snap.class).collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|r| r.snap.base.as_str())
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|r| r.snap.quote.as_str())
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|r| r.snap.settle.as_deref())
                .collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            rows.iter().map(|r| r.snap.tick_size).collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            rows.iter().map(|r| r.snap.lot_size).collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            rows.iter().map(|r| r.snap.min_notional).collect::<Vec<_>>(),
        )),
        Arc::new(UInt64Array::from(
            rows.iter()
                .map(|r| r.snap.funding_interval_ns)
                .collect::<Vec<_>>(),
        )),
        ts_array_opt(
            rows.iter()
                .map(|r| r.snap.onboard_ms.map(|ms| (ms * 1_000_000) as i64))
                .collect(),
        ),
        ts_array(rows.iter().map(|r| r.valid_from as i64).collect()),
        ts_array_opt(rows.iter().map(|r| r.valid_to.map(|v| v as i64)).collect()),
    ];
    writer
        .write_batch(cols)
        .map_err(|e| SymbologyError::Parse(e.to_string()))?;
    writer
        .close()
        .map_err(|e| SymbologyError::Parse(e.to_string()))?;
    std::fs::rename(out_dir.join(&tmp_name), out_dir.join(SCD_FILE))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dump(symbols: &[(&str, &str, &str)]) -> String {
        // (symbol, status, tickSize)
        let entries: Vec<String> = symbols
            .iter()
            .map(|(sym, status, tick)| {
                format!(
                    r#"{{"symbol":"{sym}","contractType":"PERPETUAL","baseAsset":"{}","quoteAsset":"USDT","marginAsset":"USDT","status":"{status}","onboardDate":1700000000000,"filters":[{{"filterType":"PRICE_FILTER","tickSize":"{tick}"}},{{"filterType":"LOT_SIZE","stepSize":"1"}},{{"filterType":"MIN_NOTIONAL","notional":"5"}}]}}"#,
                    sym.trim_end_matches("USDT")
                )
            })
            .collect();
        format!(r#"{{"symbols":[{}]}}"#, entries.join(","))
    }

    fn date(s: &str) -> NaiveDate {
        s.parse().unwrap()
    }

    #[test]
    fn three_day_sequence_produces_exact_intervals() {
        // Day 1: A lists. Day 2: A's tick changes, B lists.
        // Day 3: A delists (disappears), B settles (status change).
        let funding = BTreeMap::new();
        let days = vec![
            (
                date("2026-06-01"),
                day_snapshots(&dump(&[("AUSDT", "TRADING", "0.1")]), &funding).unwrap(),
            ),
            (
                date("2026-06-02"),
                day_snapshots(
                    &dump(&[("AUSDT", "TRADING", "0.01"), ("BUSDT", "TRADING", "1")]),
                    &funding,
                )
                .unwrap(),
            ),
            (
                date("2026-06-03"),
                day_snapshots(&dump(&[("BUSDT", "SETTLING", "1")]), &funding).unwrap(),
            ),
        ];
        let rows = fold_intervals(days);
        type Summary<'a> = (&'a str, u64, Option<u64>, Option<f64>, &'a str);
        let by: Vec<Summary> = rows
            .iter()
            .map(|r| {
                (
                    r.symbol.as_str(),
                    r.valid_from,
                    r.valid_to,
                    r.snap.tick_size,
                    r.snap.lifecycle,
                )
            })
            .collect();
        let d1 = day_ns(date("2026-06-01"));
        let d2 = day_ns(date("2026-06-02"));
        let d3 = day_ns(date("2026-06-03"));
        assert_eq!(
            by,
            vec![
                ("ausdt", d1, Some(d2), Some(0.1), "trading"),
                ("ausdt", d2, Some(d3), Some(0.01), "trading"),
                ("busdt", d2, Some(d3), Some(1.0), "trading"),
                ("busdt", d3, None, Some(1.0), "halted"),
            ]
        );
    }

    #[test]
    fn funding_interval_changes_open_new_interval_and_carry_forward() {
        let mut funding_day1 = BTreeMap::new();
        funding_day1.insert("AUSDT".to_string(), 4 * 3600 * 1_000_000_000_u64);
        let body = dump(&[("AUSDT", "TRADING", "0.1")]);
        let days = vec![
            (
                date("2026-06-01"),
                day_snapshots(&body, &funding_day1).unwrap(),
            ),
            // Day 2 has no fundingInfo dump: caller carries day 1 forward.
            (
                date("2026-06-02"),
                day_snapshots(&body, &funding_day1).unwrap(),
            ),
            // Day 3: symbol moved to the 8h default (absent from fundingInfo).
            (
                date("2026-06-03"),
                day_snapshots(&body, &BTreeMap::new()).unwrap(),
            ),
        ];
        let rows = fold_intervals(days);
        assert_eq!(rows.len(), 2, "carry-forward day created no interval");
        assert_eq!(
            rows[0].snap.funding_interval_ns,
            Some(4 * 3600 * 1_000_000_000)
        );
        assert_eq!(rows[0].valid_to, Some(day_ns(date("2026-06-03"))));
        assert_eq!(
            rows[1].snap.funding_interval_ns,
            Some(8 * 3600 * 1_000_000_000)
        );
        assert_eq!(rows[1].valid_to, None);
    }

    #[test]
    fn build_scd_end_to_end_with_point_in_time_answer() {
        let tmp = tempfile::tempdir().unwrap();
        let meta = tmp.path().join("meta/binance");
        std::fs::create_dir_all(&meta).unwrap();
        std::fs::write(
            meta.join("2026-06-01-exchangeInfo.json"),
            dump(&[("XUSDT", "TRADING", "0.5")]),
        )
        .unwrap();
        std::fs::write(
            meta.join("2026-06-01-fundingInfo.json"),
            r#"[{"symbol":"XUSDT","fundingIntervalHours":4}]"#,
        )
        .unwrap();
        std::fs::write(
            meta.join("2026-06-02-exchangeInfo.json"),
            dump(&[("XUSDT", "TRADING", "0.5")]),
        )
        .unwrap();
        std::fs::write(
            meta.join("2026-06-02-fundingInfo.json"),
            r#"[]"#, // back to the 8h default
        )
        .unwrap();

        let summary = build_scd(tmp.path()).unwrap();
        assert_eq!(summary.days, 2);
        assert_eq!(summary.rows, 2);

        // Point-in-time: "what was X's funding interval on 06-01?" → 4h.
        let file = std::fs::File::open(&summary.path).unwrap();
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let batch = reader.into_iter().next().unwrap().unwrap();
        let intervals = batch
            .column(10)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(intervals.value(0), 4 * 3600 * 1_000_000_000);
        assert_eq!(intervals.value(1), 8 * 3600 * 1_000_000_000);
    }
}
