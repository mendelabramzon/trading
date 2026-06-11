//! Mapping builder: latest per-venue instrument dumps + curated overrides →
//! `data/meta/symbology/mapping.parquet` (+ a build-info sidecar with
//! cross-venue match coverage). Deterministic full rebuild every run, atomic
//! publish — derived data, the dumps stay truth.

use crate::{class_from_str, make_canonical, MappingRow, Registry, SymbologyError};
use arrow::array::{Array, ArrayRef, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema};
use recorder::tables::{ts_array, ts_array_opt, ts_field, TableWriter};
use serde::Deserialize;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use venue_core::Nanos;

pub const MAPPING_FILE: &str = "mapping.parquet";
pub const BUILD_INFO_FILE: &str = "mapping.build.json";

pub struct BuildCfg {
    pub data_dir: PathBuf,
    /// Curated exceptions; missing file = no overrides.
    pub overrides_path: PathBuf,
}

#[derive(Debug)]
pub struct BuildSummary {
    pub rows_per_venue: HashMap<String, usize>,
    pub matched_canonicals: usize,
    pub overridden: usize,
    pub excluded: usize,
    pub mapping_path: PathBuf,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OverridesFile {
    #[serde(default, rename = "override")]
    overrides: Vec<OverrideEntry>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OverrideEntry {
    venue: String,
    instrument: String,
    #[serde(default)]
    exclude: bool,
    #[serde(default)]
    base: Option<String>,
    #[serde(default)]
    quote: Option<String>,
    #[serde(default)]
    settle: Option<String>,
}

/// Latest dated dump `<YYYY-MM-DD><suffix>` in `dir` (dates sort
/// lexicographically).
fn latest_dump(dir: &Path, suffix: &str) -> Option<PathBuf> {
    let mut best: Option<String> = None;
    for entry in std::fs::read_dir(dir).ok()?.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.ends_with(suffix) && best.as_ref().is_none_or(|b| &name > b) {
            best = Some(name);
        }
    }
    best.map(|name| dir.join(name))
}

fn dump_date_ns(path: &Path) -> Nanos {
    let date = path
        .file_name()
        .and_then(|n| n.to_str())
        .and_then(|n| n.get(..10))
        .and_then(|d| d.parse::<chrono::NaiveDate>().ok())
        .unwrap_or_default();
    date.and_hms_opt(0, 0, 0)
        .expect("midnight exists")
        .and_utc()
        .timestamp_nanos_opt()
        .unwrap_or_default() as u64
}

// --- Binance exchangeInfo ---

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExchangeInfo {
    symbols: Vec<BinanceSymbol>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct BinanceSymbol {
    symbol: String,
    contract_type: String,
    base_asset: String,
    quote_asset: String,
    #[serde(default)]
    margin_asset: Option<String>,
    #[serde(default)]
    onboard_date: Option<u64>,
}

pub(crate) fn parse_binance(
    body: &str,
    fallback_from: Nanos,
) -> Result<Vec<MappingRow>, SymbologyError> {
    let info: ExchangeInfo =
        serde_json::from_str(body).map_err(|e| SymbologyError::Parse(e.to_string()))?;
    Ok(info
        .symbols
        .into_iter()
        .filter(|s| s.contract_type == "PERPETUAL")
        .map(|s| MappingRow {
            venue: "binance".into(),
            instrument: s.symbol.to_lowercase(),
            canonical: make_canonical(
                &s.base_asset,
                &s.quote_asset,
                s.margin_asset.as_deref().unwrap_or(&s.quote_asset),
            ),
            valid_from: s.onboard_date.map_or(fallback_from, |ms| ms * 1_000_000),
            valid_to: None,
            origin: "derived".into(),
        })
        .collect())
}

// --- Bybit instrumentsInfo (array of verbatim page bodies) ---

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct BybitPage {
    result: BybitResult,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct BybitResult {
    list: Vec<BybitSymbol>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct BybitSymbol {
    symbol: String,
    contract_type: String,
    base_coin: String,
    quote_coin: String,
    #[serde(default)]
    settle_coin: Option<String>,
    #[serde(default)]
    launch_time: Option<String>,
}

pub(crate) fn parse_bybit(
    body: &str,
    fallback_from: Nanos,
) -> Result<Vec<MappingRow>, SymbologyError> {
    let pages: Vec<BybitPage> =
        serde_json::from_str(body).map_err(|e| SymbologyError::Parse(e.to_string()))?;
    Ok(pages
        .into_iter()
        .flat_map(|p| p.result.list)
        .filter(|s| s.contract_type == "LinearPerpetual")
        .map(|s| MappingRow {
            venue: "bybit".into(),
            instrument: s.symbol.to_lowercase(),
            canonical: make_canonical(
                &s.base_coin,
                &s.quote_coin,
                s.settle_coin.as_deref().unwrap_or(&s.quote_coin),
            ),
            valid_from: s
                .launch_time
                .and_then(|t| t.parse::<u64>().ok())
                .map_or(fallback_from, |ms| ms * 1_000_000),
            valid_to: None,
            origin: "derived".into(),
        })
        .collect())
}

pub(crate) fn apply_overrides(
    mut rows: Vec<MappingRow>,
    overrides: &[OverrideEntry],
) -> (Vec<MappingRow>, usize, usize) {
    let by_key: HashMap<(String, String), &OverrideEntry> = overrides
        .iter()
        .map(|o| ((o.venue.clone(), o.instrument.to_lowercase()), o))
        .collect();
    let mut overridden = 0;
    let mut excluded = 0;
    rows.retain_mut(|row| {
        let Some(o) = by_key.get(&(row.venue.clone(), row.instrument.clone())) else {
            return true;
        };
        if o.exclude {
            excluded += 1;
            return false;
        }
        if let Some(base) = &o.base {
            row.canonical.base = venue_core::Asset(base.as_str().into());
        }
        if let Some(quote) = &o.quote {
            row.canonical.quote = venue_core::Asset(quote.as_str().into());
        }
        if let Some(settle) = &o.settle {
            row.canonical.settle = venue_core::Asset(settle.as_str().into());
        }
        row.origin = "override".into();
        overridden += 1;
        true
    });
    (rows, overridden, excluded)
}

fn load_overrides(path: &Path) -> Result<Vec<OverrideEntry>, SymbologyError> {
    let raw = match std::fs::read_to_string(path) {
        Ok(raw) => raw,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e.into()),
    };
    let file: OverridesFile =
        toml::from_str(&raw).map_err(|e| SymbologyError::Parse(e.to_string()))?;
    Ok(file.overrides)
}

pub fn build(cfg: &BuildCfg) -> Result<BuildSummary, SymbologyError> {
    let meta = cfg.data_dir.join("meta");
    let mut rows: Vec<MappingRow> = Vec::new();

    type DumpParser = fn(&str, Nanos) -> Result<Vec<MappingRow>, SymbologyError>;
    let inputs: [(&str, &str, DumpParser); 2] = [
        ("binance", "-exchangeInfo.json", parse_binance),
        ("bybit", "-instrumentsInfo.json", parse_bybit),
    ];
    let mut input_paths = Vec::new();
    for (venue, suffix, parse) in inputs {
        match latest_dump(&meta.join(venue), suffix) {
            Some(path) => {
                let body = std::fs::read_to_string(&path)?;
                let venue_rows = parse(&body, dump_date_ns(&path))?;
                tracing::info!(venue, rows = venue_rows.len(), input = %path.display(), "parsed dump");
                input_paths.push(path.display().to_string());
                rows.extend(venue_rows);
            }
            None => {
                tracing::warn!(venue, "no instrument dump found; venue absent from mapping");
            }
        }
    }
    if rows.is_empty() {
        return Err(SymbologyError::Invalid(
            "no instrument dumps found under data/meta — nothing to build".into(),
        ));
    }

    let overrides = load_overrides(&cfg.overrides_path)?;
    let (rows, overridden, excluded) = apply_overrides(rows, &overrides);

    let out_dir = meta.join("symbology");
    std::fs::create_dir_all(&out_dir)?;
    let mapping_path = out_dir.join(MAPPING_FILE);
    write_mapping(&out_dir, &rows)?;

    let mut rows_per_venue: HashMap<String, usize> = HashMap::new();
    for row in &rows {
        *rows_per_venue.entry(row.venue.clone()).or_default() += 1;
    }
    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default() as u64;
    let matched = Registry::from_rows(rows)
        .matched_keys(&["binance", "bybit"], now)
        .len();

    let build_info = serde_json::json!({
        "built_at": chrono::Utc::now().to_rfc3339(),
        "inputs": input_paths,
        "rows_per_venue": rows_per_venue,
        "matched_canonicals": matched,
        "overridden": overridden,
        "excluded": excluded,
    });
    let info_part = out_dir.join(format!("{BUILD_INFO_FILE}.part"));
    std::fs::write(&info_part, serde_json::to_vec_pretty(&build_info).unwrap())?;
    std::fs::rename(&info_part, out_dir.join(BUILD_INFO_FILE))?;

    tracing::info!(
        matched,
        overridden,
        excluded,
        ?rows_per_venue,
        "symbology mapping built"
    );
    Ok(BuildSummary {
        rows_per_venue,
        matched_canonicals: matched,
        overridden,
        excluded,
        mapping_path,
    })
}

fn mapping_schema() -> Schema {
    Schema::new(vec![
        Field::new("venue", DataType::Utf8, false),
        Field::new("instrument", DataType::Utf8, false),
        Field::new("base", DataType::Utf8, false),
        Field::new("quote", DataType::Utf8, false),
        Field::new("class", DataType::Utf8, false),
        Field::new("settle", DataType::Utf8, false),
        ts_field("valid_from", false),
        ts_field("valid_to", true),
        Field::new("origin", DataType::Utf8, false),
    ])
}

fn write_mapping(out_dir: &Path, rows: &[MappingRow]) -> Result<(), SymbologyError> {
    let tmp_name = format!(".tmp-{MAPPING_FILE}");
    let mut writer = TableWriter::new(out_dir, &tmp_name, mapping_schema());
    let cols: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(
            rows.iter().map(|r| r.venue.as_str()).collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|r| r.instrument.as_str())
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|r| r.canonical.base.0.as_ref())
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|r| r.canonical.quote.0.as_ref())
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|r| crate::class_str(&r.canonical.class))
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|r| r.canonical.settle.0.as_ref())
                .collect::<Vec<_>>(),
        )),
        ts_array(rows.iter().map(|r| r.valid_from as i64).collect()),
        ts_array_opt(rows.iter().map(|r| r.valid_to.map(|v| v as i64)).collect()),
        Arc::new(StringArray::from(
            rows.iter().map(|r| r.origin.as_str()).collect::<Vec<_>>(),
        )),
    ];
    writer
        .write_batch(cols)
        .map_err(|e| SymbologyError::Parse(e.to_string()))?;
    writer
        .close()
        .map_err(|e| SymbologyError::Parse(e.to_string()))?;
    std::fs::rename(out_dir.join(&tmp_name), out_dir.join(MAPPING_FILE))?;
    Ok(())
}

pub fn read_mapping(path: &Path) -> Result<Vec<MappingRow>, SymbologyError> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let file = std::fs::File::open(path)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| SymbologyError::Parse(e.to_string()))?
        .build()
        .map_err(|e| SymbologyError::Parse(e.to_string()))?;
    let mut rows = Vec::new();
    for batch in reader {
        let batch = batch.map_err(|e| SymbologyError::Parse(e.to_string()))?;
        let s = |i: usize| {
            batch
                .column(i)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("mapping string column")
        };
        let valid_from = batch
            .column(6)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .expect("valid_from column");
        let valid_to = batch
            .column(7)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .expect("valid_to column");
        for i in 0..batch.num_rows() {
            rows.push(MappingRow {
                venue: s(0).value(i).to_string(),
                instrument: s(1).value(i).to_string(),
                canonical: venue_core::CanonicalInstrumentId {
                    base: venue_core::Asset(s(2).value(i).into()),
                    quote: venue_core::Asset(s(3).value(i).into()),
                    class: class_from_str(s(4).value(i))?,
                    settle: venue_core::Asset(s(5).value(i).into()),
                },
                valid_from: valid_from.value(i) as u64,
                valid_to: valid_to.is_valid(i).then(|| valid_to.value(i) as u64),
                origin: s(8).value(i).to_string(),
            });
        }
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use venue_core::{InstrumentId, VenueId};

    const BINANCE_DUMP: &str = r#"{"symbols":[
        {"symbol":"BTCUSDT","contractType":"PERPETUAL","baseAsset":"BTC","quoteAsset":"USDT","marginAsset":"USDT","status":"TRADING","onboardDate":1569398400000},
        {"symbol":"1000PEPEUSDT","contractType":"PERPETUAL","baseAsset":"1000PEPE","quoteAsset":"USDT","marginAsset":"USDT","status":"TRADING","onboardDate":1683309600000},
        {"symbol":"BTCDOMUSDT","contractType":"PERPETUAL","baseAsset":"BTCDOM","quoteAsset":"USDT","marginAsset":"USDT","status":"TRADING","onboardDate":1624362000000},
        {"symbol":"BTCUSDT_260925","contractType":"CURRENT_QUARTER","baseAsset":"BTC","quoteAsset":"USDT","marginAsset":"USDT","status":"TRADING"}
    ]}"#;

    const BYBIT_DUMP: &str = r#"[{"retCode":0,"retMsg":"OK","result":{"category":"linear","list":[
        {"symbol":"BTCUSDT","contractType":"LinearPerpetual","baseCoin":"BTC","quoteCoin":"USDT","settleCoin":"USDT","launchTime":"1584230400000","fundingInterval":480},
        {"symbol":"1000PEPEUSDT","contractType":"LinearPerpetual","baseCoin":"1000PEPE","quoteCoin":"USDT","settleCoin":"USDT","launchTime":"1683320400000","fundingInterval":480},
        {"symbol":"ETHUSDT","contractType":"LinearPerpetual","baseCoin":"ETH","quoteCoin":"USDT","settleCoin":"USDT","launchTime":"1600000000000","fundingInterval":480}
    ]}}]"#;

    fn seeded_dirs(tmp: &Path) -> BuildCfg {
        let meta = tmp.join("meta");
        std::fs::create_dir_all(meta.join("binance")).unwrap();
        std::fs::create_dir_all(meta.join("bybit")).unwrap();
        std::fs::write(
            meta.join("binance/2026-06-11-exchangeInfo.json"),
            BINANCE_DUMP,
        )
        .unwrap();
        // An older dump that must lose to the latest one.
        std::fs::write(meta.join("binance/2026-06-01-exchangeInfo.json"), "junk").unwrap();
        std::fs::write(
            meta.join("bybit/2026-06-11-instrumentsInfo.json"),
            BYBIT_DUMP,
        )
        .unwrap();
        BuildCfg {
            data_dir: tmp.to_path_buf(),
            overrides_path: tmp.join("overrides.toml"),
        }
    }

    #[test]
    fn builds_mapping_with_cross_venue_matches_and_roundtrips() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = seeded_dirs(tmp.path());
        let summary = build(&cfg).unwrap();
        assert_eq!(
            summary.rows_per_venue["binance"], 3,
            "dated future excluded"
        );
        assert_eq!(summary.rows_per_venue["bybit"], 3);
        // BTC + 1000PEPE on both; BTCDOM and ETH single-venue.
        assert_eq!(summary.matched_canonicals, 2);

        let registry = Registry::load(&summary.mapping_path).unwrap();
        assert_eq!(registry.len(), 6);
        let c = registry
            .canonical(
                &VenueId {
                    value: "binance".into(),
                },
                &InstrumentId {
                    value: "1000pepeusdt".into(),
                },
                u64::MAX - 1,
            )
            .unwrap();
        assert_eq!(c.base.0.as_ref(), "1000PEPE", "multiplier base verbatim");
        // valid_from from onboardDate, not the dump date.
        let rows = read_mapping(&summary.mapping_path).unwrap();
        let btc = rows
            .iter()
            .find(|r| r.venue == "binance" && r.instrument == "btcusdt")
            .unwrap();
        assert_eq!(btc.valid_from, 1_569_398_400_000 * 1_000_000);
        assert!(btc.valid_to.is_none());
    }

    #[test]
    fn overrides_patch_and_exclude() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = seeded_dirs(tmp.path());
        std::fs::write(
            &cfg.overrides_path,
            r#"
[[override]]
venue = "binance"
instrument = "BTCDOMUSDT"   # index product, not a coin perp: keep out
exclude = true

[[override]]
venue = "bybit"
instrument = "ethusdt"
base = "WETH"               # synthetic example of a curated re-base
"#,
        )
        .unwrap();
        let summary = build(&cfg).unwrap();
        assert_eq!(summary.excluded, 1);
        assert_eq!(summary.overridden, 1);
        assert_eq!(summary.rows_per_venue["binance"], 2);

        let rows = read_mapping(&summary.mapping_path).unwrap();
        assert!(!rows.iter().any(|r| r.instrument == "btcdomusdt"));
        let eth = rows
            .iter()
            .find(|r| r.venue == "bybit" && r.instrument == "ethusdt")
            .unwrap();
        assert_eq!(eth.canonical.base.0.as_ref(), "WETH");
        assert_eq!(eth.origin, "override");
    }

    #[test]
    fn unknown_override_fields_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = seeded_dirs(tmp.path());
        std::fs::write(
            &cfg.overrides_path,
            "[[override]]\nvenue = \"binance\"\ninstrument = \"x\"\ntypo_field = 1\n",
        )
        .unwrap();
        assert!(matches!(build(&cfg), Err(SymbologyError::Parse(_))));
    }
}
