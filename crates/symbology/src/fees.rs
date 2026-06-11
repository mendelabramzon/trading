//! Fee schedules: curated, versioned TOML in-repo (`configs/fees/<venue>.toml`)
//! is the source of truth — no API-key endpoints before Phase 6 — converted
//! to `data/meta/fees/<venue>.parquet` so research joins them in SQL.
//! Parsing is strict (`deny_unknown_fields`; rates must be sane fractions).

use crate::SymbologyError;
use arrow::array::{ArrayRef, Float64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use chrono::NaiveDate;
use recorder::tables::{ts_array, ts_field, TableWriter};
use serde::Deserialize;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FeesFile {
    schedule: Vec<FeeEntry>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FeeEntry {
    /// First day this schedule applied (UTC).
    valid_from: String,
    tier: String,
    market: String,
    /// Fractions as strings ("0.0002" = 2 bps), parsed strictly.
    maker: String,
    taker: String,
}

#[derive(Debug)]
pub struct FeeRow {
    pub venue: String,
    pub tier: String,
    pub market: String,
    pub maker: f64,
    pub taker: f64,
    pub valid_from_ns: u64,
}

fn parse_rate(raw: &str, what: &str, venue: &str) -> Result<f64, SymbologyError> {
    let v: f64 = raw
        .parse()
        .map_err(|_| SymbologyError::Parse(format!("{venue} fees: bad {what} {raw:?}")))?;
    // Maker rebates exist on some venues; anything past ±5% is a typo.
    if !v.is_finite() || v.abs() > 0.05 {
        return Err(SymbologyError::Parse(format!(
            "{venue} fees: implausible {what} {raw:?}"
        )));
    }
    Ok(v)
}

pub(crate) fn parse_fees(venue: &str, raw: &str) -> Result<Vec<FeeRow>, SymbologyError> {
    let file: FeesFile = toml::from_str(raw).map_err(|e| SymbologyError::Parse(e.to_string()))?;
    file.schedule
        .into_iter()
        .map(|e| {
            let date: NaiveDate = e.valid_from.parse().map_err(|_| {
                SymbologyError::Parse(format!("{venue} fees: bad valid_from {:?}", e.valid_from))
            })?;
            Ok(FeeRow {
                venue: venue.to_string(),
                tier: e.tier,
                market: e.market,
                maker: parse_rate(&e.maker, "maker", venue)?,
                taker: parse_rate(&e.taker, "taker", venue)?,
                valid_from_ns: date
                    .and_hms_opt(0, 0, 0)
                    .expect("midnight exists")
                    .and_utc()
                    .timestamp_nanos_opt()
                    .unwrap_or_default() as u64,
            })
        })
        .collect()
}

pub struct FeesSummary {
    pub venues: Vec<(String, usize)>,
    pub out_dir: PathBuf,
}

/// Convert every `configs/fees/<venue>.toml` to
/// `data/meta/fees/<venue>.parquet`.
pub fn build_fees(fees_dir: &Path, data_dir: &Path) -> Result<FeesSummary, SymbologyError> {
    let out_dir = data_dir.join("meta/fees");
    let entries = std::fs::read_dir(fees_dir)
        .map_err(|_| SymbologyError::Invalid(format!("fees dir {} missing", fees_dir.display())))?;
    let mut venues = Vec::new();
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        let Some(venue) = name.strip_suffix(".toml") else {
            continue;
        };
        let rows = parse_fees(venue, &std::fs::read_to_string(entry.path())?)?;
        std::fs::create_dir_all(&out_dir)?;
        write_fees(&out_dir, venue, &rows)?;
        tracing::info!(venue, rows = rows.len(), "fee schedule built");
        venues.push((venue.to_string(), rows.len()));
    }
    if venues.is_empty() {
        return Err(SymbologyError::Invalid(format!(
            "no <venue>.toml files under {}",
            fees_dir.display()
        )));
    }
    venues.sort();
    Ok(FeesSummary { venues, out_dir })
}

fn write_fees(out_dir: &Path, venue: &str, rows: &[FeeRow]) -> Result<(), SymbologyError> {
    let schema = Schema::new(vec![
        Field::new("venue", DataType::Utf8, false),
        Field::new("tier", DataType::Utf8, false),
        Field::new("market", DataType::Utf8, false),
        Field::new("maker", DataType::Float64, false),
        Field::new("taker", DataType::Float64, false),
        ts_field("valid_from", false),
    ]);
    let file_name = format!("{venue}.parquet");
    let tmp_name = format!(".tmp-{file_name}");
    let mut writer = TableWriter::new(out_dir, &tmp_name, schema);
    let cols: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(
            rows.iter().map(|r| r.venue.as_str()).collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter().map(|r| r.tier.as_str()).collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter().map(|r| r.market.as_str()).collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            rows.iter().map(|r| r.maker).collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            rows.iter().map(|r| r.taker).collect::<Vec<_>>(),
        )),
        ts_array(rows.iter().map(|r| r.valid_from_ns as i64).collect()),
    ];
    writer
        .write_batch(cols)
        .map_err(|e| SymbologyError::Parse(e.to_string()))?;
    writer
        .close()
        .map_err(|e| SymbologyError::Parse(e.to_string()))?;
    std::fs::rename(out_dir.join(&tmp_name), out_dir.join(&file_name))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_strictly_and_rejects_junk() {
        let rows = parse_fees(
            "binance",
            r#"
[[schedule]]
valid_from = "2026-01-01"
tier = "vip0"
market = "usdm_perp"
maker = "0.0002"
taker = "0.0005"
"#,
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].maker, 0.0002);
        assert_eq!(rows[0].taker, 0.0005);
        assert!(rows[0].valid_from_ns > 0);

        for bad in [
            // unknown field
            "[[schedule]]\nvalid_from = \"2026-01-01\"\ntier = \"t\"\nmarket = \"m\"\nmaker = \"0.1\"\ntaker = \"0.1\"\nrebate = \"x\"\n",
            // implausible rate
            "[[schedule]]\nvalid_from = \"2026-01-01\"\ntier = \"t\"\nmarket = \"m\"\nmaker = \"0.2\"\ntaker = \"0.0005\"\n",
            // bad date
            "[[schedule]]\nvalid_from = \"January\"\ntier = \"t\"\nmarket = \"m\"\nmaker = \"0.0002\"\ntaker = \"0.0005\"\n",
        ] {
            assert!(parse_fees("binance", bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn builds_parquet_per_venue() {
        let tmp = tempfile::tempdir().unwrap();
        let fees_dir = tmp.path().join("fees");
        std::fs::create_dir_all(&fees_dir).unwrap();
        std::fs::write(
            fees_dir.join("binance.toml"),
            "[[schedule]]\nvalid_from = \"2026-01-01\"\ntier = \"vip0\"\nmarket = \"usdm_perp\"\nmaker = \"0.0002\"\ntaker = \"0.0005\"\n",
        )
        .unwrap();
        let summary = build_fees(&fees_dir, tmp.path()).unwrap();
        assert_eq!(summary.venues, vec![("binance".to_string(), 1)]);
        assert!(tmp.path().join("meta/fees/binance.parquet").exists());
    }
}
