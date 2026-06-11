//! Data-product joinability check (the Phase-2 exit demo, runnable without
//! external tooling): unions both venues' backfilled funding history, joins
//! it to canonical ids via `mapping.parquet`, and prints the top cross-venue
//! funding spreads — proving the external research repo can answer the
//! spread question from the published files alone (`docs/data-products.md`).
//!
//!   cargo run -p symbology --example spread_check -- [data_dir] [days]
//!
//! Exits non-zero if fewer than 100 canonical perps have data on both
//! venues in the window (the products would be broken or incomplete).

use arrow::array::{Array, Float64Array, StringArray, TimestampNanosecondArray};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use symbology::Registry;
use venue_core::{InstrumentId, VenueId};

struct FundingRow {
    instrument: String,
    funding_time_ns: u64,
    rate: f64,
}

fn read_funding(dir: &Path) -> Vec<FundingRow> {
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return out;
    };
    let mut files: Vec<PathBuf> = entries
        .flatten()
        .map(|e| e.path())
        .filter(|p| {
            p.extension().is_some_and(|e| e == "parquet") && !p.to_string_lossy().contains(".tmp-")
        })
        .collect();
    files.sort();
    for path in files {
        let Ok(file) = std::fs::File::open(&path) else {
            continue;
        };
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .expect("readable parquet")
            .build()
            .expect("reader");
        for batch in reader {
            let batch = batch.expect("batch");
            let instruments = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("instrument");
            let times = batch
                .column(5)
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .expect("funding_time");
            let rates = batch
                .column(4)
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("rate");
            for i in 0..batch.num_rows() {
                if rates.is_null(i) {
                    continue;
                }
                out.push(FundingRow {
                    instrument: instruments.value(i).to_string(),
                    funding_time_ns: times.value(i) as u64,
                    rate: rates.value(i),
                });
            }
        }
    }
    out
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let data_dir = PathBuf::from(args.first().map(String::as_str).unwrap_or("data"));
    let days: u64 = args.get(1).and_then(|d| d.parse().ok()).unwrap_or(90);

    let registry = match Registry::load(&data_dir.join("meta/symbology/mapping.parquet")) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("mapping unreadable: {e} — run `symbology build` first");
            return ExitCode::from(1);
        }
    };

    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    let window_start = now_ns.saturating_sub(days * 86_400_000_000_000);

    // canonical key → per-venue (sum of rates, settlement count) inside the
    // window. Funding rates are per-settlement fractions; summing over a
    // fixed wall-clock window makes venues with different intervals
    // directly comparable (each sum = total funding paid over the window).
    let mut per_canonical: HashMap<String, HashMap<&'static str, (f64, u64)>> = HashMap::new();
    let mut rows_used = [0u64; 2];
    for (idx, venue) in ["binance", "bybit"].into_iter().enumerate() {
        let venue_id = VenueId {
            value: venue.into(),
        };
        let rows = read_funding(&data_dir.join("backfill").join(venue).join("funding"));
        if rows.is_empty() {
            eprintln!(
                "no backfilled funding for {venue} under {}",
                data_dir.display()
            );
            return ExitCode::from(1);
        }
        for row in rows {
            if row.funding_time_ns < window_start {
                continue;
            }
            let instrument = InstrumentId {
                value: row.instrument.as_str().into(),
            };
            let Some(canonical) = registry.canonical(&venue_id, &instrument, row.funding_time_ns)
            else {
                continue; // unmapped (excluded or unlisted) — fine
            };
            let entry = per_canonical
                .entry(symbology::canonical_key(canonical))
                .or_default()
                .entry(if idx == 0 { "binance" } else { "bybit" })
                .or_insert((0.0, 0));
            entry.0 += row.rate;
            entry.1 += 1;
            rows_used[idx] += 1;
        }
    }

    let mut spreads: Vec<(String, f64, f64, f64)> = per_canonical
        .iter()
        .filter_map(|(key, venues)| {
            let (b_sum, b_n) = venues.get("binance")?;
            let (y_sum, y_n) = venues.get("bybit")?;
            if *b_n == 0 || *y_n == 0 {
                return None;
            }
            // Annualize: total window funding × (365 / window days).
            let ann = 365.0 / days as f64;
            let b = b_sum * ann;
            let y = y_sum * ann;
            Some((key.clone(), b, y, b - y))
        })
        .collect();
    let matched = spreads.len();
    spreads.sort_by(|a, b| {
        b.3.abs()
            .partial_cmp(&a.3.abs())
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    println!(
        "window: last {days} days | rows used: binance={} bybit={} | canonical perps with data on both venues: {matched}",
        rows_used[0], rows_used[1]
    );
    println!("top annualized funding spreads (binance − bybit), fractions per year:");
    for (key, b, y, spread) in spreads.iter().take(15) {
        println!("  {key:<28} binance={b:+.4}  bybit={y:+.4}  spread={spread:+.4}");
    }

    if matched < 100 {
        eprintln!("FAIL: only {matched} matched canonicals with two-venue data (expected 100+)");
        return ExitCode::from(1);
    }
    println!("OK: cross-venue funding spread is answerable from the published data products.");
    ExitCode::SUCCESS
}
