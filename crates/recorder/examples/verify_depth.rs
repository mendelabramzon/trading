//! Phase-0 acceptance checker (the pu-chain / splice gate).
//!
//! Reads a WAL and verifies, per instrument:
//! 1. depth update chains: `pu == previous u` (breaks outside reconnects mean
//!    lost data);
//! 2. every REST snapshot is spliceable: some update covers `lastUpdateId`
//!    (`U <= lastUpdateId <= u`).
//!
//! Also prints the control-event timeline so chain breaks can be matched to
//! reconnects.

use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use venue_core::{ControlPayload, MarketPayload, Payload};

#[derive(Default)]
struct SymbolState {
    updates: u64,
    chain_breaks: Vec<String>,
    last_final: Option<u64>,
    snapshots: Vec<u64>,
    spliced: HashMap<u64, bool>,
    e_minus_t_ns: Vec<i64>,
}

fn main() {
    let path = std::env::args()
        .nth(1)
        .expect("usage: verify_depth <path-to-wal-file>");
    let mut reader = wire::FrameReader::new(BufReader::new(File::open(&path).expect("open WAL")));

    let mut symbols: HashMap<String, SymbolState> = HashMap::new();
    let mut by_kind: HashMap<&'static str, u64> = HashMap::new();
    let mut controls: Vec<String> = Vec::new();
    let mut total = 0u64;

    loop {
        let event = match reader.next_event() {
            Ok(Some(e)) => e,
            Ok(None) => break,
            Err(e) => {
                eprintln!("fatal decode error after {total} events: {e}");
                break;
            }
        };
        total += 1;
        let instrument = event
            .instrument
            .as_ref()
            .map(|i| i.value.to_string())
            .unwrap_or_default();

        match &event.payload {
            Payload::Market(MarketPayload::BookUpdate {
                first_update_id,
                final_update_id,
                prev_final_update_id,
                event_time,
                ..
            }) => {
                *by_kind.entry("book_update").or_default() += 1;
                let st = symbols.entry(instrument).or_default();
                st.updates += 1;

                if let (Some(pu), Some(prev_u)) = (prev_final_update_id, st.last_final) {
                    if *pu != prev_u {
                        st.chain_breaks.push(format!(
                            "update #{}: pu={} but previous u={} (local_ts {})",
                            st.updates, pu, prev_u, event.local_ts
                        ));
                    }
                }
                st.last_final = Some(*final_update_id);

                if let Some(et) = event_time {
                    if let Some(vt) = event.venue_ts {
                        st.e_minus_t_ns.push(*et as i64 - vt as i64);
                    }
                }
                for (snap_id, ok) in st.spliced.iter_mut() {
                    if !*ok && *first_update_id <= *snap_id && *snap_id <= *final_update_id {
                        *ok = true;
                    }
                }
            }
            Payload::Market(MarketPayload::BookSnapshot { last_update_id, .. }) => {
                *by_kind.entry("book_snapshot").or_default() += 1;
                let st = symbols.entry(instrument).or_default();
                st.snapshots.push(*last_update_id);
                st.spliced.insert(*last_update_id, false);
            }
            Payload::Market(m) => {
                let kind = match m {
                    MarketPayload::BookTicker { .. } => "book_ticker",
                    MarketPayload::Trades { .. } => "trades",
                    MarketPayload::MarkPrice { .. } => "mark_price",
                    MarketPayload::IndexPrice { .. } => "index_price",
                    MarketPayload::FundingRatePrediction { .. } => "funding_prediction",
                    MarketPayload::FundingRateRealized { .. } => "funding_realized",
                    MarketPayload::OpenInterest { .. } => "open_interest",
                    MarketPayload::Liquidation { .. } => "liquidation",
                    _ => "other_market",
                };
                *by_kind.entry(kind).or_default() += 1;
            }
            Payload::Control(c) => {
                *by_kind.entry("control").or_default() += 1;
                let desc = match c {
                    ControlPayload::ConnUp { label } => format!("ConnUp {label}"),
                    ControlPayload::ConnDown { label, reason } => {
                        format!("ConnDown {label}: {reason}")
                    }
                    ControlPayload::SubAck { request_id, ok, .. } => {
                        format!("SubAck id={request_id} ok={ok}")
                    }
                    other => format!("{other:?}"),
                };
                controls.push(format!("local_ts={} {desc}", event.local_ts));
            }
            _ => {
                *by_kind.entry("other").or_default() += 1;
            }
        }
    }

    println!("=== {path}");
    println!("total events: {total}");
    let mut kinds: Vec<_> = by_kind.iter().collect();
    kinds.sort();
    for (k, n) in kinds {
        println!("  {k:.<24}{n}");
    }
    println!("reader stats: {:?}", reader.stats());

    println!("\n=== control timeline");
    for c in &controls {
        println!("  {c}");
    }

    println!("\n=== depth verification");
    let mut failures = 0;
    let mut symbols_sorted: Vec<_> = symbols.iter().collect();
    symbols_sorted.sort_by_key(|(s, _)| (*s).clone());
    for (symbol, st) in symbols_sorted {
        if st.updates == 0 && st.snapshots.is_empty() {
            continue;
        }
        let unspliced: Vec<u64> = st
            .spliced
            .iter()
            .filter(|(_, ok)| !**ok)
            .map(|(id, _)| *id)
            .collect();
        let mut emt = st.e_minus_t_ns.clone();
        emt.sort_unstable();
        let p50 = emt.get(emt.len() / 2).copied().unwrap_or(0);
        let p99 = emt.get(emt.len() * 99 / 100).copied().unwrap_or(0);

        println!(
            "{symbol}: {} updates, {} snapshots, {} chain breaks, {} unspliced; E−T p50={}µs p99={}µs",
            st.updates,
            st.snapshots.len(),
            st.chain_breaks.len(),
            unspliced.len(),
            p50 / 1_000,
            p99 / 1_000,
        );
        for b in &st.chain_breaks {
            println!("    BREAK {b}");
            failures += 1;
        }
        for id in &unspliced {
            println!("    UNSPLICED snapshot lastUpdateId={id}");
            failures += 1;
        }
        if st.updates > 0 && st.snapshots.is_empty() {
            println!("    MISSING snapshot for symbol with depth updates");
            failures += 1;
        }
    }

    if failures == 0 {
        println!("\nACCEPTANCE: PASS (chain breaks outside reconnect windows: 0, all snapshots spliceable)");
    } else {
        println!("\nACCEPTANCE: {failures} finding(s) — match BREAKs against the control timeline; breaks inside reconnect windows are expected");
        std::process::exit(1);
    }
}
