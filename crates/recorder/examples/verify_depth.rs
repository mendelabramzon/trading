//! Acceptance checker (the pu-chain / splice gate), now a thin CLI over
//! `recorder::qa` — the same code path the automated sweep runs daily.
//!
//! Verifies per instrument: depth chains (`pu == previous u`, breaks split
//! into reconnect-explained vs unexplained), REST snapshot splices
//! (`U <= lastUpdateId <= u`), trade-id regressions; prints the control
//! timeline so findings can be matched to reconnects. Exits 1 on QA fail.
//!
//! Usage: `verify_depth <path-to-wal-file>`

use recorder::qa::{qa_wal, QaStatus};
use std::path::Path;

fn main() {
    let path = std::env::args()
        .nth(1)
        .expect("usage: verify_depth <path-to-wal-file>");
    let path = Path::new(&path);

    // Venue/date here are report metadata only; derive what we can.
    let date = path
        .file_stem()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| "unknown".into());
    let venue = path
        .parent()
        .and_then(|p| p.file_name())
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| "unknown".into());

    let report = qa_wal(path, &venue, &date).expect("open WAL");

    println!("=== {}", report.wal_file);
    println!("total events: {}", report.events.total);
    for (kind, n) in &report.events.by_kind {
        println!("  {kind:.<24}{n}");
    }
    println!(
        "frames: ok={} skipped_bytes={} resyncs={} undecodable={} truncated_tail={}",
        report.frames.frames_ok,
        report.frames.skipped_bytes,
        report.frames.resyncs,
        report.frames.undecodable_frames,
        report.frames.truncated_tail,
    );

    println!("\n=== control timeline");
    for line in &report.control.timeline {
        println!("  {line}");
    }
    if report.control.timeline_truncated {
        println!("  … truncated");
    }

    println!("\n=== depth verification");
    for (symbol, d) in &report.depth {
        println!(
            "{symbol}: {} updates, {} snapshots, {} explained / {} unexplained breaks, \
             {} unspliced, {} pending-at-eof, {} abandoned-by-reconnect{}",
            d.updates,
            d.snapshots,
            d.chain_breaks_explained,
            d.chain_breaks_unexplained,
            d.unspliced_snapshots,
            d.snapshots_pending_at_eof,
            d.snapshots_abandoned_by_reconnect,
            if d.missing_snapshot {
                ", MISSING SNAPSHOT"
            } else {
                ""
            },
        );
    }
    if let Some(e) = &report.latency_us.depth_e_minus_t {
        println!(
            "depth E−T: p50={}µs p99={}µs (n={})",
            e.p50_us, e.p99_us, e.count
        );
    }
    if !report.dups.is_empty() {
        println!("\n=== sequence-id regressions (report-only)");
        for (symbol, d) in &report.dups {
            println!(
                "{symbol}: trades explained={} unexplained={} ticker={}",
                d.trade_id_regressions_explained,
                d.trade_id_regressions_unexplained,
                d.book_ticker_update_id_regressions,
            );
        }
    }

    match report.status {
        QaStatus::Pass => println!("\nACCEPTANCE: PASS"),
        QaStatus::Fail => {
            println!("\nACCEPTANCE: FAIL");
            for f in &report.failures {
                println!("  - {f}");
            }
            std::process::exit(1);
        }
    }
}
