//! Bring-up probe: does a fapi stream name actually emit? Binance ACKs
//! SUBSCRIBE for dead stream names (`@aggTrade`, the whole `markPrice`
//! family), so silence — not an error — is the failure mode to test for.
//!
//! Usage: `ws_probe <stream|wss-url> [secs]` — connects to the raw-stream
//! path URL (or a full URL, e.g. the combined endpoint) and counts frames.

use futures_util::StreamExt;
use std::time::Duration;
use tokio_tungstenite::connect_async;

#[tokio::main]
async fn main() {
    let stream = std::env::args()
        .nth(1)
        .expect("usage: ws_probe <stream> [secs]");
    let secs: u64 = std::env::args()
        .nth(2)
        .map(|s| s.parse().expect("secs"))
        .unwrap_or(10);

    let url = if stream.starts_with("wss://") {
        stream.clone()
    } else {
        format!("wss://fstream.binance.com/ws/{stream}")
    };
    let (mut ws, resp) = connect_async(&url).await.expect("connect");
    println!("connected {url} (HTTP {})", resp.status());

    let mut frames = 0u64;
    let mut first: Option<String> = None;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    loop {
        tokio::select! {
            _ = tokio::time::sleep_until(deadline) => break,
            msg = ws.next() => match msg {
                Some(Ok(m)) if m.is_text() => {
                    frames += 1;
                    let t = m.into_text().unwrap();
                    first.get_or_insert_with(|| t.chars().take(160).collect());
                }
                Some(Ok(_)) => {}
                other => { println!("stream ended: {other:?}"); break; }
            }
        }
    }
    println!("{stream}: {frames} text frames in {secs}s");
    if let Some(f) = first {
        println!("first frame: {f}…");
    }
}
