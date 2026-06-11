use crate::{ms_to_nanos, now_nanos, BASE_REST_URL};
use rust_decimal::Decimal;
use serde::Deserialize;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use venue_adapter::{EventSink, VenueError};
use venue_core::{Event, InstrumentId, Level, MarketPayload, Nanos, Payload, SourceId, VenueId};

const SNAPSHOT_DEPTH_LIMIT: u32 = 1000;

/// `depth?limit=1000` costs weight 20 against the 2,400/min IP budget; 0.5 s
/// pacing keeps snapshotting under ~5% of it even across a full re-snapshot
/// sweep (P7). An IP ban here would kill the capture path too — pace
/// conservatively.
const SNAPSHOT_PACING: Duration = Duration::from_millis(500);

/// Periodic re-snapshot: belt-and-braces alongside on-reconnect snapshots, so
/// a silently broken pu-chain never lasts more than this long.
const SNAPSHOT_REFRESH: Duration = Duration::from_secs(30 * 60);

/// Binance default funding interval; `/fapi/v1/fundingInfo` lists only the
/// symbols that deviate from it.
pub(crate) const DEFAULT_FUNDING_INTERVAL_NS: Nanos = 8 * 3600 * 1_000_000_000;

#[derive(Debug)]
pub(crate) struct SnapshotRequest {
    pub symbol: Arc<str>,
    retry: bool,
}

impl SnapshotRequest {
    pub(crate) fn new(symbol: Arc<str>) -> Self {
        Self {
            symbol,
            retry: false,
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DepthSnapshotResponse {
    last_update_id: u64,
    /// Transaction time (ms) — the snapshot's venue_ts per the D7 contract.
    #[serde(rename = "T")]
    transaction_time: u64,
    bids: Vec<(Decimal, Decimal)>,
    asks: Vec<(Decimal, Decimal)>,
}

/// Spawns the per-venue snapshot fetcher. Triggers arrive over the returned
/// channel: first depthUpdate per symbol per connection session (so every
/// reconnect re-snapshots) and the periodic refresh sweep. Fetches run
/// sequentially with explicit pacing; duplicate pending requests coalesce.
pub(crate) fn spawn_snapshot_fetcher<S: EventSink>(
    sink: S,
    venue_id: VenueId,
    cancel: CancellationToken,
) -> (mpsc::Sender<SnapshotRequest>, tokio::task::JoinHandle<()>) {
    let (tx, rx) = mpsc::channel(1024);
    let handle = tokio::spawn(snapshot_fetcher_loop(rx, sink, venue_id, cancel));
    (tx, handle)
}

async fn snapshot_fetcher_loop<S: EventSink>(
    mut rx: mpsc::Receiver<SnapshotRequest>,
    sink: S,
    venue_id: VenueId,
    cancel: CancellationToken,
) {
    let client = reqwest::Client::new();
    let mut queue: VecDeque<SnapshotRequest> = VecDeque::new();
    let mut queued: HashSet<Arc<str>> = HashSet::new();
    let mut known: HashSet<Arc<str>> = HashSet::new();
    let mut refresh = tokio::time::interval_at(
        tokio::time::Instant::now() + SNAPSHOT_REFRESH,
        SNAPSHOT_REFRESH,
    );
    refresh.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut next_fetch = tokio::time::Instant::now();

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                tracing::info!("snapshot fetcher shutting down");
                return;
            }
            req = rx.recv() => {
                match req {
                    Some(req) => {
                        known.insert(req.symbol.clone());
                        if queued.insert(req.symbol.clone()) {
                            queue.push_back(req);
                        }
                    }
                    None => return, // all senders gone
                }
            }
            _ = refresh.tick() => {
                tracing::info!(symbols = known.len(), "periodic depth re-snapshot sweep");
                for symbol in &known {
                    if queued.insert(symbol.clone()) {
                        queue.push_back(SnapshotRequest::new(symbol.clone()));
                    }
                }
            }
            _ = tokio::time::sleep_until(next_fetch), if !queue.is_empty() => {
                let req = queue.pop_front().expect("guarded by !is_empty");
                queued.remove(&req.symbol);
                next_fetch = tokio::time::Instant::now() + SNAPSHOT_PACING;

                match fetch_snapshot(&client, &req.symbol).await {
                    Ok(resp) => {
                        let last_update_id = resp.last_update_id;
                        let event = snapshot_event(&venue_id, &req.symbol, resp);
                        if let Err(e) = sink.send(event).await {
                            tracing::warn!(error = ?e, symbol = %req.symbol, "snapshot dropped (sink closing?)");
                        } else {
                            tracing::info!(symbol = %req.symbol, last_update_id, "depth snapshot recorded");
                        }
                    }
                    Err(e) => {
                        tracing::warn!(symbol = %req.symbol, error = %e, "depth snapshot fetch failed");
                        // One paced retry; after that the periodic sweep
                        // picks the symbol up again.
                        if !req.retry && queued.insert(req.symbol.clone()) {
                            queue.push_back(SnapshotRequest {
                                symbol: req.symbol,
                                retry: true,
                            });
                        }
                    }
                }
            }
        }
    }
}

async fn fetch_snapshot(
    client: &reqwest::Client,
    symbol: &str,
) -> Result<DepthSnapshotResponse, reqwest::Error> {
    // REST wants the UPPERCASE symbol (WS streams use lowercase).
    let url = format!(
        "{BASE_REST_URL}/fapi/v1/depth?symbol={}&limit={SNAPSHOT_DEPTH_LIMIT}",
        symbol.to_uppercase()
    );
    client
        .get(&url)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await
}

fn snapshot_event(venue_id: &VenueId, symbol: &Arc<str>, resp: DepthSnapshotResponse) -> Event {
    let to_levels = |side: Vec<(Decimal, Decimal)>| {
        side.into_iter()
            .map(|(price, qty)| Level { price, qty })
            .collect()
    };
    Event {
        venue: venue_id.clone(),
        instrument: Some(InstrumentId {
            value: symbol.clone(),
        }),
        venue_ts: Some(ms_to_nanos(resp.transaction_time)),
        local_ts: now_nanos(),
        source: SourceId::REST,
        provenance: None,
        payload: Payload::Market(MarketPayload::BookSnapshot {
            bids: to_levels(resp.bids),
            asks: to_levels(resp.asks),
            last_update_id: resp.last_update_id,
        }),
    }
}

// --- Funding metadata (A4) ---

#[derive(Debug, Clone)]
pub(crate) struct FundingMeta {
    pub interval: Nanos,
    pub cap: Option<Decimal>,
    pub floor: Option<Decimal>,
}

/// Lowercase symbol → funding metadata for symbols deviating from the 8h
/// default. Symbols absent from the map use `DEFAULT_FUNDING_INTERVAL_NS`.
pub(crate) type FundingMap = Arc<HashMap<String, FundingMeta>>;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct FundingInfoEntry {
    symbol: String,
    #[serde(default)]
    adjusted_funding_rate_cap: Option<Decimal>,
    #[serde(default)]
    adjusted_funding_rate_floor: Option<Decimal>,
    #[serde(default)]
    funding_interval_hours: Option<u64>,
}

/// fundingInfo plus its raw body (the P5a pattern): the venue process dumps
/// the JSON to `data/meta/` daily so interval/clamp *history* feeds the
/// instruments SCD — the parsed map only ever holds "now".
pub(crate) async fn fetch_funding_info_raw() -> Result<(String, FundingMap), VenueError> {
    let url = format!("{BASE_REST_URL}/fapi/v1/fundingInfo");
    let text = reqwest::get(&url)
        .await
        .map_err(|e| VenueError::RequestFailed(e.to_string()))?
        .text()
        .await
        .map_err(|e| VenueError::RequestFailed(e.to_string()))?;
    let entries: Vec<FundingInfoEntry> = serde_json::from_str(&text)
        .map_err(|e| VenueError::RequestFailed(format!("fundingInfo parse: {e}")))?;

    let map = entries
        .into_iter()
        .map(|e| {
            let meta = FundingMeta {
                interval: e
                    .funding_interval_hours
                    .map(|h| h * 3600 * 1_000_000_000)
                    .unwrap_or(DEFAULT_FUNDING_INTERVAL_NS),
                cap: e.adjusted_funding_rate_cap,
                floor: e.adjusted_funding_rate_floor,
            };
            (e.symbol.to_lowercase(), meta)
        })
        .collect();

    Ok((text, Arc::new(map)))
}

pub(crate) async fn fetch_funding_info() -> Result<FundingMap, VenueError> {
    fetch_funding_info_raw().await.map(|(_, map)| map)
}
