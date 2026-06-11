//! Phase-2 REST pollers (A6 + the dead-`@markPrice` replacement): premium
//! index (mark/index/funding prediction), open interest, realized funding.
//! Each is an `IngestSource` sharing the adapter's sink; all REST-origin
//! events carry `SourceId::REST` and the pollers are told apart in the
//! control timeline by label (`poller-*`) and in heartbeats by event kind.
//!
//! Response bodies are teed raw before parsing (R2 applies to REST exactly
//! as to WS: fields the parser drops — `interestRate`,
//! `estimatedSettlePrice` — stay recoverable from `data/raw/`).

use crate::rest::{FundingMap, DEFAULT_FUNDING_INTERVAL_NS};
use crate::{ms_to_nanos, now_nanos, BASE_REST_URL};
use rust_decimal::Decimal;
use serde::Deserialize;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use venue_adapter::{EventSink, IngestSource, RawFrameSink};
use venue_core::{
    ControlPayload, Event, InstrumentId, MarketPayload, Nanos, Payload, RawFrame, SourceId, VenueId,
};

/// Poll cadences; bounds-checked by the config crate (5–3600 s), defaults
/// per the Phase-2 plan. ~798 symbols × 3 events / 30 s ≈ 80 events/s from
/// the premium-index poller alone — the main new volume driver.
#[derive(Debug, Clone)]
pub struct PollerCfg {
    pub premium_index: Duration,
    pub open_interest: Duration,
    pub funding_realized: Duration,
}

impl Default for PollerCfg {
    fn default() -> Self {
        Self {
            premium_index: Duration::from_secs(30),
            open_interest: Duration::from_secs(300),
            funding_realized: Duration::from_secs(300),
        }
    }
}

/// Lowercase perp symbols the OI poller sweeps. Updated live by the
/// universe manager via `watch`; a static snapshot otherwise.
pub type Universe = Arc<Vec<Arc<str>>>;

/// Where the OI poller gets its symbol list.
pub(crate) enum UniverseSource {
    Static(Universe),
    Dynamic(watch::Receiver<Universe>),
}

impl UniverseSource {
    fn current(&self) -> Universe {
        match self {
            UniverseSource::Static(u) => u.clone(),
            UniverseSource::Dynamic(rx) => rx.borrow().clone(),
        }
    }
}

/// How many consecutive failed poll cycles flip a poller's control state to
/// ConnDown. One blip is routine; three in a row is an outage worth a
/// recorded control event (A7).
const FAIL_THRESHOLD: u32 = 3;

/// Realized-funding lookback per cycle (re-fetch window for late rows) and
/// catch-up at startup (restart gap coverage). Dedup keys are pruned past
/// the catch-up horizon.
const FUNDING_LOOKBACK_MS: u64 = 3_600_000;
const FUNDING_CATCHUP_MS: u64 = 2 * 3_600_000;
const FUNDING_PAGE_LIMIT: usize = 1000;

/// fundingInfo (intervals/clamps) refresh cadence inside the pollers that
/// stamp it; the endpoint shares a small budget with fundingRate, so daily.
const FUNDING_INFO_REFRESH: Duration = Duration::from_secs(24 * 3600);

const HTTP_TIMEOUT: Duration = Duration::from_secs(30);

/// Control events from pollers mirror `ConnCtx::emit_control`: venue-scoped,
/// no instrument, no venue_ts.
async fn emit_control<S: EventSink>(sink: &S, venue_id: &VenueId, payload: ControlPayload) {
    let event = Event {
        venue: venue_id.clone(),
        instrument: None,
        venue_ts: None,
        local_ts: now_nanos(),
        source: SourceId::REST,
        provenance: None,
        payload: Payload::Control(payload),
    };
    if let Err(e) = sink.send(event).await {
        tracing::debug!(error = ?e, "control event dropped (sink closing?)");
    }
}

/// Announced-state tracker: first success emits ConnUp, the Nth consecutive
/// failure emits ConnDown, recovery emits ConnUp again. Keeps the control
/// timeline flap-free across single blips.
struct Health {
    announced: Option<bool>,
    fails: u32,
}

impl Health {
    fn new() -> Self {
        Self {
            announced: None,
            fails: 0,
        }
    }

    /// True when this success should emit ConnUp.
    fn ok(&mut self) -> bool {
        self.fails = 0;
        if self.announced != Some(true) {
            self.announced = Some(true);
            true
        } else {
            false
        }
    }

    /// True when this failure should emit ConnDown.
    fn fail(&mut self) -> bool {
        self.fails += 1;
        if self.fails == FAIL_THRESHOLD && self.announced != Some(false) {
            self.announced = Some(false);
            true
        } else {
            false
        }
    }
}

async fn report_ok<S: EventSink>(
    health: &mut Health,
    sink: &S,
    venue_id: &VenueId,
    label: &Arc<str>,
) {
    if health.ok() {
        emit_control(
            sink,
            venue_id,
            ControlPayload::ConnUp {
                label: label.clone(),
            },
        )
        .await;
    }
}

async fn report_fail<S: EventSink>(
    health: &mut Health,
    sink: &S,
    venue_id: &VenueId,
    label: &Arc<str>,
    reason: &str,
) {
    if health.fail() {
        emit_control(
            sink,
            venue_id,
            ControlPayload::ConnDown {
                label: label.clone(),
                reason: reason.into(),
            },
        )
        .await;
    }
}

/// GET a fapi path, tee the raw body (R2), return it for parsing.
async fn fetch_raw<R: RawFrameSink>(
    client: &reqwest::Client,
    url: &str,
    raw: &R,
) -> Result<String, String> {
    let body = client
        .get(url)
        .send()
        .await
        .map_err(|e| e.to_string())?
        .error_for_status()
        .map_err(|e| e.to_string())?
        .text()
        .await
        .map_err(|e| e.to_string())?;
    raw.send_raw(RawFrame {
        local_ts: now_nanos(),
        source: SourceId::REST,
        bytes: body.as_bytes().to_vec(),
    });
    Ok(body)
}

// --- Premium index (mark / index / funding prediction) ---

/// One row of `GET /fapi/v1/premiumIndex` (no symbol → all symbols, weight
/// 10, live-verified 2026-06-11: 798 rows, ~177 KB). `estimatedSettlePrice`
/// and `interestRate` are not parsed — the raw tee keeps them.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PremiumIndexEntry {
    symbol: String,
    mark_price: Decimal,
    index_price: Decimal,
    last_funding_rate: Decimal,
    /// 0 for symbols with no upcoming funding (delivery futures, settling
    /// symbols) — those rows emit no prediction.
    next_funding_time: u64,
    /// Endpoint event time (ms, minute-quantized). This source has no
    /// transaction time; venue_ts = time is the documented D7 exception,
    /// same as the dead WS stream.
    time: u64,
}

/// Pure fan-out: one response → MarkPrice + IndexPrice (+ prediction when a
/// funding cycle is live) per row, mirroring the WS `markPriceUpdate` arm.
pub(crate) fn premium_index_events(
    entries: Vec<PremiumIndexEntry>,
    venue_id: &VenueId,
    funding: &FundingMap,
    local_ts: Nanos,
) -> Vec<Event> {
    let mut out = Vec::with_capacity(entries.len() * 3);
    for entry in entries {
        let symbol: Arc<str> = entry.symbol.to_lowercase().into();
        let make = |payload: Payload| Event {
            venue: venue_id.clone(),
            instrument: Some(InstrumentId {
                value: symbol.clone(),
            }),
            venue_ts: Some(ms_to_nanos(entry.time)),
            local_ts,
            source: SourceId::REST,
            provenance: None,
            payload,
        };
        out.push(make(Payload::Market(MarketPayload::MarkPrice {
            price: entry.mark_price,
        })));
        out.push(make(Payload::Market(MarketPayload::IndexPrice {
            price: entry.index_price,
        })));
        if entry.next_funding_time > 0 {
            let meta = funding.get(symbol.as_ref());
            out.push(make(Payload::Market(
                MarketPayload::FundingRatePrediction {
                    rate: entry.last_funding_rate,
                    next_funding_time: ms_to_nanos(entry.next_funding_time),
                    interval: Some(meta.map_or(DEFAULT_FUNDING_INTERVAL_NS, |m| m.interval)),
                    premium_index: None, // endpoint carries no premium value; never fabricate
                    clamp_min: meta.and_then(|m| m.floor),
                    clamp_max: meta.and_then(|m| m.cap),
                },
            )));
        }
    }
    out
}

pub(crate) struct PremiumIndexPoller<S: EventSink, R: RawFrameSink> {
    pub sink: S,
    pub raw: R,
    pub venue_id: VenueId,
    pub funding: FundingMap,
    pub every: Duration,
    pub client: reqwest::Client,
}

impl<S: EventSink, R: RawFrameSink> PremiumIndexPoller<S, R> {
    async fn run_inner(mut self, cancel: CancellationToken) {
        let label: Arc<str> = "poller-premium-index".into();
        let mut health = Health::new();
        let mut next = tokio::time::Instant::now();
        let mut funding_age = tokio::time::Instant::now();
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = tokio::time::sleep_until(next) => {}
            }
            next = tokio::time::Instant::now() + self.every;

            // Intervals/clamps change rarely; refresh daily, keep the old
            // map on failure.
            if funding_age.elapsed() >= FUNDING_INFO_REFRESH {
                funding_age = tokio::time::Instant::now();
                match crate::rest::fetch_funding_info().await {
                    Ok(map) => self.funding = map,
                    Err(e) => {
                        tracing::warn!(error = %e, "fundingInfo refresh failed; keeping previous map")
                    }
                }
            }

            let url = format!("{BASE_REST_URL}/fapi/v1/premiumIndex");
            match fetch_raw(&self.client, &url, &self.raw).await {
                Ok(body) => match serde_json::from_str::<Vec<PremiumIndexEntry>>(&body) {
                    Ok(entries) => {
                        let events = premium_index_events(
                            entries,
                            &self.venue_id,
                            &self.funding,
                            now_nanos(),
                        );
                        let n = events.len();
                        if let Err(e) = self.sink.send_batch(events).await {
                            tracing::warn!(error = ?e, "premium-index batch dropped (sink closing?)");
                        } else {
                            tracing::debug!(events = n, "premium-index poll recorded");
                        }
                        report_ok(&mut health, &self.sink, &self.venue_id, &label).await;
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "premiumIndex parse failed");
                        report_fail(
                            &mut health,
                            &self.sink,
                            &self.venue_id,
                            &label,
                            "parse error",
                        )
                        .await;
                    }
                },
                Err(e) => {
                    tracing::warn!(error = %e, "premiumIndex fetch failed");
                    report_fail(
                        &mut health,
                        &self.sink,
                        &self.venue_id,
                        &label,
                        "fetch error",
                    )
                    .await;
                }
            }
        }
        emit_control(
            &self.sink,
            &self.venue_id,
            ControlPayload::ConnDown {
                label,
                reason: "shutdown".into(),
            },
        )
        .await;
    }
}

impl<S: EventSink, R: RawFrameSink> IngestSource for PremiumIndexPoller<S, R> {
    fn label(&self) -> Arc<str> {
        "poller-premium-index".into()
    }

    fn source_id(&self) -> SourceId {
        SourceId::REST
    }

    fn run(self: Box<Self>, cancel: CancellationToken) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(self.run_inner(cancel))
    }
}

// --- Open interest (REST-only on Binance, weight 1, per-symbol) ---

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct OpenInterestResp {
    symbol: String,
    open_interest: Decimal,
    /// ms. The live endpoint carries no notional value; that column comes
    /// from the `openInterestHist` backfill only.
    time: u64,
}

pub(crate) fn open_interest_event(
    resp: OpenInterestResp,
    venue_id: &VenueId,
    local_ts: Nanos,
) -> Event {
    Event {
        venue: venue_id.clone(),
        instrument: Some(InstrumentId {
            value: resp.symbol.to_lowercase().into(),
        }),
        venue_ts: Some(ms_to_nanos(resp.time)),
        local_ts,
        source: SourceId::REST,
        provenance: None,
        payload: Payload::Market(MarketPayload::OpenInterest {
            open_interest: resp.open_interest,
            open_interest_value: None,
        }),
    }
}

pub(crate) struct OpenInterestPoller<S: EventSink, R: RawFrameSink> {
    pub sink: S,
    pub raw: R,
    pub venue_id: VenueId,
    pub universe: UniverseSource,
    pub every: Duration,
    pub client: reqwest::Client,
}

impl<S: EventSink, R: RawFrameSink> OpenInterestPoller<S, R> {
    async fn run_inner(self, cancel: CancellationToken) {
        let label: Arc<str> = "poller-open-interest".into();
        let mut health = Health::new();
        let mut sweep_start = tokio::time::Instant::now();
        loop {
            let universe = self.universe.current();
            if universe.is_empty() {
                tracing::warn!("open-interest poller has an empty universe; idling one interval");
            }
            // Spread the sweep across the whole interval so weight usage is
            // flat (~universe/interval req/s), not bursty.
            let pace = self
                .every
                .checked_div(universe.len().max(1) as u32)
                .unwrap_or(self.every);

            for symbol in universe.iter() {
                tokio::select! {
                    _ = cancel.cancelled() => {
                        emit_control(&self.sink, &self.venue_id, ControlPayload::ConnDown {
                            label: label.clone(), reason: "shutdown".into(),
                        }).await;
                        return;
                    }
                    _ = tokio::time::sleep(pace) => {}
                }
                let url = format!(
                    "{BASE_REST_URL}/fapi/v1/openInterest?symbol={}",
                    symbol.to_uppercase()
                );
                match fetch_raw(&self.client, &url, &self.raw).await {
                    Ok(body) => match serde_json::from_str::<OpenInterestResp>(&body) {
                        Ok(resp) => {
                            let event = open_interest_event(resp, &self.venue_id, now_nanos());
                            if let Err(e) = self.sink.send(event).await {
                                tracing::warn!(error = ?e, %symbol, "open-interest event dropped");
                            }
                            report_ok(&mut health, &self.sink, &self.venue_id, &label).await;
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, %symbol, "openInterest parse failed");
                            report_fail(
                                &mut health,
                                &self.sink,
                                &self.venue_id,
                                &label,
                                "parse error",
                            )
                            .await;
                        }
                    },
                    // 4xx here is routine — symbols settle/delist mid-sweep
                    // (code -4108); the universe catches up on its next
                    // refresh. Don't count it against poller health.
                    Err(e) if e.contains("400") => {
                        tracing::debug!(%symbol, error = %e, "openInterest rejected (settling/delisted?)");
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, %symbol, "openInterest fetch failed");
                        report_fail(
                            &mut health,
                            &self.sink,
                            &self.venue_id,
                            &label,
                            "fetch error",
                        )
                        .await;
                    }
                }
            }

            // Guard against a degenerate sweep (empty universe / all-skips)
            // spinning hot: never start the next sweep before one interval
            // has passed.
            let next_sweep = sweep_start + self.every;
            sweep_start = tokio::time::Instant::now().max(next_sweep);
            tokio::select! {
                _ = cancel.cancelled() => {
                    emit_control(&self.sink, &self.venue_id, ControlPayload::ConnDown {
                        label: label.clone(), reason: "shutdown".into(),
                    }).await;
                    return;
                }
                _ = tokio::time::sleep_until(next_sweep) => {}
            }
        }
    }
}

impl<S: EventSink, R: RawFrameSink> IngestSource for OpenInterestPoller<S, R> {
    fn label(&self) -> Arc<str> {
        "poller-open-interest".into()
    }

    fn source_id(&self) -> SourceId {
        SourceId::REST
    }

    fn run(self: Box<Self>, cancel: CancellationToken) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(self.run_inner(cancel))
    }
}

// --- Realized funding (the coverage-clock source) ---

/// One row of `GET /fapi/v1/fundingRate` (venue-wide with `startTime`,
/// ascending by fundingTime; live-verified 2026-06-11 — note the ms jitter
/// on settlement timestamps, e.g. `1781172000005`).
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct FundingRateEntry {
    symbol: String,
    funding_time: u64,
    funding_rate: Decimal,
}

/// Dedup state across overlapping fetch windows. Keys are pruned past the
/// catch-up horizon so memory stays bounded over weeks of uptime.
pub(crate) struct FundingSeen {
    seen: std::collections::HashSet<(String, u64)>,
    pub max_seen_ms: u64,
}

impl FundingSeen {
    pub(crate) fn new(start_ms: u64) -> Self {
        Self {
            seen: std::collections::HashSet::new(),
            max_seen_ms: start_ms,
        }
    }

    fn prune(&mut self) {
        let horizon = self.max_seen_ms.saturating_sub(FUNDING_CATCHUP_MS);
        self.seen.retain(|(_, ft)| *ft >= horizon);
    }
}

/// Pure ingest: new (unseen) rows become FundingRateRealized events with
/// `venue_ts = funding_time`; state advances and is pruned.
pub(crate) fn funding_realized_events(
    rows: Vec<FundingRateEntry>,
    state: &mut FundingSeen,
    venue_id: &VenueId,
    funding: &FundingMap,
    local_ts: Nanos,
) -> Vec<Event> {
    let mut out = Vec::new();
    for row in rows {
        let symbol = row.symbol.to_lowercase();
        if !state.seen.insert((symbol.clone(), row.funding_time)) {
            continue;
        }
        state.max_seen_ms = state.max_seen_ms.max(row.funding_time);
        let interval = funding
            .get(&symbol)
            .map_or(DEFAULT_FUNDING_INTERVAL_NS, |m| m.interval);
        out.push(Event {
            venue: venue_id.clone(),
            instrument: Some(InstrumentId {
                value: symbol.into(),
            }),
            venue_ts: Some(ms_to_nanos(row.funding_time)),
            local_ts,
            source: SourceId::REST,
            provenance: None,
            payload: Payload::Market(MarketPayload::FundingRateRealized {
                rate: row.funding_rate,
                funding_time: ms_to_nanos(row.funding_time),
                interval: Some(interval),
            }),
        });
    }
    state.prune();
    out
}

pub(crate) struct FundingRealizedPoller<S: EventSink, R: RawFrameSink> {
    pub sink: S,
    pub raw: R,
    pub venue_id: VenueId,
    pub funding: FundingMap,
    pub every: Duration,
    pub client: reqwest::Client,
}

impl<S: EventSink, R: RawFrameSink> FundingRealizedPoller<S, R> {
    async fn run_inner(mut self, cancel: CancellationToken) {
        let label: Arc<str> = "poller-funding-realized".into();
        let mut health = Health::new();
        // Catch-up window: a restart re-fetches the trailing 2 h; downstream
        // dedups on (instrument, funding_time), same stance QA takes.
        let now_ms = now_nanos() / 1_000_000;
        let mut state = FundingSeen::new(now_ms.saturating_sub(FUNDING_CATCHUP_MS));
        let mut funding_age = tokio::time::Instant::now();
        let mut next = tokio::time::Instant::now();
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = tokio::time::sleep_until(next) => {}
            }
            next = tokio::time::Instant::now() + self.every;

            if funding_age.elapsed() >= FUNDING_INFO_REFRESH {
                funding_age = tokio::time::Instant::now();
                match crate::rest::fetch_funding_info().await {
                    Ok(map) => self.funding = map,
                    Err(e) => {
                        tracing::warn!(error = %e, "fundingInfo refresh failed; keeping previous map")
                    }
                }
            }

            // Saturation paging: a full page means more rows exist past it;
            // advance startTime and keep going. A page failure aborts the
            // cycle — max_seen survives, the next cycle resumes from there.
            let mut start_ms = state.max_seen_ms.saturating_sub(FUNDING_LOOKBACK_MS);
            loop {
                let url = format!(
                    "{BASE_REST_URL}/fapi/v1/fundingRate?startTime={start_ms}&limit={FUNDING_PAGE_LIMIT}"
                );
                let body = match fetch_raw(&self.client, &url, &self.raw).await {
                    Ok(b) => b,
                    Err(e) => {
                        tracing::warn!(error = %e, "fundingRate fetch failed");
                        report_fail(
                            &mut health,
                            &self.sink,
                            &self.venue_id,
                            &label,
                            "fetch error",
                        )
                        .await;
                        break;
                    }
                };
                let rows = match serde_json::from_str::<Vec<FundingRateEntry>>(&body) {
                    Ok(r) => r,
                    Err(e) => {
                        tracing::warn!(error = %e, "fundingRate parse failed");
                        report_fail(
                            &mut health,
                            &self.sink,
                            &self.venue_id,
                            &label,
                            "parse error",
                        )
                        .await;
                        break;
                    }
                };
                let page_len = rows.len();
                let page_last_ms = rows.last().map(|r| r.funding_time);
                let events = funding_realized_events(
                    rows,
                    &mut state,
                    &self.venue_id,
                    &self.funding,
                    now_nanos(),
                );
                let n = events.len();
                if n > 0 {
                    tracing::info!(events = n, "realized funding recorded");
                }
                for event in events {
                    if let Err(e) = self.sink.send(event).await {
                        tracing::warn!(error = ?e, "funding-realized event dropped");
                    }
                }
                report_ok(&mut health, &self.sink, &self.venue_id, &label).await;

                if page_len < FUNDING_PAGE_LIMIT {
                    break;
                }
                // Advance to the last settlement instant *inclusive*: a full
                // page can split one instant's rows across the boundary, and
                // `last + 1` would skip the remainder. Re-fetched rows dedup
                // via the seen-set; `last > start` guards progress (one
                // instant holds < limit rows: one per symbol).
                match page_last_ms {
                    Some(last) if last > start_ms => start_ms = last,
                    _ => break,
                }
                // Pace between saturated pages; cancel stays responsive.
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = tokio::time::sleep(Duration::from_millis(250)) => {}
                }
            }
        }
        emit_control(
            &self.sink,
            &self.venue_id,
            ControlPayload::ConnDown {
                label,
                reason: "shutdown".into(),
            },
        )
        .await;
    }
}

impl<S: EventSink, R: RawFrameSink> IngestSource for FundingRealizedPoller<S, R> {
    fn label(&self) -> Arc<str> {
        "poller-funding-realized".into()
    }

    fn source_id(&self) -> SourceId {
        SourceId::REST
    }

    fn run(self: Box<Self>, cancel: CancellationToken) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(self.run_inner(cancel))
    }
}

pub(crate) fn poller_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(HTTP_TIMEOUT)
        .build()
        .expect("reqwest client construction is infallible with these options")
}

/// Fixture tests from live-captured responses (P4 discipline): the JSON
/// below is verbatim from fapi probes on 2026-06-11.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::rest::FundingMeta;
    use rust_decimal_macros::dec;

    fn venue() -> VenueId {
        VenueId {
            value: "binance".into(),
        }
    }

    const T_LOCAL: Nanos = 1_781_174_100_000_000_000;

    #[test]
    fn premium_index_row_fans_out_three_events() {
        let entries: Vec<PremiumIndexEntry> = serde_json::from_str(
            r#"[{"symbol":"NEIROUSDT","markPrice":"0.00006777","indexPrice":"0.00006789","estimatedSettlePrice":"0.00006836","lastFundingRate":"0.00005000","interestRate":"0.00010000","nextFundingTime":1781179200000,"time":1781174040000}]"#,
        )
        .unwrap();
        let events = premium_index_events(entries, &venue(), &FundingMap::default(), T_LOCAL);
        assert_eq!(events.len(), 3);
        for e in &events {
            assert_eq!(e.instrument.as_ref().unwrap().value.as_ref(), "neirousdt");
            assert_eq!(e.venue_ts, Some(1_781_174_040_000_000_000));
            assert_eq!(e.local_ts, T_LOCAL);
            assert_eq!(e.source, SourceId::REST);
        }
        assert!(matches!(
            &events[0].payload,
            Payload::Market(MarketPayload::MarkPrice { price }) if *price == dec!(0.00006777)
        ));
        assert!(matches!(
            &events[1].payload,
            Payload::Market(MarketPayload::IndexPrice { price }) if *price == dec!(0.00006789)
        ));
        match &events[2].payload {
            Payload::Market(MarketPayload::FundingRatePrediction {
                rate,
                next_funding_time,
                interval,
                premium_index,
                clamp_min,
                clamp_max,
            }) => {
                assert_eq!(*rate, dec!(0.00005000));
                assert_eq!(*next_funding_time, 1_781_179_200_000_000_000);
                assert_eq!(*interval, Some(8 * 3600 * 1_000_000_000));
                assert_eq!(*premium_index, None);
                assert_eq!(*clamp_min, None);
                assert_eq!(*clamp_max, None);
            }
            other => panic!("unexpected payload: {other:?}"),
        }
    }

    #[test]
    fn settling_symbol_emits_no_prediction() {
        // Live row: COMMONUSDT mid-settlement, nextFundingTime = 0.
        let entries: Vec<PremiumIndexEntry> = serde_json::from_str(
            r#"[{"symbol":"COMMONUSDT","markPrice":"0.00045217","indexPrice":"0.00045217","estimatedSettlePrice":"0.00000000","lastFundingRate":"0.00000000","interestRate":"0.00000000","nextFundingTime":0,"time":1781174040000}]"#,
        )
        .unwrap();
        let events = premium_index_events(entries, &venue(), &FundingMap::default(), T_LOCAL);
        assert_eq!(events.len(), 2, "mark + index only, no prediction");
        assert!(matches!(
            &events[0].payload,
            Payload::Market(MarketPayload::MarkPrice { .. })
        ));
        assert!(matches!(
            &events[1].payload,
            Payload::Market(MarketPayload::IndexPrice { .. })
        ));
    }

    #[test]
    fn premium_index_stamps_funding_meta() {
        let mut map = std::collections::HashMap::new();
        map.insert(
            "btcusdt_260626".to_string(),
            FundingMeta {
                interval: 4 * 3600 * 1_000_000_000,
                cap: Some(dec!(0.02)),
                floor: Some(dec!(-0.02)),
            },
        );
        let funding: FundingMap = Arc::new(map);
        let entries: Vec<PremiumIndexEntry> = serde_json::from_str(
            r#"[{"symbol":"BTCUSDT_260626","markPrice":"110000.1","indexPrice":"110000.2","estimatedSettlePrice":"110000.3","lastFundingRate":"0.0001","interestRate":"0.0001","nextFundingTime":1781179200000,"time":1781174040000}]"#,
        )
        .unwrap();
        let events = premium_index_events(entries, &venue(), &funding, T_LOCAL);
        match &events[2].payload {
            Payload::Market(MarketPayload::FundingRatePrediction {
                interval,
                clamp_min,
                clamp_max,
                ..
            }) => {
                assert_eq!(*interval, Some(4 * 3600 * 1_000_000_000));
                assert_eq!(*clamp_min, Some(dec!(-0.02)));
                assert_eq!(*clamp_max, Some(dec!(0.02)));
            }
            other => panic!("unexpected payload: {other:?}"),
        }
    }

    #[test]
    fn open_interest_parses_live_shape() {
        // Verbatim live response; no notional value on this endpoint.
        let resp: OpenInterestResp = serde_json::from_str(
            r#"{"symbol":"BTCUSDT","openInterest":"101489.798","time":1781174049921}"#,
        )
        .unwrap();
        let e = open_interest_event(resp, &venue(), T_LOCAL);
        assert_eq!(e.instrument.as_ref().unwrap().value.as_ref(), "btcusdt");
        assert_eq!(e.venue_ts, Some(1_781_174_049_921_000_000));
        assert_eq!(e.source, SourceId::REST);
        match &e.payload {
            Payload::Market(MarketPayload::OpenInterest {
                open_interest,
                open_interest_value,
            }) => {
                assert_eq!(*open_interest, dec!(101489.798));
                assert_eq!(*open_interest_value, None);
            }
            other => panic!("unexpected payload: {other:?}"),
        }
    }

    /// Live page shape, including the ms jitter on fundingTime
    /// (1781172000005) that dedup keys must preserve verbatim.
    const FUNDING_PAGE: &str = r#"[{"symbol":"PLAYUSDT","fundingTime":1781168400000,"fundingRate":"-0.00287339","markPrice":"0.04887680"},{"symbol":"GUAUSDT","fundingTime":1781172000005,"fundingRate":"0.00001250","markPrice":"0.52461265"},{"symbol":"PLAYUSDT","fundingTime":1781172000005,"fundingRate":"-0.00540630","markPrice":"0.04863000"}]"#;

    #[test]
    fn funding_realized_emits_and_dedups_across_overlapping_windows() {
        let mut state = FundingSeen::new(1_781_160_000_000);
        let rows: Vec<FundingRateEntry> = serde_json::from_str(FUNDING_PAGE).unwrap();
        let events =
            funding_realized_events(rows, &mut state, &venue(), &FundingMap::default(), T_LOCAL);
        assert_eq!(events.len(), 3);
        assert_eq!(state.max_seen_ms, 1_781_172_000_005);

        let e = &events[1];
        assert_eq!(e.instrument.as_ref().unwrap().value.as_ref(), "guausdt");
        assert_eq!(e.venue_ts, Some(1_781_172_000_005_000_000));
        match &e.payload {
            Payload::Market(MarketPayload::FundingRateRealized {
                rate,
                funding_time,
                interval,
            }) => {
                assert_eq!(*rate, dec!(0.00001250));
                assert_eq!(*funding_time, 1_781_172_000_005_000_000);
                assert_eq!(*interval, Some(8 * 3600 * 1_000_000_000));
            }
            other => panic!("unexpected payload: {other:?}"),
        }

        // Same page again (the per-cycle 1 h lookback re-fetches): no dupes.
        let rows: Vec<FundingRateEntry> = serde_json::from_str(FUNDING_PAGE).unwrap();
        let events =
            funding_realized_events(rows, &mut state, &venue(), &FundingMap::default(), T_LOCAL);
        assert!(events.is_empty(), "overlapping window must dedup");

        // Same settlement instant, different symbol: distinct key, emitted.
        let rows: Vec<FundingRateEntry> = serde_json::from_str(
            r#"[{"symbol":"HUSDT","fundingTime":1781172000005,"fundingRate":"-0.00048076","markPrice":"0.14379000"}]"#,
        )
        .unwrap();
        let events =
            funding_realized_events(rows, &mut state, &venue(), &FundingMap::default(), T_LOCAL);
        assert_eq!(events.len(), 1);
    }

    #[test]
    fn funding_seen_prunes_but_keeps_catchup_horizon() {
        let mut state = FundingSeen::new(0);
        let old = r#"[{"symbol":"AUSDT","fundingTime":1000,"fundingRate":"0.0001"}]"#;
        let rows: Vec<FundingRateEntry> = serde_json::from_str(old).unwrap();
        funding_realized_events(rows, &mut state, &venue(), &FundingMap::default(), T_LOCAL);
        assert_eq!(state.seen.len(), 1);

        // A row far past the catch-up horizon prunes the old key …
        let newer = format!(
            r#"[{{"symbol":"AUSDT","fundingTime":{},"fundingRate":"0.0001"}}]"#,
            1000 + FUNDING_CATCHUP_MS + 1
        );
        let rows: Vec<FundingRateEntry> = serde_json::from_str(&newer).unwrap();
        funding_realized_events(rows, &mut state, &venue(), &FundingMap::default(), T_LOCAL);
        assert_eq!(state.seen.len(), 1, "old key pruned, new key kept");

        // … so re-feeding the pruned row re-emits (acceptable: downstream
        // keys on (instrument, funding_time); pruned rows are 2 h stale and
        // only reappear if the venue re-serves them).
        let rows: Vec<FundingRateEntry> = serde_json::from_str(old).unwrap();
        let events =
            funding_realized_events(rows, &mut state, &venue(), &FundingMap::default(), T_LOCAL);
        assert_eq!(events.len(), 1);
    }

    #[test]
    fn health_announces_up_once_and_down_after_threshold() {
        let mut h = Health::new();
        assert!(h.ok(), "first success announces ConnUp");
        assert!(!h.ok(), "repeat success silent");
        assert!(!h.fail(), "1st failure silent");
        assert!(!h.fail(), "2nd failure silent");
        assert!(h.fail(), "3rd consecutive failure announces ConnDown");
        assert!(!h.fail(), "further failures silent");
        assert!(h.ok(), "recovery announces ConnUp again");

        // Blip pattern: failures never reach the threshold, no flapping.
        let mut h = Health::new();
        h.ok();
        assert!(!h.fail());
        assert!(!h.fail());
        assert!(!h.ok());
        assert!(!h.fail(), "counter reset by success");
    }
}
