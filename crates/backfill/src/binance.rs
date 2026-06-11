//! Binance USD-M REST history source. Funding history is venue-wide
//! (`/fapi/v1/fundingRate` with no symbol returns every symbol's settlements
//! in [startTime, endTime), ascending — live-verified 2026-06-11), so one
//! chronological pass per month covers listed *and delisted* symbols.
//!
//! Rate notes: fundingRate + fundingInfo share a 500-requests/5-min budget
//! (separate from IP weight) → ~1.5 req/s pacer; `/futures/data/*` carries
//! 1000/5-min → ~2.9 req/s pacer for OI history, klines, exchangeInfo. A
//! full multi-year funding pull is ~1.5k pages ≈ 20 minutes, once.

use crate::klines::{Kline, KlineSource};
use crate::oi::{OiHistSource, OiPoint};
use crate::{BackfillError, FundingHistorySource, FundingPoint, Pacer, PerpMeta};
use chrono::Utc;
use rust_decimal::Decimal;
use serde::Deserialize;
use std::collections::HashSet;
use std::path::Path;
use std::time::Duration;
use tokio::sync::Mutex;

const BASE_REST_URL: &str = "https://fapi.binance.com";
const PAGE_LIMIT: usize = 1000;
/// `/futures/data/openInterestHist` max rows per request; one closed UTC day
/// at 5 m grain is 288 rows, comfortably one-shot.
const OI_HIST_LIMIT: usize = 500;

pub struct BinanceSource {
    client: reqwest::Client,
    /// fundingRate/fundingInfo share-group (500/5 min).
    funding_pacer: Mutex<Pacer>,
    /// Everything else (IP-weight metered).
    general_pacer: Mutex<Pacer>,
}

impl Default for BinanceSource {
    fn default() -> Self {
        Self::new()
    }
}

impl BinanceSource {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .expect("reqwest client construction is infallible with these options"),
            funding_pacer: Mutex::new(Pacer::new(Duration::from_millis(650))),
            general_pacer: Mutex::new(Pacer::new(Duration::from_millis(350))),
        }
    }

    fn log_weight(resp: &reqwest::Response) {
        if let Some(weight) = resp
            .headers()
            .get("x-mbx-used-weight-1m")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok())
        {
            if weight > 1800 {
                tracing::warn!(weight, "IP weight nearing the 2400/min budget");
            } else {
                tracing::debug!(weight, "binance weight");
            }
        }
    }

    async fn get(&self, pacer: &Mutex<Pacer>, url: &str) -> Result<String, BackfillError> {
        pacer.lock().await.wait().await;
        let resp = self
            .client
            .get(url)
            .send()
            .await
            .map_err(|e| BackfillError::Http(e.to_string()))?;
        Self::log_weight(&resp);
        resp.error_for_status()
            .map_err(|e| BackfillError::Http(e.to_string()))?
            .text()
            .await
            .map_err(|e| BackfillError::Http(e.to_string()))
    }

    /// Like `get` but surfaces non-2xx bodies (Binance encodes the OI-history
    /// retention edge as an HTTP error with `"code":-1130`).
    async fn get_lenient(
        &self,
        pacer: &Mutex<Pacer>,
        url: &str,
    ) -> Result<(bool, String), BackfillError> {
        pacer.lock().await.wait().await;
        let resp = self
            .client
            .get(url)
            .send()
            .await
            .map_err(|e| BackfillError::Http(e.to_string()))?;
        Self::log_weight(&resp);
        let ok = resp.status().is_success();
        let body = resp
            .text()
            .await
            .map_err(|e| BackfillError::Http(e.to_string()))?;
        Ok((ok, body))
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExchangeInfoResponse {
    symbols: Vec<SymbolInfo>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SymbolInfo {
    symbol: String,
    contract_type: String,
    #[serde(default)]
    onboard_date: Option<u64>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct FundingInfoEntry {
    symbol: String,
    #[serde(default)]
    funding_interval_hours: Option<u64>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct FundingRateEntry {
    symbol: String,
    funding_time: u64,
    funding_rate: Decimal,
}

impl FundingHistorySource for BinanceSource {
    fn venue(&self) -> &'static str {
        "binance"
    }

    fn venue_wide_history(&self) -> bool {
        true
    }

    async fn list_perps(&self, meta_dir: &Path) -> Result<Vec<PerpMeta>, BackfillError> {
        let body = self
            .get(
                &self.general_pacer,
                &format!("{BASE_REST_URL}/fapi/v1/exchangeInfo"),
            )
            .await?;
        // Same dump the venue process makes daily (P5a); only fill the gap
        // when no capture process ran today.
        let dir = meta_dir.join("binance");
        let path = dir.join(format!(
            "{}-exchangeInfo.json",
            Utc::now().format("%Y-%m-%d")
        ));
        if !path.exists() {
            std::fs::create_dir_all(&dir)?;
            std::fs::write(&path, &body)?;
            tracing::info!(path = %path.display(), "exchangeInfo dumped");
        }
        let info: ExchangeInfoResponse =
            serde_json::from_str(&body).map_err(|e| BackfillError::Parse(e.to_string()))?;

        let intervals: std::collections::HashMap<String, u64> = match self
            .get(
                &self.funding_pacer,
                &format!("{BASE_REST_URL}/fapi/v1/fundingInfo"),
            )
            .await
        {
            Ok(body) => serde_json::from_str::<Vec<FundingInfoEntry>>(&body)
                .map_err(|e| BackfillError::Parse(e.to_string()))?
                .into_iter()
                .filter_map(|e| {
                    e.funding_interval_hours
                        .map(|h| (e.symbol, h * 3_600_000_000_000))
                })
                .collect(),
            Err(e) => {
                tracing::warn!(error = %e, "fundingInfo fetch failed; intervals default to None");
                Default::default()
            }
        };

        Ok(info
            .symbols
            .into_iter()
            .filter(|s| s.contract_type == "PERPETUAL")
            .map(|s| {
                let funding_interval_ns = intervals.get(&s.symbol).copied();
                PerpMeta {
                    onboard_ms: s.onboard_date,
                    funding_interval_ns,
                    symbol: s.symbol,
                }
            })
            .collect())
    }

    async fn fetch_funding(
        &self,
        symbol: Option<&str>,
        start_ms: u64,
        end_ms: u64,
    ) -> Result<Vec<FundingPoint>, BackfillError> {
        let mut out = Vec::new();
        let mut seen: HashSet<(String, u64)> = HashSet::new();
        let mut start = start_ms;
        loop {
            let sym = symbol
                .map(|s| format!("&symbol={}", s.to_uppercase()))
                .unwrap_or_default();
            let url = format!(
                "{BASE_REST_URL}/fapi/v1/fundingRate?startTime={start}&endTime={end_ms}\
                 &limit={PAGE_LIMIT}{sym}"
            );
            let body = self.get(&self.funding_pacer, &url).await?;
            let rows: Vec<FundingRateEntry> =
                serde_json::from_str(&body).map_err(|e| BackfillError::Parse(e.to_string()))?;
            let page_len = rows.len();
            let page_last = rows.last().map(|r| r.funding_time);
            for row in rows {
                if row.funding_time < start_ms || row.funding_time >= end_ms {
                    continue;
                }
                if seen.insert((row.symbol.clone(), row.funding_time)) {
                    out.push(FundingPoint {
                        symbol: row.symbol,
                        funding_time_ms: row.funding_time,
                        rate: row.funding_rate,
                    });
                }
            }
            if page_len < PAGE_LIMIT {
                break;
            }
            // Advance to the last settlement instant *inclusive*: a full page
            // can split one instant's rows across the boundary, and skipping
            // past it (`last + 1`) would lose the remainder. Re-fetched rows
            // dedup via `seen`; progress is guaranteed because one instant
            // holds at most one row per symbol (< PAGE_LIMIT symbols).
            match page_last {
                Some(last) if last > start => start = last,
                _ => break,
            }
        }
        Ok(out)
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct OiHistEntry {
    sum_open_interest: Decimal,
    sum_open_interest_value: Decimal,
    timestamp: u64,
}

impl OiHistSource for BinanceSource {
    fn venue(&self) -> &'static str {
        "binance"
    }

    async fn list_perps(&self, meta_dir: &Path) -> Result<Vec<PerpMeta>, BackfillError> {
        FundingHistorySource::list_perps(self, meta_dir).await
    }

    async fn fetch_oi_hist(
        &self,
        symbol: &str,
        start_ms: u64,
        end_ms: u64,
    ) -> Result<Vec<OiPoint>, BackfillError> {
        let url = format!(
            "{BASE_REST_URL}/futures/data/openInterestHist?symbol={}&period=5m\
             &startTime={start_ms}&endTime={}&limit={OI_HIST_LIMIT}",
            symbol.to_uppercase(),
            end_ms - 1
        );
        let (ok, body) = self.get_lenient(&self.general_pacer, &url).await?;
        if !ok {
            // The retention edge (≈30 d) and never-listed symbols come back
            // as parameter errors, not empty arrays (live-verified: -1130).
            if body.contains("-1130") || body.contains("-4108") || body.contains("-1121") {
                tracing::debug!(
                    symbol,
                    start_ms,
                    "OI history window rejected (retention edge)"
                );
                return Ok(Vec::new());
            }
            return Err(BackfillError::Http(body));
        }
        let rows: Vec<OiHistEntry> =
            serde_json::from_str(&body).map_err(|e| BackfillError::Parse(e.to_string()))?;
        if rows.len() == OI_HIST_LIMIT {
            tracing::warn!(
                symbol,
                start_ms,
                "OI history window hit the page limit; widen day partitioning?"
            );
        }
        Ok(rows
            .into_iter()
            .filter(|r| r.timestamp >= start_ms && r.timestamp < end_ms)
            .map(|r| OiPoint {
                ts_ms: r.timestamp,
                sum_open_interest: r.sum_open_interest,
                sum_open_interest_value: r.sum_open_interest_value,
            })
            .collect())
    }
}

/// Kline rows arrive as positional JSON arrays:
/// [openTime, open, high, low, close, volume, closeTime, quoteVolume,
///  trades, takerBuyBase, takerBuyQuote, unused].
type KlineRow = (
    u64,
    Decimal,
    Decimal,
    Decimal,
    Decimal,
    Decimal,
    u64,
    Decimal,
    u64,
    Decimal,
    Decimal,
    serde_json::Value,
);

impl KlineSource for BinanceSource {
    fn venue(&self) -> &'static str {
        "binance"
    }

    async fn list_perps(&self, meta_dir: &Path) -> Result<Vec<PerpMeta>, BackfillError> {
        FundingHistorySource::list_perps(self, meta_dir).await
    }

    async fn fetch_klines_1h(
        &self,
        symbol: &str,
        start_ms: u64,
        end_ms: u64,
    ) -> Result<Vec<Kline>, BackfillError> {
        // One calendar month is ≤ 744 hourly bars < the 1000 limit: one shot.
        // endTime is inclusive on this endpoint.
        let url = format!(
            "{BASE_REST_URL}/fapi/v1/klines?symbol={}&interval=1h\
             &startTime={start_ms}&endTime={}&limit={PAGE_LIMIT}",
            symbol.to_uppercase(),
            end_ms - 1
        );
        let (ok, body) = self.get_lenient(&self.general_pacer, &url).await?;
        if !ok {
            if body.contains("-1121") {
                // Unknown symbol (delisted since exchangeInfo): no bars.
                return Ok(Vec::new());
            }
            return Err(BackfillError::Http(body));
        }
        let rows: Vec<KlineRow> =
            serde_json::from_str(&body).map_err(|e| BackfillError::Parse(e.to_string()))?;
        Ok(rows
            .into_iter()
            .filter(|r| r.0 >= start_ms && r.0 < end_ms)
            .map(|r| Kline {
                open_time_ms: r.0,
                open: r.1,
                high: r.2,
                low: r.3,
                close: r.4,
                volume: r.5,
                close_time_ms: r.6,
                quote_volume: r.7,
                trades: r.8,
                taker_buy_volume: r.9,
                taker_buy_quote_volume: r.10,
            })
            .collect())
    }
}
