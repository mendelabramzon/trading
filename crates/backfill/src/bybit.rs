//! Bybit v5 REST funding-history source — the cross-venue leg of the
//! funding-spread dataset, **history only** (no live adapter; that decision
//! belongs to Phase 5, which this pre-validates).
//!
//! Live-verified 2026-06-11: `/v5/market/instruments-info?category=linear`
//! returns all 687 linear symbols in one ≤1000 page (cursor loop kept for
//! safety) with `fundingInterval` in *minutes* and funding clamps;
//! `/v5/market/funding/history` is per-symbol, descending, ≤200 rows/page,
//! `endTime` inclusive, with depth ≥ 3 years on majors. Paging walks
//! backwards: endTime = oldest_seen − 1 until the window is covered.

use crate::{BackfillError, FundingHistorySource, FundingPoint, Pacer, PerpMeta};
use chrono::Utc;
use rust_decimal::Decimal;
use serde::Deserialize;
use std::path::Path;
use std::time::Duration;
use tokio::sync::Mutex;

const BASE_REST_URL: &str = "https://api.bybit.com";
const FUNDING_PAGE_LIMIT: usize = 200;

pub struct BybitSource {
    client: reqwest::Client,
    pacer: Mutex<Pacer>,
}

impl Default for BybitSource {
    fn default() -> Self {
        Self::new()
    }
}

impl BybitSource {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .expect("reqwest client construction is infallible with these options"),
            // Public market endpoints allow ~10 req/s/IP; stay well under.
            pacer: Mutex::new(Pacer::new(Duration::from_millis(150))),
        }
    }

    async fn get(&self, url: &str) -> Result<String, BackfillError> {
        self.pacer.lock().await.wait().await;
        self.client
            .get(url)
            .send()
            .await
            .map_err(|e| BackfillError::Http(e.to_string()))?
            .error_for_status()
            .map_err(|e| BackfillError::Http(e.to_string()))?
            .text()
            .await
            .map_err(|e| BackfillError::Http(e.to_string()))
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Envelope<T> {
    ret_code: i64,
    ret_msg: String,
    result: T,
}

fn unwrap_envelope<T>(body: &str) -> Result<T, BackfillError>
where
    T: serde::de::DeserializeOwned,
{
    let env: Envelope<T> =
        serde_json::from_str(body).map_err(|e| BackfillError::Parse(e.to_string()))?;
    if env.ret_code != 0 {
        return Err(BackfillError::Http(format!(
            "bybit retCode {}: {}",
            env.ret_code, env.ret_msg
        )));
    }
    Ok(env.result)
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct InstrumentsResult {
    list: Vec<InstrumentRow>,
    #[serde(default)]
    next_page_cursor: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct InstrumentRow {
    symbol: String,
    contract_type: String,
    /// Epoch ms as a quoted string.
    #[serde(default)]
    launch_time: Option<String>,
    /// Minutes (e.g. 240 = 4 h), unquoted.
    #[serde(default)]
    funding_interval: Option<u64>,
}

pub(crate) fn perp_meta(rows: Vec<InstrumentRow>) -> Vec<PerpMeta> {
    rows.into_iter()
        .filter(|r| r.contract_type == "LinearPerpetual")
        .map(|r| PerpMeta {
            onboard_ms: r.launch_time.and_then(|t| t.parse().ok()),
            funding_interval_ns: r.funding_interval.map(|mins| mins * 60 * 1_000_000_000),
            symbol: r.symbol,
        })
        .collect()
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct FundingResult {
    list: Vec<FundingRow>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct FundingRow {
    symbol: String,
    funding_rate: Decimal,
    /// Epoch ms as a quoted string.
    funding_rate_timestamp: String,
}

/// Descending page → ascending in-window points; returns the oldest ts seen
/// (the next backward `endTime` is that − 1).
pub(crate) fn ingest_funding_page(
    rows: Vec<FundingRow>,
    start_ms: u64,
    end_ms: u64,
    out: &mut Vec<FundingPoint>,
) -> Option<u64> {
    let mut oldest = None;
    for row in rows {
        let Ok(ts) = row.funding_rate_timestamp.parse::<u64>() else {
            tracing::warn!(symbol = %row.symbol, raw = %row.funding_rate_timestamp, "bad fundingRateTimestamp");
            continue;
        };
        oldest = Some(oldest.map_or(ts, |o: u64| o.min(ts)));
        if ts >= start_ms && ts < end_ms {
            out.push(FundingPoint {
                symbol: row.symbol,
                funding_time_ms: ts,
                rate: row.funding_rate,
            });
        }
    }
    oldest
}

impl FundingHistorySource for BybitSource {
    fn venue(&self) -> &'static str {
        "bybit"
    }

    fn venue_wide_history(&self) -> bool {
        false // per-symbol endpoint; the driver iterates list_perps
    }

    async fn list_perps(&self, meta_dir: &Path) -> Result<Vec<PerpMeta>, BackfillError> {
        let mut pages: Vec<String> = Vec::new();
        let mut rows: Vec<InstrumentRow> = Vec::new();
        let mut cursor = String::new();
        loop {
            let url = format!(
                "{BASE_REST_URL}/v5/market/instruments-info?category=linear&limit=1000{}",
                if cursor.is_empty() {
                    String::new()
                } else {
                    format!("&cursor={cursor}")
                }
            );
            let body = self.get(&url).await?;
            let result: InstrumentsResult = unwrap_envelope(&body)?;
            pages.push(body);
            rows.extend(result.list);
            match result.next_page_cursor {
                Some(next) if !next.is_empty() => cursor = next,
                _ => break,
            }
        }

        // Raw dump for the symbology/SCD builders (no live capture process
        // exists for this venue). Multi-page responses are stored verbatim
        // as a JSON array of page bodies; today it is one page.
        let dir = meta_dir.join("bybit");
        let path = dir.join(format!(
            "{}-instrumentsInfo.json",
            Utc::now().format("%Y-%m-%d")
        ));
        if !path.exists() {
            std::fs::create_dir_all(&dir)?;
            std::fs::write(&path, format!("[{}]", pages.join(",")))?;
            tracing::info!(path = %path.display(), pages = pages.len(), "instrumentsInfo dumped");
        }

        Ok(perp_meta(rows))
    }

    async fn fetch_funding(
        &self,
        symbol: Option<&str>,
        start_ms: u64,
        end_ms: u64,
    ) -> Result<Vec<FundingPoint>, BackfillError> {
        let Some(symbol) = symbol else {
            return Err(BackfillError::Invalid(
                "bybit funding history is per-symbol; the driver must iterate perps".into(),
            ));
        };
        let mut out = Vec::new();
        // endTime is inclusive and pages run newest-first.
        let mut end = end_ms - 1;
        loop {
            let url = format!(
                "{BASE_REST_URL}/v5/market/funding/history?category=linear&symbol={}\
                 &startTime={start_ms}&endTime={end}&limit={FUNDING_PAGE_LIMIT}",
                symbol.to_uppercase()
            );
            let body = self.get(&url).await?;
            let result: FundingResult = unwrap_envelope(&body)?;
            let page_len = result.list.len();
            let oldest = ingest_funding_page(result.list, start_ms, end_ms, &mut out);
            if page_len < FUNDING_PAGE_LIMIT {
                break;
            }
            match oldest {
                Some(ts) if ts > start_ms => end = ts - 1,
                _ => break,
            }
        }
        out.sort_by_key(|p| p.funding_time_ms);
        Ok(out)
    }
}

/// Fixture tests from live-captured responses (2026-06-11).
#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn instruments_row_parses_interval_minutes_and_launch_time() {
        // Trimmed live row: 0GUSDT, 4h funding interval as 240 minutes.
        let body = r#"{"retCode":0,"retMsg":"OK","result":{"category":"linear","list":[
            {"symbol":"0GUSDT","contractType":"LinearPerpetual","status":"Trading",
             "baseCoin":"0G","quoteCoin":"USDT","launchTime":"1758175736000",
             "fundingInterval":240,"settleCoin":"USDT",
             "upperFundingRate":"0.01","lowerFundingRate":"-0.01"},
            {"symbol":"BTC-26DEC25","contractType":"LinearFutures","launchTime":"1","fundingInterval":0}
        ]}}"#;
        let result: InstrumentsResult = unwrap_envelope(body).unwrap();
        let perps = perp_meta(result.list);
        assert_eq!(perps.len(), 1, "futures filtered out");
        assert_eq!(perps[0].symbol, "0GUSDT");
        assert_eq!(perps[0].onboard_ms, Some(1_758_175_736_000));
        assert_eq!(perps[0].funding_interval_ns, Some(240 * 60 * 1_000_000_000));
    }

    #[test]
    fn funding_page_ingests_descending_rows_and_reports_oldest() {
        // Verbatim live shape: descending, string timestamps.
        let body = r#"{"retCode":0,"retMsg":"OK","result":{"category":"linear","list":[
            {"symbol":"BTCUSDT","fundingRate":"0.00009561","fundingRateTimestamp":"1781164800000"},
            {"symbol":"BTCUSDT","fundingRate":"-0.00000813","fundingRateTimestamp":"1781136000000"},
            {"symbol":"BTCUSDT","fundingRate":"0.00003639","fundingRateTimestamp":"1781107200000"}
        ]}}"#;
        let result: FundingResult = unwrap_envelope(body).unwrap();
        let mut out = Vec::new();
        // Window excludes the newest row.
        let oldest =
            ingest_funding_page(result.list, 1_781_100_000_000, 1_781_150_000_000, &mut out);
        assert_eq!(oldest, Some(1_781_107_200_000));
        assert_eq!(out.len(), 2);
        assert_eq!(out[1].funding_time_ms, 1_781_107_200_000);
        assert_eq!(out[1].rate, dec!(0.00003639));
    }

    #[test]
    fn non_zero_ret_code_is_an_error() {
        let body = r#"{"retCode":10001,"retMsg":"params error","result":{}}"#;
        let err = unwrap_envelope::<serde_json::Value>(body).unwrap_err();
        assert!(matches!(err, BackfillError::Http(msg) if msg.contains("10001")));
    }
}
