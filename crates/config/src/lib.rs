//! Capture-process configuration (improvement_plan step 11).
//!
//! One TOML file per venue process. Parsing is strict (`deny_unknown_fields`
//! everywhere) and validation is loud: a config that names a stream the
//! process cannot actually capture is rejected at startup instead of
//! producing silent zero-data. Secrets never live here (env only; none are
//! needed before Phase 6 execution).

use serde::Deserialize;
use std::path::{Path, PathBuf};
use venue_adapter::{DataType, Scope, Subscription};
use venue_core::InstrumentId;

#[derive(Debug)]
pub enum ConfigError {
    Io(std::io::Error),
    Parse(toml::de::Error),
    Invalid(String),
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigError::Io(e) => write!(f, "config read failed: {e}"),
            ConfigError::Parse(e) => write!(f, "config parse failed: {e}"),
            ConfigError::Invalid(msg) => write!(f, "config invalid: {msg}"),
        }
    }
}

impl std::error::Error for ConfigError {}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub venue: VenueCfg,
    pub paths: PathsCfg,
    #[serde(default)]
    pub logging: LoggingCfg,
    #[serde(default)]
    pub capture: CaptureCfg,
    pub subscriptions: Vec<SubscriptionCfg>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VenueCfg {
    /// Capture namespace; also the WAL/raw directory name. Known venues only.
    pub id: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PathsCfg {
    /// Root of the data layout; everything else derives from it.
    pub data_dir: PathBuf,
}

impl PathsCfg {
    pub fn wal_dir(&self) -> PathBuf {
        self.data_dir.join("wal")
    }

    pub fn raw_dir(&self) -> PathBuf {
        self.data_dir.join("raw")
    }

    pub fn parquet_dir(&self) -> PathBuf {
        self.data_dir.join("parquet")
    }

    pub fn meta_dir(&self) -> PathBuf {
        self.data_dir.join("meta")
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoggingCfg {
    /// tracing `EnvFilter` directive string; the `RUST_LOG` env var wins.
    #[serde(default = "default_filter")]
    pub filter: String,
}

impl Default for LoggingCfg {
    fn default() -> Self {
        Self {
            filter: default_filter(),
        }
    }
}

fn default_filter() -> String {
    "info".to_string()
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CaptureCfg {
    /// Raw-frame tee (R2). Default ON: any venue still in bring-up keeps the
    /// "parser bug = re-run normalization" safety net.
    #[serde(default = "default_true")]
    pub raw_tee: bool,
    /// Heartbeat log cadence (P5d).
    #[serde(default = "default_heartbeat_secs")]
    pub heartbeat_secs: u64,
}

impl Default for CaptureCfg {
    fn default() -> Self {
        Self {
            raw_tee: true,
            heartbeat_secs: default_heartbeat_secs(),
        }
    }
}

fn default_true() -> bool {
    true
}

fn default_heartbeat_secs() -> u64 {
    60
}

/// One subscription: either an explicit instrument list or `all = true`
/// (venue-wide array streams), never both.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubscriptionCfg {
    /// Venue-raw instrument ids (Binance: lowercase symbols; normalized here).
    #[serde(default)]
    pub instruments: Vec<String>,
    #[serde(default)]
    pub all: bool,
    pub data: Vec<DataTypeCfg>,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DataTypeCfg {
    BookTicker,
    BookDepth,
    Trade,
    FundingRate,
    MarkPrice,
    IndexPrice,
    Liquidation,
    OpenInterest,
}

impl DataTypeCfg {
    pub fn to_data_type(self) -> DataType {
        match self {
            DataTypeCfg::BookTicker => DataType::BookTicker,
            DataTypeCfg::BookDepth => DataType::BookDepth,
            DataTypeCfg::Trade => DataType::Trade,
            DataTypeCfg::FundingRate => DataType::FundingRate,
            DataTypeCfg::MarkPrice => DataType::MarkPrice,
            DataTypeCfg::IndexPrice => DataType::IndexPrice,
            DataTypeCfg::Liquidation => DataType::Liquidation,
            DataTypeCfg::OpenInterest => DataType::OpenInterest,
        }
    }

    fn name(self) -> &'static str {
        match self {
            DataTypeCfg::BookTicker => "book_ticker",
            DataTypeCfg::BookDepth => "book_depth",
            DataTypeCfg::Trade => "trade",
            DataTypeCfg::FundingRate => "funding_rate",
            DataTypeCfg::MarkPrice => "mark_price",
            DataTypeCfg::IndexPrice => "index_price",
            DataTypeCfg::Liquidation => "liquidation",
            DataTypeCfg::OpenInterest => "open_interest",
        }
    }
}

/// Why a data type cannot be captured on Binance USD-M today, if it can't.
/// Live-verified 2026-06-10: fapi ACKs SUBSCRIBE for the whole `markPrice`
/// stream family (per-symbol and `!markPrice@arr`, both cadences, both
/// endpoints) and then emits nothing — the same acked-but-dead shape as the
/// removed `@aggTrade`. Rejecting these here turns silent zero-data into a
/// startup error; mark/index/funding capture arrives with the Phase-2 REST
/// poller (`/fapi/v1/premiumIndex`, verified live).
fn binance_unsupported_reason(dt: DataTypeCfg, venue_wide: bool) -> Option<&'static str> {
    match dt {
        DataTypeCfg::OpenInterest => Some(
            "open_interest is REST-only on Binance and not captured yet \
             (Phase-2 poller) — remove it from the config",
        ),
        DataTypeCfg::MarkPrice | DataTypeCfg::IndexPrice | DataTypeCfg::FundingRate => Some(
            "the Binance markPrice WS stream family is acked but emits nothing \
             (live-verified 2026-06-10); mark/index/funding capture arrives with \
             the Phase-2 REST poller — remove it from the config",
        ),
        DataTypeCfg::BookTicker | DataTypeCfg::BookDepth | DataTypeCfg::Trade if venue_wide => {
            Some("no venue-wide Binance stream for this data type — list instruments explicitly")
        }
        _ => None,
    }
}

impl SubscriptionCfg {
    pub fn to_subscription(&self) -> Subscription {
        let scope = if self.all {
            Scope::All
        } else {
            Scope::Instruments(
                self.instruments
                    .iter()
                    .map(|s| InstrumentId {
                        value: s.to_lowercase().into(),
                    })
                    .collect(),
            )
        };
        Subscription {
            scope,
            data: self.data.iter().map(|d| d.to_data_type()).collect(),
        }
    }
}

impl Config {
    /// All configured subscriptions in adapter form.
    pub fn subscriptions(&self) -> Vec<Subscription> {
        self.subscriptions
            .iter()
            .map(SubscriptionCfg::to_subscription)
            .collect()
    }

    /// Every configured explicit instrument id (lowercased, deduplicated) —
    /// used by venue-process to validate symbols against exchangeInfo.
    pub fn explicit_instruments(&self) -> Vec<String> {
        let mut ids: Vec<String> = self
            .subscriptions
            .iter()
            .flat_map(|s| s.instruments.iter().map(|i| i.to_lowercase()))
            .collect();
        ids.sort();
        ids.dedup();
        ids
    }

    fn validate(&self) -> Result<(), ConfigError> {
        let invalid = |msg: String| Err(ConfigError::Invalid(msg));

        if self.venue.id != "binance" {
            return invalid(format!(
                "unknown venue id {:?}; supported: \"binance\"",
                self.venue.id
            ));
        }
        if self.subscriptions.is_empty() {
            return invalid("no [[subscriptions]] configured; nothing to capture".into());
        }

        for (i, sub) in self.subscriptions.iter().enumerate() {
            let at = format!("subscriptions[{i}]");
            match (sub.all, sub.instruments.is_empty()) {
                (true, false) => {
                    return invalid(format!(
                        "{at}: `all = true` and `instruments` are mutually exclusive"
                    ));
                }
                (false, true) => {
                    return invalid(format!(
                        "{at}: set either `instruments = [..]` or `all = true`"
                    ));
                }
                _ => {}
            }
            if sub.data.is_empty() {
                return invalid(format!("{at}: `data` is empty"));
            }
            for dt in &sub.data {
                if let Some(reason) = binance_unsupported_reason(*dt, sub.all) {
                    return invalid(format!("{at}: `{}`: {reason}", dt.name()));
                }
            }
        }
        Ok(())
    }
}

pub fn parse_str(raw: &str) -> Result<Config, ConfigError> {
    let config: Config = toml::from_str(raw).map_err(ConfigError::Parse)?;
    config.validate()?;
    Ok(config)
}

pub fn load(path: &Path) -> Result<Config, ConfigError> {
    let raw = std::fs::read_to_string(path).map_err(ConfigError::Io)?;
    parse_str(&raw)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The committed example config is itself the golden parse fixture.
    const EXAMPLE: &str = include_str!("../../../configs/binance.toml");

    fn minimal(subscriptions: &str) -> String {
        format!("[venue]\nid = \"binance\"\n[paths]\ndata_dir = \"data\"\n{subscriptions}")
    }

    #[test]
    fn example_config_parses_and_validates() {
        let cfg = parse_str(EXAMPLE).expect("configs/binance.toml must stay valid");
        assert_eq!(cfg.venue.id, "binance");
        assert_eq!(cfg.paths.wal_dir(), PathBuf::from("data/wal"));
        assert_eq!(cfg.paths.raw_dir(), PathBuf::from("data/raw"));
        assert_eq!(cfg.paths.parquet_dir(), PathBuf::from("data/parquet"));
        assert_eq!(cfg.paths.meta_dir(), PathBuf::from("data/meta"));
        assert!(cfg.capture.raw_tee);
        assert_eq!(cfg.capture.heartbeat_secs, 60);
        assert_eq!(cfg.explicit_instruments(), vec!["btcusdt", "ethusdt"]);

        let subs = cfg.subscriptions();
        assert_eq!(subs.len(), 1);
        match &subs[0].scope {
            Scope::Instruments(ids) => {
                assert_eq!(ids.len(), 2);
                assert_eq!(ids[0].value.as_ref(), "btcusdt");
            }
            other => panic!("expected explicit scope, got {other:?}"),
        }
        assert!(subs[0].data.contains(&DataType::BookDepth));
    }

    #[test]
    fn defaults_apply_when_sections_omitted() {
        let cfg = parse_str(&minimal(
            "[[subscriptions]]\ninstruments = [\"btcusdt\"]\ndata = [\"trade\"]\n",
        ))
        .unwrap();
        assert_eq!(cfg.logging.filter, "info");
        assert!(cfg.capture.raw_tee);
        assert_eq!(cfg.capture.heartbeat_secs, 60);
    }

    #[test]
    fn unknown_fields_rejected_at_every_level() {
        for raw in [
            // top level
            format!("{}\nsurprise = 1\n", minimal("[[subscriptions]]\ninstruments=[\"a\"]\ndata=[\"trade\"]")),
            // venue level
            "[venue]\nid = \"binance\"\nregion = \"eu\"\n[paths]\ndata_dir = \"d\"\n[[subscriptions]]\ninstruments=[\"a\"]\ndata=[\"trade\"]".to_string(),
            // subscription level
            minimal("[[subscriptions]]\ninstruments=[\"a\"]\ndata=[\"trade\"]\ndepth=5\n"),
        ] {
            assert!(
                matches!(parse_str(&raw), Err(ConfigError::Parse(_))),
                "should reject unknown field in: {raw}"
            );
        }
    }

    #[test]
    fn instruments_xor_all_enforced() {
        let both = minimal(
            "[[subscriptions]]\ninstruments = [\"btcusdt\"]\nall = true\ndata = [\"trade\"]\n",
        );
        let neither = minimal("[[subscriptions]]\ndata = [\"trade\"]\n");
        for raw in [both, neither] {
            assert!(matches!(parse_str(&raw), Err(ConfigError::Invalid(_))));
        }
    }

    #[test]
    fn empty_subscriptions_and_empty_data_rejected() {
        // Top-level array key must precede the section headers.
        let no_subs = "subscriptions = []\n[venue]\nid = \"binance\"\n[paths]\ndata_dir = \"d\"\n";
        assert!(matches!(parse_str(no_subs), Err(ConfigError::Invalid(_))));
        assert!(matches!(
            parse_str(&minimal(
                "[[subscriptions]]\ninstruments = [\"btcusdt\"]\ndata = []\n"
            )),
            Err(ConfigError::Invalid(_))
        ));
    }

    #[test]
    fn venue_wide_rejects_types_without_array_streams() {
        for dt in ["book_ticker", "book_depth", "trade"] {
            let raw = minimal(&format!(
                "[[subscriptions]]\nall = true\ndata = [\"{dt}\"]\n"
            ));
            let err = parse_str(&raw).unwrap_err();
            assert!(
                matches!(&err, ConfigError::Invalid(msg) if msg.contains("venue-wide")),
                "{dt}: {err}"
            );
        }
        // Liquidation is the one type with a live venue-wide array stream.
        let ok = minimal("[[subscriptions]]\nall = true\ndata = [\"liquidation\"]\n");
        assert!(parse_str(&ok).is_ok());
    }

    #[test]
    fn uncapturable_types_rejected_in_both_scopes() {
        // open_interest: REST-only. mark/index/funding: WS stream family is
        // acked-but-silent on fapi (live-verified) until the Phase-2 poller.
        for dt in ["open_interest", "mark_price", "index_price", "funding_rate"] {
            for sub in [
                format!("[[subscriptions]]\ninstruments = [\"btcusdt\"]\ndata = [\"{dt}\"]\n"),
                format!("[[subscriptions]]\nall = true\ndata = [\"{dt}\"]\n"),
            ] {
                let err = parse_str(&minimal(&sub)).unwrap_err();
                assert!(
                    matches!(&err, ConfigError::Invalid(msg) if msg.contains(dt)),
                    "{dt}: {err}"
                );
            }
        }
    }

    #[test]
    fn unknown_venue_rejected() {
        let raw = "[venue]\nid = \"bybit\"\n[paths]\ndata_dir = \"d\"\n[[subscriptions]]\ninstruments=[\"a\"]\ndata=[\"trade\"]";
        assert!(matches!(parse_str(raw), Err(ConfigError::Invalid(_))));
    }

    #[test]
    fn instruments_lowercased_in_subscription_and_validation_list() {
        let cfg = parse_str(&minimal(
            "[[subscriptions]]\ninstruments = [\"BTCUSDT\"]\ndata = [\"trade\"]\n",
        ))
        .unwrap();
        assert_eq!(cfg.explicit_instruments(), vec!["btcusdt"]);
        match &cfg.subscriptions()[0].scope {
            Scope::Instruments(ids) => assert_eq!(ids[0].value.as_ref(), "btcusdt"),
            other => panic!("unexpected scope {other:?}"),
        }
    }

    #[test]
    fn every_data_type_maps() {
        let pairs = [
            (DataTypeCfg::BookTicker, DataType::BookTicker),
            (DataTypeCfg::BookDepth, DataType::BookDepth),
            (DataTypeCfg::Trade, DataType::Trade),
            (DataTypeCfg::FundingRate, DataType::FundingRate),
            (DataTypeCfg::MarkPrice, DataType::MarkPrice),
            (DataTypeCfg::IndexPrice, DataType::IndexPrice),
            (DataTypeCfg::Liquidation, DataType::Liquidation),
            (DataTypeCfg::OpenInterest, DataType::OpenInterest),
        ];
        for (cfg_dt, dt) in pairs {
            assert_eq!(cfg_dt.to_data_type(), dt);
        }
    }
}
