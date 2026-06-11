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
    #[serde(default)]
    pub pollers: PollersCfg,
    #[serde(default)]
    pub universe: UniverseCfg,
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

/// REST poller cadences (Phase 2). All venue-wide; volume scales with the
/// venue's symbol count, so bounds are validated (5–3600 s). At the 30 s
/// premium-index default Binance produces ~80 events/s (~798 symbols × 3).
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PollersCfg {
    /// /fapi/v1/premiumIndex sweep: mark + index + funding prediction.
    #[serde(default = "default_premium_index_secs")]
    pub premium_index_secs: u64,
    /// /fapi/v1/openInterest round-robin over the perp universe; one full
    /// sweep per interval (matches the venue's own 5 m OI-history grain).
    #[serde(default = "default_open_interest_secs")]
    pub open_interest_secs: u64,
    /// /fapi/v1/fundingRate tail poll for realized funding events.
    #[serde(default = "default_funding_realized_secs")]
    pub funding_realized_secs: u64,
}

impl Default for PollersCfg {
    fn default() -> Self {
        Self {
            premium_index_secs: default_premium_index_secs(),
            open_interest_secs: default_open_interest_secs(),
            funding_realized_secs: default_funding_realized_secs(),
        }
    }
}

fn default_premium_index_secs() -> u64 {
    30
}

fn default_open_interest_secs() -> u64 {
    300
}

fn default_funding_realized_secs() -> u64 {
    300
}

const POLLER_SECS_BOUNDS: std::ops::RangeInclusive<u64> = 5..=3600;

/// Universe manager (A11/R4): periodic full-symbol diff that records
/// instrument lifecycle as `Reference` events and feeds the OI poller.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UniverseCfg {
    /// Diff cadence; exchangeInfo is weight 1, listings are rare.
    #[serde(default = "default_universe_poll_secs")]
    pub poll_secs: u64,
    /// Data types to auto-subscribe for newly TRADING perps (empty = off).
    /// Only per-instrument WS types qualify: venue-wide pollers already
    /// follow the universe by themselves.
    #[serde(default)]
    pub auto_subscribe_data: Vec<DataTypeCfg>,
}

impl Default for UniverseCfg {
    fn default() -> Self {
        Self {
            poll_secs: default_universe_poll_secs(),
            auto_subscribe_data: Vec::new(),
        }
    }
}

fn default_universe_poll_secs() -> u64 {
    900
}

const UNIVERSE_POLL_BOUNDS: std::ops::RangeInclusive<u64> = 60..=86_400;

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
/// The fapi markPrice WS family is acked-but-silent (live-verified
/// 2026-06-10), so mark/index/funding come from the venue-wide
/// `/fapi/v1/premiumIndex` poller — they are only configurable as
/// `all = true`. open_interest is REST-only, captured by its poller in
/// either scope. Rejecting the rest here turns silent zero-data into a
/// startup error.
fn binance_unsupported_reason(dt: DataTypeCfg, venue_wide: bool) -> Option<&'static str> {
    match dt {
        DataTypeCfg::MarkPrice | DataTypeCfg::IndexPrice | DataTypeCfg::FundingRate
            if !venue_wide =>
        {
            Some(
                "mark/index/funding come from the venue-wide premiumIndex REST \
                 poller (the per-symbol WS stream is acked but silent) — use \
                 `all = true` for this data type",
            )
        }
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

        for (name, secs) in [
            ("premium_index_secs", self.pollers.premium_index_secs),
            ("open_interest_secs", self.pollers.open_interest_secs),
            ("funding_realized_secs", self.pollers.funding_realized_secs),
        ] {
            if !POLLER_SECS_BOUNDS.contains(&secs) {
                return invalid(format!(
                    "pollers.{name} = {secs} out of bounds ({}..={} s)",
                    POLLER_SECS_BOUNDS.start(),
                    POLLER_SECS_BOUNDS.end()
                ));
            }
        }
        if !UNIVERSE_POLL_BOUNDS.contains(&self.universe.poll_secs) {
            return invalid(format!(
                "universe.poll_secs = {} out of bounds ({}..={} s)",
                self.universe.poll_secs,
                UNIVERSE_POLL_BOUNDS.start(),
                UNIVERSE_POLL_BOUNDS.end()
            ));
        }
        for dt in &self.universe.auto_subscribe_data {
            if matches!(dt, DataTypeCfg::OpenInterest) {
                return invalid(
                    "universe.auto_subscribe_data: open_interest — the OI poller \
                     already follows the universe; remove it"
                        .into(),
                );
            }
            if let Some(reason) = binance_unsupported_reason(*dt, false) {
                return invalid(format!(
                    "universe.auto_subscribe_data: `{}`: {reason}",
                    dt.name()
                ));
            }
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
        assert_eq!(subs.len(), 2);
        match &subs[0].scope {
            Scope::Instruments(ids) => {
                assert_eq!(ids.len(), 2);
                assert_eq!(ids[0].value.as_ref(), "btcusdt");
            }
            other => panic!("expected explicit scope, got {other:?}"),
        }
        assert!(subs[0].data.contains(&DataType::BookDepth));

        // The poller-backed venue-wide tier (funding/mark/index/OI for all
        // perps per A14) ships enabled in the example config.
        assert!(matches!(subs[1].scope, Scope::All));
        for dt in [
            DataType::FundingRate,
            DataType::MarkPrice,
            DataType::IndexPrice,
            DataType::OpenInterest,
        ] {
            assert!(subs[1].data.contains(&dt));
        }
        assert_eq!(cfg.pollers.premium_index_secs, 30);
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
    fn poller_backed_types_accepted_venue_wide_only() {
        // mark/index/funding: produced by the venue-wide premiumIndex poller
        // (the WS family is acked-but-silent) — `all = true` only.
        for dt in ["mark_price", "index_price", "funding_rate"] {
            let ok = minimal(&format!(
                "[[subscriptions]]\nall = true\ndata = [\"{dt}\"]\n"
            ));
            assert!(parse_str(&ok).is_ok(), "{dt} venue-wide should be valid");

            let per_symbol = minimal(&format!(
                "[[subscriptions]]\ninstruments = [\"btcusdt\"]\ndata = [\"{dt}\"]\n"
            ));
            let err = parse_str(&per_symbol).unwrap_err();
            assert!(
                matches!(&err, ConfigError::Invalid(msg) if msg.contains("all = true")),
                "{dt}: {err}"
            );
        }
        // open_interest: REST poller serves both scopes (explicit list =
        // those symbols; all = the whole perp universe).
        for sub in [
            "[[subscriptions]]\ninstruments = [\"btcusdt\"]\ndata = [\"open_interest\"]\n"
                .to_string(),
            "[[subscriptions]]\nall = true\ndata = [\"open_interest\"]\n".to_string(),
        ] {
            assert!(parse_str(&minimal(&sub)).is_ok(), "open_interest: {sub}");
        }
    }

    #[test]
    fn poller_cadences_default_and_bounds_checked() {
        let cfg = parse_str(&minimal(
            "[[subscriptions]]\ninstruments = [\"btcusdt\"]\ndata = [\"trade\"]\n",
        ))
        .unwrap();
        assert_eq!(cfg.pollers.premium_index_secs, 30);
        assert_eq!(cfg.pollers.open_interest_secs, 300);
        assert_eq!(cfg.pollers.funding_realized_secs, 300);

        let ok = minimal(
            "[pollers]\npremium_index_secs = 60\n[[subscriptions]]\ninstruments = [\"btcusdt\"]\ndata = [\"trade\"]\n",
        );
        assert_eq!(parse_str(&ok).unwrap().pollers.premium_index_secs, 60);

        for bad in ["premium_index_secs = 4", "open_interest_secs = 3601"] {
            let raw = minimal(&format!(
                "[pollers]\n{bad}\n[[subscriptions]]\ninstruments = [\"btcusdt\"]\ndata = [\"trade\"]\n"
            ));
            let err = parse_str(&raw).unwrap_err();
            assert!(
                matches!(&err, ConfigError::Invalid(msg) if msg.contains("out of bounds")),
                "{bad}: {err}"
            );
        }

        // Strict parsing applies to [pollers] too.
        let unknown = minimal(
            "[pollers]\ncadence = 10\n[[subscriptions]]\ninstruments = [\"btcusdt\"]\ndata = [\"trade\"]\n",
        );
        assert!(matches!(parse_str(&unknown), Err(ConfigError::Parse(_))));
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
