# Data Products — the contract for external consumers

This repo captures, stores, and monitors market data. Research (notebooks),
strategies, and execution live in **other repositories** and consume the
datasets documented here. This file is the contract: paths, schemas, join
keys, provenance semantics. Schema changes are additive-only; breaking
changes get a new file/column name, never a silent re-cut.

All parquet is zstd-compressed with `Timestamp(Nanosecond, "UTC")` time
columns. Decimals are stored as nullable `Float64` in analytics tables
(exact decimals live in the WAL, which remains the source of truth).

## Roots

```
data/
  wal/<venue>/<date>.wal               # source of truth (framed MessagePack, CRC)
  raw/<venue>/<date>.rawwal            # raw venue frames (R2 tee; WS + REST bodies)
  parquet/<venue>/<date>/<type>.parquet  # LIVE capture, converted hourly per closed day
  backfill/<venue>/<dataset>/…         # REST HISTORY, refetchable, bypasses the WAL
  meta/                                # reference data + reports (details below)
```

A `parquet/<venue>/<date>/` directory is **published** iff it contains
`qa_report.json`; consume only published days (the file doubles as the QA
verdict: `status: pass|fail`).

## Live capture tables (`data/parquet/<venue>/<date>/`)

Every market table shares the envelope columns
`instrument (utf8, lowercase venue-raw), venue_ts (ts?, venue transaction
time), local_ts (ts, capture-host receive time), source (u16: 0 = REST,
1+ = WS connection)`.

| File | Extra columns | Notes |
|---|---|---|
| `book_ticker.parquet` | update_id, bid/ask price+qty | top of book |
| `book_snapshot.parquet` | last_update_id, side, level_idx, price, qty | row per level |
| `book_update.parquet` | first/final/prev_final_update_id, event_time, side, price, qty | row per changed level; splice rule `U <= lastUpdateId+1 <= u` |
| `trades.parquet` | trade_id (utf8), price, qty, side, kind | per-fill |
| `mark_price.parquet` / `index_price.parquet` | price | 30 s premiumIndex poller, all symbols |
| `funding_rate.parquet` | rate, next_funding_time, interval_ns, premium_index, clamp_min/max | funding **prediction** |
| `funding_rate_realized.parquet` | rate, funding_time, interval_ns | settlements; **key = (instrument, funding_time)** |
| `open_interest.parquet` | open_interest, open_interest_value | live poller rows have `open_interest_value = null` (endpoint lacks it); backfill rows carry it |
| `liquidation.parquet` | side, price, qty, filled_qty, avg_price, order_status | WS `@forceOrder`; **no historical backfill exists** (auth-only endpoint) — live capture is the only source |
| `reference.parquet` | kind, detail (JSON) | instrument lifecycle events from the universe manager; joins use the SCD below, this is forensics |
| `control.parquet` | kind, detail (JSON) | ConnUp/Down, SubAck, gaps — capture honesty timeline |

## Backfill datasets (`data/backfill/<venue>/<dataset>/`)

Derived, refetchable REST history. Month files `YYYY-MM.parquet` are
complete and immutable; `YYYY-MM.partial.parquet` is the open month,
refreshed per run (exclude or dedup when unioning). OI history is
day-partitioned (`YYYY-MM-DD.parquet`).

| Dataset | Venues | Schema | Caveats |
|---|---|---|---|
| `funding/` | binance, bybit | identical to `funding_rate_realized` | `interval_ns` is **null** in backfill (stamping today's interval onto old rows would lie for symbols whose cadence changed — derive realized interval as the delta between consecutive settlements per symbol); `local_ts` = fetch time |
| `oi_hist/` | binance | identical to `open_interest`, value column populated | venue retains only ~30 days at 5 m grain; a daily timer preserves it until live coverage suffices |
| `klines_1h/` | binance | envelope + close_time, OHLC, volume, quote_volume, trades, taker_buy_* | `venue_ts` = bar open time; explicit `--from`, run on demand |

**Unioning live + backfill funding:** concatenate
`parquet/*/*/funding_rate_realized.parquet` with
`backfill/<venue>/funding/*.parquet` and dedup on
`(venue, instrument, funding_time)` — overlap is expected and harmless.
The `source` column is 0 in both (REST-origin); provenance is the root the
file came from.

## Reference data (`data/meta/`)

| Path | Content |
|---|---|
| `symbology/mapping.parquet` | `(venue, instrument) → (base, quote, class, settle)` with `valid_from`/`valid_to` (null = open) and `origin (derived\|override)`. The cross-venue join key is `base-quote-class-settle`. Multiplier bases (`1000PEPE`) are verbatim and match across venues. Built from the latest instrument dumps + `configs/symbology-overrides.toml`. |
| `symbology/mapping.build.json` | build provenance + cross-venue match coverage |
| `instruments/binance.parquet` | instruments SCD: one row per (symbol, change interval), day resolution — status, lifecycle, tick/lot/min_notional, `funding_interval_ns`, onboard_ts, `valid_from`/`valid_to`. Point-in-time join: `valid_from <= t AND (valid_to IS NULL OR t < valid_to)` |
| `fees/<venue>.parquet` | venue, tier, market, maker, taker (fractions), valid_from. Curated from `configs/fees/` (VIP0 only in v1) |
| `reconciliation/<venue>/<date>.json` | daily funding coverage verdict (see below) |
| `<venue>/<date>-exchangeInfo.json` etc. | raw daily dumps — the inputs everything above is rebuilt from |
| `<venue>/universe.json` | universe-manager state (internal; not a consumer surface) |

## Quality gates

- **Daily QA** (`qa_report.json` per published day): frame integrity, depth
  chain validation, dup/gap counts, latency distributions.
- **Daily funding reconciliation**
  (`meta/reconciliation/<venue>/<date>.json`): captured settlements vs an
  independent REST refetch; `coverage_pct`, `missing[]`, `extra[]`,
  `rate_mismatches[]`, and `consecutive_green_days` (the capture plan's
  exit criterion keys on this field — `docs/implementation-plan.md`).
  Caveat recorded once
  here: live realized funding is itself REST-polled (the venue's WS family
  is dead), so this verifies pipeline completeness end-to-end, not
  dual-channel agreement.

## DuckDB snippets (contract illustration)

Cross-venue funding union with canonical join:

```sql
WITH funding AS (
  SELECT 'binance' AS venue, instrument, funding_time, rate
  FROM read_parquet('data/backfill/binance/funding/*.parquet')
  UNION ALL
  SELECT 'bybit', instrument, funding_time, rate
  FROM read_parquet('data/backfill/bybit/funding/*.parquet')
  UNION ALL
  SELECT regexp_extract(filename, 'parquet/(\w+)/', 1), instrument, funding_time, rate
  FROM read_parquet('data/parquet/*/*/funding_rate_realized.parquet', filename=true)
),
deduped AS (
  SELECT DISTINCT venue, instrument, funding_time, rate FROM funding
),
mapped AS (
  SELECT m.base || '-' || m.quote || '-' || m.class || '-' || m.settle AS canonical,
         f.*
  FROM deduped f
  JOIN read_parquet('data/meta/symbology/mapping.parquet') m
    ON m.venue = f.venue AND m.instrument = f.instrument
   AND f.funding_time >= m.valid_from
   AND (m.valid_to IS NULL OR f.funding_time < m.valid_to)
)
SELECT * FROM mapped;
```

Point-in-time instrument lookup:

```sql
SELECT funding_interval_ns
FROM read_parquet('data/meta/instruments/binance.parquet')
WHERE symbol = 'btcusdt'
  AND valid_from <= TIMESTAMP '2026-06-10 12:00:00+00'
  AND (valid_to IS NULL OR TIMESTAMP '2026-06-10 12:00:00+00' < valid_to);
```

Realized funding interval (per-symbol, honest for historical rows):

```sql
SELECT instrument, funding_time,
       funding_time - lag(funding_time) OVER (PARTITION BY instrument ORDER BY funding_time)
         AS realized_interval
FROM read_parquet('data/backfill/binance/funding/*.parquet');
```
