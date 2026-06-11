# Architecture: Trading Data Framework

*Updated 2026-06-11 after the Phase-2 build (completeness and reference:
REST pollers for funding/mark/index/OI, `backfill` + daily reconciliation,
universe manager, `symbology` — canonical mapping, instruments SCD, fees).
The Phase-2 exit SLO is accumulating; `docs/implementation-plan.md` is the
living plan and `docs/data-products.md` the consumer contract. Sections
marked **[planned]** describe components that do not exist yet; everything
else is implemented and tested.*

## 1. System Overview

**Durability lives at the edge** (A2/DOC3): each venue process writes its own
WAL in-process through `WalSink` *before* anything else sees the event. The
future event bus serves live consumers only and is allowed to be lossy — it
injects `Control::Gap` events when it drops, and it is never in the durability
path. This dissolves the old "lossless to recorder vs consumers never block"
contradiction: there is no recorder process behind the bus at all.

```
  CAPTURE EDGE (venue-process: one supervised process per venue)   [planned] LIVE PATH
 ┌──────────────────────────────────────────────┐
 │ config.toml → venue-binance                  │            ┌───────────────┐
 │  WsPool (N conns, acks, stale watchdog) ──┐  │            │ event bus     │
 │  REST snapshot fetcher (paced) ───────────┤  │   lossy    │ (UDS, lossy,  │
 │  REST pollers: premiumIndex/OI/funding ───┤  │ ──────────>│  gap-counted) │
 │  universe manager (15 min diff → Ref ev) ─┤  │            └──────┬────────┘
 │  fundingInfo / exchangeInfo fetch ────────┤  │                   │
 │                                           ▼  │                   │
 │            normalize → Event ─ StatsSink ─┬──│── data/wal/  (lossless)
 │            raw frames ──── RawWalSink ────┴──│── data/raw/  (best-effort tee)
 │  heartbeat (1/min) ── journald               │                   │
 └──────────────────────────────────────────────┘          [planned] strategy
 data/wal ──> wal-sweep (hourly systemd timer) ──> data/parquet/<venue>/<date>/
                                                     + qa_report.json (daily QA)
 data/parquet ──> [planned] replay (virtual clock) ──> same EventSink consumers
```

Single-host by design (A18): UDS-class transports only. Multi-host growth means
shipping WAL/Parquet files to other machines, never stretching the bus.

## 2. Crate Structure

### Existing

| Crate            | Purpose                                                                      |
|------------------|------------------------------------------------------------------------------|
| `venue-core`     | Envelope v2 (`Event`), domain payloads (`Market/Reference/Chain/Account/Control`), symbology types (`Asset`, `InstrumentClass`, `CanonicalInstrumentId`, `LifecycleState`), `SourceId`, `Provenance`, `RawFrame` |
| `venue-adapter`  | Traits (RPITIT, no async_trait): `EventSink` (+`send_batch`), `RawFrameSink`, `VenueAdapter<S>`, `Subscription{scope, data}`, `Scope`, `DataType`, `VenueError`; `IngestSource` + `SourceSet` (R11 — the unit of ingestion composition, dyn-compatible) |
| `venue-binance`  | USD-M futures adapter: WsPool (sharding, ack watcher, stale watchdog, jittered backoff, control events, raw tee), REST depth-snapshot fetcher, **Phase-2 REST pollers** (premiumIndex / openInterest / fundingRate — the producers for mark/index/funding/OI since the markPrice WS family is acked-but-dead), fundingInfo/exchangeInfo fetch, parser fixture tests |
| `wire`           | Framed MessagePack: `[magic "WAL1"][version u8][len u32][crc32][payload]`; self-healing `FrameReader`; golden-bytes + encoding-probe tests pin the layout |
| `recorder`       | `WalWriter`/`WalSink` (lossless, dedicated thread, 1 s fsync, midnight rotation, fatal-exit on I/O error), `RawWalWriter` (R2 tee), `stats` (P5d counters: `StatsSink`, `WriterStats`), `tables` (Parquet table writers shared with `backfill` — schema-identical live and backfilled data), Parquet converter (zstd, nullable columns, UTC-ns timestamps, 500K-row batches, incl. `reference.parquet`), `qa` (daily QA report), `sweep` + `wal-sweep` bin (P6 conversion automation) |
| `config`         | TOML capture config: strict parsing (`deny_unknown_fields`), validation that rejects data types the venue cannot deliver (silent zero-data → startup error), `[pollers]` cadences, `[universe]` policy, config→`Subscription` mapping |
| `venue-process`  | Supervised capture binary: config → writers → adapter; startup retry with rollback (N8), once-a-minute heartbeat (P5d), daily exchangeInfo + fundingInfo dumps (P5a), universe manager (A11/R4: 15-min full-symbol diff → `Reference` events, OI-universe watch feed, optional auto-subscribe), SIGTERM/SIGINT graceful shutdown; exit codes 0/1/2 |
| `backfill`       | REST history (A5): `funding` (Binance venue-wide + Bybit per-symbol), `oi-hist` (perishable ~30-day window), `klines`, and `reconcile` (daily captured-vs-REST funding coverage with `consecutive_green_days`); month/day-partitioned Parquet under `data/backfill/`, atomic publish, idempotent |
| `symbology`      | Reference-data builds (A3/A11): canonical mapping + point-in-time `Registry`, instruments SCD from accumulated dumps, fee schedules from curated TOML; one `symbology build` bin for the daily timer |

### Planned (see `docs/implementation-plan.md`)

Manifest + metrics/alerting (Phase 3), `replay` (Phase 4), `bus`/`transport`
(demand-gated: built — or `iceoryx2` adopted — when the first live consumer
exists). The strategy runtime and execution engines live in **separate
repositories** by design; they consume this repo's datasets
(`docs/data-products.md`) and, later, its replay crate. The capture-side
seam for private account data (`data/private/`, Phase 6) stays here.

### Dependency graph (actual)

```
venue-adapter  ─►  venue-core
wire           ─►  venue-core
venue-binance  ─►  venue-adapter, venue-core    (dev-dep: recorder, for examples)
recorder       ─►  venue-adapter, venue-core, wire
config         ─►  venue-adapter, venue-core
venue-process  ─►  config, recorder, venue-binance, venue-adapter, venue-core
backfill       ─►  recorder (tables), venue-core, wire
symbology      ─►  recorder (tables), venue-core
```

`venue-adapter` deliberately does **not** depend on `wire`: adapters are
transport-agnostic and only see the `EventSink`/`RawFrameSink` traits.

## 3. Schema and Wire Contracts (wire v1, frozen 2026-06-10)

### Envelope v2

```rust
pub struct Event {
    pub venue: VenueId,
    pub instrument: Option<InstrumentId>,   // None only for venue-scoped events
    pub venue_ts: Option<Nanos>,            // transaction time (see below)
    pub local_ts: Nanos,                    // mandatory capture-host time
    pub source: SourceId,                   // which conn/poller produced it (R9)
    pub provenance: Option<Provenance>,     // chains only; None for CEX (R3)
    pub payload: Payload,
}

pub enum Payload {
    Market(MarketPayload),       // ticker/books/trades/mark/index/funding/OI/liquidation
    Reference(ReferencePayload), // instrument lifecycle, market resolution (producers: Phase 2)
    Chain(ChainPayload),         // reserved, empty (Phase 5)
    Account(AccountPayload),     // reserved, empty; never on the shared bus (Phase 6)
    Control(ControlPayload),     // ConnUp/Down, Gap, Snapshot brackets, SubAck, Reorg
}
```

`SourceId` registry convention per venue process: `0` = REST, `1..` = WS
connections in spawn order. Trade ids are venue-raw strings (`Arc<str>`, R6);
book-sequence ids (`update_id`, `U`/`u`/`pu`) are `u64` — a documented deviation
from report decision 8, because pu-chain and splice arithmetic is load-bearing
and every current CEX target uses numeric book sequences. Venues with
non-numeric book versions get their own payload variants (additive-safe).

### Timestamp contract (D7)

- `venue_ts` = the venue **transaction time** wherever the venue provides one
  (bookTicker `T`, trade `T`, depth `T`, forceOrder `o.T`, REST depth `T`).
  Note: `DataType::Trade` maps to `<symbol>@trade` (per-fill, carries the fill
  type `X`) — live verification on 2026-06-10 showed fapi `@aggTrade` no longer
  emits; the aggTrade parser arm is kept as a fallback.
- Exception: `markPriceUpdate` has no transaction time (its `T` is
  next-funding-time), so mark/index/funding-prediction events carry event time
  `E` in `venue_ts`.
- Depth updates additionally keep event time `E` in `BookUpdate.event_time` so
  QA can monitor E−T distributions.
- `local_ts` is always present and is the cross-venue replay clock; run chrony
  on capture hosts and monitor `local_ts − venue_ts` per venue.

### Wire format and evolution policy (A15)

Frame: `[magic u32 LE = "WAL1"][version u8 = 1][len u32 LE][crc32 u32 LE][payload]`,
CRC over `version‖len‖payload`, `MAX_FRAME_LEN` 16 MiB. The same framing carries
`Event` (`.wal`) and `RawFrame` (`.rawwal`); the file extension distinguishes
them. MAGIC is frozen forever; the version byte is the only evolution mechanism.

The payload is rmp-serde MessagePack, and its layout rules are the schema
contract:

- **Structs encode positionally.** Field order and arity are load-bearing;
  field *names* are not. Never reorder, insert, or remove a field of any
  serialized type without bumping `WIRE_VERSION`. A reorder of same-typed
  fields decodes *silently wrong* — this is the worst failure class.
- **Enum variants encode by name.** Variant names are load-bearing; renaming is
  a wire break. **Adding variants is the only additive change**: old readers
  fail to decode just those frames, and the `FrameReader` skips exactly those
  CRC-valid frames while counting them.
- Readers support version N and N−1 once N exists; older files need an
  explicit migration.
- Guard rails in `wire`: the `encoding_probe` test pins the rmp-serde layout
  behavior; the golden-bytes test pins the exact frame bytes of a fixed event.
  If either fails after a change, the wire format moved — revert or bump.

### Reader recovery policy (D6 + P1)

`FrameReader`: bad magic / bad CRC / absurd length → resync scan for the next
MAGIC starting at `bad_frame_offset + 1` (a corrupted `len` never decides the
skip), with candidate headers fully validated. CRC-valid frames that fail rmp
decode are skipped exactly (additive-variant tolerance). **`BadVersion` aborts
and never resyncs** — a version mismatch means the file needs a different
decoder, not corruption recovery. Truncated tails are distinguished from clean
EOF. The converter fails the file outright if more than 1% of bytes had to be
skipped.

## 4. Venue Adapter Layer

### Core traits (RPITIT; not dyn-compatible — a future bus needs an eraser)

```rust
pub trait EventSink: Send + Sync + Clone + 'static {
    fn send(&self, event: Event) -> impl Future<Output = Result<(), EventSinkError>> + Send;
    fn send_batch(&self, events: Vec<Event>) -> impl Future<Output = ...> + Send; // default loops send
}

pub trait RawFrameSink: Send + Sync + Clone + 'static {
    fn send_raw(&self, frame: RawFrame);  // sync fire-and-forget; impl for () = no tee
}

pub struct Subscription { pub scope: Scope, pub data: Vec<DataType> }
pub enum Scope { Instruments(Vec<InstrumentId>), Class(InstrumentClass), All }
```

`Scope::All` maps to venue-wide streams where they exist — one stream instead
of hundreds, immune to listing lag. `Scope::Class` expansion waits for the
Phase-2 universe manager.

**Live stream-availability findings (the acked-but-dead class).** Binance
ACKs SUBSCRIBE for stream names that no longer emit, so silence — not an
error — is the failure mode; the raw tee (R2) plus `ws_probe` is how these
are diagnosed. Verified 2026-06-10:

- fapi `@aggTrade` does not emit → `DataType::Trade` maps to `@trade`
  (per-fill, carries fill type `X`); the aggTrade parser arm is a fallback.
- **The whole `markPrice` stream family does not emit** (per-symbol and
  `!markPrice@arr`, both cadences, raw and combined endpoints) →
  mark/index/funding-prediction capture moves to the Phase-2 REST poller
  (`/fapi/v1/premiumIndex`, verified live). The parser arms remain for a
  venue-side revival.
- `!bookTicker` was removed from fapi years ago.
- `config` validation rejects all of these (plus REST-only
  `DataType::OpenInterest`) at startup — a rejected SUBSCRIBE would
  reconnect-loop forever, and an acked-dead one would capture nothing
  silently. The only venue-wide stream currently allowed is
  `!forceOrder@arr` (liquidations).

### Binance connection lifecycle (ws_pool)

One `read_loop` serves initial and reconnected sessions:

- raw tee first: every text frame goes to `RawFrameSink` before parsing (R2);
- data parse first, reply watcher second: frames the parser rejects are
  classified as SUBSCRIBE acks (`SubAck` control events; rejection →
  reconnect) or venue error frames (surfaced at warn — N12);
- stale watchdog: no traffic for 5 min (server pings every ~3) → reconnect;
- ack deadline: SUBSCRIBE unacknowledged for 10 s → reconnect;
- backoff: exponential 1 s→30 s with +25% jitter; reconnect is immediate after
  a stable (≥60 s) session, backed-off when flapping;
- `ConnUp` (after ack) / `ConnDown {reason}` control events are emitted through
  the same sink and recorded in the WAL (A7) — replay sees the same
  discontinuities live consumers saw.

### REST lifecycle (Bug 1 + A4 + P5a)

- **Depth snapshots**: a per-adapter fetcher task receives triggers — first
  `depthUpdate` per symbol per connection session (so every reconnect
  re-snapshots; the session-scoped `depth_seen` set makes this emergent) and a
  30-min periodic sweep. Fetches are sequential, paced ≥0.5 s
  (`depth?limit=1000` = weight 20 vs the 2,400/min budget), deduplicated, with
  one retry. Snapshot `venue_ts` = response `T`, `source` = `SourceId::REST`.
  Splice rule: the update continuing a snapshot satisfies
  `U <= lastUpdateId + 1 <= u` (including the perfectly-contiguous
  `U == lastUpdateId + 1`); checked daily by `recorder::qa` and on demand by
  `recorder/examples/verify_depth.rs` (a thin CLI over the same code).
- **Funding metadata** (A4): `/fapi/v1/fundingInfo` is fetched at subscribe
  time; `FundingRatePrediction` events carry `interval` (8h default for
  unlisted symbols — the endpoint lists only deviants) and venue clamps.
  `premium_index` stays `None` until its stream is captured.
- **exchangeInfo dump** (P5a/N1): `fetch_instruments_raw()` returns the raw
  JSON body; `venue-process` writes it to
  `data/meta/binance/<date>-exchangeInfo.json` at startup and on every UTC
  date change (with a 30 s timeout and retry-next-minute on failure), so
  reference fields the parser drops remain recoverable. A sibling daily
  `<date>-fundingInfo.json` dump preserves interval/clamp *history* for the
  SCD. `fetch_instruments` parses tick/lot/notional filters, margin asset,
  delivery dates, lifecycle status and funding intervals into the extended
  `Instrument`; `fetch_instruments_all` skips the TRADING filter (the
  universe manager's input).

### IngestSource and the Phase-2 REST pollers (R11 + A6)

A venue process hosts N `IngestSource`s sharing one sink and one WAL. The
trait is dyn-compatible (`label()`, `source_id()`,
`run(Box<Self>, CancellationToken)`); `SourceSet` supervises spawned sources
(cancel → 3 s grace → abort, reusable across the N8 retry rollback). The
WsPool and snapshot fetcher predate the trait and match its contract
internally; the pollers implement it natively.

Three pollers, all `SourceId::REST`, told apart in the control timeline by
label and in heartbeats by event kind (REST sources share id 0 by the frozen
wire-v1 convention, so per-kind staleness *is* the per-poller detector):

- **premium-index** (`/fapi/v1/premiumIndex`, no symbol → all ~800 symbols,
  weight 10, default 30 s): emits `MarkPrice` + `IndexPrice` +
  `FundingRatePrediction` per row — the replacement for the acked-but-dead
  markPrice WS family, whose streams stay subscribed only as a free fallback
  (a revived WS producer would be distinguishable by `source >= 1`). Rows
  with `nextFundingTime = 0` (settling symbols, delivery futures) emit no
  prediction. Intervals/clamps stamped from fundingInfo, refreshed daily.
- **open-interest** (`/fapi/v1/openInterest`, weight 1, per-symbol): paced
  round-robin over the TRADING-perp universe, one sweep per interval
  (default 300 s, matching the venue's own 5 m OI-history grain). The
  universe arrives over a `watch` channel fed by the universe manager;
  HTTP 400 on settling symbols is routine and skipped.
- **realized-funding** (`/fapi/v1/fundingRate` venue-wide tail poll,
  default 300 s): 1 h lookback per cycle, 2 h catch-up after restart, dedup
  by `(symbol, fundingTime)`, saturation paging that advances to the last
  settlement instant *inclusive* (a `last + 1` advance can skip rows when an
  instant straddles a page boundary). This poller is the funding-coverage
  SLO source.

Poller health follows A7: first success / recovery emits `ConnUp{label}`,
the third consecutive failed cycle emits `ConnDown{label, reason}`. REST
response bodies are teed raw before parsing — R2 applies to REST exactly as
to WS (`interestRate`, `estimatedSettlePrice` are not parsed but stay
recoverable from `data/raw/`).

### Universe manager (A11/R4)

Every `universe.poll_secs` (default 15 min) the venue process fetches *all*
symbols, diffs the normalized `Instrument`s against persisted state
(`data/meta/<venue>/universe.json` — restart emits no duplicate burst; a
missing file produces the one-time baseline burst by design), records
transitions as `ReferencePayload` events through the normal sink (→
`reference.parquet`), updates the OI poller's watch channel, and optionally
auto-subscribes newly TRADING perps per `[universe] auto_subscribe_data`
(default off; baseline bursts never auto-subscribe).

## 5. Recording Layer

```
Events ─ StatsSink ─ WalSink ─> {data/wal}/{venue}/{date}.wal   (lossless, source of truth)
Raw WS ─ RawWalSink ─> {data/raw}/{venue}/{date}.rawwal         (best-effort tee, R2)
WAL ──(wal-sweep: hourly systemd timer, idempotent — P6)──> Parquet + qa_report.json
```

- One dedicated OS thread per writer owns all file I/O; 100K-event channel
  (~2 s at peak burst — N6); 1 s fsync cadence; **file date from `local_ts`**
  (capture truth — venue clocks never pick the file); midnight rotation drops
  finished days' writers.
- **Failure policy (N2)**: any WAL I/O error (open/write/fsync) exits the
  process — a capture process that cannot persist must die visibly and be
  restarted by the supervisor, not look healthy while recording nothing.
  Encode errors are data bugs: logged, dropped, non-fatal.
- `WalSink::send` is lossless: `try_send` fast path, blocking send inside
  `block_in_place` when full (requires the multi-thread tokio runtime).
  `send_batch` enqueues a venue message's events without interleaving awaits.
  The raw tee is deliberately best-effort: it drops (with a warning) rather
  than stall the read loop; the normalized WAL remains the source of truth.
- **Shutdown contract**: every `WalSink`/`RawWalSink` clone must be dropped
  before its writer, or the writer's `Drop` join blocks forever. Order:
  `disconnect → drop(adapter) → drop(writers)`.
- WAL files are **arrival-ordered, not timestamp-sorted** (D3): replay sorts.

### Parquet converter

Per data type, one zstd-compressed file per `(venue, date)`; timestamps are
Arrow `Timestamp(Nanosecond, "UTC")`; all price/qty/rate columns are
**nullable Float64** — a failed Decimal→f64 conversion becomes null + warning,
never a fabricated value (D5/N3); batches flush every 500K rows into separate
row groups (Bug 2). Every file carries the envelope columns
`instrument, venue_ts, local_ts, source`.

```
book_ticker:  + update_id, bid_price, bid_qty, ask_price, ask_qty
trades:       + trade_id (Utf8), price, qty, side, kind (fill type, nullable)
book_snapshot:+ last_update_id, side, level_idx, price, qty       (level_idx = rank, snapshots only)
book_update:  + first/final/prev_final_update_id, event_time, side, price, qty   (NO level_idx — D4)
funding_rate: + rate, next_funding_time, interval_ns, premium_index, clamp_min, clamp_max
funding_rate_realized: + rate, funding_time, interval_ns          (producer: Phase-2 REST backfill — N11)
mark_price / index_price: + price
liquidation:  + side, price, qty, filled_qty, avg_price, order_status
open_interest:+ open_interest, open_interest_value                (producer: Phase-2 poller)
control:      instrument?, venue_ts?, local_ts, source, kind, detail(JSON)
```

`Reference`/`Chain`/`Account` payloads are counted and logged, not yet
converted. Conversion refuses files where >1% of bytes were corrupt (P1).

## 6. Operations (Phase 1)

### Running capture

```
cargo run -p venue-process -- configs/binance.toml      # dev
systemctl enable --now trading-capture@binance          # deploy (see deploy/README.md)
```

One TOML per venue process (committed example: `configs/binance.toml`).
Parsing is strict (`deny_unknown_fields`); validation rejects subscriptions
the venue cannot deliver, and startup cross-checks configured instruments
against live exchangeInfo — a typo'd symbol is exit 2, not silent zero-data.
`RUST_LOG` overrides the config log filter.

**Exit codes**: `0` clean signal shutdown · `1` fatal runtime (N2 WAL
fatality; supervisor restarts) · `2` config/usage error (systemd
`RestartPreventExitStatus=2` keeps it down — restarting cannot fix a config).

**Startup retry (N8)**: a failed `subscribe()` can leave earlier stream
chunks live, so venue-process rolls back with `disconnect()` and retries the
whole subscription forever with capped jittered backoff (1 s→30 s) — an
unattended box rides out long outages in-process. After subscribe succeeds,
per-connection recovery is the WsPool's job. Config errors never retry.

### Heartbeat (P5d)

Once a minute (`capture.heartbeat_secs`), one log line from lock-free
counters:

```
heartbeat venue=binance up_s=3600 total=39065
  eps="book_ticker=537 book_update=19 trades=95"     events/s by kind since last beat ("idle" if none)
  wal_depth=0 wal_written=39065 fsync_age_ms=396     WAL queue depth, frames written, age of last fsync
  raw_depth=1 raw_dropped=0                          raw tee queue + drops (tee is best-effort)
  reconnects=2                                       ConnDown count since start
  staleness_s="book_ticker=0s liquidation=842s"      age of last event per kind ever seen
```

The heartbeat is the cheap detector for every silent-death mode (dead WAL
thread → `fsync_age_ms` grows; zombie connection → `eps` idle + staleness
grows; backpressure → `wal_depth` grows). Logs-only in Phase 1; rare kinds
(liquidation) are legitimately stale for hours — thresholds and alerting are
Phase-3 metrics work.

### Conversion + daily QA (P6)

`wal-sweep <wal_root> <out_root> [--as-of YYYY-MM-DD]` — hourly via systemd
timer (`Persistent=true` catches downtime), manual-safe, idempotent:

- Converts every `wal_root/<venue>/<date>.wal` with `date < as_of` (UTC
  today by default). Open days and already-published days are skipped; a WAL
  written <10 min ago is skipped (midnight-backlog guard).
- Writes into `out/<venue>/.tmp-<date>/`, runs QA, writes `qa_report.json`
  last, then publishes with one atomic rename to `out/<venue>/<date>/`.
  **Completion marker = `qa_report.json` in the final dir**; marker-less
  dirs are treated as partial and re-converted (Parquet is derived data).
- A conversion failure still publishes the QA report
  (`conversion.ok=false`, `status=fail`) so the day fails loudly instead of
  retrying forever. Exit 1 if anything failed → the timer unit shows red.

### QA report (schema_version 1, additive evolution)

Per venue-day JSON, consumed by humans now and the Phase-3 manifest later:
frame stats (corruption, resyncs, truncated tail), event totals by kind,
per-instrument coverage (counts, first/last `local_ts`), depth QA, sequence
regressions, control counts + timeline, latency histograms (`depth E−T` and
`local_ts − venue_ts` per kind: p50/p95/p99/min/max, 1 ms-bucket histogram —
conservative upper-edge percentiles).

**Fail criteria (v1)**: corrupt bytes >1%; zero events; unexplained chain
breaks; never-spliceable snapshots; depth updates with no snapshot all file;
conversion error. **Explained-vs-unexplained**: a `ConnDown` recorded for
the source feeding an instrument excuses the next chain break / trade-id
regression there, and marks still-pending snapshots "abandoned by
reconnect" — reconnect losses are visible but expected (A7); the same break
on a healthy connection fails the day. Report-only in v1: trade/ticker id
regressions, gap counts, latencies, snapshots pending at EOF (their splice
lands in the next file).

### Backfill and reconciliation (Phase 2, A5)

REST history is *derived, refetchable* data and bypasses the WAL: the
`backfill` bin writes Parquet directly to `data/backfill/<venue>/<dataset>/`,
month-partitioned (`oi_hist` day-partitioned), atomically published — the
final file is the completion marker, the wal-sweep idiom; the open period is
a `.partial` refreshed per run. Schemas are identical to the live tables via
the shared `recorder::tables` writers, so consumers union live + history and
dedup on `(instrument, funding_time)`. Datasets, venues, caveats:
`docs/data-products.md`.

The daily **reconciler** (`backfill reconcile`, 02:30 UTC timer) compares
yesterday's captured `FundingRateRealized` events (published parquet of D,
D+1's spillover parquet/WAL — a 23:59 settlement is discovered after
midnight) against an independent REST refetch, writing
`data/meta/reconciliation/<venue>/<date>.json` with `coverage_pct`,
`missing/extra/rate_mismatches`, and `consecutive_green_days`. **The Phase-2
exit criterion is the latest report reaching `consecutive_green_days >= 14`.**
Honest caveat: live realized funding is itself REST-polled (dead WS family),
so this verifies pipeline completeness end-to-end — poller uptime → WAL →
conversion → publish — not dual-channel agreement; `rate_mismatches` becomes
load-bearing if a WS source revives.

### Reference data builds (Phase 2, A3/A11)

`symbology build` (daily 04:00 UTC timer) deterministically rebuilds, from
the day's raw dumps and curated configs, three queryable datasets under
`data/meta/`: the canonical mapping (+ point-in-time `Registry` for later
phases), the instruments SCD, and fee schedules. Curated inputs live in
`configs/symbology-overrides.toml` and `configs/fees/`. Dumps stay truth;
every product is a full rebuild, atomically published.

### Runbook

See `deploy/README.md` for install, chrony prerequisite (`local_ts` is the
replay merge clock), journald commands, and what to do on a failing QA
report (read `failures[]`, match against `control.timeline`, delete the
day's output dir to re-convert after investigation).

## 7. Replay Layer **[planned — Phase 4]**

Contracts already settled, recorded here so the implementation cannot drift:

- WAL/Parquet files are arrival-ordered; **replay does the sorting** (D3):
  k-way merge across files with in-file tie-break
  `(venue_ts, local_ts, file position)`.
- The merge clock is **selectable per run** (N5/A9): `local_ts` for cross-venue
  realism (what a live strategy actually saw — venue clocks are not each
  other's), `venue_ts` for single-venue book reconstruction.
- Control events replay like market data: gaps, reconnects and snapshot
  boundaries are part of the record (A7). The pu-chain crosses midnight file
  boundaries; replay must stitch consecutive days.
- Replay reads wire version N and N−1.
- Inputs come from the manifest with QA status (R10, Phase 3); replaying
  unaudited data at minimum warns loudly.

## 8. Event Bus **[planned — demand-gated]**

Built (or `iceoryx2` adopted) only when the first live consumer exists —
expected to be a paper-trading strategy process in its own repo
(`implementation-plan.md`, DEC-3). Lossy-only, gap-counted, restart-tolerant;
spec'd in one page before building (A10). With the WAL at the edge the bus
has no durability requirement — never build lossless consumer backpressure.
Slow consumers get drops plus `Control::Gap{reason, dropped}` injections.
Private `Account` events never transit the shared bus (A13).

## 9. Scalability and Performance

The numbers below are **design targets, not measurements** (DOC4); the latency
roadmap is deliberately frozen at UDS-class transport (A1) — at a seconds-scale
strategy cadence, completeness SLOs (funding coverage, staleness, gap rate)
are the KPIs, not microseconds.

| Metric                          | Target / sizing basis |
|---------------------------------|-----------------------|
| Peak rate (1 venue, all types)  | ~50k events/sec       |
| Capture channel headroom        | ~2 s at peak (100K)   |
| Daily WAL (1 venue, all types)  | tens of GB            |
| Parquet vs WAL size             | zstd, typically 5–10× smaller |

Venue scaling: new crate + new process; nothing else changes. Instrument
scaling: WsPool shards at 200 streams/conn. Storage scaling: tiering is
config reality today (full depth for the traded subset only; venue-wide
funding/mark/index/OI — A14); the queryable manifest lands in Phase 3 (R10).
The report's Hive lake re-layout was **dropped**: `data/parquet/<venue>/<date>/`
is the published consumer contract and DuckDB reads it directly — the
manifest, not a path convention, becomes the catalog
(`docs/implementation-plan.md`, decision DEC-2).
