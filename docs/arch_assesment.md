# Architecture Assessment

*Assessed against code as of 2026-06-08. ~1,771 lines of Rust across 5 crates,
~133 lines of examples, 4.3 MB of recorded Binance data.*

*Revised 2026-06-09: added a data-model audit pass — the parsed Binance wire
fields were checked against what actually reaches the WAL and Parquet — and this
document's own claims were reconciled against `cargo clippy` / `cargo test`. New
findings are tagged "new".*

## Current State

Five crates form a working single-venue data collection pipeline: Binance
Futures WebSocket -> MessagePack WAL -> Parquet.

| Crate | LOC | Role |
|---|---|---|
| `venue-core` | 148 | Domain types: `Event`, `Payload` (8 market data variants + error), `InstrumentId(Arc<str>)`, `VenueId(Arc<str>)`, `Level`, `Trade` |
| `venue-adapter` | 81 | Trait definitions: `EventSink`, `VenueAdapter<S>`, `Subscription`, `DataType`; error types with `Display` + `Error` |
| `venue-binance` | 640 | Binance Futures adapter: REST instrument fetch, `WsPool` with connection sharding, `#[serde(tag = "e")]` single-pass JSON deser, `AtomicU64` sequence numbers, exponential backoff reconnection |
| `wire` | 189 | MessagePack via `rmp-serde` with length-prefixed framing (`[u32 len][payload]`); 6 roundtrip + edge-case tests |
| `recorder` | 713 | `WalWriter` on dedicated OS thread with periodic fsync, `Drop` impl; Parquet converter streaming from `BufReader` for all 8 data types; 1 integration test |

Four examples: `smoke` (full pipeline + clean shutdown), `fetch_instruments`,
`read_wal`, `convert_wal`.

What doesn't exist: event bus, transport layer (UDS/SHM), replay, strategy
engine, configuration system, second venue, CI pipeline.

## What's Right

**EventSink as the pluggability boundary.** The trait is minimal:

```rust
#[async_trait]
pub trait EventSink: Send + Sync + Clone + 'static {
    async fn send(&self, event: Event) -> Result<(), EventSinkError>;
}
```

Every downstream consumer (WAL, future event bus, future strategy engine) slots
in by implementing one method. Venue code is generic over `S: EventSink`,
meaning transport changes require zero modifications to venue adapters. The
`Clone` bound is correct — it means `Arc` internally, which is what you need
for sharing a sink across spawned tasks. This single design decision is the
most important thing in the codebase and it's right.

**WAL -> Parquet separation.** Append-only binary writes on the hot path,
columnar conversion offline. The WAL absorbs burst throughput without blocking;
Parquet provides query-friendly storage. The wire format is shared between WAL
and future IPC, so the hot-path write is one MessagePack encode, not a
serialize-then-reserialize chain. Proven pattern.

**WsPool sharding.** Stream deduplication (FundingRate + MarkPrice + IndexPrice
all map to a single `@markPrice` stream), chunking by the 200-stream limit, and
transparent multi-connection management. Scales to hundreds of instruments.

**Reconnection with cancellation awareness.** `reconnect_loop()` with
exponential backoff (1s to 30s cap), full resubscription on the new connection,
and `CancellationToken` integration in both the read loop and the backoff sleep.
This is the most important reliability feature for unattended recording.

**Clean shutdown chain.** `smoke.rs` uses `tokio::select!` with `ctrl_c()`,
disconnects the adapter (which cancels all WsPool tasks and awaits them with a
3s timeout), then `drop(wal)` which drops the sender, breaks the background
thread's recv loop, flushes all `BufWriter`s, fsyncs, and joins. No data loss
on normal shutdown.

**WAL writer runs on a dedicated OS thread.** Not in the async runtime. Writes
are synchronous `BufWriter::write_all` with periodic `sync_data()` every 1s.
The channel between async and sync worlds is `mpsc::sync_channel(10_000)`.

**Backpressure is genuinely lossless (new).** `SyncSender::send` blocks when the
channel is full and only errors on disconnect, so a slow disk propagates back
through the WAL channel -> the venue `mpsc` -> `sink.send().await` -> the WS read
task. No silent drops on the happy path. Trade-off: a stalled disk stalls WS
reads and can get the connection dropped by the venue for slow consumption, and
there is no lossy/gap-detected alternative yet for latency-sensitive consumers.
(The `WalWriter::send` "channel full or closed" log is slightly misleading —
`send` never returns an error for "full".)

**Aggressor-side mapping is correct (new).** `aggTrade.m` (buyer-is-maker) maps
to `AggressorSide::Sell` — right, and a commonly inverted detail.

## What's Wrong

### Data Integrity (high) — new in this revision

The findings that matter most: they silently degrade the recorded data for its
stated downstream purpose (replay, book reconstruction, gap detection), and
several are *retroactively unfixable* — data recorded today without these fields
can never be repaired.

**D1. Depth updates discard `U` / `u` / `pu` — recorded book data is
unreconstructable.** `DepthUpdateMsg` (`venue-binance/src/lib.rs`) parses only
`s, E, b, a`. Binance USD-M diff-depth also carries `U` (first update id), `u`
(final update id), and `pu` (previous final update id) — the only fields that
prove a diff applies contiguously on top of a REST snapshot. They are dropped at
parse time, so they never reach the WAL or Parquet. This is upstream of Bug 1
below: even *with* a snapshot, an L2 book cannot be correctly reconstructed from
this data. Blocks the entire replay/backtest roadmap and cannot be fixed after
the fact.

**D2. No venue sequence/trade IDs — the `sequence` field gives no gap
detection.** `sequence` is a single `Arc<AtomicU64>` shared across all WS
connections, `fetch_add(Relaxed)` at emit time. It is monotonic by construction,
so it can never reveal a message the venue dropped, says nothing about
per-instrument ordering, and `markPriceUpdate` burns three values per message.
The fields that *would* enable gap detection — `aggTrade.a`, depth `U`/`u`,
bookTicker `u` — are discarded. As recorded, "sequence" is a global emit counter
with near-zero data-quality value. (Round 2 framed this as a reliability win; it
is not one.)

**D3. Parquet is not sorted by `venue_ts`, which breaks the documented replay
design.** `architecture.md` states replay does a k-way merge on `venue_ts`
"already sorted by timestamp within each file." It is not: N WS connections fan
into one `mpsc` -> one WAL in *arrival* order, and `venue_ts` is only
millisecond-resolution from Binance, so interleaving and ties are guaranteed.
Either the recorder orders per file on write (needs buffering) or replay must
sort (unbudgeted memory). Decide the sort contract before replay is built.

**D4. `level_idx` is recorded for depth *updates*, implying order that does not
exist.** `BookDepthColumns::push_levels` assigns `level_idx` by array position
and is reused for both `BookSnapshot` and `BookUpdate`. For a snapshot that is
meaningful; for a diff update the array is an unordered set of changes, so
`level_idx` misleads any consumer.

**D5. `Decimal -> f64` failures silently become `0.0`.** Every
`.to_f64().unwrap_or(0.0)` in `parquet_converter.rs` turns a conversion failure
into a plausible-looking zero price/qty, with no log — silent corruption, worse
than the f64 precision loss noted below. Log and drop (or carry null) instead.

**D6. WAL has no header/version/magic/CRC, and readers hard-stop on the first
bad frame.** `read_wal` and `convert_wal` break/return on the first decode error,
so one torn frame (crash mid-write, bad sector) silently truncates the rest of
that day with no resync. There is also no schema-version tag: any reorder of the
`Payload` enum makes every historical WAL undecodable. Add a frame magic +
version + length + CRC and skip-to-next-frame recovery before trusting this for
24/7 capture.

### Bugs

**1. No initial order book snapshot (medium).** `DataType::BookDepth` maps to
`@depth@100ms`, which emits incremental `depthUpdate` messages. The
`BookSnapshot` payload variant exists but is never produced. Without a REST
snapshot from `/fapi/v1/depth` as a starting point, the recorded depth updates
are unusable for book reconstruction. This blocks any future order book feature.
Necessary but not sufficient — see D1: the update IDs needed to splice a snapshot
onto the diff stream are also being dropped.

**2. Parquet converter accumulates full day in memory (medium).** The streaming
WAL decode is correct — frames are read one at a time. But each column collector
(`BookTickerColumns`, `TradeColumns`, etc.) appends every row into `Vec`s, then
writes a single `RecordBatch` per file. A full day of BookTicker data at
production volume could be tens of millions of rows. The writers then `.clone()`
every column `Vec` to build the Arrow arrays, doubling peak memory at write time.
Fix: write in batches (e.g., 500K rows per `RecordBatch`), which also improves
Parquet row group compression.

**3. `markPriceUpdate` emits 3 sequential awaits (low-medium).** Three
`sink.send().await` calls per message — mark price, index price, funding rate.
Under backpressure, this triples latency for this event type. Not a correctness
issue, but measurable at scale. Fix: buffer the three events locally and send as
a batch, or use `try_send` for the second and third.

**4. Instrument kind mapping is lossy (low).** `contract_type != "PERPETUAL"`
maps to `InstrumentKind::Spot`. Binance Futures has `CURRENT_QUARTER` and
`NEXT_QUARTER` delivery contracts — these are not spot. Fix: add
`InstrumentKind::Future { expiry: Option<NaiveDate> }` or just
`InstrumentKind::Delivery`.

### Code Quality

**Read loop duplication.** `connection_task_with_reader()` and
`reconnect_loop()` contain nearly identical 30-line read loops. A bug fix in one
must be mirrored in the other. Extract a shared `read_loop` function that takes
`reader`, `writer`, `sink`, `venue_id`, `seq`, `cancel` and returns when the
connection drops or shutdown is signaled.

**No jitter on backoff.** `ExponentialBackoff` doubles the delay deterministically.
Multiple connections reconnecting simultaneously will all hit the same delay,
creating thundering herd behavior. Add `rand::thread_rng().gen_range(0..=delay/4)`
jitter.

**No stale connection detection.** The code handles Binance pings but doesn't
detect stalled connections where no data arrives. A connection could silently stop
sending without triggering Close or Error. Add a "no message received in N
seconds" timeout that triggers reconnection.

**No SUBSCRIBE-ack validation (new).** `subscribe()` / `reconnect_loop()` send the
SUBSCRIBE frame and never read Binance's `{"result":null,"id":...}` response. A
rejected resubscribe (bad stream, rate limit) leaves a connection that is "up"
but receiving nothing, indefinitely — and with no stale-connection timeout
(above) nothing recovers it.

**Reconnect backs off before the first retry (new).** `reconnect_loop` calls
`next_delay()` at the top of the loop, so every reconnect — even a transient
blip — eats >=1s of guaranteed data gap. Attempt immediately, then back off only
on failure.

**`connect()` is a no-op (new).** The real TCP connect is lazy inside
`subscribe()`; `connect()` just returns `Ok(())`. The trait's connect->subscribe
lifecycle is misleading, and a second venue or the planned `venue-process`
harness may reasonably assume `connect()` establishes a session.

**WAL writers are never rotated or evicted (new).** `writers: HashMap<venue/date,
...>` only ever inserts; old-day entries are never closed, so file descriptors
accumulate over a multi-day run and every 1s fsync loop flushes all of them.
`architecture.md` says files roll "at midnight UTC or size threshold" — neither
exists; rotation is only an emergent side-effect of the event-derived date key
changing. Late events for a prior date (clock skew, reconnect) also reopen and
append to old-day files.

**`WalWriter` does not implement `EventSink` (new).** The README headline —
recorder/strategies/bus "all produce or consume the same `Event` type" via
`EventSink` — is not yet true. `WalWriter::send(&Event)` is an inherent,
synchronous method, and `smoke.rs` manually bridges `rx.recv()` -> `wal.send()`.
The uniform-sink story is aspirational until the recorder implements the trait.

**Dead types.** `Venue` struct in `types.rs` (defined, never used anywhere).
`ErrorPayload` / `Payload::Error` (defined, never constructed). `.env.example`
at workspace root (empty file). These should be cleaned up.

**`WalWriter::send()` signature hides cost.** Takes `&Event` but internally
clones. With `Arc<str>` IDs the clone is cheap for scalars, but `BookUpdate` and
`Trades` payloads contain `Vec<Level>` / `Vec<Trade>` which are heap-allocated.
The clone is necessary for the `mpsc` channel, but the `&Event` signature
misleads callers about the cost. Consider taking `Event` by value and letting
callers decide whether to clone.

### Architectural Debt

**`async_trait` on `EventSink`.** Every `send()` call goes through
`#[async_trait]`, which generates `Box<dyn Future>`. With static dispatch
through generics (`BinanceAdapter<S: EventSink>`), LLVM can often devirtualize
and elide the allocation, but it's not guaranteed. Native async traits have been
stable since Rust 1.75. Migrating removes the `async-trait` dependency, the
heap allocation concern, and makes the generated code simpler for the optimizer.
This isn't urgent — the overhead is small relative to JSON parsing and network
I/O — but it should happen before the transport layer is built.

**No configuration system.** Venues, instruments, data paths, log levels — all
hardcoded in example code. A TOML config file parsed with `serde` is the minimal
viable solution. This is the single biggest blocker for running the recorder
unattended: you shouldn't need to edit Rust source to change which instruments
are recorded.

## Latency Calibration

The architecture doc estimates 20-55us internal latency (WS recv to consumer)
with UDS, sub-10us with SHM. These are for internal event routing only.

The dominant latency is exchange wire latency. From a well-positioned cloud
instance (AWS Tokyo for Binance), WebSocket one-way latency is typically
1-5ms. From other regions, 10-50ms. At these timescales, internal routing of
20-55us is ~1-5% of total — small but not irrelevant.

Crypto market making operates in a different regime than equity HFT. Exchanges
don't offer traditional colocation. Binance matching engine latency itself is
single-digit milliseconds. Many profitable crypto market makers run on standard
Rust/C++ stacks without FPGA or kernel bypass. The Rust + tokio stack is
competitive for crypto — not for equity HFT, but that's not the target.

Where this infrastructure provides real edge: **data quality and backtesting
rigor.** Clean recorded data, deterministic replay, rapid strategy iteration.
The WAL + Parquet + replay architecture is exactly right for this.

## Dependency Assessment

| Dependency | Used by | Notes |
|---|---|---|
| `async-trait` | venue-adapter, venue-binance | Should migrate to native async traits |
| `tokio` | venue-adapter, venue-binance | Runtime; features = `full` in venue-binance |
| `tokio-tungstenite` | venue-binance | WebSocket client, `native-tls` feature |
| `tokio-util` | venue-binance | `CancellationToken` only |
| `futures-util` | venue-binance | `SplitSink`, `SplitStream`, `StreamExt`, `SinkExt` |
| `reqwest` | venue-binance | REST API (instrument fetch) |
| `serde` + `serde_json` | venue-core, venue-binance, wire | Serialization; `rc` feature for `Arc<str>` |
| `rmp-serde` | wire | MessagePack codec |
| `rust_decimal` | venue-core, venue-binance | Precise price/qty representation; `serde-with-str` feature |
| `arrow` + `parquet` | recorder | v55; Parquet output |
| `chrono` | recorder | Date key derivation (`DateTime::from_timestamp`) |
| `tracing` | workspace | Structured logging |
| `tracing-subscriber` | workspace (dev) | Log output in examples |

The dependency set is reasonable. `arrow` + `parquet` are heavy (~large compile
time) but necessary. No vendored C libraries. No nightly features.

## Roadmap

### Phase 0: Data-Model Fixes (before recording more data) — new

These gate everything downstream and several are retroactively unfixable, so they
come before the infrastructure work below.

0a. **Capture venue identity fields** — depth `U`/`u`/`pu`, `aggTrade.a`,
    bookTicker `u`. Thread them through the payloads, wire format, and Parquet
    schemas. (Fixes D1, D2.)
0b. **Decide the Parquet sort contract** — order events per file on write, or
    specify that replay must sort. (Fixes D3.)
0c. **WAL framing** — magic + version + length + CRC per frame, with
    skip-to-next-frame recovery on decode error. (Fixes D6.)
0d. **Stop silently zeroing `Decimal -> f64` failures** — log and drop or null;
    reconsider Decimal128/string storage at the same time (see "Not
    Prioritized"). (Fixes D5.)

### Phase 2a: Unattended Recording

1. **Configuration (TOML)** — `config` crate with serde. Venues, instruments,
   data paths, log levels. A `venue-process` binary that reads config and runs.
2. **WAL rotation** — roll at midnight UTC (or size threshold). Auto-trigger
   Parquet conversion of yesterday's WAL. Clean up old writers from the HashMap.
3. **CI pipeline** — `cargo fmt --check`, `cargo clippy`, `cargo test` on push.
4. **Extract shared read loop** — deduplicate WsPool read logic.
5. **Add backoff jitter** and stale connection timeout.
6. **Delete dead code** — `Venue`, `ErrorPayload`, `.env.example`.

### Phase 2b: Transport & Event Bus

7. **`transport` crate (UDS)** — `UdsSink` / `UdsSource` implementing
   `EventSink` over Unix domain sockets. Makes multi-process architecture real.
8. **Migrate off `async_trait`** — use native async traits before building new
   `EventSink` implementations.
9. **`event-bus` crate** — central routing with topic filtering (venue,
   instrument, data type).
10. **Metrics** — events/sec, `venue_ts` to `local_ts` gap, connection state,
    queue depths. `metrics` crate with Prometheus exporter or periodic stderr.

### Phase 3: Replay & Backtesting

11. **Initial book snapshot via REST** — fetch `/fapi/v1/depth`, emit
    `BookSnapshot`, then subscribe to `@depth@100ms` for updates.
12. **`replay` crate** — Parquet -> `EventSink` with k-way merge on timestamp,
    configurable speed (`RealTime`, `Multiplied(f64)`, `MaxThroughput`).
13. **Local order book reconstruction** — `BookSnapshot` + `BookUpdate` stream
    into an L2 book structure.
14. **`Strategy` trait** — `on_event(&mut self, event: &Event) -> Vec<Signal>`.
15. **Backtest harness** — replay -> strategy -> signal log.
16. **Second venue (Bybit or OKX)** — validates the VenueAdapter abstraction.

### Phase 4: Live Trading

17. **Order placement** — Binance REST + WebSocket user data stream.
18. **Position tracking** and order lifecycle (new -> ack -> fill).
19. **Risk management** — position limits, rate limiting, kill switch.
20. **Paper trading mode** — strategy emits orders, system logs but doesn't
    send.

### Not Prioritized (Do Later If Needed)

- **Decimal128 in Parquet** — f64 precision loss is theoretical at current
  crypto value ranges. Worth doing eventually, not blocking.
- **SHM ring buffers** — Phase 2 transport. UDS first; SHM only if UDS
  latency is measured and insufficient.
- **Zero-copy wire format** — `EventRef<'a>` borrowing from buffer. Only
  matters after profiling shows decode allocation as a bottleneck.
- **`WalWriter::send()` signature change** — take `Event` by value. Minor
  API improvement, not urgent.

## Code Quality Summary

| Area | State |
|---|---|
| Tests | 7 (6 wire roundtrip/edge-case + 1 recorder write-then-read) |
| Formatting | `rustfmt.toml` (max_width=100, edition 2021), `cargo fmt` clean |
| Clippy | Clean for `cargo clippy` (lib only); `--all-targets` emits 2 warnings: unnecessary `i as u64` cast (recorder test), unused `EventSink` import (fetch_instruments example) |
| CI | None |
| Error types | `Display` + `Error` on `VenueError`, `EventSinkError`, `WireError` |
| Logging | `tracing` in venue-binance + recorder; `tracing-subscriber` in examples |
| Dead code | `Venue` struct, `ErrorPayload`, `.env.example` |
| Doc drift | `architecture.md` shows stale `wire` signatures (`encode -> usize`, `decode -> Result<Event>`); `report_phase1.md` says architecture.md "still mentions bincode" (it no longer does) |

## Bottom Line

Structurally the foundation is sound: EventSink as the pluggability boundary, the
WAL + Parquet pipeline, process-per-venue isolation, and WsPool with reconnection
are all correct patterns, and the recorder's write path is genuinely lossless.

The real risk is not the architecture — it is the **data model**. Findings D1-D4
mean the depth and sequencing data being recorded today cannot do what the
replay/backtest roadmap needs, and that stays invisible until someone tries to
reconstruct a book in a later phase and finds the update IDs were never captured.
Unlike configuration, CI, and rotation — recoverable at any time — missing venue
fields and un-framed WAL data are *retroactively unfixable*.

The gap between the architecture doc (a complete multi-venue, multi-consumer
system) and the code (a single-venue recorder) is expected at this stage, and the
EventSink abstraction does mean transport, bus, and replay can be added without
modifying venue code — that part of the original assessment holds.

Revised priority order: **(1)** capture venue update/trade IDs and frame the WAL
(Phase 0) before recording more data; **(2)** fix the silent `0.0` corruption;
**(3)** the original infrastructure track — configuration, CI, WAL rotation;
**(4)** transport (UDS), event bus, and replay.
