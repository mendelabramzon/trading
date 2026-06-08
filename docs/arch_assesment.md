# Architecture Assessment

*Assessed against code as of 2026-06-08. ~1,771 lines of Rust across 5 crates,
~133 lines of examples, 4.3 MB of recorded Binance data.*

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

## What's Wrong

### Bugs

**1. No initial order book snapshot (medium).** `DataType::BookDepth` maps to
`@depth@100ms`, which emits incremental `depthUpdate` messages. The
`BookSnapshot` payload variant exists but is never produced. Without a REST
snapshot from `/fapi/v1/depth` as a starting point, the recorded depth updates
are unusable for book reconstruction. This blocks any future order book feature.

**2. Parquet converter accumulates full day in memory (medium).** The streaming
WAL decode is correct — frames are read one at a time. But each column collector
(`BookTickerColumns`, `TradeColumns`, etc.) appends every row into `Vec`s, then
writes a single `RecordBatch` per file. A full day of BookTicker data at
production volume could be tens of millions of rows. Fix: write in batches
(e.g., 500K rows per `RecordBatch`), which also improves Parquet row group
compression.

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
| Clippy | Clean |
| CI | None |
| Error types | `Display` + `Error` on `VenueError`, `EventSinkError`, `WireError` |
| Logging | `tracing` in venue-binance + recorder; `tracing-subscriber` in examples |
| Dead code | `Venue` struct, `ErrorPayload`, `.env.example` |

## Bottom Line

The foundation is sound. EventSink as the pluggability boundary, WAL + Parquet
pipeline, process-per-venue isolation, and WsPool with reconnection are all
correct patterns. The code has been hardened through two rounds of fixes and the
recorder can be trusted for unattended data collection.

The gap is between the architecture doc (which describes a complete multi-venue,
multi-consumer system with transport layer, event bus, replay, and strategy
engine) and the code (which is a single-venue recorder). That gap is expected
at this stage — the important thing is that the existing code doesn't paint you
into a corner. The EventSink abstraction means transport, bus, and replay can be
added without modifying venue code.

Next priority: configuration system, WAL rotation, and CI. These are
infrastructure that make the recorder production-ready. After that: transport
(UDS) and event bus, which make the multi-process architecture real. Then
replay, which unlocks backtesting.
