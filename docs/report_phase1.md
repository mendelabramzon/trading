# Phase 1 Report

## Scope

Five crates implemented, ~1,771 lines of library/test Rust plus ~133 lines of
examples, end-to-end pipeline from Binance WebSocket to Parquet proven with real
data. Two rounds of hardening fixes have been applied since the initial
assessment: Round 1 addressed WAL reliability and logging, Round 2 addressed
event coverage, testing, code quality, and shutdown handling.

## What Exists

| Crate | Status | LOC | Purpose |
|---|---|---|---|
| `venue-core` | Complete | ~148 | Domain types: Event, Payload, InstrumentId (`Arc<str>`), VenueId (`Arc<str>`) |
| `venue-adapter` | Complete | ~81 | Traits: EventSink, VenueAdapter, Subscription, DataType; error types impl Display + Error |
| `venue-binance` | Working | ~640 | Binance Futures adapter: REST instruments, WS pool, 4 message types, reconnection, AtomicU64 sequence numbers |
| `wire` | Working | ~189 | 50 lib + 139 test; MessagePack encode/decode with length-prefixed framing; Display + Error on WireError |
| `recorder` | Working | ~713 | WAL writer with periodic fsync; Parquet converter for all 8 data types; BufReader streaming decode; ~95 lines tests |

Four runnable examples (~133 lines total): `smoke` (full pipeline with clean
shutdown), `fetch_instruments`, `read_wal`, `convert_wal`. Real data in
`data/wal/binance/` and corresponding Parquet output.

## What Was Done Well

**1. EventSink abstraction is the right design.** The single trait boundary
between venue code and transport is the core architectural decision, and it's
correct. Swapping `mpsc::Sender<Event>` for `UdsSink` or `ShmSink` requires zero
changes to venue code. This is a clean separation that will pay off repeatedly.

**2. WebSocket reconnection exists.** `ws_pool.rs` implements `reconnect_loop()`
with exponential backoff (min to 30s cap), resubscription of all streams on the
reconnected connection, and cancellation-token awareness. This is the single most
important reliability feature for unattended recording.

**3. WsPool sharding works correctly.** Stream deduplication (e.g. FundingRate +
MarkPrice + IndexPrice all map to a single `@markPrice` stream), chunking by the
200-stream-per-connection limit, and transparent multi-connection management are
all implemented. This scales to hundreds of instruments without code changes.

**4. WAL + Parquet pipeline is a proven pattern.** Append-only binary WAL on the
hot path, background conversion to columnar Parquet for analysis. The wire format
(MessagePack with length prefix) is shared between WAL and future IPC, avoiding
redundant serialization.

**5. WalWriter Drop impl exists.** The background thread is properly joined on
drop, and all BufWriters are flushed. This addresses the "no graceful shutdown"
bug from the assessment.

**6. Tracing is integrated.** `tracing` is a workspace dependency, `venue-binance`
imports it, and the `smoke` example initializes `tracing-subscriber` with
`EnvFilter`. This is the foundation for observability.

**7. Two rounds of hardening closed critical reliability gaps.** Round 1 fixed
WAL fsync, streaming decode, send error logging, date keying, and integrated
tracing. Round 2 added exhaustive Parquet output (all 8 event types), sequence
numbers via AtomicU64, 7 tests, `rustfmt.toml`, Display + Error on all error
types, clean shutdown in smoke.rs, and the single-parse `#[serde(tag = "e")]`
enum. The recorder can now be trusted for unattended data collection.

## Outstanding Bugs

### Fixed

**1. No WAL fsync** — Fixed (Round 1). Periodic `sync_data()` every 1 second
via `FSYNC_INTERVAL` constant. `fsync_all()` flushes BufWriter then calls
`sync_data()` on the underlying File.

**2. Full WAL loaded into memory** — Fixed (Round 1). `parquet_converter.rs`
uses `BufReader<File>` with frame-by-frame streaming decode (read 4-byte length
prefix, read payload, decode, repeat). No full-file read.

**3. Silent event drops** — Fixed (Round 1). All `sink.send()` calls in
`venue-binance` use `if let Err(e) = sink.send(...).await` with
`tracing::warn!`. WalWriter channel send failures are also logged.

**4. WAL date key uses wall clock** — Fixed (Round 1). Date key is now derived
from `event.venue_ts` (or `event.local_ts` as fallback), not `Utc::now()`.

**5. Double JSON parse** — Fixed (Round 1). `BinanceWsMessage` uses
`#[serde(tag = "e")]` with `#[serde(rename = "...")]` on each variant.
Single-pass deserialization directly to the correct type.

**6. BookSnapshot/BookUpdate/FundingRateRealized dropped** — Fixed (Round 2).
Exhaustive match in the Parquet converter handles all 8 `MarketDataPayload`
variants. Three new Parquet outputs: `book_snapshot.parquet`,
`book_update.parquet`, `funding_rate_realized.parquet`.

**7. No sequence numbers** — Fixed (Round 2). `BinanceAdapter` holds
`Arc<AtomicU64>`, cloned into each WsPool read task. Every event gets
`sequence: Some(seq.fetch_add(1, Ordering::Relaxed))`.

**8. smoke.rs no clean shutdown** — Fixed (Round 2). Uses `tokio::select!` with
`tokio::signal::ctrl_c()`, explicitly disconnects the adapter, then
`drop(wal)` to trigger flush and thread join.

### Still Outstanding

**Decimal to f64 precision loss in Parquet (medium).** `rust_decimal::Decimal`
has 28-digit precision; `f64` has ~15. For crypto value ranges this is unlikely
to cause storage issues, but accumulated rounding in downstream calculations is
a risk. Fix: use Decimal128 or string representation in Parquet.

**`markPriceUpdate` emits 3 sequential awaits (medium).** Three `sink.send()`
calls from one message (mark price, index price, funding rate). Under
backpressure this triples latency for this event type. Fix: batch the sends or
use `try_send` for non-critical events.

**WalWriter::send() clones the event (low).** Every event crossing the channel
boundary is fully cloned. This is inherent to the mpsc channel design — the
sender needs to transfer ownership. The signature (`&Event`) is slightly
misleading about the cost, but the clone is necessary given the architecture.
Accepted as-is.

## Dead Code

- **`Lifecycle` enum** — DELETED. Previously defined in venue-core with 14
  variants, never used in any `Payload` variant. Removed entirely.

- **`.env.example`** — still present at the workspace root. Empty file, no
  environment variables are read anywhere. Should be deleted.

## Architectural Opinions

### Agree with

**EventSink as the pluggability boundary.** This is the single most important
design decision and it's correct. The trait is minimal (`send(Event)`) and every
transport implementation slots in cleanly. The `Clone` bound is fine — it means
`Arc` internally, which is the right tradeoff for sharing a sink across spawned
tasks.

**Separate process per venue.** Crash isolation, independent deployment, no
shared state between venues. Each venue is a different API with different failure
modes — process isolation matches the domain.

**WAL before Parquet.** Append-only binary writes on the hot path, columnar
conversion in the background. The WAL absorbs burst throughput; Parquet provides
query-friendly storage. Correct pattern.

**MessagePack as Phase 1 wire format.** Fast, compact, schema-flexible. Bincode
would be marginally faster but ties you to Rust on both ends. MessagePack is a
reasonable default that can be replaced later without touching anything except
the `wire` crate.

### Disagree with

**`async fn send()` on EventSink.** The hot path — venue process writing to
transport — should not pay async overhead. With `async_trait`, every `send()`
heap-allocates a `Box<dyn Future>`. Native async traits (stable since Rust 1.75)
eliminate the heap allocation, but the async machinery (poll, waker) still adds
overhead compared to a synchronous channel send or ring buffer write. For Phase
1 with `mpsc::Sender`, async is fine. For Phase 2 with SHM ring buffers, you
will want a synchronous `try_send()` path.

**`InstrumentId` and `VenueId` as `String`.** RESOLVED. Both now use `Arc<str>`,
eliminating per-event heap allocation. Clone cost is an atomic increment.

**Wire format mismatch.** RESOLVED. The code has always used MessagePack via
`rmp-serde`; `architecture.md` was rewritten 2026-06-10 and documents the framed
format (`[magic][version][len][crc32][payload]`).

**No configuration system.** Still valid, unchanged. Venues, instruments, data
paths, log levels — all hardcoded in example code. A TOML config file parsed
with `serde` is the minimal viable solution and should come before any new
feature work.

## What to Implement Next

### Immediate (infrastructure)

1. **Configuration (TOML)** — venues, instruments, data paths, log levels. A
   `venue-process` binary that reads config and runs the adapter.

2. **CI pipeline** — `cargo fmt --check`, `cargo clippy`, `cargo test` on every
   push. The tests exist now; they need to run automatically.

3. **WAL rotation + auto Parquet conversion** — roll WAL at midnight UTC or size
   threshold. Trigger Parquet conversion of yesterday's WAL automatically.

4. **Delete `.env.example`** — empty file, no environment variables used.

### Next phase (transport & replay)

5. **`transport` crate (UDS)** — `UdsSink` / `UdsSource` implementing EventSink
   over Unix domain sockets. This is what makes the multi-process architecture
   real.

6. **`event-bus` crate** — central routing process with topic filtering. The
   backbone of the system.

7. **`replay` crate** — Parquet to EventSink with k-way merge on timestamp and
   configurable speed. This unlocks backtesting.

8. **Metrics** — events/sec, `venue_ts` to `local_ts` gap (measures WS
   latency), connection state, queue depths. `metrics` crate with periodic
   stderr dump or Prometheus exporter.

9. **Second venue (Bybit or OKX)** — validates that the VenueAdapter
   abstraction works for a genuinely different API. If the second venue requires
   changes to venue-core or venue-adapter, the abstraction needs rethinking.

### Later (trading)

10. **Local order book reconstruction** from BookSnapshot + BookUpdate.
11. **Strategy trait** — `on_event(&mut self, event: &Event) -> Vec<Signal>`.
12. **Backtest harness** — replay -> strategy -> signal log.
13. **Order execution** — REST/WS order placement, lifecycle tracking.
14. **Risk management** — position limits, rate limiting, kill switch.

## Code Quality

| Area | State |
|---|---|
| Tests | 7 tests (6 wire roundtrip/edge-case + 1 recorder integration) |
| Formatting | `rustfmt.toml` (max_width=100, edition 2021), `cargo fmt` applied |
| Clippy | Clean, no warnings |
| CI | None |
| Error types | All impl `Display` + `std::error::Error` (VenueError, EventSinkError, WireError) |
| Logging | `tracing` in venue-binance + recorder; `tracing-subscriber` in smoke example |

## Summary

Phase 1 delivered a working single-venue data collection pipeline: Binance
WebSocket -> MessagePack WAL -> Parquet. The core abstractions (EventSink,
VenueAdapter, WsPool) are well-designed and the reconnection logic is in place.

Two rounds of hardening fixes closed the critical reliability gaps identified in
the initial assessment. The WAL now fsyncs periodically, send failures are
logged, the Parquet converter streams instead of loading full files into memory,
all 8 event types are persisted, events carry monotonic sequence numbers, and
the smoke example shuts down cleanly. *(2026-06-09 correction: the subsequent
assessment found the captured data itself unreconstructable — D1–D7 — so
"trusted for unattended 24/7 collection" was premature. That bar was reached
with the 2026-06-10 Phase-0 re-cut; fully unattended operation still awaits the
Phase-1 venue-process/supervision work.)*

The next milestone is infrastructure: configuration (TOML), CI pipeline, and WAL
rotation. After that, the transport layer (UDS) and event bus make the
multi-process architecture real, and the replay crate unlocks backtesting.
