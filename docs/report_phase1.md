# Phase 1 Report

## Scope

Five crates implemented, ~1,060 lines of Rust, end-to-end pipeline from Binance
WebSocket to Parquet proven with real data.

## What Exists

| Crate | Status | LOC | Purpose |
|---|---|---|---|
| `venue-core` | Complete | ~370 | Domain types: Event, Payload, InstrumentId, VenueId, Level, Trade, etc. |
| `venue-adapter` | Complete | ~59 | Traits: EventSink, VenueAdapter, Subscription, DataType |
| `venue-binance` | Working | ~479 | Binance Futures adapter: REST instruments, WS pool, 4 message types, reconnection |
| `wire` | Working | ~33 | MessagePack encode/decode with length-prefixed framing |
| `recorder` | Working | ~545 | WAL writer (dedicated thread) + Parquet converter (5 data types) |

Four runnable examples: `smoke` (full pipeline), `fetch_instruments`, `read_wal`,
`convert_wal`. Real data in `data/wal/binance/2026-06-05.wal` and corresponding
Parquet output.

## What Was Done Well

**1. EventSink abstraction is the right design.** The single trait boundary
between venue code and transport is the core architectural decision, and it's
correct. Swapping `mpsc::Sender<Event>` for `UdsSink` or `ShmSink` requires zero
changes to venue code. This is a clean separation that will pay off repeatedly.

**2. WebSocket reconnection exists.** `ws_pool.rs` implements `reconnect_loop()`
with exponential backoff (min to 30s cap), resubscription of all streams on the
reconnected connection, and cancellation-token awareness. The previous assessment
listed this as missing — it has since been implemented. This is the single most
important reliability feature for unattended recording.

**3. WsPool sharding works correctly.** Stream deduplication (e.g. FundingRate +
MarkPrice + IndexPrice all map to a single `@markPrice` stream), chunking by the
200-stream-per-connection limit, and transparent multi-connection management are
all implemented. This scales to hundreds of instruments without code changes.

**4. WAL + Parquet pipeline is a proven pattern.** Append-only binary WAL on the
hot path, background conversion to columnar Parquet for analysis. This is what
serious quant shops use. The wire format (MessagePack with length prefix) is
shared between WAL and future IPC, avoiding redundant serialization.

**5. WalWriter Drop impl exists.** The background thread is properly joined on
drop, and all BufWriters are flushed. This addresses the "no graceful shutdown"
bug from the assessment.

**6. Tracing is integrated.** `tracing` is a workspace dependency, `venue-binance`
imports it, and the `smoke` example initializes `tracing-subscriber` with
`EnvFilter`. This is the foundation for observability.

## Outstanding Bugs (by severity)

### Critical

**1. No WAL fsync.** `BufWriter` flushes to OS page cache only. A process crash
or power loss loses the entire buffer. For a data recording system, this is the
most dangerous bug. Fix: periodic `fsync` (every 1s or every N events) on the
underlying `File`.

**2. Full WAL loaded into memory.** `parquet_converter.rs:15` calls
`fs::read(wal_path)?` which reads the entire file into a `Vec<u8>`. The
architecture doc estimates 50-200 GB daily WALs. This will OOM long before that.
Fix: use `BufReader<File>` and decode events in a streaming loop.

**3. Silent event drops.** `WalWriter::send()` and every `sink.send()` in
`handle_message()` discard errors with `let _ =`. At 50k events/sec, you cannot
detect data loss. Fix: at minimum `tracing::warn!` on send failure; ideally
surface backpressure metrics.

### High

**4. WAL date key uses wall clock.** `Utc::now()` determines which WAL file an
event lands in. Events near midnight UTC may end up in the wrong day. Fix: derive
the date from `event.venue_ts` (or `event.local_ts` as fallback).

**5. Double JSON parse.** `handle_message` deserializes every WebSocket message
twice — once to extract the `"e"` field, then again into the specific type. At
50k msg/sec this doubles parse cost. Fix: use `#[serde(tag = "e")]` on a single
enum, or `serde_json::RawValue` to extract the discriminant without full parse.

**6. BookSnapshot/BookUpdate/FundingRateRealized silently dropped.** The Parquet
converter's `_ => {}` arm discards these event types. Data is recorded in the WAL
but never makes it to Parquet. Fix: add column collectors and writers for these
types.

### Medium

**7. No sequence numbers assigned.** Every event has `sequence: None`. Gap
detection during replay is impossible without them. Fix: assign monotonic
sequence per venue in the adapter or in a middleware layer.

**8. Decimal to f64 precision loss in Parquet.** `rust_decimal::Decimal` has
28-digit precision; `f64` has ~15. For the value ranges in crypto this is
unlikely to cause issues in storage, but accumulated rounding in calculations
downstream is a risk. Fix: use Decimal128 or string representation in Parquet.

**9. `markPriceUpdate` emits 3 sequential awaits.** Three `sink.send().await`
calls from one message. Under backpressure this triples latency for this event
type. Fix: batch the sends or use `try_send` for non-critical events.

**10. `smoke.rs` has no clean shutdown for WAL.** Ctrl+C kills the process.
Although `WalWriter` has a `Drop` impl, the tokio runtime may not run destructors
cleanly on signal termination. Fix: explicit signal handler that drops the
`WalWriter` before exiting.

## Dead Code

- **`Lifecycle` enum** (venue-core/src/lifecycle.rs): 14 variants defined, never
  used in any `Payload` variant. No `Payload::Lifecycle(Lifecycle)` exists. Either
  wire it into the event model or delete it.

- **`.env.example`**: Empty file, no environment variables are read anywhere.

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
will want a synchronous `try_send()` path. Consider splitting the trait:

```rust
pub trait EventSink: Send + Sync + Clone + 'static {
    fn try_send(&self, event: Event) -> Result<(), EventSinkError>;
}
```

The backpressure argument for async is valid but can be handled at the transport
level (e.g., the UDS sink batches internally and applies backpressure to its
internal queue, not to the caller).

**`InstrumentId` and `VenueId` as `String`.** These are cloned on every event.
With 300 instruments at 50k events/sec, that's 50k string clones/sec. `VenueId`
is especially wasteful — you'll have 3-5 venues. Use `Arc<str>` for both (not
`enum` for VenueId, which would couple venue-core to specific venue
implementations). Or even `&'static str` if the set is known at startup and
interned once.

**Wire format is MessagePack, architecture says bincode.** The architecture doc
specifies bincode for Phase 1 wire encoding. The implementation uses `rmp-serde`
(MessagePack). This is fine (MessagePack is arguably better for cross-language
compatibility), but the docs and code should agree.

**No configuration system.** Venues, instruments, data paths, log levels — all
hardcoded in example code. This is acceptable for a prototype but blocks
unattended operation. A TOML config file parsed with `serde` is the minimal
viable solution and should come before any new feature work.

## What to Implement Next

### Immediate (before any new features)

1. **WAL fsync** — periodic fsync on a configurable interval. This is the
   difference between "data recording tool" and "tool that might record data."

2. **Streaming WAL decode** — replace `fs::read()` with `BufReader` in the
   Parquet converter. Current code will OOM on real workloads.

3. **Surface send errors** — replace `let _ = sink.send()` with
   `tracing::warn!` at minimum. You need to know when events are being dropped.

4. **Fix WAL date keying** — use event timestamp, not wall clock.

5. **Signal handler in smoke example** — trap SIGTERM/SIGINT, drop WalWriter
   cleanly, exit.

### Next phase: unattended recording

6. **Configuration (TOML)** — venues, instruments, data paths, log levels. A
   `venue-process` binary that reads config and runs the adapter.

7. **WAL rotation** — roll at midnight UTC. Trigger Parquet conversion of
   yesterday's WAL automatically.

8. **Sequence numbers** — monotonic counter per venue, assigned at event creation
   time.

9. **Handle all event types in Parquet converter** — BookSnapshot, BookUpdate,
   FundingRateRealized.

10. **Metrics** — events/sec, `venue_ts` to `local_ts` gap (measures WS
    latency), connection state, queue depths. `metrics` crate with periodic
    stderr dump or Prometheus exporter.

### After recording is solid: transport and replay

11. **`transport` crate** — `UdsSink` / `UdsSource` implementing EventSink over
    Unix domain sockets. This is what makes the multi-process architecture real.

12. **`event-bus` crate** — central routing process with topic filtering. The
    backbone of the system.

13. **`replay` crate** — Parquet to EventSink with k-way merge on timestamp and
    configurable speed. This unlocks backtesting.

14. **Second venue (Bybit or OKX)** — validates that the VenueAdapter
    abstraction works for a genuinely different API. If the second venue requires
    changes to venue-core or venue-adapter, the abstraction needs rethinking.

### Later: trading

15. **Local order book reconstruction** from BookSnapshot + BookUpdate.
16. **Strategy trait** — `on_event(&mut self, event: &Event) -> Vec<Signal>`.
17. **Backtest harness** — replay -> strategy -> signal log.
18. **Order execution** — REST/WS order placement, lifecycle tracking.
19. **Risk management** — position limits, rate limiting, kill switch.

## Code Quality

| Area | State |
|---|---|
| Tests | Zero. No `#[test]` anywhere. |
| Formatting | Inconsistent. No `rustfmt.toml`. |
| Clippy | Unknown — not integrated. |
| CI | None. |
| Error types | `VenueError` and `WireError` don't impl `std::error::Error` or `Display`. |
| Logging | tracing imported in venue-binance, not used in wire or recorder. |

Minimum next steps: `cargo fmt`, `cargo clippy`, add `#[test]` for wire
encode/decode roundtrip, WalWriter write-then-read, and stream deduplication
logic.

## Summary

Phase 1 delivered a working single-venue data collection pipeline: Binance
WebSocket -> MessagePack WAL -> Parquet. The core abstractions (EventSink,
VenueAdapter, WsPool) are well-designed and the reconnection logic is in place.
The architecture documents describe a system that is roughly 15% implemented in
code.

The critical gap is not missing features — it's reliability of what exists. No
fsync, silent event drops, and OOM-prone Parquet conversion mean the current
system cannot be trusted for unattended 24/7 recording. Fixing these (5 targeted
changes) would make the recorder production-grade and provide the foundation for
everything downstream: transport, event bus, replay, and eventually live trading.

The architectural direction is sound. The implementation needs hardening before
expansion.
