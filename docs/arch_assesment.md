# Codebase Assessment

## What's Working Well

The architecture design is strong. The `EventSink` trait as the single abstraction
boundary is the right call — it gives you pluggable transport (`mpsc` → UDS →
SHM) without touching venue or consumer code. The workspace layout is clean, the
domain types in `venue-core` are well-modeled, and the WAL → Parquet pipeline is
a proven pattern used by serious quant shops. The architecture doc is unusually
thorough for a project at this stage.

The Binance adapter works end-to-end: REST instrument fetch, WebSocket
connection sharding, stream deduplication, message parsing, and event emission.
You have real data in `data/wal/` proving the pipeline runs.

## Bugs and Correctness Issues (Fix These First)

### 1. WalWriter has no graceful shutdown

`handle` is stored but never joined. No `Drop` impl. When `WalWriter` is dropped,
the background thread may be killed mid-write, corrupting the WAL tail. The
`BufWriter` never gets a final `flush()`.

```rust
// recorder/src/lib.rs:12-14
pub struct WalWriter {
    tx: mpsc::SyncSender<Event>,
    handle: Option<thread::JoinHandle<()>>,  // never used
}
```

### 2. Silent event drops everywhere

`WalWriter::send()` discards events when the channel is full with
`let _ = self.tx.send(...)`. In `handle_message`, every `sink.send()` result is
discarded with `let _ =`. You have zero visibility into data loss. At 50k
events/sec, you won't notice losing thousands of events.

### 3. WAL writer never calls fsync

`BufWriter` only flushes to OS page cache. A process crash or power loss loses
everything in the buffer. Professional WAL implementations fsync on a
configurable interval (e.g., every 1s or every N events).

### 4. WAL date key uses wall clock, not event timestamp

`recorder/src/lib.rs:42` — `Utc::now()` determines which file an event goes to.
Events arriving at 23:59:59.999 UTC may end up in the wrong day's file if the
clock ticks over. Should use `event.venue_ts` (or a designated event date).

### 5. Parquet converter loads entire WAL into memory

`parquet_converter.rs:15` — `fs::read(wal_path)?` reads the whole file. Your
architecture doc estimates 50-200 GB daily WALs. This will OOM. Need streaming
decode.

### 6. Double JSON parse per WebSocket message

`handle_message` parses JSON twice — once into `WsEventType` to read the `"e"`
field, then again into the specific message type. On the hot path at 50k msg/sec,
this doubles your parse cost. Use `serde_json::RawValue` to extract the
discriminant without full deserialization, or use `#[serde(tag = "e")]` on a
single enum to parse once.

> **[Disagreement]** The original assessment suggested `serde_json::Value` as an
> alternative. That would be *slower* than double parsing — `Value` heap-allocates
> for every JSON value. `RawValue` or a `#[serde(tag = "e")]` enum are the
> correct fixes. `Value` is not.

### 7. No WebSocket reconnection

`ws_pool.rs:60-69` — the read task loop breaks on `Close` or `Err`, and nothing
restarts it. In production, Binance connections drop every 24h minimum (their
documented behavior), plus random disconnects. Without reconnection +
resubscription, data collection stops silently.

### 8. BookUpdate events not handled in Parquet converter

`parquet_converter.rs:88` — `_ => {}` silently drops `BookSnapshot`, `BookUpdate`,
and `FundingRateRealized`. These are defined in your payload enum but never
persisted.

## Code Quality Issues

**Zero tests.** Not a single `#[test]` in any crate. For a system that handles
money, this is the highest-priority gap. Wire encode/decode roundtripping, event
construction, stream deduplication — all trivially testable.

**No logging.** Not a single `tracing::info!` or `eprintln!`. When something goes
wrong in production (and it will), you have zero observability. Add `tracing` +
`tracing-subscriber` as a workspace dependency.

**Error types are incomplete.** `VenueError` and `WireError` don't implement
`std::error::Error` or `Display`. This prevents using `?` operator with
`anyhow`/`thiserror` and makes error messages opaque.

**Inconsistent formatting.** Mixed indentation (2-space, 4-space, and irregular)
across files. Run `cargo fmt` and add a `rustfmt.toml`.

**Unnecessary allocations on hot path.** `InstrumentId { value: String }` and
`VenueId { value: String }` — every event clones these heap strings. For a known
small set of instruments, interned strings or `Arc<str>` would eliminate clone
costs. Even better: use a fixed-size `[u8; 16]` or enum for venue IDs since
you'll have ~3-5 venues.

## What to Build Next (Priority Order)

### Tier 1: Make What You Have Production-Grade

1. **`tracing` integration** — Add structured logging to every crate. Log
   connection state changes, subscription confirmations, parse errors, sink
   failures. This is non-negotiable infrastructure.
2. **WebSocket reconnection with exponential backoff** — Connections will drop.
   Implement automatic reconnection in `WsPool` with resubscription of all
   streams on that connection. Track connection health with heartbeat monitoring.
3. **Graceful shutdown** — Implement `Drop` for `WalWriter` (drop sender, join
   thread, flush). Add signal handling (`SIGTERM`/`SIGINT`) to example binaries.
   Drain queues before exit.
4. **Tests** — Wire roundtrip tests, adapter unit tests with mock sink, WalWriter
   write-then-read tests, Parquet converter correctness tests. Aim for the data
   path being fully tested.
5. **Fix the WAL** — Add periodic fsync, streaming decode for Parquet conversion,
   event-timestamp-based date keying.

### Tier 2: Core Missing Infrastructure

6. **Configuration system** — Use `serde` + TOML config files. Venues to connect,
   instruments to subscribe, data directory paths, transport selection, log levels.
   Right now everything is hardcoded.
7. **`transport` crate (UDS)** — `UdsSink` and `UdsSource` implementing
   `EventSink`. This is what makes the multi-process architecture real. Without
   it, everything runs in one process.
8. **`event-bus` crate** — Central routing process. Accept connections from venue
   processes, fan out to consumers with topic filtering. This is the backbone.
9. **Metrics** — Instrument latency (`venue_ts` to `local_ts` gap), message rates,
   queue depths, connection counts. Use `metrics` crate with Prometheus exporter
   or at minimum periodic stderr dumps.
10. **`venue-process` binary harness** — Config-driven binary that boots a
    `VenueAdapter`, wires transport, handles signals. Eliminates the need for
    custom `main()` per venue.

### Tier 3: Trading Capabilities

11. **Order book management** — Maintain local L2 order book from `BookSnapshot` +
    `BookUpdate` streams. This is essential for any strategy beyond basic signal
    generation. The data structure choice here matters a lot for performance
    (sorted vec vs. `BTreeMap` vs. custom array-backed book).
12. **`replay` crate** — Parquet → `EventSink` with k-way merge on timestamp. Add
    `ReplaySpeed` support. This enables backtesting.
13. **Strategy engine trait** — Define `Strategy` trait that consumes events and
    emits signals/orders. Keep it minimal:
    `on_event(&mut self, event: &Event) -> Vec<Signal>`.
14. **Execution layer** — REST/WebSocket order placement on Binance. Order
    lifecycle tracking (new → ack → partial fill → filled). This is where real
    complexity lives.
15. **Risk management** — Position limits, notional limits, rate limiting, kill
    switch. Must exist before any live trading.

## Rust Learning Opportunities in This Codebase

Based on the code, here are the Rust patterns you'd benefit from studying deeper:

- **Error handling with `thiserror`** — Replace manual error enums with
  `#[derive(Error)]`. Learn the `From` trait for error conversion chains.
- **Interior mutability** — `WsPool` will need `Arc<Mutex<>>` or lock-free
  patterns when you add reconnection. Study `parking_lot` vs `std::sync`.
- **Lifetime elision in traits** — The `EventSink` trait currently requires
  `'static`. Study when this is necessary vs. when `&'a` bounds work.
- **Zero-copy deserialization** — `serde_json::from_str` allocates new strings.
  Study `#[serde(borrow)]` and `Cow<'a, str>` for the hot path.
- **Type-state pattern** — For connection lifecycle (`Disconnected` → `Connecting`
  → `Connected` → `Subscribed`). Prevents calling `subscribe()` before `connect()`
  at compile time rather than runtime.
- **Async cancellation safety** — Your `tokio::spawn` tasks need to handle
  cancellation properly. Study `tokio::select!` and `CancellationToken`.

## Bottom Line

The architecture is sound and the design is ambitious in the right ways. The main
gap is reliability engineering — the code has a "happy path works" quality level
but no resilience, no observability, and no tests. For competing with professional
shops, the data infrastructure (recording, replay, backtesting) matters more than
low-latency execution at the start — you can't build strategies without clean
historical data. Prioritize making the recording pipeline bulletproof, add
reconnection, add logging, add tests. Then build toward the event-bus and replay
system, which unlock backtesting. Execution and risk come last because they're
dangerous without the foundation being solid.

---

# Revised Assessment

## Recalibrating the Previous Review

The previous assessment was largely accurate on the technical details but too
diplomatic in its framing. Here's a more honest take after a second pass.

## The Real State of the Project

This is ~1,060 lines of Rust across 5 crates. The architecture doc alone is 420
lines, the README another 196. You've written more documentation about the system
you intend to build than actual system code. That's not inherently bad — thinking
before coding is valuable — but it means the project is at about 15% of the
architecture it describes. The README and `docs/architecture.md` read like a
finished system; the code is an early prototype with one venue adapter and a basic
recording pipeline.

What actually exists and works:

- Binance Futures WebSocket connection with stream sharding
- JSON deserialization for 4 message types
- MessagePack WAL recording
- Batch WAL-to-Parquet conversion
- 4.3 MB of real recorded data proving the pipeline runs end-to-end

What's described but doesn't exist: event bus, transport layer, UDS/SHM, replay,
strategy engine, second/third venue, configuration system, any tests.

## What the Previous Assessment Got Right

The bug list is valid and I stand by all of it. The most dangerous ones:

- **No WAL fsync** — a crash loses your buffer. Unacceptable for a data recording
  system.
- **Silent event drops** — `let _ = sink.send(...)` everywhere. You can't know if
  you're losing data.
- **No reconnection** — Binance kills connections after 24h. Your recorder
  silently dies and you discover days later that you have gaps.
- **Full WAL loaded into memory** — will OOM at production data volumes.

## What the Previous Assessment Was Too Generous About

**"The architecture design is strong"** — I'll revise this. The `EventSink` trait
abstraction is a solid idea, but the execution has issues:

- `EventSink` requires `Clone + 'static + Send + Sync` and is `async`. The
  `Clone` bound means every concrete implementation needs `Arc` internally. The
  `async` on `send()` adds overhead on the hot path (future allocation, poll
  machinery). For a latency-sensitive system, the hot-path sink should probably be
  synchronous — a ring buffer write or channel send, not an async call. You're
  paying async overhead to do what is fundamentally a memcpy.

> **[Disagreement]** This overstates the problem. `async send()` doesn't
> necessarily mean heap allocation — with native async traits (stable since Rust
> 1.75), the future is stack-allocated. The current overhead comes from
> `async_trait`'s `Box<dyn Future>`, which is fixable by switching to native async
> traits. More importantly, making `send()` synchronous means you lose
> backpressure handling. A synchronous `try_send` that drops events on a full
> channel is worse than async backpressure. The right fix is dropping
> `async_trait`, not dropping `async`.

- `connect()` is a no-op (`Ok(())`). The actual connection happens inside
  `subscribe()`. This means the `VenueAdapter` lifecycle states (connect →
  subscribe → disconnect) don't match reality. A caller can't distinguish
  "connected but not subscribed" from "not connected at all."

> **[Disagreement]** This is fine for a prototype. The connect/subscribe
> separation exists to support venues that require explicit authentication before
> subscribing (private data streams, API key handshake). Binance public market
> data doesn't need it, so a no-op is reasonable for now. The trait contract is
> forward-looking — it'll matter when you add authenticated feeds.

- `Lifecycle` enum is dead code. It's defined in `venue-core` with 14 variants
  but never appears in `Payload`. There's no `Payload::Lifecycle(Lifecycle)`
  variant. The system has no way to emit connection state changes as events.

**"The domain types are well-modeled"** — They're adequate, not well-modeled.
`InstrumentId { value: String }` is a heap-allocated string with no validation, no
normalization guarantees (you lowercase in the adapter, but nothing prevents
constructing one with uppercase), and no interning. Every event clones this
string. With 300 instruments at 50k events/sec, that's 50k string clones per
second that could be zero-cost lookups into a static table or `Arc<str>`.

`VenueId` is worse — you only have 3-5 venues. This should be an enum, not a heap
string. `VenueId { value: "binance".to_string() }` allocates on every adapter
construction and clones on every event.

> **[Disagreement]** Making `VenueId` an enum couples `venue-core` to knowledge
> of which specific venues exist. Every new venue requires modifying the core
> crate. A better middle ground: `Arc<str>` or `&'static str`, which avoids per-
> event allocation without coupling core types to venue implementations. The enum
> approach sounds clean but violates the open/closed principle the architecture
> is built on.

## What the Previous Assessment Missed

### 1. `markPriceUpdate` handler emits 3 events from 1 message

`lib.rs:280-313` — each calls `sink.send().await` sequentially. If the sink has
any backpressure, you're tripling latency for this message type. More
importantly, these 3 events share the same `local_ts` but represent different
logical timestamps — the mark price, index price, and funding rate may not have
changed simultaneously. You're conflating "Binance batches these in one message"
with "these events are simultaneous."

> **[Disagreement]** The backpressure concern is valid, but the semantics argument
> is wrong. Binance's `markPriceUpdate` message genuinely contains all three
> values (mark price, index price, funding rate) sampled at the same `event_time`.
> They *are* simultaneous from the exchange's perspective — it's a single snapshot.
> Emitting 3 separate normalized events is the correct decomposition for an
> event model that separates concerns. The issue is the sequential `await` (batch
> the sends), not the semantic split.

### 2. `WalWriter::send(&self, event: &Event)` clones the event

`recorder/src/lib.rs:32` — every event going to the WAL gets fully cloned —
including all the `String` fields in `InstrumentId`, the `Vec<Level>` in book
updates, etc. The WAL writer takes `&Event` but internally does `event.clone()`
to send across the channel. At high throughput this is a meaningful allocation
cost. The signature lies about its overhead.

### 3. The smoke example has no shutdown path

`smoke.rs:30-33` loops `while let Some(event) = rx.recv().await` forever. Ctrl+C
kills the process, the WAL `BufWriter` never flushes, last N events are lost. For
a data collection tool, this is the normal operating mode — and it guarantees data
loss on every run.

### 4. No sequence numbers are ever assigned

`sequence: None` on every event. The architecture doc discusses sequence numbers
for gap detection, but no code ever sets them. Without sequences, you can't
detect dropped events during replay.

### 5. Parquet schema loses precision

`parquet_converter.rs` converts `Decimal` to `f64` for Parquet columns. `Decimal`
has 28-digit precision; `f64` has ~15. For prices like `67123.40000000`, this is
fine. For quantities like `0.00000001` (1 satoshi), floating point representation
introduces error. Professional systems store prices as fixed-point integers
(price * 10^8) or as string/decimal128 in Parquet.

> **[Disagreement]** The practical impact is overstated for crypto. All Binance
> prices and quantities fit within f64 precision for the value ranges you'll
> encounter. `1e-8` is exactly representable as IEEE 754 double. The real risk is
> accumulated rounding in *calculations* (e.g., notional = price * qty), not in
> storage of individual values. That said, using Decimal128 in Parquet costs
> almost nothing and is the professional choice — worth doing, but not a
> correctness bug at current scale.

## Being Honest About "Competing with Market Makers"

The architecture doc targets 20-55us internal latency with UDS and sub-10us with
SHM. These numbers are for internal event routing only. The actual bottleneck is
the exchange API wire latency: Binance WebSocket from a non-colocated server is
50-200ms. Your 20us internal routing is irrelevant when your signal arrives 100ms
after the exchange event.

> **[Disagreement]** The 50-200ms figure is misleading. That's worst-case from
> across the globe. From a well-positioned cloud instance (AWS Tokyo for Binance),
> WebSocket round-trip is typically **1-10ms**. At 1-10ms wire latency, 20-55us
> internal routing is ~1-5% of total — small, but not irrelevant.
>
> More importantly, crypto market making is a different game than equity HFT.
> Crypto exchanges don't offer traditional colocation. Binance's matching engine
> latency itself is single-digit milliseconds. Many profitable crypto market
> makers run on standard Rust/C++ stacks without FPGA. The Rust + tokio stack is
> within striking distance for crypto market making — not competitive for equity
> HFT, but that's not the target.

Professional market making firms use:

- Colocation (same datacenter as the exchange matching engine)
- Kernel bypass networking (DPDK, `io_uring`, custom NIC drivers)
- FPGA-based feed handlers that parse and act in single-digit microseconds
- Lock-free data structures, no async runtime, no heap allocation on hot path

The Rust + tokio + serde_json + WebSocket stack you have is fundamentally a
different tier. That's not a criticism — it's a calibration. This stack is
appropriate for systematic/quantitative trading at medium frequency (seconds to
minutes holding periods), which is where retail and small fund infrastructure
actually operates. You can build a profitable system at this tier. But calling it
"competing with market makers" sets wrong expectations about what the latency
engineering is actually buying you.

Where this infrastructure can compete: **data quality and backtesting rigor**.
Having clean recorded data, deterministic replay, and the ability to rapidly
iterate on strategies — that's the edge that matters for a small operation. The
WAL + Parquet + replay architecture is exactly right for this. Focus there.

## Revised Priority List

The previous list was mostly right but I'd restructure it:

### Phase 1: Make recording bulletproof (this is your actual product right now)

1. `tracing` — add it everywhere, you're flying blind
2. WebSocket reconnection with exponential backoff and resubscription
3. WAL fsync on interval, `Drop` impl for `WalWriter`, signal handling in smoke
   example
4. Streaming WAL decode in Parquet converter (don't load entire file)
5. Surface send errors instead of `let _ =` — at minimum log them
6. `cargo fmt`, fix warnings, add `clippy` to your workflow

### Phase 2: Make recording run unattended

7. Configuration (TOML) — venues, instruments, paths, log levels
8. `venue-process` binary with signal handling and health reporting
9. Assign sequence numbers for gap detection
10. WAL rotation at midnight + automatic Parquet conversion of yesterday's WAL
11. Basic metrics (events/sec, latency histogram, connection state)

### Phase 3: Enable backtesting

12. `replay` crate — Parquet → `EventSink` with timestamp ordering
13. `Strategy` trait — `on_event(&mut self, event: &Event)`
14. Local order book reconstruction from depth snapshots + updates
15. Basic backtest harness that runs replay → strategy → records signals

### Phase 4: Live trading

16. Order placement (Binance REST + WebSocket user data stream)
17. Position tracking
18. Risk limits
19. Paper trading mode (strategy emits orders, system logs but doesn't send)

## Rust Patterns to Study

One revision from the previous assessment: I'd add `#[inline]` and profile-guided
optimization to the learning list. When you do get to latency optimization,
knowing where to put `#[inline]` and how to read `perf`/`flamegraph` output
matters more than changing data structures. Measure first, then optimize.

Also study `bytes::Bytes` and `bytes::BytesMut` — these are the standard
zero-copy buffer types in the Tokio ecosystem, and your wire format + transport
layer should use them instead of `Vec<u8>`.

## Bottom Line (Revised)

The project has a clear vision and the right architectural instincts. The
`EventSink` abstraction, WAL + Parquet pipeline, and live/replay transparency are
all correct patterns used by real trading firms. But the gap between the
documentation and the code is large, and the code that exists lacks the
reliability engineering needed to trust it with real data collection. The
immediate path forward is narrow and clear: make the recorder run 24/7 without
data loss, add reconnection, add logging, add tests. Everything else — event bus,
SHM transport, strategy engine — is premature until you can reliably record a
week of data without intervention. That reliable recorder is the foundation
everything else depends on.
