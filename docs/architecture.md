# Architecture: Trading Data Framework

## 1. System Overview

```
              VENUE PROCESSES                    EVENT BUS                    CONSUMERS
         (one process per venue)             (central process)          (separate processes)

  ┌─────────────────────────┐
  │  venue-binance          │                ┌──────────────┐
  │  ┌───────────────────┐  │  EventSink     │              │     ┌──────────────────┐
  │  │ BinanceAdapter     │──│──(UDS/SHM)───>│              │────>│  recorder         │
  │  │  WsPool (N conns)  │  │               │              │     │  WAL -> Parquet   │
  │  └───────────────────┘  │               │   event-bus   │     └──────────────────┘
  └─────────────────────────┘               │              │
                                             │  topic route: │     ┌──────────────────┐
  ┌─────────────────────────┐               │  venue /      │────>│  strategy-engine  │
  │  venue-bybit            │  EventSink     │  instrument / │     │  (live consumer)  │
  │  ┌───────────────────┐  │               │  data_type    │     └──────────────────┘
  │  │ BybitAdapter       │──│──(UDS/SHM)───>│              │
  │  │  WsPool (N conns)  │  │               │              │     ┌──────────────────┐
  │  └───────────────────┘  │               │              │────>│  replay           │
  └─────────────────────────┘               │              │     │  Parquet -> Sink  │
                                             └──────────────┘     └──────────────────┘
  ┌─────────────────────────┐
  │  venue-okx ...          │
  └─────────────────────────┘
```

Each venue adapter runs as its own OS process. The event bus is a central process
that receives events from all venues and fans them out to consumers. All
inter-process boundaries use the `EventSink` trait, making the transport layer
pluggable without changing venue or consumer code.

## 2. Crate Structure

### Existing

| Crate            | Purpose                                                                      |
|------------------|------------------------------------------------------------------------------|
| `venue-core`     | Domain types: `Event`, `Payload`, `MarketDataPayload`, `Level`, `Trade`, `InstrumentId`, `VenueId`, `Nanos`, `Sequence`, `Instrument` |
| `venue-adapter`  | Traits: `VenueAdapter<S: EventSink>`, `EventSink`, `Subscription`, `DataType`, `VenueError` |
| `venue-binance`  | Binance Futures adapter: `BinanceAdapter<S>`                                  |
| `wire`           | MessagePack via rmp-serde with length-prefixed framing. `encode(Event) -> bytes`, `decode(bytes) -> Event`. Display + Error on WireError. |
| `recorder`       | WAL writer (dedicated thread, periodic fsync) + Parquet converter (all 8 data types, BufReader streaming decode). |

### Planned

| Crate            | Purpose                                                                       |
|------------------|-------------------------------------------------------------------------------|
| `transport`      | `EventSink` implementations for IPC. `UdsSink`/`UdsSource` (Phase 1), `ShmSink`/`ShmSource` (Phase 2). |
| `event-bus`      | Central event distribution. Receives from venue processes, topic-filters, fans out to consumers. |
| `replay`         | Reads Parquet, emits events through `EventSink`. Indistinguishable from live. |
| `venue-process`  | Thin binary harness: boots a `VenueAdapter`, wires transport, handles signals. |

### Workspace layout

```
crates/
  venue-core/
  venue-adapter/
  venue-binance/
  wire/
  transport/
  event-bus/
  recorder/
  replay/
  venue-process/
```

### Dependency graph

```
venue-core
    ^
    |
venue-adapter ────────> wire
    ^                     ^
    |                     |
venue-binance         transport
    ^                  ^      ^
    |                 /        \
venue-process    event-bus    recorder
                                 ^
                                 |
                               replay
```

## 3. Venue Adapter Layer

### Core traits

`EventSink` (`venue-adapter/src/lib.rs`) is the pluggability boundary:

```rust
#[async_trait]
pub trait EventSink: Send + Sync + Clone + 'static {
    async fn send(&self, event: Event) -> Result<(), EventSinkError>;
}
```

`VenueAdapter<S: EventSink>` (`venue-adapter/src/lib.rs`) is the venue-agnostic
interface:

```rust
#[async_trait]
pub trait VenueAdapter<S: EventSink>: Send + Sync {
    fn venue_id(&self) -> &VenueId;
    async fn fetch_instruments(&self) -> Result<Vec<Instrument>, VenueError>;
    async fn connect(&mut self) -> Result<(), VenueError>;
    async fn subscribe(&mut self, subs: Vec<Subscription>) -> Result<(), VenueError>;
    async fn disconnect(&mut self) -> Result<(), VenueError>;
}
```

When running in-process (tests, smoke examples), `S` = `mpsc::Sender<Event>`.
When running multi-process, `S` = `UdsSink` or `ShmSink`. Venue code does not
change.

### WebSocket connection sharding (WsPool)

Venues impose per-connection stream limits (Binance: ~200). Each venue crate owns
a `WsPool` that shards subscriptions across multiple connections transparently.

```rust
// Internal to each venue crate, e.g. venue-binance/src/ws_pool.rs

struct WsConn {
    writer: WsWriter,
    read_handle: JoinHandle<()>,
    stream_count: usize,
}

struct WsPool {
    conns: Vec<WsConn>,
    max_streams_per_conn: usize,  // 200 for Binance
}
```

`subscribe()` maps `(InstrumentId, DataType)` pairs to stream names, deduplicates
(e.g. FundingRate/MarkPrice/IndexPrice all map to `@markPrice`), chunks by the
per-connection limit, and opens new connections as needed. All connections share
the same cloned `EventSink`, so events from any connection flow to the same
destination.

Each venue crate owns its pool implementation because stream naming, subscription
message format, and limits differ per venue.

### Adding a new venue

1. Create `crates/venue-newvenue/`, implement `VenueAdapter<S>`.
2. Implement venue-specific WS message types and `WsPool`.
3. Build as a process via `venue-process` harness.
4. No changes to event-bus, recorder, replay, or any other venue.

## 4. Event Bus / IPC

### Wire format (`wire` crate)

Binary encoding for `Event`. Length-prefixed frames: `[len: u32][payload bytes]`.

Phase 1: MessagePack via rmp-serde (fast, compact, minimal code).
Phase 2: hand-rolled zero-copy layout with `EventRef<'a>` that borrows from the
buffer, eliminating allocations on the read path.

```rust
pub fn encode(event: &Event, buf: &mut Vec<u8>) -> usize;
pub fn decode(buf: &[u8]) -> Result<Event, WireError>;
// Phase 2:
pub fn decode_ref<'a>(buf: &'a [u8]) -> Result<EventRef<'a>, WireError>;
```

### Transport (`transport` crate)

**Phase 1: Unix Domain Sockets**

```rust
// Client side — implements EventSink, used by venue processes.
#[derive(Clone)]
pub struct UdsSink {
    tx: mpsc::Sender<Vec<u8>>,  // encodes, then background task writes to socket
}

impl EventSink for UdsSink { ... }

// Server side — reads from a UDS connection, yields Events.
pub struct UdsSource { ... }
```

Each venue process connects to the bus via a socket at a well-known path
(e.g. `/tmp/trading/bus.sock`).

**Phase 2: Shared Memory Ring Buffers**

```rust
#[derive(Clone)]
pub struct ShmSink {
    ring: Arc<ShmRingWriter>,  // mmap'd region, atomic head/tail
}

impl EventSink for ShmSink { ... }
pub struct ShmSource { ... }
```

Transition from Phase 1 to Phase 2 requires zero changes to venue code. Only the
concrete `EventSink` type passed to the adapter changes. The `venue-process`
harness reads a config flag to select transport.

### Event bus (`event-bus` crate)

Star topology. Venue processes are publishers, consumers are subscribers, the bus
is the single broker.

```rust
struct Bus {
    sources: Vec<Box<dyn Source>>,        // one per venue process
    subscriptions: Vec<ConsumerSub>,      // one per consumer
}

struct ConsumerSub {
    sink: Box<dyn DynEventSink>,
    filter: TopicFilter,
}

struct TopicFilter {
    venues: Option<HashSet<VenueId>>,
    instruments: Option<HashSet<InstrumentId>>,
    data_types: Option<HashSet<DataType>>,
}
```

Routing: the bus receives every `Event`, checks each consumer's `TopicFilter`
(inspects `event.venue`, `event.instrument`, payload discriminant), and forwards
matches.

Backpressure: configurable per consumer. Recorder uses lossless (bus pauses if
recorder falls behind). Strategy engine may use lossy with gap detection.

## 5. Recording Layer

```
Events ──> WAL Writer ──> *.wal (append-only binary)
                              |
                              |  background, periodic
                              v
                          Parquet Converter ──> *.parquet
```

### WAL Writer (hot path)

Runs on a dedicated OS thread (not async) to avoid runtime overhead on writes.
Uses `wire::encode` — same format as IPC, no redundant serialization.

```rust
struct WalWriter {
    base_dir: PathBuf,
    writers: HashMap<WalKey, BufWriter<File>>,
}

struct WalKey {
    venue: VenueId,
    date: NaiveDate,
}
```

Each record: `[len: u32][wire-encoded bytes]`. One WAL file per venue per day,
rolled at midnight UTC or size threshold. Buffered writes, periodic fsync.

### Parquet Converter (background)

Reads completed WAL files, groups events by `(venue, date, data_type)`, writes
Arrow RecordBatches to Parquet.

Schemas per data type:

```
BookTicker:  instrument, venue_ts, local_ts, bid_price, bid_qty, ask_price, ask_qty
Trades:      instrument, venue_ts, local_ts, price, qty, aggressor_side
BookSnapshot: instrument, venue_ts, local_ts, side, level_idx, price, qty
BookUpdate:  instrument, venue_ts, local_ts, bids[{price,qty}], asks[{price,qty}]
FundingRate: instrument, venue_ts, local_ts, rate, next_funding_time
FundingRateRealized: instrument, venue_ts, local_ts, rate, funding_time
MarkPrice:   instrument, venue_ts, local_ts, price
IndexPrice:  instrument, venue_ts, local_ts, price
```

### File layout

```
data/
  wal/
    binance/
      2026-06-04.wal
    bybit/
      2026-06-04.wal
  parquet/
    binance/
      2026-06-04/
        book_ticker.parquet
        trades.parquet
        book_snapshot.parquet
        book_update.parquet
        funding_rate.parquet
        funding_rate_realized.parquet
        mark_price.parquet
        index_price.parquet
    bybit/
      2026-06-04/
        ...
```

## 6. Replay Layer

Replay emits events through `EventSink`, making it indistinguishable from live.
A strategy consuming events cannot tell whether they come from a live venue or
from replay.

```rust
pub struct ReplaySource {
    parquet_dir: PathBuf,
    filter: ReplayFilter,
}

pub struct ReplayFilter {
    pub venues: Option<Vec<VenueId>>,
    pub instruments: Option<Vec<InstrumentId>>,
    pub data_types: Option<Vec<DataType>>,
    pub time_range: (Nanos, Nanos),
}

pub enum ReplaySpeed {
    RealTime,            // original inter-event gaps
    Multiplied(f64),     // 2.0 = 2x speed
    MaxThroughput,       // no delays
}
```

When replaying multiple venues or data types, k-way merge on `venue_ts` across
Parquet files (already sorted by timestamp within each file).

Two modes:
- **Direct**: replay into an in-process `EventSink` (for backtesting).
- **Bus**: connect to event-bus, replay as if it were a venue process.

## 7. Data Flow: Single Event Trace

```
1. WS RECV
   Binance sends: {"e":"bookTicker","s":"BTCUSDT","b":"67123.40",...}
   Arrives on one of WsPool's N connections.

2. PARSE (venue-binance, spawned read task)
   handle_message() deserializes JSON -> BookTickerMsg.
   Stamps local_ts = now_nanos().
   Constructs Event { venue, instrument, venue_ts, local_ts, payload }.

3. SINK (venue process -> transport)
   sink.send(event) — EventSink impl is UdsSink.
   wire::encode() serializes to bytes.
   Background task writes [len][bytes] to Unix socket.

4. BUS (event-bus process)
   UdsSource reads frame from socket.
   wire::decode() -> Event.
   Bus checks each ConsumerSub's TopicFilter, forwards matches.

5. RECORDER (recorder process)
   Receives Event via transport.
   WalWriter appends [len][wire bytes] to binance/2026-06-04.wal.

6. PARQUET (recorder, background)
   Reads completed WAL, groups by data_type.
   Writes to parquet/binance/2026-06-04/book_ticker.parquet.

7. REPLAY (later)
   Reads book_ticker.parquet, reconstructs Event.
   sink.send(event) — downstream consumer sees identical Event.
```

### Latency budget (Phase 1, UDS)

| Hop                       | Estimate   |
|---------------------------|------------|
| WS recv + JSON parse      | ~5-20 us   |
| wire::encode              | ~0.5-1 us  |
| UDS write + read          | ~5-15 us   |
| Bus routing               | ~1-2 us    |
| UDS write to consumer     | ~5-15 us   |
| **Total WS -> consumer**  | **~20-55 us** |

Phase 2 (SHM ring buffers) eliminates kernel copies, targeting sub-10us total.

## 8. Scalability

### Add a venue

New crate, new process. No changes to bus, recorder, replay, or other venues.

### Add instruments

WsPool sharding handles it transparently. More instruments = more connections
opened automatically within the venue process.

| Scenario                        | Streams | Connections (at 200/conn) |
|---------------------------------|---------|---------------------------|
| 300 instruments * 1 data type   | 300     | 2                         |
| 300 instruments * 3 data types  | 900     | 5                         |
| 300 instruments * 5 data types  | 1500    | 8                         |

### Add consumers

New process connects to event-bus with a `TopicFilter`. Bus fans out. Consumers
are isolated — a slow or crashing consumer does not affect others.

### Throughput estimates

| Metric                          | Estimate          |
|---------------------------------|-------------------|
| Event size (wire)               | ~80-200 bytes     |
| Peak rate (1 venue, all types)  | ~50k events/sec   |
| Wire throughput per venue       | ~5-10 MB/s        |
| Daily WAL size (1 venue)        | ~50-200 GB        |
| Parquet after compression       | ~5-40 GB/day      |
