# Architecture: Trading Data Framework

*Updated 2026-06-10 after the Phase-0 schema re-cut (wire v1 freeze). Sections
marked **[planned]** describe components that do not exist yet; everything else
is implemented and tested.*

## 1. System Overview

**Durability lives at the edge** (A2/DOC3): each venue process writes its own
WAL in-process through `WalSink` *before* anything else sees the event. The
future event bus serves live consumers only and is allowed to be lossy — it
injects `Control::Gap` events when it drops, and it is never in the durability
path. This dissolves the old "lossless to recorder vs consumers never block"
contradiction: there is no recorder process behind the bus at all.

```
  CAPTURE EDGE (one process per venue)                       [planned] LIVE PATH
 ┌──────────────────────────────────────────────┐
 │ venue-binance                                │            ┌───────────────┐
 │  WsPool (N conns, acks, stale watchdog) ──┐  │            │ event bus     │
 │  REST snapshot fetcher (paced) ───────────┤  │   lossy    │ (UDS, lossy,  │
 │  fundingInfo / exchangeInfo fetch ────────┤  │ ──────────>│  gap-counted) │
 │                                           ▼  │            └──────┬────────┘
 │            normalize → Event ── WalSink ──┬──│── data/wal/  (lossless)
 │            raw frames ──── RawWalSink ────┴──│── data/raw/  (best-effort tee)
 └──────────────────────────────────────────────┘                   │
                                                            [planned] strategy
 data/wal ──> parquet converter (manual today; hourly in Phase 1) ──> data/parquet/
 data/parquet ──> [planned] replay (virtual clock) ──> same EventSink consumers
```

Single-host by design (A18): UDS-class transports only. Multi-host growth means
shipping WAL/Parquet files to other machines, never stretching the bus.

## 2. Crate Structure

### Existing

| Crate            | Purpose                                                                      |
|------------------|------------------------------------------------------------------------------|
| `venue-core`     | Envelope v2 (`Event`), domain payloads (`Market/Reference/Chain/Account/Control`), symbology types (`Asset`, `InstrumentClass`, `CanonicalInstrumentId`, `LifecycleState`), `SourceId`, `Provenance`, `RawFrame` |
| `venue-adapter`  | Traits (RPITIT, no async_trait): `EventSink` (+`send_batch`), `RawFrameSink`, `VenueAdapter<S>`, `Subscription{scope, data}`, `Scope`, `DataType`, `VenueError` |
| `venue-binance`  | USD-M futures adapter: WsPool (sharding, ack watcher, stale watchdog, jittered backoff, control events, raw tee), REST depth-snapshot fetcher, fundingInfo/exchangeInfo fetch, parser fixture tests |
| `wire`           | Framed MessagePack: `[magic "WAL1"][version u8][len u32][crc32][payload]`; self-healing `FrameReader`; golden-bytes + encoding-probe tests pin the layout |
| `recorder`       | `WalWriter`/`WalSink` (lossless, dedicated thread, 1 s fsync, midnight rotation, fatal-exit on I/O error), `RawWalWriter` (R2 tee), Parquet converter (zstd, nullable columns, UTC-ns timestamps, 500K-row batches) |

### Planned (per `report-fable-10062026.md` §6.3)

`config` + `venue-process` (Phase 1), `backfill` (Phase 2), `symbology` registry
build (Phase 2), `bus`/`transport` (Phase 3, or `iceoryx2`), `replay` (Phase 4),
`strategy` (Phase 4), `execution` (Phase 6), `qa`/`ops` (Phase 3).

### Dependency graph (actual)

```
venue-core
    ^  ^________________________
    |             |             |
venue-adapter    wire           |
    ^  ^          ^             |
    |  |__________|_____________|
    |             |
venue-binance   recorder (depends on venue-core, venue-adapter, wire)
       (examples use recorder as dev-dependency)
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
  type `X`) — live-verified 2026-06-10 that fapi `@aggTrade` no longer emits;
  the aggTrade parser arm is kept as a fallback.
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

`Scope::All` maps to venue-wide streams where they exist (Binance:
`!markPrice@arr@1s`, `!forceOrder@arr`, `!bookTicker`) — one stream instead of
hundreds, immune to listing lag. `Scope::Class` expansion waits for the
Phase-2 universe manager. `DataType::OpenInterest` is REST-only on Binance and
warn-skips until the Phase-2 poller.

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
  Splice rule: apply updates with `U <= lastUpdateId <= u` onward; verified by
  `recorder/examples/verify_depth.rs`.
- **Funding metadata** (A4): `/fapi/v1/fundingInfo` is fetched at subscribe
  time; `FundingRatePrediction` events carry `interval` (8h default for
  unlisted symbols — the endpoint lists only deviants) and venue clamps.
  `premium_index` stays `None` until its stream is captured.
- **exchangeInfo dump** (P5a/N1): `fetch_instruments_raw()` returns the raw
  JSON body; the process (today: the smoke example, Phase 1: venue-process)
  writes it to `data/meta/binance/<date>-exchangeInfo.json` daily, so reference
  fields the parser drops remain recoverable. `fetch_instruments` parses
  tick/lot/notional filters, margin asset, delivery dates, lifecycle status and
  funding intervals into the extended `Instrument`.

## 5. Recording Layer

```
Events ─ WalSink ─> {data/wal}/{venue}/{date}.wal       (lossless, source of truth)
Raw WS ─ RawWalSink ─> {data/raw}/{venue}/{date}.rawwal (best-effort tee, R2)
WAL ──(manual `convert_wal` today; hourly automation lands in Phase 1 — P6)──> Parquet
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

## 6. Replay Layer **[planned — Phase 4]**

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

## 7. Event Bus **[planned — Phase 3]**

Lossy-only, gap-counted, restart-tolerant; spec'd in one page before building
(A10), or `iceoryx2` adopted instead. With the WAL at the edge the bus has no
durability requirement — never build lossless consumer backpressure. Slow
consumers get drops plus `Control::Gap{reason, dropped}` injections. Private
`Account` events never transit the shared bus (A13).

## 8. Scalability and Performance

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
scaling: WsPool shards at 200 streams/conn. Storage scaling: tiering (full
depth only for the traded subset) and the Hive/manifest lake layout land in
Phase 3 (A14/R10).
