# Remediation Plan — "What's Wrong" + Phase 0

> **Archived 2026-06-10.** Frozen historical record — steps 1–10 shipped as the
> Phase-0 wire-v1 re-cut (STATUS below); the step-11 remainder (config,
> `venue-process`, automation) moved to Phase 1 of
> [`report-fable-10062026.md`](../report-fable-10062026.md) §7. The load-bearing
> contracts were promoted into [`architecture.md`](../architecture.md).

*Companion to [`arch_assessment.md`](arch_assessment.md). Authored 2026-06-09 as the
implementation plan for the entire **What's Wrong** section (Data Integrity D1–D6,
Bugs 1–4, Code Quality, Architectural Debt) plus **Phase 0**.*

*Reviewed 2026-06-09 against source — substantive changes are listed under
[Review deltas](#review-deltas-2026-06-09); steps 3, 6, and 9 changed materially.*

## STATUS — implemented 2026-06-10 (steps 1–10 + report riders)

**Steps 1–10 are implemented, tested and live-verified** as the Phase-0
wire-v1 freeze of [`report-fable-10062026.md`](../report-fable-10062026.md) §7,
with these amendments shipped in the same re-cut:

- **R1**: `Payload` is domain-namespaced (`Market/Reference/Chain/Account/Control`)
  instead of the flat enum; `Payload::Error` deleted as planned.
- **Step 3 amendment (R6)**: `Trade.id` is `Arc<str>` (venue-raw string), not the
  planned `u64` — Bybit hex ids / Polymarket / chain hashes don't fit u64. Book
  sequence ids (`update_id`, `U/u/pu`) stay `u64` (splice arithmetic is
  load-bearing; deviation from report decision 8, documented in
  `architecture.md` §3).
- **Envelope v2 (D2 resolution + R9/R3)**: `sequence` dropped entirely;
  `local_ts` mandatory; `source: SourceId` added; `provenance: Option<Provenance>`
  reserved for chains.
- **A4/A6/A7/R4 riders**: funding `interval`+clamps (filled from
  `/fapi/v1/fundingInfo`, 8h default), `OpenInterest`/`Liquidation` payloads
  (liquidations wired live via `forceOrder`; OI awaits the Phase-2 poller),
  `ControlPayload` (ConnUp/Down + SubAck wired; Gap/Snapshot brackets/Reorg
  schema-only), `ReferencePayload` lifecycle events (producers: Phase 2),
  symbology core types (A3) including the extended `Instrument`.
- **P1**: `BadVersion` aborts the reader, never resyncs; conversion fails files
  with >1% corrupt bytes. **P2/N3**: timestamps/decimals null+warn, never 0.
  **P3**: zstd + `Timestamp(ns, UTC)` columns. **P4**: parser fixture tests.
- **R2** (report): raw-frame tee — `RawWalWriter` → `data/raw/<venue>/<date>.rawwal`,
  default-on in the smoke capture. **R12**: workspace deps consolidated, CI added.
- **Step 9 as built**: snapshot fetcher task triggered by first depthUpdate per
  symbol per connection session (re-snapshot on reconnect emergent) + 30-min
  periodic sweep; paced ≥0.5 s; one retry.
- **Step 11 split**: the documentation half is done; the `config` crate and
  `venue-process` binary move to Phase 1 (with P5d heartbeat, P6 conversion
  automation, N8 startup retry).

Acceptance: pu-chain + snapshot-splice check passes on a live capture
(`recorder/examples/verify_depth.rs`); see `report-fable-10062026.md` §7 Phase 0
exit criterion.

## Context

The assessment found the **data model**, not the architecture, to be the real risk:
several recorded fields are *retroactively unfixable* (you cannot repair data once it
is captured without the venue update IDs). This plan implements the full **What's
Wrong** section plus **Phase 0**.

Phase 0 (0a–0d) is fully subsumed by Data Integrity: 0a = D1+D2, 0b = D3, 0c = D6,
0d = D5. **Nothing in Phase 0 is missing from the D1–D6 work** — there is no separate
Phase 0 item to add; it is implemented *via* D1–D6 (plus D4, which is extra).

Crates live under `crates/`. The outcome: a recorder whose captured data is
reconstructable and gap-detectable, a self-healing WAL, a uniform `EventSink` story,
and an unattended, config-driven entrypoint.

### Confirmed decisions
1. **D3 sort contract = "replay sorts."** Converter streams 500K-row RecordBatches
   (also fixes Bug 2). Files are arrival-ordered; replay k-way-merges across files
   and sorts within a file. Document in `architecture.md`. Do **not** sort on write.
   In-file tie-break: **`(venue_ts, local_ts, in-file position)` — not `sequence`**.
   `sequence` resets per process run while same-day WAL files are append-reopened
   (one file spans several runs ⇒ duplicates), and it is assigned at `fetch_add`
   time, an await *before* the channel send, so in-file order can locally disagree
   with it.
2. **Bug 1 = emit snapshot only.** REST `/fapi/v1/depth` → `BookSnapshot{…,last_update_id}`
   on BookDepth subscribe. No live splice (that stays Phase 3).
3. **async_trait → native RPITIT now**, dropping the `async-trait` dep.
4. **Old ~4.3 MB WALs = obsolete.** New framed format only, no legacy fallback reader.
   Nothing auto-deleted.

### Three load-bearing corrections (verified against vendored crate sources)
- **D6 version byte pins FIELD LAYOUT, not variant order.** `rmp-serde` encodes
  structs as **positional tuples** and enum variants **by name**. So reordering
  `Payload`/`MarketDataPayload` variants is safe, but struct field layout changes
  (exactly what D1/D2 do) are not. *Review precision (rmp-serde 1.3.1 source +
  probe): same-arity field **reorders** mis-decode silently; field **add/remove**
  fails loud (`LengthMismatch`) — but the step-2 self-healing reader then skips
  every pre-change frame as "corruption", i.e. silent whole-file loss. Both routes
  end in silent loss without the version byte.* The version byte's contract: "the
  positional field layout of `Event` and every payload struct-variant body is fixed
  for this version; bump on any field add/remove/reorder or variant rename." Keep
  `WIRE_VERSION = 1` for this whole PR (no real v1 data to preserve, so the final
  schema *is* v1).
- **`WalSink` clone vs `WalWriter::drop` join hazard.** The writer thread exits only
  when **all** senders drop, including cloned `WalSink`s. `WalWriter::drop` joins, so
  any `WalSink` outliving `wal` hangs shutdown forever. Entrypoint must release the
  adapter (sole sink owner) **before** `drop(wal)`.
- **CRC covers `version + len + payload`** (not payload only) and decode **caps max
  frame length**, else a corrupted length passes a payload-only CRC and triggers a
  giant/garbage read.

### Review deltas (2026-06-09)

Found by checking the plan against the code and Binance USD-M semantics; this file
was untracked at the time, so the list doubled as the change record:

1. **Step 9 rewritten — snapshot lifecycle.** Snapshot-on-subscribe-only fails D1's
   goal on the first mid-day reconnect (broken `pu` chain, no fresh snapshot ⇒ book
   unreconstructable to end of day), and fetching right after SUBSCRIBE can produce
   a snapshot that predates the stream and never splices. This was the plan's
   biggest gap.
2. **Step 6 — Bug 3 call site added.** The plan added the `send_batch` mechanism
   but never switched `markPriceUpdate` to call it, so Bug 3 stayed open.
3. **Step 3 — depth `T` capture added** (new assessment finding D7): depth events
   stamp `venue_ts` from `E` while aggTrade/bookTicker use `T`, skewing cross-stream
   order by exchange-internal delay; depth `T` is dropped today and is in the same
   retroactively-unfixable class as `U`/`u`/`pu`.
4. **Step 3 — D4 wording fixed**: `book_snapshot.parquet` / `book_update.parquet`
   are *already* separate files; the actual work is forking the two schemas.
5. **Step 2 — resync rule pinned**: resume the scan at `bad_frame_offset + 1` (a
   payload can contain MAGIC; a corrupted `len` must not decide the skip), and
   MAGIC stays constant across format versions (the version byte evolves).
6. **Decision 1 — tie-break corrected** to `(venue_ts, local_ts, in-file position)`;
   `sequence` resets per run into append-reopened files and can locally disagree
   with arrival order.
7. **Verification — added a pu-chain/splice acceptance check** (proves captured
   depth data actually reconstructs, not merely that columns exist) and kept the
   `encoding_probe` as a pinned canary test.

Deliberately *not* added (noted, judged out of scope): Bybit/OKX string trade ids
vs `Trade.id: u64` (one-line caveat in step 3), pre-emptive reconnect before
Binance's 24 h forced disconnect (step 7 note), Parquet footer disorder metadata
for replay (verification note).

---

## Implementation (ordered so the workspace keeps compiling)

### 1. Dead-code cleanup (non-schema)
- `crates/venue-core/src/types.rs`: delete unused `Venue` struct.
- Delete empty `/.env.example`.
- `crates/venue-binance/examples/fetch_instruments.rs`: remove unused
  `use venue_adapter::EventSink;`.
- `crates/recorder/src/lib.rs` test: drop the redundant `i as u64` cast (the other
  `clippy --all-targets` warning).
- (`Payload::Error`/`ErrorPayload` removal is a *schema* change → step 3.)

### 2. WAL framing + self-healing readers (D6) — isolated to `wire` + readers
- `crates/wire/Cargo.toml`: add `crc32fast = "1"`.
- `crates/wire/src/lib.rs`: new frame
  `[magic: u32 LE = b"WAL1"][version: u8 = 1][len: u32 LE][crc32: u32 LE][payload]`.
  - `const MAGIC`, `const WIRE_VERSION: u8 = 1`, `const MAX_FRAME_LEN` (e.g. 16 MiB).
  - `encode`: write header, then payload; CRC over `version‖len‖payload`.
  - `decode`: validate magic, version, `len <= MAX_FRAME_LEN`, CRC; new
    `WireError::{BadMagic, BadVersion(u8), BadCrc, FrameTooLarge}`.
  - Add a resilient streaming reader (`FrameReader` over `impl Read`, or a
    `read_frame`/`resync` pair): on any bad frame, **scan forward byte-by-byte for the
    next MAGIC and resume**, returning a skip count so callers can log. Hard `Read`
    I/O errors still propagate; corruption does not.
  - **Resync rule (review)**: when a frame whose MAGIC was found at offset `p` fails
    any check (version/len/CRC), resume scanning at `p + 1` — payload bytes can
    contain MAGIC by chance, and a corrupted `len` must never decide how far to
    skip, or one bad frame swallows good ones. Distinguish a clean EOF from a
    truncated tail frame (log the latter; it is the normal crash signature).
  - **MAGIC is frozen across format revisions** — resync scans for it on files of
    every version, so it must never become `"WAL2"`; the version byte is the only
    evolution mechanism.
  - Update existing 6 tests for the new header; add: corrupted-CRC frame is skipped,
    resync finds the next good frame, truncated tail is handled.
  - Keep the (currently uncommitted) `encoding_probe` module, converted from
    `eprintln!` to assertions: it pins the rmp-serde behaviors (positional structs,
    by-name variants, silent same-arity swap) that the whole framing/versioning
    safety argument rests on, and fails fast if an rmp-serde upgrade changes them.
  - `decode_payload` is superseded by the framed reader — fold it into the frame
    path or delete it; don't leave a second, unframed decode entrypoint.
- `crates/recorder/src/lib.rs` `run()`: still calls `wire::encode` (signature
  unchanged) — no change needed beyond the new bytes.
- `crates/recorder/src/parquet_converter.rs` + `examples/read_wal.rs` +
  recorder test `test_wal_write_read`: switch to the resilient reader; **log + skip**
  bad frames instead of `break`/`return`.

### 3. Capture venue IDs (D1/D2/D7) + drop `Payload::Error` (D4 schema split) — one coupled commit
Everything here shares the positional msgpack layout, so it lands together.
- `crates/venue-core/src/types.rs`: `Trade` gains `id: u64` (aggTrade `a`).
  *(Caveat: this is Binance-shaped — Bybit v5 trade ids are hex strings that won't
  fit `u64`, so a second venue means a wire-version bump or a fabricated id.
  `Option<u64>` only buys "absent", not string ids; acceptable to lock `u64` now,
  but document the Binance-specific semantics.)*
- `crates/venue-core/src/payloads.rs`:
  - `BookTicker { best_bid, best_ask, update_id: u64 }`  (bookTicker `u`)
  - `BookSnapshot { bids, asks, last_update_id: u64 }`
  - `BookUpdate { bids, asks, first_update_id: u64, final_update_id: u64, prev_final_update_id: Option<u64> }` (`U`/`u`/`pu`; `pu` stays `Option` for venue
    generality — note it is in fact present on *every* USD-M futures depthUpdate,
    including the first; it's spot that lacks it)
  - Remove `Payload::Error` + `ErrorPayload` (never constructed). Keep `Payload` as an
    enum (single `MarketData` variant) for forward room — do **not** flatten.
- `crates/venue-binance/src/lib.rs`: add fields to parse structs and thread through:
  - `DepthUpdateMsg`: add `#[serde(rename="U")] first_update_id`, `#[serde(rename="u")] final_update_id`, `#[serde(rename="pu")] prev_final_update_id: Option<u64>`.
  - **D7**: `DepthUpdateMsg` also gains `#[serde(rename="T")] transaction_time`,
    and depth `venue_ts` switches from `E` to `T` — uniform **venue_ts =
    transaction time** across aggTrade/bookTicker/depth (today depth alone uses
    event time, skewing cross-stream order by exchange-internal delay). Optionally
    keep `E` as an extra `BookUpdate` field for delay metrics — decide now either
    way; it cannot be recovered later. Document the contract in `architecture.md`
    (step 11).
  - `BookTickerMsg`: add `#[serde(rename="u")] update_id`.
  - `AggTradeMsg`: add `a` → `Trade.id`.
  - Construct the payloads above with the new fields.
- `crates/wire/src/lib.rs` tests: extend fixtures to assert the new fields round-trip.
- `crates/recorder/src/parquet_converter.rs` (must compile against new shapes):
  - `TradeColumns`: add `id: Vec<u64>`; schema `trade_id: UInt64`.
  - `BookTickerColumns`: add `update_id: Vec<u64>`; schema `update_id: UInt64`.
  - **D4 schema split** *(correction: the two payloads already land in separate
    files — `book_snapshot.parquet` / `book_update.parquet` — via a shared
    `BookDepthColumns`; the work is forking that collector into two schemas, not
    separating outputs)*:
    - Snapshot columns keep `level_idx` (meaningful rank) + `last_update_id` (repeated per level row).
    - Update columns **drop `level_idx`** and carry `first_update_id`/`final_update_id`/`prev_update_id` (Option→nullable) repeated per level row.

### 4. Stop silent `0.0` corruption (D5)
- `crates/recorder/src/parquet_converter.rs`: replace every
  `.to_f64().unwrap_or(0.0)` with a helper `fn dec_to_f64_opt(d: Decimal, ctx) -> Option<f64>`
  that `tracing::warn!`s on failure and returns `None`. All price/qty/rate columns
  become `Vec<Option<f64>>`; mark those Arrow fields **nullable**
  (`Float64Array::from(Vec<Option<f64>>)` is supported in arrow 55).

### 5. Streaming RecordBatch writes (Bug 2, ties to D3)
- `crates/recorder/src/parquet_converter.rs`: flush each column collector to a
  `RecordBatch` every `const BATCH_ROWS = 500_000` and on EOF; write batches via one
  `ArrowWriter` per data-type file (multiple row groups). Removes full-day buffering
  and the per-column `.clone()` (build arrays from owned drained Vecs).

### 6. Migrate `async_trait` → native RPITIT (Architectural Debt)
- `crates/venue-adapter/src/lib.rs`:
  - `EventSink::send` → `fn send(&self, e: Event) -> impl Future<Output = Result<(), EventSinkError>> + Send;`
    The `+ Send` is **mandatory** (the future is `tokio::spawn`ed at `ws_pool.rs:104`).
  - Add default `fn send_batch(&self, events: Vec<Event>) -> impl Future<…> + Send`
    (loops `send`); WalSink overrides to enqueue together (Bug 3).
  - Same RPITIT treatment for `VenueAdapter` methods; migrate the
    `impl EventSink for tokio::sync::mpsc::Sender<Event>`.
  - Drop `#[async_trait]`; remove `async-trait` from `venue-adapter/Cargo.toml` and
    `venue-binance/Cargo.toml`.
- `crates/venue-binance/src/lib.rs`: remove `#[async_trait]` from impls; bodies
  otherwise unchanged. (Grep confirmed **no** `dyn EventSink`/`dyn VenueAdapter` — safe.)
- **Bug 3 call site (review — was missing)**: switch the `markPriceUpdate` arm of
  `handle_message` to build all three events (mark, index, funding) and emit them
  with one `sink.send_batch(...)` — the trait mechanism alone never fires; without
  this change Bug 3 stays open. The three events should keep distinct `sequence`
  values and may share one `local_ts` (they already do — one `now` per WS message).

### 7. ws_pool reliability (Code Quality) — `crates/venue-binance/src/ws_pool.rs`
- **Extract `read_loop`** used by both `connection_task_with_reader` and
  `reconnect_loop` (eliminates the duplicated ~30-line `select!`).
- **SUBSCRIBE-ack**: fold an `expecting_ack` phase into `read_loop` — after sending
  SUBSCRIBE, watch for `{"result":null,"id":…}` (Binance may send data first; can't
  add a second reader to the `SplitStream`). On rejection/timeout → reconnect.
- **Stale-connection timeout**: wrap `reader.next()` in `tokio::time::timeout(N)`
  inside the `select!`; elapse → treat as disconnect → reconnect.
- **Backoff jitter**: add `rand = "0.8"` (already transitively in `Cargo.lock`) as a
  direct dep; `ExponentialBackoff::next_delay` adds `gen_range(0..=delay/4)`.
- **Immediate first reconnect**: attempt the reconnect once before sleeping; move
  `next_delay()` to *after* a failed attempt. (This also shrinks the gap from
  Binance's scheduled **24 h forced disconnect** — every connection eats one per
  day — to sub-second. A pre-emptive overlapped reconnect before the deadline would
  eliminate it entirely; defer to Phase 2a.)
- **`connect()`**: document the lazy lifecycle (subscribe establishes the session)
  with a doc comment; keep it a deliberate no-op (lowest risk).

### 8. WAL rotation + `WalSink: EventSink` (Code Quality) — `crates/recorder/src/lib.rs`
- **Rotation/eviction**: on each fsync tick compute today's UTC date; flush + `sync_data`
  + drop (close) and remove any writer whose date `< today`. Log if a late event
  reopens an evicted date. Fixes the FD leak and the fsync-all-old-files cost.
- **WalSink split**:
  - Keep `WalWriter` owning the `JoinHandle` + its own `SyncSender` (inherent sync
    `send` stays — recorder test uses it; `Drop` drops tx then joins).
  - Add `WalSink { tx: SyncSender<Event> }` (`SyncSender: Clone`); `wal.sink() -> WalSink`.
  - `impl EventSink for WalSink`: async `send` does the **blocking** `SyncSender::send`
    (preserves lossless backpressure; document the blocking-in-async caveat for
    current-thread runtimes — fine on the multi-thread runtime). Override `send_batch`
    to push all events before returning. (Optional refinement: `try_send` fast path,
    falling back to `tokio::task::block_in_place` + blocking send only on `Full` —
    zero cost on the common path, and a saturated channel can't pin all worker
    threads. Not required for correctness; the WAL thread drains independently.)
  - **Shutdown contract** (doc-comment + enforce in step 11): release the adapter
    (drops its `sink: WalSink` clones) **before** `drop(wal)`, else the join hangs.
- `crates/venue-binance/examples/smoke.rs`: construct
  `BinanceAdapter::new(wal.sink())`, delete the manual `rx.recv() -> wal.send()` bridge;
  shutdown order `disconnect → drop(adapter) → drop(wal)`.

### 9. Order book snapshots (Bug 1) — `crates/venue-binance` *(rewritten in review)*
- `GET /fapi/v1/depth?symbol=<UPPER>&limit=<n>` (REST wants **UPPERCASE** symbol;
  WS uses lowercase), reusing the `reqwest` pattern from `fetch_instruments`. Fetch
  **sequentially/bounded** (weight scales with `limit`). Emit
  `BookSnapshot { bids, asks, last_update_id }` with `venue_ts` taken from the
  response's transaction time `T` (futures depth REST returns `E`/`T`) — not from
  local fetch time. Replay does the splice later (it now has `U`/`u`/`pu`).
- **Fetch ordering**: fetch only after the **first `depthUpdate` for that symbol
  has arrived** on the connection. That guarantees `lastUpdateId` falls at or after
  the stream's start, so the futures splice condition
  `U <= lastUpdateId <= u` is satisfiable. A fetch fired right after SUBSCRIBE can
  return a snapshot *older* than the first diff (`U > lastUpdateId`) — unsplicable,
  which silently re-creates Bug 1 with extra steps. (Alternative: fetch eagerly but
  validate against the first received `U` and refetch on failure.)
- **Re-snapshot on every reconnect, plus optionally every N minutes** (config).
  Each reconnect breaks the `pu` chain; without a fresh snapshot the book is
  unreconstructable from the gap to end of day, so the *lifecycle* — not the
  initial fetch — is what makes D1's "reconstructable" promise hold for 24/7
  capture. `reconnect_loop` is WS-only today, so this needs either an async
  post-resubscribe hook passed into the pool, or a periodic snapshot task owned by
  the adapter. The periodic task is simpler, covers reconnects it can't observe,
  and bounds replay seek time; on-reconnect + periodic is the robust combination.

### 10. Instrument kind (Bug 4)
- `crates/venue-core/src/types.rs`: `InstrumentKind` add `Delivery { expiry: Option<NaiveDate> }`
  (or plain `Delivery`). `chrono::NaiveDate` already available via recorder; add to
  venue-core if chosen.
- `crates/venue-binance/src/lib.rs` `fetch_instruments`: map
  `CURRENT_QUARTER`/`NEXT_QUARTER` → `Delivery` (parse `deliveryDate` from
  exchangeInfo); the old `_ => Spot` fallback is wrong (USD-M futures are never spot).
- Update any exhaustive `match` on `InstrumentKind`.

### 11. Configuration + unattended entrypoint (Architectural Debt) + doc update
- New `crates/config`: `serde` + `toml = "0.8"`. `Config { venues, instruments,
  data_dir, log_level, depth_limit, … }`.
- New `venue-process` binary (new bin crate `crates/venue-process`, or a `[[bin]]` in
  recorder): read TOML → build `WalWriter` + `wal.sink()` → `BinanceAdapter` →
  fetch/subscribe → run until ctrl_c → **disconnect → drop(adapter) → drop(wal)**
  (honoring the step-8 shutdown contract). This is the real replacement for
  `smoke.rs`.
- **Docs**:
  - `docs/architecture.md`: replay sort contract (D3 decision, incl. the
    `(venue_ts, local_ts, in-file position)` tie-break); the **venue_ts =
    transaction time** contract (D7); fix stale `wire` signatures; note framing
    (magic/version/len/CRC) and that midnight rotation now exists; the BookUpdate
    Parquet schema row is also stale (shows nested `bids[{price,qty}]`, actual is
    exploded level rows).
  - `docs/arch_assessment.md`: mark D1–D7 and Bugs 1–4 resolved; update Code-Quality
    items, "Current State"/LOC, Code Quality Summary (test counts, dead code,
    clippy), Dependency table (+`crc32fast`,`rand`,`toml`,`config` crate; −`async-trait`);
    remove the Phase 0 section (done) and completed Phase 2a items; refresh the
    Bottom Line.
  - `docs/report_phase1.md`: drop the stale "architecture.md still mentions
    bincode" note (assessment's Doc-drift row; it no longer does).

---

## New / removed dependencies
- **+** `crc32fast` → `wire`; `rand` → `venue-binance` (direct); `toml` + `serde` → new `config`.
- **−** `async-trait` from `venue-adapter` and `venue-binance`.

## Verification
- (Recommended first: stand up the Phase-2a CI job — `cargo fmt --check`, `clippy`,
  `test` — *before* starting; this change set touches every crate.)
- Per-crate `cargo build`, `cargo clippy --all-targets` (target: clean), `cargo test`.
- `wire` tests: roundtrip incl. new fields; **corrupted-frame skip + resync**
  (including: failed frame whose payload contains MAGIC resumes at `p+1`, not past
  good data); truncated tail; the `encoding_probe` canary asserts rmp-serde's
  positional/by-name layout.
- `recorder` test updated for framed format; add a test that writes good frames,
  **corrupts one mid-stream**, and asserts `convert_wal` recovers the rest.
- Build `venue-process`; run briefly against Binance (or `smoke`); confirm a
  `BookSnapshot` is emitted for a depth subscription and depth updates carry `U/u/pu`.
- **pu-chain / splice acceptance (the real proof of D1 + Bug 1)**: on a ~10-min
  live capture, assert per symbol that consecutive depth updates satisfy
  `pu == prev.u` (reconnect boundaries exempt) and that each snapshot satisfies
  `U <= lastUpdateId <= u` against the surrounding updates — i.e. the recorded data
  *reconstructs*, not merely that the columns exist. Kill the connection mid-run
  and confirm a fresh snapshot follows.
- Inspect a converted Parquet: depth-update file has no `level_idx` but has
  `first/final/prev_update_id`; snapshot file has `level_idx` + `last_update_id`;
  failed decimals appear as **null**, not `0.0`; row groups ~500K.
- (Optional, D3 aid) converter streams a per-file max-backward-`venue_ts` stat into
  the Parquet footer key-value metadata — replay can then choose windowed merge vs
  full sort, and data-quality monitoring gets a free disorder metric.
- Confirm clean shutdown (no hang) — validates the `WalSink`/`drop(wal)` ordering.
