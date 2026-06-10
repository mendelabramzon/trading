# Infrastructure Architecture Report — 2026-06-10

*Author: Fable (Claude Code). Scope: full read of all 5 crates (~1,960 LOC including
examples), all 4 examples, all 6 docs, manifests, and recorded data; toolchain
re-verified 2026-06-10 (`cargo test`: 8/8 pass; `cargo clippy --all-targets`: the 2
known warnings; the 4.5 MB WAL and single `book_ticker.parquet` match prior audits).*

*Relationship to prior documents. This repo already carries an unusually complete
audit trail: `arch_assesment.md` (D1–D7, Bugs 1–4), `improvement_plan.md` (the
accepted 11-step remediation), `audit-fable-09062026.md` (DOC*/N*/P*), and
`architecture-audit-fable.md` (A1–A18, funding-arb fitness). None of those findings
are re-litigated here; they are referenced by ID and assumed accepted. This report
answers a broader question those documents did not: **is this the right foundation
for a long-lived trading infrastructure** whose scope will grow beyond CEX perp
funding arb to prediction markets (Polymarket), on-chain data from multiple
blockchains, DEX/CEX market data, and multiple strategy and execution engines — and
**what is the target architecture** that gets there. New findings are tagged `R*`.*

---

## 1. Executive summary

**The skeleton is right and should not change.** `EventSink` as the universal
boundary, WAL-then-Parquet with one wire format, process-per-venue isolation,
replay-as-a-sink, dual timestamps, and `Decimal` prices have now survived four
independent reviews and a line-by-line re-read. Nothing in the expanded scope
invalidates them. The `Event` envelope itself generalizes cleanly to prediction
markets and on-chain sources with only two additions (a source id and optional
chain provenance) — what does *not* generalize is everything around it.

**Five structural decisions must be taken now, while the schema is already being
re-cut** by `improvement_plan.md`, because they are capture semantics — wrong
defaults are either retroactively unfixable or force a second re-record:

1. **Domain-namespace the payload enum** (R1). One flat `MarketDataPayload` cannot
   absorb funding + OI + liquidations + control + account + prediction-market
   lifecycle + chain events without becoming a monster every consumer matches
   exhaustively. Namespace now: `Market / Reference / Chain / Account / Control`.
2. **Add a raw-frame capture tier** (R2). D1, D7, N1, and A4 are four instances of
   one failure class: *the parser dropped something that cannot be recovered*. A
   raw tee makes that whole class survivable for the price of disk.
3. **Symbology with lifecycle** (A3 + R4). Funding arb is a cross-venue join and
   the join key does not exist; prediction markets add thousands of *ephemeral*
   instruments that the static-universe model cannot represent at all.
4. **Identity types wide enough for the target venues** (R6): the planned
   `Trade.id: u64` breaks on the second venue (Bybit hex ids, Polymarket token
   trades, chain tx hashes).
5. **Per-event source identity** (R9): which connection/poller/chain-watcher
   produced an observation — required for gap forensics, WS-vs-REST dedup, and
   multi-source venues.

**The expansion targets are pollers and watchers, not WebSocket pools.** Binance
already needs REST pollers (OI is REST-only — A6); Polymarket needs CLOB-WS + REST
catalog + a chain watcher; EVM chains are subscriptions over provider RPC with
reorg semantics. The unit of composition should be formalized as: *a venue process
hosts N `IngestSource`s sharing one sink and one WAL* (R11). The `VenueAdapter`
trait survives; the "venue = WsPool" idiom does not.

**The strategy/backtest/execution layers do not exist yet, and their hardest
property — live/backtest parity — must be designed in, not bolted on** (R5). The
mechanism is: all strategy I/O (events, clock, timers, orders) mediated by a
context object; virtual clock in replay; control events (A7) recorded and
replayed; framework-owned derived views (order book, funding curve) so every
strategy doesn't re-implement reconstruction.

**Sequencing insight: research must not queue behind infrastructure.** Months of
funding/OI history are downloadable from every target CEX today. The backfill
layer (A5) is not just hygiene — it is the fastest path to validating the actual
strategy economics, and it should land before replay, the bus, or any second
venue. Conversely, the latency roadmap (SHM, zero-copy) stays frozen (A1).

---

## 2. Current state (verified 2026-06-10)

| Crate | LOC | Tests | Role |
|---|---|---|---|
| `venue-core` | ~150 | — | `Event`, `Payload` (8 market-data variants + dead `Error`), `InstrumentId`/`VenueId` (`Arc<str>`), `Level`, `Trade` |
| `venue-adapter` | ~80 | — | `EventSink`, `VenueAdapter<S>`, `Subscription`, `DataType`, error enums (still `async_trait`; migration planned) |
| `venue-binance` | ~640 | 0 | USD-M futures adapter: REST `exchangeInfo`, `WsPool` sharding (200 streams/conn), tagged-enum JSON parse, reconnect with backoff |
| `wire` | ~245 | 7 | MessagePack + `[u32 len]` framing (no magic/version/CRC yet — D6 fix planned); `encoding_probe` pins rmp-serde layout |
| `recorder` | ~715 | 1 | `WalWriter` on dedicated OS thread, 1 s fsync; Parquet converter (manual example, full-day buffering — Bug 2 fix planned) |

What exists is a **single-venue, single-process recorder**: Binance WS → normalized
`Event` → WAL → (manually run) Parquet. What does not exist: transport, event bus,
replay, strategy engine, execution, backfill, config, CI, metrics, supervision,
second venue. The one real capture (45,041 events, 2026-06-05) decodes cleanly but
was made by a pre-sequence binary and contains no depth streams (N7); only
`book_ticker.parquet` was ever converted — confirming conversion is manual (DOC2/P6).

`improvement_plan.md` (D1–D7, Bugs 1–4, framing/CRC, RPITIT, rotation, snapshots,
config) is designed and unimplemented. This report assumes it lands and adds riders
to it (§7, Phase 0).

---

## 3. Assessment by axis

### 3.1 Component boundaries and current architecture

**Strong.** The boundary inventory is exactly right for a system this young:
`venue-core` (types) ← `venue-adapter` (traits) ← `venue-binance` (impl), with
`wire` and `recorder` orthogonal. No dependency cycles; no `unsafe`; venue code is
generic over `S: EventSink`, so transports can change without touching adapters.
The five-crate split has already paid for itself in reviewability.

**Fragile.** Three boundary problems, all already known in part:

- The **recorder is documented as a bus consumer** but durability must live at the
  edge (A2/DOC3). Affirmed here: the venue process links `WalSink` in-process;
  the bus serves live consumers only, lossy with `Gap` events. This single
  placement decision simplifies the bus enormously and removes its hardest
  requirement (lossless backpressure) before it is ever built.
- The **`Event` contract is loose**: four of five fields `Option` (N3), `sequence`
  is a per-process counter with no data value (D2), and nothing records which
  connection or source produced an event (R9). Tighten in the re-cut: `local_ts`
  mandatory, `sequence` dropped or rescoped, `source: SourceId` added.
- **`venue-adapter` types derive nothing** (N9) — `DataType` can't even be put in
  a `HashSet`. Free fix, blocks the bus's `TopicFilter`.

### 3.2 Data ingestion and normalization

**Strong.** Single-pass tagged-enum JSON deserialization; correct aggressor-side
mapping; `Decimal` end-to-end; lowercased venue symbols as stable raw keys; WsPool
dedup (FundingRate/MarkPrice/IndexPrice → one `@markPrice` stream) and sharding.

**Fragile.**

- **Normalization is a one-way door with no test coverage.** `handle_message` has
  zero tests (N4), and every field it drops is gone forever — D1 (`U/u/pu`), D7
  (depth `T`), N1 (instrument filters), A4 (funding interval) are all this one
  class. Two complementary fixes: parser fixture tests from captured frames (P4),
  and — new here — **R2: a raw capture tier**. Tee raw venue frames
  `(local_ts, source_id, bytes)` to `data/raw/<venue>/<date>.rawwal` using the
  existing WalWriter machinery. Default ON for any venue in bring-up; switchable
  off per venue once its parser has fixture coverage and reconciliation (A5) is
  green. Raw capture converts "parser bug = permanent data loss" into "parser bug
  = re-run normalization", which is the correct risk posture for an expansion
  roadmap that adds venue *classes*, not just venues.
- **Ingestion is WS-shaped.** OI is REST-only on Binance (A6); funding ground
  truth is REST (A5); Polymarket needs a market-catalog poller; chains need RPC
  subscriptions. **R11:** formalize the venue process as a set of `IngestSource`s
  (WS pool, REST pollers, chain watcher) sharing one sink, one WAL, one heartbeat.
  The pollers from A5/A6 then aren't a parallel system — they're sources.
- **Subscription model can't express venue-wide streams** (A6): `!markPrice@arr`
  is one stream vs 300+, and is immune to listing lag. Add `Scope::{Instruments,
  Class, All}`.

### 3.3 Storage design

**Strong.** WAL as append-only source of truth with one wire format shared with
IPC; Parquet as derived, re-derivable columnar storage; per-venue/per-day layout;
the D3 decision (files arrival-ordered, replay sorts) keeps the hot path simple.

**Fragile / missing.**

- Current Parquet is **uncompressed** (DOC4/P3), untyped timestamps, non-Hive
  layout, day-end-only manual conversion, no tiering (A14). The fixes are all in
  flight or specified; adopt them as a block: zstd, `Timestamp(Nanosecond,"UTC")`,
  `lake/venue=<v>/date=<d>/type=<t>/`, hourly conversion, row groups sorted
  `(instrument, venue_ts)` at conversion time, L2 only for the traded subset.
- **R10 — no catalog.** At 10 venues × 10 types × 365 days, "what data exists and
  passed QA" must be a query, not a glob. Add a manifest (SQLite or JSONL, written
  by the converter): file path, venue, date, type, row count, min/max ts, QA
  status, schema version. Replay and research read the manifest; the QA gate
  (A12) becomes a queryable property instead of a convention.
- **Schema evolution policy** (A15): version byte in WAL frames (planned),
  `schema_version` in Parquet metadata, additive-only payload changes, replay
  reads N and N−1. One paragraph in `architecture.md`, enforced in review.
- **Reference data is a first-class dataset, not a side effect** (N1/A11/R4):
  daily raw `exchangeInfo` dumps + an instruments SCD table
  (`valid_from`/`valid_to`) including tick/lot/filters, funding interval, fee
  schedule, and lifecycle state. Backtests join against it point-in-time.

### 3.4 Event streaming and message flow

**Strong.** The mpsc-based in-process flow is genuinely lossless today
(`SyncSender` blocks, never drops), and the planned UDS transport behind
`EventSink` is the right Phase-1 IPC.

**Fragile / missing.**

- **The bus spec is missing its hard parts** (A10): no subscription handshake, no
  restart semantics, no slow-consumer policy. With edge-WAL (A2) the bus needs
  none of the lossless machinery — spec it as *lossy-only, gap-counted,
  restart-tolerant*, one page, before building. Build-vs-buy stays open until ≥2
  live consumers exist; `iceoryx2` is the credible buy.
- **No control plane** (A7): reconnects are silent to consumers, replay promises a
  continuity that live violates, the bus can't drop honestly without a `Gap`
  event. `Payload::Control` — `ConnUp/Down`, `Gap`, `SnapshotBegin/End`, `SubAck`,
  `InstrumentChange`, `Reorg` — recorded like everything else, replayed like
  everything else. This is load-bearing for backtest honesty, not just ops.
- **Backpressure headroom is ~0.2 s at claimed peak** (N6): size channels in time
  units, expose depth gauges.
- Latency work (SHM, zero-copy `EventRef`) stays deferred indefinitely (A1). At a
  1 s–1 min decision cadence, UDS at tens of µs is ~1000× better than required.

### 3.5 Strategy engine design

Nothing exists; the README sketches `on_event(&Event)`. That signature is
insufficient for the flagship strategy, and this is the layer where a wrong
foundation quietly poisons everything above it.

**R5 — the strategy runtime must own time, state, and I/O.**

- **Funding arb is schedule-driven as much as event-driven.** Settlements occur at
  known times; entries/exits are timed against them. Strategies need timers
  (`on_timer`) and a clock — and both must be *virtual* in backtest and *wall* in
  live, or parity dies. Any strategy that calls `SystemTime::now()` or
  `tokio::spawn` is unbacktestable; the API must make that impossible by
  construction (all effects through a `Ctx`).
- **Derived state must be framework-owned.** Order-book reconstruction
  (snapshot + diff splice with `U/u/pu` validation), funding curves per canonical
  instrument (prediction, interval, next settlement, venue clamp), staleness
  watermarks — every strategy needs these, the QA tooling needs the same code, and
  the fill simulator needs the book builder. Build once as `View`s fed by the
  event stream; strategies consume views, not raw diffs.
- **Strategies are bus consumers, not bus citizens**: a strategy process =
  runtime + views + N strategy instances, subscribing with a `TopicFilter`
  expressed over symbology (canonical ids / classes — A17), emitting
  `OrderIntent`s. Decisions, intents, and the triggering event ids should be
  journaled through the same WAL machinery — the strategy's own audit log.

### 3.6 Backtesting and simulation

The replay crate is planned and its contracts are mostly settled (D3 sort, A9
merge clock, A7 control replay). Affirmations and additions:

- **Merge on `local_ts`** for cross-venue realism (A9/N5), with the ordering axis
  exposed per run (`venue_ts` remains right for single-venue book reconstruction).
  Chrony + `local_ts − venue_ts` monitoring make `local_ts` trustworthy.
- **QA-gated inputs**: replay refuses (or loudly flags) date ranges whose manifest
  rows lack a passing QA status (R10). A backtest on unaudited data is a rumor.
- **R6 — the economics layer is missing from all plans.** For funding arb the
  backtest needs, beyond replay: (a) a **funding accrual simulator** — positions ×
  realized funding events, validated against REST history; (b) a **fee model**
  from the reference SCD (maker/taker tiers, per venue); (c) a **fill simulator**
  with a conservative default — takers cross the spread at recorded top-of-book,
  makers fill only when a recorded trade prints through the limit price
  (queue-position-pessimistic). At this frequency P&L is dominated by funding
  accrual and fees, not microstructure — a conservative fill model is adequate,
  which is exactly why this strategy is backtestable at all. State that as a
  documented assumption with the slippage knobs it implies.
- **Determinism contract**: same binary live and replay; virtual clock; seeded
  RNG if any; event-sourced inputs only through `Ctx`. Write it down as an
  invariant the runtime enforces.

### 3.7 Execution and risk management

Absent by design (Phase 4), but two cheap reservations were already accepted
(A13: `Payload::Account`, private data off the shared bus). **R7** adds the
mechanics worth fixing on paper now:

- **Execution gateways live with the keys** — per-venue `ExecutionClient`
  co-located with (or structured like) the venue process, owning REST/WS auth,
  rate budgets, and the private user-data stream. Private events (orders, fills,
  positions, **funding payments received**) are recorded through the same
  WAL→Parquet machinery into `data/private/` — that dataset is how predicted
  funding gets validated against charged funding, closing the loop on the whole
  data product.
- **Risk is two-tier**: a synchronous `RiskGate` in the strategy process
  (position/notional caps per canonical instrument, order-rate caps, price
  collars) that every `OrderIntent` passes before transport; and an out-of-process
  **kill switch** (file/socket flag the gateways poll) that flattens and halts
  independent of any strategy bug. Plus a **reconciliation loop**: venue-reported
  positions vs local OMS state on every reconnect and on a timer — divergence
  halts trading.
- **OMS as a library**, not a service: order state machine
  (`New→Acked→PartFill→Filled/Canceled/Rejected`), idempotent client order ids,
  resync-on-reconnect. One per strategy process is fine at this scale.
- Paper trading = an `ExecutionClient` impl simulating against the live book view
  — same interface, zero strategy changes (the EventSink trick, applied to
  execution).

### 3.8 Observability, monitoring, fault tolerance

The weakest axis relative to the system's stated purpose (A12, N2, P5d all stand).
For a capture system, *silent partial death is the worst failure mode and currently
the most likely one*: a dead WAL thread leaves a healthy-looking process (N2);
rejected subscriptions leave silent zombie connections; nothing measures staleness.

Affirmed fixes, consolidated: per-process Prometheus metrics (events/s by
venue×type, channel depths, reconnect counts, WAL fsync lag, staleness watermarks
per stream); a once-a-minute heartbeat log as the cheap first detector (P5d);
WAL-failure fatality policy — exit and let the supervisor restart (N2); funding
**coverage** alerts (every live perp must show a realized funding event each
expected window — the strategy's own SLO); daily QA report emitted by conversion
(coverage %, gap count, dup count, `E−T` and `local−venue` latency distributions)
written into the manifest (R10).

**R16-class addition (folded into R10/R11):** make the venue process the unit of
supervision — systemd units with restart policies, a health endpoint, chrony as a
deploy prerequisite, disk-headroom alarms. None of this is novel; all of it is the
actual hard part of "we captured everything, 24/7, on 10 venues."

### 3.9 Scalability bottlenecks

Ranked by when they actually bite, not by how interesting they are:

1. **Operational surface** — bites at venue #2. No config, no supervision, no
   metrics, manual conversion. (Phase 1 below.)
2. **Storage volume** — bites at ~3 venues × full L2 universe (TB/week,
   uncompressed today). Tiering + zstd + retention (A14) solve it outright.
3. **Converter memory** — full-day buffering (Bug 2); fix already planned
   (500K-row streaming batches).
4. **Capture-channel headroom under burst** (N6) — 0.2 s today; size in seconds.
5. **Bus fan-out copies** — only at many consumers; lossy design + topic filters
   + `iceoryx2` escape hatch cover the foreseeable range.
6. **Per-event allocations** (N10) — real but last; profile before touching.

Non-bottlenecks at this frequency: tokio, UDS syscalls, MessagePack encode, JSON
parse. The latency ceiling is the exchange (1–5 ms wire), and the strategy cadence
is seconds — internal µs are noise (A1, affirmed).

### 3.10 Adding venues, chains, markets, strategies

Honest grading of each expansion target against today's abstractions, assuming
Phase 0 (§7) lands:

| Target | Fit today | What's missing | Effort |
|---|---|---|---|
| **Bybit / OKX** (CEX perps) | Good — WS pool + REST pattern transfers | Symbology (A3); string trade ids (R6); per-venue funding semantics on the payload (A4); contract multipliers (`ctVal`) | Low-medium |
| **Hyperliquid** (DEX perps) | Good — real WS, clean API; hourly funding | Funding-interval field (A4); poller pattern for info endpoints; the abstraction *proof* (A8) | Medium |
| **dYdX v4 / Drift / GMX** | Partial | Poller/indexer adapters; per-block funding accrual convention (A8); Solana account subs (Drift) | Medium-high |
| **Polymarket** (prediction CLOB) | Envelope fits; identity/lifecycle do not | R1 (Reference payloads: resolution, lifecycle); R4 (ephemeral universe — thousands of markets created/resolved continuously; universe manager keyed by lifecycle state); CLOB-WS + catalog poller + (later) chain watcher for settlement (R11); token-id instruments work as raw keys | Medium, **after** R1/R4 land |
| **EVM chains** (on-chain data) | New ingestion family | R3 (provenance: block/tx/log index, finality tag; `Control::Reorg`); provider RPC/WSS sources with rate budgets; u256 amounts exceed `Decimal` — store scaled value + raw string when exactness matters | High (new pattern, reusable across chains) |
| **Solana** | Same family as EVM | Slot/commitment-level provenance; account-subscription source | High first time, low after EVM |
| **New strategies** | Blocked | The strategy runtime existing at all (R5) | — |

Two cross-cutting notes. First, **prediction-market identity is two problems, not
one** (R4): venue-raw identity (condition id / outcome token id — works fine as
`InstrumentId` today) and *cross-venue event identity* ("the same election on
Polymarket and Kalshi") — the latter is fuzzy, human-curated, and belongs in a
research-layer registry, **not** in capture-layer symbology. Don't let it
complicate A3. Second, **reorgs do not break the WAL model** (R3): the WAL records
*observations*, and "I saw block B, then I saw it reorged" is itself an append-only
observation stream. What's needed is the vocabulary (provenance + `Reorg` control
events + finality tags), not a mutable store.

---

## 4. New findings index

| ID | Severity | Finding | Acts on |
|---|---|---|---|
| R1 | Critical (decide now) | Flat payload enum can't absorb multi-domain growth; namespace `Market/Reference/Chain/Account/Control` in the current re-cut, additive-only policy | improvement_plan step 3 |
| R2 | Critical (decide now) | No raw-frame capture tier; every parser defect is permanent loss (the D1/D7/N1/A4 class); add a raw tee, default-on for bring-up venues | new, Phase 1 |
| R3 | High | No chain provenance/finality/reorg vocabulary; `Decimal` can't hold u256 raw amounts | schema re-cut (reserve fields), Phase 5 |
| R4 | High | No instrument lifecycle; ephemeral universes (prediction markets) unrepresentable; cross-venue *event* identity is research-layer, keep out of capture symbology | A3/A11 riders |
| R5 | High | Strategy runtime must own clock/timers/state/I-O via `Ctx`; views (book, funding) framework-owned; parity by construction | Phase 4 design |
| R6 | High | `Trade.id: u64` (plan step 3) breaks on Bybit/Polymarket/chains — make ids venue-raw strings before wire v1 freezes | improvement_plan step 3 |
| R7 | Medium | Execution mechanics: gateways co-located with keys, sync RiskGate + out-of-process kill switch, position reconciliation, private WAL | Phase 6 |
| R8 | Medium | Backtest economics layer: funding-accrual sim, fee model from SCD, conservative fill model as documented assumption | Phase 4 |
| R9 | Medium | Per-event `SourceId` (which conn/poller/watcher) — gap forensics, WS-vs-REST dedup, multi-source venues | schema re-cut |
| R10 | Medium | Data catalog/manifest with QA status; replay and research query it instead of globbing | Phase 3 |
| R11 | Medium | Formalize venue process = N `IngestSource`s sharing one sink/WAL (generalizes A8; dissolves "venue=WsPool") | Phase 1–2 |
| R12 | Low | Workspace hygiene: consolidate deps in `[workspace.dependencies]`, trim `tokio full`/`prettyprint`, add CI + lints in one pass | with config work |

---

## 5. Decisions that will bite later — with recommendations

1. **Payload shape** → domain-namespaced enum (R1). One wire format, coarse
   consumer filtering, scoped version bumps. Generic/schema-registry envelopes are
   overkill at this scale; revisit only if a non-Rust consumer appears.
2. **What the WAL holds** → normalized events remain the spine (low-latency
   consumers need them); raw frames are a parallel tee, per-venue switchable (R2).
   Raw-primary would be purer but re-plumbs everything mid-remediation for little
   gain.
3. **Identity** → venue-raw keys for storage (already right); canonical symbology
   as a versioned dataset built from `fetch_instruments` + curated overrides (A3);
   lifecycle states + universe manager (R4/A11). Cross-venue *event* identity
   (predictions) stays in research.
4. **Durability placement** → WAL at the edge, bus lossy-only (A2). Affirmed;
   decide before `transport`/`event-bus` exist.
5. **Replay clock** → `local_ts` primary for cross-venue runs, axis exposed
   (A9/N5). Affirmed.
6. **Event contract** → `local_ts` mandatory; `sequence` dropped (venue ids do its
   job post-D1/D2); `source: SourceId` added (R9); `provenance: Option<…>`
   reserved for chains (R3).
7. **Latency roadmap** → frozen at UDS; SHM/zero-copy deleted from the roadmap,
   re-pointed at completeness (A1). Affirmed.
8. **Id width** → strings (`Arc<str>`) for trade/order/venue-sequence ids (R6).
   A few bytes per trade buys the entire venue expansion.
9. **Bus build-vs-buy** → defer until two live consumers exist; if building, the
   one-page lossy spec (A10) is mandatory; `iceoryx2` is the fallback. Don't
   build lossless consumer paths at all.
10. **Where strategies run** → out-of-process bus consumers from day one (matches
    the process model and keeps keys/risk separable), but the runtime API (R5) is
    transport-agnostic so a colocated mode stays possible.

What explicitly should **not** change: `EventSink`, WAL→Parquet, process-per-venue,
`Decimal` for prices, dual timestamps, tokio, MessagePack-in-framed-WAL.

---

## 6. Target architecture

### 6.1 Principles

1. **The data model leads.** Schemas are the only retroactively-unfixable layer;
   they get designed first and re-cut once (Phase 0).
2. **Record truth at the edge; derive everything else.** WAL (and raw tee) inside
   the capture process; Parquet, books, views, signals — all derived, all
   re-derivable.
3. **Abstractions follow second implementations.** No trait gets generalized until
   a second concrete source exists (Hyperliquid before "the DEX abstraction";
   EVM before "the chain abstraction"). Schemas lead; abstractions lag.
4. **Research is never blocked on infrastructure.** REST history → Parquet → DuckDB
   works before the bus, replay, or venue #2 exist.
5. **Completeness over latency**, measured: coverage, staleness, gap-rate SLOs
   instead of µs tables (A16).

### 6.2 Topology

```
            ┌──────────────────────────────────────────────────────────┐
            │ CAPTURE EDGE (one process per venue, supervised)         │
            │                                                          │
 Binance ──▶│ WsPool ─┐                                                │
 (WS+REST)  │ OI/funding pollers ─┤→ normalize → Event ─┬─▶ WalSink → data/wal/   (lossless)
            │ exchangeInfo poller ┘        │            ├─▶ raw tee  → data/raw/  (R2, optional)
            │                              ▼            └─▶ BusPub   → bus        (lossy)
            │                       heartbeat/metrics                 │
            └──────────────────────────────────────────────────────────┘
 Polymarket: CLOB-WS + catalog poller + chain watcher → same shape (R11)
 EVM chain : provider WSS/RPC sources + reorg tracking → same shape (R3)

            ┌─────────────┐    ┌──────────────────────────────────────┐
 bus ──────▶│ EVENT BUS   │───▶│ LIVE CONSUMERS                       │
 (UDS,      │ lossy, gap- │    │ strategy procs (runtime+views+risk)  │
  star)     │ counted,    │    │ monitor / dashboards                 │
            │ topic filter│    └──────────────────────────────────────┘
            └─────────────┘
 data/wal ──▶ converter (hourly) ──▶ data/lake/ (hive, zstd) + QA report + manifest
 REST backfill ──▶ data/lake/ (source=rest) + daily reconciler (A5)
 data/lake + manifest ──▶ replay (virtual clock, local_ts merge) ──▶ strategy runtime
                                                                      │
 strategy ──OrderIntent──▶ RiskGate ──▶ ExecutionClient (per venue, owns keys)
 venue private streams ──▶ private WAL → data/private/ (fills, funding payments)
```

Single-host by design and declared as such (A18); multi-host growth = ship
WAL/Parquet files, never stretch the bus.

### 6.3 Module map

| Crate | Status | Role |
|---|---|---|
| `venue-core` | extend (Phase 0) | Envelope v2, domain payloads, control events, symbology types, provenance |
| `venue-adapter` | extend (Phase 0–1) | `EventSink` (RPITIT), `IngestSource`, `Subscription{Scope}`, derives |
| `wire` | extend (in plan) | magic/version/CRC framing, `FrameReader`, BadVersion policy (P1) |
| `recorder` | extend (in plan + R2) | `WalSink` (edge lib), raw tee, rotation, converter → lake + QA + manifest |
| `config` | new (in plan) | TOML config; secrets via env only |
| `venue-process` | new (in plan, +R11) | harness: config → sources → sinks → supervision contract |
| `backfill` | new (Phase 2) | REST history pollers, reconciler, OI/liquidation capture |
| `symbology` | new (Phase 2, or in venue-core) | canonical registry build + point-in-time lookup |
| `bus` / `transport` | new (Phase 3) | UDS lossy fan-out, handshake, `Gap` injection — or `iceoryx2` adoption |
| `replay` | new (Phase 4) | manifest-driven Parquet → sink, virtual clock, control replay |
| `strategy` | new (Phase 4) | `Strategy` trait, `Ctx`, `Clock`, views (book/funding), runner (live+backtest), fill sim |
| `execution` | new (Phase 6) | `ExecutionClient` trait + `exec-binance`, OMS lib, `RiskGate`, paper mode |
| `qa` / `ops` | new (Phase 3, may live in recorder) | QA reports, manifest, coverage checks |

### 6.4 Core contracts (sketches, not signatures to copy verbatim)

**Envelope v2** (the Phase-0 re-cut, R1/R3/R9 + plan steps 3–5):

```rust
pub struct Event {
    pub venue: VenueId,                    // capture namespace: "binance", "polymarket", "ethereum"
    pub instrument: Option<InstrumentId>,  // venue-raw key; None only for venue-scoped events
    pub venue_ts: Option<Nanos>,           // = transaction time, uniformly (D7 contract)
    pub local_ts: Nanos,                   // mandatory: capture truth, chrony-disciplined
    pub source: SourceId,                  // u16 into per-process source registry (R9)
    pub provenance: Option<Provenance>,    // chains: {block, tx_index, log_index, finality} (R3)
    pub payload: Payload,
}

pub enum Payload {
    Market(MarketPayload),      // ticker, snapshot, update (+ids), trades(+id: Arc<str>),
                                // mark, index, funding pred/realized (+interval, clamps — A4),
                                // open_interest, liquidation (A6)
    Reference(ReferencePayload),// InstrumentAdded/Changed/Delisted, lifecycle state,
                                // MarketResolved {outcome} (prediction venues) (R4/A11)
    Chain(ChainPayload),        // reserved now, populated Phase 5 (R3)
    Account(AccountPayload),    // reserved: order/fill/position/funding-payment (A13)
    Control(ControlPayload),    // ConnUp/Down{source}, Gap{reason,count},
                                // SnapshotBegin/End, SubAck, Reorg{from_block} (A7/R3)
}
```

**Symbology** (A3/R4): `Asset("BTC")`; `CanonicalInstrumentId { base, quote,
class, settle }` with `class ∈ {Spot, Perp, Future{expiry}, PredictionOutcome,
Pool}`; a versioned mapping `(VenueId, InstrumentId) ↔ CanonicalInstrumentId`
with `valid_from/valid_to`, built from `fetch_instruments` + curated overrides.
Events stay keyed venue-raw; joins are one lookup. `Instrument` gains
`tick_size, lot_size, min_notional, contract_multiplier, settle_ccy,
linear/inverse, funding_interval, lifecycle_state`.

**Ingestion** (R11): a venue process hosts `Vec<IngestSource>` — each source is
"run until cancelled, emit Events into the shared sink, register a SourceId, and
report heartbeat". WsPool becomes one source kind; REST pollers (interval + rate
budget) another; chain watchers a third. `VenueAdapter` remains the venue-facing
factory that turns `Subscription{scope, data}` into sources.

**Strategy runtime** (R5):

```rust
pub trait Strategy {
    fn on_event(&mut self, ctx: &mut Ctx, ev: &Event);
    fn on_timer(&mut self, ctx: &mut Ctx, id: TimerId);
}
// Ctx mediates *everything*:
//   ctx.now() / ctx.set_timer(at)             — virtual in replay, wall in live
//   ctx.book(venue, inst) / ctx.funding(canon) — framework-owned views
//   ctx.submit(OrderIntent) / ctx.cancel(..)   — through RiskGate to ExecutionClient
//   ctx.log_decision(..)                       — journaled to the strategy WAL
// No other I/O. Parity is enforced by the type system, not by discipline.
```

**Execution** (R7): `ExecutionClient` per venue (submit/cancel/replace +
account-event stream); OMS state-machine library with idempotent client ids and
reconnect resync; `RiskGate` synchronous pre-trade checks + kill-switch flag;
paper-trading client against live views. Private events → `data/private/` WAL,
never the shared bus.

### 6.5 Storage layout and lifecycle

```
data/
  wal/<venue>/<date>.wal          # normalized, framed (magic/ver/CRC), source of truth
  raw/<venue>/<date>.rawwal       # optional raw tee (R2): [local_ts][source][len][frame]
  lake/venue=<v>/date=<d>/type=<t>/part-*.parquet   # hive, zstd, sorted (instrument, venue_ts)
  meta/instruments/…              # SCD table + raw exchangeInfo dumps (N1/A11)
  meta/manifest.sqlite            # file catalog + QA status + schema_version (R10)
  private/<venue>/…               # account streams (Phase 6); 0600, separate retention
```

Lifecycle: capture → hourly conversion + QA → manifest row (pass/fail) → WAL to
object storage / cold after QA pass, hot Parquet local N days (A14). Tiering:
top-of-book + funding + mark + OI + liquidations for *all* perps; full depth only
for the traded subset.

### 6.6 Build vs buy

| Concern | Call | Rationale |
|---|---|---|
| Event bus | defer; build 1-page lossy UDS bus or adopt `iceoryx2` | edge-WAL removed the hard requirement |
| Chain access | buy (Alchemy/QuickNode/Helius-class providers) | provenance fields make a later self-hosted swap invisible |
| Analytics | DuckDB/Polars on the lake | no Spark/warehouse at this volume |
| Orchestration | systemd + Prometheus + chrony | not k8s; single host is declared |
| Schema registry | none — version bytes + docs | revisit only for non-Rust consumers |
| Secrets | env / sops; never in TOML | execution keys arrive in Phase 6 |

---

## 7. Roadmap

Phases are sequential gates, each with an exit criterion. Phase 0 amends the
accepted `improvement_plan.md` rather than replacing it.

**Phase 0 — one schema re-cut (improvement_plan + riders).** Land steps 1–10 as
designed, plus, in the same wire-v1 freeze: domain namespacing (R1), funding
`interval`+clamps (A4), `OpenInterest`/`Liquidation` (A6), `Payload::Control`
(A7), `Reference` lifecycle events (R4), symbology core types (A3), string ids
(R6), `SourceId` (R9), reserved `provenance` (R3), `local_ts` mandatory, plus the
audit's P1–P5 amendments (BadVersion policy, parser fixtures, exchangeInfo dump).
*Exit: wire v1 frozen containing every retroactively-unfixable field; pu-chain
acceptance passes on a live capture.*

**Phase 1 — unattended capture.** Config + `venue-process` (plan step 11), edge-WAL
placement (A2), raw tee (R2), rotation + hourly conversion automation (P6),
heartbeat (P5d), WAL fatality policy (N2), startup retry (N8), systemd
supervision, CI (R12). *Exit: Binance capturing 7×24 with zero manual steps and a
daily QA report.*

**Phase 2 — completeness and reference.** `backfill` crate + daily WS-vs-REST
reconciliation (A5), OI/liquidation pollers (A6), instruments SCD + universe
manager (A11/R4), fee schedules. **Strategy research starts here** on REST history
— months of funding/OI data are available immediately. *Exit: funding coverage
100% vs REST for 14 consecutive days; research notebook answers cross-venue
funding-spread queries.*

**Phase 3 — research surface + distribution.** Hive lake, zstd, sorted row groups,
manifest + QA gates (R10, A14); then the lossy bus (or `iceoryx2`) with handshake
+ `Gap` injection (A10) once a second live consumer exists. *Exit: manifest-driven
DuckDB workflow documented; bus survives restart with consumers gap-counting.*

**Phase 4 — replay, strategy runtime, backtest.** Replay (manifest-driven,
`local_ts` merge, control replay — A9/A7), `strategy` crate (R5: Ctx/clock/views),
economics layer (R8: funding accrual + fees + conservative fills), funding-arb
strategy v1. *Exit: identical decisions from the same strategy binary in live
(paper) and replay over the same window.*

**Phase 5 — venue expansion proofs.** Bybit or OKX (CEX #2 — validates symbology +
funding semantics), Hyperliquid (DEX proof — A8), then Polymarket (new-domain
proof: lifecycle/resolution, ephemeral universe) and the `evm-ingest` skeleton
(R3). One at a time, each closing with a reconciliation + QA gate. *Exit: 4+
venues capturing 7×24; venue-core survived without a breaking re-cut (or the bump
is documented and migrated).*

**Phase 6 — execution.** `exec-binance` + OMS + RiskGate + kill switch (R7), paper
mode, then small live size; private WAL capturing fills and funding payments;
predicted-vs-charged funding reconciliation. *Exit: a live round-trip whose entire
lifecycle is reconstructable from recorded data.*

**Deferred indefinitely:** SHM transport, zero-copy `EventRef`, lossless bus
consumers, multi-host bus, k8s, schema registry, Decimal128 Parquet (A1 + prior
"not prioritized" list — all affirmed).

---

## 8. Bottom line

This codebase is a small, honest, well-audited recorder whose core abstractions
are the right ones for a much larger system. The gap to "professional trading
infrastructure" is not architectural rework — it is (1) one disciplined schema
re-cut that bakes in every field and namespace the expanded scope needs, because
schemas are the only layer mistakes cannot be undone in; (2) a raw-capture tier
that makes parser defects survivable while venue classes multiply; (3) the
completeness layer (backfill, reconciliation, reference data, QA) that prior
audits already identified as the real product; and (4) three genuinely new
layers — strategy runtime with enforced live/backtest parity, an economics-aware
backtest, and key-isolated execution with two-tier risk — whose contracts are now
specified well enough to build in order. Everything latency-flavored stays
frozen. The fastest route to the business goal runs through REST history and the
QA-gated lake, not through more transport engineering — sequence accordingly.
