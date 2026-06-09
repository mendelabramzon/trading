# Architecture Audit — Fitness for Mid-Frequency Perp Funding Arbitrage

*Auditor: Fable (Claude Code), 2026-06-09. Scope: `docs/architecture.md` and
`README.md`, evaluated against the stated business target — **mid-frequency
funding-rate arbitrage on perpetual futures across many CEX and DEX venues, many
instruments** — and cross-checked against the current source of all five crates.*

*Relationship to prior documents: `arch_assesment.md` (D1–D6, Bugs 1–4),
`improvement_plan.md`, and `audit-fable-09062026.md` (DOC*/N*/P*) cover
code-level data integrity and are **not re-litigated** here. This audit asks a
different question: assuming the remediation plan lands cleanly, is this the
right architecture for the strategy? Finding namespace: `A*`.*

---

## 1. Verdict

The skeleton is good. `EventSink` as the single pluggability boundary,
process-per-venue isolation, WAL-then-Parquet with one wire format, and
replay-as-a-sink are all correct decisions and worth keeping. The code that
exists (WsPool with reconnect/backoff, Decimal prices, dual timestamps) is
better than the median first cut of this kind of system.

The problem is that the architecture is **optimized for the wrong axis**. Both
documents treat latency as the scarce resource (latency budget tables, an SHM
phase targeting sub-10 µs) while the strategy you are building — funding arb at
mid-frequency — is decided on a 1 s–1 min cadence against funding intervals of
1 h–8 h. At that horizon, microseconds are worth nothing and the things that
are worth everything are absent from both documents and all roadmaps:

1. **Completeness**: REST backfill and WS-vs-REST reconciliation (A5), recording
   at the edge instead of behind the bus (A2), control/gap events (A7).
2. **Cross-venue identity**: a symbology layer (A3) — funding arb is a
   cross-venue join, and the system currently has no join key.
3. **Correct funding semantics**: per-venue intervals, premium/clamp mechanics
   (A4) — without these the recorded numbers cannot be compared across venues.
4. **Inputs the strategy actually trades on**: open interest, liquidations,
   point-in-time instrument metadata, new-listing detection (A6, A11).
5. **Ops**: staleness monitoring, coverage QA, supervision (A12) — the actual
   hard part of 24/7 multi-venue capture.

None of these are expensive. Most are cheaper than the planned SHM transport.
The recommendation in one sentence: **freeze the latency roadmap at UDS, and
spend Phase 2 on completeness, symbology, and funding semantics instead.**

---

## 2. The lens: what mid-freq funding arb actually requires

Requirements derived from the strategy, used as the grading rubric:

- **R1 — Funding data is the product.** Every predicted and realized funding
  event, for every perp on every venue, with the venue's interval and clamp
  semantics attached. A missed book tick is noise; a missed funding settlement
  is a hole in the P&L ground truth.
- **R2 — Cross-venue joins are the query.** Every question has the shape
  "BTC perp on venue X vs venue Y". Canonical instrument identity is
  foundational, not a nice-to-have.
- **R3 — Completeness beats latency.** Decisions at seconds-to-minutes cadence;
  execution is limit-order working, not race-to-cancel. 20–55 µs internal hops
  are already ~1000× better than required. Gaps, staleness, and silent
  disconnects are the failure modes that lose money.
- **R4 — Capacity and crowding inputs.** Open interest, liquidations, volume —
  funding arb capacity is bounded by OI and the trade is crowded precisely when
  it looks best.
- **R5 — Point-in-time reference data.** Funding intervals change per symbol
  (Binance moves symbols between 8 h and 4 h), fees change, symbols list and
  delist. New listings are disproportionate alpha. Backtests over months need
  the specs as they were, not as they are.
- **R6 — DEX venues are first-class.** Hyperliquid, dYdX v4, GMX, Drift do not
  all speak "WebSocket with 200 streams per connection". Some are poll-only;
  funding accrues per block on some; provenance is a block height.
- **R7 — Research ergonomics.** Parquet that DuckDB/Polars can prune, daily QA
  reports, same-day data access.
- **R8 — A credible path to live execution** (private streams, orders,
  positions, funding payments received) without rearchitecting the event model.

### Minimum viable dataset for the strategy vs. current architecture

| Input | Needed for | Status today | Finding |
|---|---|---|---|
| Funding prediction + realized | core signal | captured, but schema too thin to interpret cross-venue | A4 |
| Funding **interval** per symbol | annualization, comparability | absent everywhere | A4 |
| Mark / index price | basis, margin sim | captured | — |
| Top-of-book (bookTicker) | entry/exit spread cost | captured | — |
| Open interest | capacity, crowding | absent; not capturable by WS-only design | A6 |
| Liquidations | toxicity, funding spikes | absent | A6 |
| L2 depth | slippage model (traded names only) | captured for everything (cost own-goal) | A14 |
| Instrument metadata over time | backtest correctness, listings | fetched, never stored | A11 |
| Fee schedules | net carry P&L | absent | A11 |
| REST historical funding/klines/OI | backfill, reconciliation, cold-start research | no REST capture path at all | A5 |
| Canonical cross-venue symbology | every query | absent | A3 |
| Spot top-of-book (same venue) | spot-perp carry variant | supported by `InstrumentKind::Spot`, unplanned | — |
| Private fills / funding payments | live P&L truth | absent, unreserved in event model | A13 |

---

## 3. Critical findings

### A1 — The roadmap optimizes latency; the strategy needs completeness

`architecture.md` §7 budgets hops in microseconds and §4/`README` promise an SHM
Phase 2 at sub-10 µs end-to-end. For mid-freq funding arb this is engineering
spend with zero P&L attached: the signal changes on funding-interval timescales
and execution is not latency-competitive. Meanwhile the documents contain no
backfill, no reconciliation, no symbology, no OI, no monitoring — each of which
has direct P&L or research-validity impact.

**Fix.** Declare UDS the terminal transport until a measured need appears
(it will not at this frequency). Delete the SHM phase and the hand-rolled
zero-copy `EventRef` from the roadmap (`wire` Phase 2); re-point that effort at
A3–A7. Keep the `EventSink` boundary — it makes this reversible for free, which
is exactly why the optimization can be deferred indefinitely.

### A2 — The recorder sits behind the bus, putting the bus in the durability path

The single-event trace (§7) is `venue → bus → recorder`. Every bus restart,
crash, slow consumer, or routing bug becomes **permanent data loss**, and L2
depth and trades are precisely the streams that cannot be re-fetched afterward.
This is also the root cause of the backpressure contradiction the previous
audit flagged (DOC3): "lossless to the recorder" and "never block the venue"
cannot both hold when durability lives downstream of distribution.

**Fix.** Move the WAL writer into the venue process as a library (`recorder`
already runs it on a dedicated thread; link it in-process and write before — or
in parallel with — publishing to the bus). The bus then serves *live consumers
only* and may be lossy-with-gap-counters for everyone, which simplifies it
enormously. The recorder process becomes converter + QA + archiver of WAL files
it did not write. Bonus: multi-host capture later means shipping WAL files,
not re-plumbing transports. The improvement plan's WAL work (framing, CRC,
recovery) is unaffected — only the placement changes. Decide this **before**
the `transport`/`event-bus` crates are built, because it changes both.

### A3 — No cross-venue instrument identity

`InstrumentId` is a venue symbol string (`Arc<str>`): `"BTCUSDT"` on Binance,
`"BTC-USDT-SWAP"` on OKX, `"BTC"` on Hyperliquid. Every layer keys on it —
`TopicFilter`, Parquet partitioning, replay filters. A funding-arb strategy's
first line of code is "give me BTC perp on every venue", and the framework
cannot express it; every consumer would re-invent symbol mapping, and the
README's own example filter ("all perps, FundingRate only") is not expressible
in the `TopicFilter` struct as specced (A17).

Identity is more than the symbol: OKX sizes in contracts (`ctVal` multiplier),
quote/settlement currency differs (USDT vs USDC vs coin-margined), inverse vs
linear changes every downstream formula. `Instrument { base, quote }` strings
cannot carry this.

**Fix.** Introduce a symbology layer in `venue-core` now, while the schema
is being re-cut anyway (improvement plan): a canonical `Asset` (e.g. `BTC`),
`CanonicalInstrumentId` (asset + kind + settle), and a per-venue mapping
`VenueSymbol ↔ CanonicalInstrumentId` published by each adapter from
`fetch_instruments`. Extend `Instrument` with `contract_multiplier`,
`settle_currency`, `linear/inverse`, `tick_size`, `lot_size`, `min_notional`,
`funding_interval`. Store events keyed by venue symbol (raw truth) but make the
mapping a first-class, versioned dataset so joins are one lookup.

### A4 — Funding payload semantics are too thin to be comparable across venues

`FundingRatePrediction { rate, next_funding_time }` and
`FundingRateRealized { rate, funding_time }` lose the information that makes
rates comparable:

- **Interval.** A 0.01% rate means 10.95% annualized at 8 h and 87.6% at 1 h.
  Binance mixes 8 h and 4 h per symbol and changes them; Bybit shortens
  intervals on volatile symbols; Hyperliquid is hourly; dYdX v4 settles hourly
  from per-block premium sampling. Without the interval stamped on the event
  (or in point-in-time metadata, A11), recorded predictions are ambiguous —
  and unlike the rate itself, the interval-at-the-time is hard to
  reconstruct retroactively. This is the same class of retroactively-unfixable
  capture defect as D1/D2.
- **Premium/interest decomposition and clamps.** The "predicted" rate on most
  CEXes is a running average that converges toward settlement; the premium
  index and the venue clamp (cap/floor) are what tell you where it can settle.
  Extreme-funding situations — the trade's bread and butter — are exactly where
  clamps bind.

**Fix.** While the improvement plan is already re-cutting schemas: add
`interval: Nanos` (or `prev_funding_time`) to both funding payloads, and
optional `premium_index`, `clamp_min/clamp_max`. Document per-venue semantics
in the adapter contract ("rate is venue-raw per-interval; interval must always
be present; normalization is the consumer's job").

### A5 — No REST/backfill/reconciliation path exists anywhere in the design

The architecture is WS-capture-only. But for funding arb, the ground truth —
realized funding history, premium-index klines, price klines — is served by
every CEX's REST API (Binance `/fapi/v1/fundingRate`, Bybit, OKX equivalents;
Hyperliquid `fundingHistory`). This has three consequences the design ignores:

1. **Outage recovery.** After any capture gap, funding/mark/kline history is
   fully repairable via REST; only L2/trades are not. The architecture treats
   all streams as equally ephemeral, which both overstates the uptime burden
   for funding data and understates it for depth.
2. **Validation.** WS-captured funding should be reconciled daily against REST
   history; disagreement (or a missed settlement) is a capture bug surfaced for
   free. Without it you will not know your dataset has holes until a backtest
   lies to you.
3. **Cold-start research.** Months of funding history are available *today*
   via REST for every target venue. Strategy research could begin before the
   bus exists. Nothing in the roadmap exploits this.

**Fix.** Add a `backfill` crate: REST pollers writing the *same* Parquet
schemas (bypassing WAL — provenance-tag rows `source=ws|rest`), plus a daily
reconciler that diffs WS funding vs REST and patches/flags. Note OI makes this
mandatory anyway (A6): Binance OI is REST-only. This is the single
highest-leverage component absent from the plan.

---

## 4. High-severity findings

### A6 — Missing data types: open interest, liquidations; no venue-wide subscriptions

`DataType` has six variants; none is `OpenInterest` or `Liquidation`. OI is a
core funding-arb input (capacity, crowding) and on Binance it is **not on
WebSocket at all** — it must be polled (`/fapi/v1/openInterest`), and the
historical endpoint only goes back ~30 days, so live capture is the only way
to build the series. Liquidation streams (`!forceOrder@arr`) mark funding-spike
regimes. Both are cheap to add and impossible to backfill later.

Separately, `Subscription { instrument, data_type }` forces per-instrument
streams, but the efficient capture for "funding for *every* perp" is the
venue-wide stream — Binance `!markPrice@arr@1s` is one stream instead of 300+
(and immune to listing lag). The subscription model cannot express it.

**Fix.** Add `OpenInterest` and `Liquidation` payloads/datatypes; add a
`Scope::{Instruments(Vec<_>), All}` to `Subscription`; let adapters map
`All` to venue-wide streams where available and to symbol fan-out where not.
OI capture lands naturally on the A5 poller infrastructure.

### A7 — No control-plane events: gaps, reconnects, and staleness are invisible to consumers

`Payload` has market data and a (currently dead) `Error`. There is no
`ConnectionUp/Down`, no `GapDetected`, no `SnapshotStart/End`, no
subscription ack. Consequences:

- A strategy cannot distinguish "funding rate unchanged" from "WS died 40
  minutes ago"; acting on a stale mark is the classic way this strategy blows
  up. The WsPool reconnects (good) but reconnection is *silent* to consumers.
- Replay's core promise — "indistinguishable from live" (§6) — is currently
  *false in the dangerous direction*: recorded streams contain no gap/reconnect
  markers, so backtests experience a fantasy continuity that live will violate.
- The bus cannot implement honest lossy backpressure without a `Gap` event to
  inject where it dropped.

**Fix.** Add `Payload::Control(ControlPayload)` — `ConnUp/ConnDown {conn_id,
streams}`, `Gap {reason, dropped}`, `SnapshotBegin/End`, `SubAck` — emitted by
adapters and the bus, recorded in the WAL like everything else, and replayed.
Cheap now; a schema migration plus a re-record later.

### A8 — The "venue = WsPool" assumption breaks on the DEX half of the target

§3 hardcodes the pattern (step 2 of "adding a venue": "implement WS message
types and WsPool") and all scaling math is streams-per-connection. On the
stated targets: Hyperliquid has WS but funding/mark come from validator-set
oracles hourly; dYdX v4 is an indexer WS with per-block premium sampling; GMX
has no WS at all (RPC/subgraph polling); Drift is Solana account subscriptions.
The `VenueAdapter` trait itself is transport-agnostic (good — nothing in it
says WebSocket), but the architecture provides no blessed polling-adapter
pattern, no rate-limit budgeting, and no on-chain provenance: `venue_ts` on a
DEX is a block timestamp (second granularity, reorg-mutable), and there is
nowhere to put block height/slot.

**Fix.** Document a poller-adapter pattern (same `EventSink`, same control
events, explicit poll intervals and rate budgets — shares infrastructure with
A5/A6). Add optional provenance to `Event` (e.g. `source_seq: Option<u64>`
carrying block height for on-chain venues) and a documented convention for
continuous/per-block funding accrual (sampled accrual events vs discrete
settlements). Treat one DEX adapter (Hyperliquid is the easiest: real WS,
clean API) as the Phase-2 proof that the abstraction holds — before the
abstraction calcifies around Binance.

### A9 — Replay merges on `venue_ts`: wrong clock for a cross-venue strategy

§6 k-way-merges multiple venues on `venue_ts`. Venue clocks are not your
clock and are not each other's: cross-venue skew can exceed the latency
differences that matter, `venue_ts` is `Option` (merge key can be absent), and
a DEX block timestamp has second granularity. A backtest joining Binance and
OKX funding on venue clocks can see arbitrage that never existed, or miss real
sequencing.

**Fix.** Merge on `local_ts` (capture time) with tie-break
`(venue, in-file position)` — consistent with the improvement plan's D3
in-file contract. That timeline is *exactly* what a live strategy on this host
would have seen, which is the actual meaning of "indistinguishable from live";
keep `venue_ts` as an analysis column. Corollary (fold into A12): `local_ts`
must come from a disciplined clock — run chrony, and monitor
`local_ts − venue_ts` per venue as both a venue-clock and own-latency health
metric.

### A10 — The custom bus: SPOF by design, no subscription protocol, and an unexamined build-vs-buy

The star topology makes one process the intersection of every data path —
acceptable only because A2 removes it from the durability path; the documents
should say so explicitly. Beyond that, the `event-bus` spec is missing its
hardest parts: **how a consumer communicates `TopicFilter` to the bus**
(no handshake/wire protocol is specified anywhere), what happens to venue
publishers and consumers across a bus restart (reconnect? buffer? gap events?),
and slow-consumer policy mechanics (eviction vs drop-with-`Gap`, per-consumer
queue depths).

Build-vs-buy is never discussed. For mid-freq, `iceoryx2` (Rust SHM pub/sub),
Aeron, or even NATS would each clear the latency bar with years of hardening
on exactly the fan-out/backpressure/reconnect problems above — and with
edge-WAL (A2), the bus carries only live fan-out, making "buy" even more
defensible. Hand-rolling is justifiable as a learning goal, but then scope it
honestly: lossy-only, gap-counted, restart-tolerant.

**Fix.** Either adopt an existing transport behind `EventSink`, or write the
missing one page of spec: subscription handshake, restart semantics,
per-consumer drop policy with `Gap` injection (A7), and metrics. Do not build
lossless consumer backpressure at all — with A2, nothing on the bus needs it.

### A11 — Reference data has no point-in-time story; the universe is static

`fetch_instruments` returns specs that are stored nowhere. Funding intervals
change per symbol, tick/lot sizes change, leverage tiers change, fee schedules
change, and symbols list/delist weekly — new listings being some of the best
funding opportunities, and delistings being how backtests acquire
survivorship bias. The adapter trait also has no path from "instrument
appeared" to "subscription updated": `subscribe()` is called once with a
static list.

**Fix.** (a) Persist instrument metadata as a slowly-changing dimension:
daily snapshot + on-change rows (`valid_from`/`valid_to`) in Parquet alongside
market data; include fee schedules. (b) Add a universe manager loop to
`venue-process`: poll `fetch_instruments` periodically, diff, emit
`InstrumentAdded/Delisted` control events, auto-subscribe per config policy
(e.g. "all perps"). Venue-wide streams (A6) reduce how much this matters for
funding specifically — listings are captured the second they print — but book
data still needs the subscription update.

### A12 — Observability and ops are absent from the architecture

Nothing in either document covers metrics, alerting, supervision, config, or
deployment — for a system whose value is "we captured everything, 24/7, on
10+ venues", this is the majority of the engineering. Concretely missing:
per-stream staleness watermarks ("seconds since last event, per venue × type ×
instrument-tier"), queue depths and drop counters at every hop, WS reconnect
rates, funding-coverage checks (every instrument must have a
`FundingRateRealized` within each expected window — alert otherwise), WAL
fsync lag, disk headroom, and a daily QA report (coverage %, gap count, dup
count per venue/type) that research treats as a data-quality gate (R7).

**Fix.** Prometheus exporter in every process (venue, bus, recorder);
staleness and funding-coverage alerts; the QA report as part of the
WAL→Parquet conversion job (it already reads every event). Cheap, boring,
and the difference between a dataset and a liability. The same per-stream
watermarks feed strategy-side staleness checks via control events (A7).

---

## 5. Medium and low findings

### A13 (medium) — Live trading is promised but the event model reserves no room for it

README: "built for strategy development and live trading." Funding arb live
means private streams (orders, fills, positions, margin, **funding payments
received** — the realized edge), simultaneous execution on 2+ venues, and
inter-venue collateral moves. No OMS needed now, but two decisions are cheap
today and expensive later: (a) reserve the namespace — `Payload::Account(..)`
/ `Payload::Execution(..)` variants and an `AccountStream` subscription kind;
(b) decide that private data does **not** transit the shared bus (per-strategy
private channel from venue process), because `TopicFilter` has no
authorization concept and retrofitting tenancy into a broadcast bus is
miserable. Capturing your own fills/funding-payments through the same
WAL→Parquet pipeline is also how you validate the data products (predicted vs
charged funding).

### A14 (medium) — Storage: untiered capture and a query-hostile layout

§8 sizes 50–200 GB/day/venue, i.e. L2 for everything — at 10 venues that is
double-digit TB/week serving a strategy that needs depth only for names it
trades. Tier the universe: top-of-book + funding + mark + OI for *all* perps
(cheap, and it is the actual signal set), full depth for an active subset.
Layout: adopt Hive partitioning (`venue=binance/date=2026-06-09/type=...`) so
DuckDB/Polars prune without glob games; zstd; row groups sorted by
`(instrument, ts)` with column stats for pruning (consistent with D3's
"replay sorts" — sorting at *conversion* time is allowed and helps research,
just don't promise it on write). Define retention: WAL → object storage after
conversion + QA pass (WAL is the source of truth; Parquet is derived and
re-derivable), hot Parquet local for N days. Convert hourly, not at day end —
same-day data matters for live monitoring and for R7.

### A15 (medium) — No schema-evolution policy for data that outlives every refactor

The improvement plan fixes framing mechanics (CRC, self-describing frames);
what is still missing at the architecture level is the *policy*: WAL and
Parquet files will outlive `venue-core`'s structs by years. State the rules in
`architecture.md`: every frame/file carries a format version; payload changes
are additive-only (new optional fields); breaking changes require a new
version with a documented migration; Parquet schemas carry
`schema_version` metadata; replay reads N and N−1. One paragraph now prevents
the "which binary wrote this file" archaeology in 2027.

### A16 (low) — Latency budget table: wrong KPI, fictional precision

µs-level budgets for unbuilt components anchor reviews on the wrong axis.
Replace with the SLOs that match R1/R3: funding-event coverage 100%
(reconciled vs REST), per-stream staleness P99, gap rate, daily QA pass rate.
Keep a one-line latency note ("UDS, tens of µs, vastly exceeds requirement").

### A17 (low) — `TopicFilter` cannot express the README's own examples

"All perps, FundingRate only" requires filtering by instrument *kind*, but the
filter has only concrete `InstrumentId` sets. With A3's symbology this becomes
either a `kind`/asset predicate or resolve-at-subscribe (bus expands "all
perps" against the live universe — interacts with A11's listing events).
Specify which; the current spec silently can't do the thing the diagram shows.

### A18 (low) — Single-host assumption is implicit

UDS and SHM are single-host transports; fine, but undeclared. The realistic
growth path (capture node in Tokyo near venue endpoints, research box
elsewhere) is already served by A2: ship WAL/Parquet files, don't stretch the
bus. Say this in §8 so nobody builds a TCP bus bridge nobody needs.

---

## 6. What the architecture gets right

Credit where due, because these should *not* change:

- **`EventSink` as the universal boundary** — it is what makes A1's deferral
  free, A10's build-vs-buy swappable, and replay-as-live possible at all.
- **Process-per-venue isolation** — one venue's parser panic cannot take down
  another's capture; also the right unit for key isolation later (A13).
- **WAL-first durability with one wire format** for IPC and disk — no
  double-serialization, and the WAL remains the re-derivable source of truth.
- **Replay through the same sink interface** — the right backtest/live parity
  design; it just needs control events (A7) and the right merge clock (A9) to
  make the promise true.
- **Dual timestamps and Decimal prices** in `venue-core` — the foundations a
  data vendor gets wrong.
- **WsPool sharding with reconnect/backoff already implemented** — the Binance
  adapter is ahead of its own documentation here.

---

## 7. Recommended roadmap (re-sequenced)

Phase numbering continues from the improvement plan; items reference findings.

| Phase | Contents | Rationale |
|---|---|---|
| **1 (in flight)** | improvement_plan.md as accepted (D1–D6, bugs, framing/CRC) **plus three schema riders that must land in the same re-cut**: funding `interval`+clamps (A4), `OpenInterest`/`Liquidation` payloads (A6), `Payload::Control` (A7), and symbology core types (A3) | All four are retroactively-unfixable capture semantics; re-cutting schemas twice means re-recording twice |
| **2** | Edge-WAL decision (A2) → then `transport` (UDS) + minimal **lossy** bus with subscription handshake and `Gap` injection, or `iceoryx2` adoption (A10) | Placement decision must precede transport/bus implementation |
| **3** | `backfill` crate + daily reconciliation (A5), OI/liquidation pollers (A6), instrument metadata SCD + universe manager (A11) | The completeness layer; also unblocks cold-start research from REST history immediately |
| **4** | Observability baseline + daily QA report (A12), storage tiering + Hive layout + hourly conversion (A14) | Makes the capture trustworthy and queryable |
| **5** | `replay` with `local_ts` merge, control-event replay, QA-gated inputs (A9, A7) | Backtests only after data provably complete |
| **6** | First DEX adapter (Hyperliquid) as abstraction proof (A8) | Before the Binance idiom calcifies |
| **Deferred indefinitely** | SHM transport, zero-copy `EventRef` (A1); OMS/execution beyond namespace reservation (A13) | No P&L at this frequency until proven otherwise |

## 8. Finding index

| ID | Severity | One-line summary |
|---|---|---|
| A1 | Critical | Roadmap spends on µs latency; strategy needs completeness — freeze at UDS |
| A2 | Critical | Recorder behind the bus puts the bus in the durability path; WAL at the edge |
| A3 | Critical | No canonical cross-venue instrument identity; the strategy's join key is missing |
| A4 | Critical | Funding payloads lack interval/premium/clamp — rates not comparable, not retro-fixable |
| A5 | Critical | No REST backfill/reconciliation path; funding ground truth never validated |
| A6 | High | OI and liquidations missing; no venue-wide subscriptions; OI is REST-only anyway |
| A7 | High | No control events — gaps/reconnects invisible to consumers, recorder, and replay |
| A8 | High | WS-pool venue pattern breaks on DEX targets; no provenance for on-chain data |
| A9 | High | Replay merges venues on `venue_ts`; cross-venue backtests need `local_ts` |
| A10 | High | Bus lacks subscription protocol and restart semantics; build-vs-buy unexamined |
| A11 | High | No point-in-time reference data; static universe misses listings/delistings |
| A12 | High | No metrics, staleness alerts, funding-coverage checks, or QA reports |
| A13 | Medium | Live-trading promise with no private-data namespace or bus tenancy decision |
| A14 | Medium | Untiered L2-everything capture; non-Hive layout; day-end-only conversion |
| A15 | Medium | No schema-evolution policy for files that outlive the code |
| A16 | Low | Latency table is the wrong KPI; replace with coverage/staleness SLOs |
| A17 | Low | `TopicFilter` cannot express "all perps" shown in README |
| A18 | Low | Single-host transport assumption undeclared |
