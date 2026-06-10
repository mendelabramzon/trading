# Codebase & Documentation Audit — 2026-06-09

*Auditor: Fable (Claude Code). Scope: full read of all 5 crates (1,827 LOC including
the uncommitted `encoding_probe` module), all 4 examples, `README.md`,
`docs/architecture.md`, `docs/arch_assesment.md`, `docs/report_phase1.md`, and
`docs/improvement_plan.md`. Method: every load-bearing prior claim was re-verified
against source and toolchain — `cargo clippy --all-targets`, `cargo test`, grep
audits for dead code and `dyn` usage, a byte-level decode of the real recorded WAL,
and inspection of the vendored `parquet-55.2.0` sources.*

*`improvement_plan.md` is treated as the accepted forward plan. Known findings
(D1–D7, Bugs 1–4, Code Quality, Architectural Debt) are **not** re-litigated here;
this audit (a) verifies them where cheap, (b) reviews the two public-facing docs,
(c) reports findings absent from both the assessment and the plan, and (d) critiques
the plan itself. Finding namespaces: `DOC*` = documentation, `N*` = new code
findings, `P*` = plan critique — chosen not to collide with the assessment's D/Bug
numbering.*

## STATUS — 2026-06-10 dispositions (post Phase-0 implementation)

- **Done**: P1 (BadVersion abort fence + >1% corrupt-bytes conversion gate),
  P2/N3 (null+warn, nullable columns), P3 (zstd + `Timestamp(ns, UTC)`),
  P4/N4 (parser fixture suite in `venue-binance`), N1/P5a (raw exchangeInfo
  dump via `fetch_instruments_raw` — smoke writes `data/meta/`; venue-process
  owns it in Phase 1), N2 (WAL I/O error → process exit), N6 (channel 100K ≈ 2 s),
  N9 (derives), N12 (error frames surfaced by the reply watcher; pong failure →
  reconnect; tokio features trimmed; workspace deps consolidated; read_wal /
  convert_wal take path arguments), DOC1–DOC6 (README/architecture.md rewritten
  2026-06-10).
- **Decided, documented**: N11 — `FundingRateRealized` stays in the schema with
  no live producer; the Phase-2 REST backfill (`/fapi/v1/fundingRate`) is its
  producer. N5 — replay ordering axis contract recorded in `architecture.md` §6.
- **Still open → Phase 1**: P5b/N8 (startup retry policy), P5c (config-driven
  fatality policy), P5d (heartbeat), P6 (conversion automation), N7 (long-horizon
  depth soak — first live pu-chain acceptance passed 2026-06-10), N10 (alloc
  hygiene — deliberately deferred, profile first).

---

## 1. Verification of prior claims

| Claim (source) | Result |
|---|---|
| Clippy: clean lib, 2 warnings with `--all-targets` (assessment) | **Confirmed** — exactly the `i as u64` cast and the unused `EventSink` import |
| 7 tests (assessment) | **Confirmed**, now 8 with the uncommitted `encoding_probe` test; all pass |
| `Venue` struct, `ErrorPayload`/`Payload::Error` dead (assessment) | **Confirmed** by grep — `Payload::Error` appears only in its definition and one converter match arm |
| No `dyn EventSink` / `dyn VenueAdapter` (plan, step 6) | **Confirmed** — RPITIT migration is safe |
| Lossless backpressure via `SyncSender` (assessment) | **Confirmed** — `send` blocks on full, errors only on disconnect |
| Old recordings obsolete (plan, decision 4) | **Confirmed and strengthened** — the sole real WAL (`data/wal/binance/2026-06-05.wal`, 4.5 MB) decodes cleanly to 45,041 events, all stamped `sequence: None`, i.e. captured by a pre-sequence binary |
| rmp-serde positional/by-name behavior (plan, corrections) | Pinned by the `encoding_probe` test, which passes (currently `eprintln!`-only; plan step 2 converts it to assertions) |

The assessment is accurate everywhere I could check, and the plan's four confirmed
decisions are sound. Everything below is additive.

---

## 2. Documentation review

### README.md

**DOC1 (medium) — diagrams present planned components in present tense.** The crate
map's Status column is honest (`built`/`planned`), but every mermaid diagram —
Event Bus routing, strategy fan-out, live-vs-replay equivalence — renders
unbuilt components identically to built ones, and the prose around them is
declarative ("The framework decides whether events come from live venues or from
recorded Parquet files"). A newcomer cannot tell from the diagrams that the bus,
replay, strategies, Bybit, and OKX do not exist. Mark planned nodes in the diagrams
or add one prominent status note above them.

**DOC2 (medium) — the recording pipeline diagram claims automatic conversion that
does not exist anywhere.** README line 80 (`REC-->REC: background: WAL → Parquet`)
and `architecture.md` §5 ("background, periodic") describe a background converter.
Reality: `convert_wal` is a manually-run example with a hardcoded path. More
importantly, **the improvement plan does not add automation either** — see P6. After
the plan lands, the system records 24/7 and converts never.

### docs/architecture.md

**DOC3 (high) — §4 and §8 contradict each other on backpressure, and the contradiction
hides a real design decision.** §4: "Recorder uses lossless (bus pauses if recorder
falls behind)." §8: "Consumers are isolated — a slow or crashing consumer does not
affect others." With a single broker both cannot hold: a paused bus stalls every
consumer; per-consumer lossless queues mean unbounded memory instead. The plan
already implies the right resolution — steps 8/11 put `WalSink` **inside the venue
process**, so capture is lossless at the edge, *before* any bus hop, and the bus
can then serve live consumers with bounded lossy queues + gap detection. Make that
the documented topology: recording is not a bus consumer. This dissolves the
contradiction and removes the recorder from the bus's worst-case path.

**DOC4 (medium) — performance numbers are presented as measurements of a system that
doesn't exist, and one is wrong on current defaults.** The §7 latency budget
(20–55 µs end-to-end) and §8 throughput table describe UDS transport and a bus that
are not built; no benchmark exists in the repo. Label them as design targets.
Concretely wrong: "Parquet after compression ~5–40 GB/day" — the converter passes
`None` properties to `ArrowWriter`, and `parquet-55.2.0` defaults to
`Compression::UNCOMPRESSED` (verified in `properties.rs:36` of the vendored
source). Dictionary encoding applies, block compression does not. See P3. Also, the
throughput table mixes peak rate (50k events/s ⇒ ~4–10 MB/s ⇒ 350–860 GB/day
sustained) with a 50–200 GB/day WAL estimate — consistent only under an unstated
~20% duty cycle; label which numbers are peak and which are average.

**DOC5 (low) — the §2 dependency graph is wrong even as a planned-state diagram.**
It shows `venue-adapter → wire` (no such edge exists, and none should — the adapter
layer is transport-agnostic by design) and `recorder` hanging off `transport`
(recorder depends on `wire` directly). `replay → recorder` is speculative. Redraw
from the actual `Cargo.toml`s plus genuinely planned edges.

**DOC6 (low) — accumulated drift beyond the assessment's doc-drift row.** §5 schema
table says `FundingRate` where the code variant is `FundingRatePrediction`; the
`BookUpdate` row shows nested `bids[{price,qty}]` while actual output is exploded
per-level rows (plan step 11 already notes this); §3's `WsConn { writer,
read_handle, stream_count }` sketch doesn't match the actual `{ cancel, handle }`;
the workspace layout lists `transport/`, `event-bus/`, `replay/`, `venue-process/`
as if present; the `TopicFilter` sketch (`HashSet<DataType>`) cannot compile against
the real `DataType`, which derives nothing (see N9). The stale `wire` signatures and
the "already sorted by timestamp within each file" claim are known (assessment +
plan step 11).

### Other docs (one line each)

- `report_phase1.md`: "The recorder can now be trusted for unattended 24/7 data
  collection" is contradicted by the assessment's own later findings (no rotation,
  D1–D7) — soften or annotate; "proven with real data" holds only for
  bookTicker/aggTrade (see N7).
- `arch_assesment.md`: filename typo ("assesment") — two docs link to it; rename
  only if you accept the link churn.

---

## 3. New code findings (absent from assessment and plan)

**N1 (high — same retroactively-unfixable class as D1/D7) — no instrument reference
data is captured, anywhere.** `SymbolInfo` (`venue-binance/src/lib.rs:29-35`) parses
only `symbol/contractType/baseAsset/quoteAsset/status`; tick size, step size,
precisions, notional filters, and `deliveryDate` are discarded at parse time, and
`fetch_instruments` output is never persisted (the example prints 10 rows). Binance
changes filters over time and `exchangeInfo` is current-state only — the tick/lot
schedule in effect on a recorded day is **not recoverable later**, and backtests
that quantize prices/fills need it. The fix costs almost nothing: dump the raw
`exchangeInfo` JSON to `data/meta/binance/<date>-exchangeInfo.json` at
venue-process startup and daily. No schema commitment needed — parse later.
→ Add to plan step 11.

**N2 (high) — a dead WAL thread leaves a healthy-looking process that records
nothing.** `recorder/src/lib.rs:74,81` `expect()` in the writer thread: the first
filesystem error (permissions, disk full at open) panics the thread; from then on
every `send` logs a per-event warn (with the misleading "channel full or closed"
text at `lib.rs:35`) while the adapter keeps consuming WS data into the void.
Persistent `write_all`/fsync failures (disk full mid-run) similarly warn-spam
forever. For the *unattended recorder* goal this is the worst failure shape: silent
partial capture, process alive, no exit code for a supervisor to act on. Policy
needed in steps 8/11: WAL-thread panic or persistent write failure ⇒ process exit
(let the supervisor restart) — or at minimum a sticky health flag surfaced by the
heartbeat (P5d).

**N3 (medium) — the converter zeroes missing timestamps and silently skips
instrument-less events: D5's siblings, untouched by the plan.**
`parquet_converter.rs:47-48` (`venue_ts.unwrap_or(0)`, `local_ts.unwrap_or(0)`) and
`:45` (`None => continue`, unlogged, uncounted). A zero `venue_ts` sorts to the
front of any merge — the same silent-plausible-value corruption D5 fixes for
decimals. Latent today (the adapter always sets `Some`), but the plan rewrites
exactly these lines in steps 3–5 — extend the D5 treatment (null + `warn!`) to them
in the same commit. The deeper smell is `Event` itself (`events.rs:8-12`): four of
five fields are `Option`, forcing every consumer to invent defaults for absences
that cannot currently occur. Decide which are genuinely optional and tighten or
document the contract.

**N4 (medium) — zero tests on the venue parsing path, which is the layer the whole
plan exists to protect.** `handle_message` and the serde message structs have no
tests at all; venue-binance has no test target. A `#[serde(rename)]` typo, an
inverted `m` flag, or a wrong ms→ns multiplier ships straight into the WAL —
everything downstream is garbage-in-faithfully-recorded, and D1/D7-class data is
unfixable after the fact. Plan step 3 extends *wire* fixtures only, which test the
roundtrip of already-constructed Events, not their extraction from Binance JSON.
Add JSON-fixture tests (captured live frames) asserting full `Event` construction:
aggressor side, timestamps, lowercasing, and the new `U/u/pu/T/id` fields. → P4.

**N5 (medium) — the replay ordering axis is unspecified, and `venue_ts` ordering is
not "indistinguishable from live" for multi-venue use.** §6 and the D3 decision sort
on `venue_ts` (tie-break `local_ts`, file position). Correct for single-venue book
reconstruction. Wrong axis for cross-venue execution realism: live, a strategy sees
events in **local arrival order**; replaying two venues merged by their own
exchange-stamped clocks can hand the strategy information in an order it could
never have observed, silently flattering latency-sensitive strategies. Replay
should expose the ordering key (`venue_ts` for reconstruction correctness,
`local_ts` for arrival realism) per run. Decide and document before the replay
crate exists — it shapes the k-way merge interface.

**N6 (medium) — capture-channel headroom is ~0.2 s at the architecture's own peak
rate, and fsync runs inline with the drain loop.** `sync_channel(10_000)`
(`recorder/src/lib.rs:20`) against the claimed ~50k events/s peak is 200 ms of
buffer; the smoke bridge (`mpsc(1000)`) is 20 ms. `fsync_all` blocks the recv loop
while the channel fills, and until step-8 rotation lands its cost scales with every
file ever opened. When the chain stalls, backpressure escalates to the venue:
Binance disconnects slow consumers, converting a local disk hiccup into a forced
reconnect plus a broken `pu` chain (which, post-step-9, also forces a re-snapshot).
Size the channel in time units (1–2 s of peak), make it config (step 11), and emit
a channel-depth gauge.

**N7 (medium) — the depth pipeline has never run end-to-end.** `smoke.rs` (the only
runnable pipeline) subscribes BookTicker + Trade only; the one real WAL contains no
depth streams and was captured by a pre-sequence binary (§1). `BookSnapshot` has no
producer (known: Bug 1) and `BookUpdate` has never been exercised against real
data. The plan's pu-chain/splice acceptance check is therefore the **first-ever**
real exercise of the depth path — treat it as a mandatory gate, not an optional
verification item.

**N8 (low) — no startup retry; partial-subscribe state on mid-loop failure.**
`ws_pool.rs:63-89`: if chunk *i* of *n* fails `connect_async` or the SUBSCRIBE
send, `subscribe()` returns `Err` with chunks `0..i` live in the pool; the caller
cannot tell which streams are active. Reconnect-with-backoff exists only *after* a
connection is established. The step-11 entrypoint must define startup policy
(retry-with-backoff or fail-fast for the supervisor) and either roll back or report
partial subscriptions.

**N9 (low) — type-level prep for the bus is missing.** `DataType`
(`venue-adapter/src/lib.rs:4-11`) and `Subscription` derive nothing — not even
`Debug`/`Clone`. The architecture's own `TopicFilter` needs `DataType:
Hash + Eq`. Adding `Debug/Clone/Copy/PartialEq/Eq/Hash` now is free.

**N10 (low) — hot-path allocation hygiene vs the low-latency positioning.** Per
event: `to_lowercase()` String + a fresh `Arc<str>` (`lib.rs:216` and three more
arms), a `Vec<Trade>` per aggTrade (always length 1), a full deep `Event` clone
into the WAL channel, and `rmp_serde::to_vec` allocating a fresh `Vec` inside
`wire::encode` (`wire/src/lib.rs:24`) despite the reusable `buf` parameter
(serialize into `buf` directly: reserve 4 bytes, encode, backfill the length).
Irrelevant at today's volumes; list as perf debt with a symbol-interning map as the
first move before the transport phase.

**N11 (low) — `FundingRateRealized` is a never-constructed variant**, exactly the
class `Payload::Error` was (`payloads.rs:30`; grep: definition + converter arm
only). The `@markPrice` stream yields predictions only, so
`funding_rate_realized.parquet` can never contain live data. Unlike D1/D7 this *is*
retroactively recoverable (`/fapi/v1/fundingRate` keeps history), so: schedule a
small REST poller, or drop the variant + writer until one exists — but decide,
don't leave a third dead limb after step 1 removes the other two.

**N12 (low) — one-liners.**
- Binance `{"error":...}` frames and SUBSCRIBE acks fail the tagged-enum parse and
  vanish at `tracing::debug` (`lib.rs:204`) — when step 7 adds ack-watching, surface
  error objects too.
- Pong-send failures are discarded (`ws_pool.rs:163`, `let _ =`); a failed pong
  means a dead socket — break to reconnect instead of waiting for the read error.
- Subscribing `FundingRate` alone still emits Mark + Index + Funding events (no
  emission-side filtering) — fine for capture, worth one documented line.
- The `pu` chain crosses midnight WAL file boundaries (file key is event-date);
  replay must carry book state across day files — step 9's periodic snapshots
  bound the cost, note it in the D3/replay contract.
- `tokio` `features = ["full"]` in venue-binance and the unused `prettyprint`
  feature on arrow in recorder — trim while touching the manifests.
- Only `tracing*` lives in `[workspace.dependencies]`; consolidate shared deps
  while step 11 adds new crates.
- `read_wal`/`convert_wal` hardcode `data/wal/binance/2026-06-05.wal` — take args
  alongside the step-11 config work.

---

## 4. Critique of improvement_plan.md

The plan is unusually rigorous — decisions verified against vendored sources, a
probe test pinning serializer behavior, the WalSink/Drop join hazard caught before
implementation, and the step-9 snapshot-lifecycle rewrite is exactly right. Four of
four confirmed decisions check out against the code. The items below are the gaps.

**P1 (high — fix the design before implementing step 2): `BadVersion` must not feed
the resync path.** Step 2's reader resyncs on "any bad frame", and
`WireError::BadVersion` is enumerated as one of them. That recreates D6's diagnosed
silent-loss failure one version bump later: a reader on a file with frames of an
unsupported version skips *every* frame as "corruption", and `convert_wal` exits 0
having written a confident-looking empty/partial Parquet. Mixed-version files are
*expected* under this design (same-day append-reopen across a binary upgrade), so
the policy must be: **corruption-class** errors (`BadCrc`, mid-file `BadMagic`,
truncated tail, `FrameTooLarge`) ⇒ resync + count; **schema-class** (`BadVersion`)
⇒ dispatch to a versioned decoder when `version ∈ supported`, else **abort
loudly**. Add a conversion-level guard too: bad-frame ratio above a threshold
(~1%) fails the conversion rather than logging through it. The plan's own D6
analysis names this exact failure shape for the unversioned case — the version
byte only helps if the reader refuses to treat version mismatch as corruption.

**P2 (medium) — extend step 4 (D5) to N3**: timestamp `unwrap_or(0)` and the
silent `instrument: None` skip. Same file, same commit, same null+warn treatment.

**P3 (medium) — step 5 must set `WriterProperties` explicitly.** Default is
`UNCOMPRESSED` (verified, §DOC4); set ZSTD (or Snappy) — the architecture's storage
claims assume it. And since steps 3+5 rewrite every schema anyway, decide now
between `UInt64` nanos and Arrow `Timestamp(Nanosecond, "UTC")` for
`venue_ts`/`local_ts` — semantic typing is free at this point and a migration
later; if `UInt64` stays, document why.

**P4 (medium) — step 3 should include the `handle_message` JSON-fixture tests
(N4).** The plan tests the wire roundtrip of new fields but never their extraction
from Binance JSON — which is the actual D1/D7 risk surface, and the only part of
the pipeline with zero coverage.

**P5 (medium) — step 11 scope additions**: (a) persist raw `exchangeInfo` daily
(N1); (b) startup retry policy (N8); (c) WAL-failure fatality policy (N2); (d) a
once-a-minute heartbeat log — events/sec by data type and channel depth. The
heartbeat is the cheapest detector for every silent-death mode in this audit (N2,
dead connections pre-step-7, ack rejections) and should not wait for the Phase-2b
metrics item.

**P6 (medium) — conversion automation is missing from the plan entirely.** Step 8
rotation produces completed WAL files; nothing converts them (DOC2). Post-plan, the
"unattended" recorder still requires a human to run `convert_wal` per file per day.
Either a sidecar task in venue-process converting yesterday's rotated files, or an
external cron — pick one and write it into step 11.

**P7 (low) — small step amendments.** Step 7: the ack watcher should also surface
`{"error":...}` frames (N12). Step 9: make the REST weight bound explicit —
`depth?limit=1000` costs weight 20 against the 2,400/min futures budget, so 300
symbols ≈ 6,000 weight ⇒ ≥2.5 min of paced fetching; "sequential/bounded" is
right, quantify it. Step 6: RPITIT makes `EventSink` non-dyn-compatible — fine
(verified no `dyn` today), but the future bus's `DynEventSink` erasure wrapper
becomes mandatory, worth one line in the step.

**P8 (low) — verification additions.** Add a disk-full / kill-WAL-thread drill
(N2) and the parser-fixture suite (P4) to the acceptance list. Given N7, the
pu-chain/splice acceptance check is the single most valuable item in the plan —
do not soften it.

---

## 5. What's sound (recorded so it isn't re-litigated)

- The EventSink boundary, WAL→Parquet split, process-per-venue topology, and WsPool
  sharding are right, as both prior docs conclude. No `unsafe` anywhere; error
  enums all implement `Display + Error`; `Cargo.lock` is committed.
- The write path is genuinely lossless end-to-end: the real WAL decodes
  45,041/45,041 frames with zero errors.
- Aggressor-side mapping (`m=true ⇒ Sell`) is correct; `venue_ts = T` for
  aggTrade/bookTicker is correct and D7 completes the contract for depth.
- The plan's review deltas (resync at `p+1`, frozen MAGIC, the
  `(venue_ts, local_ts, position)` tie-break, snapshot-after-first-diff ordering)
  all survive scrutiny.
- `.gitignore` is appropriately paranoid for a trading repo (one note: bare
  directory patterns like `trades/`, `run/` match at any depth — a future
  `crates/trades/` would silently vanish from git).

## 6. Priority delta (merge into the plan, in order)

1. **P1** — BadVersion reader policy: design change to step 2, before any code.
2. **N1/P5a** — daily raw `exchangeInfo` persistence: the one remaining
   retroactively-unfixable hole after D1/D7 close.
3. **P4/N4** — parser fixture tests inside step 3's commit.
4. **P2/N3** — timestamp-zeroing + None-skip folded into step 4.
5. **P3** — compression + timestamp typing while every schema is already open.
6. **P6 + N2 + N8 + P5d** — step 11 becomes genuinely unattended: conversion
   automation, WAL-failure fatality, startup retry, heartbeat.
7. **N6** — channel sizing as config + depth gauge.
8. **DOC3/DOC4/DOC5 + N5** — architecture.md: adopt record-at-the-edge as the
   documented topology, label perf numbers as targets, fix the dependency graph,
   and specify the replay ordering-key contract next to D7's timestamp contract.
9. **N9** (derives), **N11** (funding-realized decision), **N10/N12** — recorded
   as debt; no immediate action.

## Bottom line

The two prior documents are unusually honest and verified accurate; the plan's
decisions all check out against source. The codebase's real risks are exactly where
the assessment put them — the data model — plus three things this audit adds:
**one design flaw in the plan itself** (P1: the self-healing reader will eat
whole files on the first version bump unless BadVersion is fenced off from the
resync path), **one more unfixable-class capture gap** (N1: no instrument
reference data, trivially closed by dumping raw `exchangeInfo` daily), and **an
operational blind spot** (N2/P5d: the unattended recorder can die silently and
look healthy — it needs a fatality policy and a heartbeat). The public docs
(README, architecture.md) oversell present capability in diagrams and performance
tables, contain one genuine design contradiction (DOC3, bus backpressure vs
consumer isolation — resolved for free by documenting the record-at-the-edge
topology the plan already builds), and one concretely false claim on current
defaults (DOC4: Parquet output is uncompressed). None of this changes the plan's
ordering; items 1–5 of the delta belong inside the existing steps they amend, and
item 6 belongs in step 11 before the system is called unattended.
