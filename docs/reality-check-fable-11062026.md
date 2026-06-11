# Reality Check — 2026-06-11 (fable)

*Full-source review of all 9 crates (~15k lines incl. tests), configs, deploy
units, and docs, asking three questions: what is overengineered, what is too
simplified, where is the architecture not optimal. Findings carry RC-ids in
house style. Claims are labeled **verified** (reproduced or test-pinned) or
**reasoned** (from code reading). Baseline at review time: 115 workspace
tests pass, clippy clean.*

## Verdict

The codebase is unusually disciplined for its age: the wire format is pinned
by golden bytes and an encoding probe, QA distinguishes explained from
unexplained data loss via the recorded control timeline, every batch job is
idempotent with atomic publishes, and parsers are tested against dated live
captures. The real risks are not inside components but in the **seams
between them**: a verified shutdown deadlock between `venue-process` and the
WAL writer's drop protocol, venue knowledge split inconsistently between
config and adapter, a symbology layer that promises point-in-time and
delivers latest-snapshot, and converter/QA passes that never check each
other. The system errs *over-built in vocabulary* (traits and enum variants
ahead of second implementations, against its own stated rule) and
*under-built in lifecycle* (HTTP timeouts, retention, backups).

---

## 1. Bugs found during the review

### RC-1 — SIGTERM/ctrl-C shutdown deadlocks (verified)

`run()` clones the WAL-backed sink for the universe manager:

- `crates/venue-process/src/main.rs:117` — `let reference_sink = sink.clone();`
- `main.rs:285` / `main.rs:207` — `shutdown(adapter, wal, …)` is the tail
  expression of `run()`, so **`reference_sink` is still alive while
  `shutdown` executes** (locals drop after the tail expression completes).
- Inside `shutdown`, `drop(wal)` → `WalWriter::drop`
  (`crates/recorder/src/lib.rs:212-225`) joins the writer thread, which only
  exits when the channel disconnects (`lib.rs:68`) — i.e. when **all**
  `WalSink` clones are dropped. `reference_sink` can only drop after
  `shutdown` returns. Circular wait; the join never returns.

The mechanism was verified with a temporary probe test: dropping `WalWriter`
with one live sink clone blocks past a 2 s deadline and completes immediately
once the clone is dropped (probe deleted after the run). The hazard is even
documented at `recorder/src/lib.rs:199-201` ("every `WalSink` clone …
must be dropped before the `WalWriter`, or `Drop` blocks forever") — and the
contract's only production caller violates it. This is almost certainly a
Phase-2 regression: the Phase-1 graceful-shutdown demo predates the universe
manager that introduced `reference_sink`.

**Consequences.** "WAL flushed; exiting cleanly" is dead code in effect.
Under systemd the unit hangs in deactivating until `TimeoutStopSec` (default
90 s) SIGKILLs it; signal handling is also dead during the hang (the runtime
worker is blocked inside a synchronous `join()`), so a second ctrl-C on the
dev box does nothing. Data impact is bounded — the writer keeps fsyncing on
its 1 s cadence while deadlocked — so this is an operational bug, not a
durability bug. It also poisons the Phase-2 deploy procedure: every config
change or binary upgrade does a SIGKILL-shaped restart.

**Fix.** Immediate: `drop(reference_sink);` before calling `shutdown` (and
audit the early-return path at `main.rs:207`). Structural fix is RC-10 —
this bug is the proof that the Drop-join protocol is fragile by construction.

### RC-2 — Unbounded join on the snapshot fetcher in `disconnect` (reasoned)

`BinanceAdapter::disconnect` (`crates/venue-binance/src/lib.rs:519-523`)
does `cancel.cancel(); let _ = handle.await;` with no deadline, and the
snapshot fetcher awaits `fetch_snapshot()` *inline in a select arm*
(`rest.rs:110-137`) with a client built as `reqwest::Client::new()` —
**no timeout** (`rest.rs:74`). A black-holed TCP connection mid-fetch means
cancellation is never polled and `disconnect` hangs, which hangs the same
shutdown path as RC-1 by a second route. `WsPool::disconnect` and
`SourceSet::shutdown` both got 3 s join budgets; this join was missed.

---

## 2. Too simplified

### RC-3 — Symbology mapping is not point-in-time; cross-venue research inherits survivorship bias

The `Registry` has full point-in-time machinery — validity windows,
`covers(at)`, both lookup directions (`crates/symbology/src/lib.rs:79-183`)
— but the builder can never produce anything for it to discriminate:

- `build()` reads only the **latest** dump per venue (`build.rs:58-67`,
  `build.rs:229-257`);
- `valid_to` is hardcoded `None` in both parsers (`build.rs:122-126`,
  `174-181`);
- symbols delisted before the latest dump are **absent entirely**, not
  closed.

So `mapping.parquet` is "current listings with backdated `valid_from`", while
`lib.rs` documents it as a versioned mapping with validity intervals. The
consequence is live today, not in Phase 5: the Bybit funding backfill
reaches back ~3 years specifically to support funding-spread research, and
any join through the mapping silently drops every symbol delisted since the
latest dump — survivorship bias baked into the exact dataset Phase 2 was
built to deliver. The instruments SCD (`scd.rs`) does this correctly by
folding *all* accumulated dumps; the mapping builder needs the same fold (the
dumps are already on disk for Binance; Bybit accumulates daily from the
backfill timer).

**Recommendation.** Until the fold is implemented, state the bias explicitly
in `data-products.md` (consumers currently have no way to know). Implement
before any research consumes multi-year cross-venue joins, and no later than
Phase 5 venue expansion.

### RC-4 — Ad-hoc HTTP clients without timeouts in the adapter

The crate knows this failure mode — `venue-process` wraps its own
exchangeInfo calls in `tokio::time::timeout` and says why
(`main.rs:29-31`) — but the discipline is inconsistent:

- `fetch_exchange_info` uses bare `reqwest::get` (one-shot client, no
  timeout, new TLS handshake per call) — `venue-binance/src/lib.rs:280`;
- `fetch_funding_info_raw` likewise — `rest.rs:213`;
- the snapshot fetcher's long-lived client has no timeout — `rest.rs:74`;
- `subscribe()` calls `fetch_funding_info()` and (on the static-OI path)
  `fetch_instruments()` **unwrapped** (`lib.rs:412`, `lib.rs:487`) inside the
  startup retry loop — a stalled connection wedges startup indefinitely;
- the pollers' daily fundingInfo refresh awaits inline without selecting on
  cancel (`pollers.rs:291-299`) — a stalled refresh wedges that poller until
  process restart (visible in heartbeat staleness, but nothing recovers).

The worst case is the snapshot fetcher: one hung depth-snapshot fetch stops
all future snapshots silently; the failure surfaces a day later as QA
`missing_snapshot` failures, burning Phase-2 green days. Meanwhile
`poller_client()` (30 s timeout) and both backfill sources (30 s) got it
right.

**Recommendation.** One shared `reqwest::Client` with a 30 s timeout per
adapter, used by every REST path; delete the bare `reqwest::get` calls. This
is a small diff and removes a whole failure class before the unattended
window starts counting.

### RC-5 — OI poller cadence drifts above the configured interval, and the backfill-retirement criterion won't notice

The sweep paces as `pace = every / universe.len()` and then, per symbol,
sleeps `pace` **and then** fetches sequentially (`pollers.rs:424-443`).
Fetch latency is not subtracted, so one sweep takes
`every + N × RTT`. With ~500–700 TRADING perps and 100–250 ms to fapi,
that's +60–170 s per configured 300 s sweep — real cadence ~360–470 s. The
guard at `pollers.rs:487-497` only enforces a *minimum* interval.

This matters because of the plan, not the number itself: the OI-history
backfill (5-minute venue grid) is to be retired once
`open_interest.parquet` spans 30 published days
(implementation-plan, deploy/README). The retirement criterion counts
**days**, not samples/day, so the dataset would silently drop from a uniform
288-points/day grid to ~190–240 irregular points/day at the moment the
better source is switched off.

**Recommendation.** Subtract elapsed fetch time from the per-symbol sleep
(or run fetches with small bounded concurrency), and make the retirement
check compare daily sample counts against the 288-row grid, not day coverage.

### RC-6 — Converter and QA never check each other

`sweep` runs `convert_wal` and `qa_wal` as two independent passes over the
same WAL (`sweep.rs:147-149`). The converter's per-table row counts are
computed and then discarded (`parquet_converter.rs:323-334` ignores
`finish()` results); QA's `events.by_kind` totals are never compared against
what actually landed in Parquet. A converter bug that silently drops rows —
the exact class the raw tee exists to survive — passes QA and publishes a
green day. The invariant is nearly free: `convert_wal` already counts rows
per table; return them and assert against QA's by-kind counts in the report
(modulo the documented row-explosions: per-level book rows, per-trade rows).

### RC-7 — No data lifecycle: retention, disk, backups

- WAL + raw tee grow ~2–3 GB/day/venue (deploy/README:17-19) and nothing
  ever deletes or archives them; Parquet then duplicates the WAL content, so
  steady state is ~3 copies of every byte (wal + rawwal + parquet).
- The N2 policy makes disk-full = capture death *by design* — correct — but
  the only prevention is "check `df`" until Phase-3 alerts. Disk-full is the
  most likely way this system dies in month two.
- There is no backup/replication story at all, while the docs repeatedly
  (and correctly) call reference/lifecycle events "unrecoverable if not
  captured". Single **host** is a decision (A18); single **copy** is not
  stated as one, and a dead disk currently loses everything including the
  multi-year backfills.

**Recommendation.** Write the retention policy down and automate it in the
sweep: e.g. delete `.rawwal` after N consecutive green QA+reconcile days for
that date; compress or archive `.wal` after conversion + reconciliation.
Add an off-host sync (rsync/restic) for `data/parquet`, `data/meta`,
`data/backfill` at minimum. Both belong in Phase 3 alongside the manifest.

---

## 3. Overengineered

### RC-8 — `VenueAdapter<S>` is a decorative abstraction (against the repo's own rule)

The trait (`venue-adapter/src/lib.rs:120-134`) is generic over
`S: EventSink`, but `S` appears in **no method signature** — it's a phantom
parameter that only constrains the impl. There is exactly one
implementation, and the only consumer (`venue-process`) is hardwired to
`BinanceAdapter` anyway because everything it actually needs is inherent
methods outside the trait: `fetch_instruments_raw`, `fetch_funding_info_raw`,
`fetch_instruments_all`, `with_universe`, `with_poller_cfg`. The real venue
seam is demonstrably bigger than this trait, and Bybit (Phase 5) will force
a re-cut regardless.

The standing constraint says it best: *"no trait generalizes until venue #2
forces it."* `IngestSource` honors that rule and earns its keep (four
heterogeneous source types already run under `SourceSet`). `VenueAdapter`
violates it. **Recommendation:** delete the trait (keep the inherent
methods), or strip the phantom param and accept it as documentation; either
way, stop maintaining the pretense that it is the venue seam.

### RC-9 — Dead subscription arms encode venue knowledge in the wrong layer

The config crate knows `!bookTicker` was removed from fapi and rejects
venue-wide book_ticker/trade/depth with a good error
(`config/src/lib.rs:274-277`; confirmed by the comment in
`configs/binance.toml`). The adapter does not know: `Scope::All` +
`BookTicker` still happily subscribes `"!bookTicker"`
(`venue-binance/src/lib.rs:373-376`) — an acked-but-dead stream, the exact
silent-zero-data failure mode this repo's config validation was built to
kill. Similarly `Scope::Class` is accepted by the API and warn-skipped at
runtime (`lib.rs:399-404`), and is unconstructible from config. Today only
`venue-process` embeds the adapter so the config shield holds; any other
embedder (the Phase-4 replay tooling, tests, a future paper-trading
process) bypasses it.

**Recommendation:** venue capability knowledge belongs in the venue adapter.
Make `subscribe` return `SubscriptionFailed` for venue-wide types with no
live stream (config can keep its friendlier message), and delete the
`Scope::Class` arm until the universe manager actually expands it.

### RC-10 — Drop-as-shutdown is the fragile pattern under RC-1

The contract "all sink clones dropped before the writer" is documented three
times (recorder twice, venue-process once) and enforced zero times. RC-1 is
what that costs: any future clone — this time `reference_sink`, next time a
metrics exporter or bus tee — reintroduces a silent hang that no test
catches and no log explains. Protocols that live only in comments don't
survive contact with Phase-2-sized changes; this one already didn't.

**Recommendation.** Make shutdown an API instead of a convention:
`WalWriter::close(self, deadline)` that closes intake (sentinel message or
explicit flag + drain), joins with a timeout, and logs queue depth if it
must abandon; `Drop` becomes best-effort with a loud warning. Same deadline
treatment for the snapshot-fetcher join (RC-2). This converts the invisible
ordering requirement into a compile-visible call.

### RC-11 — Reserved wire vocabulary: looks speculative, is actually justified — keep it

`provenance: Option<Provenance>` (always `None`), empty `ChainPayload` /
`AccountPayload` enums, `Reorg`, `MarketResolved`, `Pool`,
`PredictionOutcome`, `Finality`: a reviewer's first instinct is YAGNI. But
wire v1 is **positional and frozen** — adding an envelope field later means a
wire-version bump and a dual-decoder migration, while reserving now costs
one nil byte per event and zero-cost never-constructed variants. This is
bought insurance against the expensive failure, taken deliberately (R3/A13).
The same logic does *not* extend to behavior (see RC-8/RC-9): reserving
vocabulary is cheap; maintaining dead code paths is not.

Similarly, full static dispatch (`<S: EventSink, R: RawFrameSink>` threaded
through adapter, pollers, `ConnCtx`) is defensible-but-costly: the hot path
is a channel `try_send` plus buffered disk write, so `dyn` would be
invisible in the noise, and the genericity tax is real (the
`with_raw_tee` destructure-rebuild dance at `lib.rs:220-233`, generic
infection of every helper). Not worth reverting; worth not extending
dogmatically — the future bus and replay layers can be `dyn` without guilt.
Note the system already mixes models: `IngestSource` boxes its futures for
dyn-compatibility one level above the statically-dispatched sinks, which is
the right pragmatism.

### RC-12 — Documentation apparatus at the edge of single-maintainer capacity

3.2k lines of governance docs for 15k of code, with phase status restated on
five surfaces (README, implementation-plan, architecture.md, report §7,
deploy/README). The system demonstrably works — code comments cite finding
IDs, superseded docs get archived, the README was honestly updated this
revision — but every status change now requires 3–5 synchronized edits, and
drift between surfaces is the likely first failure ("build complete"
appears with slightly different qualifications in three places already).
**Recommendation:** one authoritative status table (implementation-plan);
everything else links to it instead of restating. Cheap discipline now,
compounding savings later.

---

## 4. Architecture not optimal (structural, non-urgent)

- **RC-13 — `recorder` is a grab-bag crate.** WAL writers + Parquet tables +
  converter + QA + sweep + stats + two bins. `backfill` and `symbology`
  depend on it *only* for `tables.rs` helpers, so the dependency edge
  "symbology → recorder" misstates the architecture (reference-data builds
  do not depend on capture). Compile-time coupling only. When convenient:
  split `tables.rs` + timestamp helpers into a `lake-tables` crate;
  `recorder` keeps the capture side.
- **RC-14 — Four parallel type vocabularies.** `MarketPayload` (wire) /
  `EventKind` (stats+QA) / `DataType` (subscription) / `DataTypeCfg`
  (config) with hand-written total mappings. Each is locally justified
  (wire freeze, array indexing, venue surface, strict TOML), but adding one
  data type now touches ~6 files (payload enum, EventKind ×4 places,
  converter arm, table struct, QA arm, config+adapter). Mostly irreducible
  given the freeze rules — but write the "add a data type" checklist into
  architecture.md before venue #2 makes someone discover it by omission.
- **RC-15 — `tables.rs` repetition.** Ten table structs hand-roll identical
  `flush`/`maybe_flush`/`finish` (~900 lines). Chosen explicitness, fine at
  this scale, greppable; revisit with a small macro only when the table
  count next doubles. Lowest priority in this report.
- **RC-16 — Incremental subscribes never coalesce.** Each `subscribe()` call
  chunks into *new* connections even when existing ones sit far below the
  200-stream cap (`ws_pool.rs:176-232`). With universe auto-subscribe
  enabled, every new listing costs a connection (fd + task + venue conn
  slot) forever. Theoretical today — the example config leaves
  `auto_subscribe_data` empty — but worth a TODO where it bites.
- **RC-17 — Cross-process rate budgets are coordinated by clock, not by
  budget.** Live pollers, the 02:30 reconciler, the 03:30 OI backfill, and
  any manual pulls share the same IP weight and funding-endpoint budgets
  with per-process pacers only. Volumes are currently small and the timer
  spacing is deliberate; a second venue process or an ad-hoc daytime
  backfill run erodes the margin invisibly. A shared budget is
  overengineering today — a one-line warning in deploy/README ("don't run
  manual backfills while capture is up without checking weight logs") is
  not.

### Smaller observations

- `ms_to_nanos` (`venue-binance/src/lib.rs:577`) wraps silently in release
  on garbage input; venue-supplied u64 ms × 10⁶ is safe for sane values
  (year-2100 sentinel ≈ 4.1e18 < u64::MAX) but a corrupt field would
  misroute rather than fail. A `debug_assert!` or saturating multiply is
  free.
- `nanos_to_date` (`recorder/src/lib.rs:36-43`) falls back to *today* on an
  out-of-range timestamp — silent misrouting into the wrong day file; a
  `warn!` would make it diagnosable.
- `WalWriter::send(&event)` clones every event and is used only by tests —
  take ownership or remove in favor of the sink.
- QA dup/regression checks are report-only — documented as a deliberate
  promotion plan; fine, keep the promotion on the Phase-3 list so it isn't
  forgotten.
- `tokio-tungstenite` with `native-tls` ties deploys to the host OpenSSL;
  `rustls` would remove an ops dependency. Taste.
- Backfilled funding rows stamp `interval_ns = NULL` rather than today's
  value — correct call (today's interval is wrong for symbols whose cadence
  changed), and documented at the write site.

---

## 5. What the reality check confirms is right

Briefly, because it shapes the priorities: the wire crate (golden bytes +
`encoding_probe` pinning rmp's positional/name semantics — exactly the
right tripwire for a frozen format); `FrameReader` recovery with sticky
`BadVersion`; durability-at-the-edge with the raw tee and fatal-on-WAL-IO;
QA's explained/unexplained split tied to the recorded control timeline (the
single best idea in the repo — backtests and audits see the same
discontinuities live saw); idempotent sweep with marker files and atomic
renames everywhere; live-captured fixture tests with dates; config that
rejects what the venue can't deliver; the DEC-4 honesty that reconciliation
verifies pipeline completeness, not dual-channel agreement; and the
backfill-vs-live schema-identity test. None of the findings above argue for
redesign; they argue for finishing the lifecycle edges of a sound design.

## 6. Priorities

| # | Finding | When | Why |
|---|---------|------|-----|
| 1 | RC-1 drop `reference_sink` before shutdown | now (1 line) | every deploy/restart is currently a SIGKILL |
| 2 | RC-10/RC-2 explicit `close(deadline)` for writers + fetcher join | this week | removes the regression class, not just the instance |
| 3 | RC-4 shared 30 s-timeout client in the adapter | before the SLO window matters | silent snapshot death burns green days undetected for a day |
| 4 | RC-5 OI pacing + sample-density retirement check | before ~2026-07-12 | the backfill-retirement decision is otherwise blind to the grain drop |
| 5 | RC-3 mapping point-in-time fold (or documented bias) | before multi-year cross-venue research | survivorship bias in the flagship dataset |
| 6 | RC-6 converter↔QA row-count cross-check | Phase 3 (manifest) | closes the last unaudited hop in the pipeline |
| 7 | RC-7 retention policy + off-host sync | Phase 3 (alerts) | disk-full is the likeliest death; single-copy contradicts "unrecoverable" |
| 8 | RC-8/RC-9 delete decorative trait + dead arms | anytime | cheap deletions; aligns code with its own stated rules |
| 9 | RC-12 single status surface; RC-13..17 | opportunistic | maintenance tax, not risk |
