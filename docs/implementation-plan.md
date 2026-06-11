# Implementation Plan — revised 2026-06-11

*The living plan and the **only** phase-status surface — other docs link
here instead of restating status. Findings cited by ID (R*, A*, N*, P*,
D*) come from the review documents that preceded this plan, deleted
2026-06-12 and recoverable from git history (`docs/` before that date).
Convention: phases are sequential gates with exit criteria; as-built notes
are appended, never rewritten over.*

## Phase status

| Phase | Scope | Status |
|---|---|---|
| 0 | wire v1 schema re-cut | ✅ exit met 2026-06-10 (golden bytes + live pu-chain acceptance) |
| 1 | unattended capture | ✅ build complete 2026-06-11; demonstrated locally (full day 2026-06-10: rotation → sweep → QA pass). The 7×24-under-systemd run is subsumed by the Phase-2 watch |
| 2 | completeness + reference | 🔶 **build complete 2026-06-11; exit SLO accumulating once deployed** (earliest ~2026-06-26) |
| 3 | manifest + metrics/alerting | next up — start while Phase 2 accumulates |
| 4 | replay | planned (this repo's half; strategy runtime is out-of-repo) |
| 5 | venue expansion | planned (Bybit first — already pre-validated by backfill) |
| 6 | private-data capture seam | planned (gateways/OMS/risk live in the execution repo) |

## Decisions taken since the report (2026-06-11)

- **DEC-1 — repo boundary.** This repo is capture / storage / monitoring /
  delivery only. Research notebooks, the strategy runtime (R5), the backtest
  economics layer (R8), and execution engines (R7) live in **separate
  repositories** and consume this repo's datasets (`docs/data-products.md`)
  and, later, its `replay` crate. Consequence: the report's Phase 4 shrinks
  here to replay; Phase 6 shrinks to the private-data capture seam. The
  contracts in report §6.4 (Ctx, RiskGate, OMS —
  `report-fable-10062026.md`, git history) remain the spec the other repos
  build against.
- **DEC-2 — keep the published Parquet layout; drop the Hive re-layout.**
  `data/parquet/<venue>/<date>/<type>.parquet` is the consumer contract and
  DuckDB reads it directly; re-cutting paths now would break consumers for
  cosmetic gain. The **manifest** (R10), not a path convention, becomes the
  catalog. (Supersedes report §6.5's `lake/venue=…/date=…` layout.)
- **DEC-3 — the bus is demand-gated, not phase-scheduled.** No live consumer
  exists; the first will be a paper-trading strategy process (other repo).
  Build the one-page lossy UDS bus (A10) — or adopt `iceoryx2` — when that
  consumer is real. Until then WAL + hourly Parquet is the only distribution.
- **DEC-4 — REST is the live producer for funding/mark/index/OI on Binance.**
  The markPrice WS family is acked-but-dead (verified 2026-06-10), so the
  Phase-2 pollers are not a stopgap. Consequences: daily reconciliation
  verifies *pipeline completeness*, not dual-channel agreement; WS parser
  arms stay as a free fallback; `source >= 1` would identify a venue-side
  revival.

## Phase 2 — exit watch (now → ~2026-06-26 at the earliest)

No code remains; the phase exits on operational evidence, and the same
window doubles as Phase 1's 7×24-under-systemd demonstration. Capture so
far has run on the dev machine; the SLO clock effectively starts with the
first *full* poller-covered UTC day on the deploy host (a mid-day start
misses that day's earlier settlements and reconciles red by construction —
2026-06-11 will; the streak legitimately starts after it).

- **First: deploy** per `deploy/README.md` (chrony, units, timers, one-time
  history pulls), then leave it alone — that is the point.
- Daily: `journalctl -u trading-reconcile` /
  `data/meta/reconciliation/binance/<date>.json` — investigate any red day
  same-day while the raw tee still has the evidence; `blocked` means the
  sweep failed first.
- Daily: QA reports stay `pass`; heartbeat shows all poller kinds emitting
  (`deploy/README.md` has the healthy-beat reference).
- **Exit: latest report reaches `consecutive_green_days >= 14`.**
- Follow-on (not exit-blocking): once
  `parquet/binance/*/open_interest.parquet` spans ≥ 30 consecutive published
  days (~2026-07-12), disable `trading-backfill-oi.timer` per
  `deploy/README.md`.

## Phase 3 — queryable surface + ops hardening

Was "research surface + distribution"; the research surface largely shipped
early (`data-products.md`, DuckDB snippets, `spread_check`), and the bus
moved behind DEC-3. What remains is making data status *queryable* and capture
health *alertable* — start now; nothing here blocks on the Phase-2 SLO.

1. **Manifest (R10)** — SQLite at `data/meta/manifest.sqlite`, written by
   `wal-sweep` (per converted day: venue, date, type, path, rows, min/max
   ts, schema_version, QA status), by `backfill` (published months/days),
   and by `symbology build`; the reconciler folds in its verdict. Readers:
   replay (QA gate), research (one query replaces globbing). Existing
   outputs are backfilled by a one-shot scan; JSON reports remain the
   human surface.
2. **Metrics + alerts** — export the existing heartbeat counters
   (events/kind, staleness/kind, wal_depth, fsync age, raw drops,
   reconnects) on a Prometheus endpoint in `venue-process`; timers report
   via textfile/exit status. Alert rules encode the SLOs the docs already
   state: per-kind staleness vs poller cadence, `fsync_age_ms`, growing
   `raw_dropped`, reconcile not `pass`, QA `fail`, disk headroom, timer red.
   This retires the "staleness is information, not an alarm" caveat.
3. **Sorted row groups** — converter sorts each batch by
   `(instrument, venue_ts)` before writing (report §3.3; verified not yet
   implemented). Cheap at the existing 500K-row batch boundary; predicate
   pushdown is the payoff as days accumulate.

*Exit: a manifest query answers "which venue-days exist and passed QA +
reconciliation" for everything published to date; one alert fires within
two minutes in a staged poller-death drill (and a staged disk-full drill
pages before capture dies).*

## Phase 4 — replay (this repo's half)

Manifest-driven Parquet/WAL → `EventSink`, the bridge from recorded data to
the out-of-repo strategy runtime. Contracts are settled and recorded in
`architecture.md` §7: k-way merge over arrival-ordered files (D3), merge
clock selectable per run (`local_ts` cross-venue / `venue_ts` single-venue —
A9/N5), control events replayed (A7), pu-chains stitched across midnight
file boundaries, wire versions N and N−1, QA-gated inputs via the manifest
(unaudited data at minimum warns loudly).

*Exit: the same recorded window replays twice into an identical event
sequence (count, order, content hash) under each clock; a two-day window
stitches the midnight pu-chain with zero unexplained breaks; the
strategy-repo smoke consumer observes the same control timeline live capture
recorded.*

## Phase 5 — venue expansion proofs

One at a time, each closing with fixture tests, raw tee on, reconciliation,
mapping coverage, and 7 consecutive green QA days before the next starts:

1. **Bybit live capture** — the cheapest second venue: funding history,
   fee schedule, and canonical mapping already exist (Phase 2 shipped them;
   `spread_check` proves the join), so this validates the adapter pattern
   (`IngestSource`s, pollers, universe manager) against a second WS/REST
   API and extends the SCD beyond Binance.
2. **Hyperliquid** — DEX proof (A8): hourly funding, info-endpoint pollers.
3. **Polymarket** — new-domain proof: `Reference` lifecycle/resolution
   producers, ephemeral universe (R4); CLOB-WS + catalog poller.
4. **`evm-ingest` skeleton** — provenance/finality/reorg vocabulary in
   anger (R3); provider-hosted RPC (buy, per report §6.6).

*Exit (unchanged from the report): 4+ venues capturing 7×24; `venue-core`
survived without a breaking re-cut, or the bump is documented and migrated.*

## Phase 6 — private-data capture seam

Execution (gateways, OMS, RiskGate, kill switch — R7) lives in the
execution repo. This repo ships the seam it records through: `AccountPayload`
variants populated additively (orders, fills, positions, funding payments),
`data/private/<venue>/` WAL→Parquet with 0600 permissions and separate
retention, never transiting any shared bus (A13), and the
predicted-vs-charged funding reconciliation joining private fills against
this repo's funding datasets — closing the loop on the whole data product.

*Exit: a live round-trip (any size) whose entire lifecycle is
reconstructable from recorded data in this repo.*

## Standing constraints (unchanged)

- Wire v1 freeze rules: positional struct fields, variant names
  load-bearing, additive-only; `encoding_probe` + golden-bytes tests are the
  tripwire (`architecture.md` §3).
- Durability at the edge; any future bus is lossy-only, gap-counted.
- Single host (A18); growth ships files, never stretches a bus.
- Latency roadmap frozen at UDS-class (A1); completeness SLOs are the KPIs.
- Abstractions follow second implementations — no trait generalizes until
  venue #2 forces it.
- Schemas/datasets evolve additively; breaking changes get new names
  (`data-products.md`).
