# Trading Data Framework

Event-driven market data infrastructure for collecting, recording, and replaying
data from multiple venues. Built for strategy development and live trading.

**Status (2026-06-11):** single-venue capture is real, hardened, and
**unattended** (Phase 1 complete) — Binance USD-M futures → framed WAL
(+ raw-frame tee) → zstd Parquet with a daily QA report, run from a TOML
config under systemd with startup retry, heartbeat, and hourly conversion
(see `docs/report-fable-10062026.md` §7). The event bus, replay, and strategy
layers are designed but **not built yet**; diagrams below mark them
*(planned)*.

## Architecture

**Durability lives at the edge**: each venue process writes its own WAL
in-process before anything else sees an event. The future bus serves live
consumers only and is allowed to be lossy (it injects `Gap` control events) —
it is never in the durability path.

```mermaid
graph LR
    subgraph Venue ["Venue Process (built: Binance)"]
        direction TB
        WS[WsPool<br/><i>acks · stale watchdog · reconnect</i>]
        REST[REST<br/><i>depth snapshots · fundingInfo</i>]
        WAL[("data/wal/*.wal<br/>data/raw/*.rawwal")]
        WS --> WAL
        REST --> WAL
    end

    subgraph Bus ["Event Bus (planned)"]
        R{{"Topic Router<br/><i>lossy · gap-counted</i>"}}
    end

    subgraph Consumers ["Live Consumers (planned)"]
        S1[Strategies]
        MON[Monitor / Metrics]
    end

    WS -.-> R
    R -.-> S1
    R -.-> MON

    WAL --> PC["wal-sweep<br/><i>hourly timer · idempotent · daily QA</i>"]
    PC --> PQ[("data/parquet/<br/>zstd, per type + qa_report.json")]

    style Venue fill:#1a1a2e,stroke:#16213e,color:#e0e0e0
    style Bus fill:#0f3460,stroke:#16213e,color:#e0e0e0,stroke-dasharray: 5 5
    style Consumers fill:#1a1a2e,stroke:#16213e,color:#e0e0e0,stroke-dasharray: 5 5
```

### Event envelope (wire v1, frozen)

All components produce or consume one `Event` type:

```rust
Event {
    venue, instrument,
    venue_ts,      // venue transaction time
    local_ts,      // capture-host time (mandatory; replay merge clock)
    source,        // which connection/poller produced it
    provenance,    // reserved for on-chain sources
    payload,       // Market | Reference | Chain | Account | Control
}
```

Market data covers books (ticker / snapshots / diffs with venue update-id
chains), trades (string ids), mark/index, funding (with interval + clamps),
open interest, and liquidations. Control events (`ConnUp/Down`, `SubAck`,
`Gap`, …) are recorded in the WAL like market data, so replay sees the same
discontinuities live consumers saw. The wire format is versioned, CRC-framed,
and self-healing on read; field order is frozen per version
(`docs/architecture.md` §3).

### Live vs Replay: same interface *(replay planned — Phase 4)*

Strategies implement a single event handler; the framework decides whether
events come from live venues or recorded files. Replay sorts (files are
arrival-ordered) and replays control events, so backtests cannot see a fantasy
continuity that live never had.

### Recording pipeline (built)

```mermaid
graph LR
    E[Events] --> W["WalSink<br/><i>lossless, dedicated thread,<br/>1s fsync, midnight rotation</i>"]
    RT[Raw WS frames] --> RW["RawWalSink<br/><i>best-effort tee (R2)</i>"]
    W --> F[("data/wal/binance/<date>.wal")]
    RW --> F2[("data/raw/binance/<date>.rawwal")]
    F --> PC["wal-sweep<br/><i>hourly systemd timer; QA gate</i>"]
    PC --> PQ[("data/parquet/binance/<date>/<br/>book_ticker · book_update · trades<br/>liquidation · control · qa_report.json · …")]

    style W fill:#0f3460,stroke:#16213e,color:#e0e0e0
    style RW fill:#0f3460,stroke:#16213e,color:#e0e0e0
    style PC fill:#0f3460,stroke:#16213e,color:#e0e0e0
```

A WAL I/O error exits the process (a capture process that cannot persist must
die visibly, not look healthy while recording nothing). The raw tee makes
parser defects survivable: re-run normalization instead of losing the day.

### WebSocket connection sharding (built)

`WsPool` shards subscriptions at 200 streams/connection, watches SUBSCRIBE
acks, reconnects with jittered backoff (immediately after stable sessions),
re-snapshots depth on every reconnect, and emits `ConnUp/ConnDown` control
events through the same sink.

## Crate Map

| Crate | Status | Purpose |
|-------|--------|---------|
| `venue-core` | built | Envelope v2, domain payloads, symbology types (`Asset`, `InstrumentClass`, `CanonicalInstrumentId`), `SourceId`, `Provenance`, `RawFrame` |
| `venue-adapter` | built | Traits (RPITIT): `EventSink` (+`send_batch`), `RawFrameSink`, `VenueAdapter<S>`, `Subscription{scope,data}` |
| `venue-binance` | built | Binance USD-M adapter: WsPool, REST snapshot fetcher, fundingInfo, exchangeInfo dump, fixture tests |
| `wire` | built | Framed MessagePack (`magic/version/len/crc32`), self-healing `FrameReader`, golden-bytes layout pin |
| `recorder` | built | `WalWriter`/`WalSink`, `RawWalWriter`, capture stats (heartbeat counters), zstd Parquet converter, QA module, `wal-sweep` bin, acceptance checker (`verify_depth`) |
| `config` | built | Strict TOML capture config; rejects subscriptions the venue can't deliver |
| `venue-process` | built | Unattended supervised capture entrypoint: startup retry, heartbeat, daily exchangeInfo dump, graceful shutdown |
| `backfill` / `symbology` | planned (Phase 2) | REST history + reconciliation (incl. funding/mark via `premiumIndex` — the WS markPrice family is dead on fapi); canonical instrument registry |
| `bus` / `replay` / `strategy` / `execution` | planned (Phases 3–6) | See `docs/report-fable-10062026.md` §7 roadmap |

## Quick start

```bash
# Capture (config-driven; ctrl+c / SIGTERM for graceful shutdown):
cargo run -p venue-process -- configs/binance.toml

# Convert closed days + daily QA report (idempotent; hourly via systemd in deploy):
cargo run -p recorder --bin wal-sweep -- data/wal data/parquet

# Inspect:
cargo run -p recorder --example read_wal data/wal/binance/<date>.wal
cargo run -p recorder --example verify_depth data/wal/binance/<date>.wal   # QA gate, on demand
cat data/parquet/binance/<date>/qa_report.json
```

Deployment (systemd units, chrony prerequisite, runbook): `deploy/README.md`.
The `smoke` example remains as the bounded acceptance tool
(`cargo run -p venue-binance --example smoke 100000 180`); `ws_probe` checks
whether a fapi stream name actually emits (Binance ACKs dead stream names).

## See Also

- [docs/architecture.md](docs/architecture.md) — contracts: schema freeze rules, timestamp semantics, reader recovery, converter schemas
- [docs/report-fable-10062026.md](docs/report-fable-10062026.md) — target architecture and phased roadmap (R1–R12)
- [docs/archive/improvement_plan.md](docs/archive/improvement_plan.md) — the implemented Phase-0 remediation, with as-built amendments (archived)
