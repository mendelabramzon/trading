# Trading Data Framework

Event-driven market data infrastructure for collecting, recording, and replaying
data from multiple venues. Built for strategy development and live trading.

## Architecture

### System Overview

Every component communicates through a single abstraction — `EventSink`. Venue
adapters, the event bus, recorder, replay engine, and trading strategies all
produce or consume the same `Event` type. Swapping transport, adding venues, or
plugging in a new strategy requires no changes to existing components.

```mermaid
graph LR
    subgraph Venues ["Venue Processes"]
        direction TB
        B[Binance<br/><i>WsPool · N conns</i>]
        BY[Bybit<br/><i>WsPool · N conns</i>]
        OKX[OKX<br/><i>WsPool · N conns</i>]
    end

    subgraph Bus ["Event Bus"]
        direction TB
        R{{"Topic Router<br/><i>venue · instrument · data_type</i>"}}
    end

    subgraph Consumers ["Consumer Processes"]
        direction TB
        REC[Recorder<br/><i>WAL → Parquet</i>]
        S1[Strategy A]
        S2[Strategy B]
        MON[Monitor / Metrics]
    end

    B -- "EventSink" --> R
    BY -- "EventSink" --> R
    OKX -- "EventSink" --> R

    R -- "filtered" --> REC
    R -- "filtered" --> S1
    R -- "filtered" --> S2
    R -- "filtered" --> MON

    style Venues fill:#1a1a2e,stroke:#16213e,color:#e0e0e0
    style Bus fill:#0f3460,stroke:#16213e,color:#e0e0e0
    style Consumers fill:#1a1a2e,stroke:#16213e,color:#e0e0e0
```

### Event Flow

A single event traced from exchange WebSocket to strategy and storage:

```mermaid
sequenceDiagram
    participant WS as Exchange WS
    participant VA as Venue Adapter
    participant BUS as Event Bus
    participant STR as Strategy
    participant REC as Recorder

    WS->>VA: JSON frame
    activate VA
    Note over VA: Deserialize<br/>Stamp local_ts<br/>Construct Event
    VA->>BUS: sink.send(Event)
    deactivate VA
    activate BUS
    Note over BUS: Topic filter:<br/>venue / instrument / type
    par fan-out
        BUS->>STR: Event
        BUS->>REC: Event
    end
    deactivate BUS
    activate STR
    Note over STR: on_event(&Event)<br/>signal generation
    deactivate STR
    activate REC
    Note over REC: WAL append<br/>(hot path)
    REC-->>REC: background: WAL → Parquet
    deactivate REC
```

### Live vs Replay: Same Interface

Strategies implement a single event handler. The framework decides whether events
come from live venues or from recorded Parquet files. The strategy cannot tell
the difference.

```mermaid
graph TB
    subgraph Live
        V[Venue Adapter] -- "EventSink" --> BUS1[Event Bus]
    end

    subgraph Replay
        PQ[(Parquet Files)] --> RP[Replay Engine]
    end

    BUS1 -- "Event" --> EH
    RP -- "Event" --> EH

    EH["Strategy<br/><i>fn on_event(&Event)</i>"]

    style Live fill:#1a1a2e,stroke:#16213e,color:#e0e0e0
    style Replay fill:#1a1a2e,stroke:#16213e,color:#e0e0e0
    style EH fill:#533483,stroke:#16213e,color:#e0e0e0
```

### Recording Pipeline

```mermaid
graph LR
    E[Events] --> WAL["WAL Writer<br/><i>append-only binary<br/>dedicated OS thread</i>"]
    WAL --> F[("data/wal/<br/>binance/2026-06-04.wal")]
    F --> PC["Parquet Converter<br/><i>background task</i>"]
    PC --> PQ[("data/parquet/<br/>binance/2026-06-04/<br/>book_ticker.parquet<br/>trades.parquet<br/>...")]

    style WAL fill:#0f3460,stroke:#16213e,color:#e0e0e0
    style PC fill:#0f3460,stroke:#16213e,color:#e0e0e0
```

### WebSocket Connection Sharding

Each venue adapter automatically shards subscriptions across multiple WebSocket
connections when the stream count exceeds the exchange's per-connection limit.

```mermaid
graph LR
    SUB["subscribe(300 instruments × 3 types)<br/><i>= 900 streams</i>"]

    SUB --> POOL[WsPool]

    POOL --> C1["WS Conn 1<br/><i>200 streams</i>"]
    POOL --> C2["WS Conn 2<br/><i>200 streams</i>"]
    POOL --> C3["WS Conn 3<br/><i>200 streams</i>"]
    POOL --> C4["WS Conn 4<br/><i>200 streams</i>"]
    POOL --> C5["WS Conn 5<br/><i>100 streams</i>"]

    C1 --> SINK["EventSink<br/><i>shared, cloned</i>"]
    C2 --> SINK
    C3 --> SINK
    C4 --> SINK
    C5 --> SINK

    style POOL fill:#533483,stroke:#16213e,color:#e0e0e0
    style SINK fill:#0f3460,stroke:#16213e,color:#e0e0e0
```

### Plugging In a Strategy

A strategy is any consumer that receives events from the bus. Subscribe with a
topic filter to receive only the data you need.

```mermaid
graph LR
    BUS[Event Bus]

    BUS -->|"all events"| REC[Recorder]
    BUS -->|"btcusdt + ethusdt<br/>BookTicker only"| MM["Market Making<br/>Strategy"]
    BUS -->|"all perps<br/>FundingRate only"| ARB["Funding Arb<br/>Strategy"]
    BUS -->|"btcusdt<br/>all data types"| MOM["Momentum<br/>Strategy"]

    style BUS fill:#0f3460,stroke:#16213e,color:#e0e0e0
    style MM fill:#533483,stroke:#16213e,color:#e0e0e0
    style ARB fill:#533483,stroke:#16213e,color:#e0e0e0
    style MOM fill:#533483,stroke:#16213e,color:#e0e0e0
```

## Crate Map

| Crate | Status | Purpose |
|-------|--------|---------|
| `venue-core` | built | Domain types: `Event`, `Payload`, `Level`, `Trade`, `InstrumentId`, `VenueId`, `Nanos` |
| `venue-adapter` | built | Traits: `VenueAdapter<S: EventSink>`, `EventSink`, `Subscription`, `DataType` |
| `venue-binance` | wip | Binance Futures adapter with WsPool sharding |
| `wire` | planned | Binary event serialization for IPC |
| `transport` | planned | `EventSink` impls: `UdsSink` (Phase 1), `ShmSink` (Phase 2) |
| `event-bus` | planned | Central pub/sub event router with topic filtering |
| `recorder` | planned | WAL hot capture + Parquet conversion |
| `replay` | planned | Parquet reader, emits events through `EventSink` |

## IPC Transport Phases

| Phase | Transport | Latency | Change required |
|-------|-----------|---------|-----------------|
| 1 | Unix Domain Sockets | ~20-55 us end-to-end | — |
| 2 | Shared Memory Ring Buffers | < 10 us end-to-end | swap `EventSink` impl at startup |

No venue, strategy, or consumer code changes between phases. Only the concrete
type passed to `BinanceAdapter::new(sink)` differs.

## See Also

- [docs/architecture.md](docs/architecture.md) — full technical architecture with code examples, wire formats, and latency budgets
