# Deploy (single Linux host, systemd)

Phase-1 topology: one supervised capture process per venue writing WAL + raw
tee; an hourly timer converts closed days to Parquet and emits the daily QA
report. Single host by design (A18) — multi-host growth ships files, never a
network bus.

## Prerequisites

- **chrony** (or equivalent NTP discipline), running *before* capture:
  `local_ts` is the replay merge clock (A9); an undisciplined clock poisons
  cross-venue ordering forever. Verify with `chronyc tracking` (offset should
  be well under 1 ms). The QA report's `local_minus_venue` distributions are
  the ongoing monitor.
- Disk headroom: budget ~1–2 GB/day per venue at the default subscription set
  (WAL + raw tee + Parquet); raw tee roughly doubles WAL volume. Headroom
  alarms are manual in Phase 1 — check `df` until Phase-3 metrics land.

## Install

```sh
cargo build --release -p venue-process -p recorder
sudo useradd --system --home /var/lib/trading trading   # once

sudo install -D target/release/venue-process /opt/trading/bin/venue-process
sudo install -D target/release/wal-sweep     /opt/trading/bin/wal-sweep
sudo install -D -m 0644 configs/binance.toml /etc/trading/binance.toml
#   …then edit /etc/trading/binance.toml: data_dir = "/var/lib/trading/data"
sudo install -d -o trading /var/lib/trading

sudo install -m 0644 deploy/systemd/trading-capture@.service  /etc/systemd/system/
sudo install -m 0644 deploy/systemd/trading-wal-sweep.service /etc/systemd/system/
sudo install -m 0644 deploy/systemd/trading-wal-sweep.timer   /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now trading-capture@binance trading-wal-sweep.timer
```

## Exit-code contract

| Code | Meaning | Supervisor behavior |
|---|---|---|
| 0 | clean signal shutdown | restart (Restart=always) |
| 1 | fatal runtime — WAL I/O error (N2) or crash | restart after 2 s |
| 2 | invalid config/usage (incl. unknown instrument id) | **stay down** (`RestartPreventExitStatus=2`) |

## Operate

```sh
journalctl -u trading-capture@binance -f          # heartbeat once a minute
systemctl list-timers trading-wal-sweep.timer     # next sweep
journalctl -u trading-wal-sweep                   # conversion + QA results
cat /var/lib/trading/data/parquet/binance/<date>/qa_report.json
```

Healthy heartbeat: non-idle `eps`, `wal_depth` near 0, `fsync_age_ms` < 2000,
`raw_dropped` not growing, `reconnects` stable. Rare kinds (liquidation) are
legitimately stale for hours — staleness is information, not an alarm,
until Phase-3 metrics add per-stream SLOs.

On a `status: "fail"` QA report: read `failures[]`, match depth findings
against `control.timeline` (breaks inside reconnect windows are explained
automatically; unexplained ones mean venue-side loss on a healthy
connection). To re-convert a day after investigation, delete
`data/parquet/<venue>/<date>/` and re-run
`wal-sweep data/wal data/parquet` (or wait for the timer); to force-sweep a
still-open day use `--as-of <tomorrow>`.
