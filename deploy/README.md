# Deploy (single Linux host, systemd)

Topology: one supervised capture process per venue writing WAL + raw
tee (WS streams + REST pollers for funding/mark/index/OI); an hourly timer
converts closed days to Parquet with a daily QA report; daily timers run the
funding reconciler, the perishable OI-history backfill, and the
reference-data build. Single host by design (A18) — multi-host growth ships
files, never a network bus.

## Prerequisites

- **chrony** (or equivalent NTP discipline), running *before* capture:
  `local_ts` is the replay merge clock (A9); an undisciplined clock poisons
  cross-venue ordering forever. Verify with `chronyc tracking` (offset should
  be well under 1 ms). The QA report's `local_minus_venue` distributions are
  the ongoing monitor.
- Disk headroom: budget ~2–3 GB/day per venue with the poller tier enabled
  (premium-index at 30 s ≈ +0.7 GB/day WAL; raw tee roughly doubles WAL
  volume). Headroom alarms are manual until Phase-3 metrics land — check `df`.

## Install

```sh
cargo build --release -p venue-process -p recorder -p backfill -p symbology
sudo useradd --system --home /var/lib/trading trading   # once

sudo install -D target/release/venue-process /opt/trading/bin/venue-process
sudo install -D target/release/wal-sweep     /opt/trading/bin/wal-sweep
sudo install -D target/release/backfill      /opt/trading/bin/backfill
sudo install -D target/release/symbology     /opt/trading/bin/symbology
sudo install -D -m 0644 configs/binance.toml /etc/trading/binance.toml
#   …then edit /etc/trading/binance.toml: data_dir = "/var/lib/trading/data"
sudo install -D -m 0644 configs/symbology-overrides.toml /etc/trading/symbology-overrides.toml
sudo install -D -m 0644 configs/fees/binance.toml /etc/trading/fees/binance.toml
sudo install -D -m 0644 configs/fees/bybit.toml   /etc/trading/fees/bybit.toml
sudo install -d -o trading /var/lib/trading

sudo install -m 0644 deploy/systemd/trading-capture@.service      /etc/systemd/system/
sudo install -m 0644 deploy/systemd/trading-wal-sweep.service     /etc/systemd/system/
sudo install -m 0644 deploy/systemd/trading-wal-sweep.timer       /etc/systemd/system/
sudo install -m 0644 deploy/systemd/trading-reconcile.service     /etc/systemd/system/
sudo install -m 0644 deploy/systemd/trading-reconcile.timer       /etc/systemd/system/
sudo install -m 0644 deploy/systemd/trading-backfill-oi.service   /etc/systemd/system/
sudo install -m 0644 deploy/systemd/trading-backfill-oi.timer     /etc/systemd/system/
sudo install -m 0644 deploy/systemd/trading-symbology.service     /etc/systemd/system/
sudo install -m 0644 deploy/systemd/trading-symbology.timer       /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now trading-capture@binance trading-wal-sweep.timer \
  trading-reconcile.timer trading-backfill-oi.timer trading-symbology.timer
```

One-time history pulls (paced; funding ~30–60 min, run as the trading user):

```sh
/opt/trading/bin/backfill funding --venue binance --data-dir /var/lib/trading/data
/opt/trading/bin/backfill funding --venue bybit --from 2023-01 --data-dir /var/lib/trading/data
/opt/trading/bin/backfill klines --venue binance --from 2025-01 --data-dir /var/lib/trading/data  # optional
```

## Exit-code contract

| Code | Meaning | Supervisor behavior |
|---|---|---|
| 0 | clean signal shutdown / batch success (reconcile: pass) | restart (Restart=always) / timer green |
| 1 | fatal runtime — WAL I/O error (N2), crash, batch failure, reconcile fail/blocked | restart after 2 s / timer red |
| 2 | invalid config/usage (incl. unknown instrument id) | **stay down** (`RestartPreventExitStatus=2`) |

## Operate

```sh
journalctl -u trading-capture@binance -f          # heartbeat once a minute
systemctl list-timers 'trading-*'                 # all recurring jobs
journalctl -u trading-wal-sweep                   # conversion + QA results
journalctl -u trading-reconcile                   # daily funding coverage
cat /var/lib/trading/data/parquet/binance/<date>/qa_report.json
cat /var/lib/trading/data/meta/reconciliation/binance/<date>.json
```

Healthy heartbeat: non-idle `eps` including the poller kinds
(`mark_price`/`index_price`/`funding_prediction` ≈ universe/30 s each,
`open_interest` ≈ universe/300 s, `funding_realized` bursty), `wal_depth`
near 0, `fsync_age_ms` < 2000, `raw_dropped` not growing, `reconnects`
stable. Staleness is per-kind and per-kind = per-poller: `funding_realized`
is legitimately stale up to its interval + poll cadence (hours for
8h-interval-only universes); liquidations for hours on quiet markets —
staleness is information, not an alarm, until Phase-3 metrics add SLOs.

**Phase-2 exit check** (criterion defined in
`docs/implementation-plan.md`): the latest
`data/meta/reconciliation/binance/<date>.json` must reach
`"consecutive_green_days" >= 14`. A `blocked` status means the day was never
converted/QA-passed (fix the sweep first); `fail` lists exactly which
settlements are missing.

**OI-backfill retirement**: `trading-backfill-oi.timer` exists because the
venue retains only ~30 days of OI history. Once
`data/parquet/binance/*/open_interest.parquet` spans ≥ 30 consecutive
published days, disable the timer (`systemctl disable --now
trading-backfill-oi.timer`) — the live poller is then the better source.

On a `status: "fail"` QA report: read `failures[]`, match depth findings
against `control.timeline` (breaks inside reconnect windows are explained
automatically; unexplained ones mean venue-side loss on a healthy
connection). To re-convert a day after investigation, delete
`data/parquet/<venue>/<date>/` and re-run
`wal-sweep data/wal data/parquet` (or wait for the timer); to force-sweep a
still-open day use `--as-of <tomorrow>`.
