# Test Plans — Phase 1 (Ingestion) + Phase 2 (0DTE Features/Labels)

This document defines **rerunnable** test plans for validating:

- **Phase 1**: SPX options chain snapshot ingestion into Postgres/TimescaleDB + archive/quarantine behavior.
- **Phase 2**: 0DTE feature and multi-horizon label builder for ML/scoring.

These tests are intended to be run on:
- local dev machines, and
- production-like servers (e.g. Jarvis5) during deployment / upgrades.

They are **non-destructive** by default and use **synthetic test data** where possible.

---

## Prerequisites

### Software

- Python venv created and deps installed:

```bash
cd /opt/OptionsPredicator
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

- TimescaleDB/Postgres available.

### Environment variables

Phase 1 + Phase 2 use:

- `SPX_CHAIN_DATABASE_URL` (required for these tests)
- `ARCHIVE_ROOT` (optional; used by Phase 1 smoke test)

On Jarvis5 (example):

```bash
export SPX_CHAIN_DATABASE_URL=postgresql://spx:spxpass@localhost:5433/spxdb
export ARCHIVE_ROOT=/mnt/options_ai
```

---

## Test Plan A — Service health (deployment sanity)

### A1) Verify systemd services

```bash
sudo systemctl status optionspredicator-stack --no-pager -l
sudo systemctl status spx_chain_ingester --no-pager -l
sudo systemctl status spx_chain_phase2 --no-pager -l
```

**Pass criteria**:
- `optionspredicator-stack`: `active (exited)` is expected.
- `spx_chain_ingester`: `active (running)`.
- `spx_chain_phase2`: `active (running)`.

### A2) Verify Timescale container is up

```bash
sudo docker ps --format 'table {{.Names}}\t{{.Ports}}\t{{.Status}}'
```

**Pass criteria**:
- `spx_timescaledb` is running.

---

## Test Plan B — Phase 1 smoke test (ingest + archive/quarantine path)

This test does NOT require write access to `/mnt/SPX`.
It uses a temporary **read-only** directory to simulate the read-only mount.

### B1) Run smoke test

```bash
cd /opt/OptionsPredicator
source .venv/bin/activate
python scripts/phase1_smoke_test.py --dsn "$SPX_CHAIN_DATABASE_URL" --archive-root "$ARCHIVE_ROOT"
```

**Pass criteria**:
- Script prints `PASS phase1_smoke_test`.
- A test file is present under:
  - `$ARCHIVE_ROOT/test_runs/phase1_<run_id>/archive/20250303/`

---

## Test Plan C — Phase 2 smoke test (features + labels)

This test inserts synthetic rows directly into `spx.option_chain` for two timestamps (`t` and `t+15min`), then:

- computes features for both snapshots,
- computes labels for the 15-minute horizon,
- asserts label directions,
- optionally cleans up inserted rows.

### C1) Run smoke test

```bash
cd /opt/OptionsPredicator
source .venv/bin/activate
python scripts/phase2_smoke_test.py --dsn "$SPX_CHAIN_DATABASE_URL" --cleanup
```

**Pass criteria**:
- Script prints `PASS phase2_smoke_test`.
- It asserts:
  - `label_atm_iv_dir == +1`
  - `label_skew_25d_dir == -1`
  - `is_missing_future == false`
  - `is_low_quality == false`

---

## Test Plan D — Production validation (real data)

Once real JSON snapshots are arriving in `/mnt/SPX` and Phase 1 ingestion is running:

### D1) Confirm ingestion is adding rows

```sql
SELECT max(snapshot_ts) AS latest_snapshot, count(*) AS total_rows
FROM spx.option_chain;
```

### D2) Confirm 0DTE feature rows

```sql
SELECT max(snapshot_ts) AS latest_features, count(*) AS feature_rows
FROM spx.chain_features_0dte;
```

### D3) Confirm label rows

```sql
SELECT horizon_minutes, count(*)
FROM spx.chain_labels_0dte
GROUP BY 1
ORDER BY 1;
```

---

## Troubleshooting

### Port conflict on 5432

If 5432 is already used on the host, set:

- `TIMESCALE_PORT=5433`
- `SPX_CHAIN_DATABASE_URL=...:5433/...`

and restart the stack.

### /mnt/SPX is read-only

This is supported. The ingester will:
- copy to `$ARCHIVE_ROOT/archive/YYYYMMDD/`
- append to `$ARCHIVE_ROOT/state/processed.log`

---

## Notes on idempotency

- Phase 1 DB writes are idempotent via primary key `(snapshot_ts, option_symbol)`.
- Phase 1 file handling in read-only input mode is idempotent via `processed.log`.
- Phase 2 uses upserts:
  - `chain_features_0dte`: PK `(snapshot_ts)`
  - `chain_labels_0dte`: PK `(snapshot_ts, horizon_minutes)`

---

## Test Plan E — Multi-anchor debit spreads (candidates + labels)

This validates the **multi-anchor debit spread system**:

- Computes GEX levels (call wall / put wall / magnet) from gamma*OI.
- Builds adjacent-strike debit spread candidates at each anchor.
- Labels each candidate by horizon (default 30m) using debit change.

### E1) Run smoke test

```bash
cd /opt/OptionsPredicator
source .venv/bin/activate
python scripts/debit_spreads_smoke_test.py --dsn "$SPX_CHAIN_DATABASE_URL" --cleanup
```

**Pass criteria**:
- Script prints `PASS debit_spreads_smoke_test`.
- Synthetic data produces:
  - non-empty candidates at `spx.debit_spread_candidates_0dte`
  - non-empty labels at `spx.debit_spread_labels_0dte`

### E2) Verify tables

```sql
SELECT to_regclass('spx.gex_levels_0dte'),
       to_regclass('spx.debit_spread_candidates_0dte'),
       to_regclass('spx.debit_spread_labels_0dte');
```
