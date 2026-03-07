# Stack & Services (Deployment Reference)

This repo contains **multiple pipelines**:

1) The original OptionsPredicator daemon (SQLite + LLM/ML routing)
2) SPX options-chain ingestion to TimescaleDB (Phase 1)
3) Feature/label builder (Phase 2)
4) Multi-anchor debit spread builder + labels
5) Debit spread ML scorer
6) Dashboard UI (FastAPI)
7) Execution v1 workers (intent builder / executor / monitor / risk guard)

This document lists the services and how they are expected to be run on a server.

> Reference host used during development: `Jarvis5`

---

## Ports

- Dashboard API/UI: **8088** (FastAPI)
- TimescaleDB: host port **5433** (container port 5432)
  - 5432 may already be in use by another Postgres/Timescale instance

---

## TimescaleDB (Docker)

Compose file:
- `docker-compose.timescale.yml`

Bring up:
```bash
docker compose -f docker-compose.timescale.yml up -d
```

Restart behavior:
- container uses `restart: unless-stopped`

---

## systemd units

### Stack (Timescale container)
- `systemd/optionspredicator-stack.service`
  - brings up TimescaleDB via docker compose

### Phase 1 — chain ingester
- `systemd/spx_chain_ingester.service`
  - runs: `python -m options_ai.spx_chain_main`

### Phase 2 — 0DTE features + labels
- `systemd/spx_chain_phase2.service`
  - runs: `python -m options_ai.phase2_main`

### Debit spread candidates + labels
- `systemd/spx_debit_spreads.service`
  - runs: `python -m options_ai.debit_spreads_main`

### Debit spread ML scorer
- `systemd/spx_debit_ml.service`
  - runs: `python -m options_ai.debit_spread_ml_main`

### Dashboard
- `systemd/options_ai_dashboard_api.service`
  - runs: `uvicorn options_ai.dashboard_api.main:app --port 8088`

### Execution v1 workers
- `systemd/options_ai_execution.service`
  - runs: `python -m options_ai.execution_main`
- `systemd/options_ai_execution_monitor.service`
  - runs: `python -m options_ai.execution_monitor_main`
- `systemd/options_ai_risk_guard.service` (optional but recommended)
  - runs: `python -m options_ai.execution_risk_guard_main`

---

## Recommended enable/start

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now optionspredicator-stack
sudo systemctl enable --now spx_chain_ingester
sudo systemctl enable --now spx_chain_phase2
sudo systemctl enable --now spx_debit_spreads
sudo systemctl enable --now spx_debit_ml
sudo systemctl enable --now options_ai_dashboard_api

# execution v1
sudo systemctl enable --now options_ai_execution
sudo systemctl enable --now options_ai_execution_monitor
# optional (session risk / force-close automation)
sudo systemctl enable --now options_ai_risk_guard
```

---

## Critical environment variables

All services load `/opt/OptionsPredicator/.env` via `EnvironmentFile=`.

### Timescale / chain ingestion
- `TIMESCALE_PORT=5433`
- `SPX_CHAIN_DATABASE_URL=postgresql://spx:spxpass@localhost:5433/spxdb`
- `INPUT_DIR=/mnt/SPX` (often read-only)
- `ARCHIVE_ROOT=/mnt/options_ai`

### Phase 2
- `TZ_LOCAL=America/Chicago`
- `HORIZONS_MINUTES=15,30,45,60,90`

### Debit spreads
- `DEBIT_HORIZONS_MINUTES=30`
- `MAX_DEBIT_POINTS=5.0`

### Big-win definition for probability
- `DEBIT_BIGWIN_MULT_ATM=2.0`
- `DEBIT_BIGWIN_MULT_WALL=4.0`  (applies to CALL_WALL/PUT_WALL/MAGNET)

### Execution v1 (new)
- `TRADING_ENABLED=false` (safe default)
- `BROKER_NAME=tastytrade`
- `BROKER_ENV=sandbox`
- `TASTY_BASE_URL=https://api.cert.tastyworks.com`
- `TASTY_ACCOUNT_NUMBER=...`
- `MAX_DAILY_LOSS_USD=300`
- `SESSION_TZ=America/Chicago`
- `FORCE_CLOSE_MINUTES_BEFORE_END=15`
- `REPRICE_MAX_ATTEMPTS=3`
- `REPRICE_STEP=0.05`
- `REPRICE_INTERVAL_SECONDS=25`
- `REPRICE_MAX_TOTAL_CONCESSION=0.15`

---

## Common failure mode: DB not ready at reboot

If Phase 2 starts before the Timescale container is ready, it may fail with a connection error and auto-restart.

Check:
```bash
sudo systemctl status spx_chain_phase2 --no-pager -l
sudo docker ps
```

Restart:
```bash
sudo systemctl restart spx_chain_phase2
```
