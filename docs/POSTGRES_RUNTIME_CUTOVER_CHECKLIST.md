# Postgres Runtime Cutover Checklist (No SQLite Runtime)

## Current live state
- Data parity: ✅ sqlite vs postgres for migrated app-state tables
- Trading safety: ✅ `TRADING_ENABLED=false`
- Execution workers: ✅ stopped for cutover window
- Dashboard: ✅ restarted (read visibility), execution services remain off

## Refactor workstream (required before flipping DATABASE_URL)

### 1) Connection layer
- [ ] Introduce shared DB connection abstraction supporting PostgreSQL for state DB.
- [ ] Remove sqlite-only assumptions in:
  - `options_ai/db.py`
  - `options_ai/dashboard_api/main.py` (`_db_path_from_database_url`, `_connect`)

### 2) SQL dialect migration
- [ ] Replace sqlite-only SQL in runtime paths:
  - `PRAGMA ...`
  - `INSERT OR IGNORE`
  - `INSERT OR REPLACE`
  - sqlite date funcs (`datetime(...)`, `strftime(...)`) in state queries

### 3) Service modules (priority)
- [ ] `options_ai/execution_main.py`
- [ ] `options_ai/execution_monitor_main.py`
- [ ] `options_ai/execution_risk_guard_main.py`
- [ ] `options_ai/execution/*` workers using direct sqlite3
- [ ] `options_ai/dashboard_api/tuning_control.py`
- [ ] `options_ai/queries.py`
- [ ] `options_ai/storage_monitor_main.py`

### 4) Startup/bootstrap behavior
- [ ] Ensure schema init/migrations are Postgres-safe and idempotent.
- [ ] Ensure service startup does not execute sqlite-only DDL on PG connections.

### 5) Verification
- [ ] Unit/smoke tests for critical endpoints and execution loops.
- [ ] Bring up services in order with trading disabled:
  1. dashboard API
  2. execution worker
  3. execution monitor
  4. risk guard
- [ ] Endpoint smoke checks:
  - `/api/portfolio_backtest/status`
  - `/api/portfolios`
  - `/api/execution/intents`
  - `/api/execution/risk-session`

### 6) Enable execution after validation
- [ ] Confirm no sqlite errors in journals.
- [ ] Re-enable trading only after explicit confirmation.
