# SQLite -> Postgres Full Cutover Runbook (State + Execution)

## Scope
Remove SQLite runtime dependencies by moving all app state to Postgres (`spx` schema currently).

## Status (executed)
- [x] Data migrated from SQLite to Postgres for:
  - predictions/system tables (`predictions`, `model_usage`, `system_events`, etc.)
  - backtest/group/session tables
  - execution subsystem tables
  - tuning + storage monitor tables
- [x] ID-preserving upsert migration script added:
  - `scripts/migrate_sqlite_to_postgres_state.py`
- [x] Parity checker added:
  - `scripts/pg_state_parity_check.py`
- [x] `TRADING_ENABLED=false` set in `/opt/OptionsPredicator/.env` (safety)

## Important runtime reality
Current codebase still has many SQLite-specific SQL paths (PRAGMA, INSERT OR REPLACE/IGNORE, sqlite placeholders, sqlite-only URL parsing).
Data is migrated, but runtime cutover requires code refactor before flipping `DATABASE_URL` to Postgres.

## Phase A — Safety + freeze
1. Confirm `TRADING_ENABLED=false`.
2. Stop execution workers (`options_ai_execution*`, `options_ai_risk_guard`) with sudo.
3. Keep dashboard up read-only if desired.

## Phase B — Code cutover tasks
1. **DB adapter**
   - Implement DB abstraction layer for sqlite/postgres parameter style and row mapping.
2. **Replace sqlite-only parsing**
   - `options_ai/dashboard_api/main.py:_db_path_from_database_url` must accept postgres URLs.
3. **Replace sqlite-only SQL syntax**
   - Convert `INSERT OR REPLACE/IGNORE` to Postgres upsert syntax.
   - Remove/guard `PRAGMA` usage.
4. **Modules to refactor first**
   - `options_ai/dashboard_api/main.py`
   - `options_ai/dashboard_api/tuning_control.py`
   - `options_ai/queries.py`
   - `options_ai/db.py`
   - execution workers (`options_ai/execution/*`)

## Phase C — Validation
1. Run parity check:
   ```bash
   /opt/OptionsPredicator/.venv/bin/python scripts/pg_state_parity_check.py \
     --pg "postgresql://spx:spxpass@localhost:5433/spxdb" --schema spx --strict
   ```
2. Start dashboard + workers against Postgres state.
3. Smoke tests:
   - `/api/portfolio_backtest/status`
   - `/api/portfolios`
   - `/api/parameter-groups`
   - `/api/execution/intents`, `/api/execution/trades/open`, risk session endpoints

## Phase D — Cutover
1. Flip runtime config to Postgres state DB URL(s).
2. Restart services.
3. Monitor for 1 full session.

## Phase E — Retirement
1. Keep SQLite file immutable backup for rollback window.
2. Remove SQLite code paths after stable burn-in.
3. Remove SQLite service assumptions/docs.

## Rollback
- Keep previous `.env` backup and sqlite DB snapshot.
- Revert config + restart services.
- No destructive migration steps were applied to SQLite.


### One-command freeze/sync/parity helper

Use:
```bash
cd /opt/OptionsPredicator
./scripts/run_pg_cutover_freeze.sh "postgresql://spx:spxpass@localhost:5433/spxdb" spx
```

This script will:
- backup `.env`
- force `TRADING_ENABLED=false`
- stop execution + dashboard services (sudo)
- run final incremental sqlite->pg sync
- run strict parity check

