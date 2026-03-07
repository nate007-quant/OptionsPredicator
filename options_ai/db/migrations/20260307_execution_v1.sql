-- Execution v1 schema (SQLite)

CREATE TABLE IF NOT EXISTS execution_intents (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at_utc TEXT NOT NULL,
  updated_at_utc TEXT NOT NULL,
  environment TEXT NOT NULL, -- sandbox|live
  broker_name TEXT NOT NULL,
  status TEXT NOT NULL,      -- pending|submitting|working|filled|rejected|cancelled|expired|error
  strategy_key TEXT,
  symbol TEXT,
  candidate_ref TEXT,
  idempotency_key TEXT NOT NULL,
  intent_payload_json TEXT NOT NULL,
  error TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS uniq_execution_intents_idempotency_key
  ON execution_intents(idempotency_key);
CREATE INDEX IF NOT EXISTS idx_execution_intents_status
  ON execution_intents(status, created_at_utc DESC);

CREATE TABLE IF NOT EXISTS trade_runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at_utc TEXT NOT NULL,
  updated_at_utc TEXT NOT NULL,
  environment TEXT NOT NULL,
  broker_name TEXT NOT NULL,
  execution_intent_id INTEGER,
  status TEXT NOT NULL,      -- opening|open|closing|closed|rejected|error
  underlying TEXT,
  side TEXT,
  qty INTEGER,
  entry_order_id TEXT,
  exit_order_id TEXT,
  opened_at_utc TEXT,
  closed_at_utc TEXT,
  open_reason TEXT,
  close_reason TEXT,
  pnl_realized_usd REAL,
  pnl_unrealized_usd REAL,
  run_payload_json TEXT,
  FOREIGN KEY(execution_intent_id) REFERENCES execution_intents(id)
);

CREATE INDEX IF NOT EXISTS idx_trade_runs_status
  ON trade_runs(status, created_at_utc DESC);
CREATE INDEX IF NOT EXISTS idx_trade_runs_open
  ON trade_runs(environment, status);

CREATE TABLE IF NOT EXISTS order_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at_utc TEXT NOT NULL,
  environment TEXT NOT NULL,
  broker_name TEXT NOT NULL,
  trade_run_id INTEGER,
  execution_intent_id INTEGER,
  order_id TEXT,
  event_type TEXT NOT NULL,
  status TEXT,
  raw_payload_json TEXT NOT NULL,
  FOREIGN KEY(trade_run_id) REFERENCES trade_runs(id),
  FOREIGN KEY(execution_intent_id) REFERENCES execution_intents(id)
);

CREATE INDEX IF NOT EXISTS idx_order_events_trade_run
  ON order_events(trade_run_id, created_at_utc DESC);
CREATE INDEX IF NOT EXISTS idx_order_events_status
  ON order_events(environment, status, created_at_utc DESC);

CREATE TABLE IF NOT EXISTS position_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at_utc TEXT NOT NULL,
  environment TEXT NOT NULL,
  broker_name TEXT NOT NULL,
  trade_run_id INTEGER,
  position_key TEXT,
  event_type TEXT NOT NULL,
  qty REAL,
  price REAL,
  pnl_unrealized_usd REAL,
  pnl_realized_usd REAL,
  raw_payload_json TEXT NOT NULL,
  FOREIGN KEY(trade_run_id) REFERENCES trade_runs(id)
);

CREATE INDEX IF NOT EXISTS idx_position_events_trade_run
  ON position_events(trade_run_id, created_at_utc DESC);

CREATE TABLE IF NOT EXISTS risk_session_state (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at_utc TEXT NOT NULL,
  updated_at_utc TEXT NOT NULL,
  environment TEXT NOT NULL,
  broker_name TEXT NOT NULL,
  session_day_local TEXT NOT NULL,
  session_tz TEXT NOT NULL,
  realized_pnl_usd REAL NOT NULL DEFAULT 0,
  unrealized_pnl_usd REAL NOT NULL DEFAULT 0,
  max_daily_loss_usd REAL NOT NULL DEFAULT 300,
  block_new_entries INTEGER NOT NULL DEFAULT 0,
  reason TEXT,
  UNIQUE(environment, broker_name, session_day_local)
);

CREATE INDEX IF NOT EXISTS idx_risk_session_state_block
  ON risk_session_state(environment, block_new_entries, updated_at_utc DESC);

CREATE TABLE IF NOT EXISTS reprice_policy (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at_utc TEXT NOT NULL,
  updated_at_utc TEXT NOT NULL,
  environment TEXT NOT NULL,
  underlying TEXT NOT NULL DEFAULT 'SPX',
  max_attempts INTEGER NOT NULL DEFAULT 3,
  step REAL NOT NULL DEFAULT 0.05,
  interval_seconds INTEGER NOT NULL DEFAULT 25,
  max_total_concession REAL NOT NULL DEFAULT 0.15,
  enabled INTEGER NOT NULL DEFAULT 1,
  UNIQUE(environment, underlying)
);

CREATE TABLE IF NOT EXISTS audit_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at_utc TEXT NOT NULL,
  environment TEXT NOT NULL,
  actor TEXT NOT NULL,
  action TEXT NOT NULL,
  entity_type TEXT,
  entity_id TEXT,
  details_json TEXT
);

CREATE INDEX IF NOT EXISTS idx_audit_log_created
  ON audit_log(created_at_utc DESC);
