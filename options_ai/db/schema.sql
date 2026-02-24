-- Options AI v2.4 schema

PRAGMA journal_mode=WAL;
PRAGMA busy_timeout=5000;

CREATE TABLE IF NOT EXISTS predictions (
  id INTEGER PRIMARY KEY,
  timestamp TEXT NOT NULL, -- ISO-8601 UTC (legacy)

  -- v2.4 event-time fields (ISO-8601 UTC, no micros)
  observed_ts_utc TEXT,
  outcome_ts_utc TEXT,

  ticker TEXT NOT NULL,
  expiration_date TEXT NOT NULL,
  source_snapshot_file TEXT NOT NULL,
  source_snapshot_hash TEXT NOT NULL,
  chart_file TEXT,
  spot_price REAL NOT NULL,
  signals_used TEXT NOT NULL,
  chart_description TEXT,
  predicted_direction TEXT NOT NULL,
  predicted_magnitude REAL NOT NULL,
  confidence REAL NOT NULL,
  strategy_suggested TEXT NOT NULL,
  reasoning TEXT NOT NULL,
  prompt_version TEXT NOT NULL,

  -- v2.2 routing
  model_used TEXT NOT NULL,
  model_provider TEXT NOT NULL,
  routing_reason TEXT NOT NULL,

  price_at_prediction REAL,
  price_at_outcome REAL,
  actual_move REAL,
  result TEXT,
  pnl_simulated REAL,
  outcome_notes TEXT,
  scored_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_predictions_timestamp ON predictions(timestamp);
CREATE INDEX IF NOT EXISTS idx_predictions_observed_ts_utc ON predictions(observed_ts_utc);
CREATE INDEX IF NOT EXISTS idx_predictions_outcome_ts_utc ON predictions(outcome_ts_utc);
CREATE INDEX IF NOT EXISTS idx_predictions_result_null ON predictions(result);

CREATE TABLE IF NOT EXISTS performance_summary (
  id INTEGER PRIMARY KEY,
  generated_at TEXT NOT NULL,
  total_predictions INTEGER NOT NULL,
  total_scored INTEGER NOT NULL,
  overall_accuracy REAL,
  summary_json TEXT NOT NULL
);

-- v2.3: store ERROR/CRITICAL system events (for postmortem/debug)
CREATE TABLE IF NOT EXISTS system_events (
  id INTEGER PRIMARY KEY,
  timestamp TEXT NOT NULL,
  level TEXT NOT NULL,
  component TEXT NOT NULL,
  event TEXT NOT NULL,
  message TEXT NOT NULL,
  snapshot_hash TEXT,
  model_used TEXT,
  details_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_system_events_timestamp ON system_events(timestamp);
CREATE INDEX IF NOT EXISTS idx_system_events_level ON system_events(level);
