-- Options AI v2.8 schema

PRAGMA journal_mode=WAL;
PRAGMA busy_timeout=5000;

CREATE TABLE IF NOT EXISTS predictions (
  id INTEGER PRIMARY KEY,
  timestamp TEXT NOT NULL,
  observed_ts_utc TEXT,
  outcome_ts_utc TEXT,
  features_version TEXT,
  features_json TEXT,
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
CREATE INDEX IF NOT EXISTS idx_predictions_result_null ON predictions(result);

CREATE TABLE IF NOT EXISTS performance_summary (
  id INTEGER PRIMARY KEY,
  generated_at TEXT NOT NULL,
  total_predictions INTEGER NOT NULL,
  total_scored INTEGER NOT NULL,
  overall_accuracy REAL,
  summary_json TEXT NOT NULL
);

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

CREATE TABLE IF NOT EXISTS model_usage (
  id INTEGER PRIMARY KEY,
  ts_utc TEXT NOT NULL,
  observed_ts_utc TEXT,
  snapshot_hash TEXT,
  kind TEXT NOT NULL,
  model_used TEXT,
  model_provider TEXT,
  prompt_chars INTEGER,
  output_chars INTEGER,
  latency_ms INTEGER,
  input_tokens INTEGER,
  output_tokens INTEGER,
  total_tokens INTEGER,
  est_input_tokens INTEGER NOT NULL,
  est_output_tokens INTEGER NOT NULL,
  est_total_tokens INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_model_usage_ts_utc ON model_usage(ts_utc);
CREATE INDEX IF NOT EXISTS idx_model_usage_snapshot_hash ON model_usage(snapshot_hash);
CREATE INDEX IF NOT EXISTS idx_model_usage_kind ON model_usage(kind);

-- ML EOD (Early 60m -> cash close)
CREATE TABLE IF NOT EXISTS eod_predictions (
  trade_day TEXT NOT NULL, -- YYYY-MM-DD Central
  asof_minutes INTEGER NOT NULL,
  levels_asof_snapshot_index INTEGER NOT NULL,
  model_version TEXT NOT NULL,

  created_at_utc TEXT NOT NULL,

  open_price REAL,
  early_end_price REAL,
  close_price REAL,

  levels_json TEXT,

  features_version TEXT,
  features_json TEXT,

  pred_dir TEXT,
  pred_conf REAL,
  pred_move_pts REAL,
  p_action REAL,
  event_probs_json TEXT,

  label_dir TEXT,
  label_move_pts REAL,
  label_band_pts REAL,
  label_events_json TEXT,
  scored_at TEXT,

  PRIMARY KEY (trade_day, asof_minutes, levels_asof_snapshot_index, model_version)
);

CREATE INDEX IF NOT EXISTS idx_eod_predictions_trade_day ON eod_predictions(trade_day);
CREATE INDEX IF NOT EXISTS idx_eod_predictions_model_version ON eod_predictions(model_version);
