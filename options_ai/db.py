from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path


def db_path_from_url(database_url: str) -> str:
    """Parse sqlite DATABASE_URL like sqlite:////mnt/options_ai/database/predictions.db"""
    if database_url.startswith("sqlite:////"):
        return "/" + database_url[len("sqlite:////") :]
    if database_url.startswith("sqlite:///"):
        return "/" + database_url[len("sqlite:///") :]
    if database_url.startswith("sqlite:"):
        return database_url[len("sqlite:") :]
    raise ValueError(f"Unsupported DATABASE_URL: {database_url!r}")


@contextmanager
def connect(db_path: str):
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=5.0)
    try:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=5000;")
        yield conn
        conn.commit()
    finally:
        conn.close()


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    cols = set()
    for r in conn.execute(f"PRAGMA table_info({table!r})"):
        cols.add(str(r[1]))
    return cols


def _index_columns(conn: sqlite3.Connection, index_name: str) -> list[str]:
    cols = []
    for r in conn.execute(f"PRAGMA index_info({index_name!r})"):
        cols.append(str(r[2]))
    return cols


def _needs_migration_v22(conn: sqlite3.Connection) -> bool:
    cols = _table_columns(conn, "predictions")
    needed = {"model_used", "model_provider", "routing_reason"}
    if not needed.issubset(cols):
        return True

    # If older unique indexes exist they can block v2.2 idempotency.
    try:
        idx_rows = list(conn.execute("PRAGMA index_list('predictions')"))
    except Exception:
        return False

    for r in idx_rows:
        is_unique = int(r[2]) == 1
        if not is_unique:
            continue
        cols_idx = _index_columns(conn, r[1])
        if cols_idx in (["source_snapshot_hash"], ["source_snapshot_hash", "prompt_version"]):
            return True

    return False


def _migrate_to_v22(conn: sqlite3.Connection) -> None:
    # Rebuild predictions table to ensure required columns + correct unique index.
    cols = [
        "id",
        "timestamp",
        "ticker",
        "expiration_date",
        "source_snapshot_file",
        "source_snapshot_hash",
        "chart_file",
        "spot_price",
        "signals_used",
        "chart_description",
        "predicted_direction",
        "predicted_magnitude",
        "confidence",
        "strategy_suggested",
        "reasoning",
        "prompt_version",
        "model_used",
        "model_provider",
        "routing_reason",
        "price_at_prediction",
        "price_at_outcome",
        "actual_move",
        "result",
        "pnl_simulated",
        "outcome_notes",
        "scored_at",
    ]

    existing_cols = _table_columns(conn, "predictions")

    conn.execute("BEGIN")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS predictions_new (
          id INTEGER PRIMARY KEY,
          timestamp TEXT NOT NULL,
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
        """
    )

    # Build SELECT list with defaults if columns absent
    def sel(col: str, default_sql: str) -> str:
        return col if col in existing_cols else default_sql + f" AS {col}"

    select_cols = [
        sel("id", "NULL"),
        sel("timestamp", "''"),
        sel("ticker", "'SPX'"),
        sel("expiration_date", "''"),
        sel("source_snapshot_file", "''"),
        sel("source_snapshot_hash", "''"),
        sel("chart_file", "NULL"),
        sel("spot_price", "0"),
        sel("signals_used", "'{}'"),
        sel("chart_description", "NULL"),
        sel("predicted_direction", "'neutral'"),
        sel("predicted_magnitude", "0"),
        sel("confidence", "0"),
        sel("strategy_suggested", "''"),
        sel("reasoning", "''"),
        sel("prompt_version", "'unknown'"),
        sel("model_used", "'unknown'"),
        sel("model_provider", "'unknown'"),
        sel("routing_reason", "'migrated'"),
        sel("price_at_prediction", "NULL"),
        sel("price_at_outcome", "NULL"),
        sel("actual_move", "NULL"),
        sel("result", "NULL"),
        sel("pnl_simulated", "NULL"),
        sel("outcome_notes", "NULL"),
        sel("scored_at", "NULL"),
    ]

    insert_cols_sql = ",".join(cols)
    select_cols_sql = ",".join(select_cols)
    conn.execute(f"INSERT INTO predictions_new ({insert_cols_sql}) SELECT {select_cols_sql} FROM predictions")

    conn.execute("DROP TABLE predictions")
    conn.execute("ALTER TABLE predictions_new RENAME TO predictions")

    # Drop any leftover indexes that might conflict, then recreate.
    for r in conn.execute("PRAGMA index_list('predictions')"):
        name = r[1]
        if name in {"uniq_predictions_hash_prompt", "uniq_predictions_hash_prompt_model"}:
            try:
                conn.execute(f"DROP INDEX IF EXISTS {name}")
            except Exception:
                pass

    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS uniq_predictions_hash_prompt_model ON predictions(source_snapshot_hash, prompt_version, model_used)"
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_predictions_timestamp ON predictions(timestamp)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_predictions_result_null ON predictions(result)")

    conn.execute("COMMIT")


def _ensure_event_time_columns(conn: sqlite3.Connection) -> None:
    cols = _table_columns(conn, "predictions")

    if "observed_ts_utc" not in cols:
        conn.execute("ALTER TABLE predictions ADD COLUMN observed_ts_utc TEXT")
    if "outcome_ts_utc" not in cols:
        conn.execute("ALTER TABLE predictions ADD COLUMN outcome_ts_utc TEXT")

    # v2.7 ML feature storage columns
    if "features_version" not in cols:
        conn.execute("ALTER TABLE predictions ADD COLUMN features_version TEXT")
    if "features_json" not in cols:
        conn.execute("ALTER TABLE predictions ADD COLUMN features_json TEXT")

    # Best-effort backfill:
    # - observed_ts_utc defaults to legacy timestamp
    # - outcome_ts_utc defaults to timestamp + 15 minutes (legacy default)
    conn.execute("UPDATE predictions SET observed_ts_utc = COALESCE(observed_ts_utc, timestamp) WHERE observed_ts_utc IS NULL")
    conn.execute(
        "UPDATE predictions SET outcome_ts_utc = COALESCE("
        "outcome_ts_utc, (replace(datetime(observed_ts_utc, '+15 minutes'), ' ', 'T') || '+00:00')"
        " ) WHERE outcome_ts_utc IS NULL"
    )

    # Normalize any legacy SQLite datetime strings (YYYY-MM-DD HH:MM:SS) to ISO-ish UTC.
    conn.execute(
        "UPDATE predictions SET outcome_ts_utc = (replace(outcome_ts_utc, ' ', 'T') || '+00:00') "
        "WHERE outcome_ts_utc LIKE '____-__-__ __:__:__'"
    )

    conn.execute("CREATE INDEX IF NOT EXISTS idx_predictions_observed_ts_utc ON predictions(observed_ts_utc)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_predictions_outcome_ts_utc ON predictions(outcome_ts_utc)")


def init_db(db_path: str, schema_sql_path: str) -> None:
    schema_sql = Path(schema_sql_path).read_text(encoding="utf-8")
    with connect(db_path) as conn:
        conn.executescript(schema_sql)
        # Best-effort migration to v2.2
        try:
            if _needs_migration_v22(conn):
                _migrate_to_v22(conn)
        except Exception:
            pass

        # Ensure v2.2 unique index exists (safe to run even if already present).
        try:
            conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS uniq_predictions_hash_prompt_model ON predictions(source_snapshot_hash, prompt_version, model_used)"
            )
        except Exception:
            pass

        # v2.4 event-time columns + indexes
        try:
            _ensure_event_time_columns(conn)
        except Exception:
            pass

        # v2.6 model_usage telemetry table + indexes
        try:
            conn.execute(
                """
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
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_model_usage_ts_utc ON model_usage(ts_utc)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_model_usage_snapshot_hash ON model_usage(snapshot_hash)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_model_usage_kind ON model_usage(kind)")
        except Exception:
            pass
        # v2.8 eod_predictions table + indexes
        try:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS eod_predictions (
                  trade_day TEXT NOT NULL,
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
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_eod_predictions_trade_day ON eod_predictions(trade_day)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_eod_predictions_model_version ON eod_predictions(model_version)")
        except Exception:
            pass

