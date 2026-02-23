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

    # ensure correct unique index exists and no conflicting unique index blocks it
    idx_rows = list(conn.execute("PRAGMA index_list('predictions')"))

    has_v22_unique = False
    has_old_unique = False

    for r in idx_rows:
        name = r[1]
        is_unique = int(r[2]) == 1
        cols_idx = _index_columns(conn, name)

        if name == "uniq_predictions_hash_prompt_model" and is_unique and cols_idx == ["source_snapshot_hash", "prompt_version", "model_used"]:
            has_v22_unique = True
        # old v1.6 index
        if is_unique and cols_idx == ["source_snapshot_hash", "prompt_version"]:
            has_old_unique = True
        if is_unique and cols_idx == ["source_snapshot_hash"]:
            has_old_unique = True

    if not has_v22_unique:
        return True
    if has_old_unique:
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
