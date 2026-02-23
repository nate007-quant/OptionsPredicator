from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path


def db_path_from_url(database_url: str) -> str:
    """Parse sqlite DATABASE_URL like sqlite:////mnt/options_ai/database/predictions.db"""
    if database_url.startswith("sqlite:////"):
        # four slashes indicates absolute path on unix
        return "/" + database_url[len("sqlite:////") :]
    if database_url.startswith("sqlite:///"):
        return "/" + database_url[len("sqlite:///") :]
    if database_url.startswith("sqlite:"):
        # sqlite:/abs/path OR sqlite:relative
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


def _index_columns(conn: sqlite3.Connection, index_name: str) -> list[str]:
    cols = []
    for r in conn.execute(f"PRAGMA index_info({index_name!r})"):
        cols.append(str(r[2]))
    return cols


def _needs_v160_migration(conn: sqlite3.Connection) -> bool:
    # If an older schema has UNIQUE(source_snapshot_hash) (as a constraint), SQLite creates
    # a unique autoindex on that single column. v1.6 requires uniqueness on (hash,prompt_version).
    try:
        idx_rows = list(conn.execute("PRAGMA index_list('predictions')"))
    except Exception:
        return False

    has_composite = False
    has_unique_single_hash = False

    for r in idx_rows:
        name = r[1]
        is_unique = int(r[2]) == 1
        cols = _index_columns(conn, name)

        if name == "uniq_predictions_hash_prompt" and is_unique and cols == ["source_snapshot_hash", "prompt_version"]:
            has_composite = True
        if is_unique and cols == ["source_snapshot_hash"]:
            has_unique_single_hash = True

    # If we already have composite unique index, but also a unique single-hash constraint,
    # reprocessing across prompt versions will still be blocked -> migrate.
    return has_unique_single_hash


def _migrate_predictions_unique_constraint(conn: sqlite3.Connection) -> None:
    # Rebuild predictions table without UNIQUE(source_snapshot_hash).
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
        "price_at_prediction",
        "price_at_outcome",
        "actual_move",
        "result",
        "pnl_simulated",
        "outcome_notes",
        "scored_at",
    ]

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

    col_sql = ",".join(cols)
    conn.execute(f"INSERT INTO predictions_new ({col_sql}) SELECT {col_sql} FROM predictions")

    conn.execute("DROP TABLE predictions")
    conn.execute("ALTER TABLE predictions_new RENAME TO predictions")

    # Recreate indexes
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS uniq_predictions_hash_prompt ON predictions(source_snapshot_hash, prompt_version)"
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_predictions_timestamp ON predictions(timestamp)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_predictions_result_null ON predictions(result)")

    conn.execute("COMMIT")


def init_db(db_path: str, schema_sql_path: str) -> None:
    schema_sql = Path(schema_sql_path).read_text(encoding="utf-8")
    with connect(db_path) as conn:
        conn.executescript(schema_sql)
        # Best-effort migration from v1.3 uniqueness (hash-only) to v1.6 (hash,prompt_version)
        try:
            if _needs_v160_migration(conn):
                _migrate_predictions_unique_constraint(conn)
        except Exception:
            # Do not crash startup if migration fails; schema.sql already applied.
            pass
