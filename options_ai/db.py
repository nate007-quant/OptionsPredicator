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


def init_db(db_path: str, schema_sql_path: str) -> None:
    schema_sql = Path(schema_sql_path).read_text(encoding="utf-8")
    with connect(db_path) as conn:
        conn.executescript(schema_sql)
