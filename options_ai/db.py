from __future__ import annotations

from contextlib import contextmanager

from options_ai.db_compat import connect_compat


def db_path_from_url(database_url: str) -> str:
    """Postgres-only runtime DB URL parser."""
    d = str(database_url or "").strip()
    if d.startswith("postgresql://") or d.startswith("postgres://"):
        return d
    raise ValueError(f"Postgres DATABASE_URL required; got: {database_url!r}")


@contextmanager
def connect(db_path: str):
    target = db_path_from_url(db_path)
    with connect_compat(target, timeout=10.0) as conn:
        yield conn


def init_db(db_path: str, schema_sql_path: str) -> None:
    """Postgres runtime: schema/migrations handled by dedicated PG paths."""
    _ = schema_sql_path
    db_path_from_url(db_path)
    return
