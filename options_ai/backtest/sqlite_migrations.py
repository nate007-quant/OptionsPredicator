from __future__ import annotations


def migrate_backtest_schema(con) -> None:
    """Deprecated: SQLite migrations removed after Postgres cutover."""
    _ = con
    return


def backfill_params_hash(con, *, hash_fn, batch_size: int = 500) -> int:
    """Deprecated no-op compatibility shim."""
    _ = (con, hash_fn, batch_size)
    return 0
