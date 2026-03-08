from __future__ import annotations

import sqlite3
from typing import Any


def _has_column(con: sqlite3.Connection, table: str, col: str) -> bool:
    rows = con.execute(f"PRAGMA table_info({table})").fetchall()
    return any(r[1] == col for r in rows)


def _ensure_index(con: sqlite3.Connection, sql: str) -> None:
    con.execute(sql)


def migrate_backtest_schema(con: sqlite3.Connection) -> None:
    # Existing tables should already exist from legacy startup; keep CREATE IF NOT EXISTS for safety.
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS backtest_runs (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          strategy_key TEXT NOT NULL,
          created_at_utc TEXT NOT NULL,
          preset_id INTEGER NULL,
          preset_name_at_run TEXT NULL,
          params_json TEXT NOT NULL,
          summary_json TEXT NOT NULL,
          result_json TEXT NULL
        );
        """
    )

    # Add required columns (idempotent)
    alters: list[str] = []
    if not _has_column(con, "backtest_runs", "schema_version"):
        alters.append("ALTER TABLE backtest_runs ADD COLUMN schema_version INTEGER NOT NULL DEFAULT 1")
    if not _has_column(con, "backtest_runs", "params_hash"):
        alters.append("ALTER TABLE backtest_runs ADD COLUMN params_hash TEXT NULL")
    if not _has_column(con, "backtest_runs", "refinement_launched"):
        alters.append("ALTER TABLE backtest_runs ADD COLUMN refinement_launched INTEGER NOT NULL DEFAULT 0")
    if not _has_column(con, "backtest_runs", "refinement_sampler_id"):
        alters.append("ALTER TABLE backtest_runs ADD COLUMN refinement_sampler_id INTEGER NULL")
    if not _has_column(con, "backtest_runs", "refinement_launched_at_utc"):
        alters.append("ALTER TABLE backtest_runs ADD COLUMN refinement_launched_at_utc TEXT NULL")
    if not _has_column(con, "backtest_runs", "result_json"):
        alters.append("ALTER TABLE backtest_runs ADD COLUMN result_json TEXT NULL")

    for sql in alters:
        con.execute(sql)

    _ensure_index(
        con,
        """
        CREATE INDEX IF NOT EXISTS idx_backtest_runs_dedupe
        ON backtest_runs(strategy_key, schema_version, params_hash)
        """,
    )

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS backtest_sampler_sessions (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          strategy_key TEXT NOT NULL,
          schema_version INTEGER NOT NULL,
          created_at_utc TEXT NOT NULL,
          started_at_utc TEXT NULL,
          stopped_at_utc TEXT NULL,
          status TEXT NOT NULL,
          base_params_json TEXT NOT NULL,
          search_plan_json TEXT NOT NULL,
          seed INTEGER NULL,
          runs_completed INTEGER NOT NULL DEFAULT 0,
          duplicates_skipped INTEGER NOT NULL DEFAULT 0,
          runs_failed INTEGER NOT NULL DEFAULT 0,
          cancel_requested INTEGER NOT NULL DEFAULT 0,
          last_run_id INTEGER NULL
        );
        """
    )

    # Add new sampler session columns (idempotent)
    sampler_alters: list[str] = []
    if not _has_column(con, "backtest_sampler_sessions", "precheck_rejected"):
        sampler_alters.append("ALTER TABLE backtest_sampler_sessions ADD COLUMN precheck_rejected INTEGER NOT NULL DEFAULT 0")
    if not _has_column(con, "backtest_sampler_sessions", "last_activity_at_utc"):
        sampler_alters.append("ALTER TABLE backtest_sampler_sessions ADD COLUMN last_activity_at_utc TEXT NULL")
    for sql in sampler_alters:
        con.execute(sql)

    _ensure_index(
        con,
        """
        CREATE INDEX IF NOT EXISTS idx_sampler_sessions_status
        ON backtest_sampler_sessions(status, created_at_utc DESC)
        """,
    )



    # Portfolio group runs (run multiple saved portfolios)
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS portfolio_group_runs (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          created_at_utc TEXT NOT NULL,
          started_at_utc TEXT NULL,
          stopped_at_utc TEXT NULL,
          status TEXT NOT NULL,
          portfolio_ids_json TEXT NOT NULL,
          portfolios_total INTEGER NOT NULL,
          portfolios_completed INTEGER NOT NULL DEFAULT 0,
          portfolios_failed INTEGER NOT NULL DEFAULT 0,
          cancel_requested INTEGER NOT NULL DEFAULT 0,
          last_activity_at_utc TEXT NULL,
          group_summary_json TEXT NULL,
          group_equity_json TEXT NULL
        );
        """
    )

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS portfolio_group_run_portfolios (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          group_run_id INTEGER NOT NULL,
          portfolio_id INTEGER NOT NULL,
          portfolio_name TEXT NOT NULL,
          status TEXT NOT NULL,
          error TEXT NULL,
          combined_summary_json TEXT NULL,
          combined_equity_json TEXT NULL,
          legs_summaries_json TEXT NULL
        );
        """
    )

    _ensure_index(
        con,
        """
        CREATE INDEX IF NOT EXISTS idx_portfolio_group_runs_status
        ON portfolio_group_runs(status, created_at_utc DESC)
        """,
    )

    _ensure_index(
        con,
        """
        CREATE INDEX IF NOT EXISTS idx_portfolio_group_run_portfolios
        ON portfolio_group_run_portfolios(group_run_id, portfolio_id)
        """,
    )
    # Portfolio backtest sessions
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS portfolio_backtest_sessions (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          created_at_utc TEXT NOT NULL,
          started_at_utc TEXT NULL,
          stopped_at_utc TEXT NULL,
          status TEXT NOT NULL,
          legs_json TEXT NOT NULL,
          legs_total INTEGER NOT NULL,
          legs_completed INTEGER NOT NULL DEFAULT 0,
          legs_failed INTEGER NOT NULL DEFAULT 0,
          cancel_requested INTEGER NOT NULL DEFAULT 0,
          last_activity_at_utc TEXT NULL,
          combined_summary_json TEXT NULL,
          combined_equity_json TEXT NULL,
          legs_summaries_json TEXT NULL
        );
        """
    )

    _ensure_index(
        con,
        """
        CREATE INDEX IF NOT EXISTS idx_portfolio_sessions_status
        ON portfolio_backtest_sessions(status, created_at_utc DESC)
        """,
    )

    # Portfolio definitions (named collections of legs)
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS portfolio_defs (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          name TEXT NOT NULL UNIQUE,
          legs_json TEXT NOT NULL,
          created_at_utc TEXT NOT NULL,
          updated_at_utc TEXT NOT NULL
        );
        """
    )

    _ensure_index(
        con,
        """
        CREATE INDEX IF NOT EXISTS idx_portfolio_defs_updated
        ON portfolio_defs(updated_at_utc DESC)
        """,
    )

    # Portfolio definitions: execution mode toggle (independent|merged)
    if not _has_column(con, "portfolio_defs", "execution_mode"):
        con.execute("ALTER TABLE portfolio_defs ADD COLUMN execution_mode TEXT NOT NULL DEFAULT 'independent'")
        con.commit()



def backfill_params_hash(
    con: sqlite3.Connection,
    *,
    hash_fn: Any,
    batch_size: int = 500,
) -> int:
    # hash_fn(row_strategy_key, row_schema_version, params_json_text) -> str
    n = 0
    while True:
        rows = con.execute(
            """
            SELECT id, strategy_key, COALESCE(schema_version,1) AS schema_version, params_json
            FROM backtest_runs
            WHERE params_hash IS NULL
            ORDER BY id ASC
            LIMIT ?
            """,
            (int(batch_size),),
        ).fetchall()
        if not rows:
            break
        for r in rows:
            rid = int(r[0])
            strategy_key = str(r[1])
            schema_version = int(r[2] or 1)
            params_json = str(r[3] or "{}")
            ph = str(hash_fn(strategy_key, schema_version, params_json))
            con.execute("UPDATE backtest_runs SET params_hash=? WHERE id=?", (ph, rid))
            n += 1
        con.commit()
    return n
