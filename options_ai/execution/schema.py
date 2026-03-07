from __future__ import annotations

import sqlite3


def _table_columns(con: sqlite3.Connection, table: str) -> set[str]:
    cols: set[str] = set()
    for r in con.execute(f"PRAGMA table_info({table!r})"):
        cols.add(str(r[1]))
    return cols


def _add_col_if_missing(con: sqlite3.Connection, table: str, col: str, ddl_type: str) -> None:
    cols = _table_columns(con, table)
    if col in cols:
        return
    con.execute(f"ALTER TABLE {table} ADD COLUMN {col} {ddl_type}")


def ensure_execution_hardening_schema(con: sqlite3.Connection) -> None:
    """Execution v1.1 hardening additive schema.

    Safe to call repeatedly.
    """
    # execution_intents additions
    _add_col_if_missing(con, "execution_intents", "broker_external_id", "TEXT")
    _add_col_if_missing(con, "execution_intents", "precheck_status", "TEXT")
    _add_col_if_missing(con, "execution_intents", "precheck_payload_json", "TEXT")
    _add_col_if_missing(con, "execution_intents", "risk_gate_status", "TEXT")
    _add_col_if_missing(con, "execution_intents", "quarantine_reason", "TEXT")

    # trade_runs additions
    _add_col_if_missing(con, "trade_runs", "complex_exit_order_id", "TEXT")
    _add_col_if_missing(con, "trade_runs", "protection_state", "TEXT")
    _add_col_if_missing(con, "trade_runs", "circuit_breaker_flag", "INTEGER NOT NULL DEFAULT 0")
    _add_col_if_missing(con, "trade_runs", "close_mode", "TEXT")
    _add_col_if_missing(con, "trade_runs", "degraded_protection_mode", "INTEGER NOT NULL DEFAULT 0")

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS broker_reconciliation_log (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          snapshot_ts TEXT NOT NULL,
          environment TEXT NOT NULL,
          broker_name TEXT NOT NULL,
          open_orders_json TEXT NOT NULL,
          open_positions_json TEXT NOT NULL,
          diff_json TEXT,
          resolved_bool INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_broker_reconciliation_log_snapshot ON broker_reconciliation_log(environment, broker_name, snapshot_ts DESC)"
    )

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS incident_events (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          created_at_utc TEXT NOT NULL,
          environment TEXT NOT NULL,
          broker_name TEXT NOT NULL,
          severity TEXT NOT NULL,
          incident_type TEXT NOT NULL,
          trade_run_id INTEGER,
          execution_intent_id INTEGER,
          details_json TEXT
        )
        """
    )
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_incident_events_created ON incident_events(environment, broker_name, created_at_utc DESC)"
    )
