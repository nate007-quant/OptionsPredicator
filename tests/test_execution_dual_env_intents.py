from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from options_ai.db import init_db
from options_ai.execution.intent_builder import ExecutionIntentBuilder


def _mk_db(tmp_path: Path) -> str:
    db = tmp_path / "predictions.db"
    schema = Path(__file__).resolve().parents[1] / "options_ai" / "db" / "schema.sql"
    init_db(str(db), str(schema))
    return str(db)


def _seed_backtest_run(db_path: str, *, live_enabled: bool) -> None:
    con = sqlite3.connect(db_path)
    try:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS backtest_runs(
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              strategy_key TEXT NOT NULL,
              created_at_utc TEXT NOT NULL,
              preset_id INTEGER NULL,
              preset_name_at_run TEXT NULL,
              params_json TEXT NOT NULL,
              summary_json TEXT NOT NULL
            )
            """
        )
        params = {
            "start_day": "2026-01-01",
            "end_day": "2026-01-02",
            "entry_mode": "time_range",
            "entry_start_ct": "08:40",
            "entry_end_ct": "09:20",
            "execute_live": live_enabled,
        }
        summary = {"trades": 1, "cum_pnl_dollars": 10.0}
        con.execute(
            """
            INSERT INTO backtest_runs(strategy_key, created_at_utc, params_json, summary_json)
            VALUES(?,?,?,?)
            """,
            (
                "debit_spreads:anchor_based:exp0dte",
                "2026-03-07T00:00:00Z",
                json.dumps(params, separators=(",", ":"), sort_keys=True),
                json.dumps(summary, separators=(",", ":"), sort_keys=True),
            ),
        )
        con.commit()
    finally:
        con.close()


def test_builder_always_creates_sandbox_intent(tmp_path: Path) -> None:
    db_path = _mk_db(tmp_path)
    _seed_backtest_run(db_path, live_enabled=False)

    b = ExecutionIntentBuilder(
        db_path=db_path,
        broker_name="tastytrade",
        dual_env_enabled=True,
        live_execution_enabled=True,
    )
    st = b.build_once(limit=10)
    assert st["inserted"] == 1

    con = sqlite3.connect(db_path)
    try:
        rows = con.execute("SELECT environment, COUNT(*) FROM execution_intents GROUP BY environment ORDER BY environment").fetchall()
        assert rows == [("sandbox", 1)]
    finally:
        con.close()


def test_builder_adds_live_intent_when_param_enabled(tmp_path: Path) -> None:
    db_path = _mk_db(tmp_path)
    _seed_backtest_run(db_path, live_enabled=True)

    b = ExecutionIntentBuilder(
        db_path=db_path,
        broker_name="tastytrade",
        dual_env_enabled=True,
        live_execution_enabled=True,
    )
    st = b.build_once(limit=10)
    assert st["inserted"] == 2

    con = sqlite3.connect(db_path)
    try:
        rows = con.execute("SELECT environment, COUNT(*) FROM execution_intents GROUP BY environment ORDER BY environment").fetchall()
        assert rows == [("live", 1), ("sandbox", 1)]
    finally:
        con.close()
