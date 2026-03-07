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


def _seed_backtest_runs(db_path: str) -> None:
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
            "take_profit_pct": 1.5,
            "stop_loss_pct": 0.5,
        }
        summary = {"trades": 3, "cum_pnl_dollars": 120.0}
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


def test_intent_builder_idempotent(tmp_path: Path) -> None:
    db_path = _mk_db(tmp_path)
    _seed_backtest_runs(db_path)

    b = ExecutionIntentBuilder(db_path=db_path, environment="sandbox", broker_name="tastytrade")

    r1 = b.build_once(limit=10)
    assert r1["qualified"] == 1
    assert r1["inserted"] == 1

    r2 = b.build_once(limit=10)
    assert r2["qualified"] == 1
    assert r2["inserted"] == 0
    assert r2["existing"] == 1

    con = sqlite3.connect(db_path)
    try:
        n = con.execute("SELECT COUNT(*) FROM execution_intents").fetchone()[0]
        assert int(n) == 1

        row = con.execute("SELECT environment, broker_name, status, intent_payload_json FROM execution_intents").fetchone()
        assert row is not None
        assert row[0] == "sandbox"
        assert row[1] == "tastytrade"
        assert row[2] == "pending"

        payload = json.loads(row[3])
        ew = payload.get("entry_window_ct") or {}
        assert ew.get("first_trade_time_ct") == "08:40"
        assert ew.get("last_new_entry_time_ct") == "09:20"
        risk = payload.get("risk") or {}
        assert float(risk.get("take_profit_pct")) == 1.5
        assert float(risk.get("stop_loss")) == 0.5
    finally:
        con.close()
