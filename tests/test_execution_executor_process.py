from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from options_ai.db import init_db
from options_ai.execution.executor import ExecutionExecutor, RepricePolicy


def _mk_db(tmp_path: Path) -> str:
    db = tmp_path / "predictions.db"
    schema = Path(__file__).resolve().parents[1] / "options_ai" / "db" / "schema.sql"
    init_db(str(db), str(schema))
    return str(db)


def test_process_once_rejects_missing_symbols(tmp_path: Path) -> None:
    db_path = _mk_db(tmp_path)
    con = sqlite3.connect(db_path)
    try:
        payload = {
            "source": {"type": "backtest_run", "id": 1},
            "strategy_key": "debit_spreads:anchor_based:exp0dte",
            "symbol": "SPX",
            "params": {
                "entry_mode": "time_range",
                "entry_start_ct": "00:00",
                "entry_end_ct": "23:59",
                # intentionally no long/short symbol keys
            },
            "entry_window_ct": {"first_trade_time_ct": "00:00", "last_new_entry_time_ct": "23:59"},
            "risk": {"take_profit_pct": 1.0, "stop_loss": 0.5},
        }
        con.execute(
            """
            INSERT INTO execution_intents(
              created_at_utc, updated_at_utc, environment, broker_name,
              status, strategy_key, symbol, candidate_ref, idempotency_key, intent_payload_json, error
            )
            VALUES(datetime('now'), datetime('now'), 'sandbox', 'tastytrade', 'pending', ?, 'SPX', 'test', 'k1', ?, NULL)
            """,
            (
                "debit_spreads:anchor_based:exp0dte",
                json.dumps(payload, separators=(",", ":"), sort_keys=True),
            ),
        )
        con.commit()
    finally:
        con.close()

    ex = ExecutionExecutor(
        db_path=db_path,
        environment="sandbox",
        broker_name="tastytrade",
        session_tz="America/Chicago",
        trading_enabled=False,
        max_daily_loss_usd=300.0,
        reprice_defaults=RepricePolicy(max_attempts=3, step=0.05, interval_seconds=1, max_total_concession=0.15),
    )

    st = ex.process_once(limit=10)
    assert st["rejected"] == 1

    con2 = sqlite3.connect(db_path)
    try:
        row = con2.execute("SELECT status, error FROM execution_intents WHERE idempotency_key='k1'").fetchone()
        assert row is not None
        assert row[0] == "rejected"
        assert "missing long/short" in (row[1] or "")

        ev = con2.execute("SELECT event_type, status FROM order_events ORDER BY id DESC LIMIT 1").fetchone()
        assert ev is not None
        assert ev[0] == "validation_failed"
        assert ev[1] == "rejected"
    finally:
        con2.close()
