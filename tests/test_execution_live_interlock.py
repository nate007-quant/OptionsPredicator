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


def _insert_intent(db_path: str) -> None:
    payload = {
        "source": {"type": "backtest_run", "id": 1},
        "strategy_key": "debit_spreads:anchor_based:exp0dte",
        "symbol": "SPX",
        "params": {
            "entry_mode": "time_range",
            "entry_start_ct": "00:00",
            "entry_end_ct": "23:59",
            "long_leg_symbol": "SPXW  260101C06000000",
            "short_leg_symbol": "SPXW  260101C06050000",
        },
        "entry_window_ct": {"first_trade_time_ct": "00:00", "last_new_entry_time_ct": "23:59"},
        "risk": {},
    }
    con = sqlite3.connect(db_path)
    try:
        con.execute(
            """
            INSERT INTO execution_intents(
              created_at_utc, updated_at_utc, environment, broker_name,
              status, strategy_key, symbol, candidate_ref, idempotency_key, intent_payload_json, error
            ) VALUES(datetime('now'), datetime('now'), 'live', 'tastytrade', 'pending', ?, 'SPX', 't', 'k-live', ?, NULL)
            """,
            (
                "debit_spreads:anchor_based:exp0dte",
                json.dumps(payload, separators=(",", ":"), sort_keys=True),
            ),
        )
        con.commit()
    finally:
        con.close()


def test_live_interlock_quarantines_when_not_armed(tmp_path: Path) -> None:
    db_path = _mk_db(tmp_path)
    _insert_intent(db_path)

    ex = ExecutionExecutor(
        db_path=db_path,
        environment="live",
        broker_name="tastytrade",
        session_tz="America/Chicago",
        trading_enabled=True,
        max_daily_loss_usd=300.0,
        reprice_defaults=RepricePolicy(max_attempts=1, step=0.05, interval_seconds=1, max_total_concession=0.15),
        live_armed=False,
    )

    st = ex.process_once(limit=10)
    assert st["quarantined"] == 1

    con = sqlite3.connect(db_path)
    try:
        r = con.execute("SELECT status, quarantine_reason FROM execution_intents WHERE idempotency_key='k-live'").fetchone()
        assert r is not None
        assert r[0] == "QUARANTINED"
        assert r[1] == "live_not_armed"
    finally:
        con.close()
