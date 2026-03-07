from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from options_ai.db import init_db
from options_ai.execution.intent_builder import ExecutionIntentBuilder
from options_ai.execution.executor import ExecutionExecutor, RepricePolicy
from options_ai.execution.monitor import ExecutionMonitor


class _FakeExecClient:
    def __init__(self):
        self.account_number = "ABC123"
        self.place_calls = 0
        self.replace_calls = 0
        self.oco_calls = 0

    def place_order(self, dto, *, dry_run=None):
        self.place_calls += 1
        return {"ok": True, "dry_run": bool(dry_run), "data": {"id": f"ord-{self.place_calls}"}}

    def replace_order(self, *, account_number, order_id, dto, dry_run=None):
        self.replace_calls += 1
        return {"ok": True, "dry_run": bool(dry_run), "data": {"id": str(order_id)}}

    def place_oco_exits(self, *, account_number, take_profit, stop_loss, dry_run=None):
        self.oco_calls += 1
        return {"ok": True, "dry_run": bool(dry_run), "oco_id": f"oco-{self.oco_calls}"}


class _FakeMonClient:
    def get_orders(self, *, account_number: str, status: str | None = None):
        return {"data": {"items": [{"id": "o1", "underlying": "SPX", "status": "Working"}]}}

    def get_positions(self, *, account_number: str):
        return {"data": {"items": [{"symbol": "SPXW  260101C06000000", "underlying": "SPX", "qty": 1}]}}


def _mk_db(tmp_path: Path) -> str:
    db = tmp_path / "predictions.db"
    schema = Path(__file__).resolve().parents[1] / "options_ai" / "db" / "schema.sql"
    init_db(str(db), str(schema))
    return str(db)


def _seed_backtest_run(db_path: str) -> None:
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
            "entry_start_ct": "00:00",
            "entry_end_ct": "23:59",
            "long_leg_symbol": "SPXW  260101C06000000",
            "short_leg_symbol": "SPXW  260101C06050000",
            "take_profit_pct": 1.5,
            "stop_loss_pct": 0.5,
        }
        summary = {"trades": 2, "cum_pnl_dollars": 100.0}
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


def test_signal_to_intent_to_order_and_oco(tmp_path: Path) -> None:
    db_path = _mk_db(tmp_path)
    _seed_backtest_run(db_path)

    b = ExecutionIntentBuilder(db_path=db_path, environment="sandbox", broker_name="tastytrade")
    st_b = b.build_once(limit=10)
    assert st_b["inserted"] == 1

    fc = _FakeExecClient()
    ex = ExecutionExecutor(
        db_path=db_path,
        environment="sandbox",
        broker_name="tastytrade",
        session_tz="America/Chicago",
        trading_enabled=False,
        max_daily_loss_usd=300.0,
        reprice_defaults=RepricePolicy(max_attempts=3, step=0.05, interval_seconds=1, max_total_concession=0.15),
        client=fc,
    )
    st = ex.process_once(limit=10)
    assert st["filled"] == 1
    assert fc.place_calls >= 1
    assert fc.oco_calls == 1

    con = sqlite3.connect(db_path)
    try:
        r = con.execute("SELECT status FROM execution_intents ORDER BY id DESC LIMIT 1").fetchone()
        assert r is not None and r[0] == "filled"

        tr = con.execute("SELECT status FROM trade_runs ORDER BY id DESC LIMIT 1").fetchone()
        assert tr is not None and tr[0] == "open"

        oco = con.execute(
            "SELECT event_type FROM order_events WHERE event_type='oco_armed' ORDER BY id DESC LIMIT 1"
        ).fetchone()
        assert oco is not None
    finally:
        con.close()


def test_daily_loss_breach_blocks_new_entries(tmp_path: Path) -> None:
    db_path = _mk_db(tmp_path)
    _seed_backtest_run(db_path)

    # build one intent
    ExecutionIntentBuilder(db_path=db_path, environment="sandbox", broker_name="tastytrade").build_once(limit=10)

    # insert risk session blocked row for current CT day
    con = sqlite3.connect(db_path)
    try:
        sess_day = datetime.now(timezone.utc).astimezone(ZoneInfo("America/Chicago")).date().isoformat()
        now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        con.execute(
            """
            INSERT INTO risk_session_state(
              created_at_utc, updated_at_utc, environment, broker_name,
              session_day_local, session_tz, realized_pnl_usd, unrealized_pnl_usd,
              max_daily_loss_usd, block_new_entries, reason
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
            """,
            (now, now, "sandbox", "tastytrade", sess_day, "America/Chicago", -400.0, 0.0, 300.0, 1, "daily_loss_breach"),
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
        client=_FakeExecClient(),
    )
    st = ex.process_once(limit=10)
    assert st["blocked"] == 1


def test_monitor_rest_reconcile_recovery(tmp_path: Path) -> None:
    db_path = _mk_db(tmp_path)
    con = sqlite3.connect(db_path)
    try:
        con.execute(
            """
            INSERT INTO trade_runs(
              created_at_utc, updated_at_utc, environment, broker_name,
              execution_intent_id, status, underlying, side, qty, run_payload_json
            ) VALUES(datetime('now'), datetime('now'), 'sandbox', 'tastytrade', 1, 'open', 'SPX', 'DEBIT', 1, '{}')
            """
        )
        con.commit()
    finally:
        con.close()

    mon = ExecutionMonitor(
        db_path=db_path,
        environment="sandbox",
        broker_name="tastytrade",
        account_number="ABC123",
        client=_FakeMonClient(),
    )

    st = mon.process_once(limit=20)
    assert st["order_events_written"] == 1
    assert st["position_events_written"] == 1
