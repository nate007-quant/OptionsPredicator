from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from options_ai.db import init_db
from options_ai.execution.risk_guard import RiskGuard, should_force_close_now


class _FakeClient:
    def __init__(self):
        self.closed = []
        self.cancelled = []

    def close_position(self, *, account_number: str, symbol: str, quantity: int, dry_run: bool | None = None):
        self.closed.append((account_number, symbol, quantity, bool(dry_run)))
        return {"ok": True, "action": "close_position", "symbol": symbol, "quantity": quantity, "dry_run": bool(dry_run)}

    def cancel_order(self, *, account_number: str, order_id: str, dry_run: bool | None = None):
        self.cancelled.append((account_number, order_id, bool(dry_run)))
        return {"ok": True, "action": "cancel_order", "order_id": order_id, "dry_run": bool(dry_run)}


def _mk_db(tmp_path: Path) -> str:
    db = tmp_path / "predictions.db"
    schema = Path(__file__).resolve().parents[1] / "options_ai" / "db" / "schema.sql"
    init_db(str(db), str(schema))
    return str(db)


def test_should_force_close_now_true_near_close() -> None:
    # 14:50 CT = 20:50 UTC when CT is UTC-6
    now_utc = datetime(2026, 1, 5, 20, 50, tzinfo=timezone.utc)
    assert should_force_close_now(now_utc=now_utc, session_tz="America/Chicago", force_close_minutes_before_end=15) is True


def test_should_force_close_now_false_earlier() -> None:
    # 14:30 CT
    now_utc = datetime(2026, 1, 5, 20, 30, tzinfo=timezone.utc)
    assert should_force_close_now(now_utc=now_utc, session_tz="America/Chicago", force_close_minutes_before_end=15) is False


def test_risk_guard_blocks_on_daily_loss_breach(tmp_path: Path) -> None:
    db_path = _mk_db(tmp_path)
    con = sqlite3.connect(db_path)
    try:
        con.execute(
            """
            INSERT INTO trade_runs(
              created_at_utc, updated_at_utc, environment, broker_name,
              execution_intent_id, status, underlying, side, qty,
              pnl_realized_usd, pnl_unrealized_usd, run_payload_json
            )
            VALUES(datetime('now'), datetime('now'), 'sandbox', 'tastytrade', 1, 'closed', 'SPX', 'DEBIT', 1, -350.0, 0.0, '{}')
            """
        )
        con.commit()
    finally:
        con.close()

    client = _FakeClient()
    rg = RiskGuard(
        db_path=db_path,
        environment="sandbox",
        broker_name="tastytrade",
        session_tz="America/Chicago",
        max_daily_loss_usd=300.0,
        force_close_minutes_before_end=15,
        trading_enabled=False,
        client=client,
        account_number="ABC123",
    )

    st = rg.process_once()
    assert st["updated_session"] == 1
    assert st["blocked_new_entries"] == 1

    con2 = sqlite3.connect(db_path)
    try:
        row = con2.execute(
            "SELECT block_new_entries, reason FROM risk_session_state ORDER BY id DESC LIMIT 1"
        ).fetchone()
        assert row is not None
        assert int(row[0]) == 1
        assert row[1] == "daily_loss_breach"
    finally:
        con2.close()


def test_force_close_marks_trades_closed(tmp_path: Path) -> None:
    db_path = _mk_db(tmp_path)
    con = sqlite3.connect(db_path)
    try:
        con.execute(
            """
            INSERT INTO trade_runs(
              created_at_utc, updated_at_utc, environment, broker_name,
              execution_intent_id, status, underlying, side, qty,
              exit_order_id, run_payload_json
            )
            VALUES(datetime('now'), datetime('now'), 'sandbox', 'tastytrade', 1, 'open', 'SPX', 'DEBIT', 1, 'exit-1', ?)
            """,
            (json.dumps({"params": {}}, separators=(",", ":"), sort_keys=True),),
        )
        con.commit()
    finally:
        con.close()

    client = _FakeClient()
    rg = RiskGuard(
        db_path=db_path,
        environment="sandbox",
        broker_name="tastytrade",
        session_tz="America/Chicago",
        max_daily_loss_usd=300.0,
        force_close_minutes_before_end=24 * 60,  # always true for test run
        trading_enabled=False,
        client=client,
        account_number="ABC123",
    )

    st = rg.process_once()
    assert st["force_closed"] == 1
    assert len(client.cancelled) == 1
    assert len(client.closed) == 1

    con2 = sqlite3.connect(db_path)
    try:
        tr = con2.execute("SELECT status, close_reason FROM trade_runs ORDER BY id DESC LIMIT 1").fetchone()
        assert tr is not None
        assert tr[0] == "closed"
        assert tr[1] == "FORCE_CLOSE_SESSION_END"
    finally:
        con2.close()
