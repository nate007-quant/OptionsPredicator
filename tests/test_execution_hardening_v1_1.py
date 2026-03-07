from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from options_ai.db import init_db
from options_ai.execution.executor import ExecutionExecutor, RepricePolicy
from options_ai.execution.monitor import ExecutionMonitor
from options_ai.execution.risk_guard import RiskGuard


def _mk_db(tmp_path: Path) -> str:
    db = tmp_path / "predictions.db"
    schema = Path(__file__).resolve().parents[1] / "options_ai" / "db" / "schema.sql"
    init_db(str(db), str(schema))
    return str(db)


def _insert_intent(db_path: str, *, idem: str, qty: int = 1) -> None:
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
            "take_profit_pct": 1.5,
            "stop_loss_pct": 0.5,
            "qty": qty,
        },
        "entry_window_ct": {"first_trade_time_ct": "00:00", "last_new_entry_time_ct": "23:59"},
        "risk": {"take_profit_pct": 1.5, "stop_loss": 0.5},
    }
    con = sqlite3.connect(db_path)
    try:
        con.execute(
            """
            INSERT INTO execution_intents(
              created_at_utc, updated_at_utc, environment, broker_name,
              status, strategy_key, symbol, candidate_ref, idempotency_key, intent_payload_json, error
            ) VALUES(datetime('now'), datetime('now'), 'sandbox', 'tastytrade', 'pending', ?, 'SPX', 't', ?, ?, NULL)
            """,
            (
                "debit_spreads:anchor_based:exp0dte",
                idem,
                json.dumps(payload, separators=(",", ":"), sort_keys=True),
            ),
        )
        con.commit()
    finally:
        con.close()


class _WarnReconfirmClient:
    def __init__(self):
        self.account_number = "ABC123"
        self.oco_qty = None

    def place_order(self, dto, *, dry_run=None):
        return {"ok": True, "data": {"id": "ord-init"}}

    def place_order_with_warning_reconfirm(self, dto, *, dry_run=None):
        return {
            "initial": {"warning": "reconfirm needed"},
            "reconfirm": {"ok": True, "data": {"id": "ord-reconfirm", "filled_quantity": 2}},
        }

    def replace_order(self, *, account_number, order_id, dto, dry_run=None):
        return {"ok": True, "data": {"id": order_id}}

    def place_oco_exits(self, *, account_number, take_profit, stop_loss, dry_run=None):
        self.oco_qty = int(take_profit.quantity)
        return {"ok": True, "data": {"id": "cx-1"}}

    def submit_complex_order(self, *, account_number, payload, dry_run=None):
        self.oco_qty = int((payload.get("orders") or [{}])[0].get("qty") or 0)
        return {"ok": True, "data": {"id": "cx-1"}}

    def close_position(self, *, account_number, symbol, quantity, dry_run=None):
        return {"ok": True}


class _MonMismatchClient:
    def get_orders(self, *, account_number: str, status: str | None = None):
        return {"data": {"items": [{"id": "o1", "underlying": "SPX"}]}}

    def get_positions(self, *, account_number: str):
        # mismatch: empty positions while open trades exist
        return {"data": {"items": []}}


class _RGClient:
    def __init__(self):
        self.cancel_complex = 0

    def cancel_complex_order(self, *, account_number: str, complex_order_id: str, dry_run: bool | None = None):
        self.cancel_complex += 1
        return {"ok": True}

    def close_position(self, *, account_number: str, symbol: str, quantity: int, dry_run: bool | None = None):
        return {"ok": True}


def test_warning_reconfirm_and_partial_fill_protection_qty(tmp_path: Path) -> None:
    db_path = _mk_db(tmp_path)
    _insert_intent(db_path, idem="k-warn", qty=3)

    fc = _WarnReconfirmClient()
    ex = ExecutionExecutor(
        db_path=db_path,
        environment="sandbox",
        broker_name="tastytrade",
        session_tz="America/Chicago",
        trading_enabled=False,
        max_daily_loss_usd=300.0,
        reprice_defaults=RepricePolicy(max_attempts=1, step=0.05, interval_seconds=1, max_total_concession=0.15),
        client=fc,
        require_complex_exit_orders=True,
    )

    st = ex.process_once(limit=10)
    assert st["filled"] == 1
    assert fc.oco_qty == 2  # partial fill quantity used for protection

    con = sqlite3.connect(db_path)
    try:
        tr = con.execute("SELECT protection_state FROM trade_runs ORDER BY id DESC LIMIT 1").fetchone()
        assert tr is not None
        assert tr[0] == "partial"

        ie = con.execute(
            "SELECT status FROM execution_intents WHERE idempotency_key='k-warn'"
        ).fetchone()
        assert ie is not None
        assert ie[0] == "filled"
    finally:
        con.close()


def test_stream_mismatch_enters_close_only_block(tmp_path: Path) -> None:
    db_path = _mk_db(tmp_path)
    con = sqlite3.connect(db_path)
    try:
        con.execute(
            """
            INSERT INTO trade_runs(created_at_utc, updated_at_utc, environment, broker_name, execution_intent_id, status, underlying, side, qty, run_payload_json)
            VALUES(datetime('now'), datetime('now'), 'sandbox', 'tastytrade', 1, 'open', 'SPX', 'DEBIT', 1, '{}')
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
        client=_MonMismatchClient(),
    )
    mon.process_once(limit=20)

    con2 = sqlite3.connect(db_path)
    try:
        rs = con2.execute("SELECT block_new_entries, reason FROM risk_session_state ORDER BY id DESC LIMIT 1").fetchone()
        assert rs is not None
        assert int(rs[0]) == 1
        assert rs[1] in ("position_mismatch_close_only", "stream_down_breaker")
    finally:
        con2.close()


def test_complex_cancel_path_used_in_force_close(tmp_path: Path) -> None:
    db_path = _mk_db(tmp_path)
    con = sqlite3.connect(db_path)
    try:
        con.execute(
            """
            INSERT INTO trade_runs(
              created_at_utc, updated_at_utc, environment, broker_name,
              execution_intent_id, status, underlying, side, qty,
              complex_exit_order_id, run_payload_json
            ) VALUES(datetime('now'), datetime('now'), 'sandbox', 'tastytrade', 1, 'open', 'SPX', 'DEBIT', 1, 'cx-123', '{}')
            """
        )
        con.commit()
    finally:
        con.close()

    rc = _RGClient()
    rg = RiskGuard(
        db_path=db_path,
        environment="sandbox",
        broker_name="tastytrade",
        session_tz="America/Chicago",
        max_daily_loss_usd=300.0,
        force_close_minutes_before_end=24 * 60,
        trading_enabled=False,
        client=rc,
        account_number="ABC123",
    )
    st = rg.process_once()
    assert st["force_closed"] == 1
    assert rc.cancel_complex == 1
