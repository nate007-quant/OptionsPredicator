from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from options_ai.db import init_db
from options_ai.execution.monitor import ExecutionMonitor


class _FakeClient:
    def get_orders(self, *, account_number: str, status: str | None = None):
        return {
            "data": {
                "items": [
                    {"id": "ord-1", "underlying": "SPX", "status": "Working"},
                    {"id": "ord-2", "underlying": "NDX", "status": "Working"},
                ]
            }
        }

    def get_positions(self, *, account_number: str):
        return {
            "data": {
                "items": [
                    {"symbol": "SPXW  260101C06000000", "underlying": "SPX", "qty": 1},
                ]
            }
        }


def _mk_db(tmp_path: Path) -> str:
    db = tmp_path / "predictions.db"
    schema = Path(__file__).resolve().parents[1] / "options_ai" / "db" / "schema.sql"
    init_db(str(db), str(schema))
    return str(db)


def _seed_open_trade(db_path: str) -> None:
    con = sqlite3.connect(db_path)
    try:
        con.execute(
            """
            INSERT INTO trade_runs(
              created_at_utc, updated_at_utc, environment, broker_name,
              execution_intent_id, status, underlying, side, qty, run_payload_json
            )
            VALUES(datetime('now'), datetime('now'), 'sandbox', 'tastytrade', 1, 'open', 'SPX', 'DEBIT', 1, ?)
            """,
            (json.dumps({"params": {}}, separators=(",", ":"), sort_keys=True),),
        )
        con.commit()
    finally:
        con.close()


def test_monitor_process_once_writes_events_and_alert(tmp_path: Path) -> None:
    db_path = _mk_db(tmp_path)
    _seed_open_trade(db_path)

    mon = ExecutionMonitor(
        db_path=db_path,
        environment="sandbox",
        broker_name="tastytrade",
        account_number="ABC123",
        client=_FakeClient(),
    )

    st = mon.process_once(limit=20)
    assert st["scanned"] == 1
    assert st["order_events_written"] == 1
    assert st["position_events_written"] == 1
    assert st["missing_protection_alerts"] == 1

    # second poll should not duplicate missing-protection alert for same trade_run
    st2 = mon.process_once(limit=20)
    assert st2["missing_protection_alerts"] == 0

    con = sqlite3.connect(db_path)
    try:
        oe = con.execute("SELECT event_type FROM order_events ORDER BY id DESC LIMIT 1").fetchone()
        assert oe is not None
        assert oe[0] == "rest_sync_orders"

        pe = con.execute("SELECT event_type FROM position_events ORDER BY id DESC LIMIT 1").fetchone()
        assert pe is not None
        assert pe[0] == "rest_sync_positions"

        al = con.execute("SELECT action FROM audit_log ORDER BY id DESC LIMIT 1").fetchone()
        assert al is not None
        assert al[0] == "missing_protective_exit_detected"
    finally:
        con.close()


def test_monitor_ingest_stream_event(tmp_path: Path) -> None:
    db_path = _mk_db(tmp_path)
    mon = ExecutionMonitor(
        db_path=db_path,
        environment="sandbox",
        broker_name="tastytrade",
        account_number="ABC123",
        client=_FakeClient(),
    )

    out = mon.ingest_stream_event({"type": "order_update", "status": "Filled", "order_id": "x1"})
    assert out["ok"] is True

    con = sqlite3.connect(db_path)
    try:
        ev = con.execute("SELECT event_type, status FROM order_events ORDER BY id DESC LIMIT 1").fetchone()
        assert ev is not None
        assert ev[0] == "stream:order_update"
        assert ev[1] == "Filled"
    finally:
        con.close()
