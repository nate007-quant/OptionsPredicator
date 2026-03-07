from __future__ import annotations

import sqlite3
from pathlib import Path

import httpx
import pytest

from options_ai.brokers.tastytrade.client import TastytradeClient
from options_ai.db import init_db
from options_ai.execution.executor import ExecutionExecutor, RepricePolicy


def _mk_db(tmp_path: Path) -> str:
    db = tmp_path / "predictions.db"
    schema = Path(__file__).resolve().parents[1] / "options_ai" / "db" / "schema.sql"
    init_db(str(db), str(schema))
    return str(db)


def test_startup_reconcile_gate_blocks_without_snapshot(tmp_path: Path) -> None:
    db_path = _mk_db(tmp_path)
    ex = ExecutionExecutor(
        db_path=db_path,
        environment="sandbox",
        broker_name="tastytrade",
        session_tz="America/Chicago",
        trading_enabled=False,
        max_daily_loss_usd=300.0,
        reprice_defaults=RepricePolicy(max_attempts=1, step=0.05, interval_seconds=1, max_total_concession=0.15),
        startup_reconcile_required=True,
    )
    ok, meta = ex.startup_reconcile_ready()
    assert ok is False
    assert meta["reason"] == "no_reconciliation_snapshot"


def test_startup_reconcile_gate_allows_resolved_snapshot(tmp_path: Path) -> None:
    db_path = _mk_db(tmp_path)
    con = sqlite3.connect(db_path)
    try:
        con.execute(
            """
            INSERT INTO broker_reconciliation_log(snapshot_ts, environment, broker_name, open_orders_json, open_positions_json, diff_json, resolved_bool)
            VALUES(datetime('now'), 'sandbox', 'tastytrade', '{}', '{}', '{}', 1)
            """
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
        reprice_defaults=RepricePolicy(max_attempts=1, step=0.05, interval_seconds=1, max_total_concession=0.15),
        startup_reconcile_required=True,
    )
    ok, meta = ex.startup_reconcile_ready()
    assert ok is True
    assert meta["resolved_bool"] is True


def test_client_retry_backoff_on_429(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {"n": 0}

    def fake_request(self, method, url, headers=None, json=None, params=None):
        calls["n"] += 1
        req = httpx.Request(method, url)
        if calls["n"] < 3:
            resp = httpx.Response(status_code=429, request=req)
            raise httpx.HTTPStatusError("rate limited", request=req, response=resp)
        return httpx.Response(status_code=200, json={"ok": True}, request=req)

    monkeypatch.setattr(httpx.Client, "request", fake_request)

    c = TastytradeClient(base_url="https://api.cert.tastyworks.com", dry_run=False, http_max_retries=3, http_backoff_seconds=0)
    out = c._request("GET", "/health")
    assert out["ok"] is True
    assert calls["n"] == 3
