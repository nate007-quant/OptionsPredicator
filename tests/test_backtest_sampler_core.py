from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from options_ai.backtest.executor import BacktestExecutor
from options_ai.backtest.registry import params_hash
from options_ai.backtest.sqlite_migrations import migrate_backtest_schema
from options_ai.backtest.sampler_service import BacktestSamplerService


def _connect_tmp(db_path: str) -> sqlite3.Connection:
    con = sqlite3.connect(db_path, timeout=30.0)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=10000")
    return con


def test_params_hash_stable() -> None:
    ph1 = params_hash(strategy_key="k", schema_version=1, params_json_canonical='{"a":1,"b":2}')
    ph2 = params_hash(strategy_key="k", schema_version=1, params_json_canonical='{"a":1,"b":2}')
    assert ph1 == ph2


def test_executor_global_dedupe_skip(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db = tmp_path / "t.db"
    with _connect_tmp(str(db)) as con:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS backtest_presets(
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              strategy_key TEXT NOT NULL,
              name TEXT NOT NULL,
              params_json TEXT NOT NULL,
              schema_version INTEGER NOT NULL DEFAULT 1,
              created_at_utc TEXT NOT NULL,
              updated_at_utc TEXT NOT NULL,
              last_run_id INTEGER NULL,
              last_run_at_utc TEXT NULL,
              last_summary_json TEXT NULL,
              UNIQUE(strategy_key, name)
            );
            """
        )
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
            );
            """
        )
        migrate_backtest_schema(con)
        con.commit()

    ex = BacktestExecutor(db_path=str(db), connect_fn=_connect_tmp)
    strat = ex.registry.get("debit_spreads")

    # stub out the Timescale runner
    def _fake_run(canon: dict) -> dict:
        return {"summary": {"trades": 10, "cum_pnl_dollars": 1.0, "profit_factor": 1.1}}

    monkeypatch.setattr(strat, "run", _fake_run)

    payload = {
        "start_day": "2026-01-01",
        "end_day": "2026-01-02",
        "expiration_mode": "0dte",
        "horizon_minutes": 30,
        "entry_mode": "time_range",
        "strategy_mode": "anchor_based",
        "anchor_mode": "ATM",
        "anchor_policy": "any",
        "allowed_spreads": ["PUT", "CALL"],
    }

    r1 = ex.execute_and_persist(strategy_id="debit_spreads", payload=payload, strict=True)
    assert r1.get("duplicate_skipped") is False
    run_id_1 = int(r1["run_id"])

    r2 = ex.execute_and_persist(strategy_id="debit_spreads", payload=payload, strict=True)
    assert r2.get("duplicate_skipped") is True
    assert int(r2["run_id"]) == run_id_1


def test_schema_version_bump_allows_rerun(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db = tmp_path / "t.db"
    with _connect_tmp(str(db)) as con:
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
            );
            """
        )
        migrate_backtest_schema(con)
        con.commit()

    ex = BacktestExecutor(db_path=str(db), connect_fn=_connect_tmp)
    strat = ex.registry.get("debit_spreads")

    def _fake_run(_canon: dict) -> dict:
        return {"summary": {"trades": 10, "cum_pnl_dollars": 1.0, "profit_factor": 1.1}}

    monkeypatch.setattr(strat, "run", _fake_run)

    payload = {
        "start_day": "2026-01-01",
        "end_day": "2026-01-02",
        "expiration_mode": "0dte",
        "entry_mode": "time_range",
        "strategy_mode": "anchor_based",
    }

    r1 = ex.execute_and_persist(strategy_id="debit_spreads", payload=payload, strict=True)
    assert r1.get("duplicate_skipped") is False

    # bump schema_version on strategy instance
    monkeypatch.setattr(strat, "schema_version", 2)

    r2 = ex.execute_and_persist(strategy_id="debit_spreads", payload=payload, strict=True)
    assert r2.get("duplicate_skipped") is False
    assert int(r2["schema_version"]) == 2


def test_refine_idempotency_latch(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db = tmp_path / "t.db"
    with _connect_tmp(str(db)) as con:
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
            );
            """
        )
        migrate_backtest_schema(con)
        # seed one run
        params_json = json.dumps({"start_day": "2026-01-01", "end_day": "2026-01-02", "expiration_mode": "0dte", "entry_mode": "time_range", "strategy_mode": "anchor_based"}, separators=(",", ":"), sort_keys=True)
        con.execute(
            """INSERT INTO backtest_runs(strategy_key, created_at_utc, params_json, summary_json, schema_version, params_hash, refinement_launched)
                VALUES('debit_spreads:anchor_based:exp0dte','2026-01-01T00:00:00Z',?,?,1,'x',0)""",
            (params_json, "{}"),
        )
        run_id = int(con.execute("SELECT id FROM backtest_runs").fetchone()[0])
        con.commit()

    svc = BacktestSamplerService(db_path=str(db), connect_fn=_connect_tmp)

    # Prevent worker thread from running in this unit test
    monkeypatch.setattr(svc, "_spawn_worker", lambda **kwargs: None)

    r1 = svc.refine_from_run(parent_run_id=run_id, budget=1, rounds=1, shrink=0.5)
    assert int(r1["sampler_id"]) > 0

    with pytest.raises(Exception):
        svc.refine_from_run(parent_run_id=run_id, budget=1, rounds=1, shrink=0.5)
