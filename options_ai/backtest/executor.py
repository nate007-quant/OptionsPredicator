from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from typing import Any

from fastapi import HTTPException

from options_ai.backtest.registry import StrategyRegistry, canonical_json, params_hash


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


class BacktestExecutor:
    def __init__(self, *, db_path: str, connect_fn: Any | None = None) -> None:
        self.db_path = str(db_path)
        self.registry = StrategyRegistry()
        self._connect_fn = connect_fn

    def _connect(self) -> sqlite3.Connection:
        if self._connect_fn is not None:
            return self._connect_fn(self.db_path)
        con = sqlite3.connect(self.db_path, timeout=30.0)
        con.row_factory = sqlite3.Row
        # hardening
        try:
            con.execute("PRAGMA journal_mode=WAL")
        except Exception:
            pass
        try:
            con.execute("PRAGMA busy_timeout=10000")
        except Exception:
            pass
        return con

    def execute_and_persist(
        self,
        *,
        strategy_id: str,
        payload: dict[str, Any],
        preset_id: int | None = None,
        preset_name_at_run: str | None = None,
        strict: bool = True,
        force_run: bool = False,
    ) -> dict[str, Any]:
        strat = self.registry.get(strategy_id)
        canonical_params = strat.validate_and_normalize(payload or {}, strict=strict)
        strategy_key = strat.strategy_key(canonical_params)
        schema_version = int(getattr(strat, "schema_version", 1))
        params_json = canonical_json(canonical_params)
        ph = params_hash(strategy_key=strategy_key, schema_version=schema_version, params_json_canonical=params_json)

        existing_run_id: int | None = None

        # global dedupe (unless force_run)
        with self._connect() as con:
            row = con.execute(
                """
                SELECT id, params_json, summary_json, result_json
                FROM backtest_runs
                WHERE strategy_key=? AND schema_version=? AND params_hash=?
                ORDER BY id ASC
                LIMIT 1
                """,
                (strategy_key, int(schema_version), ph),
            ).fetchone()
            if row is not None and not force_run:
                # Return existing stored result for duplicate params so UI can re-render charts/trades.
                try:
                    cfg = json.loads(row[1] or '{}')
                except Exception:
                    cfg = None
                try:
                    summ = json.loads(row[2] or '{}')
                except Exception:
                    summ = None
                try:
                    cached = json.loads(row[3] or '{}')
                except Exception:
                    cached = {}
                if not isinstance(cached, dict):
                    cached = {}
                cached.setdefault("config", cfg)
                cached.setdefault("summary", summ)
                cached.update({
                    "duplicate_skipped": True,
                    "run_id": int(row[0]),
                    "strategy_key": strategy_key,
                    "schema_version": schema_version,
                    "params_hash": ph,
                })
                return cached
            if row is not None and force_run:
                existing_run_id = int(row[0])

        # Execute backtest
        result = strat.run(canonical_params)

        summary = (result or {}).get("summary") or {}
        summary_json = json.dumps(summary, separators=(",", ":"), sort_keys=True)
        result_json = json.dumps((result or {}), separators=(",", ":"), sort_keys=True)

        now = now_utc_iso()

        with self._connect() as con:
            if existing_run_id is not None:
                # Force rerun: keep the same run_id, but update stored summary and timestamp
                con.execute(
                    """
                    UPDATE backtest_runs
                    SET created_at_utc=?, preset_id=?, preset_name_at_run=?, summary_json=?, result_json=?
                    WHERE id=?
                    """,
                    (
                        now,
                        int(preset_id) if preset_id is not None else None,
                        preset_name_at_run,
                        summary_json,
                        result_json,
                        int(existing_run_id),
                    ),
                )
                run_id = int(existing_run_id)
            else:
                cur = con.execute(
                    """
                    INSERT INTO backtest_runs(
                        strategy_key, created_at_utc, preset_id, preset_name_at_run,
                        params_json, summary_json, result_json,
                        schema_version, params_hash,
                        refinement_launched, refinement_sampler_id, refinement_launched_at_utc
                    )
                    VALUES(?,?,?,?,?,?,?,?,?,0,NULL,NULL)
                    """,
                    (
                        strategy_key,
                        now,
                        int(preset_id) if preset_id is not None else None,
                        preset_name_at_run,
                        params_json,
                        summary_json,
                        result_json,
                        int(schema_version),
                        ph,
                    ),
                )
                run_id = int(cur.lastrowid)
            con.commit()

        if isinstance(result, dict):
            result["run_id"] = run_id
            result["preset_id"] = preset_id
            result["duplicate_skipped"] = (existing_run_id is not None)
            result["forced_rerun"] = bool(existing_run_id is not None)
            result["strategy_key"] = strategy_key
            result["schema_version"] = schema_version
            result["params_hash"] = ph
        return result


def score_summary(summary: dict[str, Any], *, min_trades: int = 5) -> float | None:
    try:
        trades = int(summary.get("trades") or 0)
        if trades < int(min_trades):
            return None
        pnl = float(summary.get("cum_pnl_dollars") or 0.0)
        pf = float(summary.get("profit_factor") or 0.0)
        if pf != pf or pf <= 0:
            return None
        # cap profit factor to avoid inf
        pf_capped = min(pf, 10.0)
        import math

        return pnl + 250.0 * math.log(pf_capped)
    except Exception:
        return None
