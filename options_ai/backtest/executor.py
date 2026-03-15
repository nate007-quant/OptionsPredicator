from __future__ import annotations

import json
import sqlite3
import threading
import time
from datetime import datetime, timezone
from typing import Any

try:
    import psycopg
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover
    psycopg = None  # type: ignore
    dict_row = None  # type: ignore

from options_ai.backtest.registry import StrategyRegistry, canonical_json, params_hash


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


class BacktestExecutor:
    def __init__(
        self,
        *,
        db_path: str,
        connect_fn: Any | None = None,
        pg_dsn: str | None = None,
    ) -> None:
        self.db_path = str(db_path)
        self.registry = StrategyRegistry()
        self._connect_fn = connect_fn
        self._lock = threading.RLock()
        self.pg_dsn = (pg_dsn or "").strip() or None

    def _connect(self) -> sqlite3.Connection:
        if self._connect_fn is not None:
            return self._connect_fn(self.db_path)
        con = sqlite3.connect(self.db_path, timeout=120.0)
        con.row_factory = sqlite3.Row
        try:
            con.execute("PRAGMA journal_mode=WAL")
        except Exception:
            pass
        try:
            con.execute("PRAGMA busy_timeout=60000")
        except Exception:
            pass
        return con

    def _pg_connect(self):
        if not self.pg_dsn:
            raise RuntimeError("pg_dsn not configured")
        if psycopg is None:
            raise RuntimeError("psycopg not installed")
        return psycopg.connect(self.pg_dsn, row_factory=dict_row)

    def _serialized(self, fn):
        with self._lock:
            return fn()

    def _with_retry(self, fn, *, retries: int = 12, base_sleep: float = 0.2):
        last = None
        for i in range(max(1, int(retries))):
            try:
                return fn()
            except Exception as e:
                msg = str(e).lower()
                retriable = ("locked" in msg) or ("busy" in msg) or ("could not serialize" in msg)
                if not retriable:
                    raise
                last = e
                time.sleep(float(base_sleep) * (i + 1))
        if last is not None:
            raise last
        raise RuntimeError("retry exhausted")

    def _execute_and_persist_sqlite(
        self,
        *,
        strategy_id: str,
        payload: dict[str, Any],
        preset_id: int | None,
        preset_name_at_run: str | None,
        strict: bool,
        force_run: bool,
    ) -> dict[str, Any]:
        strat = self.registry.get(strategy_id)
        canonical_params = strat.validate_and_normalize(payload or {}, strict=strict)
        strategy_key = strat.strategy_key(canonical_params)
        schema_version = int(getattr(strat, "schema_version", 1))
        params_json = canonical_json(canonical_params)
        ph = params_hash(strategy_key=strategy_key, schema_version=schema_version, params_json_canonical=params_json)

        def _read_existing():
            with self._connect() as con:
                return con.execute(
                    """
                    SELECT id, params_json, summary_json, result_json
                    FROM backtest_runs
                    WHERE strategy_key=? AND schema_version=? AND params_hash=?
                    ORDER BY id ASC
                    LIMIT 1
                    """,
                    (strategy_key, int(schema_version), ph),
                ).fetchone()

        row = self._with_retry(lambda: self._serialized(_read_existing))
        existing_run_id: int | None = int(row[0]) if row is not None else None

        if row is not None and not force_run:
            try:
                cfg = json.loads(row[1] or "{}")
            except Exception:
                cfg = None
            try:
                summ = json.loads(row[2] or "{}")
            except Exception:
                summ = None
            try:
                cached = json.loads(row[3] or "{}")
            except Exception:
                cached = {}
            if not isinstance(cached, dict):
                cached = {}
            cached.setdefault("config", cfg)
            cached.setdefault("summary", summ)
            cached.update(
                {
                    "duplicate_skipped": True,
                    "forced_rerun": False,
                    "run_id": int(row[0]),
                    "strategy_key": strategy_key,
                    "schema_version": schema_version,
                    "params_hash": ph,
                }
            )
            return cached

        result = self._serialized(lambda: strat.run(canonical_params))
        summary = (result or {}).get("summary") or {}
        summary_json = json.dumps(summary, separators=(",", ":"), sort_keys=True)
        result_json = json.dumps((result or {}), separators=(",", ":"), sort_keys=True)
        now = now_utc_iso()

        def _write_run() -> int:
            with self._connect() as con:
                if existing_run_id is not None:
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
                    rid = int(existing_run_id)
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
                    rid = int(cur.lastrowid)
                con.commit()
                return rid

        run_id: int | None = None
        persist_error: str | None = None
        try:
            run_id = int(self._with_retry(lambda: self._serialized(_write_run)))
        except Exception as e:
            if "locked" in str(e).lower() or "busy" in str(e).lower():
                persist_error = str(e)
                run_id = int(existing_run_id) if existing_run_id is not None else None
            else:
                raise

        if isinstance(result, dict):
            forced = bool(force_run and existing_run_id is not None)
            result["run_id"] = run_id
            result["preset_id"] = preset_id
            result["duplicate_skipped"] = False
            result["forced_rerun"] = forced
            result["strategy_key"] = strategy_key
            result["schema_version"] = schema_version
            result["params_hash"] = ph
            if persist_error:
                result["persistence_warning"] = "sqlite_locked_persist_skipped"
                result["persistence_error"] = persist_error
        return result

    def _execute_and_persist_pg(
        self,
        *,
        strategy_id: str,
        payload: dict[str, Any],
        preset_id: int | None,
        preset_name_at_run: str | None,
        strict: bool,
        force_run: bool,
    ) -> dict[str, Any]:
        strat = self.registry.get(strategy_id)
        canonical_params = strat.validate_and_normalize(payload or {}, strict=strict)
        strategy_key = strat.strategy_key(canonical_params)
        schema_version = int(getattr(strat, "schema_version", 1))
        params_json = canonical_json(canonical_params)
        ph = params_hash(strategy_key=strategy_key, schema_version=schema_version, params_json_canonical=params_json)

        def _read_existing():
            with self._pg_connect() as con:
                with con.cursor() as cur:
                    cur.execute(
                        """
                        SELECT id, params_json, summary_json, result_json
                        FROM backtest_runs
                        WHERE strategy_key=%s AND schema_version=%s AND params_hash=%s
                        ORDER BY id ASC
                        LIMIT 1
                        """,
                        (strategy_key, int(schema_version), ph),
                    )
                    return cur.fetchone()

        row = self._with_retry(_read_existing)
        existing_run_id: int | None = int(row["id"]) if row is not None else None

        if row is not None and not force_run:
            try:
                cfg = json.loads(row.get("params_json") or "{}")
            except Exception:
                cfg = None
            try:
                summ = json.loads(row.get("summary_json") or "{}")
            except Exception:
                summ = None
            try:
                cached = json.loads(row.get("result_json") or "{}")
            except Exception:
                cached = {}
            if not isinstance(cached, dict):
                cached = {}
            cached.setdefault("config", cfg)
            cached.setdefault("summary", summ)
            cached.update(
                {
                    "duplicate_skipped": True,
                    "forced_rerun": False,
                    "run_id": int(row["id"]),
                    "strategy_key": strategy_key,
                    "schema_version": schema_version,
                    "params_hash": ph,
                }
            )
            return cached

        result = strat.run(canonical_params)
        summary = (result or {}).get("summary") or {}
        summary_json = json.dumps(summary, separators=(",", ":"), sort_keys=True)
        result_json = json.dumps((result or {}), separators=(",", ":"), sort_keys=True)
        now = now_utc_iso()

        def _write_run() -> int:
            with self._pg_connect() as con:
                with con.cursor() as cur:
                    if existing_run_id is not None:
                        cur.execute(
                            """
                            UPDATE backtest_runs
                            SET created_at_utc=%s, preset_id=%s, preset_name_at_run=%s, summary_json=%s, result_json=%s
                            WHERE id=%s
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
                        rid = int(existing_run_id)
                    else:
                        cur.execute(
                            """
                            INSERT INTO backtest_runs(
                                strategy_key, created_at_utc, preset_id, preset_name_at_run,
                                params_json, summary_json, result_json,
                                schema_version, params_hash,
                                refinement_launched, refinement_sampler_id, refinement_launched_at_utc
                            )
                            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,0,NULL,NULL)
                            RETURNING id
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
                        _r = cur.fetchone()
                        rid = int(_r["id"] if isinstance(_r, dict) else _r[0])
                con.commit()
                return rid

        run_id = int(self._with_retry(_write_run))
        if isinstance(result, dict):
            forced = bool(force_run and existing_run_id is not None)
            result["run_id"] = run_id
            result["preset_id"] = preset_id
            result["duplicate_skipped"] = False
            result["forced_rerun"] = forced
            result["strategy_key"] = strategy_key
            result["schema_version"] = schema_version
            result["params_hash"] = ph
        return result

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
        if self.pg_dsn:
            return self._execute_and_persist_pg(
                strategy_id=strategy_id,
                payload=payload,
                preset_id=preset_id,
                preset_name_at_run=preset_name_at_run,
                strict=strict,
                force_run=force_run,
            )
        return self._execute_and_persist_sqlite(
            strategy_id=strategy_id,
            payload=payload,
            preset_id=preset_id,
            preset_name_at_run=preset_name_at_run,
            strict=strict,
            force_run=force_run,
        )


def score_summary(summary: dict[str, Any], *, min_trades: int = 5) -> float | None:
    try:
        trades = int(summary.get("trades") or 0)
        if trades < int(min_trades):
            return None
        pnl = float(summary.get("cum_pnl_dollars") or 0.0)
        pf = float(summary.get("profit_factor") or 0.0)
        if pf != pf or pf <= 0:
            return None
        pf_capped = min(pf, 10.0)
        import math

        return pnl + 250.0 * math.log(pf_capped)
    except Exception:
        return None
