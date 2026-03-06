from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from fastapi import HTTPException

from options_ai.backtest.registry import StrategyRegistry


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _parse_iso(ts: str) -> datetime | None:
    try:
        return datetime.fromisoformat(str(ts))
    except Exception:
        return None


def _max_drawdown(points: list[float]) -> float:
    peak = 0.0
    mdd = 0.0
    for x in points:
        peak = max(peak, x)
        mdd = min(mdd, x - peak)
    return float(mdd)


def combine_trades_to_equity(trades_by_leg: list[list[dict[str, Any]]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    events: list[tuple[datetime, float]] = []
    pnl_vals: list[float] = []

    for trades in trades_by_leg:
        for t in trades or []:
            pnl = t.get("pnl_dollars")
            if pnl is None:
                continue
            try:
                pnl_f = float(pnl)
            except Exception:
                continue
            ts = t.get("exit_ts") or t.get("entry_ts")
            if not ts:
                continue
            dt = _parse_iso(str(ts))
            if dt is None:
                continue
            # normalize to UTC for combined timeline
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            dt_utc = dt.astimezone(timezone.utc)
            events.append((dt_utc, pnl_f))
            pnl_vals.append(pnl_f)

    events.sort(key=lambda x: x[0])

    eq: list[dict[str, Any]] = []
    cum = 0.0
    eq_points: list[float] = []
    for dt, pnl in events:
        cum += float(pnl)
        eq_points.append(cum)
        eq.append({"ts": dt.isoformat(), "cum_pnl_dollars": float(cum)})

    wins = sum(1 for v in pnl_vals if v > 0)
    losses = sum(1 for v in pnl_vals if v < 0)
    sum_gain = sum(v for v in pnl_vals if v > 0)
    sum_loss = sum(v for v in pnl_vals if v < 0)

    summary = {
        "trades": int(len(pnl_vals)),
        "wins": int(wins),
        "losses": int(losses),
        "win_rate": float(wins / len(pnl_vals)) if pnl_vals else 0.0,
        "cum_pnl_dollars": float(cum),
        "avg_pnl_dollars": float(sum(pnl_vals) / len(pnl_vals)) if pnl_vals else 0.0,
        "max_drawdown_dollars": float(_max_drawdown(eq_points) if eq_points else 0.0),
        "profit_factor": float(sum_gain / abs(sum_loss)) if sum_loss < 0 else (float("inf") if sum_gain > 0 else 0.0),
    }

    return eq, summary


@dataclass
class PortfolioStatus:
    session_id: int
    status: str
    legs_total: int
    legs_completed: int
    legs_failed: int
    cancel_requested: int
    last_activity_at_utc: str | None


class PortfolioBacktestService:
    def __init__(self, *, db_path: str, connect_fn: Any) -> None:
        self.db_path = str(db_path)
        self._connect = connect_fn
        self._registry = StrategyRegistry()
        self._lock = threading.Lock()
        self._worker: threading.Thread | None = None

    def _ensure_no_active(self) -> None:
        with self._connect(self.db_path) as con:
            r = con.execute(
                "SELECT id FROM portfolio_backtest_sessions WHERE status IN ('running','stopping') ORDER BY id DESC LIMIT 1"
            ).fetchone()
            if r is not None:
                raise HTTPException(status_code=409, detail=f"portfolio backtest already active: {int(r[0])}")

    def start(self, *, legs: list[dict[str, Any]]) -> dict[str, Any]:
        if not isinstance(legs, list) or not legs:
            raise HTTPException(status_code=400, detail="legs must be a non-empty list")
        self._ensure_no_active()

        # Basic validation
        norm_legs: list[dict[str, Any]] = []
        for leg in legs:
            if not isinstance(leg, dict):
                raise HTTPException(status_code=400, detail="each leg must be an object")
            sid = str(leg.get("strategy_id") or "").strip() or "debit_spreads"
            params = leg.get("params")
            if not isinstance(params, dict):
                raise HTTPException(status_code=400, detail="leg.params must be an object")
            # validate/normalize now to fail fast
            strat = self._registry.get(sid)
            canon = strat.validate_and_normalize(params, strict=False)
            norm_legs.append({"strategy_id": sid, "params": canon})

        now = now_utc_iso()
        with self._connect(self.db_path) as con:
            cur = con.execute(
                """
                INSERT INTO portfolio_backtest_sessions(
                  created_at_utc, started_at_utc, stopped_at_utc,
                  status, legs_json, legs_total,
                  legs_completed, legs_failed,
                  cancel_requested, last_activity_at_utc,
                  combined_summary_json, combined_equity_json, legs_summaries_json
                )
                VALUES(?, ?, NULL, 'running', ?, ?, 0, 0, 0, ?, NULL, NULL, NULL)
                """,
                (
                    now,
                    now,
                    json.dumps(norm_legs, separators=(",", ":"), sort_keys=True),
                    int(len(norm_legs)),
                    now,
                ),
            )
            session_id = int(cur.lastrowid)
            con.commit()

        self._spawn_worker(session_id=session_id)
        return {"session_id": session_id, "status": "running", "legs_total": len(norm_legs)}

    def stop(self, *, session_id: int) -> dict[str, Any]:
        with self._connect(self.db_path) as con:
            r = con.execute(
                "SELECT id,status FROM portfolio_backtest_sessions WHERE id=?",
                (int(session_id),),
            ).fetchone()
            if not r:
                raise HTTPException(status_code=404, detail="session not found")
            st = str(r[1])
            if st not in {"running", "stopping"}:
                return {"session_id": int(session_id), "status": st}
            con.execute(
                "UPDATE portfolio_backtest_sessions SET cancel_requested=1, status='stopping', last_activity_at_utc=? WHERE id=?",
                (now_utc_iso(), int(session_id)),
            )
            con.commit()
        return {"session_id": int(session_id), "status": "stopping"}

    def status(self, *, session_id: int | None = None) -> dict[str, Any] | None:
        with self._connect(self.db_path) as con:
            if session_id is None:
                r = con.execute(
                    """
                    SELECT id,status,legs_total,legs_completed,legs_failed,cancel_requested,last_activity_at_utc,
                           combined_summary_json, combined_equity_json, legs_summaries_json
                    FROM portfolio_backtest_sessions
                    ORDER BY id DESC LIMIT 1
                    """
                ).fetchone()
            else:
                r = con.execute(
                    """
                    SELECT id,status,legs_total,legs_completed,legs_failed,cancel_requested,last_activity_at_utc,
                           combined_summary_json, combined_equity_json, legs_summaries_json
                    FROM portfolio_backtest_sessions
                    WHERE id=?
                    """,
                    (int(session_id),),
                ).fetchone()
            if not r:
                return None

            out: dict[str, Any] = {
                "session_id": int(r[0]),
                "status": str(r[1]),
                "legs_total": int(r[2] or 0),
                "legs_completed": int(r[3] or 0),
                "legs_failed": int(r[4] or 0),
                "cancel_requested": int(r[5] or 0),
                "last_activity_at_utc": (str(r[6]) if r[6] is not None else None),
            }

            # Attach results if finished
            if str(r[1]) in {"stopped", "failed"}:
                try:
                    out["combined_summary"] = json.loads(r[7]) if r[7] else None
                except Exception:
                    out["combined_summary"] = None
                try:
                    out["combined_equity_curve"] = json.loads(r[8]) if r[8] else []
                except Exception:
                    out["combined_equity_curve"] = []
                try:
                    out["legs_summaries"] = json.loads(r[9]) if r[9] else []
                except Exception:
                    out["legs_summaries"] = []

            return out

    def _spawn_worker(self, *, session_id: int) -> None:
        with self._lock:
            if self._worker is not None and self._worker.is_alive():
                return
            t = threading.Thread(target=self._worker_main, args=(int(session_id),), daemon=True)
            self._worker = t
            t.start()

    def _bump(self, session_id: int, *, completed: int = 0, failed: int = 0) -> None:
        sets: list[str] = []
        params: list[Any] = []
        if completed:
            sets.append("legs_completed = legs_completed + ?")
            params.append(int(completed))
        if failed:
            sets.append("legs_failed = legs_failed + ?")
            params.append(int(failed))
        sets.append("last_activity_at_utc = ?")
        params.append(now_utc_iso())
        params.append(int(session_id))
        with self._connect(self.db_path) as con:
            con.execute(f"UPDATE portfolio_backtest_sessions SET {', '.join(sets)} WHERE id=?", tuple(params))
            con.commit()

    def _set_done(self, session_id: int, *, status: str, combined_summary: dict[str, Any] | None, combined_equity: list[dict[str, Any]] | None, legs_summaries: list[dict[str, Any]] | None) -> None:
        with self._connect(self.db_path) as con:
            con.execute(
                """
                UPDATE portfolio_backtest_sessions
                SET status=?, stopped_at_utc=?, last_activity_at_utc=?,
                    combined_summary_json=?, combined_equity_json=?, legs_summaries_json=?
                WHERE id=?
                """,
                (
                    str(status),
                    now_utc_iso(),
                    now_utc_iso(),
                    (json.dumps(combined_summary, separators=(",", ":"), sort_keys=True) if combined_summary is not None else None),
                    (json.dumps(combined_equity, separators=(",", ":"), sort_keys=True) if combined_equity is not None else None),
                    (json.dumps(legs_summaries, separators=(",", ":"), sort_keys=True) if legs_summaries is not None else None),
                    int(session_id),
                ),
            )
            con.commit()

    def _cancel_requested(self, session_id: int) -> bool:
        with self._connect(self.db_path) as con:
            r = con.execute(
                "SELECT cancel_requested FROM portfolio_backtest_sessions WHERE id=?",
                (int(session_id),),
            ).fetchone()
            return bool(r and int(r[0] or 0) == 1)

    def _worker_main(self, session_id: int) -> None:
        try:
            with self._connect(self.db_path) as con:
                r = con.execute(
                    "SELECT legs_json FROM portfolio_backtest_sessions WHERE id=?",
                    (int(session_id),),
                ).fetchone()
                if not r:
                    return
                legs = json.loads(r[0] or "[]")

            trades_by_leg: list[list[dict[str, Any]]] = []
            legs_summaries: list[dict[str, Any]] = []

            for leg in legs:
                if self._cancel_requested(session_id):
                    break

                sid = str(leg.get("strategy_id") or "debit_spreads")
                params = leg.get("params") or {}
                try:
                    strat = self._registry.get(sid)
                    res = strat.run(params)
                    summ = (res or {}).get("summary") or {}
                    trades = (res or {}).get("trades") or []
                    trades_by_leg.append(list(trades) if isinstance(trades, list) else [])
                    legs_summaries.append({"strategy_id": sid, "summary": summ, "params": params})
                    self._bump(session_id, completed=1)
                except Exception as e:
                    legs_summaries.append({"strategy_id": sid, "error": str(e), "params": params})
                    trades_by_leg.append([])
                    self._bump(session_id, failed=1)

            combined_equity, combined_summary = combine_trades_to_equity(trades_by_leg)

            # If cancelled, still mark stopped and return partial results
            status = "stopped" if self._cancel_requested(session_id) else "stopped"
            self._set_done(
                session_id,
                status=status,
                combined_summary=combined_summary,
                combined_equity=combined_equity,
                legs_summaries=legs_summaries,
            )
        except Exception:
            self._set_done(session_id, status="failed", combined_summary=None, combined_equity=None, legs_summaries=None)
