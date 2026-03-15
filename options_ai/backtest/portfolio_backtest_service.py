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


def _trade_margin_required_dollars(tr: dict[str, Any]) -> float | None:
    try:
        style = str((tr or {}).get('spread_style') or 'debit').strip().lower()
        entry = (tr or {}).get('entry_debit')
        width = (tr or {}).get('width_points')
        if entry is None:
            return None
        entry_f = float(entry)
        if style == 'credit':
            if width is None:
                return None
            width_f = float(width)
            risk_pts = max(0.0, width_f - entry_f)
            return float(risk_pts * 100.0)
        # debit default: capital outlay
        return float(max(0.0, entry_f) * 100.0)
    except Exception:
        return None


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


def combine_trades_merged_to_equity(trades_by_leg: list[list[dict[str, Any]]], *, exit_policy: str = "any_leg") -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Merged mode: treat all legs as one shared strategy lifecycle.

    Approximates shared management by allowing one open at a time across the union of leg trades.
    Open trigger = earliest next entry across all legs.
    Close trigger policy:
      - any_leg: earliest exit at/after open across all legs (legacy behavior)
      - entry_leg: earliest exit from the same leg that opened the position
    """
    recs: list[dict[str, Any]] = []
    for li, trades in enumerate(trades_by_leg):
        for t in trades or []:
            ts_e = t.get("entry_ts") or t.get("exit_ts")
            ts_x = t.get("exit_ts") or t.get("entry_ts")
            if not ts_e or not ts_x:
                continue
            dt_e = _parse_iso(str(ts_e))
            dt_x = _parse_iso(str(ts_x))
            if dt_e is None or dt_x is None:
                continue
            if dt_e.tzinfo is None:
                dt_e = dt_e.replace(tzinfo=timezone.utc)
            if dt_x.tzinfo is None:
                dt_x = dt_x.replace(tzinfo=timezone.utc)
            dt_e = dt_e.astimezone(timezone.utc)
            dt_x = dt_x.astimezone(timezone.utc)
            try:
                pnl = float(t.get("pnl_dollars") or 0.0)
            except Exception:
                pnl = 0.0
            recs.append({"leg": int(li), "entry": dt_e, "exit": dt_x, "pnl": pnl})

    recs.sort(key=lambda r: (r["entry"], r["exit"]))

    policy = str(exit_policy or "any_leg").strip().lower()
    if policy not in {"any_leg", "entry_leg"}:
        policy = "any_leg"
    if not recs:
        return [], {
            "trades": 0,
            "wins": 0,
            "losses": 0,
            "win_rate": 0.0,
            "cum_pnl_dollars": 0.0,
            "avg_pnl_dollars": 0.0,
            "max_drawdown_dollars": 0.0,
            "profit_factor": 0.0,
            "mode": "merged",
        "merge_exit_policy": policy,
        }

    synth_pnls: list[float] = []
    eq_points: list[float] = []
    eq: list[dict[str, Any]] = []
    cum = 0.0

    # walk timeline with one active position at a time
    cursor: datetime | None = None
    while True:
        open_candidates = [r for r in recs if cursor is None or r["entry"] > cursor]
        if not open_candidates:
            break
        opener = min(open_candidates, key=lambda r: (r["entry"], r["exit"]))
        open_ts = opener["entry"]

        if policy == "entry_leg":
            close_candidates = [r for r in recs if r["leg"] == opener["leg"] and r["entry"] >= open_ts and r["exit"] >= open_ts]
        else:
            close_candidates = [r for r in recs if r["entry"] >= open_ts and r["exit"] >= open_ts]
        if not close_candidates:
            break
        closer = min(close_candidates, key=lambda r: (r["exit"], r["entry"]))
        close_ts = closer["exit"]
        pnl = float(closer["pnl"])

        # Guard: ensure time cursor always advances to avoid pathological loops.
        if cursor is not None and close_ts <= cursor:
            break

        synth_pnls.append(pnl)
        cum += pnl
        eq_points.append(cum)
        eq.append({
            "ts": close_ts.isoformat(),
            "cum_pnl_dollars": float(cum),
            "entry_ts": open_ts.isoformat(),
            "close_leg": int(closer["leg"]),
            "open_leg": int(opener["leg"]),
        })
        cursor = close_ts

    wins = sum(1 for v in synth_pnls if v > 0)
    losses = sum(1 for v in synth_pnls if v < 0)
    sum_gain = sum(v for v in synth_pnls if v > 0)
    sum_loss = sum(v for v in synth_pnls if v < 0)

    # Diagnostics to explain when merged ~= independent (e.g., little/no overlap).
    candidates_total = int(len(recs))
    selected_total = int(len(synth_pnls))
    skipped_total = int(max(0, candidates_total - selected_total))

    overlap_events = 0
    if recs:
        recs_by_entry = sorted(recs, key=lambda r: (r['entry'], r['exit']))
        cur_end = recs_by_entry[0]['exit']
        for rr in recs_by_entry[1:]:
            if rr['entry'] < cur_end:
                overlap_events += 1
                if rr['exit'] > cur_end:
                    cur_end = rr['exit']
            else:
                cur_end = rr['exit']

    summary = {
        "trades": int(len(synth_pnls)),
        "wins": int(wins),
        "losses": int(losses),
        "win_rate": float(wins / len(synth_pnls)) if synth_pnls else 0.0,
        "cum_pnl_dollars": float(cum),
        "avg_pnl_dollars": float(sum(synth_pnls) / len(synth_pnls)) if synth_pnls else 0.0,
        "max_drawdown_dollars": float(_max_drawdown(eq_points) if eq_points else 0.0),
        "profit_factor": float(sum_gain / abs(sum_loss)) if sum_loss < 0 else (float("inf") if sum_gain > 0 else 0.0),
        "mode": "merged",
        "merge_exit_policy": policy,
        "merge_candidates_total": candidates_total,
        "merge_selected_trades": selected_total,
        "merge_skipped_trades": skipped_total,
        "merge_overlap_events": int(overlap_events),
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
                """
                SELECT id, status, COALESCE(cancel_requested,0) AS cancel_requested, last_activity_at_utc
                FROM portfolio_backtest_sessions
                WHERE status IN ('running','stopping')
                ORDER BY id DESC LIMIT 1
                """
            ).fetchone()
            if r is None:
                return

            sid = int(r[0])
            status = str(r[1] or 'running')
            cancel_requested = int(r[2] or 0)
            last_activity = str(r[3] or '')
            now = now_utc_iso()

            # If user already requested stop, reconcile immediately to unblock next run.
            if cancel_requested == 1 or status == 'stopping':
                con.execute(
                    "UPDATE portfolio_backtest_sessions SET status='stopped', stopped_at_utc=?, last_activity_at_utc=? WHERE id=?",
                    (now, now, sid),
                )
                con.commit()
                return

            # Stale active session safeguard (e.g., crashed worker thread)
            dt = _parse_iso(last_activity) if last_activity else None
            if dt is not None and dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            age_sec = None
            if dt is not None:
                age_sec = max(0.0, (datetime.now(timezone.utc) - dt.astimezone(timezone.utc)).total_seconds())
            if age_sec is not None and age_sec > 600.0:
                con.execute(
                    "UPDATE portfolio_backtest_sessions SET status='failed', stopped_at_utc=?, last_activity_at_utc=? WHERE id=?",
                    (now, now, sid),
                )
                con.commit()
                return

            raise HTTPException(status_code=409, detail=f"portfolio backtest already active: {sid}")

    def start(self, *, legs: list[dict[str, Any]], merge_mode: str = "independent", merge_exit_policy: str = "any_leg") -> dict[str, Any]:
        if not isinstance(legs, list) or not legs:
            raise HTTPException(status_code=400, detail="legs must be a non-empty list")
        merge_mode = str(merge_mode or "independent").strip().lower()
        if merge_mode not in {"independent", "merged"}:
            raise HTTPException(status_code=400, detail="merge_mode must be independent|merged")
        merge_exit_policy = str(merge_exit_policy or "any_leg").strip().lower()
        if merge_exit_policy not in {"any_leg", "entry_leg"}:
            raise HTTPException(status_code=400, detail="merge_exit_policy must be any_leg|entry_leg")
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
                RETURNING id
                """,
                (
                    now,
                    now,
                    json.dumps({"merge_mode": merge_mode, "merge_exit_policy": merge_exit_policy, "legs": norm_legs}, separators=(",", ":"), sort_keys=True),
                    int(len(norm_legs)),
                    now,
                ),
            )
            rr = cur.fetchone()
            session_id = int(rr[0] if not isinstance(rr, dict) else rr.get("id"))
            con.commit()

        self._spawn_worker(session_id=session_id)
        return {"session_id": session_id, "status": "running", "legs_total": len(norm_legs), "merge_mode": merge_mode, "merge_exit_policy": merge_exit_policy}

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
                    SELECT id,status,started_at_utc,stopped_at_utc,legs_total,legs_completed,legs_failed,cancel_requested,last_activity_at_utc,
                           combined_summary_json, combined_equity_json, legs_summaries_json
                    FROM portfolio_backtest_sessions
                    ORDER BY id DESC LIMIT 1
                    """
                ).fetchone()
            else:
                r = con.execute(
                    """
                    SELECT id,status,started_at_utc,stopped_at_utc,legs_total,legs_completed,legs_failed,cancel_requested,last_activity_at_utc,
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
                "started_at_utc": (str(r[2]) if r[2] is not None else None),
                "stopped_at_utc": (str(r[3]) if r[3] is not None else None),
                "legs_total": int(r[4] or 0),
                "legs_completed": int(r[5] or 0),
                "legs_failed": int(r[6] or 0),
                "cancel_requested": int(r[7] or 0),
                "last_activity_at_utc": (str(r[8]) if r[8] is not None else None),
            }

            # Attach results if finished
            if str(r[1]) in {"stopped", "failed"}:
                try:
                    out["combined_summary"] = json.loads(r[9]) if r[9] else None
                except Exception:
                    out["combined_summary"] = None
                try:
                    out["combined_equity_curve"] = json.loads(r[10]) if r[10] else []
                except Exception:
                    out["combined_equity_curve"] = []
                try:
                    out["legs_summaries"] = json.loads(r[11]) if r[11] else []
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
                payload = json.loads(r[0] or "[]")
                if isinstance(payload, dict):
                    merge_mode = str(payload.get("merge_mode") or "independent").strip().lower()
                    merge_exit_policy = str(payload.get("merge_exit_policy") or "any_leg").strip().lower()
                    legs = payload.get("legs") or []
                else:
                    merge_mode = "independent"
                    merge_exit_policy = "any_leg"
                    legs = payload

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
                    entry_count = int(sum(1 for t in (trades or []) if isinstance(t, dict) and t.get("entry_ts")))
                    exit_count = int(sum(1 for t in (trades or []) if isinstance(t, dict) and t.get("exit_ts")))
                    margins = [m for m in (_trade_margin_required_dollars(t) for t in (trades or [])) if m is not None]
                    max_margin = float(max(margins)) if margins else None
                    min_margin = float(min(margins)) if margins else None
                    legs_summaries.append({
                        "strategy_id": sid,
                        "summary": summ,
                        "params": params,
                        "entry_triggers": entry_count,
                        "exit_triggers": exit_count,
                        "max_margin_required_dollars": max_margin,
                        "min_margin_required_dollars": min_margin,
                    })
                    self._bump(session_id, completed=1)
                except Exception as e:
                    legs_summaries.append({"strategy_id": sid, "error": str(e), "params": params, "entry_triggers": 0, "exit_triggers": 0, "max_margin_required_dollars": None, "min_margin_required_dollars": None})
                    trades_by_leg.append([])
                    self._bump(session_id, failed=1)

            if str(merge_mode) == "merged":
                combined_equity, combined_summary = combine_trades_merged_to_equity(trades_by_leg, exit_policy=merge_exit_policy)
            else:
                combined_equity, combined_summary = combine_trades_to_equity(trades_by_leg)
                if isinstance(combined_summary, dict):
                    combined_summary["mode"] = "independent"
                    combined_summary.setdefault("merge_candidates_total", int(sum(len(x or []) for x in trades_by_leg)))
                    combined_summary.setdefault("merge_selected_trades", int(sum(len(x or []) for x in trades_by_leg)))
                    combined_summary.setdefault("merge_skipped_trades", 0)
                    combined_summary.setdefault("merge_overlap_events", 0)
                    combined_summary.setdefault("merge_exit_policy", "any_leg")

            if isinstance(combined_summary, dict):
                combined_summary.setdefault("line_entry_triggers_total", int(sum(int(x.get("entry_triggers") or 0) for x in legs_summaries if isinstance(x, dict))))
                combined_summary.setdefault("line_exit_triggers_total", int(sum(int(x.get("exit_triggers") or 0) for x in legs_summaries if isinstance(x, dict))))
                _mvals = [float(x.get("max_margin_required_dollars")) for x in legs_summaries if isinstance(x, dict) and x.get("max_margin_required_dollars") is not None]
                _nvals = [float(x.get("min_margin_required_dollars")) for x in legs_summaries if isinstance(x, dict) and x.get("min_margin_required_dollars") is not None]
                combined_summary.setdefault("line_max_margin_required_dollars", (max(_mvals) if _mvals else None))
                combined_summary.setdefault("line_min_margin_required_dollars", (min(_nvals) if _nvals else None))

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
