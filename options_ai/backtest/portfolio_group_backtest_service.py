from __future__ import annotations

import json
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from fastapi import HTTPException

from options_ai.backtest.portfolio_backtest_service import combine_trades_to_equity
from options_ai.backtest.registry import StrategyRegistry


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


@dataclass
class GroupStatus:
    run_id: int
    status: str
    portfolios_total: int
    portfolios_completed: int
    portfolios_failed: int
    cancel_requested: int
    last_activity_at_utc: str | None


class PortfolioGroupBacktestService:
    """Runs multiple saved portfolios (portfolio_defs) and combines the results.

    Notes:
      - Equal-weight, additive PnL only (sum trade PnL events by exit_ts).
      - Runs portfolios sequentially for safety.
    """

    def __init__(self, *, db_path: str, connect_fn: Any) -> None:
        self.db_path = str(db_path)
        self._connect = connect_fn
        self._registry = StrategyRegistry()
        self._lock = threading.Lock()
        self._worker: threading.Thread | None = None

    def _ensure_no_active(self) -> None:
        with self._connect(self.db_path) as con:
            r = con.execute(
                "SELECT id FROM portfolio_group_runs WHERE status IN ('running','stopping') ORDER BY id DESC LIMIT 1"
            ).fetchone()
            if r is not None:
                raise HTTPException(status_code=409, detail=f"portfolio group run already active: {int(r[0])}")

    def start(self, *, portfolio_ids: list[int]) -> dict[str, Any]:
        if not isinstance(portfolio_ids, list) or not portfolio_ids:
            raise HTTPException(status_code=400, detail="portfolio_ids must be a non-empty list")
        ids = [int(x) for x in portfolio_ids]
        ids = list(dict.fromkeys(ids))
        self._ensure_no_active()

        # Load definitions (fail fast if any missing)
        defs: list[dict[str, Any]] = []
        with self._connect(self.db_path) as con:
            for pid in ids:
                r = con.execute("SELECT id,name,legs_json FROM portfolio_defs WHERE id=?", (int(pid),)).fetchone()
                if not r:
                    raise HTTPException(status_code=404, detail=f"portfolio not found: {pid}")
                try:
                    legs = json.loads(r[2] or "[]")
                except Exception:
                    legs = []
                if not isinstance(legs, list):
                    legs = []
                defs.append({"id": int(r[0]), "name": str(r[1]), "legs": legs})

        now = now_utc_iso()
        with self._connect(self.db_path) as con:
            cur = con.execute(
                """
                INSERT INTO portfolio_group_runs(
                  created_at_utc, started_at_utc, stopped_at_utc,
                  status, portfolio_ids_json,
                  portfolios_total, portfolios_completed, portfolios_failed,
                  cancel_requested, last_activity_at_utc,
                  group_summary_json, group_equity_json
                )
                VALUES(?, ?, NULL, 'running', ?, ?, 0, 0, 0, ?, NULL, NULL)
                """,
                (
                    now,
                    now,
                    json.dumps(ids, separators=(",", ":"), sort_keys=True),
                    int(len(ids)),
                    now,
                ),
            )
            run_id = int(cur.lastrowid)
            # seed child rows
            for d in defs:
                con.execute(
                    """
                    INSERT INTO portfolio_group_run_portfolios(
                      group_run_id, portfolio_id, portfolio_name,
                      status, error,
                      combined_summary_json, combined_equity_json, legs_summaries_json
                    )
                    VALUES(?,?,?,'queued',NULL,NULL,NULL,NULL)
                    """,
                    (int(run_id), int(d["id"]), str(d["name"])),
                )
            con.commit()

        self._spawn_worker(run_id=run_id)
        return {"run_id": run_id, "status": "running", "portfolios_total": len(ids)}

    def stop(self, *, run_id: int) -> dict[str, Any]:
        with self._connect(self.db_path) as con:
            r = con.execute("SELECT id,status FROM portfolio_group_runs WHERE id=?", (int(run_id),)).fetchone()
            if not r:
                raise HTTPException(status_code=404, detail="run not found")
            st = str(r[1])
            if st not in {"running", "stopping"}:
                return {"run_id": int(run_id), "status": st}
            con.execute(
                "UPDATE portfolio_group_runs SET cancel_requested=1, status='stopping', last_activity_at_utc=? WHERE id=?",
                (now_utc_iso(), int(run_id)),
            )
            con.commit()
        return {"run_id": int(run_id), "status": "stopping"}

    def status(self, *, run_id: int | None = None) -> dict[str, Any] | None:
        with self._connect(self.db_path) as con:
            if run_id is None:
                r = con.execute(
                    """
                    SELECT id,status,portfolios_total,portfolios_completed,portfolios_failed,cancel_requested,last_activity_at_utc,
                           group_summary_json, group_equity_json
                    FROM portfolio_group_runs
                    ORDER BY id DESC LIMIT 1
                    """
                ).fetchone()
            else:
                r = con.execute(
                    """
                    SELECT id,status,portfolios_total,portfolios_completed,portfolios_failed,cancel_requested,last_activity_at_utc,
                           group_summary_json, group_equity_json
                    FROM portfolio_group_runs
                    WHERE id=?
                    """ ,
                    (int(run_id),),
                ).fetchone()
            if not r:
                return None

            out: dict[str, Any] = {
                "run_id": int(r[0]),
                "status": str(r[1]),
                "portfolios_total": int(r[2] or 0),
                "portfolios_completed": int(r[3] or 0),
                "portfolios_failed": int(r[4] or 0),
                "cancel_requested": int(r[5] or 0),
                "last_activity_at_utc": (str(r[6]) if r[6] is not None else None),
            }

            if str(r[1]) in {"stopped", "failed"}:
                try:
                    out["group_summary"] = json.loads(r[7]) if r[7] else None
                except Exception:
                    out["group_summary"] = None
                try:
                    out["group_equity_curve"] = json.loads(r[8]) if r[8] else []
                except Exception:
                    out["group_equity_curve"] = []

                rows = con.execute(
                    """
                    SELECT portfolio_id, portfolio_name, status, error, combined_summary_json
                    FROM portfolio_group_run_portfolios
                    WHERE group_run_id=?
                    ORDER BY id ASC
                    """,
                    (int(out["run_id"]),),
                ).fetchall()
                items = []
                for rr in rows:
                    try:
                        summ = json.loads(rr[4]) if rr[4] else None
                    except Exception:
                        summ = None
                    items.append(
                        {
                            "portfolio_id": int(rr[0]),
                            "portfolio_name": str(rr[1]),
                            "status": str(rr[2]),
                            "error": (str(rr[3]) if rr[3] is not None else None),
                            "combined_summary": summ,
                        }
                    )
                out["portfolios"] = items

            return out

    # ---- worker ----

    def _spawn_worker(self, *, run_id: int) -> None:
        with self._lock:
            if self._worker is not None and self._worker.is_alive():
                return
            t = threading.Thread(target=self._worker_main, args=(int(run_id),), daemon=True)
            self._worker = t
            t.start()

    def _cancel_requested(self, run_id: int) -> bool:
        with self._connect(self.db_path) as con:
            r = con.execute("SELECT cancel_requested FROM portfolio_group_runs WHERE id=?", (int(run_id),)).fetchone()
            return bool(r and int(r[0] or 0) == 1)

    def _bump(self, run_id: int, *, completed: int = 0, failed: int = 0) -> None:
        sets: list[str] = []
        params: list[Any] = []
        if completed:
            sets.append("portfolios_completed = portfolios_completed + ?")
            params.append(int(completed))
        if failed:
            sets.append("portfolios_failed = portfolios_failed + ?")
            params.append(int(failed))
        sets.append("last_activity_at_utc = ?")
        params.append(now_utc_iso())
        params.append(int(run_id))
        with self._connect(self.db_path) as con:
            con.execute(f"UPDATE portfolio_group_runs SET {', '.join(sets)} WHERE id=?", tuple(params))
            con.commit()

    def _set_done(self, run_id: int, *, status: str, group_summary: dict[str, Any] | None, group_equity: list[dict[str, Any]] | None) -> None:
        with self._connect(self.db_path) as con:
            con.execute(
                """
                UPDATE portfolio_group_runs
                SET status=?, stopped_at_utc=?, last_activity_at_utc=?,
                    group_summary_json=?, group_equity_json=?
                WHERE id=?
                """,
                (
                    str(status),
                    now_utc_iso(),
                    now_utc_iso(),
                    (json.dumps(group_summary, separators=(",", ":"), sort_keys=True) if group_summary is not None else None),
                    (json.dumps(group_equity, separators=(",", ":"), sort_keys=True) if group_equity is not None else None),
                    int(run_id),
                ),
            )
            con.commit()

    def _update_portfolio_row(
        self,
        run_id: int,
        portfolio_id: int,
        *,
        status: str,
        error: str | None,
        combined_summary: dict[str, Any] | None,
        combined_equity: list[dict[str, Any]] | None,
        legs_summaries: list[dict[str, Any]] | None,
    ) -> None:
        with self._connect(self.db_path) as con:
            con.execute(
                """
                UPDATE portfolio_group_run_portfolios
                SET status=?, error=?,
                    combined_summary_json=?, combined_equity_json=?, legs_summaries_json=?
                WHERE group_run_id=? AND portfolio_id=?
                """,
                (
                    str(status),
                    (str(error) if error is not None else None),
                    (json.dumps(combined_summary, separators=(",", ":"), sort_keys=True) if combined_summary is not None else None),
                    (json.dumps(combined_equity, separators=(",", ":"), sort_keys=True) if combined_equity is not None else None),
                    (json.dumps(legs_summaries, separators=(",", ":"), sort_keys=True) if legs_summaries is not None else None),
                    int(run_id),
                    int(portfolio_id),
                ),
            )
            con.commit()

    def _worker_main(self, run_id: int) -> None:
        try:
            with self._connect(self.db_path) as con:
                rr = con.execute("SELECT portfolio_ids_json FROM portfolio_group_runs WHERE id=?", (int(run_id),)).fetchone()
                if not rr:
                    return
                ids = json.loads(rr[0] or "[]")

            all_trades_by_leg: list[list[dict[str, Any]]] = []

            for pid in ids:
                if self._cancel_requested(run_id):
                    break

                # mark running
                self._update_portfolio_row(run_id, int(pid), status="running", error=None, combined_summary=None, combined_equity=None, legs_summaries=None)

                try:
                    with self._connect(self.db_path) as con:
                        r = con.execute("SELECT name, legs_json FROM portfolio_defs WHERE id=?", (int(pid),)).fetchone()
                        if not r:
                            raise RuntimeError(f"portfolio not found: {pid}")
                        name = str(r[0])
                        legs = json.loads(r[1] or "[]")

                    trades_by_leg: list[list[dict[str, Any]]] = []
                    legs_summaries: list[dict[str, Any]] = []

                    for leg in (legs or []):
                        if self._cancel_requested(run_id):
                            break
                        sid = str((leg or {}).get("strategy_id") or "debit_spreads")
                        params = (leg or {}).get("params") or {}
                        strat = self._registry.get(sid)
                        canon = strat.validate_and_normalize(params, strict=False)
                        res = strat.run(canon)
                        summ = (res or {}).get("summary") or {}
                        trades = (res or {}).get("trades") or []
                        trades_by_leg.append(list(trades) if isinstance(trades, list) else [])
                        legs_summaries.append({"strategy_id": sid, "summary": summ, "params": canon})

                    combined_equity, combined_summary = combine_trades_to_equity(trades_by_leg)
                    # store
                    self._update_portfolio_row(
                        run_id,
                        int(pid),
                        status="stopped",
                        error=None,
                        combined_summary=combined_summary,
                        combined_equity=combined_equity,
                        legs_summaries=legs_summaries,
                    )
                    self._bump(run_id, completed=1)

                    # for group combine
                    all_trades_by_leg.extend(trades_by_leg)
                except Exception as e:
                    self._update_portfolio_row(run_id, int(pid), status="failed", error=str(e), combined_summary=None, combined_equity=None, legs_summaries=None)
                    self._bump(run_id, failed=1)

            group_equity, group_summary = combine_trades_to_equity(all_trades_by_leg)
            self._set_done(run_id, status="stopped" if not self._cancel_requested(run_id) else "stopped", group_summary=group_summary, group_equity=group_equity)
        except Exception:
            self._set_done(run_id, status="failed", group_summary=None, group_equity=None)
