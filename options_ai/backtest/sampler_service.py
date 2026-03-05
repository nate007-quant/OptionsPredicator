from __future__ import annotations

import json
import random
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from fastapi import HTTPException

from options_ai.backtest.executor import BacktestExecutor, now_utc_iso, score_summary
from options_ai.backtest.registry import StrategyRegistry, ParamSpec, canonical_json


ALLOWED_0DTE_HORIZONS = [15, 30, 45, 60, 90, 120]


@dataclass
class SamplerStatus:
    sampler_id: int
    status: str
    runs_completed: int
    duplicates_skipped: int
    runs_failed: int
    last_run_id: int | None


def _read_int(x: Any, default: int) -> int:
    try:
        return int(x)
    except Exception:
        return int(default)


def _read_float(x: Any, default: float) -> float:
    try:
        return float(x)
    except Exception:
        return float(default)


class BacktestSamplerService:
    def __init__(self, *, db_path: str, connect_fn: Any | None = None) -> None:
        self.db_path = str(db_path)
        self._connect_fn = connect_fn
        self._registry = StrategyRegistry()
        self._executor = BacktestExecutor(db_path=self.db_path, connect_fn=connect_fn)
        self._lock = threading.Lock()
        self._worker: threading.Thread | None = None

    def _connect(self):
        return self._executor._connect()

    def _has_live_worker(self) -> bool:
        return self._worker is not None and self._worker.is_alive()

    def reconcile_orphaned_sessions(self) -> int:
        """Mark DB sessions as stopped/failed if they claim to be running but there is no live worker thread.

        This primarily handles process restarts: the in-memory thread is gone, but the last session row
        would otherwise remain 'running' forever and block new sampler starts.
        """
        if self._has_live_worker():
            return 0
        now = now_utc_iso()
        with self._connect() as con:
            rows = con.execute("SELECT id, cancel_requested FROM backtest_sampler_sessions WHERE status IN ('running','stopping')").fetchall()
            if not rows:
                return 0
            for r in rows:
                sid = int(r[0])
                cancel = int(r[1] or 0)
                new_status = 'stopped' if cancel == 1 else 'failed'
                con.execute("UPDATE backtest_sampler_sessions SET status=?, stopped_at_utc=? WHERE id=?", (new_status, now, sid))
            con.commit()
            return len(rows)

    def _ensure_no_active(self) -> None:
        self.reconcile_orphaned_sessions()
        with self._connect() as con:
            r = con.execute(
                """SELECT id FROM backtest_sampler_sessions WHERE status IN ('running','stopping') ORDER BY id DESC LIMIT 1"""
            ).fetchone()
            if r is not None:
                raise HTTPException(status_code=409, detail=f"sampler already active: {int(r[0])}")

    def start(
        self,
        *,
        strategy_id: str,
        base_params: dict[str, Any],
        budget: int = 300,
        seed: int | None = None,
        search_plan: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        self._ensure_no_active()

        strat = self._registry.get(strategy_id)
        canonical_base = strat.validate_and_normalize(base_params or {}, strict=True)
        # freeze dates by construction (sampler won't touch them)
        strategy_key = strat.strategy_key(canonical_base)
        schema_version = int(getattr(strat, "schema_version", 1))

        plan = search_plan or {
            "budget": int(budget),
            "explore_frac": 0.75,
            "top_k": 12,
            "rounds": 3,
            "shrink": 0.5,
            "min_trades": 5,
        }
        plan["budget"] = int(plan.get("budget") or budget)

        now = now_utc_iso()
        with self._connect() as con:
            cur = con.execute(
                """
                INSERT INTO backtest_sampler_sessions(
                  strategy_key, schema_version, created_at_utc, started_at_utc, stopped_at_utc,
                  status, base_params_json, search_plan_json, seed,
                  runs_completed, duplicates_skipped, runs_failed, cancel_requested, last_run_id
                )
                VALUES(?,?,?,?,NULL,'running',?,?,?,0,0,0,0,NULL)
                """,
                (
                    strategy_key,
                    schema_version,
                    now,
                    now,
                    canonical_json(canonical_base),
                    json.dumps(plan, separators=(",", ":"), sort_keys=True),
                    int(seed) if seed is not None else None,
                ),
            )
            sampler_id = int(cur.lastrowid)
            con.commit()

        self._spawn_worker(sampler_id=sampler_id, strategy_id=strategy_id)
        return {"sampler_id": sampler_id, "status": "running", "strategy_key": strategy_key, "schema_version": schema_version}

    def stop(self, *, sampler_id: int) -> dict[str, Any]:
        with self._connect() as con:
            r = con.execute("SELECT id,status FROM backtest_sampler_sessions WHERE id=?", (int(sampler_id),)).fetchone()
            if not r:
                raise HTTPException(status_code=404, detail="sampler not found")
            st = str(r[1])
            if st not in {"running", "stopping"}:
                return {"sampler_id": int(sampler_id), "status": st}

            # If the worker thread isn't actually alive (e.g. service restarted), don't leave it stuck.
            if not self._has_live_worker():
                con.execute("UPDATE backtest_sampler_sessions SET cancel_requested=1, status='stopped', stopped_at_utc=? WHERE id=?", (now_utc_iso(), int(sampler_id)))
                con.commit()
                return {"sampler_id": int(sampler_id), "status": "stopped"}

            con.execute(
                "UPDATE backtest_sampler_sessions SET cancel_requested=1, status='stopping' WHERE id=?",
                (int(sampler_id),),
            )
            con.commit()
        return {"sampler_id": int(sampler_id), "status": "stopping"}

    def status(self, *, sampler_id: int | None = None) -> SamplerStatus | None:
        # If process restarted, a DB session might say running while the in-memory worker is gone.
        self.reconcile_orphaned_sessions()
        with self._connect() as con:
            if sampler_id is None:
                r = con.execute(
                    """SELECT id,status,runs_completed,duplicates_skipped,runs_failed,last_run_id
                        FROM backtest_sampler_sessions
                        ORDER BY id DESC LIMIT 1"""
                ).fetchone()
            else:
                r = con.execute(
                    """SELECT id,status,runs_completed,duplicates_skipped,runs_failed,last_run_id
                        FROM backtest_sampler_sessions WHERE id=?""",
                    (int(sampler_id),),
                ).fetchone()
            if not r:
                return None
            return SamplerStatus(
                sampler_id=int(r[0]),
                status=str(r[1]),
                runs_completed=int(r[2] or 0),
                duplicates_skipped=int(r[3] or 0),
                runs_failed=int(r[4] or 0),
                last_run_id=(int(r[5]) if r[5] is not None else None),
            )

    def refine_from_run(self, *, parent_run_id: int, budget: int = 200, rounds: int = 3, shrink: float = 0.5) -> dict[str, Any]:
        # load run + latch
        with self._connect() as con:
            rr = con.execute(
                """SELECT id,strategy_key,COALESCE(schema_version,1) AS schema_version,params_json,
                           refinement_launched, refinement_sampler_id
                    FROM backtest_runs WHERE id=?""",
                (int(parent_run_id),),
            ).fetchone()
            if not rr:
                raise HTTPException(status_code=404, detail="run not found")
            if int(rr[4] or 0) == 1:
                raise HTTPException(status_code=409, detail={"sampler_id": rr[5], "message": "refinement already launched"})
            strategy_key = str(rr[1])
            schema_version = int(rr[2] or 1)
            params_json = str(rr[3])

        # infer strategy_id from strategy_key prefix
        if not strategy_key.startswith("debit_spreads:"):
            raise HTTPException(status_code=400, detail="unsupported strategy for refinement")
        strategy_id = "debit_spreads"

        base_params = json.loads(params_json)
        plan = {
            "budget": int(budget),
            "explore_frac": 0.0,
            "top_k": 1,
            "rounds": int(rounds),
            "shrink": float(shrink),
            "min_trades": 5,
        }

        res = self.start(strategy_id=strategy_id, base_params=base_params, budget=int(budget), search_plan=plan)
        sampler_id = int(res["sampler_id"])

        # latch
        with self._connect() as con:
            con.execute(
                """
                UPDATE backtest_runs
                SET refinement_launched=1, refinement_sampler_id=?, refinement_launched_at_utc=?
                WHERE id=?
                """,
                (int(sampler_id), now_utc_iso(), int(parent_run_id)),
            )
            con.commit()

        return res

    # ---- worker ----

    def _spawn_worker(self, *, sampler_id: int, strategy_id: str) -> None:
        with self._lock:
            if self._worker is not None and self._worker.is_alive():
                # service is single-active; should be prevented by _ensure_no_active
                return
            t = threading.Thread(target=self._worker_main, args=(int(sampler_id), str(strategy_id)), daemon=True)
            self._worker = t
            t.start()

    def _session_row(self, sampler_id: int) -> dict[str, Any]:
        with self._connect() as con:
            r = con.execute(
                """SELECT id,status,base_params_json,search_plan_json,cancel_requested
                    FROM backtest_sampler_sessions WHERE id=?""",
                (int(sampler_id),),
            ).fetchone()
            if not r:
                raise RuntimeError("sampler session disappeared")
            return {"id": int(r[0]), "status": str(r[1]), "base_params_json": str(r[2]), "search_plan_json": str(r[3]), "cancel_requested": int(r[4] or 0)}

    def _bump_counter(self, sampler_id: int, *, completed: int = 0, dup: int = 0, failed: int = 0, last_run_id: int | None = None) -> None:
        sets = []
        params: list[Any] = []
        if completed:
            sets.append("runs_completed = runs_completed + ?")
            params.append(int(completed))
        if dup:
            sets.append("duplicates_skipped = duplicates_skipped + ?")
            params.append(int(dup))
        if failed:
            sets.append("runs_failed = runs_failed + ?")
            params.append(int(failed))
        if last_run_id is not None:
            sets.append("last_run_id = ?")
            params.append(int(last_run_id))
        if not sets:
            return
        params.append(int(sampler_id))
        with self._connect() as con:
            con.execute(f"UPDATE backtest_sampler_sessions SET {', '.join(sets)} WHERE id=?", tuple(params))
            con.commit()

    def _set_status(self, sampler_id: int, status: str) -> None:
        with self._connect() as con:
            con.execute(
                "UPDATE backtest_sampler_sessions SET status=?, stopped_at_utc=CASE WHEN ? IN ('stopped','failed') THEN ? ELSE stopped_at_utc END WHERE id=?",
                (str(status), str(status), now_utc_iso(), int(sampler_id)),
            )
            con.commit()

    def _worker_main(self, sampler_id: int, strategy_id: str) -> None:
        try:
            strat = self._registry.get(strategy_id)
            specs = strat.param_specs()

            sess = self._session_row(sampler_id)
            base_params = json.loads(sess["base_params_json"])
            plan = json.loads(sess["search_plan_json"])

            budget = _read_int(plan.get("budget"), 300)
            explore_frac = _read_float(plan.get("explore_frac"), 0.75)
            top_k = _read_int(plan.get("top_k"), 12)
            rounds = _read_int(plan.get("rounds"), 3)
            shrink = _read_float(plan.get("shrink"), 0.5)
            min_trades = _read_int(plan.get("min_trades"), 5)

            seed = plan.get("seed")
            rng = random.Random(int(seed) if seed is not None else None)

            sweep_specs = [s for s in specs if s.sweepable and s.typ in {"int", "float", "bool", "enum", "list_enum"}]

            results: list[dict[str, Any]] = []  # {params, score, run_id}

            def cancelled() -> bool:
                st = self._session_row(sampler_id)
                return int(st.get("cancel_requested") or 0) == 1

            def random_candidate() -> dict[str, Any]:
                cand = dict(base_params)
                for sp in sweep_specs:
                    if sp.applies_when is not None:
                        try:
                            if not bool(sp.applies_when(cand)):
                                continue
                        except Exception:
                            continue

                    if sp.typ == "bool":
                        cand[sp.key] = bool(rng.getrandbits(1))
                    elif sp.typ == "enum":
                        if sp.choices:
                            cand[sp.key] = rng.choice(list(sp.choices))
                    elif sp.typ == "list_enum":
                        if sp.choices:
                            # pick 1 or 2 choices
                            k = 1 if rng.random() < 0.65 else min(2, len(sp.choices))
                            cand[sp.key] = sorted(set(rng.sample(list(sp.choices), k=k)))
                    elif sp.typ == "int":
                        if sp.key == 'horizon_minutes' and str(cand.get('expiration_mode') or '').lower() == '0dte':
                            cand[sp.key] = rng.choice(ALLOWED_0DTE_HORIZONS)
                            continue
                        lo = int(sp.min) if sp.min is not None else 0
                        hi = int(sp.max) if sp.max is not None else lo
                        stp = int(sp.step) if sp.step is not None else 1
                        if hi < lo:
                            hi = lo
                        grid = list(range(lo, hi + 1, max(stp, 1)))
                        cand[sp.key] = rng.choice(grid) if grid else lo
                    elif sp.typ == "float":
                        lo = float(sp.min) if sp.min is not None else 0.0
                        hi = float(sp.max) if sp.max is not None else lo
                        stp = float(sp.step) if sp.step is not None else 0.0
                        if hi < lo:
                            hi = lo
                        if stp and stp > 0:
                            n = int((hi - lo) / stp) if hi > lo else 0
                            i = rng.randint(0, max(n, 0))
                            cand[sp.key] = lo + i * stp
                        else:
                            cand[sp.key] = rng.uniform(lo, hi)
                return cand

            def _snap_horizon_0dte(v: float | int) -> int:
                # snap to closest allowed horizon
                x = float(v)
                best = min(ALLOWED_0DTE_HORIZONS, key=lambda h: abs(float(h) - x))
                return int(best)

            def neighborhood(center: dict[str, Any], *, scale: float) -> list[dict[str, Any]]:
                neigh: list[dict[str, Any]] = []
                for sp in sweep_specs:
                    if not sp.refineable:
                        continue
                    if sp.typ not in {"int", "float"}:
                        continue
                    if sp.step is None:
                        continue
                    step = float(sp.step) * float(scale)
                    if step <= 0:
                        continue

                    v = center.get(sp.key)
                    if v is None:
                        continue

                    for sign in (-1.0, 1.0):
                        cand = dict(center)
                        if sp.typ == "int":
                            nv = int(round(int(v) + sign * step))
                            if sp.key == 'horizon_minutes' and str(center.get('expiration_mode') or '').lower() == '0dte':
                                nv = _snap_horizon_0dte(nv)
                            cand[sp.key] = int(nv)
                        else:
                            cand[sp.key] = float(v) + sign * step
                        neigh.append(cand)
                rng.shuffle(neigh)
                return neigh

            n_explore = int(round(budget * explore_frac))
            n_run = 0

            # Explore
            for _ in range(n_explore):
                if n_run >= budget or cancelled():
                    break
                cand = random_candidate()
                try:
                    r = self._executor.execute_and_persist(strategy_id=strategy_id, payload=cand, strict=False)
                    if r.get("duplicate_skipped"):
                        self._bump_counter(sampler_id, dup=1, last_run_id=int(r.get("run_id")))
                    else:
                        self._bump_counter(sampler_id, completed=1, last_run_id=int(r.get("run_id")))
                        sc = score_summary((r or {}).get("summary") or {}, min_trades=min_trades)
                        if sc is not None:
                            results.append({"params": (r or {}).get("config") or cand, "score": float(sc), "run_id": int(r.get("run_id"))})
                except Exception:
                    self._bump_counter(sampler_id, failed=1)
                n_run += 1

            # Refine
            results.sort(key=lambda x: float(x.get("score") or -1e18), reverse=True)
            centers = results[: max(1, int(top_k))] if results else []

            for rd in range(int(rounds)):
                if n_run >= budget or cancelled():
                    break
                scale = float(shrink) ** float(rd)
                new_centers: list[dict[str, Any]] = []
                for c in centers[: max(1, int(top_k))]:
                    if n_run >= budget or cancelled():
                        break
                    center_params = c.get("params")
                    if not isinstance(center_params, dict):
                        continue
                    for cand in neighborhood(center_params, scale=scale):
                        if n_run >= budget or cancelled():
                            break
                        try:
                            r = self._executor.execute_and_persist(strategy_id=strategy_id, payload=cand, strict=False)
                            if r.get("duplicate_skipped"):
                                self._bump_counter(sampler_id, dup=1, last_run_id=int(r.get("run_id")))
                            else:
                                self._bump_counter(sampler_id, completed=1, last_run_id=int(r.get("run_id")))
                                sc = score_summary((r or {}).get("summary") or {}, min_trades=min_trades)
                                if sc is not None:
                                    new_centers.append({"params": (r or {}).get("config") or cand, "score": float(sc), "run_id": int(r.get("run_id"))})
                        except Exception:
                            self._bump_counter(sampler_id, failed=1)
                        n_run += 1

                if new_centers:
                    new_centers.sort(key=lambda x: float(x.get("score") or -1e18), reverse=True)
                    centers = new_centers[: max(1, int(top_k))]

            if cancelled():
                self._set_status(sampler_id, "stopped")
            else:
                self._set_status(sampler_id, "stopped")

        except Exception:
            self._set_status(sampler_id, "failed")
