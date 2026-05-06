#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _log(msg: str, **kw: Any) -> None:
    payload = {"ts": _now_utc().isoformat(), "msg": msg, **kw}
    print(json.dumps(payload, separators=(",", ":"), sort_keys=True), flush=True)


def _api_base() -> str:
    return str(os.getenv("DASHBOARD_API_BASE", "http://127.0.0.1:8088")).rstrip("/")


def _json_req(method: str, path: str, body: dict[str, Any] | None = None, timeout: int = 30) -> Any:
    url = f"{_api_base()}{path}"
    data = None
    headers = {"Content-Type": "application/json"}
    if body is not None:
        data = json.dumps(body, separators=(",", ":")).encode("utf-8")
    req = Request(url=url, method=method.upper(), data=data, headers=headers)
    try:
        with urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            return json.loads(raw) if raw else {}
    except HTTPError as e:
        raw = e.read().decode("utf-8", errors="replace") if hasattr(e, "read") else ""
        raise RuntimeError(f"HTTP {e.code} {method} {path}: {raw[:500]}")
    except URLError as e:
        raise RuntimeError(f"URL error {method} {path}: {e}")


def _systemctl(action: str, unit: str) -> tuple[int, str]:
    cmd = ["sudo", "-n", "systemctl", action, unit]
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    return int(p.returncode), str(p.stdout or "").strip()


def _is_active(unit: str) -> bool:
    code, _ = _systemctl("is-active", unit)
    return code == 0


def _ensure_services_started(units: list[str]) -> list[str]:
    started: list[str] = []
    for u in units:
        if _is_active(u):
            _log("service_already_active", service=u)
            continue
        code, out = _systemctl("start", u)
        if code != 0:
            raise RuntimeError(f"failed to start {u}: {out}")
        started.append(u)
        _log("service_started", service=u)
    return started


def _stop_services(units: list[str]) -> None:
    for u in reversed(units):
        code, out = _systemctl("stop", u)
        if code != 0:
            _log("service_stop_failed", service=u, error=out)
        else:
            _log("service_stopped", service=u)


def _wait_for_health(max_wait_sec: int = 90) -> None:
    t0 = time.time()
    while (time.time() - t0) < max_wait_sec:
        try:
            d = _json_req("GET", "/api/health", timeout=8)
            if bool(d.get("ok")):
                _log("api_healthy")
                return
        except Exception:
            pass
        time.sleep(2)
    raise RuntimeError("dashboard api health check timed out")


def _latest_data_day() -> str | None:
    dsn = str(os.getenv("SPX_CHAIN_DATABASE_URL", "")).strip()
    if not dsn:
        return None
    try:
        import psycopg  # type: ignore

        sql = """
            SELECT max(d)::text
            FROM (
              SELECT max(snapshot_ts)::date AS d FROM spx.debit_spread_scores_0dte
              UNION ALL
              SELECT max(snapshot_ts)::date AS d FROM spx.debit_spread_scores_term
            ) q
        """
        with psycopg.connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                r = cur.fetchone()
                if not r or not r[0]:
                    return None
                return str(r[0])
    except Exception as e:
        _log("latest_data_day_query_failed", error=str(e))
        return None


@dataclass
class RunResult:
    portfolio_id: int
    name: str
    status: str
    detail: str = ""
    session_id: int | None = None


def _poll_session(session_id: int, timeout_sec: int) -> dict[str, Any]:
    t0 = time.time()
    while (time.time() - t0) < timeout_sec:
        st = _json_req("GET", f"/api/portfolio_backtest/status?session_id={int(session_id)}", timeout=25)
        s = str(st.get("status") or "").lower()
        if s in {"stopped", "failed"}:
            return st
        time.sleep(15)
    raise TimeoutError(f"session {session_id} timed out after {timeout_sec}s")


def _run_one_group(group: dict[str, Any], latest_day: str, timeout_sec: int) -> RunResult:
    pid = int(group.get("id"))
    name = str(group.get("name") or f"Group {pid}")

    # Refresh group end-day to newest available data day.
    _json_req("PUT", f"/api/portfolios/{pid}", body={"group_end_day": latest_day}, timeout=30)
    _log("group_end_day_updated", portfolio_id=pid, group_end_day=latest_day)

    # Re-read group so we use latest persisted settings/legs.
    g = _json_req("GET", f"/api/portfolios/{pid}", timeout=30)
    legs = list(g.get("legs") or [])
    if not legs:
        return RunResult(portfolio_id=pid, name=name, status="skipped", detail="no_legs")

    mode = str(g.get("execution_mode") or "independent").strip().lower()
    if mode not in {"independent", "merged"}:
        mode = "independent"
    exit_policy = str(g.get("execution_exit_policy") or "any_leg").strip().lower()
    if exit_policy not in {"any_leg", "entry_leg"}:
        exit_policy = "any_leg"

    req = {
        "merge_mode": mode,
        "merge_exit_policy": exit_policy,
        "legs": [{"strategy_id": (l or {}).get("strategy_id") or "debit_spreads", "params": (l or {}).get("params") or {}} for l in legs],
    }
    started = _json_req("POST", "/api/portfolio_backtest/start", body=req, timeout=45)
    sid = int(started.get("session_id"))
    _log("group_backtest_started", portfolio_id=pid, session_id=sid, mode=mode)
    st = _poll_session(sid, timeout_sec=timeout_sec)
    fin = str(st.get("status") or "unknown").lower()
    _log("group_backtest_finished", portfolio_id=pid, session_id=sid, status=fin)
    return RunResult(portfolio_id=pid, name=name, status=fin, session_id=sid)


def _market_close_plus_20_ct_day() -> str:
    # informational stamp only
    if ZoneInfo is None:
        return ""
    ct = ZoneInfo("America/Chicago")
    now = datetime.now(ct)
    return now.isoformat()


def main() -> int:
    timeout_sec = int(str(os.getenv("DAILY_GROUP_BACKTEST_TIMEOUT_SEC", "7200")) or "7200")
    services_csv = str(
        os.getenv(
            "DAILY_GROUP_BACKTEST_REQUIRED_SERVICES",
            "optionspredicator-stack.service,options_ai_dashboard_api.service",
        )
    )
    services = [x.strip() for x in services_csv.split(",") if x.strip()]

    started_by_me: list[str] = []
    results: list[RunResult] = []

    try:
        _log("daily_group_backtests_begin", scheduled_at_ct=_market_close_plus_20_ct_day(), services=services)
        started_by_me = _ensure_services_started(services)
        _wait_for_health()

        latest_day = _latest_data_day()
        if not latest_day:
            _log("no_latest_data_day", detail="skipping all groups")
            return 2

        plist = _json_req("GET", "/api/portfolios", timeout=30)
        items = list(plist.get("items") or [])
        paired = [
            p
            for p in items
            if str((p or {}).get("paired_account_label") or "").strip()
        ]

        _log("paired_groups_discovered", count=len(paired), latest_data_day=latest_day)
        for g in paired:
            pid = int(g.get("id"))
            name = str(g.get("name") or f"Group {pid}")
            try:
                rr = _run_one_group(g, latest_day=latest_day, timeout_sec=timeout_sec)
                results.append(rr)
            except Exception as e:
                _log("group_backtest_error", portfolio_id=pid, name=name, error=str(e))
                results.append(RunResult(portfolio_id=pid, name=name, status="failed", detail=str(e)))

        ok = sum(1 for r in results if r.status == "stopped")
        fail = sum(1 for r in results if r.status == "failed")
        skipped = sum(1 for r in results if r.status == "skipped")
        _log("daily_group_backtests_summary", total=len(results), ok=ok, failed=fail, skipped=skipped)
        return 0 if fail == 0 else 1
    finally:
        if started_by_me:
            _stop_services(started_by_me)


if __name__ == "__main__":
    sys.exit(main())
