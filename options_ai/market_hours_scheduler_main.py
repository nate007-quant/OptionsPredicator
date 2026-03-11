from __future__ import annotations

import json
import os
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, time as dtime, timedelta
from pathlib import Path
from urllib.request import urlopen
from zoneinfo import ZoneInfo


TZ = ZoneInfo("America/Chicago")
DATA_ROOT = Path(os.getenv("OPTIONS_AI_DATA_ROOT", "/mnt/options_ai"))
STATE_PATH = DATA_ROOT / "state" / "market_scheduler_state.json"
CFG_PATH = DATA_ROOT / "state" / "market_scheduler_config.json"

ZERO_DTE_SERVICES = [
    "options_predicator",
    "spx_chain_ingester",
    "spx_chain_phase2",
    "spx_debit_spreads",
    "spx_debit_ml",
]
TERM_SERVICES = [
    "spx_chain_phase2_term_dte7t2",
    "spx_chain_phase2_term_dte14t2",
    "spx_chain_phase2_term_dte21t3",
    "spx_chain_phase2_term_dte30t5",
    "spx_debit_spreads_term_dte7t2",
    "spx_debit_spreads_term_dte14t2",
    "spx_debit_spreads_term_dte21t3",
    "spx_debit_spreads_term_dte30t5",
    "spx_debit_ml_term_dte7t2",
    "spx_debit_ml_term_dte14t2",
    "spx_debit_ml_term_dte21t3",
    "spx_debit_ml_term_dte30t5",
]
EXEC_SERVICES = [
    "options_ai_execution",
    "options_ai_execution_monitor",
    "options_ai_risk_guard",
]

DEFAULT_SCHEDULER_CONFIG = {
    "include_start_zero_dte": True,
    "include_start_term": True,
    "include_start_execution": True,
}


def _parse_hhmm(v: str, default: dtime) -> dtime:
    try:
        hh, mm = [int(x) for x in str(v).strip().split(":", 1)]
        return dtime(hour=max(0, min(23, hh)), minute=max(0, min(59, mm)))
    except Exception:
        return default


def _split_services(v: str, fallback: list[str]) -> list[str]:
    raw = [x.strip() for x in str(v or "").split(",") if x.strip()]
    return raw or fallback


def _load_json(path: Path) -> dict:
    try:
        if path.exists():
            obj = json.loads(path.read_text())
            if isinstance(obj, dict):
                return obj
    except Exception:
        pass
    return {}


def _save_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, separators=(",", ":"), sort_keys=True))


def _load_state() -> dict:
    return _load_json(STATE_PATH)


def _save_state(st: dict) -> None:
    _save_json(STATE_PATH, st)


def _load_scheduler_config() -> dict:
    c = dict(DEFAULT_SCHEDULER_CONFIG)
    c.update(_load_json(CFG_PATH))
    return c


def _run_systemctl(action: str, service: str) -> tuple[bool, str]:
    cmds = [
        ["sudo", "-n", "systemctl", action, service],
        ["systemctl", action, service],
    ]
    last = ""
    for cmd in cmds:
        p = subprocess.run(cmd, check=False, capture_output=True, text=True, timeout=20)
        if p.returncode == 0:
            return True, (p.stdout or "ok").strip() or "ok"
        last = ((p.stderr or "").strip() or (p.stdout or "").strip() or f"rc={p.returncode}")
    return False, last


def _is_pipeline_idle(api_base: str) -> bool:
    try:
        with urlopen(f"{api_base}/api/metrics/processing/summary?window=15m", timeout=8) as r:
            d = json.loads(r.read().decode("utf-8"))
        k = d.get("kpis", {}) if isinstance(d, dict) else {}
        q = int(k.get("queue_depth_total") or 0)
        inf = int(k.get("inflight_count") or 0)
        return q == 0 and inf == 0
    except Exception:
        return False


@dataclass
class Cfg:
    enabled: bool
    poll_seconds: int
    preopen_start_ct: dtime
    market_close_ct: dtime
    stop_grace_minutes: int
    api_base: str
    stop_services: list[str]


def _load_cfg() -> Cfg:
    return Cfg(
        enabled=str(os.getenv("MARKET_SCHEDULER_ENABLED", "true")).lower() in {"1", "true", "yes", "on"},
        poll_seconds=max(15, int(os.getenv("MARKET_SCHEDULER_POLL_SECONDS", "60") or "60")),
        preopen_start_ct=_parse_hhmm(os.getenv("MARKET_PREOPEN_START_CT", "08:25"), dtime(8, 25)),
        market_close_ct=_parse_hhmm(os.getenv("MARKET_CLOSE_CT", "15:00"), dtime(15, 0)),
        stop_grace_minutes=max(0, int(os.getenv("MARKET_STOP_GRACE_MINUTES", "15") or "15")),
        api_base=os.getenv("MARKET_SCHEDULER_API_BASE", "http://127.0.0.1:8088").rstrip("/"),
        stop_services=_split_services(
            os.getenv("MARKET_AUTOSTOP_SERVICES", ""),
            ZERO_DTE_SERVICES + TERM_SERVICES + EXEC_SERVICES,
        ),
    )


def _today_key(now: datetime) -> str:
    return now.date().isoformat()


def _build_start_services(toggle_cfg: dict) -> list[str]:
    out: list[str] = []
    if bool(toggle_cfg.get("include_start_zero_dte", True)):
        out.extend(ZERO_DTE_SERVICES)
    if bool(toggle_cfg.get("include_start_term", True)):
        out.extend(TERM_SERVICES)
    if bool(toggle_cfg.get("include_start_execution", True)):
        out.extend(EXEC_SERVICES)
    # uniq preserve order
    seen = set()
    uniq = []
    for x in out:
        if x in seen:
            continue
        seen.add(x)
        uniq.append(x)
    return uniq


def main() -> None:
    while True:
        cfg = _load_cfg()  # reload env-controlled schedule each cycle
        if not cfg.enabled:
            time.sleep(cfg.poll_seconds)
            continue

        sched_cfg = _load_scheduler_config()
        start_services = _build_start_services(sched_cfg)
        stop_services = list(dict.fromkeys(cfg.stop_services + ZERO_DTE_SERVICES + TERM_SERVICES + EXEC_SERVICES))

        now = datetime.now(TZ)
        weekday = now.weekday() < 5  # Mon-Fri
        st = _load_state()
        dayk = _today_key(now)

        start_dt = datetime.combine(now.date(), cfg.preopen_start_ct, TZ)
        close_dt = datetime.combine(now.date(), cfg.market_close_ct, TZ)
        stop_deadline = close_dt + timedelta(minutes=cfg.stop_grace_minutes)

        if weekday and now >= start_dt and st.get("last_started_day") != dayk:
            for svc in start_services:
                _run_systemctl("start", svc)
            st["last_started_day"] = dayk
            st["last_start_at"] = datetime.now(TZ).isoformat()
            st["last_start_count"] = len(start_services)
            _save_state(st)

        should_try_stop = weekday and now >= close_dt and st.get("last_stopped_day") != dayk
        if should_try_stop:
            idle = _is_pipeline_idle(cfg.api_base)
            if now >= stop_deadline or idle:
                for svc in stop_services:
                    _run_systemctl("stop", svc)
                st["last_stopped_day"] = dayk
                st["last_stop_at"] = datetime.now(TZ).isoformat()
                st["last_stop_reason"] = "idle" if idle else "grace_elapsed"
                st["last_stop_count"] = len(stop_services)
                _save_state(st)

        time.sleep(cfg.poll_seconds)


if __name__ == "__main__":
    main()
