from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from common import sha256_obj


def compile_idea_to_spec(idea: dict[str, Any], *, default_underlying: str = "SPX") -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    train_start = (now - timedelta(days=120)).date().isoformat()
    train_end = (now - timedelta(days=20)).date().isoformat()
    test_start = (now - timedelta(days=19)).date().isoformat()
    test_end = (now - timedelta(days=1)).date().isoformat()

    structure = str(idea.get("structure_type") or "VERTICAL").upper()
    sid = str(idea.get("idea_id") or "idea")

    legs = [
        {
            "side": "BUY",
            "option_type": "CALL",
            "expiry_rule": {"kind": "DTE", "target": 0},
            "strike_rule": {"kind": "DELTA", "target": 0.5},
            "qty_ratio": 1,
        }
    ]

    if structure in {"VERTICAL", "IRON_CONDOR", "BUTTERFLY", "RATIO", "CALENDAR", "STRANGLE", "STRADDLE"}:
        legs.append(
            {
                "side": "SELL",
                "option_type": "CALL",
                "expiry_rule": {"kind": "DTE", "target": 0},
                "strike_rule": {"kind": "DELTA", "target": 0.25},
                "qty_ratio": 1,
            }
        )

    spec = {
        "strategy_id": sid,
        "version": "1.0.0",
        "name": str(idea.get("description") or sid),
        "structure_type": structure,
        "underlyings": [default_underlying],
        "timeframe": {"bar": "1m", "session": "RTH"},
        "entry_window": {
            "timezone": "America/Chicago",
            "first_trade_time": "08:40",
            "last_new_entry_time": "09:30",
        },
        "regime_filters": {
            "iv_rank_min": 20,
            "iv_rank_max": 80,
            "rv_iv_spread_min": -20,
            "event_blackout": True,
        },
        "legs": legs,
        "position_sizing": {"method": "fixed_contracts", "contracts": 1},
        "adjustments": {"allow_roll": True, "recenter_on_delta": 0.2},
        "exits": {"take_profit_pct": 1.5, "stop_loss_pct": 0.5, "max_dte_exit": 0},
        "cost_model": {"commission_per_contract": 0.65, "exchange_fee_per_contract": 0.03, "slippage_bps": 5},
        "risk_limits": {"max_entry_debit": 5.0, "max_strategy_drawdown_pct": 20.0, "max_portfolio_alloc_pct": 15.0},
        "validation_plan": {
            "train": {"start": train_start, "end": train_end},
            "test": {"start": test_start, "end": test_end},
            "walk_forward_windows": 5,
        },
        "meta": dict(idea.get("seed_params") or {}),
    }
    return spec


def validate_spec_runnable(spec: dict[str, Any], schema_path: Path) -> tuple[bool, list[str]]:
    errs: list[str] = []
    try:
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
    except Exception as e:
        return False, [f"schema_read_error:{e}"]

    required = schema.get("required") or []
    for k in required:
        if k not in spec:
            errs.append(f"missing:{k}")

    if not isinstance(spec.get("legs"), list) or len(spec.get("legs") or []) == 0:
        errs.append("legs must be non-empty")

    ew = spec.get("entry_window") or {}
    for k in ("first_trade_time", "last_new_entry_time", "timezone"):
        if not ew.get(k):
            errs.append(f"entry_window.{k} missing")

    vp = spec.get("validation_plan") or {}
    if not (vp.get("train") and vp.get("test")):
        errs.append("validation_plan train/test required")

    # strict machine-runnable: no ambiguous placeholders
    txt = json.dumps(spec)
    if "TODO" in txt or "<" in txt or "TBD" in txt:
        errs.append("ambiguous placeholders found")

    return len(errs) == 0, errs


def spec_hash(spec: dict[str, Any]) -> str:
    return sha256_obj(spec)
