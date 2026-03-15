from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from datetime import date
from typing import Any, Callable, Literal

from fastapi import HTTPException

try:
    import psycopg
except Exception:  # pragma: no cover
    psycopg = None  # type: ignore

from options_ai.backtest.debit_spreads import DebitBacktestConfig, run_backtest_debit_spreads


ParamType = Literal["int", "float", "bool", "enum", "str", "list_enum", "date"]


@dataclass(frozen=True)
class ParamSpec:
    key: str
    typ: ParamType
    default: Any
    min: float | int | None = None
    max: float | int | None = None
    step: float | int | None = None
    choices: list[str] | None = None
    sweepable: bool = True
    refineable: bool = True
    applies_when: Callable[[dict[str, Any]], bool] | None = None


def canonical_json(obj: Any) -> str:
    return json.dumps(obj, separators=(",", ":"), sort_keys=True)


def params_hash(*, strategy_key: str, schema_version: int, params_json_canonical: str) -> str:
    s = f"{strategy_key}|v{int(schema_version)}|{params_json_canonical}".encode("utf-8")
    return hashlib.sha256(s).hexdigest()


class StrategyDefinition:
    id: str
    display_name: str
    schema_version: int

    def param_specs(self) -> list[ParamSpec]:
        raise NotImplementedError

    def validate_and_normalize(self, payload: dict[str, Any], *, strict: bool) -> dict[str, Any]:
        raise NotImplementedError

    def strategy_key(self, canonical_params: dict[str, Any]) -> str:
        raise NotImplementedError


    def _build_cfg(self, canonical_params: dict[str, Any]) -> DebitBacktestConfig:
        """Build DebitBacktestConfig from validated canonical_params.

        Kept as a helper so the sampler can reuse it for precheck gating.
        """

        def g(key: str, default: Any) -> Any:
            return canonical_params.get(key, default)

        return DebitBacktestConfig(
            start_day=date.fromisoformat(str(canonical_params["start_day"])),
            end_day=date.fromisoformat(str(canonical_params["end_day"])),
            expiration_mode=str(g("expiration_mode", "0dte")),
            target_dte_days=(int(g("target_dte_days", 7)) if canonical_params.get("expiration_mode") == "target_dte" else None),
            dte_tolerance_days=int(g("dte_tolerance_days", 2)),
            horizon_minutes=int(g("horizon_minutes", 30)),
            entry_mode=str(g("entry_mode", "time_range")),
            session_start_ct=str(g("session_start_ct", "08:30")),
            entry_first_n_minutes=int(g("entry_first_n_minutes", 60)),
            entry_start_ct=str(g("entry_start_ct", "08:40")),
            entry_end_ct=str(g("entry_end_ct", "09:30")),
            max_trades_per_day=int(g("max_trades_per_day", 1)),
            one_trade_at_a_time=bool(g("one_trade_at_a_time", True)),
            spread_style=str(g("spread_style", "debit")),
            credit_stop_loss_mult=float(g("credit_stop_loss_mult", 2.00)),
            credit_take_profit_pct=float(g("credit_take_profit_pct", 0.50)),
            anchor_mode=str(g("anchor_mode", "ATM")),
            anchor_policy=str(g("anchor_policy", os.getenv("DEBIT_ANCHOR_POLICY", "any"))),
            min_p_bigwin=float(g("min_p_bigwin", 0.0)),
            min_pred_change=float(g("min_pred_change", 0.0)),
            min_pred_return=float(g("min_pred_return", 0.0)),
            contrarian_enabled=bool(g("contrarian_enabled", False)),
            contrarian_mode=str(g("contrarian_mode", "direction_only")),
            contrarian_fallback_same_direction=bool(g("contrarian_fallback_same_direction", True)),
            strategy_mode=str(g("strategy_mode", "anchor_based")),
            enable_pw_trade=bool(g("enable_pw_trade", True)),
            enable_cw_trade=bool(g("enable_cw_trade", True)),
            long_leg_moneyness=str(g("long_leg_moneyness", "ATM")),
            max_width_points=float(g("max_width_points", 25.0)),
            min_width_points=float(g("min_width_points", 5.0)),
            proximity_min_points=float(g("proximity_min_points", 0.0)),
            proximity_max_points=float(g("proximity_max_points", 30.0)),
            rotation_filter=str(g("rotation_filter", "spot_delta_5m")),
            prefer_pw_on_tie=bool(g("prefer_pw_on_tie", True)),
            short_put_offset_steps=int(g("short_put_offset_steps", 0)),
            short_call_offset_steps=int(g("short_call_offset_steps", 0)),
            allowed_spreads=tuple(g("allowed_spreads", ["CALL", "PUT"])),
            flow_gate_enabled=bool(g("flow_gate_enabled", False)),
            flow_live_ok_filter_enabled=bool(g("flow_live_ok_filter_enabled", False)),
            flow_gate_min_bucket_z=float(g("flow_gate_min_bucket_z", 1.5)),
            flow_gate_min_breadth=float(g("flow_gate_min_breadth", 0.60)),
            flow_gate_min_confidence=float(g("flow_gate_min_confidence", 0.60)),
            regime_enabled=bool(g("regime_enabled", False)),
            regime_min_confidence=float(g("regime_min_confidence", 0.55)),
            max_debit_points=float(g("max_debit_points", 5.0)),
            stop_loss_pct=float(g("stop_loss_pct", 0.50)),
            take_profit_pct=float(g("take_profit_pct", 2.00)),
            max_future_lookahead_minutes=int(g("max_future_lookahead_minutes", 120)),
            price_mode=str(g("price_mode", "mid")),
            tz_local=str(g("tz_local", "America/Chicago")),
            include_missing_exits=bool(g("include_missing_exits", False)),
        )

    def run(self, canonical_params: dict[str, Any]) -> dict[str, Any]:
        dsn = os.getenv("SPX_CHAIN_DATABASE_URL", "").strip() or None
        if not dsn:
            raise HTTPException(status_code=503, detail="SPX_CHAIN_DATABASE_URL not configured")
        if psycopg is None:
            raise HTTPException(status_code=500, detail="psycopg not installed on server")

        cfg = self._build_cfg(canonical_params)

        try:
            with psycopg.connect(dsn) as conn:
                return run_backtest_debit_spreads(conn, cfg)
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=503, detail=f"timescale connect failed: {e}")


class DebitSpreadsStrategy(StrategyDefinition):
    """Debit/Credit spreads backtest strategy.

    This registry entry is used by:
      - BacktestExecutor (dedupe + run storage)
      - BacktestSamplerService (parameter sweeps)
      - PortfolioBacktestService (multi-leg runs)

    The dashboard UI builds a `strategy_key` with the same logic.
    """

    id = "debit_spreads"
    display_name = "Debit Spreads"
    schema_version = 1

    def param_specs(self) -> list[ParamSpec]:
        # UI-driven; specs are currently informational only.
        return []

    def validate_and_normalize(self, payload: dict[str, Any], *, strict: bool) -> dict[str, Any]:
        if not isinstance(payload, dict):
            raise HTTPException(status_code=400, detail="params must be an object")

        def s(key: str, default: str) -> str:
            v = payload.get(key, default)
            return str(v) if v is not None else str(default)

        def i(key: str, default: int) -> int:
            v = payload.get(key, default)
            try:
                return int(v)
            except Exception:
                return int(default)

        def f(key: str, default: float) -> float:
            v = payload.get(key, default)
            try:
                return float(v)
            except Exception:
                return float(default)

        def b(key: str, default: bool) -> bool:
            v = payload.get(key, default)
            return bool(v) if v is not None else bool(default)

        def e(key: str, default: str, choices: list[str]) -> str:
            v = s(key, default)
            vv = str(v).strip().lower()
            norm = {str(c).strip().lower(): c for c in choices}
            return norm.get(vv, default)

        start_day = s("start_day", "")
        end_day = s("end_day", "")
        if strict and (not start_day or not end_day):
            raise HTTPException(status_code=400, detail="start_day and end_day are required")

        if start_day:
            try:
                date.fromisoformat(start_day)
            except Exception:
                raise HTTPException(status_code=400, detail="start_day must be YYYY-MM-DD")
        if end_day:
            try:
                date.fromisoformat(end_day)
            except Exception:
                raise HTTPException(status_code=400, detail="end_day must be YYYY-MM-DD")

        allowed_spreads = payload.get("allowed_spreads")
        if allowed_spreads is None:
            allowed_spreads = ["CALL", "PUT"]
        if not isinstance(allowed_spreads, list) or not all(isinstance(x, str) for x in allowed_spreads):
            allowed_spreads = ["CALL", "PUT"]

        canon: dict[str, Any] = {
            "start_day": start_day,
            "end_day": end_day,
            "expiration_mode": s("expiration_mode", "0dte"),
            "target_dte_days": i("target_dte_days", 7),
            "dte_tolerance_days": i("dte_tolerance_days", 2),
            "horizon_minutes": i("horizon_minutes", 30),
            "entry_mode": s("entry_mode", "time_range"),
            "session_start_ct": s("session_start_ct", "08:30"),
            "entry_first_n_minutes": i("entry_first_n_minutes", 60),
            "entry_start_ct": s("entry_start_ct", "08:40"),
            "entry_end_ct": s("entry_end_ct", "09:30"),
            "max_trades_per_day": i("max_trades_per_day", 1),
            "one_trade_at_a_time": b("one_trade_at_a_time", True),
            "strategy_mode": s("strategy_mode", "anchor_based"),
            "spread_style": s("spread_style", "debit"),
            "credit_stop_loss_mult": f("credit_stop_loss_mult", 2.0),
            "credit_take_profit_pct": f("credit_take_profit_pct", 0.5),
            "anchor_mode": s("anchor_mode", "ATM"),
            "anchor_policy": s("anchor_policy", os.getenv("DEBIT_ANCHOR_POLICY", "any")),
            "allowed_spreads": [str(x).upper() for x in allowed_spreads],
            "flow_gate_enabled": b("flow_gate_enabled", False),
            "flow_live_ok_filter_enabled": b("flow_live_ok_filter_enabled", False),
            "flow_gate_min_bucket_z": f("flow_gate_min_bucket_z", 1.5),
            "flow_gate_min_breadth": f("flow_gate_min_breadth", 0.60),
            "flow_gate_min_confidence": f("flow_gate_min_confidence", 0.60),
            "regime_enabled": b("regime_enabled", False),
            "regime_min_confidence": f("regime_min_confidence", 0.55),
            "min_p_bigwin": f("min_p_bigwin", 0.0),
            "min_pred_change": f("min_pred_change", 0.0),
            "min_pred_return": f("min_pred_return", 0.0),
            "contrarian_enabled": b("contrarian_enabled", False),
            "contrarian_mode": e("contrarian_mode", "direction_only", ["direction_only", "ml_invert", "full"]),
            "contrarian_fallback_same_direction": b("contrarian_fallback_same_direction", True),
            "enable_pw_trade": b("enable_pw_trade", True),
            "enable_cw_trade": b("enable_cw_trade", True),
            "long_leg_moneyness": s("long_leg_moneyness", "ATM"),
            "max_width_points": f("max_width_points", 25.0),
            "min_width_points": f("min_width_points", 5.0),
            "proximity_min_points": f("proximity_min_points", 0.0),
            "proximity_max_points": f("proximity_max_points", 30.0),
            "rotation_filter": s("rotation_filter", "spot_delta_5m"),
            "prefer_pw_on_tie": b("prefer_pw_on_tie", True),
            "short_put_offset_steps": i("short_put_offset_steps", 0),
            "short_call_offset_steps": i("short_call_offset_steps", 0),
            "max_debit_points": f("max_debit_points", 5.0),
            "stop_loss_pct": f("stop_loss_pct", 0.5),
            "take_profit_pct": f("take_profit_pct", 2.0),
            "max_future_lookahead_minutes": i("max_future_lookahead_minutes", 120),
            "price_mode": s("price_mode", "mid"),
            "tz_local": s("tz_local", "America/Chicago"),
            "include_missing_exits": b("include_missing_exits", False),
        }
        return canon

    def strategy_key(self, canonical_params: dict[str, Any]) -> str:
        ss = str((canonical_params or {}).get("spread_style") or "debit").strip().lower()
        prefix = "credit_spreads" if ss == "credit" else "debit_spreads"

        mode = str((canonical_params or {}).get("strategy_mode") or "anchor_based").strip().lower()

        exp_mode = str((canonical_params or {}).get("expiration_mode") or "0dte").strip().lower()
        if exp_mode == "0dte":
            exp_key = "exp0dte"
        else:
            td = int((canonical_params or {}).get("target_dte_days") or 7)
            tol = int((canonical_params or {}).get("dte_tolerance_days") or 2)
            exp_key = f"dte{td}t{tol}"

        return f"{prefix}:{mode}:{exp_key}"


class StrategyRegistry:
    def __init__(self) -> None:
        self._by_id: dict[str, StrategyDefinition] = {
            DebitSpreadsStrategy.id: DebitSpreadsStrategy(),
        }

    def list(self) -> list[dict[str, Any]]:
        out = []
        for sid, s in self._by_id.items():
            out.append({"id": sid, "display_name": getattr(s, "display_name", sid), "schema_version": int(getattr(s, "schema_version", 1))})
        return out

    def get(self, strategy_id: str) -> StrategyDefinition:
        sid = str(strategy_id or "").strip()
        if sid not in self._by_id:
            raise HTTPException(status_code=404, detail=f"unknown strategy: {sid}")
        return self._by_id[sid]
