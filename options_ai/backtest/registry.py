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
