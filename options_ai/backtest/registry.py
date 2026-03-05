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

    def run(self, canonical_params: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError


class DebitSpreadsStrategy(StrategyDefinition):
    id = "debit_spreads"
    display_name = "Debit Spreads"
    schema_version = 1

    def param_specs(self) -> list[ParamSpec]:
        # Applies/validity rules are encoded in validate_and_normalize.
        return [
            ParamSpec("start_day", "date", None, sweepable=False, refineable=False),
            ParamSpec("end_day", "date", None, sweepable=False, refineable=False),
            ParamSpec("expiration_mode", "enum", "0dte", choices=["0dte", "target_dte"], sweepable=False, refineable=False),
            ParamSpec("target_dte_days", "int", None, min=1, max=45, step=1, applies_when=lambda p: p.get("expiration_mode") == "target_dte"),
            ParamSpec("dte_tolerance_days", "int", 2, min=0, max=10, step=1, applies_when=lambda p: p.get("expiration_mode") == "target_dte"),
            ParamSpec("horizon_minutes", "int", 30, min=5, max=180, step=5),
            ParamSpec("entry_mode", "enum", "time_range", choices=["time_range", "first_n_minutes"], sweepable=False, refineable=False),
            ParamSpec("session_start_ct", "str", "08:30", sweepable=False, refineable=False),
            ParamSpec("entry_first_n_minutes", "int", 60, min=5, max=180, step=5, applies_when=lambda p: p.get("entry_mode") == "first_n_minutes"),
            ParamSpec("entry_start_ct", "str", "08:40", sweepable=False, refineable=False, applies_when=lambda p: p.get("entry_mode") == "time_range"),
            ParamSpec("entry_end_ct", "str", "09:30", sweepable=False, refineable=False, applies_when=lambda p: p.get("entry_mode") == "time_range"),
            ParamSpec("max_trades_per_day", "int", 1, min=1, max=10, step=1),
            ParamSpec("one_trade_at_a_time", "bool", True),
            ParamSpec("max_debit_points", "float", 5.0, min=0.25, max=20.0, step=0.25),
            ParamSpec("stop_loss_pct", "float", 0.50, min=0.01, max=1.0, step=0.01),
            ParamSpec("take_profit_pct", "float", 2.00, min=0.10, max=10.0, step=0.05),
            ParamSpec("max_future_lookahead_minutes", "int", 120, min=30, max=360, step=30),
            ParamSpec("price_mode", "enum", "mid", choices=["mid"], sweepable=False, refineable=False),
            ParamSpec("tz_local", "str", "America/Chicago", sweepable=False, refineable=False),
            ParamSpec("include_missing_exits", "bool", False),
            ParamSpec("strategy_mode", "enum", "anchor_based", choices=["anchor_based", "structural_walls"], sweepable=False, refineable=False),
            # anchor_based knobs
            ParamSpec("anchor_mode", "enum", "ATM", choices=["ATM", "WALLS", "MAGNET", "ALL"], applies_when=lambda p: p.get("strategy_mode") == "anchor_based"),
            ParamSpec("anchor_policy", "enum", os.getenv("DEBIT_ANCHOR_POLICY", "any"), choices=["any", "opposite_wall", "same_wall"], applies_when=lambda p: p.get("strategy_mode") == "anchor_based"),
            ParamSpec("min_p_bigwin", "float", 0.0, min=0.0, max=1.0, step=0.01, applies_when=lambda p: p.get("strategy_mode") == "anchor_based"),
            ParamSpec("min_pred_change", "float", 0.0, min=0.0, max=10.0, step=0.01, applies_when=lambda p: p.get("strategy_mode") == "anchor_based"),
            ParamSpec("allowed_spreads", "list_enum", ["CALL", "PUT"], choices=["CALL", "PUT"], applies_when=lambda p: p.get("strategy_mode") == "anchor_based"),
            # structural_walls knobs
            ParamSpec("enable_pw_trade", "bool", True, applies_when=lambda p: p.get("strategy_mode") == "structural_walls"),
            ParamSpec("enable_cw_trade", "bool", True, applies_when=lambda p: p.get("strategy_mode") == "structural_walls"),
            ParamSpec("long_leg_moneyness", "enum", "ATM", choices=["ATM", "1_ITM"], applies_when=lambda p: p.get("strategy_mode") == "structural_walls"),
            ParamSpec("max_width_points", "float", 25.0, min=1.0, max=200.0, step=1.0, applies_when=lambda p: p.get("strategy_mode") == "structural_walls"),
            ParamSpec("min_width_points", "float", 5.0, min=1.0, max=200.0, step=1.0, applies_when=lambda p: p.get("strategy_mode") == "structural_walls"),
            ParamSpec("proximity_min_points", "float", 0.0, min=0.0, max=200.0, step=1.0, applies_when=lambda p: p.get("strategy_mode") == "structural_walls"),
            ParamSpec("proximity_max_points", "float", 30.0, min=0.0, max=400.0, step=1.0, applies_when=lambda p: p.get("strategy_mode") == "structural_walls"),
            ParamSpec("rotation_filter", "enum", "spot_delta_5m", choices=["none", "spot_delta_5m"], applies_when=lambda p: p.get("strategy_mode") == "structural_walls"),
            ParamSpec("prefer_pw_on_tie", "bool", True, applies_when=lambda p: p.get("strategy_mode") == "structural_walls"),
            ParamSpec("short_put_offset_steps", "int", 0, min=0, max=10, step=1, applies_when=lambda p: p.get("strategy_mode") == "structural_walls"),
            ParamSpec("short_call_offset_steps", "int", 0, min=0, max=10, step=1, applies_when=lambda p: p.get("strategy_mode") == "structural_walls"),
        ]

    def validate_and_normalize(self, payload: dict[str, Any], *, strict: bool) -> dict[str, Any]:
        if not isinstance(payload, dict):
            raise HTTPException(status_code=400, detail="payload must be an object")

        # strip UI/service-only keys
        p = dict(payload)
        for k in ("preset_id", "sampler_id"):
            p.pop(k, None)

        # apply defaults
        base: dict[str, Any] = {}
        for spec in self.param_specs():
            if spec.key in p:
                base[spec.key] = p.get(spec.key)
            else:
                base[spec.key] = spec.default

        # required dates
        try:
            sd = date.fromisoformat(str(base.get("start_day")))
            ed = date.fromisoformat(str(base.get("end_day")))
        except Exception:
            raise HTTPException(status_code=400, detail="start_day/end_day required as YYYY-MM-DD")
        base["start_day"] = sd.isoformat()
        base["end_day"] = ed.isoformat()

        # enums
        def _enum(key: str, allowed: list[str]) -> str:
            v = str(base.get(key) or "").strip()
            if v not in allowed:
                raise HTTPException(status_code=400, detail=f"invalid {key}: {v}")
            return v

        base["expiration_mode"] = _enum("expiration_mode", ["0dte", "target_dte"])
        base["entry_mode"] = _enum("entry_mode", ["time_range", "first_n_minutes"])
        base["strategy_mode"] = _enum("strategy_mode", ["anchor_based", "structural_walls"])

        # applicability pruning
        def applies(spec: ParamSpec) -> bool:
            if spec.applies_when is None:
                return True
            try:
                return bool(spec.applies_when(base))
            except Exception:
                return False

        # numeric quantize + bounds
        def _quantize_num(x: float, step: float) -> float:
            if step <= 0:
                return float(x)
            return round(round(float(x) / float(step)) * float(step), 10)

        out: dict[str, Any] = {}
        for spec in self.param_specs():
            if not applies(spec):
                if strict and spec.key in p and p.get(spec.key) not in (None, ""):
                    raise HTTPException(status_code=400, detail=f"param not applicable: {spec.key}")
                continue

            v = base.get(spec.key)
            if spec.typ == "bool":
                out[spec.key] = bool(v)
            elif spec.typ == "int":
                if v in (None, ""):
                    out[spec.key] = None
                else:
                    try:
                        iv = int(v)
                    except Exception:
                        raise HTTPException(status_code=400, detail=f"invalid int {spec.key}")
                    if spec.min is not None and iv < int(spec.min):
                        raise HTTPException(status_code=400, detail=f"{spec.key} below min")
                    if spec.max is not None and iv > int(spec.max):
                        raise HTTPException(status_code=400, detail=f"{spec.key} above max")
                    if spec.step is not None:
                        # quantize to step from 0
                        st = int(spec.step)
                        if st > 0:
                            iv = int(round(iv / st) * st)
                    out[spec.key] = int(iv)
            elif spec.typ == "float":
                if v in (None, ""):
                    out[spec.key] = None
                else:
                    try:
                        fv = float(v)
                    except Exception:
                        raise HTTPException(status_code=400, detail=f"invalid float {spec.key}")
                    if spec.min is not None and fv < float(spec.min):
                        raise HTTPException(status_code=400, detail=f"{spec.key} below min")
                    if spec.max is not None and fv > float(spec.max):
                        raise HTTPException(status_code=400, detail=f"{spec.key} above max")
                    if spec.step is not None:
                        fv = _quantize_num(fv, float(spec.step))
                    out[spec.key] = float(fv)
            elif spec.typ == "enum":
                allowed = spec.choices or []
                ev = str(v if v is not None else "").strip()
                if ev == "" and spec.default is not None:
                    ev = str(spec.default)
                if allowed and ev not in allowed:
                    raise HTTPException(status_code=400, detail=f"invalid {spec.key}: {ev}")
                out[spec.key] = ev
            elif spec.typ == "str":
                out[spec.key] = str(v) if v is not None else ""
            elif spec.typ == "list_enum":
                allowed = set(spec.choices or [])
                if v is None:
                    lst = list(spec.default or [])
                elif isinstance(v, (list, tuple)):
                    lst = [str(x).upper().strip() for x in v if str(x).strip()]
                else:
                    raise HTTPException(status_code=400, detail=f"invalid list {spec.key}")
                # validate and canonicalize
                if allowed:
                    bad = [x for x in lst if x not in allowed]
                    if bad:
                        raise HTTPException(status_code=400, detail=f"invalid {spec.key} values: {bad}")
                lst = sorted(set(lst))
                out[spec.key] = lst
            elif spec.typ == "date":
                # already validated
                out[spec.key] = str(base[spec.key])
            else:
                out[spec.key] = v

        # hard frozen: if expiration_mode == 0dte, do not include target DTE knobs
        if out.get("expiration_mode") == "0dte":
            out.pop("target_dte_days", None)
            out.pop("dte_tolerance_days", None)

        if out.get("entry_mode") == "first_n_minutes":
            out.pop("entry_start_ct", None)
            out.pop("entry_end_ct", None)
        else:
            out.pop("entry_first_n_minutes", None)

        # strategy_mode pruning
        if out.get("strategy_mode") == "anchor_based":
            for k in (
                "enable_pw_trade",
                "enable_cw_trade",
                "long_leg_moneyness",
                "max_width_points",
                "min_width_points",
                "proximity_min_points",
                "proximity_max_points",
                "rotation_filter",
                "prefer_pw_on_tie",
                "short_put_offset_steps",
                "short_call_offset_steps",
            ):
                out.pop(k, None)
        else:
            for k in ("anchor_mode", "anchor_policy", "min_p_bigwin", "min_pred_change", "allowed_spreads"):
                out.pop(k, None)

        return out

    def strategy_key(self, canonical_params: dict[str, Any]) -> str:
        strategy_mode = str(canonical_params.get("strategy_mode") or "anchor_based")
        exp_mode = str(canonical_params.get("expiration_mode") or "0dte").strip().lower()
        if exp_mode == "0dte":
            exp_key = "exp0dte"
        else:
            td = int(canonical_params.get("target_dte_days") or 7)
            tol = int(canonical_params.get("dte_tolerance_days") or 2)
            exp_key = f"dte{td}t{tol}"
        return f"debit_spreads:{strategy_mode}:{exp_key}"

    def run(self, canonical_params: dict[str, Any]) -> dict[str, Any]:
        dsn = os.getenv("SPX_CHAIN_DATABASE_URL", "").strip() or None
        if not dsn:
            raise HTTPException(status_code=503, detail="SPX_CHAIN_DATABASE_URL not configured")
        if psycopg is None:
            raise HTTPException(status_code=500, detail="psycopg not installed on server")

        # build config (re-apply defaults for optional fields)
        # Note: canonical_params has pruned keys; use defaults.
        def g(key: str, default: Any) -> Any:
            return canonical_params.get(key, default)

        cfg = DebitBacktestConfig(
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
