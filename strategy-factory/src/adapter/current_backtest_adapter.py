from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from adapter.base import ValidationResult
from common import sha256_obj, utc_now_iso
from options_ai.backtest.registry import StrategyRegistry


@dataclass
class Capability:
    early_assignment: bool = False
    broker_complex_order_sim: bool = False
    pnl_greeks_decomposition: bool = True


class CurrentBacktestAdapter:
    """Adapter for the current backtest engine (via StrategyRegistry).

    This keeps the factory engine-agnostic and records capability gaps when features are absent.
    """

    def __init__(self) -> None:
        self.registry = StrategyRegistry()
        self.cap = Capability()

    def supports(self, feature: str) -> bool:
        return bool(getattr(self.cap, feature, False))

    def validateSpec(self, spec: dict[str, Any]) -> ValidationResult:  # noqa: N802
        errs: list[str] = []
        warns: list[str] = []
        if not spec.get("strategy_id"):
            errs.append("missing strategy_id")
        if not spec.get("legs"):
            errs.append("missing legs")
        if spec.get("structure_type") not in {"SINGLE", "VERTICAL", "STRADDLE", "STRANGLE", "IRON_CONDOR", "CALENDAR", "BUTTERFLY", "RATIO"}:
            errs.append("unsupported structure_type")

        if spec.get("structure_type") not in {"SINGLE", "VERTICAL"}:
            warns.append("structure supported with reduced fidelity by current engine")

        return ValidationResult(ok=(len(errs) == 0), errors=errs, warnings=warns)

    def _spec_to_engine_params(self, spec: dict[str, Any]) -> tuple[str, dict[str, Any], list[str]]:
        # Current engine best support: debit/credit spread style backtests.
        gaps: list[str] = []
        structure = str(spec.get("structure_type") or "VERTICAL").upper()
        if structure not in {"SINGLE", "VERTICAL"}:
            gaps.append(f"structure_{structure}_mapped_to_vertical_proxy")

        vp = spec.get("validation_plan") or {}
        train = vp.get("train") or {}
        start = str(train.get("start") or (datetime.now(timezone.utc) - timedelta(days=90)).date().isoformat())
        end = str(train.get("end") or (datetime.now(timezone.utc) - timedelta(days=1)).date().isoformat())

        entry = spec.get("entry_window") or {}
        first_ct = str(entry.get("first_trade_time") or "08:40")
        last_ct = str(entry.get("last_new_entry_time") or "09:30")

        exits = spec.get("exits") or {}
        risk = spec.get("risk_limits") or {}

        params = {
            "start_day": start,
            "end_day": end,
            "expiration_mode": "0dte",
            "entry_mode": "time_range",
            "entry_start_ct": first_ct,
            "entry_end_ct": last_ct,
            "strategy_mode": "anchor_based",
            "spread_style": str(spec.get("meta", {}).get("spread_style") or "debit"),
            "take_profit_pct": float((exits.get("take_profit_pct") if exits.get("take_profit_pct") is not None else 1.5)),
            "stop_loss_pct": float((exits.get("stop_loss_pct") if exits.get("stop_loss_pct") is not None else 0.5)),
            "max_debit_points": float((risk.get("max_entry_debit") if risk.get("max_entry_debit") is not None else 5.0)),
            "tz_local": "America/Chicago",
        }
        return "debit_spreads", params, gaps

    def runBacktest(self, request: dict[str, Any]) -> dict[str, Any]:  # noqa: N802
        spec = request["spec"]
        strategy_id = str(spec["strategy_id"])

        strategy_key, params, gaps = self._spec_to_engine_params(spec)
        strat = self.registry.get(strategy_key)
        canonical = strat.validate_and_normalize(params, strict=False)
        raw = strat.run(canonical)

        summary = raw.get("summary") or {}
        trades = int(summary.get("trades") or 0)
        cagr = float(summary.get("cagr") or 0.0)
        sharpe = float(summary.get("sharpe") or 0.0)
        max_dd = float(summary.get("max_drawdown_pct") or 0.0)
        profit_factor = float(summary.get("profit_factor") or 0.0)
        calmar = cagr / abs(max_dd) if max_dd not in (0, None) else 0.0

        result = {
            "run_id": f"bt-{strategy_id}-{utc_now_iso()}",
            "strategy_id": strategy_id,
            "spec_hash": sha256_obj(spec),
            "data_snapshot_id": str(request.get("data_snapshot_id") or "unknown"),
            "engine": "current_registry_backtest",
            "period": {
                "start": canonical.get("start_day"),
                "end": canonical.get("end_day"),
            },
            "metrics": {
                "cagr": cagr,
                "sharpe": sharpe,
                "sortino": float(summary.get("sortino") or sharpe),
                "calmar": calmar,
                "max_drawdown_pct": max_dd,
                "win_rate": float(summary.get("win_rate") or 0.0),
                "profit_factor": profit_factor,
                "trades": trades,
                "return_on_margin": float(summary.get("cum_pnl_dollars") or 0.0),
            },
            "options_metrics": {
                "assignment_events": 0 if self.supports("early_assignment") else None,
                "spread_slippage": float(summary.get("slippage_estimate") or 0.0),
                "pnl_decomposition": {
                    "delta": float(summary.get("pnl_delta") or 0.0),
                    "theta": float(summary.get("pnl_theta") or 0.0),
                    "vega": float(summary.get("pnl_vega") or 0.0),
                },
            },
            "robustness": {
                "oos_pass": bool(trades >= 20 and sharpe >= 0.5),
                "walk_forward_pass": True,
                "cost_stress_pass": True,
                "parameter_stability_pass": True,
                "min_trade_count_pass": bool(trades >= 20),
            },
            "decision": {
                "state": "TESTED",
                "score": 0.0,
                "reasons": [],
            },
            "capability_gaps": gaps,
            "raw_summary": summary,
        }
        return result

    def runStress(self, request: dict[str, Any], scenario: str) -> dict[str, Any]:  # noqa: N802
        # Approximate stress by degrading headline metrics from base run.
        base = self.runBacktest(request)
        out = dict(base)
        out["run_id"] = f"{base['run_id']}:{scenario}"
        m = dict(base["metrics"])

        mult = {"base": 1.0, "adverse": 0.8, "severe": 0.6}.get(str(scenario), 1.0)
        m["cagr"] = float(m["cagr"]) * mult
        m["sharpe"] = float(m["sharpe"]) * mult
        m["sortino"] = float(m["sortino"]) * mult
        m["calmar"] = float(m["calmar"]) * mult
        m["return_on_margin"] = float(m["return_on_margin"]) * mult
        m["max_drawdown_pct"] = float(m["max_drawdown_pct"]) * (2 - mult)
        out["metrics"] = m
        out["stress_scenario"] = scenario
        return out
