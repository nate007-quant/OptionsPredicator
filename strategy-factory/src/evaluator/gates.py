from __future__ import annotations

from typing import Any


def evaluate_hard_gates(result: dict[str, Any], gates_cfg: dict[str, Any], stress_results: list[dict[str, Any]] | None = None) -> tuple[bool, list[str], dict[str, bool]]:
    g = (gates_cfg or {}).get("hard_gates") or {}
    m = (result.get("metrics") or {})
    r = (result.get("robustness") or {})

    reasons: list[str] = []
    flags: dict[str, bool] = {}

    def chk(name: str, cond: bool, msg: str) -> None:
        flags[name] = bool(cond)
        if not cond:
            reasons.append(msg)

    trades = int(m.get("trades") or 0)
    max_dd = float(m.get("max_drawdown_pct") or 0.0)
    sharpe = float(m.get("sharpe") or 0.0)
    calmar = float(m.get("calmar") or 0.0)

    chk("min_trade_count_pass", trades >= int(g.get("min_trades", 20)), f"trades<{g.get('min_trades',20)}")
    chk("max_drawdown_pass", max_dd <= float(g.get("max_drawdown_pct", 25.0)), f"max_dd>{g.get('max_drawdown_pct',25.0)}")
    chk("oos_sharpe_pass", sharpe >= float(g.get("min_oos_sharpe", 0.6)), f"sharpe<{g.get('min_oos_sharpe',0.6)}")
    chk("oos_calmar_pass", calmar >= float(g.get("min_oos_calmar", 0.4)), f"calmar<{g.get('min_oos_calmar',0.4)}")

    wf_ratio = 1.0 if bool(r.get("walk_forward_pass", False)) else 0.0
    chk("walk_forward_pass", wf_ratio >= float(g.get("min_walk_forward_pass_ratio", 0.6)), "walk_forward_fail")

    stability = 1.0 if bool(r.get("parameter_stability_pass", False)) else 0.0
    chk("parameter_stability_pass", stability >= float(g.get("min_parameter_stability_score", 0.55)), "parameter_stability_fail")

    # cost stress degradation gate
    stress = stress_results or []
    base_rom = float(m.get("return_on_margin") or 0.0)
    worst = base_rom
    for s in stress:
        worst = min(worst, float((s.get("metrics") or {}).get("return_on_margin") or base_rom))
    degr = 0.0
    if abs(base_rom) > 1e-9:
        degr = max(0.0, ((base_rom - worst) / abs(base_rom)) * 100.0)
    chk("cost_stress_pass", degr <= float(g.get("max_cost_stress_degradation_pct", 35.0)), f"cost_degradation>{g.get('max_cost_stress_degradation_pct',35.0)}")

    ok = len(reasons) == 0
    return ok, reasons, flags
