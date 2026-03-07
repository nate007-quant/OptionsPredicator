from __future__ import annotations

from typing import Any


def score_strategy(result: dict[str, Any], scoring_cfg: dict[str, Any]) -> tuple[float, list[str]]:
    w = (scoring_cfg or {}).get("weights") or {}
    p = (scoring_cfg or {}).get("penalties") or {}
    m = (result.get("metrics") or {})
    gaps = result.get("capability_gaps") or []

    sharpe = float(m.get("sharpe") or 0.0)
    calmar = float(m.get("calmar") or 0.0)
    rom = float(m.get("return_on_margin") or 0.0)
    max_dd = float(m.get("max_drawdown_pct") or 0.0)
    pf = float(m.get("profit_factor") or 0.0)

    # normalize into rough [0,1]
    n_sharpe = max(0.0, min(1.0, sharpe / 2.0))
    n_calmar = max(0.0, min(1.0, calmar / 2.0))
    n_rom = max(0.0, min(1.0, rom / 2000.0))
    n_pf = max(0.0, min(1.0, pf / 3.0))
    n_dd_pen = max(0.0, min(1.0, 1.0 - (max_dd / 50.0)))

    score = 0.0
    score += float(w.get("oos_sharpe", 0.25)) * n_sharpe
    score += float(w.get("oos_calmar", 0.20)) * n_calmar
    score += float(w.get("return_on_margin", 0.20)) * n_rom
    score += float(w.get("profit_factor", 0.10)) * n_pf
    score += float(w.get("max_drawdown_penalty", 0.15)) * n_dd_pen
    score += float(w.get("parameter_stability_score", 0.10)) * (1.0 if (result.get("robustness") or {}).get("parameter_stability_pass") else 0.0)

    reasons: list[str] = []
    if gaps:
        score -= float(p.get("capability_gap", 0.20))
        reasons.append("capability_gap_penalty")
    if (result.get("degraded_data") is True):
        score -= float(p.get("degraded_data", 0.15))
        reasons.append("degraded_data_penalty")
    if int(m.get("trades") or 0) < 20:
        score -= float(p.get("low_trade_count", 0.25))
        reasons.append("low_trade_count_penalty")

    score = max(0.0, min(1.0, score))
    return score, reasons


def classify_state_from_score(score: float, scoring_cfg: dict[str, Any]) -> str:
    t = (scoring_cfg or {}).get("thresholds") or {}
    if score >= float(t.get("paper_min_score", 0.70)):
        return "CANDIDATE"
    if score >= float(t.get("robust_min_score", 0.55)):
        return "ROBUST"
    return "TESTED"
