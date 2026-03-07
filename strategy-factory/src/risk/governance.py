from __future__ import annotations

from typing import Any


def enforce_autonomy_policy(*, target_state: str, policy: dict[str, Any], explicit_live_approval: bool) -> tuple[bool, str]:
    if target_state != "LIVE":
        return True, "ok"
    req = bool(((policy.get("permissions") or {}).get("paper_to_live_requires_explicit_approval", True)))
    if req and not explicit_live_approval:
        return False, "live approval flag required"
    return True, "ok"


def global_kill_switch_triggered(*, metrics: dict[str, Any], policy: dict[str, Any]) -> tuple[bool, list[str]]:
    ks = (policy.get("kill_switches") or {})
    reasons: list[str] = []

    dd = float(metrics.get("portfolio_drawdown_pct") or 0.0)
    if dd > float(ks.get("max_portfolio_drawdown_pct", 15.0)):
        reasons.append("max_portfolio_drawdown")

    daily_loss = float(metrics.get("daily_loss_usd") or 0.0)
    if daily_loss < -abs(float(ks.get("max_daily_loss_usd", 300.0))):
        reasons.append("max_daily_loss")

    conc = float(metrics.get("single_underlying_concentration_pct") or 0.0)
    if conc > float(ks.get("max_single_underlying_concentration_pct", 35.0)):
        reasons.append("single_underlying_concentration")

    margin = float(metrics.get("margin_utilization_pct") or 0.0)
    if margin > float(ks.get("max_margin_utilization_pct", 60.0)):
        reasons.append("margin_utilization")

    return len(reasons) > 0, reasons
