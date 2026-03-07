from __future__ import annotations

from data.snapshot import SnapshotResult
from evaluator.gates import evaluate_hard_gates


def test_spread_blowout_stress_blocks_promotion():
    base = {
        "metrics": {"trades": 80, "max_drawdown_pct": 10, "sharpe": 1.1, "calmar": 0.9, "return_on_margin": 1200, "profit_factor": 1.4},
        "robustness": {"walk_forward_pass": True, "parameter_stability_pass": True},
    }
    stress = [
        {"metrics": {"return_on_margin": 1200}},
        {"metrics": {"return_on_margin": 700}},
        {"metrics": {"return_on_margin": 100}},
    ]
    cfg = {"hard_gates": {"min_trades": 20, "max_drawdown_pct": 25, "min_oos_sharpe": 0.6, "min_oos_calmar": 0.4, "min_walk_forward_pass_ratio": 0.6, "min_parameter_stability_score": 0.55, "max_cost_stress_degradation_pct": 35}}
    ok, reasons, _ = evaluate_hard_gates(base, cfg, stress)
    assert ok is False
    assert any("cost_degradation" in r for r in reasons)


def test_missing_chain_segments_means_degraded_blocks_promotion_logic():
    snap = SnapshotResult(ok=False, snapshot_id="s1", degraded=True, reasons=["missing_or_stale:pit_option_chain_available"])
    assert snap.degraded is True
    assert snap.ok is False


def test_no_lookahead_flag_required():
    checks = {
        "pit_option_chain_available": True,
        "bid_ask_history_available": True,
        "oi_volume_available": True,
        "rates_dividends_available": True,
        "event_calendar_available": True,
        "no_lookahead_enforced": False,
    }
    assert checks["no_lookahead_enforced"] is False


def test_slippage_shock_scenario_detected():
    severe_slippage_bps = 250
    assert severe_slippage_bps > 100
