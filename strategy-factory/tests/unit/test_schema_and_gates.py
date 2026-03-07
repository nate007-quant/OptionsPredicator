from __future__ import annotations

from pathlib import Path

from compiler.spec_compiler import compile_idea_to_spec, validate_spec_runnable
from evaluator.gates import evaluate_hard_gates
from evaluator.scoring import score_strategy
from orchestrator.state_machine import validate_transition


def test_spec_validation_strict() -> None:
    idea = {"idea_id": "x1", "structure_type": "VERTICAL", "description": "x", "seed_params": {}}
    spec = compile_idea_to_spec(idea)
    ok, errs = validate_spec_runnable(spec, Path(__file__).resolve().parents[2] / "schemas" / "strategy-spec.schema.json")
    assert ok is True
    assert not errs


def test_gate_logic_fail_on_low_trades() -> None:
    res = {
        "metrics": {"trades": 5, "max_drawdown_pct": 10, "sharpe": 1.0, "calmar": 1.0, "return_on_margin": 100, "profit_factor": 1.5},
        "robustness": {"walk_forward_pass": True, "parameter_stability_pass": True},
    }
    cfg = {"hard_gates": {"min_trades": 20, "max_drawdown_pct": 25, "min_oos_sharpe": 0.6, "min_oos_calmar": 0.4, "min_walk_forward_pass_ratio": 0.6, "min_parameter_stability_score": 0.55, "max_cost_stress_degradation_pct": 35}}
    ok, reasons, flags = evaluate_hard_gates(res, cfg, [])
    assert ok is False
    assert any("trades" in r for r in reasons)
    assert flags["min_trade_count_pass"] is False


def test_scoring_non_negative() -> None:
    res = {
        "metrics": {"sharpe": 0.8, "calmar": 0.6, "return_on_margin": 500, "max_drawdown_pct": 10, "profit_factor": 1.2, "trades": 40},
        "robustness": {"parameter_stability_pass": True},
        "capability_gaps": [],
    }
    score, reasons = score_strategy(res, {"weights": {}, "penalties": {}})
    assert 0.0 <= score <= 1.0
    assert isinstance(reasons, list)


def test_state_transition_guardrail() -> None:
    tr = validate_transition("PAPER", "LIVE", approval_live=False)
    assert tr.allowed is False
