from __future__ import annotations

from pathlib import Path

import orchestrator.pipeline as pl


class _FakeAdapter:
    def validateSpec(self, spec):
        from adapter.base import ValidationResult

        return ValidationResult(ok=True, errors=[], warnings=[])

    def supports(self, feature: str) -> bool:
        return feature != "early_assignment"

    def runBacktest(self, request):
        spec = request["spec"]
        return {
            "run_id": "r1",
            "strategy_id": spec["strategy_id"],
            "spec_hash": "h",
            "data_snapshot_id": request.get("data_snapshot_id", "s"),
            "engine": "fake",
            "period": {"start": "2026-01-01", "end": "2026-02-01"},
            "metrics": {
                "cagr": 0.2,
                "sharpe": 1.2,
                "sortino": 1.3,
                "calmar": 0.8,
                "max_drawdown_pct": 12,
                "win_rate": 0.55,
                "profit_factor": 1.4,
                "trades": 60,
                "return_on_margin": 1200,
            },
            "options_metrics": {"assignment_events": 0, "spread_slippage": 0.02, "pnl_decomposition": {"delta": 1, "theta": 1, "vega": 1}},
            "robustness": {
                "oos_pass": True,
                "walk_forward_pass": True,
                "cost_stress_pass": True,
                "parameter_stability_pass": True,
                "min_trade_count_pass": True,
            },
            "decision": {"state": "TESTED", "score": 0.0, "reasons": []},
            "capability_gaps": [],
        }

    def runStress(self, request, scenario):
        out = self.runBacktest(request)
        out["run_id"] = f"r-{scenario}"
        if scenario == "severe":
            out["metrics"]["return_on_margin"] *= 0.8
        return out


def test_e2e_pipeline_to_robust(tmp_path: Path, monkeypatch):
    repo = tmp_path / "repo"
    repo.mkdir(parents=True)

    # copy factory folder only
    src_root = Path(__file__).resolve().parents[2]
    import shutil

    shutil.copytree(src_root / "configs", repo / "strategy-factory" / "configs", dirs_exist_ok=True)
    shutil.copytree(src_root / "schemas", repo / "strategy-factory" / "schemas", dirs_exist_ok=True)
    (repo / "strategy-factory" / "artifacts" / "runs").mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(pl, "CurrentBacktestAdapter", lambda: _FakeAdapter())

    out = pl.run_pipeline(repo_root=repo)
    assert out["strategies"]
    assert any(s.get("state") in {"ROBUST", "CANDIDATE", "PAPER"} for s in out["strategies"])
