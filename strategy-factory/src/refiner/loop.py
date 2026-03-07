from __future__ import annotations

import copy
from dataclasses import dataclass
from typing import Any

from adapter.base import BacktestAdapter
from compiler.spec_compiler import spec_hash
from evaluator.gates import evaluate_hard_gates
from evaluator.scoring import score_strategy


@dataclass
class RefineConfig:
    max_iterations: int = 8
    max_non_improving: int = 3


def mutate_spec(spec: dict[str, Any], i: int) -> dict[str, Any]:
    """Bounded mutation operators."""
    out = copy.deepcopy(spec)
    exits = out.setdefault("exits", {})
    regime = out.setdefault("regime_filters", {})

    # bounded knobs
    tp = float(exits.get("take_profit_pct", 1.5))
    sl = float(exits.get("stop_loss_pct", 0.5))
    iv_min = float(regime.get("iv_rank_min", 20))
    iv_max = float(regime.get("iv_rank_max", 80))

    if i % 3 == 0:
        exits["take_profit_pct"] = max(0.5, min(4.0, tp + 0.1))
    elif i % 3 == 1:
        exits["stop_loss_pct"] = max(0.2, min(2.0, sl + 0.05))
    else:
        regime["iv_rank_min"] = max(0, min(70, iv_min + 2))
        regime["iv_rank_max"] = max(regime["iv_rank_min"] + 5, min(100, iv_max - 1))

    # bounded DTE / structure switch within approved set
    st = str(out.get("structure_type") or "VERTICAL")
    approved = ["SINGLE", "VERTICAL", "STRADDLE", "STRANGLE", "IRON_CONDOR", "CALENDAR", "BUTTERFLY", "RATIO"]
    out["structure_type"] = approved[(approved.index(st) + 1) % len(approved)] if i % 7 == 0 and st in approved else st

    return out


def refine_strategy(
    *,
    adapter: BacktestAdapter,
    spec: dict[str, Any],
    request_base: dict[str, Any],
    gates_cfg: dict[str, Any],
    scoring_cfg: dict[str, Any],
    cfg: RefineConfig,
) -> tuple[dict[str, Any], dict[str, Any]]:
    best_spec = copy.deepcopy(spec)
    best_result = adapter.runBacktest({**request_base, "spec": best_spec})
    best_score, _ = score_strategy(best_result, scoring_cfg)

    non_improving = 0
    for i in range(1, cfg.max_iterations + 1):
        cand = mutate_spec(best_spec, i)
        cand_res = adapter.runBacktest({**request_base, "spec": cand})
        stress = [adapter.runStress({**request_base, "spec": cand}, s) for s in ["base", "adverse", "severe"]]
        ok, _, _ = evaluate_hard_gates(cand_res, gates_cfg, stress)
        cand_score, _ = score_strategy(cand_res, scoring_cfg)

        if ok and cand_score > best_score:
            best_spec = cand
            best_result = cand_res
            best_score = cand_score
            non_improving = 0
        else:
            non_improving += 1

        if non_improving >= cfg.max_non_improving:
            break

    best_result = dict(best_result)
    best_result["spec_hash"] = spec_hash(best_spec)
    return best_spec, best_result
