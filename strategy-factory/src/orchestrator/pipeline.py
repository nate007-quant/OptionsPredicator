from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from adapter.current_backtest_adapter import CurrentBacktestAdapter
from common import FactoryContext, append_ndjson, ensure_dir, load_yaml_like, read_json, sha256_obj, utc_now_iso, write_json
from compiler.spec_compiler import compile_idea_to_spec, spec_hash, validate_spec_runnable
from data.snapshot import create_data_snapshot
from evaluator.gates import evaluate_hard_gates
from evaluator.scoring import classify_state_from_score, score_strategy
from generator.idea_generator import generate_ideas
from orchestrator.state_machine import validate_transition
from refiner.loop import RefineConfig, refine_strategy
from reporting.reporter import maybe_write_weekly_digest, write_daily_report
from risk.governance import enforce_autonomy_policy, global_kill_switch_triggered


@dataclass
class PipelineOptions:
    explicit_live_approval: bool = False
    max_ideas: int = 6


def _git_sha(repo_root: Path) -> str:
    try:
        return (
            subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=str(repo_root), stderr=subprocess.DEVNULL)
            .decode()
            .strip()
        )
    except Exception:
        return "unknown"


def build_context(*, repo_root: Path) -> FactoryContext:
    factory_root = repo_root / "strategy-factory"
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_dir = ensure_dir(factory_root / "artifacts" / "runs" / run_id)
    return FactoryContext(
        repo_root=repo_root,
        factory_root=factory_root,
        configs_dir=factory_root / "configs",
        schemas_dir=factory_root / "schemas",
        artifacts_dir=factory_root / "artifacts",
        run_id=run_id,
        run_dir=run_dir,
        code_version=_git_sha(repo_root),
    )


def _audit(ctx: FactoryContext, event: str, **fields: Any) -> None:
    append_ndjson(
        ctx.run_dir / "audit.ndjson",
        {
            "ts": utc_now_iso(),
            "run_id": ctx.run_id,
            "event": event,
            **fields,
        },
    )


def run_pipeline(*, repo_root: Path, opts: PipelineOptions | None = None) -> dict[str, Any]:
    opts = opts or PipelineOptions()
    ctx = build_context(repo_root=repo_root)

    gates_cfg = load_yaml_like(ctx.configs_dir / "validation-gates.yaml")
    scoring_cfg = load_yaml_like(ctx.configs_dir / "scoring.yaml")
    autonomy_cfg = load_yaml_like(ctx.configs_dir / "autonomy-policy.yaml")

    _audit(ctx, "pipeline_start", code_version=ctx.code_version)

    # 1) data snapshot + health
    snap = create_data_snapshot(run_dir=ctx.run_dir)
    _audit(ctx, "data_snapshot", snapshot_id=snap.snapshot_id, degraded=snap.degraded, reasons=snap.reasons)

    # 2) regime update (MVP placeholder)
    regime = {"market_regime": "neutral", "iv_env": "normal"}
    write_json(ctx.run_dir / "regime.json", regime)
    _audit(ctx, "regime_update", **regime)

    # 3) idea generation + spec compilation
    ideas = generate_ideas(run_id=ctx.run_id, max_ideas=opts.max_ideas)
    adapter = CurrentBacktestAdapter()

    strategies: list[dict[str, Any]] = []
    capability_gaps: dict[str, list[str]] = {}

    for idea in ideas:
        row: dict[str, Any] = {
            "idea_id": idea.idea_id,
            "state": "PROPOSED",
            "reasons": [],
            "score": 0.0,
        }
        _audit(ctx, "idea_proposed", idea_id=idea.idea_id, structure=idea.structure_type)

        spec = compile_idea_to_spec(idea.__dict__)
        ok_spec, spec_err = validate_spec_runnable(spec, ctx.schemas_dir / "strategy-spec.schema.json")
        v = adapter.validateSpec(spec)
        if not ok_spec or not v.ok:
            row["reasons"].extend(spec_err + v.errors)
            _audit(ctx, "spec_invalid", strategy_id=spec.get("strategy_id"), errors=row["reasons"])
            strategies.append(row)
            continue

        tr = validate_transition("PROPOSED", "SPECIFIED", approval_live=False)
        if not tr.allowed:
            row["reasons"].append(tr.reason)
            strategies.append(row)
            continue
        row["state"] = "SPECIFIED"
        row["strategy_id"] = spec["strategy_id"]
        row["spec_hash"] = spec_hash(spec)
        _audit(ctx, "specified", strategy_id=spec["strategy_id"], spec_hash=row["spec_hash"])

        # 4) backtest batch
        req = {"spec": spec, "data_snapshot_id": snap.snapshot_id}
        bt = adapter.runBacktest(req)
        stress = [adapter.runStress(req, s) for s in ["base", "adverse", "severe"]]
        _audit(ctx, "backtest_completed", strategy_id=spec["strategy_id"], run_id=bt.get("run_id"))

        tr2 = validate_transition("SPECIFIED", "TESTED", approval_live=False)
        if tr2.allowed:
            row["state"] = "TESTED"

        # 5) refinement loop for weak strategies
        gate_ok, gate_reasons, flags = evaluate_hard_gates(bt, gates_cfg, stress)
        if not gate_ok:
            refined_spec, refined_bt = refine_strategy(
                adapter=adapter,
                spec=spec,
                request_base={"data_snapshot_id": snap.snapshot_id},
                gates_cfg=gates_cfg,
                scoring_cfg=scoring_cfg,
                cfg=RefineConfig(max_iterations=8, max_non_improving=3),
            )
            spec = refined_spec
            bt = refined_bt
            stress = [adapter.runStress({"spec": spec, "data_snapshot_id": snap.snapshot_id}, s) for s in ["base", "adverse", "severe"]]
            gate_ok, gate_reasons, flags = evaluate_hard_gates(bt, gates_cfg, stress)
            _audit(ctx, "refinement_completed", strategy_id=spec["strategy_id"], gate_ok=gate_ok)

        # 6) scoring/ranking + state transitions
        score, score_reasons = score_strategy(bt, scoring_cfg)
        bt["decision"]["score"] = score

        if gate_ok:
            tr3 = validate_transition("TESTED", "ROBUST", approval_live=False)
            if tr3.allowed:
                row["state"] = "ROBUST"
            tr4 = validate_transition("ROBUST", "CANDIDATE", approval_live=False)
            if tr4.allowed:
                row["state"] = classify_state_from_score(score, scoring_cfg)
                if row["state"] == "CANDIDATE":
                    row["state"] = "CANDIDATE"
        else:
            row["reasons"].extend(gate_reasons)

        gaps = list(bt.get("capability_gaps") or [])
        if gaps:
            capability_gaps[spec["strategy_id"]] = gaps

        # Promotions with governance policy
        if row["state"] == "CANDIDATE":
            tr5 = validate_transition("CANDIDATE", "PAPER", approval_live=False)
            if tr5.allowed:
                row["state"] = "PAPER"

        if row["state"] == "PAPER":
            allow_live, why = enforce_autonomy_policy(target_state="LIVE", policy=autonomy_cfg, explicit_live_approval=opts.explicit_live_approval)
            if allow_live:
                tr6 = validate_transition("PAPER", "LIVE", approval_live=opts.explicit_live_approval)
                if tr6.allowed and opts.explicit_live_approval:
                    row["state"] = "LIVE"
            else:
                row["reasons"].append(why)

        row["score"] = score
        row["metrics"] = bt.get("metrics")
        row["gate_flags"] = flags
        row["reasons"].extend(score_reasons)

        # Data integrity hard block: no promotion beyond TESTED when snapshot degraded.
        if snap.degraded and row.get("state") in {"ROBUST", "CANDIDATE", "PAPER", "LIVE"}:
            row["state"] = "TESTED"
            row.setdefault("reasons", []).append("degraded_data_block")

        # severe breach retirement
        severe = (float((bt.get("metrics") or {}).get("max_drawdown_pct") or 0.0) > 60.0)
        if severe:
            row["state"] = "RETIRED"
            row["reasons"].append("severe_breach")

        _audit(ctx, "strategy_decision", strategy_id=spec["strategy_id"], state=row["state"], score=score, reasons=row["reasons"])
        strategies.append(row)

    # global kill checks for portfolio-level simulation snapshot
    agg = {
        "portfolio_drawdown_pct": max([float((s.get("metrics") or {}).get("max_drawdown_pct") or 0.0) for s in strategies] + [0.0]),
        "daily_loss_usd": 0.0,
        "single_underlying_concentration_pct": 0.0,
        "margin_utilization_pct": 0.0,
    }
    kill, kill_reasons = global_kill_switch_triggered(metrics=agg, policy=autonomy_cfg)
    if kill:
        for s in strategies:
            if s.get("state") == "LIVE":
                s["state"] = "PAPER"
                s.setdefault("reasons", []).append("rollback_live_drift")
        _audit(ctx, "global_kill_switch", reasons=kill_reasons)

    report = {
        "run_id": ctx.run_id,
        "timestamp_utc": utc_now_iso(),
        "data_snapshot_id": snap.snapshot_id,
        "code_version": ctx.code_version,
        "degraded": snap.degraded,
        "degraded_reasons": snap.reasons,
        "strategies": strategies,
        "capability_gaps": capability_gaps,
    }

    write_daily_report(run_dir=ctx.run_dir, report=report)
    maybe_write_weekly_digest(artifacts_runs_dir=ctx.artifacts_dir / "runs", latest_run_report=report)
    write_json(ctx.run_dir / "capability_gaps.json", capability_gaps)
    _audit(ctx, "pipeline_end", strategies=len(strategies))
    return report
