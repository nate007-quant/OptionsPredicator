# Autonomous Options Strategy Factory (MVP -> Production-Ready)

This module builds a daily, options-native strategy factory that:

1. snapshots/validates data integrity
2. generates options strategy ideas (single + multi-leg structures)
3. compiles ideas into strict `StrategySpec`
4. backtests via adapter against current engine
5. auto-refines weak strategies
6. enforces hard validation gates + scoring
7. transitions lifecycle states with guardrails
8. writes immutable audit + daily/weekly reports

---

## Layout

```
strategy-factory/
  configs/
  schemas/
  src/
    orchestrator/ data/ generator/ compiler/ adapter/ evaluator/ refiner/ risk/ reporting/
  tests/
    unit/ integration/ redteam/
  artifacts/
    runs/
```

---

## Quick start

From repo root:

```bash
# one-shot daily pipeline run
python strategy-factory/src/orchestrator/run_daily.py --repo-root .

# with explicit live approval for this run
python strategy-factory/src/orchestrator/run_daily.py --repo-root . --approve-live
```

Artifacts are written to:

- `strategy-factory/artifacts/runs/<run_id>/audit.ndjson`
- `strategy-factory/artifacts/runs/<run_id>/daily_summary.json`
- `strategy-factory/artifacts/runs/weekly_digest.json` (Sunday UTC)

---

## Scheduling (00:05 UTC default)

Configured in `configs/scheduler.yaml`.

Use cron/systemd timer to call:

```bash
python /opt/OptionsPredicator/strategy-factory/src/orchestrator/run_daily.py --repo-root /opt/OptionsPredicator
```

Scheduler features:
- lock file (skip if run already active)
- bounded retries
- max runtime cutoff

Systemd timer files are included:
- `systemd/strategy_factory_daily.service`
- `systemd/strategy_factory_daily.timer`

---

## Backtest adapter contract

`src/adapter/base.py` defines:

- `validateSpec(spec)`
- `runBacktest(request)`
- `runStress(request, scenario)`
- `supports(feature)`

`CurrentBacktestAdapter` translates `StrategySpec` to the current backtest engine (`options_ai.backtest.registry`), normalizes output into `BacktestResult`, and records capability gaps.

---

## Lifecycle state machine

Exact states:

`PROPOSED -> SPECIFIED -> TESTED -> ROBUST -> CANDIDATE -> PAPER -> LIVE -> RETIRED`

Guardrails:
- `PAPER -> LIVE` requires explicit approval flag
- severe breach can force retirement
- degraded data blocks promotion beyond `TESTED`

---

## Hard gates + scoring

Config-driven in:
- `configs/validation-gates.yaml`
- `configs/scoring.yaml`

Stress scenarios enforced:
- base / adverse / severe

No `ROBUST` promotion if any hard gate fails.

---

## Risk governance and autonomy

`configs/autonomy-policy.yaml` controls:
- Tier0 auto research flow
- Tier1 auto+alert paper actions
- Tier2 approval-required live/capital/risk changes
- global kill-switch thresholds

---

## Data integrity

Snapshot step validates point-in-time safety checks and records degraded mode.

If required data is missing/stale, run is marked degraded and promotions are blocked.

For testing degraded behavior:

```bash
FORCE_DEGRADED_SNAPSHOT=true python strategy-factory/src/orchestrator/run_daily.py --repo-root .
```

---

## Failure recovery

1. Check latest run folder under `artifacts/runs/`
2. Inspect `audit.ndjson` and `daily_summary.json`
3. Fix data/adapter issue
4. Re-run one-shot pipeline

---

## Live approval workflow

- Default autonomous promotions stop at `PAPER`.
- To allow `PAPER -> LIVE`, run with `--approve-live` and satisfy policy gates.
- Keep strategy execution controls (kill switches / interlocks) enabled in `options_ai` execution services.

---

## Assumptions

- Current engine best supports spread-like structures; unsupported structures are proxied and tagged via `capability_gaps`.
- Stress scenarios are deterministic degradations in MVP and can be replaced by engine-native stress support later.
- YAML parsing uses PyYAML when available; fallback parser supports current config subset.


## AI idea generation (optional)

By default, idea generation is deterministic. To enable AI-assisted ideation:

1. Set API key:

```bash
export OPENAI_API_KEY=...
```

2. Enable one of:

- `configs/generator.yaml` -> `ai.enabled: true`, or
- env flag: `STRATEGY_FACTORY_AI_ENABLED=true`

Optional model override:

```bash
export STRATEGY_FACTORY_AI_MODEL=gpt-5.2
```

Safety rails remain unchanged: AI ideas must compile to valid `StrategySpec` and pass hard gates before any promotion.
