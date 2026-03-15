# flow_phase3_v1 — Canary Rollout Plan

This is the next deployment stage after feature/outcome backfill:

1. run **baseline vs gated** comparison backtests,
2. review deltas,
3. enable gate flags in canary scope only,
4. monitor lags/coverage continuously.

## 1) Preconditions

- `spx_chain_phase2` and term phase2 services are running.
- outcomes backfill is active and near current.
- dashboard Processing tab is healthy.

Quick checks:

```bash
/opt/OptionsPredicator/.venv/bin/python /opt/OptionsPredicator/scripts/validate_flow_phase3_v1.py --dsn "$SPX_CHAIN_DATABASE_URL"
/opt/OptionsPredicator/.venv/bin/python /opt/OptionsPredicator/scripts/monitor_flow_phase3_progress.py --dsn "$SPX_CHAIN_DATABASE_URL" --once
```

## 2) Canary comparison (offline, no behavior change)

Run baseline vs flow-gated comparison over recent days:

```bash
/opt/OptionsPredicator/.venv/bin/python /opt/OptionsPredicator/scripts/run_flow_phase3_canary.py \
  --dsn "$SPX_CHAIN_DATABASE_URL" \
  --auto-days 30 \
  --horizon 30 \
  --gate-bucket-z 1.5 \
  --gate-breadth 0.60 \
  --gate-confidence 0.60 \
  --gate-live-ok
```

Output report is written to:
- `/mnt/options_ai/test_runs/flow_phase3_canary_*.json`

Inspect key deltas:
- `trades` (should drop moderately, not collapse)
- `win_rate`
- `avg_pnl_dollars`
- `cum_pnl_dollars`
- `max_drawdown_dollars`

## 3) Canary enablement (runtime flags)

By default, gating remains OFF. To canary-enable in runtime:

```bash
# in /opt/OptionsPredicator/.env
BACKTEST_FLOW_GATE_ENABLED=true
BACKTEST_FLOW_LIVE_OK_FILTER_ENABLED=true
BACKTEST_FLOW_GATE_MIN_BUCKET_Z=1.5
BACKTEST_FLOW_GATE_MIN_BREADTH=0.60
BACKTEST_FLOW_GATE_MIN_CONFIDENCE=0.60
```

Then restart relevant services:

```bash
sudo systemctl restart spx_debit_spreads.service spx_debit_ml.service
```

## 4) Monitor during canary

```bash
/opt/OptionsPredicator/.venv/bin/python /opt/OptionsPredicator/scripts/monitor_flow_phase3_progress.py \
  --dsn "$SPX_CHAIN_DATABASE_URL" --interval 10
```

In dashboard Processing tab, track:
- feature lag
- OUT_U lag
- OUT_ATM lag
- OUT_* missing(window)

## 5) Rollback

Immediate rollback to baseline behavior:

```bash
# in /opt/OptionsPredicator/.env
BACKTEST_FLOW_GATE_ENABLED=false
BACKTEST_FLOW_LIVE_OK_FILTER_ENABLED=false

sudo systemctl restart spx_debit_spreads.service spx_debit_ml.service
```
