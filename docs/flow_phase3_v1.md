# flow_phase3_v1

This release adds configurable **event-time flow features**, **OUT_ outcome labels**, and optional **flow gate filters** for debit spread backtests/ML.

## What changed

## 1) Flow engine (`options_ai/features/flow_engine.py`)

Core functions:
- `robust_z(x, v)` = `0.6745 * (v - median(x)) / MAD(x)`
- `window_robust_z(history, v, win)`
- `sigmoid(x)`
- `winsorize_p99(values)`
- `compute_options_flow(contracts, spot, args, strike_history, bucket_series)`

Flow computation:
- premium proxy: `volume * mid`
- per-strike net flow: `call_premium - put_premium`
- strike distance filter from spot
- optional moneyness weighting (gaussian/linear)
- optional p99 winsorization
- strike-level + bucket-level robust-z
- confidence from confirm windows

Parity guard for `flow_live_ok_default`:
- not `Strong Bullish`
- bucket robust-z `< 2.0`
- skew `< 0.20`
- `(pctBull - pctBear) < 0.20`
- not `(confidence >= 0.75 and bullish bias)`

## 2) Phase2 features (0DTE + term)

Updated:
- `options_ai/phase2_builder.py`
- `options_ai/phase2_term_builder.py`

Added feature columns (idempotent migration):
- Buckets: `itm_vol, atm_vol, otm_vol, tot_vol`
- Deltas: `d_tot_vol, d_call_oi, d_put_oi`
- Rolling: `sma_spot_5, sma_spot_20, bb_mid_20, bb_upper_20, bb_lower_20, bb_pctb_20, rsi_14, twap_spot_day, vwap_chainweighted_spot_day`
- OS/OB labels: `rsi_osob_label, bb_osob_label, pcr_oi_osob_label`
- Flow fields: all `flow_*` columns

Determinism and no leakage:
- feature calculations only use current and prior snapshots
- missing snapshot candidates are processed in event-time order

## 3) Outcomes tables (trading-day aligned)

New module: `options_ai/outcomes_phase3.py`

New tables:
- `spx.chain_outcomes_underlying`
- `spx.chain_outcomes_atm_options`

Supported:
- Horizons (trading days): default `5,10,21`
- Alignment: `FirstOfDay | SameTime | LastOfDay`
- Underlying return: `(future_spot - spot_t) / spot_t`
- Label with flat band (`FLAT_PCT_BAND`, default `0.0025`): `Up | Flat | Down`
- ATM option MTM for call/put using nearest expiration `>= eval_date`, nearest ATM strike at t0
- Missing-data flags for expiration/t0 quote/future quote

## 4) Debit spread ML + backtest

Updated:
- `options_ai/debit_spread_ml.py`
  - includes new rolling/flow fields
  - default model version: `debit_ridge_v2_flow`

- `options_ai/backtest/debit_spreads.py`
  - optional flow gate (default OFF)
  - CALL gate:
    - bias in `{Moderate Bullish, Strong Bullish}`
    - bucket z `>= threshold`
    - breadth `>= threshold`
    - confidence `>= threshold`
  - PUT gate mirrors bearish direction
  - optional `flow_live_ok_default` filter (default OFF)

Also updated `options_ai/backtest/registry.py` to accept gate params.

## 5) New env knobs

Config additions in `options_ai/config.py` and env loaders:
- `FLOW_MAX_STRIKE_DIST_PCT` (0.40)
- `FLOW_USE_MONEYNESS_WEIGHT` (true)
- `FLOW_USE_GAUSSIAN` (true)
- `FLOW_WINSORIZE_BUCKET` (true)
- `FLOW_CONFIRM_FAST_WIN` (10)
- `FLOW_CONFIRM_SLOW_WIN` (60)
- `FLOW_HISTORY_PER_STRIKE` (600)
- `FLOW_BUCKET_Z_WINDOW` (60)
- `FLOW_MIN_BREADTH` (0.60)
- `FLOW_MIN_BUCKET_Z` (1.5)
- `FLOW_CONF_MIN` (0.60)
- `FLOW_ATM_CORRIDOR_PCT` (0.01)
- `OUTCOME_ALIGN` (`FirstOfDay`)
- `OUTCOME_HORIZONS_TD` (`5,10,21`)
- `FLAT_PCT_BAND` (`0.0025`)
- `BACKTEST_FLOW_GATE_ENABLED` (false)
- `BACKTEST_FLOW_LIVE_OK_FILTER_ENABLED` (false)
- `BACKTEST_FLOW_GATE_MIN_BUCKET_Z` (1.5)
- `BACKTEST_FLOW_GATE_MIN_BREADTH` (0.60)
- `BACKTEST_FLOW_GATE_MIN_CONFIDENCE` (0.60)

## Backfill / recompute

1. Ensure schema changes are applied by running daemons once:
```bash
python -m options_ai.phase2_main
python -m options_ai.phase2_term_main
```

2. Backfill missing feature/label/outcome rows through normal daemon loops.

3. Re-train scoring model with flow fields:
```bash
python -m options_ai.debit_spread_ml_main
```

## Test

```bash
pytest -q tests/test_flow_engine.py
```


## Validation + monitoring scripts

- Validate schema/sanity/no-leak checks:
```bash
python scripts/validate_flow_phase3_v1.py --dsn "$SPX_CHAIN_DATABASE_URL"
```

- One-shot progress snapshot:
```bash
python scripts/monitor_flow_phase3_progress.py --dsn "$SPX_CHAIN_DATABASE_URL" --once
```

- Continuous progress monitor:
```bash
python scripts/monitor_flow_phase3_progress.py --dsn "$SPX_CHAIN_DATABASE_URL" --interval 10
```

- Baseline vs gated canary backtest comparison:
```bash
/opt/OptionsPredicator/.venv/bin/python /opt/OptionsPredicator/scripts/run_flow_phase3_canary.py   --dsn "$SPX_CHAIN_DATABASE_URL" --auto-days 30 --horizon 30 --gate-live-ok
```

See `docs/flow_phase3_canary_rollout.md` for rollout/rollback details.
