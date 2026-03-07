from __future__ import annotations

from options_ai.execution.intent_builder import ExecutionIntentBuilder


def test_idempotency_key_deterministic() -> None:
    params_a = {"a": 1, "b": 2}
    params_b = {"b": 2, "a": 1}

    k1 = ExecutionIntentBuilder._idempotency_key(
        source_type="backtest_run",
        source_id=42,
        strategy_key="debit_spreads:anchor_based:exp0dte",
        params=params_a,
        environment="sandbox",
    )
    k2 = ExecutionIntentBuilder._idempotency_key(
        source_type="backtest_run",
        source_id=42,
        strategy_key="debit_spreads:anchor_based:exp0dte",
        params=params_b,
        environment="sandbox",
    )
    assert k1 == k2


def test_tp_sl_extraction_from_credit_params() -> None:
    params = {
        "credit_take_profit_pct": 0.5,
        "credit_stop_loss_mult": 2.0,
    }
    out = ExecutionIntentBuilder._tp_sl_from_params(params)
    assert out["take_profit_pct"] == 0.5
    assert out["stop_loss"] == 2.0
    assert out["stop_loss_kind"] == "mult"
