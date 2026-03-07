from __future__ import annotations

from adapter.current_backtest_adapter import CurrentBacktestAdapter


def test_adapter_contract_shapes():
    a = CurrentBacktestAdapter()
    spec = {
        "strategy_id": "s1",
        "version": "1",
        "name": "n",
        "structure_type": "VERTICAL",
        "underlyings": ["SPX"],
        "timeframe": {"bar": "1m"},
        "entry_window": {"timezone": "America/Chicago", "first_trade_time": "08:40", "last_new_entry_time": "09:30"},
        "regime_filters": {},
        "legs": [{"side": "BUY", "option_type": "CALL", "expiry_rule": {}, "strike_rule": {}, "qty_ratio": 1}],
        "position_sizing": {},
        "adjustments": {},
        "exits": {},
        "cost_model": {},
        "risk_limits": {},
        "validation_plan": {"train": {"start": "2026-01-01", "end": "2026-02-01"}, "test": {"start": "2026-02-02", "end": "2026-03-01"}},
    }
    v = a.validateSpec(spec)
    assert hasattr(v, "ok")
    assert isinstance(a.supports("early_assignment"), bool)
