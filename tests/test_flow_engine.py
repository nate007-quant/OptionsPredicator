from __future__ import annotations

from options_ai.features.flow_engine import compute_options_flow, robust_z, sigmoid, window_robust_z


class _Args:
    flow_max_strike_dist_pct = 0.4
    flow_use_moneyness_weight = True
    flow_use_gaussian = True
    flow_winsorize_bucket = True
    flow_confirm_fast_win = 10
    flow_confirm_slow_win = 60
    flow_history_per_strike = 600
    flow_bucket_z_window = 60
    flow_min_breadth = 0.60
    flow_min_bucket_z = 1.5
    flow_conf_min = 0.60
    flow_atm_corridor_pct = 0.01


def test_robust_z_mad_formula() -> None:
    x = [1.0, 2.0, 3.0, 4.0, 100.0]
    z = robust_z(x, 4.0)
    assert z is not None
    # median=3, mad=1 -> 0.6745*(1)/1
    assert abs(z - 0.6745) < 1e-9


def test_window_robust_z_and_sigmoid() -> None:
    h = [1, 2, 2, 3, 100]
    z = window_robust_z(h, 3.0, win=4)
    assert z is not None
    assert sigmoid(0.0) == 0.5


def test_compute_options_flow_smoke_and_parity_flags() -> None:
    contracts = [
        {"side": "call", "strike": 5000, "volume": 100, "mid": 5.0},
        {"side": "put", "strike": 5000, "volume": 20, "mid": 4.5},
        {"side": "call", "strike": 5010, "volume": 10, "mid": 3.0},
        {"side": "put", "strike": 4990, "volume": 120, "mid": 3.0},
        {"side": "call", "strike": 5300, "volume": 999, "mid": 9.0},  # filtered by strike dist
    ]
    strike_history: dict[float, list[float]] = {
        5000.0: [100.0, 120.0, 130.0],
        5010.0: [20.0, 25.0, 30.0],
        4990.0: [-200.0, -150.0, -180.0],
    }
    bucket_series = [-40.0, -30.0, -20.0, -10.0]

    out = compute_options_flow(contracts, 5000.0, _Args(), strike_history, bucket_series)

    assert out["flow_total_strikes"] >= 3
    assert 0.0 <= out["flow_pct_bullish"] <= 1.0
    assert 0.0 <= out["flow_pct_bearish"] <= 1.0
    assert out["flow_bias_summary"] in {
        "Strong Bullish",
        "Moderate Bullish",
        "Neutral",
        "Moderate Bearish",
        "Strong Bearish",
    }

    # Deterministic + parity guard: live_ok_default implies all guard predicates
    if out["flow_live_ok_default"]:
        assert out["flow_bias_summary"] != "Strong Bullish"
        assert out["flow_bucket_robust_z"] < 2.0
        assert out["flow_skew"] < 0.20
        assert (out["flow_pct_bullish"] - out["flow_pct_bearish"]) < 0.20
