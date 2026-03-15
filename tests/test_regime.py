from __future__ import annotations

from options_ai.regime import classify_regime


def _base_snapshot(observed_utc: str = "2026-03-14T14:45:00+00:00", has_ohlcv: bool = True) -> dict:
    return {"observed_utc": observed_utc, "spot_price": 5000.0, "has_ohlcv": has_ohlcv}


def _base_signals() -> dict:
    return {
        "trend": "neutral",
        "volume_class": "normal",
        "expected_move_abs": 18.0,
        "expected_move_pct": 0.004,
        "atm_iv": 0.18,
        "put_call_oi_ratio": 1.0,
        "put_call_volume_ratio": 1.0,
        "unusual_activity_count": 0,
        "gex": {
            "regime_label": "positive_gamma",
            "levels": {
                "magnet": {"strike": 5000, "distance_pct": 0.0008},
                "flip": {"strike": 4998, "distance_pct": 0.0010},
            },
        },
    }


def test_regime_trend_up():
    s = _base_signals()
    s["trend"] = "bullish"
    s["put_call_oi_ratio"] = 0.7
    s["put_call_volume_ratio"] = 0.8
    out = classify_regime(_base_snapshot(), s, {"hysteresis_snapshots": 2, "prior_labels": []})
    assert out["label"] == "trend_up"


def test_regime_trend_down():
    s = _base_signals()
    s["trend"] = "bearish"
    s["put_call_oi_ratio"] = 1.5
    s["put_call_volume_ratio"] = 1.3
    out = classify_regime(_base_snapshot(), s, {"hysteresis_snapshots": 2, "prior_labels": []})
    assert out["label"] == "trend_down"


def test_regime_pin_mean_revert():
    s = _base_signals()
    out = classify_regime(_base_snapshot("2026-03-14T17:10:00+00:00"), s, {"hysteresis_snapshots": 2, "prior_labels": []})
    assert out["label"] == "pin_mean_revert"


def test_regime_vol_expansion_breakout():
    s = _base_signals()
    s["volume_class"] = "high"
    s["expected_move_pct"] = 0.015
    s["unusual_activity_count"] = 5
    s["gex"]["regime_label"] = "negative_gamma"
    out = classify_regime(_base_snapshot("2026-03-14T13:35:00+00:00"), s, {"hysteresis_snapshots": 2, "prior_labels": []})
    assert out["label"] == "vol_expansion_breakout"


def test_regime_missing_data_reduces_confidence():
    out = classify_regime(_base_snapshot(has_ohlcv=False), _base_signals(), {"hysteresis_snapshots": 2, "prior_labels": []})
    assert out["confidence"] < 0.7


def test_regime_low_quality_reduces_confidence():
    out = classify_regime(
        _base_snapshot(),
        _base_signals(),
        {
            "hysteresis_snapshots": 2,
            "prior_labels": [],
            "chain_features_0dte": {"low_quality": True, "contract_count": 20, "valid_mid_count": 5},
        },
    )
    assert out["confidence"] <= 0.5


def test_hysteresis_hold_previous_label():
    s = _base_signals()
    s["trend"] = "bearish"
    out = classify_regime(_base_snapshot(), s, {"hysteresis_snapshots": 3, "prior_labels": ["trend_up", "trend_up"]})
    assert out["label"] == "trend_up"
    assert "hysteresis_hold" in out["reasons"]


def test_hysteresis_switch_after_persistence():
    s = _base_signals()
    s["trend"] = "bearish"
    out = classify_regime(_base_snapshot(), s, {"hysteresis_snapshots": 3, "prior_labels": ["trend_down", "trend_down"]})
    assert out["label"] == "trend_down"
    assert "hysteresis_hold" not in out["reasons"]
