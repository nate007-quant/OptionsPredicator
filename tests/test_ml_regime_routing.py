from __future__ import annotations

from options_ai.regime import select_ml_model_version


def test_ml_route_uses_mapped_model_when_confident_and_exists():
    mv, reason = select_ml_model_version(
        regime_payload={"label": "trend_up", "confidence": 0.8},
        regime_enabled=True,
        regime_model_map={"trend_up": "ml_v1_trend_up"},
        fallback_model_version="ml_v1",
        min_confidence_for_routing=0.55,
        model_exists=lambda m: m == "ml_v1_trend_up",
    )
    assert mv == "ml_v1_trend_up"
    assert reason == "ml_regime_route"


def test_ml_route_fallback_low_confidence():
    mv, reason = select_ml_model_version(
        regime_payload={"label": "trend_up", "confidence": 0.4},
        regime_enabled=True,
        regime_model_map={"trend_up": "ml_v1_trend_up"},
        fallback_model_version="ml_v1",
        min_confidence_for_routing=0.55,
        model_exists=lambda _: True,
    )
    assert mv == "ml_v1"
    assert reason == "ml_regime_fallback_low_confidence"


def test_ml_route_fallback_missing_bundle():
    mv, reason = select_ml_model_version(
        regime_payload={"label": "trend_up", "confidence": 0.9},
        regime_enabled=True,
        regime_model_map={"trend_up": "ml_v1_trend_up"},
        fallback_model_version="ml_v1",
        min_confidence_for_routing=0.55,
        model_exists=lambda _: False,
    )
    assert mv == "ml_v1"
    assert reason == "ml_regime_fallback_missing_bundle"
