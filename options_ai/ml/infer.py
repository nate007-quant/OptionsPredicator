from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

import numpy as np

from options_ai.ml.model_store import ModelBundle


@dataclass(frozen=True)
class MLInferenceOut:
    predicted_direction: str  # bullish|bearish|neutral
    predicted_magnitude: float  # points
    confidence: float  # p_action
    diagnostics: dict[str, Any]


def _neutral_band_pts(expected_move_abs: float, *, min_band_pts: float, k_em: float) -> float:
    try:
        em = float(expected_move_abs)
    except Exception:
        em = 0.0
    return float(max(float(min_band_pts), float(k_em) * em))


def infer(
    *,
    bundle: ModelBundle,
    features: dict[str, float],
    feature_keys: list[str],
    expected_move_abs: float,
    min_neutral_band_pts: float,
    k_em: float,
    action_threshold: float,
) -> MLInferenceOut:
    """Two-stage conservative inference.

    confidence := p_action
    """

    x = np.array([[float(features.get(k, 0.0)) for k in feature_keys]], dtype=float)

    # classifier probability of action
    try:
        proba = bundle.classifier.predict_proba(x)
        p_action = float(proba[0][1])
    except Exception:
        # fallback: classifier might only implement decision_function
        p_action = float(bundle.classifier.predict(x)[0])
        p_action = max(0.0, min(1.0, p_action))

    pred_move_pts = float(bundle.regressor.predict(x)[0])

    neutral_band = _neutral_band_pts(expected_move_abs, min_band_pts=min_neutral_band_pts, k_em=k_em)

    direction = "neutral"
    magnitude = 0.0

    if p_action >= float(action_threshold):
        if abs(pred_move_pts) >= neutral_band:
            direction = "bullish" if pred_move_pts > 0 else "bearish"
            magnitude = float(abs(pred_move_pts))

    return MLInferenceOut(
        predicted_direction=direction,
        predicted_magnitude=float(magnitude),
        confidence=float(p_action),
        diagnostics={
            "p_action": float(p_action),
            "pred_move_pts": float(pred_move_pts),
            "neutral_band_pts": float(neutral_band),
            "action_threshold": float(action_threshold),
        },
    )
