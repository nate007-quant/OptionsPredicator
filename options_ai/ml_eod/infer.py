from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np

from options_ai.ml_eod.store import EodModelBundle


@dataclass(frozen=True)
class EodInferenceOut:
    pred_dir: str  # bullish|neutral|bearish
    pred_conf: float
    pred_move_pts: float
    p_action: float
    event_probs: dict[str, float]


def infer(
    *,
    bundle: EodModelBundle,
    features: dict[str, float],
    feature_keys: list[str],
    action_threshold: float,
    band_eod_pts: float,
) -> EodInferenceOut:
    x = np.array([[float(features.get(k, 0.0)) for k in feature_keys]], dtype=float)

    # Action probability
    try:
        p_action = float(bundle.action_clf.predict_proba(x)[0][1])
    except Exception:
        p_action = float(bundle.action_clf.predict(x)[0])
        p_action = max(0.0, min(1.0, p_action))

    # Move regression
    pred_move = float(bundle.move_reg.predict(x)[0])

    pred_dir = "neutral"
    if p_action >= float(action_threshold) and abs(pred_move) >= float(band_eod_pts):
        pred_dir = "bullish" if pred_move > 0 else "bearish"

    # Event probs
    event_probs: dict[str, float] = {}
    for k, clf in (bundle.event_clfs or {}).items():
        try:
            event_probs[k] = float(clf.predict_proba(x)[0][1])
        except Exception:
            try:
                event_probs[k] = float(clf.predict(x)[0])
            except Exception:
                event_probs[k] = 0.0

    return EodInferenceOut(
        pred_dir=pred_dir,
        pred_conf=float(p_action),
        pred_move_pts=float(abs(pred_move)) if pred_dir != "neutral" else 0.0,
        p_action=float(p_action),
        event_probs=event_probs,
    )
