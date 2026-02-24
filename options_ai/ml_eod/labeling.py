from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from options_ai.ml_eod.features import _em_abs


@dataclass(frozen=True)
class EodLabels:
    dir_eod: str  # bullish|neutral|bearish
    move_eod_pts: float
    band_eod_pts: float
    level_events: dict[str, dict[str, int]]  # level -> {near,touch,reject}


def eod_direction_label(*, move_eod_pts: float, band_eod_pts: float) -> str:
    if move_eod_pts > band_eod_pts:
        return "bullish"
    if move_eod_pts < -band_eod_pts:
        return "bearish"
    return "neutral"


def compute_band_eod(*, spot: float, atm_iv: float, min_eod_band_pts: float, k_eod: float) -> float:
    em_day = _em_abs(float(spot), float(atm_iv), 390.0)
    return float(max(float(min_eod_band_pts), float(k_eod) * float(em_day)))


def level_event_labels(
    *,
    early_prices: list[tuple[datetime, float]],
    strike: float,
    expected_move_abs_60m: float,
    expected_move_abs_30m: float,
    min_near_pts: float,
    k_near: float,
    min_reject_pts: float,
    k_reject: float,
    reject_lookahead_minutes: int,
) -> dict[str, int]:
    if not early_prices or not strike:
        return {"near": 0, "touch": 0, "reject": 0}

    prices = [p for _, p in early_prices]

    # TOUCH: any segment crosses the strike
    touch = 0
    for i in range(1, len(prices)):
        if (prices[i - 1] - strike) * (prices[i] - strike) <= 0:
            touch = 1
            break

    min_dist = min(abs(p - strike) for p in prices)
    near_threshold = max(float(min_near_pts), float(k_near) * float(expected_move_abs_60m))
    near = 1 if (min_dist <= near_threshold) else 0

    # REJECT: first time near or touch, then rebound within lookahead window
    reject = 0
    if near or touch:
        # find hit time
        hit_i = None
        for i, (t, p) in enumerate(early_prices):
            if abs(p - strike) <= near_threshold:
                hit_i = i
                break
        if hit_i is None:
            hit_i = 0

        hit_t = early_prices[hit_i][0]
        end_t = hit_t + timedelta(minutes=int(reject_lookahead_minutes))

        rebound = 0.0
        for t, p in early_prices[hit_i:]:
            if t > end_t:
                break
            rebound = max(rebound, abs(p - strike))

        reject_threshold = max(float(min_reject_pts), float(k_reject) * float(expected_move_abs_30m))
        reject = 1 if rebound >= reject_threshold else 0

    return {"near": int(near), "touch": int(touch), "reject": int(reject)}
