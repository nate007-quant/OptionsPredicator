from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime
from typing import Any


MINUTES_PER_YEAR = 365.0 * 24.0 * 60.0


def _em_abs(spot: float, atm_iv: float, minutes: float) -> float:
    # em = spot * iv * sqrt(t)
    t = float(minutes) / float(MINUTES_PER_YEAR)
    if t <= 0:
        return 0.0
    return float(spot) * float(atm_iv) * math.sqrt(t)


def _std(vals: list[float]) -> float:
    if not vals:
        return 0.0
    m = sum(vals) / len(vals)
    v = sum((x - m) ** 2 for x in vals) / len(vals)
    return math.sqrt(v)


@dataclass(frozen=True)
class EodFeatures:
    features_version: str
    features: dict[str, float]


def build_eod_features(
    *,
    trade_day: str,
    open_price: float,
    early_end_price: float,
    early_prices: list[tuple[datetime, float]],
    atm_iv: float,
    spot_asof: float,
    expected_move_abs_15m: float,
    levels: dict[str, Any],
    gex_regime_label: str | None,
    early_window_minutes: int,
) -> EodFeatures:
    prices = [float(p) for _, p in early_prices]
    early_return_pts = float(early_end_price) - float(open_price)
    early_range_pts = (max(prices) - min(prices)) if prices else 0.0

    # 5-min returns std proxy
    rets = []
    for i in range(1, len(prices)):
        rets.append(prices[i] - prices[i - 1])
    vol_proxy = _std(rets)

    em_60 = _em_abs(spot_asof, atm_iv, float(early_window_minutes))
    em_30 = _em_abs(spot_asof, atm_iv, 30.0)
    em_day = _em_abs(spot_asof, atm_iv, 390.0)

    def lv(name: str) -> float:
        obj = levels.get(name)
        if isinstance(obj, dict) and obj.get("strike") is not None:
            try:
                return float(obj.get("strike"))
            except Exception:
                return 0.0
        return 0.0

    s_mag = lv("magnet")
    s_cw = lv("call_wall")
    s_pw = lv("put_wall")

    feats: dict[str, float] = {
        "open_price": float(open_price),
        "early_end_price": float(early_end_price),
        "early_return_pts": float(early_return_pts),
        "early_range_pts": float(early_range_pts),
        "early_vol_proxy": float(vol_proxy),
        "atm_iv": float(atm_iv),
        "spot_asof": float(spot_asof),
        "expected_move_abs_15m": float(expected_move_abs_15m),
        "expected_move_abs_60m": float(em_60),
        "expected_move_abs_30m": float(em_30),
        "expected_move_abs_day": float(em_day),
        "gex_regime_positive": 1.0 if (gex_regime_label == "positive_gamma") else 0.0,
        "gex_regime_negative": 1.0 if (gex_regime_label == "negative_gamma") else 0.0,
        "magnet_strike": float(s_mag),
        "call_wall_strike": float(s_cw),
        "put_wall_strike": float(s_pw),
        "dist_open_to_magnet": float(open_price) - float(s_mag),
        "dist_open_to_call_wall": float(open_price) - float(s_cw),
        "dist_open_to_put_wall": float(open_price) - float(s_pw),
        "dist_early_end_to_magnet": float(early_end_price) - float(s_mag),
        "dist_early_end_to_call_wall": float(early_end_price) - float(s_cw),
        "dist_early_end_to_put_wall": float(early_end_price) - float(s_pw),
    }

    # min distance early to levels
    def min_dist(strike: float) -> float:
        if not prices:
            return 0.0
        return float(min(abs(p - strike) for p in prices))

    feats["min_dist_early_to_magnet"] = min_dist(s_mag) if s_mag else 0.0
    feats["min_dist_early_to_call_wall"] = min_dist(s_cw) if s_cw else 0.0
    feats["min_dist_early_to_put_wall"] = min_dist(s_pw) if s_pw else 0.0

    return EodFeatures(features_version="ml_eod_features_v1", features=feats)
