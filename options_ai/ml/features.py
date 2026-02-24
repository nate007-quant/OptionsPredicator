from __future__ import annotations

from typing import Any


def _as_float(x: Any) -> float | None:
    if x is None:
        return None
    try:
        return float(x)
    except Exception:
        return None


def _as_int(x: Any) -> int | None:
    if x is None:
        return None
    try:
        return int(x)
    except Exception:
        try:
            return int(float(x))
        except Exception:
            return None


def _encode_trend(trend: Any) -> float:
    t = (trend or "").strip().lower() if isinstance(trend, str) else ""
    if t == "bullish":
        return 1.0
    if t == "bearish":
        return -1.0
    return 0.0


def _encode_volume_class(vc: Any) -> float:
    v = (vc or "").strip().lower() if isinstance(vc, str) else ""
    if v == "high":
        return 1.0
    if v == "low":
        return -1.0
    if v == "normal":
        return 0.5
    return 0.0


def build_features(snapshot_summary: dict[str, Any], model_signals: dict[str, Any]) -> dict[str, float]:
    """Build a deterministic flat numeric feature dict.

    Uses only event-time snapshot_summary + deterministic model_signals (no future leakage).
    """

    spot = _as_float(snapshot_summary.get("spot_price")) or 0.0

    gex = (model_signals.get("gex") or {}) if isinstance(model_signals.get("gex"), dict) else {}
    levels = (gex.get("levels") or {}) if isinstance(gex.get("levels"), dict) else {}

    def lv(name: str) -> tuple[float | None, float | None]:
        obj = levels.get(name)
        if not isinstance(obj, dict):
            return None, None
        return _as_float(obj.get("strike")), _as_float(obj.get("distance_pct"))

    call_wall, dist_call = lv("call_wall")
    put_wall, dist_put = lv("put_wall")
    magnet, dist_mag = lv("magnet")
    flip, dist_flip = lv("flip")

    regime_label = (gex.get("regime_label") or "") if isinstance(gex.get("regime_label"), str) else ""

    feats: dict[str, float] = {
        # spot/vol
        "spot": float(spot),
        "expected_move_abs": float(_as_float(model_signals.get("expected_move_abs")) or 0.0),
        "expected_move_pct": float(_as_float(model_signals.get("expected_move_pct")) or 0.0),
        "atm_iv": float(_as_float(model_signals.get("atm_iv")) or 0.0),

        # pcr / flow
        "put_call_oi_ratio": float(_as_float(model_signals.get("put_call_oi_ratio")) or 0.0),
        "put_call_volume_ratio": float(_as_float(model_signals.get("put_call_volume_ratio")) or 0.0),
        "unusual_activity_count": float(_as_int(model_signals.get("unusual_activity_count")) or 0),

        # trend/volume
        "trend_enc": float(_encode_trend(model_signals.get("trend"))),
        "volume_class_enc": float(_encode_volume_class(model_signals.get("volume_class"))),

        # gex regime + levels
        "gex_regime_positive": 1.0 if regime_label == "positive_gamma" else 0.0,
        "gex_regime_negative": 1.0 if regime_label == "negative_gamma" else 0.0,
        "gex_call_wall_strike": float(call_wall or 0.0),
        "gex_put_wall_strike": float(put_wall or 0.0),
        "gex_magnet_strike": float(magnet or 0.0),
        "gex_flip_strike": float(flip or 0.0),
        "gex_call_wall_dist_pct": float(dist_call or 0.0),
        "gex_put_wall_dist_pct": float(dist_put or 0.0),
        "gex_magnet_dist_pct": float(dist_mag or 0.0),
        "gex_flip_dist_pct": float(dist_flip or 0.0),
    }

    # Relative features
    if spot:
        feats["spot_minus_flip"] = float(spot - float(flip or spot))
        feats["spot_minus_magnet"] = float(spot - float(magnet or spot))
        feats["spot_minus_call_wall"] = float(spot - float(call_wall or spot))
        feats["spot_minus_put_wall"] = float(spot - float(put_wall or spot))
    else:
        feats["spot_minus_flip"] = 0.0
        feats["spot_minus_magnet"] = 0.0
        feats["spot_minus_call_wall"] = 0.0
        feats["spot_minus_put_wall"] = 0.0

    return feats
