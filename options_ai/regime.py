from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo


REGIME_LABELS = (
    "trend_up",
    "trend_down",
    "pin_mean_revert",
    "vol_expansion_breakout",
    "event_unstable",
)


@dataclass(frozen=True)
class RegimeResult:
    label: str
    confidence: float
    version: str
    reasons: list[str]
    inputs_used: dict[str, Any]


def _f(x: Any) -> float | None:
    if x is None:
        return None
    try:
        return float(x)
    except Exception:
        return None


def _i(x: Any) -> int | None:
    if x is None:
        return None
    try:
        return int(x)
    except Exception:
        try:
            return int(float(x))
        except Exception:
            return None


def _clamp01(x: float) -> float:
    if x < 0.0:
        return 0.0
    if x > 1.0:
        return 1.0
    return float(x)


def session_flags_from_observed_utc(observed_utc: str | None) -> dict[str, float]:
    if not observed_utc:
        return {"session_open": 0.0, "session_lunch": 0.0, "session_power_hour": 0.0}
    try:
        dt = datetime.fromisoformat(str(observed_utc).replace("Z", "+00:00"))
        et = dt.astimezone(ZoneInfo("America/New_York"))
        mins = et.hour * 60 + et.minute
        return {
            "session_open": 1.0 if (9 * 60 + 30) <= mins < (10 * 60 + 30) else 0.0,
            "session_lunch": 1.0 if (12 * 60) <= mins < (13 * 60 + 30) else 0.0,
            "session_power_hour": 1.0 if (15 * 60) <= mins <= (16 * 60) else 0.0,
        }
    except Exception:
        return {"session_open": 0.0, "session_lunch": 0.0, "session_power_hour": 0.0}


def _compute_raw_regime(
    snapshot_summary: dict[str, Any],
    model_signals: dict[str, Any],
    extra_ctx: dict[str, Any] | None,
    *,
    version: str,
) -> RegimeResult:
    extra_ctx = extra_ctx or {}
    observed_utc = snapshot_summary.get("observed_utc") if isinstance(snapshot_summary, dict) else None
    sess = session_flags_from_observed_utc(observed_utc if isinstance(observed_utc, str) else None)

    trend = (model_signals.get("trend") or "").strip().lower() if isinstance(model_signals.get("trend"), str) else ""
    volume_class = (model_signals.get("volume_class") or "").strip().lower() if isinstance(model_signals.get("volume_class"), str) else ""
    expected_move_pct = _f(model_signals.get("expected_move_pct"))
    expected_move_abs = _f(model_signals.get("expected_move_abs"))
    atm_iv = _f(model_signals.get("atm_iv"))
    pcr_oi = _f(model_signals.get("put_call_oi_ratio"))
    pcr_vol = _f(model_signals.get("put_call_volume_ratio"))
    unusual = _i(model_signals.get("unusual_activity_count")) or 0

    gex = model_signals.get("gex") if isinstance(model_signals.get("gex"), dict) else {}
    gex_label = (gex.get("regime_label") or "").strip().lower() if isinstance(gex.get("regime_label"), str) else ""
    levels = gex.get("levels") if isinstance(gex.get("levels"), dict) else {}

    def _dist(name: str) -> float | None:
        obj = levels.get(name)
        if not isinstance(obj, dict):
            return None
        return _f(obj.get("distance_pct"))

    dist_magnet = _dist("magnet")
    dist_flip = _dist("flip")

    chain_features = extra_ctx.get("chain_features_0dte") if isinstance(extra_ctx.get("chain_features_0dte"), dict) else {}
    low_quality = bool(chain_features.get("low_quality", False))
    contract_count = _i(chain_features.get("contract_count"))
    valid_mid_count = _i(chain_features.get("valid_mid_count"))
    skew_25d = _f(chain_features.get("skew_25d"))
    bf_25d = _f(chain_features.get("bf_25d"))
    atm_spread = _f(chain_features.get("atm_bidask_spread"))

    quality_penalty = 0.0
    reasons: list[str] = []

    if not bool(snapshot_summary.get("has_ohlcv", False)):
        quality_penalty += 0.25
        reasons.append("ohlcv_missing")

    if low_quality:
        quality_penalty += 0.35
        reasons.append("chain_low_quality")

    if contract_count is not None and contract_count < 80:
        quality_penalty += 0.10
        reasons.append("contract_count_low")

    if valid_mid_count is not None and contract_count and valid_mid_count < max(20, int(contract_count * 0.4)):
        quality_penalty += 0.15
        reasons.append("mid_quotes_sparse")

    if atm_spread is not None and atm_spread > 1.5:
        quality_penalty += 0.10
        reasons.append("atm_spread_wide")

    scores = {k: 0.0 for k in REGIME_LABELS}

    if trend == "bullish":
        scores["trend_up"] += 0.65
    if trend == "bearish":
        scores["trend_down"] += 0.65

    if volume_class == "high":
        scores["vol_expansion_breakout"] += 0.20
    if unusual >= 3:
        scores["vol_expansion_breakout"] += 0.25
        scores["event_unstable"] += 0.20

    if expected_move_pct is not None:
        if expected_move_pct >= 0.010:
            scores["vol_expansion_breakout"] += 0.35
        elif expected_move_pct <= 0.004:
            scores["pin_mean_revert"] += 0.20

    if expected_move_abs is not None and expected_move_abs >= 45:
        scores["vol_expansion_breakout"] += 0.15

    if atm_iv is not None and atm_iv >= 0.28:
        scores["event_unstable"] += 0.20

    if pcr_oi is not None and pcr_vol is not None:
        if pcr_oi > 1.4 and pcr_vol > 1.2:
            scores["trend_down"] += 0.25
        elif pcr_oi < 0.8 and pcr_vol < 0.9:
            scores["trend_up"] += 0.25

    if gex_label == "positive_gamma":
        scores["pin_mean_revert"] += 0.20
    elif gex_label == "negative_gamma":
        scores["vol_expansion_breakout"] += 0.30

    if dist_magnet is not None and abs(dist_magnet) <= 0.0015:
        scores["pin_mean_revert"] += 0.25
    if dist_flip is not None and abs(dist_flip) <= 0.0015:
        scores["event_unstable"] += 0.15

    if sess.get("session_open", 0.0) > 0:
        scores["vol_expansion_breakout"] += 0.10
    if sess.get("session_lunch", 0.0) > 0:
        scores["pin_mean_revert"] += 0.10
    if sess.get("session_power_hour", 0.0) > 0:
        scores["event_unstable"] += 0.10

    if skew_25d is not None and skew_25d > 0.06:
        scores["trend_down"] += 0.08
    if bf_25d is not None and abs(bf_25d) > 0.03:
        scores["event_unstable"] += 0.08

    best_label = max(scores.keys(), key=lambda k: scores[k])
    sorted_scores = sorted(scores.items(), key=lambda kv: kv[1], reverse=True)
    top = sorted_scores[0][1]
    second = sorted_scores[1][1] if len(sorted_scores) > 1 else 0.0

    base_conf = 0.50 + min(0.40, max(0.0, top - second))
    conf = _clamp01(base_conf - quality_penalty)

    if top < 0.30:
        best_label = "event_unstable"
        conf = min(conf, 0.45)
        reasons.append("weak_signal_consensus")

    reasons.append(f"top_score={top:.3f}")
    reasons.append(f"runner_up={second:.3f}")

    inputs_used = {
        "trend": trend,
        "volume_class": volume_class,
        "expected_move_abs": expected_move_abs,
        "expected_move_pct": expected_move_pct,
        "atm_iv": atm_iv,
        "put_call_oi_ratio": pcr_oi,
        "put_call_volume_ratio": pcr_vol,
        "unusual_activity_count": unusual,
        "gex_regime_label": gex_label,
        "gex_dist_magnet": dist_magnet,
        "gex_dist_flip": dist_flip,
        "session_flags": sess,
        "quality_penalty": quality_penalty,
        "chain_low_quality": low_quality,
    }

    return RegimeResult(label=best_label, confidence=conf, version=version, reasons=reasons, inputs_used=inputs_used)


def classify_regime(snapshot_summary: dict[str, Any], model_signals: dict[str, Any], extra_ctx: dict[str, Any] | None = None) -> dict[str, Any]:
    extra_ctx = extra_ctx or {}
    version = str(extra_ctx.get("regime_version") or "regime_v1")
    hysteresis_n = max(1, int(extra_ctx.get("hysteresis_snapshots") or 2))
    prior_labels = [str(x) for x in (extra_ctx.get("prior_labels") or []) if isinstance(x, str)]

    raw = _compute_raw_regime(snapshot_summary, model_signals, extra_ctx, version=version)

    final_label = raw.label
    reasons = list(raw.reasons)

    if prior_labels and prior_labels[0] != raw.label:
        needed = max(1, hysteresis_n - 1)
        same_new = 0
        for lbl in prior_labels:
            if lbl == raw.label:
                same_new += 1
            else:
                break
        if same_new < needed:
            final_label = prior_labels[0]
            reasons.append("hysteresis_hold")
        else:
            reasons.append("hysteresis_switch")

    return {
        "label": final_label,
        "confidence": _clamp01(raw.confidence),
        "version": version,
        "reasons": reasons,
        "inputs_used": raw.inputs_used,
    }


# Requested alias compatibility
classify_reg = classify_regime


def select_ml_model_version(
    *,
    regime_payload: dict[str, Any] | None,
    regime_enabled: bool,
    regime_model_map: dict[str, str] | None,
    fallback_model_version: str,
    min_confidence_for_routing: float,
    model_exists: callable[[str], bool],
) -> tuple[str, str]:
    if not regime_enabled:
        return fallback_model_version, "ml_regime_disabled"

    payload = regime_payload or {}
    label = str(payload.get("label") or "")
    confidence = _f(payload.get("confidence")) or 0.0

    if confidence < float(min_confidence_for_routing):
        return fallback_model_version, "ml_regime_fallback_low_confidence"

    mapped = (regime_model_map or {}).get(label)
    if not mapped:
        return fallback_model_version, "ml_regime_fallback_unmapped"

    if not model_exists(mapped):
        return fallback_model_version, "ml_regime_fallback_missing_bundle"

    return mapped, "ml_regime_route"
