from __future__ import annotations

import math
from statistics import median
from typing import Any


def robust_z(x: list[float], v: float | None) -> float | None:
    """Median/MAD robust z-score.

    z = 0.6745 * (v - median(x)) / MAD(x)
    """
    if v is None:
        return None
    vals = [float(a) for a in x if a is not None]
    if not vals:
        return None
    med = float(median(vals))
    abs_dev = [abs(a - med) for a in vals]
    mad = float(median(abs_dev)) if abs_dev else 0.0
    if mad <= 0:
        return 0.0
    return 0.6745 * (float(v) - med) / mad


def window_robust_z(history: list[float], v: float | None, win: int) -> float | None:
    if v is None:
        return None
    if win <= 0:
        return robust_z(history, v)
    h = [float(a) for a in history[-int(win):] if a is not None]
    return robust_z(h, v)


def sigmoid(x: float) -> float:
    xx = max(min(float(x), 60.0), -60.0)
    return 1.0 / (1.0 + math.exp(-xx))


def winsorize_p99(values: list[float]) -> list[float]:
    if not values:
        return []
    abs_vals = sorted(abs(float(v)) for v in values)
    idx = max(0, min(len(abs_vals) - 1, int(math.floor(0.99 * (len(abs_vals) - 1)))))
    lim = abs_vals[idx]
    out: list[float] = []
    for v in values:
        fv = float(v)
        out.append(max(-lim, min(lim, fv)))
    return out


def _arg(args: Any, key: str, default: Any) -> Any:
    if isinstance(args, dict):
        return args.get(key, default)
    return getattr(args, key, default)


def _mid(c: dict[str, Any]) -> float | None:
    m = c.get("mid")
    if m is not None:
        try:
            return float(m)
        except Exception:
            return None
    b = c.get("bid")
    a = c.get("ask")
    try:
        if b is not None and a is not None and float(a) >= float(b):
            return 0.5 * (float(a) + float(b))
    except Exception:
        return None
    return None


def compute_options_flow(
    contracts: list[dict[str, Any]],
    spot: float | None,
    args: Any,
    strike_history: dict[float, list[float]],
    bucket_series: list[float],
) -> dict[str, Any]:
    """Compute flow-phase3 option flow metrics for one event-time snapshot.

    `strike_history` and `bucket_series` are mutable rolling stores and are updated
    in-place with current snapshot values.
    """

    if spot is None or spot <= 0:
        return {
            "flow_total_strikes": 0,
            "flow_pct_bullish": 0.0,
            "flow_pct_bearish": 0.0,
            "flow_breadth": 0.0,
            "flow_bucket_net_flow": 0.0,
            "flow_bucket_robust_z": 0.0,
            "flow_skew": 0.0,
            "flow_confidence": 0.0,
            "flow_atm_corridor_net": 0.0,
            "flow_atm_corridor_frac": 0.0,
            "flow_top3_share": 0.0,
            "flow_top5_share": 0.0,
            "flow_bias_summary": "Neutral",
            "flow_breadth_pass": False,
            "flow_bucketz_pass": False,
            "flow_live_ok_default": True,
            "flow_strike_net_map": {},
        }

    max_dist_pct = float(_arg(args, "flow_max_strike_dist_pct", 0.40))
    use_weight = bool(_arg(args, "flow_use_moneyness_weight", True))
    use_gaussian = bool(_arg(args, "flow_use_gaussian", True))
    winsor = bool(_arg(args, "flow_winsorize_bucket", True))
    z_fast_win = int(_arg(args, "flow_confirm_fast_win", 10))
    z_slow_win = int(_arg(args, "flow_confirm_slow_win", 60))
    history_per_strike = int(_arg(args, "flow_history_per_strike", 600))
    z_bucket_win = int(_arg(args, "flow_bucket_z_window", 60))
    min_breadth = float(_arg(args, "flow_min_breadth", 0.60))
    min_bucket_z = float(_arg(args, "flow_min_bucket_z", 1.5))
    atm_corridor_pct = float(_arg(args, "flow_atm_corridor_pct", 0.01))

    by_strike: dict[float, dict[str, float]] = {}

    for c in contracts:
        side = str(c.get("side") or "").lower()
        if side not in {"call", "put"}:
            continue
        try:
            strike = float(c.get("strike"))
        except Exception:
            continue

        dist_pct = abs(strike - float(spot)) / float(spot)
        if dist_pct > max_dist_pct:
            continue

        vol = c.get("volume")
        try:
            vol_f = float(vol) if vol is not None else 0.0
        except Exception:
            vol_f = 0.0
        if vol_f <= 0:
            continue

        mid = _mid(c)
        if mid is None or mid <= 0:
            continue

        premium = vol_f * float(mid)

        if use_weight:
            if use_gaussian:
                sigma = max(max_dist_pct / 2.0, 1e-6)
                w = math.exp(-0.5 * (dist_pct / sigma) ** 2)
            else:
                w = max(0.0, 1.0 - dist_pct / max(max_dist_pct, 1e-9))
            premium *= float(w)

        d = by_strike.setdefault(float(strike), {"call": 0.0, "put": 0.0})
        d[side] += float(premium)

    strike_nets: dict[float, float] = {k: float(v["call"] - v["put"]) for k, v in by_strike.items()}

    nets = list(strike_nets.values())
    if winsor and nets:
        clipped = winsorize_p99(nets)
        strike_nets = {k: float(v) for k, v in zip(strike_nets.keys(), clipped, strict=True)}
        nets = list(strike_nets.values())

    total = len(nets)
    bull_n = sum(1 for n in nets if n > 0)
    bear_n = sum(1 for n in nets if n < 0)
    pct_bull = float(bull_n / total) if total else 0.0
    pct_bear = float(bear_n / total) if total else 0.0
    breadth = max(pct_bull, pct_bear) if total else 0.0

    bucket_net = float(sum(nets)) if nets else 0.0
    bucket_z = window_robust_z(bucket_series, bucket_net, z_bucket_win)
    if bucket_z is None:
        bucket_z = 0.0

    pos_sum = float(sum(n for n in nets if n > 0))
    neg_sum = float(sum(-n for n in nets if n < 0))
    denom = pos_sum + neg_sum
    skew = float((pos_sum - neg_sum) / denom) if denom > 0 else 0.0

    confirm_count = 0
    for k, v in strike_nets.items():
        hist = strike_history.get(float(k), [])
        zf = window_robust_z(hist, v, z_fast_win)
        zs = window_robust_z(hist, v, z_slow_win)
        if zf is not None and zs is not None and (zf * zs) > 0 and abs(zf) >= 1.0 and abs(zs) >= 1.0:
            confirm_count += 1

        hist2 = list(hist)
        hist2.append(float(v))
        if len(hist2) > history_per_strike:
            hist2 = hist2[-history_per_strike:]
        strike_history[float(k)] = hist2

    if total > 0:
        confidence = float(confirm_count / total)
    else:
        confidence = 0.0

    abs_sorted = sorted((abs(float(v)) for v in nets), reverse=True)
    abs_total = float(sum(abs_sorted)) if abs_sorted else 0.0
    top3_share = float(sum(abs_sorted[:3]) / abs_total) if abs_total > 0 else 0.0
    top5_share = float(sum(abs_sorted[:5]) / abs_total) if abs_total > 0 else 0.0

    corridor = [
        v for k, v in strike_nets.items() if abs(float(k) - float(spot)) / float(spot) <= atm_corridor_pct
    ]
    atm_corridor_net = float(sum(corridor)) if corridor else 0.0
    atm_corridor_frac = float(atm_corridor_net / bucket_net) if bucket_net != 0 else 0.0

    if pct_bull >= 0.70 and bucket_z >= min_bucket_z:
        bias = "Strong Bullish"
    elif pct_bull >= 0.55:
        bias = "Moderate Bullish"
    elif pct_bear >= 0.70 and bucket_z <= -min_bucket_z:
        bias = "Strong Bearish"
    elif pct_bear >= 0.55:
        bias = "Moderate Bearish"
    else:
        bias = "Neutral"

    is_bullish_bias = bias in {"Strong Bullish", "Moderate Bullish"}

    live_ok = (
        bias != "Strong Bullish"
        and float(bucket_z) < 2.0
        and float(skew) < 0.20
        and (pct_bull - pct_bear) < 0.20
        and not (confidence >= 0.75 and is_bullish_bias)
    )

    bucket_series.append(bucket_net)
    if len(bucket_series) > max(history_per_strike, z_bucket_win):
        del bucket_series[: len(bucket_series) - max(history_per_strike, z_bucket_win)]

    return {
        "flow_total_strikes": int(total),
        "flow_pct_bullish": float(pct_bull),
        "flow_pct_bearish": float(pct_bear),
        "flow_breadth": float(breadth),
        "flow_bucket_net_flow": float(bucket_net),
        "flow_bucket_robust_z": float(bucket_z),
        "flow_skew": float(skew),
        "flow_confidence": float(confidence),
        "flow_atm_corridor_net": float(atm_corridor_net),
        "flow_atm_corridor_frac": float(atm_corridor_frac),
        "flow_top3_share": float(top3_share),
        "flow_top5_share": float(top5_share),
        "flow_bias_summary": str(bias),
        "flow_breadth_pass": bool(breadth >= min_breadth),
        "flow_bucketz_pass": bool(abs(float(bucket_z)) >= min_bucket_z),
        "flow_live_ok_default": bool(live_ok),
        "flow_strike_net_map": strike_nets,
    }
