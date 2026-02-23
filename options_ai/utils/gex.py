from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, TYPE_CHECKING

if TYPE_CHECKING:
    from options_ai.utils.signals import OptionRow


@dataclass(frozen=True)
class GexStructure:
    call_wall_strike: float | None
    put_wall_strike: float | None
    magnet_strike: float | None
    flip_strike: float | None
    regime_label: str | None  # positive_gamma | negative_gamma

    distance_to_call_wall: float | None  # abs pct distance (fraction)
    distance_to_put_wall: float | None
    distance_to_magnet: float | None
    distance_to_flip: float | None

    net_gex_by_strike: dict[str, float]
    abs_gex_by_strike: dict[str, float]


def _pct_dist(spot: float, strike: float) -> float | None:
    if spot <= 0:
        return None
    return abs(float(strike) - float(spot)) / float(spot)


def compute_gex_structure(rows: Iterable[Any], spot_price: float) -> GexStructure:
    """Compute basic GEX structural levels per v1.6 spec.

    contrib = gamma * openInterest * 100 * spot^2 * sign
      sign = +1 for calls, -1 for puts

    Notes:
    - Rows missing gamma or openInterest are ignored.
    - Output keys are deterministic.
    """

    spot = float(spot_price)

    net_by_k: dict[float, float] = {}
    for r in rows:
        gamma = getattr(r, "gamma", None)
        oi_val = getattr(r, "openInterest", None)
        strike_val = getattr(r, "strike", None)
        side = getattr(r, "side", None)
        if gamma is None or oi_val is None or strike_val is None or side is None:
            continue
        try:
            g = float(gamma)
            oi = float(oi_val)
            k = float(strike_val)
        except Exception:
            continue
        sign = 1.0 if str(side) == "call" else -1.0
        contrib = g * oi * 100.0 * (spot * spot) * sign
        net_by_k[k] = net_by_k.get(k, 0.0) + float(contrib)

    if not net_by_k:
        return GexStructure(
            call_wall_strike=None,
            put_wall_strike=None,
            magnet_strike=None,
            flip_strike=None,
            regime_label=None,
            distance_to_call_wall=None,
            distance_to_put_wall=None,
            distance_to_magnet=None,
            distance_to_flip=None,
            net_gex_by_strike={},
            abs_gex_by_strike={},
        )

    call_wall = max(net_by_k.items(), key=lambda kv: kv[1])[0]
    put_wall = min(net_by_k.items(), key=lambda kv: kv[1])[0]

    abs_by_k = {k: abs(v) for k, v in net_by_k.items()}

    lo = spot * 0.99
    hi = spot * 1.01
    in_band = [(k, abs_by_k[k]) for k in abs_by_k.keys() if lo <= k <= hi]
    magnet = None
    if in_band:
        magnet = max(in_band, key=lambda kv: kv[1])[0]

    # Flip: cumulative crossing of net GEX
    strikes = sorted(net_by_k.keys())
    cum = 0.0
    cum_series: list[tuple[float, float]] = []
    for k in strikes:
        cum += net_by_k[k]
        cum_series.append((k, cum))

    flip = None
    best_k = None
    best_abs = None

    prev_k, prev_c = cum_series[0]
    best_k, best_abs = prev_k, abs(prev_c)

    if prev_c == 0:
        flip = prev_k
    else:
        for k, c in cum_series[1:]:
            if abs(c) < (best_abs or float("inf")):
                best_k, best_abs = k, abs(c)
            if prev_c == 0:
                flip = prev_k
                break
            if prev_c * c < 0:
                flip = k if abs(c) <= abs(prev_c) else prev_k
                break
            prev_k, prev_c = k, c

    if flip is None:
        flip = best_k

    total_net = sum(net_by_k.values())
    regime = "positive_gamma" if total_net >= 0 else "negative_gamma"

    net_str = {str(int(k)) if float(k).is_integer() else str(k): float(v) for k, v in net_by_k.items()}
    abs_str = {str(int(k)) if float(k).is_integer() else str(k): float(v) for k, v in abs_by_k.items()}

    return GexStructure(
        call_wall_strike=float(call_wall),
        put_wall_strike=float(put_wall),
        magnet_strike=float(magnet) if magnet is not None else None,
        flip_strike=float(flip) if flip is not None else None,
        regime_label=regime,
        distance_to_call_wall=_pct_dist(spot, float(call_wall)),
        distance_to_put_wall=_pct_dist(spot, float(put_wall)),
        distance_to_magnet=_pct_dist(spot, float(magnet)) if magnet is not None else None,
        distance_to_flip=_pct_dist(spot, float(flip)) if flip is not None else None,
        net_gex_by_strike=net_str,
        abs_gex_by_strike=abs_str,
    )


def gex_to_signals(g: GexStructure) -> dict[str, Any]:
    return {
        "gex_call_wall_strike": g.call_wall_strike,
        "gex_put_wall_strike": g.put_wall_strike,
        "gex_magnet_strike": g.magnet_strike,
        "gex_flip_strike": g.flip_strike,
        "gex_regime_label": g.regime_label,
        "gex_distance_to_call_wall": g.distance_to_call_wall,
        "gex_distance_to_put_wall": g.distance_to_put_wall,
        "gex_distance_to_magnet": g.distance_to_magnet,
        "gex_distance_to_flip": g.distance_to_flip,
    }
