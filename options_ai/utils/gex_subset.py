from __future__ import annotations

from bisect import bisect_left
from dataclasses import dataclass
from typing import Any


def _as_float_strike(k: str) -> float | None:
    try:
        return float(k)
    except Exception:
        return None


def _key_for_strike(x: float) -> str:
    # match compute_gex_structure formatting
    return str(int(x)) if float(x).is_integer() else str(x)


def _topk_abs_strikes(abs_map: dict[str, float], k: int) -> list[float]:
    if k <= 0:
        return []
    items: list[tuple[float, float]] = []
    for sk, v in abs_map.items():
        s = _as_float_strike(sk)
        if s is None:
            continue
        try:
            vv = float(v)
        except Exception:
            continue
        items.append((s, vv))

    items.sort(key=lambda t: t[1], reverse=True)
    out = []
    for s, _ in items[:k]:
        out.append(float(s))
    return out


def _neighbors(strikes_sorted: list[float], center: float, n: int) -> list[float]:
    if not strikes_sorted or n <= 0:
        return []

    # nearest index
    i = bisect_left(strikes_sorted, center)
    if i >= len(strikes_sorted):
        i = len(strikes_sorted) - 1
    if i > 0 and abs(strikes_sorted[i - 1] - center) < abs(strikes_sorted[i] - center):
        i = i - 1

    lo = max(0, i - n)
    hi = min(len(strikes_sorted), i + n + 1)
    return [float(x) for x in strikes_sorted[lo:hi]]


@dataclass(frozen=True)
class CompactGex:
    levels: dict[str, Any]
    regime_label: str | None
    subset: dict[str, Any]


def build_compact_gex(
    *,
    spot: float,
    net_map: dict[str, float],
    abs_map: dict[str, float],
    call_wall: float | None,
    put_wall: float | None,
    magnet: float | None,
    flip: float | None,
    dist_call_wall: float | None,
    dist_put_wall: float | None,
    dist_magnet: float | None,
    dist_flip: float | None,
    regime_label: str | None,
    neighbors_each_level: int,
    topk_abs: int,
    sticky_strikes: list[float] | None,
    sticky_day_max: int,
) -> CompactGex:
    strikes_sorted = sorted({_as_float_strike(k) for k in net_map.keys() if _as_float_strike(k) is not None})

    chosen: set[float] = set()

    # levels first
    level_pairs = [
        ("call_wall", call_wall, dist_call_wall),
        ("put_wall", put_wall, dist_put_wall),
        ("magnet", magnet, dist_magnet),
        ("flip", flip, dist_flip),
    ]

    levels: dict[str, Any] = {}
    for name, strike, dist in level_pairs:
        if strike is None:
            continue
        levels[name] = {"strike": float(strike), "distance_pct": float(dist) if dist is not None else None}
        chosen.add(float(strike))
        for nb in _neighbors(strikes_sorted, float(strike), int(neighbors_each_level)):
            chosen.add(float(nb))

    # top-k abs across chain
    for s in _topk_abs_strikes(abs_map, int(topk_abs)):
        chosen.add(float(s))

    # sticky strikes
    if sticky_strikes:
        for s in sticky_strikes[: max(0, int(sticky_day_max))]:
            try:
                chosen.add(float(s))
            except Exception:
                continue

    # final subset
    subset_strikes = sorted(chosen)

    # maps limited to subset
    net_subset: dict[str, float] = {}
    abs_subset: dict[str, float] = {}

    # Build float lookup from original maps
    net_f: dict[float, float] = {}
    abs_f: dict[float, float] = {}
    for k, v in net_map.items():
        s = _as_float_strike(k)
        if s is None:
            continue
        try:
            net_f[float(s)] = float(v)
        except Exception:
            pass
    for k, v in abs_map.items():
        s = _as_float_strike(k)
        if s is None:
            continue
        try:
            abs_f[float(s)] = float(v)
        except Exception:
            pass

    for s in subset_strikes:
        key = _key_for_strike(float(s))
        if float(s) in net_f:
            net_subset[key] = float(net_f[float(s)])
        if float(s) in abs_f:
            abs_subset[key] = float(abs_f[float(s)])

    subset = {
        "strikes": subset_strikes,
        "net_gex_by_strike": net_subset,
        "abs_gex_by_strike": abs_subset,
        "method": {
            "neighbors_each_level": int(neighbors_each_level),
            "topk_abs": int(topk_abs),
            "sticky_day_enabled": bool(sticky_strikes),
            "sticky_day_max": int(sticky_day_max),
        },
    }

    return CompactGex(levels=levels, regime_label=regime_label, subset=subset)
