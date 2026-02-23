from __future__ import annotations

import math
from options_ai.utils.gex import compute_gex_structure, gex_to_signals
from dataclasses import dataclass
from typing import Any, Iterable


@dataclass(frozen=True)
class OptionRow:
    optionSymbol: str
    side: str  # "call" | "put"
    strike: float
    expiration_epoch: int
    bid: float | None
    ask: float | None
    mid: float | None
    openInterest: int | None
    volume: int | None
    iv: float | None
    delta: float | None
    gamma: float | None


def compute_put_call_ratio(rows: Iterable[OptionRow]) -> dict[str, float | None]:
    call_oi = 0
    put_oi = 0
    call_vol = 0
    put_vol = 0

    for r in rows:
        if r.side == "call":
            if r.openInterest is not None:
                call_oi += int(r.openInterest)
            if r.volume is not None:
                call_vol += int(r.volume)
        elif r.side == "put":
            if r.openInterest is not None:
                put_oi += int(r.openInterest)
            if r.volume is not None:
                put_vol += int(r.volume)

    oi_ratio = None if call_oi == 0 else (put_oi / call_oi)
    vol_ratio = None if call_vol == 0 else (put_vol / call_vol)

    return {
        "put_call_oi_ratio": oi_ratio,
        "put_call_volume_ratio": vol_ratio,
        "call_oi_sum": float(call_oi),
        "put_oi_sum": float(put_oi),
        "call_volume_sum": float(call_vol),
        "put_volume_sum": float(put_vol),
    }


def compute_atm_iv(rows: list[OptionRow], spot_price: float) -> float | None:
    iv_rows = [r for r in rows if r.iv is not None and isinstance(r.strike, (int, float))]
    if not iv_rows:
        return None

    iv_rows.sort(key=lambda r: abs(float(r.strike) - float(spot_price)))
    # average of closest few strikes
    k = min(6, len(iv_rows))
    vals = [float(iv_rows[i].iv) for i in range(k) if iv_rows[i].iv is not None]
    if not vals:
        return None
    return sum(vals) / len(vals)


def compute_expected_move(rows: list[OptionRow], spot_price: float, horizon_minutes: int = 15) -> dict[str, float | None]:
    atm_iv = compute_atm_iv(rows, spot_price)
    if atm_iv is None:
        return {"expected_move_abs": None, "expected_move_pct": None, "atm_iv": None}

    # Treat iv as annualized (e.g., 0.20). Expected move (1-sigma) over horizon.
    t_years = horizon_minutes / (365.0 * 24.0 * 60.0)
    em_abs = float(spot_price) * float(atm_iv) * math.sqrt(t_years)
    em_pct = em_abs / float(spot_price) if spot_price else None
    return {"expected_move_abs": em_abs, "expected_move_pct": em_pct, "atm_iv": float(atm_iv)}


def _parse_ohlcv(raw: Any) -> list[dict[str, float]]:
    """Accepts either list[dict] or dict of arrays. Returns list of bars with keys o,h,l,c,v."""
    if raw is None:
        return []

    if isinstance(raw, list):
        bars: list[dict[str, float]] = []
        for b in raw:
            if not isinstance(b, dict):
                continue
            if "c" in b and "v" in b:
                try:
                    bars.append(
                        {
                            "o": float(b.get("o")) if b.get("o") is not None else float("nan"),
                            "h": float(b.get("h")) if b.get("h") is not None else float("nan"),
                            "l": float(b.get("l")) if b.get("l") is not None else float("nan"),
                            "c": float(b.get("c")),
                            "v": float(b.get("v")) if b.get("v") is not None else 0.0,
                        }
                    )
                except Exception:
                    continue
        return bars

    if isinstance(raw, dict):
        # arrays style: {"c": [...], "v": [...], ...}
        c = raw.get("c")
        v = raw.get("v")
        if not isinstance(c, list) or not isinstance(v, list):
            return []
        n = min(len(c), len(v))
        bars = []
        for i in range(n):
            try:
                bars.append(
                    {
                        "o": float(raw.get("o")[i]) if isinstance(raw.get("o"), list) else float("nan"),
                        "h": float(raw.get("h")[i]) if isinstance(raw.get("h"), list) else float("nan"),
                        "l": float(raw.get("l")[i]) if isinstance(raw.get("l"), list) else float("nan"),
                        "c": float(c[i]),
                        "v": float(v[i]) if v[i] is not None else 0.0,
                    }
                )
            except Exception:
                continue
        return bars

    return []


def compute_volume_class(price_series_raw: Any) -> str | None:
    bars = _parse_ohlcv(price_series_raw)
    if len(bars) < 6:
        return None

    vols = [b["v"] for b in bars[-21:] if isinstance(b.get("v"), (int, float))]
    if len(vols) < 6:
        return None

    latest = vols[-1]
    baseline = sum(vols[:-1]) / max(1, len(vols) - 1)
    if baseline <= 0:
        return None

    ratio = latest / baseline
    if ratio >= 1.5:
        return "high"
    if ratio <= 0.7:
        return "low"
    return "normal"


def compute_trend(price_series_raw: Any) -> str | None:
    bars = _parse_ohlcv(price_series_raw)
    if len(bars) < 10:
        return None

    closes = [b["c"] for b in bars if isinstance(b.get("c"), (int, float))]
    if len(closes) < 10:
        return None

    last = closes[-1]
    ma_short = sum(closes[-5:]) / 5.0
    ma_long = sum(closes[-10:]) / 10.0

    # deterministic buckets
    if last > ma_short > ma_long:
        return "bullish"
    if last < ma_short < ma_long:
        return "bearish"
    return "choppy"


def detect_unusual_activity(rows: list[OptionRow]) -> dict[str, Any]:
    # Flags contracts where volume is unusually large relative to OI.
    ratios: list[tuple[float, OptionRow]] = []
    for r in rows:
        if r.volume is None or r.openInterest is None:
            continue
        oi = float(r.openInterest)
        vol = float(r.volume)
        if oi <= 0:
            continue
        ratios.append((vol / oi, r))

    ratios.sort(key=lambda x: x[0], reverse=True)
    top = ratios[:5]
    flagged = [
        {
            "optionSymbol": r.optionSymbol,
            "side": r.side,
            "strike": r.strike,
            "volume": r.volume,
            "openInterest": r.openInterest,
            "vol_oi_ratio": round(float(ratio), 4),
        }
        for ratio, r in top
        if ratio >= 1.0
    ]

    return {
        "unusual_activity_count": len(flagged),
        "unusual_activity_top": flagged,
    }


def compute_all_signals(rows: list[OptionRow], spot_price: float, price_series_raw: Any) -> dict[str, Any]:
    pcr = compute_put_call_ratio(rows)
    em = compute_expected_move(rows, spot_price, horizon_minutes=15)
    gex = compute_gex_structure(rows, spot_price)
    return {
        **pcr,
        **em,
        "trend": compute_trend(price_series_raw),
        "volume_class": compute_volume_class(price_series_raw),
        **detect_unusual_activity(rows),
        **gex_to_signals(gex),
        "gex_net_by_strike": gex.net_gex_by_strike,
        "gex_abs_by_strike": gex.abs_gex_by_strike,
    }
