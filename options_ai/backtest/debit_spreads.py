from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Any, Literal

from zoneinfo import ZoneInfo

import psycopg


EntryMode = Literal["first_n_minutes", "time_range"]
AnchorMode = Literal["ATM", "WALLS", "MAGNET", "ALL"]
AnchorPolicy = Literal["any", "opposite_wall", "same_wall"]
PriceMode = Literal["mid"]

StrategyMode = Literal["anchor_based", "structural_walls"]
LongLegMoneyness = Literal["ATM", "1_ITM"]
RotationFilter = Literal["none", "spot_delta_5m"]
ExpirationMode = Literal["0dte", "target_dte"]


@dataclass(frozen=True)
class DebitBacktestConfig:
    start_day: date
    end_day: date

    # --- expiration selection ---
    expiration_mode: ExpirationMode = "0dte"
    target_dte_days: int | None = None
    dte_tolerance_days: int = 2

    # --- shared / defaults ---
    horizon_minutes: int = 30

    entry_mode: EntryMode = "time_range"
    session_start_ct: str = "08:30"
    entry_first_n_minutes: int = 60
    entry_start_ct: str = "08:40"
    entry_end_ct: str = "09:30"

    max_trades_per_day: int = 1
    one_trade_at_a_time: bool = True

    max_debit_points: float = 5.0

    stop_loss_pct: float = 0.50
    take_profit_pct: float = 2.00

    max_future_lookahead_minutes: int = 120
    price_mode: PriceMode = "mid"

    tz_local: str = "America/Chicago"
    include_missing_exits: bool = False

    # --- anchor-based mode knobs ---
    strategy_mode: StrategyMode = "anchor_based"
    anchor_mode: AnchorMode = "ATM"
    anchor_policy: AnchorPolicy = "any"
    min_p_bigwin: float = 0.0
    min_pred_change: float = 0.0
    allowed_spreads: tuple[str, ...] = ("CALL", "PUT")

    # --- structural walls mode knobs ---
    enable_pw_trade: bool = True
    enable_cw_trade: bool = True
    long_leg_moneyness: LongLegMoneyness = "ATM"
    max_width_points: float = 25.0
    min_width_points: float = 5.0
    proximity_min_points: float = 0.0
    proximity_max_points: float = 30.0
    rotation_filter: RotationFilter = "spot_delta_5m"
    prefer_pw_on_tie: bool = True

    # 0=at/below wall, 1=one strike beyond wall, etc.
    short_put_offset_steps: int = 0
    short_call_offset_steps: int = 0


def _parse_hhmm(s: str) -> time:
    hh, mm = s.strip().split(":", 1)
    return time(int(hh), int(mm), 0)


def _daterange(d0: date, d1: date) -> list[date]:
    out: list[date] = []
    d = d0
    while d <= d1:
        out.append(d)
        d = d + timedelta(days=1)
    return out


def _anchor_mode_allowed(mode: AnchorMode) -> list[str]:
    if mode == "ATM":
        return ["ATM"]
    if mode == "WALLS":
        return ["CALL_WALL", "PUT_WALL"]
    if mode == "MAGNET":
        return ["MAGNET"]
    return ["ATM", "CALL_WALL", "PUT_WALL", "MAGNET"]


def _anchor_policy_sets(policy: AnchorPolicy) -> tuple[list[str] | None, list[str] | None]:
    """Return allowed anchors for (CALL spreads, PUT spreads)."""
    p = (policy or "any").lower()
    if p == "any":
        return None, None
    if p == "opposite_wall":
        return ["PUT_WALL", "MAGNET", "ATM"], ["CALL_WALL", "MAGNET", "ATM"]
    if p == "same_wall":
        return ["CALL_WALL", "MAGNET", "ATM"], ["PUT_WALL", "MAGNET", "ATM"]
    return None, None


def _mid(bid: float | None, ask: float | None, mid: float | None) -> float | None:
    if mid is not None:
        return float(mid)
    if bid is not None and ask is not None and ask >= bid:
        return 0.5 * (float(bid) + float(ask))
    return None


def _clamp(x: float, lo: float, hi: float) -> float:
    return float(min(max(float(x), float(lo)), float(hi)))


def _max_drawdown(equity: list[float]) -> float:
    peak = 0.0
    mdd = 0.0
    for x in equity:
        peak = max(peak, x)
        mdd = min(mdd, x - peak)
    return float(mdd)


def _fetch_snapshots_in_window(
    conn: psycopg.Connection,
    *,
    day_local: date,
    start_t: time,
    end_t: time,
    tz_local: str,
) -> list[datetime]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT snapshot_ts
            FROM spx.chain_features_0dte
            WHERE (snapshot_ts AT TIME ZONE %s)::date = %s::date
              AND (snapshot_ts AT TIME ZONE %s)::time >= %s::time
              AND (snapshot_ts AT TIME ZONE %s)::time < %s::time
            ORDER BY snapshot_ts ASC
            """,
            (tz_local, day_local.isoformat(), tz_local, start_t.strftime("%H:%M:%S"), tz_local, end_t.strftime("%H:%M:%S")),
        )
        return [r[0] for r in cur.fetchall()]


def _fetch_spot_and_exp(conn: psycopg.Connection, *, snapshot_ts: datetime) -> tuple[float | None, date | None]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT spot, expiration_date
            FROM spx.chain_features_0dte
            WHERE snapshot_ts = %s
            """,
            (snapshot_ts,),
        )
        r = cur.fetchone()
        if not r:
            return None, None
        return (float(r[0]) if r[0] is not None else None, r[1])


def _fetch_walls(conn: psycopg.Connection, *, snapshot_ts: datetime) -> tuple[float | None, float | None]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT put_wall, call_wall
            FROM spx.gex_levels_0dte
            WHERE snapshot_ts = %s
            """,
            (snapshot_ts,),
        )
        r = cur.fetchone()
        if not r:
            return None, None
        return (float(r[0]) if r[0] is not None else None, float(r[1]) if r[1] is not None else None)



def _fetch_levels(conn: psycopg.Connection, *, snapshot_ts: datetime) -> tuple[float | None, float | None, float | None]:
    """Return (put_wall, call_wall, magnet) from gex_levels_0dte for the snapshot.

    Note: gex_levels_0dte is computed for the 0DTE chain, but the levels are on SPX
    and can be reused as anchors for non-0DTE tenors (best-effort).
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT put_wall, call_wall, magnet
            FROM spx.gex_levels_0dte
            WHERE snapshot_ts = %s
            """,
            (snapshot_ts,),
        )
        r = cur.fetchone()
        if not r:
            return None, None, None
        pw = float(r[0]) if r[0] is not None else None
        cw = float(r[1]) if r[1] is not None else None
        mg = float(r[2]) if r[2] is not None else None
        return pw, cw, mg


def _pick_expiration_for_target_dte(
    conn: psycopg.Connection,
    *,
    snapshot_ts: datetime,
    target_dte_days: int,
    dte_tolerance_days: int,
    tz_local: str,
) -> date | None:
    """Pick an expiration_date closest to target DTE (within tolerance) for a snapshot.

    DTE is computed as (expiration_date - local_trade_date(snapshot_ts, tz_local)).days
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT expiration_date
            FROM (
              SELECT
                expiration_date,
                ABS((expiration_date - (snapshot_ts AT TIME ZONE %s)::date) - %s) AS dte_diff
              FROM spx.option_chain
              WHERE snapshot_ts = %s
              GROUP BY expiration_date
            ) t
            WHERE dte_diff <= %s
            ORDER BY dte_diff ASC, expiration_date ASC
            LIMIT 1
            """,
            (tz_local, int(target_dte_days), snapshot_ts, int(dte_tolerance_days)),
        )
        r = cur.fetchone()
        if not r:
            return None
        return r[0]


def _build_simple_debit_candidate(
    conn: psycopg.Connection,
    *,
    snapshot_ts: datetime,
    expiration_date: date,
    spread_type: str,
    strikes: list[float],
    anchor_strike: float,
    max_debit_points: float,
    max_short_candidates: int = 12,
) -> dict[str, Any] | None:
    """Build a simple debit spread around an anchor strike (best-effort, on-the-fly).

    CALL: long call @ anchor, short call above.
    PUT:  long put  @ anchor, short put  below.

    Returns candidate with long/short symbols and debit_points if tradable.
    """
    st = (spread_type or "").upper().strip()
    if st not in {"CALL", "PUT"}:
        return None

    side = "call" if st == "CALL" else "put"

    k_long = float(anchor_strike)
    if st == "CALL":
        short_strikes = [float(k) for k in strikes if float(k) > k_long][: int(max_short_candidates)]
    else:
        short_strikes = [float(k) for k in reversed(strikes) if float(k) < k_long][: int(max_short_candidates)]

    long_pick = _pick_contract_at_strike(conn, snapshot_ts=snapshot_ts, expiration_date=expiration_date, side=side, strike=k_long)
    if not long_pick:
        return None
    long_sym, long_mid = long_pick

    best = None
    for k_short in short_strikes:
        short_pick = _pick_contract_at_strike(conn, snapshot_ts=snapshot_ts, expiration_date=expiration_date, side=side, strike=float(k_short))
        if not short_pick:
            continue
        short_sym, short_mid = short_pick
        width = abs(float(k_short) - float(k_long))
        if width <= 0:
            continue
        debit = _clamp(float(long_mid - short_mid), 0.0, width)
        if debit <= 0 or debit > float(max_debit_points):
            continue
        cand = {
            "k_long": float(k_long),
            "k_short": float(k_short),
            "long_symbol": str(long_sym),
            "short_symbol": str(short_sym),
            "debit_points": float(debit),
            "width_points": float(width),
        }
        key = (float(debit), float(width))
        if best is None or key < best[0]:
            best = (key, cand)

    return best[1] if best else None


def _fetch_candidates_term_anchor_based(
    conn: psycopg.Connection,
    *,
    snapshots: list[datetime],
    cfg: DebitBacktestConfig,
    allowed_anchors: list[str],
    call_anchors: list[str] | None,
    put_anchors: list[str] | None,
) -> dict[datetime, list[dict[str, Any]]]:
    if not snapshots:
        return {}

    by_ts: dict[datetime, list[dict[str, Any]]] = {}
    exp_cache: dict[datetime, date | None] = {}

    for ts in snapshots:
        spot, _exp0 = _fetch_spot_and_exp(conn, snapshot_ts=ts)
        if spot is None:
            continue

        target = cfg.target_dte_days
        if target is None:
            # default to 1w if not provided
            target = 7

        if ts in exp_cache:
            exp = exp_cache[ts]
        else:
            exp = _pick_expiration_for_target_dte(
                conn,
                snapshot_ts=ts,
                target_dte_days=int(target),
                dte_tolerance_days=int(cfg.dte_tolerance_days),
                tz_local=cfg.tz_local,
            )
            exp_cache[ts] = exp

        if exp is None:
            continue

        strikes = _fetch_strikes(conn, snapshot_ts=ts, expiration_date=exp)
        if not strikes:
            continue

        pw, cw, mg = _fetch_levels(conn, snapshot_ts=ts)
        anchors: dict[str, float] = {"ATM": float(spot)}
        if pw is not None:
            anchors["PUT_WALL"] = float(pw)
        if cw is not None:
            anchors["CALL_WALL"] = float(cw)
        if mg is not None:
            anchors["MAGNET"] = float(mg)

        cands: list[dict[str, Any]] = []

        for anchor_type in allowed_anchors:
            if anchor_type not in anchors:
                continue
            anchor_level = float(anchors[anchor_type])
            k_anchor = _nearest_strike(strikes, anchor_level)
            if k_anchor is None:
                continue

            for spread_type in cfg.allowed_spreads:
                st = (spread_type or "").upper().strip()
                if st == "CALL" and call_anchors is not None and anchor_type not in call_anchors:
                    continue
                if st == "PUT" and put_anchors is not None and anchor_type not in put_anchors:
                    continue

                base = _build_simple_debit_candidate(
                    conn,
                    snapshot_ts=ts,
                    expiration_date=exp,
                    spread_type=st,
                    strikes=strikes,
                    anchor_strike=float(k_anchor),
                    max_debit_points=float(cfg.max_debit_points),
                )
                if not base:
                    continue

                cands.append(
                    {
                        "snapshot_ts": ts,
                        "anchor_type": anchor_type,
                        "spread_type": st,
                        "expiration_date": exp,
                        "anchor_strike": float(k_anchor),
                        "k_long": base["k_long"],
                        "k_short": base["k_short"],
                        "long_symbol": base["long_symbol"],
                        "short_symbol": base["short_symbol"],
                        "debit_points": base["debit_points"],
                        "pred_change": None,
                        "p_bigwin": None,
                    }
                )

        if cands:
            cands.sort(key=lambda x: float(x.get("debit_points") or 1e18))
            by_ts[ts] = cands

    return by_ts


def _fetch_strikes(conn: psycopg.Connection, *, snapshot_ts: datetime, expiration_date: date) -> list[float]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT strike
            FROM spx.option_chain
            WHERE snapshot_ts = %s AND expiration_date = %s AND strike IS NOT NULL
            ORDER BY strike ASC
            """,
            (snapshot_ts, expiration_date),
        )
        return [float(r[0]) for r in cur.fetchall()]


def _nearest_strike(strikes: list[float], target: float) -> float | None:
    if not strikes:
        return None
    best = None
    best_k = None
    for k in strikes:
        d = abs(float(k) - float(target))
        cand = (d, float(k))
        if best is None or cand < best:
            best = cand
            best_k = float(k)
    return best_k


def _next_up(strikes: list[float], k: float) -> float | None:
    for s in strikes:
        if float(s) > float(k):
            return float(s)
    return None


def _next_dn(strikes: list[float], k: float) -> float | None:
    for s in reversed(strikes):
        if float(s) < float(k):
            return float(s)
    return None


def _step_dn(strikes: list[float], k: float, steps: int) -> float | None:
    out = float(k)
    for _ in range(int(steps)):
        nxt = _next_dn(strikes, out)
        if nxt is None:
            return None
        out = nxt
    return out


def _step_up(strikes: list[float], k: float, steps: int) -> float | None:
    out = float(k)
    for _ in range(int(steps)):
        nxt = _next_up(strikes, out)
        if nxt is None:
            return None
        out = nxt
    return out


def _pick_contract_at_strike(
    conn: psycopg.Connection,
    *,
    snapshot_ts: datetime,
    expiration_date: date,
    side: str,
    strike: float,
) -> tuple[str, float] | None:
    """Pick a contract symbol + mid for a given side/strike.

    Prefers rows with a valid stored mid; falls back to bid/ask midpoint.
    Skips negative mids.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT option_symbol, bid, ask, mid
            FROM spx.option_chain
            WHERE snapshot_ts = %s
              AND expiration_date = %s
              AND side = %s
              AND strike = %s
            """,
            (snapshot_ts, expiration_date, side, float(strike)),
        )

        best = None
        best_sym = None
        best_mid = None
        for sym, bid, ask, md in cur.fetchall():
            m = _mid(bid, ask, md)
            if m is None:
                continue
            if m < 0:
                continue
            score = 0 if md is not None else 1
            cand = (score, float(m))
            if best is None or cand < best:
                best = cand
                best_sym = str(sym)
                best_mid = float(m)

        if best_sym is None or best_mid is None:
            return None
        return best_sym, best_mid


def _fetch_candidates_for_window(
    conn: psycopg.Connection,
    *,
    snapshots: list[datetime],
    horizon_minutes: int,
    max_debit_points: float,
    min_p_bigwin: float,
    min_pred_change: float,
    allowed_anchors: list[str],
    allowed_spreads: tuple[str, ...],
    call_anchors: list[str] | None,
    put_anchors: list[str] | None,
) -> dict[datetime, list[dict[str, Any]]]:
    if not snapshots:
        return {}

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
              c.snapshot_ts,
              c.anchor_type,
              c.spread_type,
              c.expiration_date,
              c.anchor_strike,
              c.k_long,
              c.k_short,
              c.long_symbol,
              c.short_symbol,
              c.debit_points,
              s.pred_change,
              s.p_bigwin
            FROM spx.debit_spread_candidates_0dte c
            JOIN spx.chain_features_0dte f
              ON f.snapshot_ts = c.snapshot_ts
            LEFT JOIN spx.debit_spread_scores_0dte s
              ON s.snapshot_ts = c.snapshot_ts
             AND s.horizon_minutes = %s
             AND s.anchor_type = c.anchor_type
             AND s.spread_type = c.spread_type
            WHERE c.snapshot_ts = ANY(%s)
              AND c.tradable = true
              AND f.low_quality = false
              AND c.debit_points > 0
              AND c.debit_points <= %s
              AND c.anchor_type = ANY(%s)
              AND c.spread_type = ANY(%s)
              AND (s.p_bigwin IS NULL OR s.p_bigwin >= %s)
              AND (s.pred_change IS NULL OR s.pred_change >= %s)
              AND (s.pred_change IS NULL OR s.pred_change > 0)
              AND (
                (%s::text[] IS NULL AND %s::text[] IS NULL)
                OR (c.spread_type='CALL' AND c.anchor_type = ANY(%s::text[]))
                OR (c.spread_type='PUT' AND c.anchor_type = ANY(%s::text[]))
              )
            ORDER BY
              c.snapshot_ts ASC,
              CASE WHEN s.p_bigwin IS NULL THEN 1 ELSE 0 END ASC,
              s.p_bigwin DESC NULLS LAST,
              CASE WHEN s.pred_change IS NULL THEN 1 ELSE 0 END ASC,
              s.pred_change DESC NULLS LAST,
              c.debit_points ASC NULLS LAST
            """,
            (
                int(horizon_minutes),
                snapshots,
                float(max_debit_points),
                allowed_anchors,
                list(allowed_spreads),
                float(min_p_bigwin),
                float(min_pred_change),
                call_anchors,
                put_anchors,
                call_anchors,
                put_anchors,
            ),
        )

        by_ts: dict[datetime, list[dict[str, Any]]] = {}
        for r in cur.fetchall():
            ts = r[0]
            by_ts.setdefault(ts, []).append(
                {
                    "snapshot_ts": ts,
                    "anchor_type": r[1],
                    "spread_type": r[2],
                    "expiration_date": r[3],
                    "anchor_strike": float(r[4]) if r[4] is not None else None,
                    "k_long": float(r[5]) if r[5] is not None else None,
                    "k_short": float(r[6]) if r[6] is not None else None,
                    "long_symbol": r[7],
                    "short_symbol": r[8],
                    "debit_points": float(r[9]) if r[9] is not None else None,
                    "pred_change": float(r[10]) if r[10] is not None else None,
                    "p_bigwin": float(r[11]) if r[11] is not None else None,
                }
            )
        return by_ts


def _select_best_candidate(cands: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not cands:
        return None
    return cands[0]  # pre-sorted by SQL


def _fetch_path_snapshots(
    conn: psycopg.Connection,
    *,
    entry_ts: datetime,
    horizon_minutes: int,
    max_future_lookahead_minutes: int,
    day_local: date,
    tz_local: str,
) -> tuple[list[datetime], datetime | None]:
    target = entry_ts + timedelta(minutes=int(horizon_minutes))
    max_ts = target + timedelta(minutes=int(max_future_lookahead_minutes))

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT snapshot_ts
            FROM spx.chain_features_0dte
            WHERE snapshot_ts >= %s
              AND snapshot_ts <= %s
              AND (snapshot_ts AT TIME ZONE %s)::date = %s::date
            ORDER BY snapshot_ts ASC
            """,
            (entry_ts, max_ts, tz_local, day_local.isoformat()),
        )
        snaps = [r[0] for r in cur.fetchall()]

    tH = None
    for ts in snaps:
        if ts >= target:
            tH = ts
            break
    return snaps, tH


def _fetch_option_mids(
    conn: psycopg.Connection,
    *,
    snapshot_ts: list[datetime],
    expiration_date: date,
    symbols: tuple[str, str],
) -> dict[tuple[datetime, str], float | None]:
    if not snapshot_ts:
        return {}

    long_sym, short_sym = symbols

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT snapshot_ts, option_symbol, bid, ask, mid
            FROM spx.option_chain
            WHERE snapshot_ts = ANY(%s)
              AND expiration_date = %s
              AND option_symbol = ANY(%s)
            """,
            (snapshot_ts, expiration_date, [long_sym, short_sym]),
        )

        out: dict[tuple[datetime, str], float | None] = {}
        for r in cur.fetchall():
            ts, sym, bid, ask, md = r
            m = _mid(bid, ask, md)
            if m is not None and m < 0:
                m = None
            out[(ts, sym)] = m
        return out


def _build_structural_candidate(
    conn: psycopg.Connection,
    *,
    snapshot_ts: datetime,
    expiration_date: date,
    strikes: list[float],
    spot: float,
    put_wall: float | None,
    call_wall: float | None,
    side: str,
    long_leg_moneyness: LongLegMoneyness,
    max_width_points: float,
    min_width_points: float,
    max_debit_points: float,
    short_offset_steps: int,
) -> dict[str, Any] | None:
    """Build a structural-walls debit spread using current price as the long leg anchor.

    PUT: long put at/near ATM, short put at/below PW (clamped by width).
    CALL: long call at/near ATM, short call at/above CW (clamped by width).

    Entry debit is clamped to [0,width].
    """

    k_atm = _nearest_strike(strikes, float(spot))
    if k_atm is None:
        return None

    if side == "put":
        if put_wall is None:
            return None

        k_long = float(k_atm)
        if long_leg_moneyness == "1_ITM":
            k_long = _next_up(strikes, k_long) or k_long

        # target strike at/below wall
        k_target = None
        for k in reversed(strikes):
            if float(k) <= float(put_wall):
                k_target = float(k)
                break
        if k_target is None:
            k_target = float(strikes[0])

        k_short = _step_dn(strikes, k_target, int(short_offset_steps))
        if k_short is None:
            return None

        if k_short >= k_long:
            k_short = _next_dn(strikes, k_long)
            if k_short is None:
                return None

        width = float(k_long - k_short)
        if width > float(max_width_points):
            k_short = _nearest_strike(strikes, float(k_long - max_width_points))
            if k_short is None:
                return None
            if k_short >= k_long:
                k_short = _next_dn(strikes, k_long)
                if k_short is None:
                    return None
            width = float(k_long - k_short)

        if width < float(min_width_points):
            target2 = float(k_long - min_width_points)
            k_short2 = None
            for k in reversed(strikes):
                if float(k) <= target2:
                    k_short2 = float(k)
                    break
            if k_short2 is None:
                return None
            k_short = k_short2
            width = float(k_long - k_short)

        if not (float(min_width_points) <= width <= float(max_width_points)):
            return None

        long_pick = _pick_contract_at_strike(conn, snapshot_ts=snapshot_ts, expiration_date=expiration_date, side="put", strike=k_long)
        short_pick = _pick_contract_at_strike(conn, snapshot_ts=snapshot_ts, expiration_date=expiration_date, side="put", strike=k_short)
        if not long_pick or not short_pick:
            return None
        long_sym, long_mid = long_pick
        short_sym, short_mid = short_pick

        debit = _clamp(float(long_mid - short_mid), 0.0, width)
        if debit <= 0 or debit > float(max_debit_points):
            return None

        return {
            "snapshot_ts": snapshot_ts,
            "direction": "PW_PUT",
            "anchor_type": "PUT_WALL",
            "spread_type": "PUT",
            "k_long": k_long,
            "k_short": k_short,
            "width": width,
            "long_symbol": long_sym,
            "short_symbol": short_sym,
            "entry_debit": debit,
            "spot": float(spot),
            "put_wall": float(put_wall),
            "call_wall": float(call_wall) if call_wall is not None else None,
        }

    # CALL
    if call_wall is None:
        return None

    k_long = float(k_atm)
    if long_leg_moneyness == "1_ITM":
        k_long = _next_dn(strikes, k_long) or k_long

    # target strike at/above wall
    k_target = None
    for k in strikes:
        if float(k) >= float(call_wall):
            k_target = float(k)
            break
    if k_target is None:
        k_target = float(strikes[-1])

    k_short = _step_up(strikes, k_target, int(short_offset_steps))
    if k_short is None:
        return None

    if k_short <= k_long:
        k_short = _next_up(strikes, k_long)
        if k_short is None:
            return None

    width = float(k_short - k_long)
    if width > float(max_width_points):
        k_short = _nearest_strike(strikes, float(k_long + max_width_points))
        if k_short is None:
            return None
        if k_short <= k_long:
            k_short = _next_up(strikes, k_long)
            if k_short is None:
                return None
        width = float(k_short - k_long)

    if width < float(min_width_points):
        target2 = float(k_long + min_width_points)
        k_short2 = None
        for k in strikes:
            if float(k) >= target2:
                k_short2 = float(k)
                break
        if k_short2 is None:
            return None
        k_short = k_short2
        width = float(k_short - k_long)

    if not (float(min_width_points) <= width <= float(max_width_points)):
        return None

    long_pick = _pick_contract_at_strike(conn, snapshot_ts=snapshot_ts, expiration_date=expiration_date, side="call", strike=k_long)
    short_pick = _pick_contract_at_strike(conn, snapshot_ts=snapshot_ts, expiration_date=expiration_date, side="call", strike=k_short)
    if not long_pick or not short_pick:
        return None
    long_sym, long_mid = long_pick
    short_sym, short_mid = short_pick

    debit = _clamp(float(long_mid - short_mid), 0.0, width)
    if debit <= 0 or debit > float(max_debit_points):
        return None

    return {
        "snapshot_ts": snapshot_ts,
        "direction": "CW_CALL",
        "anchor_type": "CALL_WALL",
        "spread_type": "CALL",
        "k_long": k_long,
        "k_short": k_short,
        "width": width,
        "long_symbol": long_sym,
        "short_symbol": short_sym,
        "entry_debit": debit,
        "spot": float(spot),
        "put_wall": float(put_wall) if put_wall is not None else None,
        "call_wall": float(call_wall),
    }


def run_backtest_debit_spreads(conn: psycopg.Connection, cfg: DebitBacktestConfig) -> dict[str, Any]:
    tz_local = cfg.tz_local

    # entry window
    session_start = _parse_hhmm(cfg.session_start_ct)
    if cfg.entry_mode == "first_n_minutes":
        start_t = session_start
        dt0 = datetime(2000, 1, 1, session_start.hour, session_start.minute, 0)
        dt1 = dt0 + timedelta(minutes=int(cfg.entry_first_n_minutes))
        end_t = dt1.time()
    else:
        start_t = _parse_hhmm(cfg.entry_start_ct)
        end_t = _parse_hhmm(cfg.entry_end_ct)

    allowed_anchors = _anchor_mode_allowed(cfg.anchor_mode)
    call_anchors, put_anchors = _anchor_policy_sets(cfg.anchor_policy)

    trades: list[dict[str, Any]] = []
    equity_curve: list[dict[str, Any]] = []

    cum_pnl = 0.0
    eq_points: list[float] = []

    strike_cache: dict[tuple[datetime, date], list[float]] = {}

    for day_local in _daterange(cfg.start_day, cfg.end_day):
        day_trades = 0
        in_position = False
        position_exit_ts: datetime | None = None

        snaps = _fetch_snapshots_in_window(conn, day_local=day_local, start_t=start_t, end_t=end_t, tz_local=tz_local)

        by_ts: dict[datetime, list[dict[str, Any]]] = {}
        if cfg.strategy_mode == "anchor_based":
            if cfg.expiration_mode == "0dte":
                by_ts = _fetch_candidates_for_window(
                    conn,
                    snapshots=snaps,
                    horizon_minutes=cfg.horizon_minutes,
                    max_debit_points=cfg.max_debit_points,
                    min_p_bigwin=cfg.min_p_bigwin,
                    min_pred_change=cfg.min_pred_change,
                    allowed_anchors=allowed_anchors,
                    allowed_spreads=cfg.allowed_spreads,
                    call_anchors=call_anchors,
                    put_anchors=put_anchors,
                )
            else:
                by_ts = _fetch_candidates_term_anchor_based(
                    conn,
                    snapshots=snaps,
                    cfg=cfg,
                    allowed_anchors=allowed_anchors,
                    call_anchors=call_anchors,
                    put_anchors=put_anchors,
                )

        for ts in snaps:
            if day_trades >= int(cfg.max_trades_per_day):
                break

            if cfg.one_trade_at_a_time and in_position:
                # Only resume looking for entries after exit time.
                if position_exit_ts is None or ts <= position_exit_ts:
                    continue
                in_position = False

            entry_ts = ts
            exp_date: date | None = None
            long_sym: str | None = None
            short_sym: str | None = None
            entry_debit: float | None = None
            width_points: float | None = None
            direction: str | None = None
            cand: dict[str, Any] | None = None

            if cfg.strategy_mode == "anchor_based":
                cand = _select_best_candidate(by_ts.get(ts, []))
                if not cand:
                    continue
                entry_debit = float(cand["debit_points"]) if cand.get("debit_points") is not None else None
                if entry_debit is None or entry_debit <= 0:
                    continue

                exp_date = cand["expiration_date"]
                long_sym = str(cand["long_symbol"])
                short_sym = str(cand["short_symbol"])

                # width for clamping (if missing, we won't clamp)
                if cand.get("k_long") is not None and cand.get("k_short") is not None:
                    width_points = abs(float(cand["k_short"]) - float(cand["k_long"]))

            else:
                # structural walls mode
                spot, exp0 = _fetch_spot_and_exp(conn, snapshot_ts=ts)
                if spot is None:
                    continue

                if cfg.expiration_mode == "0dte":
                    exp_date = exp0
                else:
                    target = cfg.target_dte_days if cfg.target_dte_days is not None else 7
                    exp_date = _pick_expiration_for_target_dte(
                        conn,
                        snapshot_ts=ts,
                        target_dte_days=int(target),
                        dte_tolerance_days=int(cfg.dte_tolerance_days),
                        tz_local=cfg.tz_local,
                    )

                if exp_date is None:
                    continue
                put_wall, call_wall = _fetch_walls(conn, snapshot_ts=ts)
                if put_wall is None and call_wall is None:
                    continue

                # rotation filter
                spot_delta_5m = None
                if cfg.rotation_filter == "spot_delta_5m":
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            SELECT spot
                            FROM spx.chain_features_0dte
                            WHERE (snapshot_ts AT TIME ZONE %s)::date = %s::date
                              AND snapshot_ts < %s
                            ORDER BY snapshot_ts DESC
                            LIMIT 1
                            """,
                            (tz_local, day_local.isoformat(), ts),
                        )
                        r = cur.fetchone()
                        if r and r[0] is not None:
                            spot_delta_5m = float(spot - float(r[0]))

                key = (ts, exp_date)
                strikes = strike_cache.get(key)
                if strikes is None:
                    strikes = _fetch_strikes(conn, snapshot_ts=ts, expiration_date=exp_date)
                    strike_cache[key] = strikes
                if not strikes:
                    continue

                candidates: list[dict[str, Any]] = []

                # PW bearish PUT
                if cfg.enable_pw_trade and put_wall is not None and spot > float(put_wall):
                    d_pw = float(spot - put_wall)
                    if float(cfg.proximity_min_points) <= d_pw <= float(cfg.proximity_max_points):
                        if cfg.rotation_filter != "spot_delta_5m" or (spot_delta_5m is not None and spot_delta_5m < 0):
                            c = _build_structural_candidate(
                                conn,
                                snapshot_ts=ts,
                                expiration_date=exp_date,
                                strikes=strikes,
                                spot=spot,
                                put_wall=put_wall,
                                call_wall=call_wall,
                                side="put",
                                long_leg_moneyness=cfg.long_leg_moneyness,
                                max_width_points=cfg.max_width_points,
                                min_width_points=cfg.min_width_points,
                                max_debit_points=cfg.max_debit_points,
                                short_offset_steps=cfg.short_put_offset_steps,
                            )
                            if c:
                                c["distance_to_wall"] = d_pw
                                candidates.append(c)

                # CW bullish CALL
                if cfg.enable_cw_trade and call_wall is not None and spot < float(call_wall):
                    d_cw = float(call_wall - spot)
                    if float(cfg.proximity_min_points) <= d_cw <= float(cfg.proximity_max_points):
                        if cfg.rotation_filter != "spot_delta_5m" or (spot_delta_5m is not None and spot_delta_5m > 0):
                            c = _build_structural_candidate(
                                conn,
                                snapshot_ts=ts,
                                expiration_date=exp_date,
                                strikes=strikes,
                                spot=spot,
                                put_wall=put_wall,
                                call_wall=call_wall,
                                side="call",
                                long_leg_moneyness=cfg.long_leg_moneyness,
                                max_width_points=cfg.max_width_points,
                                min_width_points=cfg.min_width_points,
                                max_debit_points=cfg.max_debit_points,
                                short_offset_steps=cfg.short_call_offset_steps,
                            )
                            if c:
                                c["distance_to_wall"] = d_cw
                                candidates.append(c)

                if not candidates:
                    continue

                # tie-break
                candidates.sort(key=lambda x: (float(x.get("distance_to_wall") or 1e9), float(x.get("entry_debit") or 1e9)))
                if len(candidates) >= 2 and float(candidates[0].get("distance_to_wall") or 0) == float(candidates[1].get("distance_to_wall") or 0):
                    if cfg.prefer_pw_on_tie:
                        for c in candidates:
                            if c.get("direction") == "PW_PUT":
                                candidates = [c]
                                break

                cand = candidates[0]
                entry_debit = float(cand["entry_debit"])
                exp_date = exp_date
                long_sym = str(cand["long_symbol"])
                short_sym = str(cand["short_symbol"])
                width_points = float(cand["width"])
                direction = str(cand.get("direction") or "")

            if exp_date is None or long_sym is None or short_sym is None or entry_debit is None:
                continue

            # Build forward snapshot path for exits
            path_snaps, tH = _fetch_path_snapshots(
                conn,
                entry_ts=entry_ts,
                horizon_minutes=cfg.horizon_minutes,
                max_future_lookahead_minutes=cfg.max_future_lookahead_minutes,
                day_local=day_local,
                tz_local=tz_local,
            )

            mids = _fetch_option_mids(
                conn,
                snapshot_ts=path_snaps,
                expiration_date=exp_date,
                symbols=(long_sym, short_sym),
            )

            stop_level = entry_debit * (1.0 - float(cfg.stop_loss_pct))
            tp_level = entry_debit * (1.0 + float(cfg.take_profit_pct))

            exit_ts: datetime | None = None
            exit_reason: str = "MISSING"
            exit_debit: float | None = None

            clamp_hi = float(width_points) if width_points is not None else float("inf")

            # Walk snapshots in order to find first SL/TP hit
            for p_ts in path_snaps:
                m_long = mids.get((p_ts, long_sym))
                m_short = mids.get((p_ts, short_sym))
                if m_long is None or m_short is None:
                    continue
                raw = float(m_long - m_short)
                spread_mid = _clamp(raw, 0.0, clamp_hi)

                if spread_mid <= stop_level:
                    exit_ts = p_ts
                    exit_reason = "SL"
                    exit_debit = spread_mid
                    break
                if spread_mid >= tp_level:
                    exit_ts = p_ts
                    exit_reason = "TP"
                    exit_debit = spread_mid
                    break

            # If no SL/TP, time exit
            if exit_ts is None:
                if tH is None:
                    exit_reason = "MISSING"
                else:
                    m_long = mids.get((tH, long_sym))
                    m_short = mids.get((tH, short_sym))
                    if m_long is None or m_short is None:
                        exit_reason = "MISSING"
                        exit_ts = tH
                    else:
                        raw = float(m_long - m_short)
                        exit_reason = "TIME"
                        exit_ts = tH
                        exit_debit = _clamp(raw, 0.0, clamp_hi)

            change_points = None
            pnl_dollars = None
            roi = None
            if exit_debit is not None:
                change_points = float(exit_debit - entry_debit)
                pnl_dollars = float(change_points * 100.0)
                roi = float(change_points / entry_debit) if entry_debit > 0 else None

            if exit_reason == "MISSING" and (not cfg.include_missing_exits):
                # still mark position as exited for gating
                position_exit_ts = exit_ts
                in_position = cfg.one_trade_at_a_time
                day_trades += 1
                continue

            tdict = {
                "day_local": day_local.isoformat(),
                "entry_ts": entry_ts.astimezone(ZoneInfo(tz_local)).isoformat(),
                "exit_ts": exit_ts.astimezone(ZoneInfo(tz_local)).isoformat() if exit_ts else None,
                "exit_reason": exit_reason,
                "anchor_type": (cand or {}).get("anchor_type"),
                "spread_type": (cand or {}).get("spread_type"),
                "k_long": (cand or {}).get("k_long"),
                "k_short": (cand or {}).get("k_short"),
                "width_points": width_points,
                "direction": direction,
                "long_symbol": long_sym,
                "short_symbol": short_sym,
                "entry_debit": entry_debit,
                "exit_debit": exit_debit,
                "change_points": change_points,
                "pnl_dollars": pnl_dollars,
                "roi": roi,
                "p_bigwin": (cand or {}).get("p_bigwin"),
                "pred_change": (cand or {}).get("pred_change"),
                "spot": (cand or {}).get("spot"),
                "put_wall": (cand or {}).get("put_wall"),
                "call_wall": (cand or {}).get("call_wall"),
            }
            trades.append(tdict)

            if pnl_dollars is not None:
                cum_pnl += pnl_dollars
                eq_points.append(cum_pnl)
                equity_curve.append(
                    {
                        "ts": (exit_ts or entry_ts).astimezone(ZoneInfo(tz_local)).isoformat(),
                        "cum_pnl_dollars": float(cum_pnl),
                    }
                )

            position_exit_ts = exit_ts
            in_position = cfg.one_trade_at_a_time
            day_trades += 1

    # Summary
    pnl_vals = [t["pnl_dollars"] for t in trades if t.get("pnl_dollars") is not None]
    roi_vals = [t["roi"] for t in trades if t.get("roi") is not None]

    wins = sum(1 for t in trades if (t.get("pnl_dollars") or 0) > 0)
    losses = sum(1 for t in trades if (t.get("pnl_dollars") or 0) < 0)

    sum_gain = sum(v for v in pnl_vals if v > 0)
    sum_loss = sum(v for v in pnl_vals if v < 0)

    summary = {
        "days": len(_daterange(cfg.start_day, cfg.end_day)),
        "trades": len(trades),
        "wins": wins,
        "losses": losses,
        "win_rate": float(wins / len(trades)) if trades else 0.0,
        "avg_roi": float(sum(roi_vals) / len(roi_vals)) if roi_vals else 0.0,
        "avg_pnl_dollars": float(sum(pnl_vals) / len(pnl_vals)) if pnl_vals else 0.0,
        "cum_pnl_dollars": float(cum_pnl),
        "max_drawdown_dollars": float(_max_drawdown(eq_points) if eq_points else 0.0),
        "profit_factor": float(sum_gain / abs(sum_loss)) if sum_loss < 0 else (float("inf") if sum_gain > 0 else 0.0),
    }

    return {
        "config": {
            "start_day": cfg.start_day.isoformat(),
            "end_day": cfg.end_day.isoformat(),
            "expiration_mode": cfg.expiration_mode,
            "target_dte_days": int(cfg.target_dte_days) if cfg.target_dte_days is not None else None,
            "dte_tolerance_days": int(cfg.dte_tolerance_days),
            "horizon_minutes": int(cfg.horizon_minutes),
            "entry_mode": cfg.entry_mode,
            "session_start_ct": cfg.session_start_ct,
            "entry_first_n_minutes": int(cfg.entry_first_n_minutes),
            "entry_start_ct": cfg.entry_start_ct,
            "entry_end_ct": cfg.entry_end_ct,
            "max_trades_per_day": int(cfg.max_trades_per_day),
            "one_trade_at_a_time": bool(cfg.one_trade_at_a_time),
            "max_debit_points": float(cfg.max_debit_points),
            "stop_loss_pct": float(cfg.stop_loss_pct),
            "take_profit_pct": float(cfg.take_profit_pct),
            "max_future_lookahead_minutes": int(cfg.max_future_lookahead_minutes),
            "price_mode": cfg.price_mode,
            "tz_local": cfg.tz_local,
            "include_missing_exits": bool(cfg.include_missing_exits),
            "strategy_mode": cfg.strategy_mode,
            "anchor_mode": cfg.anchor_mode,
            "anchor_policy": cfg.anchor_policy,
            "min_p_bigwin": float(cfg.min_p_bigwin),
            "min_pred_change": float(cfg.min_pred_change),
            "allowed_spreads": list(cfg.allowed_spreads),
            "enable_pw_trade": bool(cfg.enable_pw_trade),
            "enable_cw_trade": bool(cfg.enable_cw_trade),
            "long_leg_moneyness": cfg.long_leg_moneyness,
            "max_width_points": float(cfg.max_width_points),
            "min_width_points": float(cfg.min_width_points),
            "proximity_min_points": float(cfg.proximity_min_points),
            "proximity_max_points": float(cfg.proximity_max_points),
            "rotation_filter": cfg.rotation_filter,
            "prefer_pw_on_tie": bool(cfg.prefer_pw_on_tie),
            "short_put_offset_steps": int(cfg.short_put_offset_steps),
            "short_call_offset_steps": int(cfg.short_call_offset_steps),
        },
        "summary": summary,
        "equity_curve": equity_curve,
        "trades": trades,
    }
