from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Literal

from zoneinfo import ZoneInfo

import psycopg


EntryMode = Literal["first_n_minutes", "time_range"]
AnchorMode = Literal["ATM", "WALLS", "MAGNET", "ALL"]
AnchorPolicy = Literal["any", "opposite_wall", "same_wall"]
PriceMode = Literal["mid"]


@dataclass(frozen=True)
class DebitBacktestConfig:
    start_day: date
    end_day: date

    horizon_minutes: int = 30

    entry_mode: EntryMode = "time_range"
    session_start_ct: str = "08:30"
    entry_first_n_minutes: int = 60
    entry_start_ct: str = "08:40"
    entry_end_ct: str = "09:30"

    max_trades_per_day: int = 1
    one_trade_at_a_time: bool = True

    anchor_mode: AnchorMode = "ATM"
    anchor_policy: AnchorPolicy = "any"

    min_p_bigwin: float = 0.0
    min_pred_change: float = 0.0

    allowed_spreads: tuple[str, ...] = ("CALL", "PUT")
    max_debit_points: float = 5.0

    stop_loss_pct: float = 0.50
    take_profit_pct: float = 2.00

    max_future_lookahead_minutes: int = 120
    price_mode: PriceMode = "mid"

    tz_local: str = "America/Chicago"

    include_missing_exits: bool = False


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
            out[(ts, sym)] = _mid(bid, ask, md)
        return out


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

    for day_local in _daterange(cfg.start_day, cfg.end_day):
        day_trades = 0
        in_position = False
        position_exit_ts: datetime | None = None

        snaps = _fetch_snapshots_in_window(conn, day_local=day_local, start_t=start_t, end_t=end_t, tz_local=tz_local)
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

        for ts in snaps:
            if day_trades >= int(cfg.max_trades_per_day):
                break

            if cfg.one_trade_at_a_time and in_position:
                # Only resume looking for entries after exit time.
                if position_exit_ts is None or ts <= position_exit_ts:
                    continue
                in_position = False

            cand = _select_best_candidate(by_ts.get(ts, []))
            if not cand:
                continue

            entry_ts = cand["snapshot_ts"]
            entry_debit = float(cand["debit_points"]) if cand.get("debit_points") is not None else None
            if entry_debit is None or entry_debit <= 0:
                continue

            exp_date = cand["expiration_date"]
            long_sym = str(cand["long_symbol"])
            short_sym = str(cand["short_symbol"])

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

            # Walk snapshots in order to find first SL/TP hit
            for p_ts in path_snaps:
                m_long = mids.get((p_ts, long_sym))
                m_short = mids.get((p_ts, short_sym))
                if m_long is None or m_short is None:
                    continue
                spread_mid = float(m_long - m_short)

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
                        exit_reason = "TIME"
                        exit_ts = tH
                        exit_debit = float(m_long - m_short)

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

            trade = {
                "day_local": day_local.isoformat(),
                "entry_ts": entry_ts.astimezone(ZoneInfo(tz_local)).isoformat(),
                "exit_ts": exit_ts.astimezone(ZoneInfo(tz_local)).isoformat() if exit_ts else None,
                "exit_reason": exit_reason,
                "anchor_type": cand["anchor_type"],
                "spread_type": cand["spread_type"],
                "k_long": cand.get("k_long"),
                "k_short": cand.get("k_short"),
                "long_symbol": long_sym,
                "short_symbol": short_sym,
                "entry_debit": entry_debit,
                "exit_debit": exit_debit,
                "change_points": change_points,
                "pnl_dollars": pnl_dollars,
                "roi": roi,
                "p_bigwin": cand.get("p_bigwin"),
                "pred_change": cand.get("pred_change"),
            }
            trades.append(trade)

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
            "horizon_minutes": int(cfg.horizon_minutes),
            "entry_mode": cfg.entry_mode,
            "session_start_ct": cfg.session_start_ct,
            "entry_first_n_minutes": int(cfg.entry_first_n_minutes),
            "entry_start_ct": cfg.entry_start_ct,
            "entry_end_ct": cfg.entry_end_ct,
            "max_trades_per_day": int(cfg.max_trades_per_day),
            "one_trade_at_a_time": bool(cfg.one_trade_at_a_time),
            "anchor_mode": cfg.anchor_mode,
            "anchor_policy": cfg.anchor_policy,
            "min_p_bigwin": float(cfg.min_p_bigwin),
            "min_pred_change": float(cfg.min_pred_change),
            "allowed_spreads": list(cfg.allowed_spreads),
            "max_debit_points": float(cfg.max_debit_points),
            "stop_loss_pct": float(cfg.stop_loss_pct),
            "take_profit_pct": float(cfg.take_profit_pct),
            "max_future_lookahead_minutes": int(cfg.max_future_lookahead_minutes),
            "price_mode": cfg.price_mode,
            "tz_local": cfg.tz_local,
            "include_missing_exits": bool(cfg.include_missing_exits),
        },
        "summary": summary,
        "equity_curve": equity_curve,
        "trades": trades,
    }
