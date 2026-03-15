from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any

import psycopg


@dataclass(frozen=True)
class OutcomesConfig:
    align_mode: str = "FirstOfDay"  # FirstOfDay|SameTime|LastOfDay
    horizons_td: tuple[int, ...] = (5, 10, 21)
    flat_pct_band: float = 0.0025
    tz_local: str = "America/Chicago"


SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS spx;

CREATE TABLE IF NOT EXISTS spx.chain_outcomes_underlying (
  snapshot_ts TIMESTAMPTZ NOT NULL,
  align_mode TEXT NOT NULL,
  horizon_td INT NOT NULL,
  eval_snapshot_ts TIMESTAMPTZ,

  spot_t NUMERIC,
  spot_h NUMERIC,
  ret_pct NUMERIC,
  out_label TEXT,
  is_missing_future BOOLEAN NOT NULL DEFAULT FALSE,
  computed_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  PRIMARY KEY (snapshot_ts, align_mode, horizon_td)
);

CREATE TABLE IF NOT EXISTS spx.chain_outcomes_atm_options (
  snapshot_ts TIMESTAMPTZ NOT NULL,
  align_mode TEXT NOT NULL,
  horizon_td INT NOT NULL,
  side TEXT NOT NULL,
  eval_snapshot_ts TIMESTAMPTZ,

  expiration_date DATE,
  strike NUMERIC,

  mid_t NUMERIC,
  mid_h NUMERIC,
  ret_pct NUMERIC,

  missing_expiration BOOLEAN NOT NULL DEFAULT FALSE,
  missing_t0_quote BOOLEAN NOT NULL DEFAULT FALSE,
  missing_future_quote BOOLEAN NOT NULL DEFAULT FALSE,
  computed_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  PRIMARY KEY (snapshot_ts, align_mode, horizon_td, side)
);
"""

UPSERT_UNDERLYING_SQL = """
INSERT INTO spx.chain_outcomes_underlying (
  snapshot_ts, align_mode, horizon_td, eval_snapshot_ts,
  spot_t, spot_h, ret_pct, out_label, is_missing_future
) VALUES (
  %(snapshot_ts)s, %(align_mode)s, %(horizon_td)s, %(eval_snapshot_ts)s,
  %(spot_t)s, %(spot_h)s, %(ret_pct)s, %(out_label)s, %(is_missing_future)s
)
ON CONFLICT (snapshot_ts, align_mode, horizon_td) DO UPDATE SET
  eval_snapshot_ts = EXCLUDED.eval_snapshot_ts,
  spot_t = EXCLUDED.spot_t,
  spot_h = EXCLUDED.spot_h,
  ret_pct = EXCLUDED.ret_pct,
  out_label = EXCLUDED.out_label,
  is_missing_future = EXCLUDED.is_missing_future,
  computed_at = now();
"""

UPSERT_ATM_SQL = """
INSERT INTO spx.chain_outcomes_atm_options (
  snapshot_ts, align_mode, horizon_td, side, eval_snapshot_ts,
  expiration_date, strike,
  mid_t, mid_h, ret_pct,
  missing_expiration, missing_t0_quote, missing_future_quote
) VALUES (
  %(snapshot_ts)s, %(align_mode)s, %(horizon_td)s, %(side)s, %(eval_snapshot_ts)s,
  %(expiration_date)s, %(strike)s,
  %(mid_t)s, %(mid_h)s, %(ret_pct)s,
  %(missing_expiration)s, %(missing_t0_quote)s, %(missing_future_quote)s
)
ON CONFLICT (snapshot_ts, align_mode, horizon_td, side) DO UPDATE SET
  eval_snapshot_ts = EXCLUDED.eval_snapshot_ts,
  expiration_date = EXCLUDED.expiration_date,
  strike = EXCLUDED.strike,
  mid_t = EXCLUDED.mid_t,
  mid_h = EXCLUDED.mid_h,
  ret_pct = EXCLUDED.ret_pct,
  missing_expiration = EXCLUDED.missing_expiration,
  missing_t0_quote = EXCLUDED.missing_t0_quote,
  missing_future_quote = EXCLUDED.missing_future_quote,
  computed_at = now();
"""


def ensure_schema(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)
        conn.commit()


def _as_float(x: Any) -> float | None:
    if x is None:
        return None
    try:
        return float(x)
    except Exception:
        return None


def _mid_from_row(bid: Any, ask: Any, mid: Any) -> float | None:
    m = _as_float(mid)
    if m is not None:
        return m
    b = _as_float(bid)
    a = _as_float(ask)
    if b is not None and a is not None and a >= b:
        return 0.5 * (a + b)
    return None


def _label_from_ret(ret_pct: float | None, flat_band: float) -> str | None:
    if ret_pct is None:
        return None
    if ret_pct > float(flat_band):
        return "Up"
    if ret_pct < -float(flat_band):
        return "Down"
    return "Flat"


def _trade_day_index(conn: psycopg.Connection, *, snapshot_ts: datetime, tz_local: str) -> int | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT day_idx
            FROM (
              SELECT
                d,
                dense_rank() OVER (ORDER BY d) - 1 AS day_idx
              FROM (
                SELECT DISTINCT (snapshot_ts AT TIME ZONE %s)::date AS d
                FROM spx.chain_features_0dte
              ) x
            ) z
            WHERE z.d = (%s AT TIME ZONE %s)::date
            """,
            (tz_local, snapshot_ts, tz_local),
        )
        r = cur.fetchone()
        return int(r[0]) if r else None


def _day_from_index(conn: psycopg.Connection, *, idx: int, tz_local: str) -> date | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT d
            FROM (
              SELECT
                d,
                dense_rank() OVER (ORDER BY d) - 1 AS day_idx
              FROM (
                SELECT DISTINCT (snapshot_ts AT TIME ZONE %s)::date AS d
                FROM spx.chain_features_0dte
              ) x
            ) z
            WHERE z.day_idx = %s
            """,
            (tz_local, int(idx)),
        )
        r = cur.fetchone()
        return r[0] if r else None


def _pick_eval_snapshot(
    conn: psycopg.Connection,
    *,
    base_ts: datetime,
    target_day: date,
    align_mode: str,
    tz_local: str,
) -> datetime | None:
    align = (align_mode or "FirstOfDay").strip()
    with conn.cursor() as cur:
        if align == "LastOfDay":
            cur.execute(
                """
                SELECT snapshot_ts
                FROM spx.chain_features_0dte
                WHERE (snapshot_ts AT TIME ZONE %s)::date = %s::date
                ORDER BY snapshot_ts DESC
                LIMIT 1
                """,
                (tz_local, target_day.isoformat()),
            )
        elif align == "SameTime":
            cur.execute(
                """
                SELECT snapshot_ts
                FROM spx.chain_features_0dte
                WHERE (snapshot_ts AT TIME ZONE %s)::date = %s::date
                  AND (snapshot_ts AT TIME ZONE %s)::time >= (%s AT TIME ZONE %s)::time
                ORDER BY snapshot_ts ASC
                LIMIT 1
                """,
                (tz_local, target_day.isoformat(), tz_local, base_ts, tz_local),
            )
        else:  # FirstOfDay
            cur.execute(
                """
                SELECT snapshot_ts
                FROM spx.chain_features_0dte
                WHERE (snapshot_ts AT TIME ZONE %s)::date = %s::date
                ORDER BY snapshot_ts ASC
                LIMIT 1
                """,
                (tz_local, target_day.isoformat()),
            )
        r = cur.fetchone()
        return r[0] if r else None


def _spot_at_snapshot(conn: psycopg.Connection, snapshot_ts: datetime) -> float | None:
    with conn.cursor() as cur:
        cur.execute("SELECT spot FROM spx.chain_features_0dte WHERE snapshot_ts=%s", (snapshot_ts,))
        r = cur.fetchone()
        return _as_float(r[0]) if r else None


def _pick_expiration_nearest_ge(conn: psycopg.Connection, *, snapshot_ts: datetime, eval_date: date) -> date | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT expiration_date
            FROM spx.option_chain
            WHERE snapshot_ts=%s
              AND expiration_date >= %s
            GROUP BY expiration_date
            ORDER BY expiration_date ASC
            LIMIT 1
            """,
            (snapshot_ts, eval_date),
        )
        r = cur.fetchone()
        return r[0] if r else None


def _nearest_strike(conn: psycopg.Connection, *, snapshot_ts: datetime, expiration_date: date, spot: float) -> float | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT strike
            FROM spx.option_chain
            WHERE snapshot_ts=%s
              AND expiration_date=%s
              AND strike IS NOT NULL
            ORDER BY ABS(strike - %s) ASC, strike ASC
            LIMIT 1
            """,
            (snapshot_ts, expiration_date, float(spot)),
        )
        r = cur.fetchone()
        return _as_float(r[0]) if r else None


def _atm_mid(conn: psycopg.Connection, *, snapshot_ts: datetime, expiration_date: date, side: str, strike: float) -> float | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT bid, ask, mid
            FROM spx.option_chain
            WHERE snapshot_ts=%s
              AND expiration_date=%s
              AND side=%s
              AND strike=%s
            ORDER BY option_symbol ASC
            LIMIT 1
            """,
            (snapshot_ts, expiration_date, side, float(strike)),
        )
        r = cur.fetchone()
        if not r:
            return None
        return _mid_from_row(r[0], r[1], r[2])


def compute_outcomes_for_snapshot(conn: psycopg.Connection, *, snapshot_ts: datetime, cfg: OutcomesConfig) -> int:
    base_spot = _spot_at_snapshot(conn, snapshot_ts)
    if base_spot is None:
        return 0

    idx = _trade_day_index(conn, snapshot_ts=snapshot_ts, tz_local=cfg.tz_local)
    if idx is None:
        return 0

    upserted = 0

    for h in cfg.horizons_td:
        target_day = _day_from_index(conn, idx=int(idx + h), tz_local=cfg.tz_local)
        eval_ts = _pick_eval_snapshot(
            conn,
            base_ts=snapshot_ts,
            target_day=target_day,
            align_mode=cfg.align_mode,
            tz_local=cfg.tz_local,
        ) if target_day is not None else None

        spot_h = _spot_at_snapshot(conn, eval_ts) if eval_ts is not None else None
        ret = (float(spot_h - base_spot) / float(base_spot)) if (spot_h is not None and base_spot and base_spot != 0) else None

        with conn.cursor() as cur:
            cur.execute(
                UPSERT_UNDERLYING_SQL,
                {
                    "snapshot_ts": snapshot_ts,
                    "align_mode": cfg.align_mode,
                    "horizon_td": int(h),
                    "eval_snapshot_ts": eval_ts,
                    "spot_t": float(base_spot),
                    "spot_h": spot_h,
                    "ret_pct": ret,
                    "out_label": _label_from_ret(ret, cfg.flat_pct_band),
                    "is_missing_future": bool(eval_ts is None or spot_h is None),
                },
            )
            upserted += 1

        eval_date = target_day
        exp = None
        k_atm = None
        missing_exp = False
        missing_t0 = False
        missing_h = False

        if eval_date is None:
            missing_exp = True
            missing_t0 = True
            missing_h = True
        else:
            exp = _pick_expiration_nearest_ge(conn, snapshot_ts=snapshot_ts, eval_date=eval_date)
            if exp is None:
                missing_exp = True
                missing_t0 = True
                missing_h = True
            else:
                k_atm = _nearest_strike(conn, snapshot_ts=snapshot_ts, expiration_date=exp, spot=float(base_spot))
                if k_atm is None:
                    missing_t0 = True
                    missing_h = True

        for side in ("call", "put"):
            mid_t = None
            mid_h = None
            ret_opt = None

            if exp is not None and k_atm is not None:
                mid_t = _atm_mid(conn, snapshot_ts=snapshot_ts, expiration_date=exp, side=side, strike=float(k_atm))
                if mid_t is None or mid_t <= 0:
                    missing_t0 = True
                if eval_ts is not None:
                    mid_h = _atm_mid(conn, snapshot_ts=eval_ts, expiration_date=exp, side=side, strike=float(k_atm))
                if mid_h is None:
                    missing_h = True
                if mid_t is not None and mid_h is not None and mid_t > 0:
                    ret_opt = float((mid_h - mid_t) / mid_t)

            with conn.cursor() as cur:
                cur.execute(
                    UPSERT_ATM_SQL,
                    {
                        "snapshot_ts": snapshot_ts,
                        "align_mode": cfg.align_mode,
                        "horizon_td": int(h),
                        "side": side,
                        "eval_snapshot_ts": eval_ts,
                        "expiration_date": exp,
                        "strike": k_atm,
                        "mid_t": mid_t,
                        "mid_h": mid_h,
                        "ret_pct": ret_opt,
                        "missing_expiration": bool(missing_exp),
                        "missing_t0_quote": bool(missing_t0),
                        "missing_future_quote": bool(missing_h),
                    },
                )
                upserted += 1

    conn.commit()
    return upserted


def snapshots_missing_outcomes(conn: psycopg.Connection, *, align_mode: str, limit: int) -> list[datetime]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT f.snapshot_ts
            FROM spx.chain_features_0dte f
            WHERE NOT EXISTS (
              SELECT 1
              FROM spx.chain_outcomes_underlying o
              WHERE o.snapshot_ts=f.snapshot_ts
                AND o.align_mode=%s
            )
            ORDER BY f.snapshot_ts ASC
            LIMIT %s
            """,
            (align_mode, int(limit)),
        )
        return [r[0] for r in cur.fetchall()]
