from __future__ import annotations

import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from statistics import mean
from typing import Any, Iterable

from zoneinfo import ZoneInfo

import psycopg

from options_ai.features.flow_engine import compute_options_flow
from options_ai.outcomes_phase3 import (
    OutcomesConfig,
    compute_outcomes_for_snapshot,
    ensure_schema as ensure_outcomes_schema,
    snapshots_missing_outcomes,
)
from options_ai.utils.task_state import utc_now_iso, write_task_state


TZ_LOCAL_DEFAULT = "America/Chicago"


@dataclass(frozen=True)
class Phase2Config:
    db_dsn: str
    tz_local: str = TZ_LOCAL_DEFAULT

    horizons_minutes: tuple[int, ...] = (15, 30, 45, 60, 90)
    max_future_lookahead_minutes: int = 120

    label_eps_atm_iv: float = 0.0025
    label_eps_skew_25d: float = 0.0025

    min_contracts: int = 50

    # Flow phase3 knobs
    flow_max_strike_dist_pct: float = 0.40
    flow_use_moneyness_weight: bool = True
    flow_use_gaussian: bool = True
    flow_winsorize_bucket: bool = True
    flow_confirm_fast_win: int = 10
    flow_confirm_slow_win: int = 60
    flow_history_per_strike: int = 600
    flow_bucket_z_window: int = 60
    flow_min_breadth: float = 0.60
    flow_min_bucket_z: float = 1.5
    flow_conf_min: float = 0.60
    flow_atm_corridor_pct: float = 0.01

    # Outcomes
    outcome_align: str = "FirstOfDay"
    outcome_horizons_td: tuple[int, ...] = (5, 10, 21)
    flat_pct_band: float = 0.0025

    # Poll loop
    poll_seconds: float = 15.0
    batch_limit: int = 200


SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS spx;

CREATE TABLE IF NOT EXISTS spx.chain_features_0dte (
  snapshot_ts TIMESTAMPTZ PRIMARY KEY,
  expiration_date DATE NOT NULL,

  spot NUMERIC,
  ttm_minutes INT,

  atm_strike NUMERIC,
  atm_iv NUMERIC,
  atm_call_iv NUMERIC,
  atm_put_iv NUMERIC,
  atm_bidask_spread NUMERIC,

  iv_put_25d NUMERIC,
  iv_call_25d NUMERIC,
  skew_25d NUMERIC,
  bf_25d NUMERIC,

  put_volume BIGINT,
  call_volume BIGINT,
  pcr_volume NUMERIC,
  put_oi BIGINT,
  call_oi BIGINT,
  pcr_oi NUMERIC,

  itm_vol BIGINT,
  atm_vol BIGINT,
  otm_vol BIGINT,
  tot_vol BIGINT,

  d_tot_vol BIGINT,
  d_call_oi BIGINT,
  d_put_oi BIGINT,

  sma_spot_5 NUMERIC,
  sma_spot_20 NUMERIC,
  bb_mid_20 NUMERIC,
  bb_upper_20 NUMERIC,
  bb_lower_20 NUMERIC,
  bb_pctb_20 NUMERIC,
  rsi_14 NUMERIC,
  twap_spot_day NUMERIC,
  vwap_chainweighted_spot_day NUMERIC,

  rsi_osob_label TEXT,
  bb_osob_label TEXT,
  pcr_oi_osob_label TEXT,

  flow_total_strikes INT,
  flow_pct_bullish NUMERIC,
  flow_pct_bearish NUMERIC,
  flow_breadth NUMERIC,
  flow_bucket_net_flow NUMERIC,
  flow_bucket_robust_z NUMERIC,
  flow_skew NUMERIC,
  flow_confidence NUMERIC,
  flow_atm_corridor_net NUMERIC,
  flow_atm_corridor_frac NUMERIC,
  flow_top3_share NUMERIC,
  flow_top5_share NUMERIC,
  flow_bias_summary TEXT,
  flow_breadth_pass BOOLEAN NOT NULL DEFAULT FALSE,
  flow_bucketz_pass BOOLEAN NOT NULL DEFAULT FALSE,
  flow_live_ok_default BOOLEAN NOT NULL DEFAULT TRUE,

  contract_count INT,
  valid_iv_count INT,
  valid_mid_count INT,
  low_quality BOOLEAN NOT NULL DEFAULT FALSE,
  computed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS spx.chain_flow_strike_0dte (
  snapshot_ts TIMESTAMPTZ NOT NULL,
  strike NUMERIC NOT NULL,
  net_flow NUMERIC,
  PRIMARY KEY (snapshot_ts, strike)
);

CREATE TABLE IF NOT EXISTS spx.chain_labels_0dte (
  snapshot_ts TIMESTAMPTZ NOT NULL,
  horizon_minutes INT NOT NULL,

  atm_iv_t NUMERIC,
  atm_iv_tH NUMERIC,
  atm_iv_change NUMERIC,
  label_atm_iv_dir SMALLINT,

  skew_25d_t NUMERIC,
  skew_25d_tH NUMERIC,
  skew_25d_change NUMERIC,
  label_skew_25d_dir SMALLINT,

  is_missing_future BOOLEAN NOT NULL DEFAULT FALSE,
  is_low_quality BOOLEAN NOT NULL DEFAULT FALSE,
  computed_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  PRIMARY KEY (snapshot_ts, horizon_minutes)
);

CREATE INDEX IF NOT EXISTS chain_features_0dte_exp_ts_idx ON spx.chain_features_0dte (expiration_date, snapshot_ts DESC);
CREATE INDEX IF NOT EXISTS chain_labels_0dte_ts_idx ON spx.chain_labels_0dte (snapshot_ts DESC);
CREATE INDEX IF NOT EXISTS chain_flow_strike_0dte_ts_idx ON spx.chain_flow_strike_0dte (snapshot_ts DESC);
"""

ALTER_SQL = """
ALTER TABLE spx.chain_features_0dte ADD COLUMN IF NOT EXISTS itm_vol BIGINT;
ALTER TABLE spx.chain_features_0dte ADD COLUMN IF NOT EXISTS atm_vol BIGINT;
ALTER TABLE spx.chain_features_0dte ADD COLUMN IF NOT EXISTS otm_vol BIGINT;
ALTER TABLE spx.chain_features_0dte ADD COLUMN IF NOT EXISTS tot_vol BIGINT;
ALTER TABLE spx.chain_features_0dte ADD COLUMN IF NOT EXISTS d_tot_vol BIGINT;
ALTER TABLE spx.chain_features_0dte ADD COLUMN IF NOT EXISTS d_call_oi BIGINT;
ALTER TABLE spx.chain_features_0dte ADD COLUMN IF NOT EXISTS d_put_oi BIGINT;
ALTER TABLE spx.chain_features_0dte ADD COLUMN IF NOT EXISTS sma_spot_5 NUMERIC;
ALTER TABLE spx.chain_features_0dte ADD COLUMN IF NOT EXISTS sma_spot_20 NUMERIC;
ALTER TABLE spx.chain_features_0dte ADD COLUMN IF NOT EXISTS bb_mid_20 NUMERIC;
ALTER TABLE spx.chain_features_0dte ADD COLUMN IF NOT EXISTS bb_upper_20 NUMERIC;
ALTER TABLE spx.chain_features_0dte ADD COLUMN IF NOT EXISTS bb_lower_20 NUMERIC;
ALTER TABLE spx.chain_features_0dte ADD COLUMN IF NOT EXISTS bb_pctb_20 NUMERIC;
ALTER TABLE spx.chain_features_0dte ADD COLUMN IF NOT EXISTS rsi_14 NUMERIC;
ALTER TABLE spx.chain_features_0dte ADD COLUMN IF NOT EXISTS twap_spot_day NUMERIC;
ALTER TABLE spx.chain_features_0dte ADD COLUMN IF NOT EXISTS vwap_chainweighted_spot_day NUMERIC;
ALTER TABLE spx.chain_features_0dte ADD COLUMN IF NOT EXISTS rsi_osob_label TEXT;
ALTER TABLE spx.chain_features_0dte ADD COLUMN IF NOT EXISTS bb_osob_label TEXT;
ALTER TABLE spx.chain_features_0dte ADD COLUMN IF NOT EXISTS pcr_oi_osob_label TEXT;
ALTER TABLE spx.chain_features_0dte ADD COLUMN IF NOT EXISTS flow_total_strikes INT;
ALTER TABLE spx.chain_features_0dte ADD COLUMN IF NOT EXISTS flow_pct_bullish NUMERIC;
ALTER TABLE spx.chain_features_0dte ADD COLUMN IF NOT EXISTS flow_pct_bearish NUMERIC;
ALTER TABLE spx.chain_features_0dte ADD COLUMN IF NOT EXISTS flow_breadth NUMERIC;
ALTER TABLE spx.chain_features_0dte ADD COLUMN IF NOT EXISTS flow_bucket_net_flow NUMERIC;
ALTER TABLE spx.chain_features_0dte ADD COLUMN IF NOT EXISTS flow_bucket_robust_z NUMERIC;
ALTER TABLE spx.chain_features_0dte ADD COLUMN IF NOT EXISTS flow_skew NUMERIC;
ALTER TABLE spx.chain_features_0dte ADD COLUMN IF NOT EXISTS flow_confidence NUMERIC;
ALTER TABLE spx.chain_features_0dte ADD COLUMN IF NOT EXISTS flow_atm_corridor_net NUMERIC;
ALTER TABLE spx.chain_features_0dte ADD COLUMN IF NOT EXISTS flow_atm_corridor_frac NUMERIC;
ALTER TABLE spx.chain_features_0dte ADD COLUMN IF NOT EXISTS flow_top3_share NUMERIC;
ALTER TABLE spx.chain_features_0dte ADD COLUMN IF NOT EXISTS flow_top5_share NUMERIC;
ALTER TABLE spx.chain_features_0dte ADD COLUMN IF NOT EXISTS flow_bias_summary TEXT;
ALTER TABLE spx.chain_features_0dte ADD COLUMN IF NOT EXISTS flow_breadth_pass BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE spx.chain_features_0dte ADD COLUMN IF NOT EXISTS flow_bucketz_pass BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE spx.chain_features_0dte ADD COLUMN IF NOT EXISTS flow_live_ok_default BOOLEAN NOT NULL DEFAULT TRUE;
"""


UPSERT_FEATURE_SQL = """
INSERT INTO spx.chain_features_0dte (
  snapshot_ts, expiration_date,
  spot, ttm_minutes,
  atm_strike, atm_iv, atm_call_iv, atm_put_iv, atm_bidask_spread,
  iv_put_25d, iv_call_25d, skew_25d, bf_25d,
  put_volume, call_volume, pcr_volume,
  put_oi, call_oi, pcr_oi,
  itm_vol, atm_vol, otm_vol, tot_vol,
  d_tot_vol, d_call_oi, d_put_oi,
  sma_spot_5, sma_spot_20,
  bb_mid_20, bb_upper_20, bb_lower_20, bb_pctb_20,
  rsi_14, twap_spot_day, vwap_chainweighted_spot_day,
  rsi_osob_label, bb_osob_label, pcr_oi_osob_label,
  flow_total_strikes, flow_pct_bullish, flow_pct_bearish, flow_breadth,
  flow_bucket_net_flow, flow_bucket_robust_z, flow_skew, flow_confidence,
  flow_atm_corridor_net, flow_atm_corridor_frac, flow_top3_share, flow_top5_share,
  flow_bias_summary, flow_breadth_pass, flow_bucketz_pass, flow_live_ok_default,
  contract_count, valid_iv_count, valid_mid_count,
  low_quality
) VALUES (
  %(snapshot_ts)s, %(expiration_date)s,
  %(spot)s, %(ttm_minutes)s,
  %(atm_strike)s, %(atm_iv)s, %(atm_call_iv)s, %(atm_put_iv)s, %(atm_bidask_spread)s,
  %(iv_put_25d)s, %(iv_call_25d)s, %(skew_25d)s, %(bf_25d)s,
  %(put_volume)s, %(call_volume)s, %(pcr_volume)s,
  %(put_oi)s, %(call_oi)s, %(pcr_oi)s,
  %(itm_vol)s, %(atm_vol)s, %(otm_vol)s, %(tot_vol)s,
  %(d_tot_vol)s, %(d_call_oi)s, %(d_put_oi)s,
  %(sma_spot_5)s, %(sma_spot_20)s,
  %(bb_mid_20)s, %(bb_upper_20)s, %(bb_lower_20)s, %(bb_pctb_20)s,
  %(rsi_14)s, %(twap_spot_day)s, %(vwap_chainweighted_spot_day)s,
  %(rsi_osob_label)s, %(bb_osob_label)s, %(pcr_oi_osob_label)s,
  %(flow_total_strikes)s, %(flow_pct_bullish)s, %(flow_pct_bearish)s, %(flow_breadth)s,
  %(flow_bucket_net_flow)s, %(flow_bucket_robust_z)s, %(flow_skew)s, %(flow_confidence)s,
  %(flow_atm_corridor_net)s, %(flow_atm_corridor_frac)s, %(flow_top3_share)s, %(flow_top5_share)s,
  %(flow_bias_summary)s, %(flow_breadth_pass)s, %(flow_bucketz_pass)s, %(flow_live_ok_default)s,
  %(contract_count)s, %(valid_iv_count)s, %(valid_mid_count)s,
  %(low_quality)s
)
ON CONFLICT (snapshot_ts) DO UPDATE SET
  expiration_date = EXCLUDED.expiration_date,
  spot = EXCLUDED.spot,
  ttm_minutes = EXCLUDED.ttm_minutes,
  atm_strike = EXCLUDED.atm_strike,
  atm_iv = EXCLUDED.atm_iv,
  atm_call_iv = EXCLUDED.atm_call_iv,
  atm_put_iv = EXCLUDED.atm_put_iv,
  atm_bidask_spread = EXCLUDED.atm_bidask_spread,
  iv_put_25d = EXCLUDED.iv_put_25d,
  iv_call_25d = EXCLUDED.iv_call_25d,
  skew_25d = EXCLUDED.skew_25d,
  bf_25d = EXCLUDED.bf_25d,
  put_volume = EXCLUDED.put_volume,
  call_volume = EXCLUDED.call_volume,
  pcr_volume = EXCLUDED.pcr_volume,
  put_oi = EXCLUDED.put_oi,
  call_oi = EXCLUDED.call_oi,
  pcr_oi = EXCLUDED.pcr_oi,
  itm_vol = EXCLUDED.itm_vol,
  atm_vol = EXCLUDED.atm_vol,
  otm_vol = EXCLUDED.otm_vol,
  tot_vol = EXCLUDED.tot_vol,
  d_tot_vol = EXCLUDED.d_tot_vol,
  d_call_oi = EXCLUDED.d_call_oi,
  d_put_oi = EXCLUDED.d_put_oi,
  sma_spot_5 = EXCLUDED.sma_spot_5,
  sma_spot_20 = EXCLUDED.sma_spot_20,
  bb_mid_20 = EXCLUDED.bb_mid_20,
  bb_upper_20 = EXCLUDED.bb_upper_20,
  bb_lower_20 = EXCLUDED.bb_lower_20,
  bb_pctb_20 = EXCLUDED.bb_pctb_20,
  rsi_14 = EXCLUDED.rsi_14,
  twap_spot_day = EXCLUDED.twap_spot_day,
  vwap_chainweighted_spot_day = EXCLUDED.vwap_chainweighted_spot_day,
  rsi_osob_label = EXCLUDED.rsi_osob_label,
  bb_osob_label = EXCLUDED.bb_osob_label,
  pcr_oi_osob_label = EXCLUDED.pcr_oi_osob_label,
  flow_total_strikes = EXCLUDED.flow_total_strikes,
  flow_pct_bullish = EXCLUDED.flow_pct_bullish,
  flow_pct_bearish = EXCLUDED.flow_pct_bearish,
  flow_breadth = EXCLUDED.flow_breadth,
  flow_bucket_net_flow = EXCLUDED.flow_bucket_net_flow,
  flow_bucket_robust_z = EXCLUDED.flow_bucket_robust_z,
  flow_skew = EXCLUDED.flow_skew,
  flow_confidence = EXCLUDED.flow_confidence,
  flow_atm_corridor_net = EXCLUDED.flow_atm_corridor_net,
  flow_atm_corridor_frac = EXCLUDED.flow_atm_corridor_frac,
  flow_top3_share = EXCLUDED.flow_top3_share,
  flow_top5_share = EXCLUDED.flow_top5_share,
  flow_bias_summary = EXCLUDED.flow_bias_summary,
  flow_breadth_pass = EXCLUDED.flow_breadth_pass,
  flow_bucketz_pass = EXCLUDED.flow_bucketz_pass,
  flow_live_ok_default = EXCLUDED.flow_live_ok_default,
  contract_count = EXCLUDED.contract_count,
  valid_iv_count = EXCLUDED.valid_iv_count,
  valid_mid_count = EXCLUDED.valid_mid_count,
  low_quality = EXCLUDED.low_quality,
  computed_at = now();
"""

UPSERT_STRIKE_FLOW_SQL = """
INSERT INTO spx.chain_flow_strike_0dte (snapshot_ts, strike, net_flow)
VALUES (%s, %s, %s)
ON CONFLICT (snapshot_ts, strike) DO UPDATE SET net_flow = EXCLUDED.net_flow;
"""

UPSERT_LABEL_SQL = """
INSERT INTO spx.chain_labels_0dte (
  snapshot_ts, horizon_minutes,
  atm_iv_t, atm_iv_tH, atm_iv_change, label_atm_iv_dir,
  skew_25d_t, skew_25d_tH, skew_25d_change, label_skew_25d_dir,
  is_missing_future, is_low_quality
) VALUES (
  %(snapshot_ts)s, %(horizon_minutes)s,
  %(atm_iv_t)s, %(atm_iv_tH)s, %(atm_iv_change)s, %(label_atm_iv_dir)s,
  %(skew_25d_t)s, %(skew_25d_tH)s, %(skew_25d_change)s, %(label_skew_25d_dir)s,
  %(is_missing_future)s, %(is_low_quality)s
)
ON CONFLICT (snapshot_ts, horizon_minutes) DO UPDATE SET
  atm_iv_t = EXCLUDED.atm_iv_t,
  atm_iv_tH = EXCLUDED.atm_iv_tH,
  atm_iv_change = EXCLUDED.atm_iv_change,
  label_atm_iv_dir = EXCLUDED.label_atm_iv_dir,
  skew_25d_t = EXCLUDED.skew_25d_t,
  skew_25d_tH = EXCLUDED.skew_25d_tH,
  skew_25d_change = EXCLUDED.skew_25d_change,
  label_skew_25d_dir = EXCLUDED.label_skew_25d_dir,
  is_missing_future = EXCLUDED.is_missing_future,
  is_low_quality = EXCLUDED.is_low_quality,
  computed_at = now();
"""


def _sign_eps(x: float | None, eps: float) -> int | None:
    if x is None:
        return None
    if x > eps:
        return 1
    if x < -eps:
        return -1
    return 0


def _median(vals: list[float]) -> float | None:
    if not vals:
        return None
    s = sorted(vals)
    n = len(s)
    mid = n // 2
    if n % 2 == 1:
        return float(s[mid])
    return 0.5 * (float(s[mid - 1]) + float(s[mid]))


def _std(vals: list[float]) -> float:
    if len(vals) <= 1:
        return 0.0
    mu = mean(vals)
    v = sum((x - mu) ** 2 for x in vals) / len(vals)
    return float(v ** 0.5)


def _rsi(values: list[float], period: int = 14) -> float | None:
    if len(values) < period + 1:
        return None
    diffs = [values[i] - values[i - 1] for i in range(1, len(values))]
    window = diffs[-period:]
    gains = sum(max(d, 0.0) for d in window) / period
    losses = sum(max(-d, 0.0) for d in window) / period
    if losses == 0:
        return 100.0 if gains > 0 else 50.0
    rs = gains / losses
    return 100.0 - (100.0 / (1.0 + rs))


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
        return None


def _local_trade_date(snapshot_ts: datetime, tz_local: str) -> datetime.date:
    return snapshot_ts.astimezone(ZoneInfo(tz_local)).date()


def ensure_phase2_schema(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)
        cur.execute(ALTER_SQL)
        conn.commit()


def _fetch_chain_rows_for_snapshot(cur: psycopg.Cursor, snapshot_ts: datetime, expiration_date: datetime.date) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT side, strike, iv, delta, mid, bid, ask, underlying_price, volume, open_interest
        FROM spx.option_chain
        WHERE snapshot_ts = %s
          AND expiration_date = %s
        """,
        (snapshot_ts, expiration_date),
    )
    rows = []
    for r in cur.fetchall():
        rows.append(
            {
                "side": r[0],
                "strike": _as_float(r[1]),
                "iv": _as_float(r[2]),
                "delta": _as_float(r[3]),
                "mid": _as_float(r[4]),
                "bid": _as_float(r[5]),
                "ask": _as_float(r[6]),
                "underlying_price": _as_float(r[7]),
                "volume": _as_int(r[8]),
                "open_interest": _as_int(r[9]),
            }
        )
    return rows


def _fetch_prev_feature_row(cur: psycopg.Cursor, snapshot_ts: datetime) -> dict[str, Any] | None:
    cur.execute(
        """
        SELECT snapshot_ts, spot, tot_vol, call_oi, put_oi
        FROM spx.chain_features_0dte
        WHERE snapshot_ts < %s
        ORDER BY snapshot_ts DESC
        LIMIT 1
        """,
        (snapshot_ts,),
    )
    r = cur.fetchone()
    if not r:
        return None
    return {
        "snapshot_ts": r[0],
        "spot": _as_float(r[1]),
        "tot_vol": _as_int(r[2]),
        "call_oi": _as_int(r[3]),
        "put_oi": _as_int(r[4]),
    }


def _fetch_recent_spots(cur: psycopg.Cursor, snapshot_ts: datetime, limit: int) -> list[float]:
    cur.execute(
        """
        SELECT spot
        FROM spx.chain_features_0dte
        WHERE snapshot_ts < %s
          AND spot IS NOT NULL
        ORDER BY snapshot_ts DESC
        LIMIT %s
        """,
        (snapshot_ts, int(limit)),
    )
    vals = [float(r[0]) for r in cur.fetchall() if r[0] is not None]
    vals.reverse()
    return vals


def _fetch_day_spot_vol(cur: psycopg.Cursor, snapshot_ts: datetime, tz_local: str) -> list[tuple[float, float]]:
    cur.execute(
        """
        SELECT spot, COALESCE(tot_vol, 0)
        FROM spx.chain_features_0dte
        WHERE snapshot_ts < %s
          AND (snapshot_ts AT TIME ZONE %s)::date = (%s AT TIME ZONE %s)::date
          AND spot IS NOT NULL
        ORDER BY snapshot_ts ASC
        """,
        (snapshot_ts, tz_local, snapshot_ts, tz_local),
    )
    out: list[tuple[float, float]] = []
    for s, v in cur.fetchall():
        if s is None:
            continue
        out.append((float(s), float(v or 0.0)))
    return out


def _fetch_bucket_history(cur: psycopg.Cursor, snapshot_ts: datetime, limit: int) -> list[float]:
    cur.execute(
        """
        SELECT flow_bucket_net_flow
        FROM spx.chain_features_0dte
        WHERE snapshot_ts < %s
          AND flow_bucket_net_flow IS NOT NULL
        ORDER BY snapshot_ts DESC
        LIMIT %s
        """,
        (snapshot_ts, int(limit)),
    )
    vals = [float(r[0]) for r in cur.fetchall() if r[0] is not None]
    vals.reverse()
    return vals


def _fetch_strike_histories(cur: psycopg.Cursor, snapshot_ts: datetime, strikes: list[float], max_per_strike: int) -> dict[float, list[float]]:
    if not strikes:
        return {}
    cur.execute(
        """
        SELECT strike, net_flow
        FROM (
          SELECT strike, net_flow,
                 ROW_NUMBER() OVER (PARTITION BY strike ORDER BY snapshot_ts DESC) AS rn
          FROM spx.chain_flow_strike_0dte
          WHERE snapshot_ts < %s
            AND strike = ANY(%s)
        ) x
        WHERE rn <= %s
        ORDER BY strike ASC, rn DESC
        """,
        (snapshot_ts, strikes, int(max_per_strike)),
    )
    out: dict[float, list[float]] = {}
    for k, v in cur.fetchall():
        if k is None or v is None:
            continue
        out.setdefault(float(k), []).append(float(v))
    return out


def compute_features_for_snapshot(conn: psycopg.Connection, *, snapshot_ts: datetime, tz_local: str, min_contracts: int, cfg: Phase2Config) -> dict[str, Any] | None:
    exp_date = _local_trade_date(snapshot_ts, tz_local)

    with conn.cursor() as cur:
        chain = _fetch_chain_rows_for_snapshot(cur, snapshot_ts, exp_date)
        if not chain:
            return None

        contract_count = len(chain)
        spot_vals = [c["underlying_price"] for c in chain if c.get("underlying_price") is not None]
        spot = _median([float(x) for x in spot_vals if x is not None])

        valid_iv_count = sum(1 for c in chain if c.get("iv") is not None)
        valid_mid_count = 0
        for c in chain:
            mid = c.get("mid")
            if mid is None:
                b, a = c.get("bid"), c.get("ask")
                if b is not None and a is not None and a >= b:
                    mid = 0.5 * (b + a)
            if mid is not None:
                valid_mid_count += 1

        strikes = sorted({c["strike"] for c in chain if c.get("strike") is not None})
        atm_strike = None
        if spot is not None and strikes:
            atm_strike = min(strikes, key=lambda k: (abs(float(k) - float(spot)), float(k)))

        def _best_iv_at_strike(side: str) -> float | None:
            if atm_strike is None:
                return None
            cands = []
            for c in chain:
                if c.get("side") != side:
                    continue
                if c.get("strike") is None or float(c["strike"]) != float(atm_strike):
                    continue
                iv = c.get("iv")
                if iv is None:
                    continue
                mid = c.get("mid")
                if mid is None:
                    b, a = c.get("bid"), c.get("ask")
                    if b is not None and a is not None and a >= b:
                        mid = 0.5 * (b + a)
                cands.append((0 if mid is not None else 1, float(iv)))
            if not cands:
                return None
            cands.sort()
            return float(cands[0][1])

        atm_call_iv = _best_iv_at_strike("call")
        atm_put_iv = _best_iv_at_strike("put")
        atm_iv = 0.5 * (atm_call_iv + atm_put_iv) if (atm_call_iv is not None and atm_put_iv is not None) else (atm_call_iv if atm_call_iv is not None else atm_put_iv)

        atm_bidask_spread = None
        if atm_strike is not None:
            spreads = [float(c["ask"] - c["bid"]) for c in chain if c.get("strike") is not None and float(c["strike"]) == float(atm_strike) and c.get("bid") is not None and c.get("ask") is not None and c["ask"] >= c["bid"]]
            if spreads:
                atm_bidask_spread = _median(spreads)

        def _closest_iv_by_delta(side: str, target: float) -> float | None:
            best = None
            best_iv = None
            for c in chain:
                if c.get("side") != side:
                    continue
                iv, d = c.get("iv"), c.get("delta")
                if iv is None or d is None:
                    continue
                mid = c.get("mid")
                if mid is None:
                    b, a = c.get("bid"), c.get("ask")
                    if b is not None and a is not None and a >= b:
                        mid = 0.5 * (b + a)
                score = (0 if mid is not None else 1, abs(float(d) - float(target)))
                if best is None or score < best:
                    best = score
                    best_iv = float(iv)
            return best_iv

        iv_put_25d = _closest_iv_by_delta("put", -0.25)
        iv_call_25d = _closest_iv_by_delta("call", 0.25)

        skew_25d = None
        bf_25d = None
        if iv_put_25d is not None and iv_call_25d is not None:
            skew_25d = float(iv_put_25d - iv_call_25d)
            if atm_iv is not None:
                bf_25d = float(0.5 * (iv_put_25d + iv_call_25d) - atm_iv)

        put_volume = sum(int(c["volume"]) for c in chain if c.get("side") == "put" and c.get("volume") is not None)
        call_volume = sum(int(c["volume"]) for c in chain if c.get("side") == "call" and c.get("volume") is not None)
        put_oi = sum(int(c["open_interest"]) for c in chain if c.get("side") == "put" and c.get("open_interest") is not None)
        call_oi = sum(int(c["open_interest"]) for c in chain if c.get("side") == "call" and c.get("open_interest") is not None)

        pcr_volume = float(put_volume) / float(call_volume) if call_volume else None
        pcr_oi = float(put_oi) / float(call_oi) if call_oi else None

        itm_vol = atm_vol = otm_vol = 0
        atm_band = float(cfg.flow_atm_corridor_pct)
        if spot is not None and spot > 0:
            for c in chain:
                vol = int(c.get("volume") or 0)
                if vol <= 0:
                    continue
                k = c.get("strike")
                if k is None:
                    continue
                dist = abs(float(k) - float(spot)) / float(spot)
                if dist <= atm_band:
                    atm_vol += vol
                    continue
                sd = str(c.get("side") or "").lower()
                if sd == "call":
                    if float(k) < float(spot):
                        itm_vol += vol
                    else:
                        otm_vol += vol
                elif sd == "put":
                    if float(k) > float(spot):
                        itm_vol += vol
                    else:
                        otm_vol += vol
        tot_vol = int(itm_vol + atm_vol + otm_vol)

        prev = _fetch_prev_feature_row(cur, snapshot_ts)
        d_tot_vol = int(tot_vol - int(prev["tot_vol"])) if prev and prev.get("tot_vol") is not None else None
        d_call_oi = int(call_oi - int(prev["call_oi"])) if prev and prev.get("call_oi") is not None else None
        d_put_oi = int(put_oi - int(prev["put_oi"])) if prev and prev.get("put_oi") is not None else None

        hist_spots = _fetch_recent_spots(cur, snapshot_ts, 30)
        if spot is not None:
            hist_spots = hist_spots + [float(spot)]
        sma_spot_5 = mean(hist_spots[-5:]) if len(hist_spots) >= 1 else None
        sma_spot_20 = mean(hist_spots[-20:]) if len(hist_spots) >= 1 else None

        bb_mid_20 = bb_upper_20 = bb_lower_20 = bb_pctb_20 = None
        if len(hist_spots) >= 20:
            w = hist_spots[-20:]
            bb_mid_20 = float(mean(w))
            sd = _std(w)
            bb_upper_20 = float(bb_mid_20 + 2.0 * sd)
            bb_lower_20 = float(bb_mid_20 - 2.0 * sd)
            if spot is not None and bb_upper_20 > bb_lower_20:
                bb_pctb_20 = float((spot - bb_lower_20) / (bb_upper_20 - bb_lower_20))

        rsi_14 = _rsi(hist_spots, period=14) if hist_spots else None

        day_vals = _fetch_day_spot_vol(cur, snapshot_ts, tz_local)
        if spot is not None:
            day_vals = day_vals + [(float(spot), float(tot_vol))]
        twap_spot_day = float(sum(s for s, _ in day_vals) / len(day_vals)) if day_vals else None
        v_denom = sum(v for _, v in day_vals)
        vwap_chainweighted_spot_day = float(sum(s * v for s, v in day_vals) / v_denom) if v_denom > 0 else (twap_spot_day if day_vals else None)

        rsi_osob_label = "Overbought" if (rsi_14 is not None and rsi_14 > 70.0) else ("Oversold" if (rsi_14 is not None and rsi_14 < 30.0) else "Neutral")
        bb_osob_label = "Overbought" if (bb_pctb_20 is not None and bb_pctb_20 > 1.0) else ("Oversold" if (bb_pctb_20 is not None and bb_pctb_20 < 0.0) else "Neutral")
        pcr_oi_osob_label = "Oversold" if (pcr_oi is not None and pcr_oi >= 1.30) else ("Overbought" if (pcr_oi is not None and pcr_oi <= 0.70) else "Neutral")

        strike_hist = _fetch_strike_histories(cur, snapshot_ts, [float(s) for s in strikes], int(cfg.flow_history_per_strike))
        bucket_series = _fetch_bucket_history(cur, snapshot_ts, max(int(cfg.flow_bucket_z_window), 200))
        flow = compute_options_flow(chain, spot, cfg, strike_hist, bucket_series)

        low_quality = contract_count < int(min_contracts) or atm_iv is None

        feat = {
            "snapshot_ts": snapshot_ts,
            "expiration_date": exp_date,
            "spot": spot,
            "ttm_minutes": None,
            "atm_strike": atm_strike,
            "atm_iv": atm_iv,
            "atm_call_iv": atm_call_iv,
            "atm_put_iv": atm_put_iv,
            "atm_bidask_spread": atm_bidask_spread,
            "iv_put_25d": iv_put_25d,
            "iv_call_25d": iv_call_25d,
            "skew_25d": skew_25d,
            "bf_25d": bf_25d,
            "put_volume": put_volume,
            "call_volume": call_volume,
            "pcr_volume": pcr_volume,
            "put_oi": put_oi,
            "call_oi": call_oi,
            "pcr_oi": pcr_oi,
            "itm_vol": itm_vol,
            "atm_vol": atm_vol,
            "otm_vol": otm_vol,
            "tot_vol": tot_vol,
            "d_tot_vol": d_tot_vol,
            "d_call_oi": d_call_oi,
            "d_put_oi": d_put_oi,
            "sma_spot_5": sma_spot_5,
            "sma_spot_20": sma_spot_20,
            "bb_mid_20": bb_mid_20,
            "bb_upper_20": bb_upper_20,
            "bb_lower_20": bb_lower_20,
            "bb_pctb_20": bb_pctb_20,
            "rsi_14": rsi_14,
            "twap_spot_day": twap_spot_day,
            "vwap_chainweighted_spot_day": vwap_chainweighted_spot_day,
            "rsi_osob_label": rsi_osob_label,
            "bb_osob_label": bb_osob_label,
            "pcr_oi_osob_label": pcr_oi_osob_label,
            "flow_total_strikes": flow["flow_total_strikes"],
            "flow_pct_bullish": flow["flow_pct_bullish"],
            "flow_pct_bearish": flow["flow_pct_bearish"],
            "flow_breadth": flow["flow_breadth"],
            "flow_bucket_net_flow": flow["flow_bucket_net_flow"],
            "flow_bucket_robust_z": flow["flow_bucket_robust_z"],
            "flow_skew": flow["flow_skew"],
            "flow_confidence": flow["flow_confidence"],
            "flow_atm_corridor_net": flow["flow_atm_corridor_net"],
            "flow_atm_corridor_frac": flow["flow_atm_corridor_frac"],
            "flow_top3_share": flow["flow_top3_share"],
            "flow_top5_share": flow["flow_top5_share"],
            "flow_bias_summary": flow["flow_bias_summary"],
            "flow_breadth_pass": flow["flow_breadth_pass"],
            "flow_bucketz_pass": flow["flow_bucketz_pass"],
            "flow_live_ok_default": flow["flow_live_ok_default"],
            "contract_count": contract_count,
            "valid_iv_count": valid_iv_count,
            "valid_mid_count": valid_mid_count,
            "low_quality": bool(low_quality),
        }

        cur.execute(UPSERT_FEATURE_SQL, feat)
        for strike, netf in flow.get("flow_strike_net_map", {}).items():
            cur.execute(UPSERT_STRIKE_FLOW_SQL, (snapshot_ts, float(strike), float(netf)))
        conn.commit()
        return feat


def _fetch_next_feature_at_or_after(cur: psycopg.Cursor, *, ts_target: datetime, ts_max: datetime) -> dict[str, Any] | None:
    cur.execute(
        """
        SELECT snapshot_ts, atm_iv, skew_25d, low_quality
        FROM spx.chain_features_0dte
        WHERE snapshot_ts >= %s
          AND snapshot_ts <= %s
        ORDER BY snapshot_ts ASC
        LIMIT 1
        """,
        (ts_target, ts_max),
    )
    r = cur.fetchone()
    if not r:
        return None
    return {"snapshot_ts": r[0], "atm_iv": _as_float(r[1]), "skew_25d": _as_float(r[2]), "low_quality": bool(r[3])}


def compute_labels_for_snapshot(conn: psycopg.Connection, *, snapshot_ts: datetime, horizons_minutes: Iterable[int], max_future_lookahead_minutes: int, label_eps_atm_iv: float, label_eps_skew_25d: float) -> int:
    horizons = [int(h) for h in horizons_minutes]
    if not horizons:
        return 0

    with conn.cursor() as cur:
        cur.execute("SELECT snapshot_ts, atm_iv, skew_25d, low_quality FROM spx.chain_features_0dte WHERE snapshot_ts = %s", (snapshot_ts,))
        base = cur.fetchone()
        if not base:
            return 0

        atm_iv_t = _as_float(base[1])
        skew_25d_t = _as_float(base[2])
        lowq_t = bool(base[3])

        upserted = 0
        for h in horizons:
            target = snapshot_ts + timedelta(minutes=h)
            max_ts = target + timedelta(minutes=int(max_future_lookahead_minutes))
            fut = _fetch_next_feature_at_or_after(cur, ts_target=target, ts_max=max_ts)
            is_missing_future = fut is None

            atm_iv_tH = fut["atm_iv"] if fut else None
            skew_25d_tH = fut["skew_25d"] if fut else None
            lowq_tH = bool(fut["low_quality"]) if fut else False

            atm_iv_change = float(atm_iv_tH - atm_iv_t) if (atm_iv_t is not None and atm_iv_tH is not None) else None
            skew_change = float(skew_25d_tH - skew_25d_t) if (skew_25d_t is not None and skew_25d_tH is not None) else None

            cur.execute(
                UPSERT_LABEL_SQL,
                {
                    "snapshot_ts": snapshot_ts,
                    "horizon_minutes": h,
                    "atm_iv_t": atm_iv_t,
                    "atm_iv_tH": atm_iv_tH,
                    "atm_iv_change": atm_iv_change,
                    "label_atm_iv_dir": _sign_eps(atm_iv_change, float(label_eps_atm_iv)),
                    "skew_25d_t": skew_25d_t,
                    "skew_25d_tH": skew_25d_tH,
                    "skew_25d_change": skew_change,
                    "label_skew_25d_dir": _sign_eps(skew_change, float(label_eps_skew_25d)),
                    "is_missing_future": bool(is_missing_future),
                    "is_low_quality": bool(lowq_t or lowq_tH),
                },
            )
            upserted += 1

        conn.commit()
        return upserted


def _candidate_snapshot_ts(conn: psycopg.Connection, *, limit: int) -> list[datetime]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT oc.snapshot_ts
            FROM (
                SELECT DISTINCT snapshot_ts
                FROM spx.option_chain
                ORDER BY snapshot_ts DESC
                LIMIT %s
            ) oc
            LEFT JOIN spx.chain_features_0dte f ON f.snapshot_ts = oc.snapshot_ts
            WHERE f.snapshot_ts IS NULL
            ORDER BY oc.snapshot_ts ASC
            """,
            (int(limit),),
        )
        return [r[0] for r in cur.fetchall()]


def _candidate_label_snapshot_ts(conn: psycopg.Connection, *, limit: int) -> list[datetime]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT f.snapshot_ts
            FROM spx.chain_features_0dte f
            LEFT JOIN spx.chain_labels_0dte l ON l.snapshot_ts = f.snapshot_ts
            GROUP BY f.snapshot_ts
            HAVING COUNT(l.horizon_minutes) = 0
            ORDER BY f.snapshot_ts ASC
            LIMIT %s
            """,
            (int(limit),),
        )
        return [r[0] for r in cur.fetchall()]


def run_phase2_daemon(cfg: Phase2Config) -> None:
    task_path = os.getenv("PHASE2_TASK_PATH", "/mnt/options_ai/state/task_phase2.json")

    with psycopg.connect(cfg.db_dsn) as conn:
        ensure_phase2_schema(conn)
        ensure_outcomes_schema(conn)

    outcomes_cfg = OutcomesConfig(
        align_mode=cfg.outcome_align,
        horizons_td=tuple(int(x) for x in cfg.outcome_horizons_td),
        flat_pct_band=float(cfg.flat_pct_band),
        tz_local=cfg.tz_local,
    )

    while True:
        did = False
        try:
            with psycopg.connect(cfg.db_dsn) as conn:
                for ts in _candidate_snapshot_ts(conn, limit=cfg.batch_limit):
                    write_task_state(task_path, {"stage": "phase2_features", "snapshot_ts": ts.isoformat().replace("+00:00", "Z"), "started_at": utc_now_iso()})
                    r = compute_features_for_snapshot(conn, snapshot_ts=ts, tz_local=cfg.tz_local, min_contracts=cfg.min_contracts, cfg=cfg)
                    if r:
                        did = True

                for ts in _candidate_label_snapshot_ts(conn, limit=cfg.batch_limit):
                    write_task_state(task_path, {"stage": "phase2_labels", "snapshot_ts": ts.isoformat().replace("+00:00", "Z"), "started_at": utc_now_iso()})
                    n = compute_labels_for_snapshot(
                        conn,
                        snapshot_ts=ts,
                        horizons_minutes=cfg.horizons_minutes,
                        max_future_lookahead_minutes=cfg.max_future_lookahead_minutes,
                        label_eps_atm_iv=cfg.label_eps_atm_iv,
                        label_eps_skew_25d=cfg.label_eps_skew_25d,
                    )
                    if n:
                        did = True

                for ts in snapshots_missing_outcomes(conn, align_mode=cfg.outcome_align, limit=cfg.batch_limit):
                    write_task_state(task_path, {"stage": "phase3_outcomes", "snapshot_ts": ts.isoformat().replace("+00:00", "Z"), "started_at": utc_now_iso()})
                    n = compute_outcomes_for_snapshot(conn, snapshot_ts=ts, cfg=outcomes_cfg)
                    if n:
                        did = True

            if not did:
                write_task_state(task_path, None)
        except Exception:
            write_task_state(task_path, {"stage": "error", "at": utc_now_iso()})

        time.sleep(cfg.poll_seconds if not did else 0.1)


def _get_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in {"1", "true", "yes", "y", "on"}


def load_phase2_config_from_env() -> Phase2Config:
    db = os.getenv("PHASE2_DB_DSN", "").strip() or os.getenv("SPX_CHAIN_DATABASE_URL", "").strip() or os.getenv("DATABASE_URL", "").strip()
    if not db:
        raise RuntimeError("PHASE2_DB_DSN (or SPX_CHAIN_DATABASE_URL) is required")

    horizons_s = os.getenv("HORIZONS_MINUTES", "15,30,45,60,90").strip()
    horizons = tuple(int(x) for x in horizons_s.split(",") if x.strip())

    out_h_s = os.getenv("OUTCOME_HORIZONS_TD", "5,10,21").strip()
    out_h = tuple(int(x) for x in out_h_s.split(",") if x.strip())

    return Phase2Config(
        db_dsn=db,
        tz_local=os.getenv("TZ_LOCAL", TZ_LOCAL_DEFAULT).strip() or TZ_LOCAL_DEFAULT,
        horizons_minutes=horizons,
        max_future_lookahead_minutes=int(os.getenv("MAX_FUTURE_LOOKAHEAD_MINUTES", "120")),
        label_eps_atm_iv=float(os.getenv("LABEL_EPS_ATM_IV", "0.0025")),
        label_eps_skew_25d=float(os.getenv("LABEL_EPS_SKEW_25D", "0.0025")),
        min_contracts=int(os.getenv("MIN_CONTRACTS", "50")),
        flow_max_strike_dist_pct=float(os.getenv("FLOW_MAX_STRIKE_DIST_PCT", "0.40")),
        flow_use_moneyness_weight=_get_bool("FLOW_USE_MONEYNESS_WEIGHT", True),
        flow_use_gaussian=_get_bool("FLOW_USE_GAUSSIAN", True),
        flow_winsorize_bucket=_get_bool("FLOW_WINSORIZE_BUCKET", True),
        flow_confirm_fast_win=int(os.getenv("FLOW_CONFIRM_FAST_WIN", "10")),
        flow_confirm_slow_win=int(os.getenv("FLOW_CONFIRM_SLOW_WIN", "60")),
        flow_history_per_strike=int(os.getenv("FLOW_HISTORY_PER_STRIKE", "600")),
        flow_bucket_z_window=int(os.getenv("FLOW_BUCKET_Z_WINDOW", "60")),
        flow_min_breadth=float(os.getenv("FLOW_MIN_BREADTH", "0.60")),
        flow_min_bucket_z=float(os.getenv("FLOW_MIN_BUCKET_Z", "1.5")),
        flow_conf_min=float(os.getenv("FLOW_CONF_MIN", "0.60")),
        flow_atm_corridor_pct=float(os.getenv("FLOW_ATM_CORRIDOR_PCT", "0.01")),
        outcome_align=os.getenv("OUTCOME_ALIGN", "FirstOfDay").strip() or "FirstOfDay",
        outcome_horizons_td=out_h,
        flat_pct_band=float(os.getenv("FLAT_PCT_BAND", "0.0025")),
        poll_seconds=float(os.getenv("PHASE2_POLL_SECONDS", "15")),
        batch_limit=int(os.getenv("PHASE2_BATCH_LIMIT", "200")),
    )
