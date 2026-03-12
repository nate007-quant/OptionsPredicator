from __future__ import annotations

import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from statistics import mean
from typing import Any

import psycopg

from options_ai.features.flow_engine import compute_options_flow
from options_ai.term_expiration import pick_expiration_for_target_dte, term_bucket_name


TZ_LOCAL_DEFAULT = "America/Chicago"


@dataclass(frozen=True)
class Phase2TermConfig:
    db_dsn: str
    tz_local: str = TZ_LOCAL_DEFAULT

    target_dte_days: int = 7
    dte_tolerance_days: int = 2
    term_bucket: str = ""

    horizons_minutes: tuple[int, ...] = (5760, 10080)
    max_future_lookahead_minutes: int = 20160

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

    poll_seconds: float = 15.0
    batch_limit: int = 200


SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS spx;

CREATE TABLE IF NOT EXISTS spx.chain_features_term (
  snapshot_ts TIMESTAMPTZ NOT NULL,
  expiration_date DATE NOT NULL,

  term_bucket TEXT NOT NULL,
  target_dte_days INT NOT NULL,
  dte_tolerance_days INT NOT NULL,
  dte_days INT,
  dte_diff INT,

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
  computed_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  PRIMARY KEY (snapshot_ts, expiration_date)
);

CREATE TABLE IF NOT EXISTS spx.chain_flow_strike_term (
  snapshot_ts TIMESTAMPTZ NOT NULL,
  term_bucket TEXT NOT NULL,
  strike NUMERIC NOT NULL,
  net_flow NUMERIC,
  PRIMARY KEY (snapshot_ts, term_bucket, strike)
);

CREATE INDEX IF NOT EXISTS chain_features_term_bucket_ts_idx ON spx.chain_features_term (term_bucket, snapshot_ts DESC);
CREATE INDEX IF NOT EXISTS chain_features_term_exp_ts_idx ON spx.chain_features_term (expiration_date, snapshot_ts DESC);
CREATE INDEX IF NOT EXISTS chain_flow_strike_term_bucket_ts_idx ON spx.chain_flow_strike_term (term_bucket, snapshot_ts DESC);

CREATE TABLE IF NOT EXISTS spx.chain_labels_term (
  snapshot_ts TIMESTAMPTZ NOT NULL,
  expiration_date DATE NOT NULL,
  horizon_minutes INT NOT NULL,

  term_bucket TEXT NOT NULL,

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

  PRIMARY KEY (snapshot_ts, expiration_date, horizon_minutes)
);

CREATE INDEX IF NOT EXISTS chain_labels_term_bucket_ts_idx ON spx.chain_labels_term (term_bucket, snapshot_ts DESC);
"""

ALTER_SQL = """
ALTER TABLE spx.chain_features_term ADD COLUMN IF NOT EXISTS itm_vol BIGINT;
ALTER TABLE spx.chain_features_term ADD COLUMN IF NOT EXISTS atm_vol BIGINT;
ALTER TABLE spx.chain_features_term ADD COLUMN IF NOT EXISTS otm_vol BIGINT;
ALTER TABLE spx.chain_features_term ADD COLUMN IF NOT EXISTS tot_vol BIGINT;
ALTER TABLE spx.chain_features_term ADD COLUMN IF NOT EXISTS d_tot_vol BIGINT;
ALTER TABLE spx.chain_features_term ADD COLUMN IF NOT EXISTS d_call_oi BIGINT;
ALTER TABLE spx.chain_features_term ADD COLUMN IF NOT EXISTS d_put_oi BIGINT;
ALTER TABLE spx.chain_features_term ADD COLUMN IF NOT EXISTS sma_spot_5 NUMERIC;
ALTER TABLE spx.chain_features_term ADD COLUMN IF NOT EXISTS sma_spot_20 NUMERIC;
ALTER TABLE spx.chain_features_term ADD COLUMN IF NOT EXISTS bb_mid_20 NUMERIC;
ALTER TABLE spx.chain_features_term ADD COLUMN IF NOT EXISTS bb_upper_20 NUMERIC;
ALTER TABLE spx.chain_features_term ADD COLUMN IF NOT EXISTS bb_lower_20 NUMERIC;
ALTER TABLE spx.chain_features_term ADD COLUMN IF NOT EXISTS bb_pctb_20 NUMERIC;
ALTER TABLE spx.chain_features_term ADD COLUMN IF NOT EXISTS rsi_14 NUMERIC;
ALTER TABLE spx.chain_features_term ADD COLUMN IF NOT EXISTS twap_spot_day NUMERIC;
ALTER TABLE spx.chain_features_term ADD COLUMN IF NOT EXISTS vwap_chainweighted_spot_day NUMERIC;
ALTER TABLE spx.chain_features_term ADD COLUMN IF NOT EXISTS rsi_osob_label TEXT;
ALTER TABLE spx.chain_features_term ADD COLUMN IF NOT EXISTS bb_osob_label TEXT;
ALTER TABLE spx.chain_features_term ADD COLUMN IF NOT EXISTS pcr_oi_osob_label TEXT;
ALTER TABLE spx.chain_features_term ADD COLUMN IF NOT EXISTS flow_total_strikes INT;
ALTER TABLE spx.chain_features_term ADD COLUMN IF NOT EXISTS flow_pct_bullish NUMERIC;
ALTER TABLE spx.chain_features_term ADD COLUMN IF NOT EXISTS flow_pct_bearish NUMERIC;
ALTER TABLE spx.chain_features_term ADD COLUMN IF NOT EXISTS flow_breadth NUMERIC;
ALTER TABLE spx.chain_features_term ADD COLUMN IF NOT EXISTS flow_bucket_net_flow NUMERIC;
ALTER TABLE spx.chain_features_term ADD COLUMN IF NOT EXISTS flow_bucket_robust_z NUMERIC;
ALTER TABLE spx.chain_features_term ADD COLUMN IF NOT EXISTS flow_skew NUMERIC;
ALTER TABLE spx.chain_features_term ADD COLUMN IF NOT EXISTS flow_confidence NUMERIC;
ALTER TABLE spx.chain_features_term ADD COLUMN IF NOT EXISTS flow_atm_corridor_net NUMERIC;
ALTER TABLE spx.chain_features_term ADD COLUMN IF NOT EXISTS flow_atm_corridor_frac NUMERIC;
ALTER TABLE spx.chain_features_term ADD COLUMN IF NOT EXISTS flow_top3_share NUMERIC;
ALTER TABLE spx.chain_features_term ADD COLUMN IF NOT EXISTS flow_top5_share NUMERIC;
ALTER TABLE spx.chain_features_term ADD COLUMN IF NOT EXISTS flow_bias_summary TEXT;
ALTER TABLE spx.chain_features_term ADD COLUMN IF NOT EXISTS flow_breadth_pass BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE spx.chain_features_term ADD COLUMN IF NOT EXISTS flow_bucketz_pass BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE spx.chain_features_term ADD COLUMN IF NOT EXISTS flow_live_ok_default BOOLEAN NOT NULL DEFAULT TRUE;
"""

UPSERT_FEATURE_SQL = """
INSERT INTO spx.chain_features_term (
  snapshot_ts, expiration_date,
  term_bucket, target_dte_days, dte_tolerance_days, dte_days, dte_diff,
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
  contract_count, valid_iv_count, valid_mid_count, low_quality
) VALUES (
  %(snapshot_ts)s, %(expiration_date)s,
  %(term_bucket)s, %(target_dte_days)s, %(dte_tolerance_days)s, %(dte_days)s, %(dte_diff)s,
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
  %(contract_count)s, %(valid_iv_count)s, %(valid_mid_count)s, %(low_quality)s
)
ON CONFLICT (snapshot_ts, expiration_date) DO UPDATE SET
  term_bucket = EXCLUDED.term_bucket,
  target_dte_days = EXCLUDED.target_dte_days,
  dte_tolerance_days = EXCLUDED.dte_tolerance_days,
  dte_days = EXCLUDED.dte_days,
  dte_diff = EXCLUDED.dte_diff,
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
INSERT INTO spx.chain_flow_strike_term (snapshot_ts, term_bucket, strike, net_flow)
VALUES (%s, %s, %s, %s)
ON CONFLICT (snapshot_ts, term_bucket, strike) DO UPDATE SET net_flow = EXCLUDED.net_flow;
"""

UPSERT_LABEL_SQL = """
INSERT INTO spx.chain_labels_term (
  snapshot_ts, expiration_date, horizon_minutes,
  term_bucket,
  atm_iv_t, atm_iv_tH, atm_iv_change, label_atm_iv_dir,
  skew_25d_t, skew_25d_tH, skew_25d_change, label_skew_25d_dir,
  is_missing_future, is_low_quality
) VALUES (
  %(snapshot_ts)s, %(expiration_date)s, %(horizon_minutes)s,
  %(term_bucket)s,
  %(atm_iv_t)s, %(atm_iv_tH)s, %(atm_iv_change)s, %(label_atm_iv_dir)s,
  %(skew_25d_t)s, %(skew_25d_tH)s, %(skew_25d_change)s, %(label_skew_25d_dir)s,
  %(is_missing_future)s, %(is_low_quality)s
)
ON CONFLICT (snapshot_ts, expiration_date, horizon_minutes) DO UPDATE SET
  term_bucket = EXCLUDED.term_bucket,
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


def _median(vals: list[float]) -> float | None:
    if not vals:
        return None
    s = sorted(vals)
    n = len(s)
    if n % 2 == 1:
        return float(s[n // 2])
    return 0.5 * (float(s[n // 2 - 1]) + float(s[n // 2]))


def _std(vals: list[float]) -> float:
    if len(vals) <= 1:
        return 0.0
    mu = mean(vals)
    return float((sum((x - mu) ** 2 for x in vals) / len(vals)) ** 0.5)


def _rsi(values: list[float], period: int = 14) -> float | None:
    if len(values) < period + 1:
        return None
    d = [values[i] - values[i - 1] for i in range(1, len(values))]
    w = d[-period:]
    g = sum(max(x, 0.0) for x in w) / period
    l = sum(max(-x, 0.0) for x in w) / period
    if l == 0:
        return 100.0 if g > 0 else 50.0
    rs = g / l
    return 100.0 - (100.0 / (1.0 + rs))


def ensure_schema(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)
        cur.execute(ALTER_SQL)
        conn.commit()


def _fetch_chain_rows(cur: psycopg.Cursor, snapshot_ts: datetime, expiration_date: Any) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT side, strike, iv, delta, mid, bid, ask, underlying_price, volume, open_interest
        FROM spx.option_chain
        WHERE snapshot_ts = %s AND expiration_date = %s
        """,
        (snapshot_ts, expiration_date),
    )
    out = []
    for r in cur.fetchall():
        out.append(
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
    return out


def _pick_atm_strike(chain: list[dict[str, Any]], spot: float | None) -> float | None:
    strikes = sorted({c["strike"] for c in chain if c.get("strike") is not None})
    if spot is None or not strikes:
        return None
    return min(strikes, key=lambda k: (abs(float(k) - float(spot)), float(k)))


def _fetch_prev_feature(cur: psycopg.Cursor, snapshot_ts: datetime, term_bucket: str) -> dict[str, Any] | None:
    cur.execute(
        """
        SELECT tot_vol, call_oi, put_oi
        FROM spx.chain_features_term
        WHERE term_bucket=%s AND snapshot_ts < %s
        ORDER BY snapshot_ts DESC
        LIMIT 1
        """,
        (term_bucket, snapshot_ts),
    )
    r = cur.fetchone()
    if not r:
        return None
    return {"tot_vol": _as_int(r[0]), "call_oi": _as_int(r[1]), "put_oi": _as_int(r[2])}


def _fetch_recent_spots(cur: psycopg.Cursor, snapshot_ts: datetime, term_bucket: str, limit: int) -> list[float]:
    cur.execute(
        """
        SELECT spot
        FROM spx.chain_features_term
        WHERE term_bucket=%s AND snapshot_ts < %s AND spot IS NOT NULL
        ORDER BY snapshot_ts DESC
        LIMIT %s
        """,
        (term_bucket, snapshot_ts, int(limit)),
    )
    vals = [float(r[0]) for r in cur.fetchall() if r[0] is not None]
    vals.reverse()
    return vals


def _fetch_day_spot_vol(cur: psycopg.Cursor, snapshot_ts: datetime, term_bucket: str, tz_local: str) -> list[tuple[float, float]]:
    cur.execute(
        """
        SELECT spot, COALESCE(tot_vol,0)
        FROM spx.chain_features_term
        WHERE term_bucket=%s
          AND snapshot_ts < %s
          AND (snapshot_ts AT TIME ZONE %s)::date = (%s AT TIME ZONE %s)::date
          AND spot IS NOT NULL
        ORDER BY snapshot_ts ASC
        """,
        (term_bucket, snapshot_ts, tz_local, snapshot_ts, tz_local),
    )
    return [(float(s), float(v or 0.0)) for s, v in cur.fetchall() if s is not None]


def _fetch_strike_histories(cur: psycopg.Cursor, snapshot_ts: datetime, term_bucket: str, strikes: list[float], max_per: int) -> dict[float, list[float]]:
    if not strikes:
        return {}
    cur.execute(
        """
        SELECT strike, net_flow
        FROM (
          SELECT strike, net_flow,
                 ROW_NUMBER() OVER (PARTITION BY strike ORDER BY snapshot_ts DESC) rn
          FROM spx.chain_flow_strike_term
          WHERE term_bucket=%s AND snapshot_ts < %s AND strike = ANY(%s)
        ) q
        WHERE rn <= %s
        ORDER BY strike ASC, rn DESC
        """,
        (term_bucket, snapshot_ts, strikes, int(max_per)),
    )
    out: dict[float, list[float]] = {}
    for k, v in cur.fetchall():
        if k is None or v is None:
            continue
        out.setdefault(float(k), []).append(float(v))
    return out


def _fetch_bucket_series(cur: psycopg.Cursor, snapshot_ts: datetime, term_bucket: str, limit: int) -> list[float]:
    cur.execute(
        """
        SELECT flow_bucket_net_flow
        FROM spx.chain_features_term
        WHERE term_bucket=%s AND snapshot_ts < %s AND flow_bucket_net_flow IS NOT NULL
        ORDER BY snapshot_ts DESC
        LIMIT %s
        """,
        (term_bucket, snapshot_ts, int(limit)),
    )
    vals = [float(r[0]) for r in cur.fetchall() if r[0] is not None]
    vals.reverse()
    return vals


def compute_features_for_snapshot(conn: psycopg.Connection, *, snapshot_ts: datetime, cfg: Phase2TermConfig) -> dict[str, Any] | None:
    picked = pick_expiration_for_target_dte(
        conn,
        snapshot_ts=snapshot_ts,
        target_dte_days=int(cfg.target_dte_days),
        dte_tolerance_days=int(cfg.dte_tolerance_days),
        tz_local=str(cfg.tz_local),
    )
    if not picked:
        return None

    exp_date = picked.expiration_date

    with conn.cursor() as cur:
        chain = _fetch_chain_rows(cur, snapshot_ts, exp_date)
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

        atm_strike = _pick_atm_strike(chain, spot)

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
                iv = c.get("iv")
                d = c.get("delta")
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
        if spot is not None and spot > 0:
            for c in chain:
                vol = int(c.get("volume") or 0)
                if vol <= 0 or c.get("strike") is None:
                    continue
                dist = abs(float(c["strike"]) - float(spot)) / float(spot)
                if dist <= float(cfg.flow_atm_corridor_pct):
                    atm_vol += vol
                    continue
                sd = str(c.get("side") or "").lower()
                if sd == "call":
                    itm_vol += vol if float(c["strike"]) < float(spot) else 0
                    otm_vol += vol if float(c["strike"]) >= float(spot) else 0
                elif sd == "put":
                    itm_vol += vol if float(c["strike"]) > float(spot) else 0
                    otm_vol += vol if float(c["strike"]) <= float(spot) else 0

        tot_vol = int(itm_vol + atm_vol + otm_vol)

        prev = _fetch_prev_feature(cur, snapshot_ts, cfg.term_bucket)
        d_tot_vol = int(tot_vol - int(prev["tot_vol"])) if prev and prev.get("tot_vol") is not None else None
        d_call_oi = int(call_oi - int(prev["call_oi"])) if prev and prev.get("call_oi") is not None else None
        d_put_oi = int(put_oi - int(prev["put_oi"])) if prev and prev.get("put_oi") is not None else None

        hist_spots = _fetch_recent_spots(cur, snapshot_ts, cfg.term_bucket, 30)
        if spot is not None:
            hist_spots.append(float(spot))

        sma_spot_5 = mean(hist_spots[-5:]) if hist_spots else None
        sma_spot_20 = mean(hist_spots[-20:]) if hist_spots else None

        bb_mid_20 = bb_upper_20 = bb_lower_20 = bb_pctb_20 = None
        if len(hist_spots) >= 20:
            w = hist_spots[-20:]
            bb_mid_20 = float(mean(w))
            sd = _std(w)
            bb_upper_20 = bb_mid_20 + 2.0 * sd
            bb_lower_20 = bb_mid_20 - 2.0 * sd
            if spot is not None and bb_upper_20 > bb_lower_20:
                bb_pctb_20 = float((spot - bb_lower_20) / (bb_upper_20 - bb_lower_20))

        rsi_14 = _rsi(hist_spots, 14) if hist_spots else None

        day_vals = _fetch_day_spot_vol(cur, snapshot_ts, cfg.term_bucket, cfg.tz_local)
        if spot is not None:
            day_vals.append((float(spot), float(tot_vol)))
        twap_spot_day = float(sum(s for s, _ in day_vals) / len(day_vals)) if day_vals else None
        denom = sum(v for _, v in day_vals)
        vwap_chainweighted_spot_day = float(sum(s * v for s, v in day_vals) / denom) if denom > 0 else twap_spot_day

        rsi_osob_label = "Overbought" if (rsi_14 is not None and rsi_14 > 70.0) else ("Oversold" if (rsi_14 is not None and rsi_14 < 30.0) else "Neutral")
        bb_osob_label = "Overbought" if (bb_pctb_20 is not None and bb_pctb_20 > 1.0) else ("Oversold" if (bb_pctb_20 is not None and bb_pctb_20 < 0.0) else "Neutral")
        pcr_oi_osob_label = "Oversold" if (pcr_oi is not None and pcr_oi >= 1.30) else ("Overbought" if (pcr_oi is not None and pcr_oi <= 0.70) else "Neutral")

        strikes = sorted({float(c["strike"]) for c in chain if c.get("strike") is not None})
        strike_hist = _fetch_strike_histories(cur, snapshot_ts, cfg.term_bucket, strikes, int(cfg.flow_history_per_strike))
        bucket_series = _fetch_bucket_series(cur, snapshot_ts, cfg.term_bucket, max(int(cfg.flow_bucket_z_window), 200))
        flow = compute_options_flow(chain, spot, cfg, strike_hist, bucket_series)

        low_quality = contract_count < int(cfg.min_contracts) or atm_iv is None

        feat = {
            "snapshot_ts": snapshot_ts,
            "expiration_date": exp_date,
            "term_bucket": cfg.term_bucket,
            "target_dte_days": int(cfg.target_dte_days),
            "dte_tolerance_days": int(cfg.dte_tolerance_days),
            "dte_days": int(picked.dte_days),
            "dte_diff": int(picked.dte_diff),
            "spot": spot,
            "ttm_minutes": int(picked.dte_days) * 1440,
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
        for k, v in flow.get("flow_strike_net_map", {}).items():
            cur.execute(UPSERT_STRIKE_FLOW_SQL, (snapshot_ts, cfg.term_bucket, float(k), float(v)))
        conn.commit()
        return feat


def _fetch_next_feature_row(cur: psycopg.Cursor, *, term_bucket: str, ts_target: datetime, ts_max: datetime) -> dict[str, Any] | None:
    cur.execute(
        """
        SELECT snapshot_ts, atm_iv, skew_25d, low_quality
        FROM spx.chain_features_term
        WHERE term_bucket = %s
          AND snapshot_ts >= %s
          AND snapshot_ts <= %s
        ORDER BY snapshot_ts ASC
        LIMIT 1
        """,
        (term_bucket, ts_target, ts_max),
    )
    r = cur.fetchone()
    if not r:
        return None
    return {"snapshot_ts": r[0], "atm_iv": _as_float(r[1]), "skew_25d": _as_float(r[2]), "low_quality": bool(r[3])}


def compute_labels_for_feature_row(conn: psycopg.Connection, *, snapshot_ts: datetime, expiration_date: Any, cfg: Phase2TermConfig) -> int:
    horizons = [int(h) for h in cfg.horizons_minutes]
    if not horizons:
        return 0

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT atm_iv, skew_25d, low_quality
            FROM spx.chain_features_term
            WHERE snapshot_ts=%s AND expiration_date=%s AND term_bucket=%s
            """,
            (snapshot_ts, expiration_date, cfg.term_bucket),
        )
        base = cur.fetchone()
        if not base:
            return 0

        atm_iv_t = _as_float(base[0])
        skew_25d_t = _as_float(base[1])
        lowq_t = bool(base[2])

        upserted = 0
        for h in horizons:
            target = snapshot_ts + timedelta(minutes=h)
            max_ts = target + timedelta(minutes=int(cfg.max_future_lookahead_minutes))
            fut = _fetch_next_feature_row(cur, term_bucket=cfg.term_bucket, ts_target=target, ts_max=max_ts)

            atm_iv_tH = fut["atm_iv"] if fut else None
            skew_tH = fut["skew_25d"] if fut else None
            lowq_tH = bool(fut["low_quality"]) if fut else False

            atm_iv_change = float(atm_iv_tH - atm_iv_t) if (atm_iv_t is not None and atm_iv_tH is not None) else None
            skew_change = float(skew_tH - skew_25d_t) if (skew_25d_t is not None and skew_tH is not None) else None

            cur.execute(
                UPSERT_LABEL_SQL,
                {
                    "snapshot_ts": snapshot_ts,
                    "expiration_date": expiration_date,
                    "horizon_minutes": int(h),
                    "term_bucket": cfg.term_bucket,
                    "atm_iv_t": atm_iv_t,
                    "atm_iv_tH": atm_iv_tH,
                    "atm_iv_change": atm_iv_change,
                    "label_atm_iv_dir": _sign_eps(atm_iv_change, float(cfg.label_eps_atm_iv)),
                    "skew_25d_t": skew_25d_t,
                    "skew_25d_tH": skew_tH,
                    "skew_25d_change": skew_change,
                    "label_skew_25d_dir": _sign_eps(skew_change, float(cfg.label_eps_skew_25d)),
                    "is_missing_future": bool(fut is None),
                    "is_low_quality": bool(lowq_t or lowq_tH),
                },
            )
            upserted += 1

        conn.commit()
        return upserted


def _candidate_snapshot_ts(conn: psycopg.Connection, *, cfg: Phase2TermConfig, limit: int) -> list[datetime]:
    n = max(1, int(limit))
    n_recent = max(1, n // 2)
    n_hist = max(1, n - n_recent)
    max_h = max((int(x) for x in cfg.horizons_minutes), default=0)

    with conn.cursor() as cur:
        cur.execute("SELECT max(snapshot_ts) FROM spx.option_chain")
        r = cur.fetchone()
        max_ts = r[0] if r else None

        cur.execute(
            """
            SELECT DISTINCT snapshot_ts
            FROM spx.option_chain
            ORDER BY snapshot_ts DESC
            LIMIT %s
            """,
            (n_recent,),
        )
        recent = [x[0] for x in cur.fetchall()]

        hist: list[datetime] = []
        if max_ts is not None and max_h > 0:
            cutoff = max_ts - timedelta(minutes=int(max_h))
            cur.execute(
                """
                SELECT DISTINCT snapshot_ts
                FROM spx.option_chain
                WHERE snapshot_ts <= %s
                ORDER BY snapshot_ts DESC
                LIMIT %s
                """,
                (cutoff, n_hist),
            )
            hist = [x[0] for x in cur.fetchall()]

    out: list[datetime] = []
    seen: set[datetime] = set()
    for ts in recent + hist:
        if ts in seen:
            continue
        seen.add(ts)
        out.append(ts)
    return out


def _feature_rows_missing_labels(conn: psycopg.Connection, *, term_bucket: str, limit: int) -> list[tuple[datetime, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT f.snapshot_ts, f.expiration_date
            FROM spx.chain_features_term f
            WHERE f.term_bucket = %s
              AND NOT EXISTS (
                SELECT 1
                FROM spx.chain_labels_term l
                WHERE l.snapshot_ts = f.snapshot_ts
                  AND l.expiration_date = f.expiration_date
              )
            ORDER BY f.snapshot_ts DESC
            LIMIT %s
            """,
            (term_bucket, int(limit)),
        )
        return [(r[0], r[1]) for r in cur.fetchall()]


def run_daemon(cfg: Phase2TermConfig) -> None:
    with psycopg.connect(cfg.db_dsn) as conn:
        ensure_schema(conn)

    while True:
        did = False
        try:
            with psycopg.connect(cfg.db_dsn) as conn:
                for ts in _candidate_snapshot_ts(conn, cfg=cfg, limit=cfg.batch_limit):
                    if compute_features_for_snapshot(conn, snapshot_ts=ts, cfg=cfg):
                        did = True

                for ts, exp in _feature_rows_missing_labels(conn, term_bucket=cfg.term_bucket, limit=cfg.batch_limit):
                    n = compute_labels_for_feature_row(conn, snapshot_ts=ts, expiration_date=exp, cfg=cfg)
                    if n:
                        did = True
        except Exception:
            pass

        time.sleep(cfg.poll_seconds if not did else 0.1)


def _get_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in {"1", "true", "yes", "y", "on"}


def load_config_from_env() -> Phase2TermConfig:
    db = (
        os.getenv("PHASE2_DB_DSN", "").strip()
        or os.getenv("SPX_CHAIN_DATABASE_URL", "").strip()
        or os.getenv("DATABASE_URL", "").strip()
    )
    if not db:
        raise RuntimeError("PHASE2_DB_DSN (or SPX_CHAIN_DATABASE_URL) is required")

    target = int(os.getenv("TARGET_DTE_DAYS", "7"))
    tol = int(os.getenv("DTE_TOLERANCE_DAYS", "2"))
    bucket = os.getenv("TERM_BUCKET", "").strip() or term_bucket_name(target_dte_days=target, dte_tolerance_days=tol)

    horizons_s = os.getenv("HORIZONS_MINUTES", "5760,10080").strip()
    horizons = tuple(int(x) for x in horizons_s.split(",") if x.strip())

    return Phase2TermConfig(
        db_dsn=db,
        tz_local=os.getenv("TZ_LOCAL", TZ_LOCAL_DEFAULT).strip() or TZ_LOCAL_DEFAULT,
        target_dte_days=target,
        dte_tolerance_days=tol,
        term_bucket=bucket,
        horizons_minutes=horizons,
        max_future_lookahead_minutes=int(os.getenv("MAX_FUTURE_LOOKAHEAD_MINUTES", "20160")),
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
        poll_seconds=float(os.getenv("PHASE2_POLL_SECONDS", "15")),
        batch_limit=int(os.getenv("PHASE2_BATCH_LIMIT", "200")),
    )
