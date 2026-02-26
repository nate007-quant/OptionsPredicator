from __future__ import annotations

import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

from zoneinfo import ZoneInfo

import psycopg


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

    # Poll loop
    poll_seconds: float = 15.0

    # Backfill window when starting up: how many distinct snapshot_ts to consider per loop.
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

  contract_count INT,
  valid_iv_count INT,
  valid_mid_count INT,
  low_quality BOOLEAN NOT NULL DEFAULT FALSE,
  computed_at TIMESTAMPTZ NOT NULL DEFAULT now()
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
"""


UPSERT_FEATURE_SQL = """
INSERT INTO spx.chain_features_0dte (
  snapshot_ts, expiration_date,
  spot, ttm_minutes,
  atm_strike, atm_iv, atm_call_iv, atm_put_iv, atm_bidask_spread,
  iv_put_25d, iv_call_25d, skew_25d, bf_25d,
  put_volume, call_volume, pcr_volume,
  put_oi, call_oi, pcr_oi,
  contract_count, valid_iv_count, valid_mid_count,
  low_quality
) VALUES (
  %(snapshot_ts)s, %(expiration_date)s,
  %(spot)s, %(ttm_minutes)s,
  %(atm_strike)s, %(atm_iv)s, %(atm_call_iv)s, %(atm_put_iv)s, %(atm_bidask_spread)s,
  %(iv_put_25d)s, %(iv_call_25d)s, %(skew_25d)s, %(bf_25d)s,
  %(put_volume)s, %(call_volume)s, %(pcr_volume)s,
  %(put_oi)s, %(call_oi)s, %(pcr_oi)s,
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
  contract_count = EXCLUDED.contract_count,
  valid_iv_count = EXCLUDED.valid_iv_count,
  valid_mid_count = EXCLUDED.valid_mid_count,
  low_quality = EXCLUDED.low_quality,
  computed_at = now();
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


def ensure_phase2_schema(conn: psycopg.Connection) -> None:
    # No extension requirements; pure Postgres.
    with conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)
        conn.commit()


def _local_trade_date(snapshot_ts: datetime, tz_local: str) -> datetime.date:
    tz = ZoneInfo(tz_local)
    return snapshot_ts.astimezone(tz).date()


def _fetch_chain_rows_for_snapshot(
    cur: psycopg.Cursor,
    snapshot_ts: datetime,
    expiration_date: datetime.date,
) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT
          side, strike, iv, delta,
          mid, bid, ask,
          underlying_price,
          volume, open_interest
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


def compute_features_for_snapshot(
    conn: psycopg.Connection,
    *,
    snapshot_ts: datetime,
    tz_local: str,
    min_contracts: int,
) -> dict[str, Any] | None:
    """Compute and upsert 0DTE features for one snapshot_ts.

    Returns inserted/updated feature dict, or None if snapshot skipped (no 0DTE rows).
    """

    exp_date = _local_trade_date(snapshot_ts, tz_local)

    with conn.cursor() as cur:
        chain = _fetch_chain_rows_for_snapshot(cur, snapshot_ts, exp_date)

        if not chain:
            return None

        contract_count = len(chain)

        # Spot = median underlying_price
        spot_vals = [c["underlying_price"] for c in chain if c.get("underlying_price") is not None]
        spot = _median([float(x) for x in spot_vals if x is not None])

        # Valid counts
        valid_iv_count = sum(1 for c in chain if c.get("iv") is not None)
        # mid validity: prefer stored mid, otherwise compute from bid/ask
        valid_mid_count = 0
        for c in chain:
            mid = c.get("mid")
            if mid is None:
                b, a = c.get("bid"), c.get("ask")
                if b is not None and a is not None and a >= b:
                    mid = 0.5 * (b + a)
            if mid is not None:
                valid_mid_count += 1

        # ATM strike selection
        strikes = sorted({c["strike"] for c in chain if c.get("strike") is not None})
        atm_strike = None
        if spot is not None and strikes:
            best = None
            for k in strikes:
                d = abs(float(k) - float(spot))
                # tiebreak: lower strike
                cand = (d, float(k))
                if best is None or cand < best:
                    best = cand
                    atm_strike = float(k)

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
                # Prefer those with a valid mid; else allow.
                mid = c.get("mid")
                if mid is None:
                    b, a = c.get("bid"), c.get("ask")
                    if b is not None and a is not None and a >= b:
                        mid = 0.5 * (b + a)
                score = 0 if mid is not None else 1
                cands.append((score, float(iv)))
            if not cands:
                return None
            cands.sort()
            return float(cands[0][1])

        atm_call_iv = _best_iv_at_strike("call")
        atm_put_iv = _best_iv_at_strike("put")
        atm_iv = None
        if atm_call_iv is not None and atm_put_iv is not None:
            atm_iv = 0.5 * (atm_call_iv + atm_put_iv)
        else:
            atm_iv = atm_call_iv if atm_call_iv is not None else atm_put_iv

        # ATM bid-ask spread (optional)
        atm_bidask_spread = None
        if atm_strike is not None:
            spreads: list[float] = []
            for c in chain:
                if c.get("strike") is None or float(c["strike"]) != float(atm_strike):
                    continue
                b, a = c.get("bid"), c.get("ask")
                if b is None or a is None:
                    continue
                if a < b:
                    continue
                spreads.append(float(a - b))
            if spreads:
                atm_bidask_spread = _median(spreads)

        # 25d selection using delta
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
                # Prefer valid quote but not required
                mid = c.get("mid")
                if mid is None:
                    b, a = c.get("bid"), c.get("ask")
                    if b is not None and a is not None and a >= b:
                        mid = 0.5 * (b + a)
                dist = abs(float(d) - float(target))
                score = (0 if mid is not None else 1, dist)
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

        # Flow features
        put_volume = sum(int(c["volume"]) for c in chain if c.get("side") == "put" and c.get("volume") is not None)
        call_volume = sum(int(c["volume"]) for c in chain if c.get("side") == "call" and c.get("volume") is not None)
        put_oi = sum(int(c["open_interest"]) for c in chain if c.get("side") == "put" and c.get("open_interest") is not None)
        call_oi = sum(int(c["open_interest"]) for c in chain if c.get("side") == "call" and c.get("open_interest") is not None)

        pcr_volume = None
        if call_volume and call_volume != 0:
            pcr_volume = float(put_volume) / float(call_volume)
        pcr_oi = None
        if call_oi and call_oi != 0:
            pcr_oi = float(put_oi) / float(call_oi)

        low_quality = False
        if contract_count < int(min_contracts):
            low_quality = True
        if atm_iv is None:
            low_quality = True

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
            "contract_count": contract_count,
            "valid_iv_count": valid_iv_count,
            "valid_mid_count": valid_mid_count,
            "low_quality": bool(low_quality),
        }

        cur.execute(UPSERT_FEATURE_SQL, feat)
        conn.commit()
        return feat


def _fetch_next_feature_at_or_after(
    cur: psycopg.Cursor,
    *,
    ts_target: datetime,
    ts_max: datetime,
) -> dict[str, Any] | None:
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
    return {
        "snapshot_ts": r[0],
        "atm_iv": _as_float(r[1]),
        "skew_25d": _as_float(r[2]),
        "low_quality": bool(r[3]),
    }


def compute_labels_for_snapshot(
    conn: psycopg.Connection,
    *,
    snapshot_ts: datetime,
    horizons_minutes: Iterable[int],
    max_future_lookahead_minutes: int,
    label_eps_atm_iv: float,
    label_eps_skew_25d: float,
) -> int:
    """Compute label rows for (snapshot_ts, horizon) using feature table.

    Returns number of label rows upserted.
    """

    horizons = [int(h) for h in horizons_minutes]
    if not horizons:
        return 0

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT snapshot_ts, atm_iv, skew_25d, low_quality
            FROM spx.chain_features_0dte
            WHERE snapshot_ts = %s
            """,
            (snapshot_ts,),
        )
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

            atm_iv_change = None
            if atm_iv_t is not None and atm_iv_tH is not None:
                atm_iv_change = float(atm_iv_tH - atm_iv_t)

            skew_change = None
            if skew_25d_t is not None and skew_25d_tH is not None:
                skew_change = float(skew_25d_tH - skew_25d_t)

            row = {
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
            }

            cur.execute(UPSERT_LABEL_SQL, row)
            upserted += 1

        conn.commit()
        return upserted


def _candidate_snapshot_ts(conn: psycopg.Connection, *, limit: int) -> list[datetime]:
    """Pick recent snapshot_ts that exist in option_chain but may not yet have features."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT oc.snapshot_ts
            FROM spx.option_chain oc
            LEFT JOIN spx.chain_features_0dte f
              ON f.snapshot_ts = oc.snapshot_ts
            WHERE f.snapshot_ts IS NULL
            ORDER BY oc.snapshot_ts ASC
            LIMIT %s
            """,
            (int(limit),),
        )
        return [r[0] for r in cur.fetchall()]


def _candidate_label_snapshot_ts(conn: psycopg.Connection, *, limit: int) -> list[datetime]:
    """Pick feature snapshots that are missing at least one label horizon."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT f.snapshot_ts
            FROM spx.chain_features_0dte f
            LEFT JOIN spx.chain_labels_0dte l
              ON l.snapshot_ts = f.snapshot_ts
            GROUP BY f.snapshot_ts
            HAVING COUNT(l.horizon_minutes) = 0
            ORDER BY f.snapshot_ts ASC
            LIMIT %s
            """,
            (int(limit),),
        )
        return [r[0] for r in cur.fetchall()]


def run_phase2_daemon(cfg: Phase2Config) -> None:
    tz_local = cfg.tz_local

    with psycopg.connect(cfg.db_dsn) as conn:
        ensure_phase2_schema(conn)

    while True:
        did = False
        try:
            with psycopg.connect(cfg.db_dsn) as conn:
                # Features backfill
                for ts in _candidate_snapshot_ts(conn, limit=cfg.batch_limit):
                    compute_features_for_snapshot(
                        conn,
                        snapshot_ts=ts,
                        tz_local=tz_local,
                        min_contracts=cfg.min_contracts,
                    )
                    did = True

                # Labels backfill (only for feature snapshots with no labels yet)
                for ts in _candidate_label_snapshot_ts(conn, limit=cfg.batch_limit):
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
        except Exception:
            # Keep daemon alive; rely on journalctl for stack traces.
            # (We intentionally avoid logging dependencies here.)
            pass

        time.sleep(cfg.poll_seconds if not did else 0.1)


def load_phase2_config_from_env() -> Phase2Config:
    # Default to the chain DB DSN if present, else DATABASE_URL.
    db = os.getenv("PHASE2_DB_DSN", "").strip() or os.getenv("SPX_CHAIN_DATABASE_URL", "").strip() or os.getenv("DATABASE_URL", "").strip()
    if not db:
        raise RuntimeError("PHASE2_DB_DSN (or SPX_CHAIN_DATABASE_URL) is required")

    tz_local = os.getenv("TZ_LOCAL", TZ_LOCAL_DEFAULT).strip() or TZ_LOCAL_DEFAULT

    horizons_s = os.getenv("HORIZONS_MINUTES", "15,30,45,60,90").strip()
    horizons = tuple(int(x) for x in horizons_s.split(",") if x.strip())

    return Phase2Config(
        db_dsn=db,
        tz_local=tz_local,
        horizons_minutes=horizons,
        max_future_lookahead_minutes=int(os.getenv("MAX_FUTURE_LOOKAHEAD_MINUTES", "120")),
        label_eps_atm_iv=float(os.getenv("LABEL_EPS_ATM_IV", "0.0025")),
        label_eps_skew_25d=float(os.getenv("LABEL_EPS_SKEW_25D", "0.0025")),
        min_contracts=int(os.getenv("MIN_CONTRACTS", "50")),
        poll_seconds=float(os.getenv("PHASE2_POLL_SECONDS", "15")),
        batch_limit=int(os.getenv("PHASE2_BATCH_LIMIT", "200")),
    )
