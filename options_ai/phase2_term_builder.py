from __future__ import annotations

import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Iterable

from zoneinfo import ZoneInfo

import psycopg

from options_ai.term_expiration import pick_expiration_for_target_dte, term_bucket_name


TZ_LOCAL_DEFAULT = "America/Chicago"


@dataclass(frozen=True)
class Phase2TermConfig:
    db_dsn: str
    tz_local: str = TZ_LOCAL_DEFAULT

    target_dte_days: int = 7
    dte_tolerance_days: int = 2
    term_bucket: str = ""

    horizons_minutes: tuple[int, ...] = (5760, 10080)  # default: half/full for dte7
    max_future_lookahead_minutes: int = 20160

    label_eps_atm_iv: float = 0.0025
    label_eps_skew_25d: float = 0.0025

    min_contracts: int = 50

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

  contract_count INT,
  valid_iv_count INT,
  valid_mid_count INT,
  low_quality BOOLEAN NOT NULL DEFAULT FALSE,
  computed_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  PRIMARY KEY (snapshot_ts, expiration_date)
);

CREATE INDEX IF NOT EXISTS chain_features_term_bucket_ts_idx ON spx.chain_features_term (term_bucket, snapshot_ts DESC);
CREATE INDEX IF NOT EXISTS chain_features_term_exp_ts_idx ON spx.chain_features_term (expiration_date, snapshot_ts DESC);

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


UPSERT_FEATURE_SQL = """
INSERT INTO spx.chain_features_term (
  snapshot_ts, expiration_date,
  term_bucket, target_dte_days, dte_tolerance_days, dte_days, dte_diff,
  spot, ttm_minutes,
  atm_strike, atm_iv, atm_call_iv, atm_put_iv, atm_bidask_spread,
  iv_put_25d, iv_call_25d, skew_25d, bf_25d,
  put_volume, call_volume, pcr_volume,
  put_oi, call_oi, pcr_oi,
  contract_count, valid_iv_count, valid_mid_count,
  low_quality
) VALUES (
  %(snapshot_ts)s, %(expiration_date)s,
  %(term_bucket)s, %(target_dte_days)s, %(dte_tolerance_days)s, %(dte_days)s, %(dte_diff)s,
  %(spot)s, %(ttm_minutes)s,
  %(atm_strike)s, %(atm_iv)s, %(atm_call_iv)s, %(atm_put_iv)s, %(atm_bidask_spread)s,
  %(iv_put_25d)s, %(iv_call_25d)s, %(skew_25d)s, %(bf_25d)s,
  %(put_volume)s, %(call_volume)s, %(pcr_volume)s,
  %(put_oi)s, %(call_oi)s, %(pcr_oi)s,
  %(contract_count)s, %(valid_iv_count)s, %(valid_mid_count)s,
  %(low_quality)s
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
  contract_count = EXCLUDED.contract_count,
  valid_iv_count = EXCLUDED.valid_iv_count,
  valid_mid_count = EXCLUDED.valid_mid_count,
  low_quality = EXCLUDED.low_quality,
  computed_at = now();
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


def ensure_schema(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)
        conn.commit()


def _fetch_chain_rows(cur: psycopg.Cursor, snapshot_ts: datetime, expiration_date) -> list[dict[str, Any]]:
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


def _pick_atm_strike(chain: list[dict[str, Any]], spot: float | None) -> float | None:
    strikes = sorted({c["strike"] for c in chain if c.get("strike") is not None})
    if spot is None or not strikes:
        return None
    best = None
    atm = None
    for k in strikes:
        d = abs(float(k) - float(spot))
        cand = (d, float(k))
        if best is None or cand < best:
            best = cand
            atm = float(k)
    return atm


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

        atm_bidask_spread = None
        if atm_strike is not None:
            spreads: list[float] = []
            for c in chain:
                if c.get("strike") is None or float(c["strike"]) != float(atm_strike):
                    continue
                b, a = c.get("bid"), c.get("ask")
                if b is None or a is None or a < b:
                    continue
                spreads.append(float(a - b))
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

        put_volume = sum(int(c["volume"]) for c in chain if c.get("side") == "put" and c.get("volume") is not None)
        call_volume = sum(int(c["volume"]) for c in chain if c.get("side") == "call" and c.get("volume") is not None)
        put_oi = sum(int(c["open_interest"]) for c in chain if c.get("side") == "put" and c.get("open_interest") is not None)
        call_oi = sum(int(c["open_interest"]) for c in chain if c.get("side") == "call" and c.get("open_interest") is not None)

        pcr_volume = float(put_volume) / float(call_volume) if call_volume else None
        pcr_oi = float(put_oi) / float(call_oi) if call_oi else None

        low_quality = False
        if contract_count < int(cfg.min_contracts):
            low_quality = True
        if atm_iv is None:
            low_quality = True

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
            "contract_count": contract_count,
            "valid_iv_count": valid_iv_count,
            "valid_mid_count": valid_mid_count,
            "low_quality": bool(low_quality),
        }

        cur.execute(UPSERT_FEATURE_SQL, feat)
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
    return {
        "snapshot_ts": r[0],
        "atm_iv": _as_float(r[1]),
        "skew_25d": _as_float(r[2]),
        "low_quality": bool(r[3]),
    }


def compute_labels_for_feature_row(
    conn: psycopg.Connection,
    *,
    snapshot_ts: datetime,
    expiration_date,
    cfg: Phase2TermConfig,
) -> int:
    horizons = [int(h) for h in cfg.horizons_minutes]
    if not horizons:
        return 0

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT atm_iv, skew_25d, low_quality
            FROM spx.chain_features_term
            WHERE snapshot_ts = %s
              AND expiration_date = %s
              AND term_bucket = %s
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
            is_missing_future = fut is None

            atm_iv_tH = fut["atm_iv"] if fut else None
            skew_tH = fut["skew_25d"] if fut else None
            lowq_tH = bool(fut["low_quality"]) if fut else False

            atm_iv_change = None
            if atm_iv_t is not None and atm_iv_tH is not None:
                atm_iv_change = float(atm_iv_tH - atm_iv_t)

            skew_change = None
            if skew_25d_t is not None and skew_tH is not None:
                skew_change = float(skew_tH - skew_25d_t)

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
                    "is_missing_future": bool(is_missing_future),
                    "is_low_quality": bool(lowq_t or lowq_tH),
                },
            )
            upserted += 1

        conn.commit()
        return upserted


def _candidate_snapshot_ts(conn: psycopg.Connection, *, cfg: Phase2TermConfig, limit: int) -> list[datetime]:
    """Pick snapshot_ts values to process.

    For term horizons measured in days, the most recent snapshots won't have enough
    future data to form labels yet. To bootstrap training, we mix:

    - recent snapshots (for near-live features)
    - snapshots old enough that (ts + max_horizon) is likely within the available data

    We still compute missing-future labels when they occur; ML training filters those out.
    """

    n = max(1, int(limit))
    n_recent = max(1, n // 2)
    n_hist = max(1, n - n_recent)
    max_h = max((int(x) for x in cfg.horizons_minutes), default=0)

    with conn.cursor() as cur:
        cur.execute("SELECT max(snapshot_ts) FROM spx.option_chain")
        r = cur.fetchone()
        max_ts = r[0] if r else None

        # 1) very recent
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
                # Features backfill: sample recent snapshot_ts, pick term expiration per snapshot
                for ts in _candidate_snapshot_ts(conn, cfg=cfg, limit=cfg.batch_limit):
                    r = compute_features_for_snapshot(conn, snapshot_ts=ts, cfg=cfg)
                    if r:
                        did = True

                # Labels backfill: for feature rows in this bucket missing any labels
                for ts, exp in _feature_rows_missing_labels(conn, term_bucket=cfg.term_bucket, limit=cfg.batch_limit):
                    n = compute_labels_for_feature_row(conn, snapshot_ts=ts, expiration_date=exp, cfg=cfg)
                    if n:
                        did = True

        except Exception:
            pass

        time.sleep(cfg.poll_seconds if not did else 0.1)


def load_config_from_env() -> Phase2TermConfig:
    db = (
        os.getenv("PHASE2_DB_DSN", "").strip()
        or os.getenv("SPX_CHAIN_DATABASE_URL", "").strip()
        or os.getenv("DATABASE_URL", "").strip()
    )
    if not db:
        raise RuntimeError("PHASE2_DB_DSN (or SPX_CHAIN_DATABASE_URL) is required")

    tz_local = os.getenv("TZ_LOCAL", TZ_LOCAL_DEFAULT).strip() or TZ_LOCAL_DEFAULT

    target = int(os.getenv("TARGET_DTE_DAYS", "7"))
    tol = int(os.getenv("DTE_TOLERANCE_DAYS", "2"))
    bucket = os.getenv("TERM_BUCKET", "").strip() or term_bucket_name(target_dte_days=target, dte_tolerance_days=tol)

    horizons_s = os.getenv("HORIZONS_MINUTES", "5760,10080").strip()
    horizons = tuple(int(x) for x in horizons_s.split(",") if x.strip())

    return Phase2TermConfig(
        db_dsn=db,
        tz_local=tz_local,
        target_dte_days=target,
        dte_tolerance_days=tol,
        term_bucket=bucket,
        horizons_minutes=horizons,
        max_future_lookahead_minutes=int(os.getenv("MAX_FUTURE_LOOKAHEAD_MINUTES", "20160")),
        label_eps_atm_iv=float(os.getenv("LABEL_EPS_ATM_IV", "0.0025")),
        label_eps_skew_25d=float(os.getenv("LABEL_EPS_SKEW_25D", "0.0025")),
        min_contracts=int(os.getenv("MIN_CONTRACTS", "50")),
        poll_seconds=float(os.getenv("PHASE2_POLL_SECONDS", "60")),
        batch_limit=int(os.getenv("PHASE2_BATCH_LIMIT", "200")),
    )
