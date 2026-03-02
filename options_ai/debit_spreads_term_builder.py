from __future__ import annotations

import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import psycopg

from options_ai.term_expiration import pick_expiration_for_target_dte, term_bucket_name


@dataclass(frozen=True)
class DebitSpreadTermConfig:
    db_dsn: str
    tz_local: str = "America/Chicago"

    target_dte_days: int = 7
    dte_tolerance_days: int = 2
    term_bucket: str = ""

    horizons_minutes: tuple[int, ...] = (5760, 10080)
    max_future_lookahead_minutes: int = 20160

    max_debit_points: float = 5.0

    poll_seconds: float = 20.0
    batch_limit: int = 200


ANCHOR_TYPES = ("ATM", "CALL_WALL", "PUT_WALL", "MAGNET")
SPREAD_TYPES = ("CALL", "PUT")


SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS spx;

CREATE TABLE IF NOT EXISTS spx.gex_levels_term (
  snapshot_ts TIMESTAMPTZ NOT NULL,
  expiration_date DATE NOT NULL,

  term_bucket TEXT NOT NULL,
  target_dte_days INT NOT NULL,
  dte_tolerance_days INT NOT NULL,

  call_wall NUMERIC,
  put_wall NUMERIC,
  magnet NUMERIC,
  computed_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  PRIMARY KEY (snapshot_ts, expiration_date)
);

CREATE INDEX IF NOT EXISTS gex_levels_term_bucket_ts_idx ON spx.gex_levels_term (term_bucket, snapshot_ts DESC);

CREATE TABLE IF NOT EXISTS spx.debit_spread_candidates_term (
  snapshot_ts TIMESTAMPTZ NOT NULL,
  expiration_date DATE NOT NULL,

  term_bucket TEXT NOT NULL,
  target_dte_days INT NOT NULL,
  dte_tolerance_days INT NOT NULL,

  anchor_type TEXT NOT NULL,
  spread_type TEXT NOT NULL,

  spot NUMERIC,

  anchor_strike NUMERIC,
  k_long NUMERIC,
  k_short NUMERIC,
  long_symbol TEXT,
  short_symbol TEXT,

  debit_points NUMERIC,
  tradable BOOLEAN NOT NULL,
  reject_reason TEXT,

  computed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (snapshot_ts, expiration_date, anchor_type, spread_type)
);

CREATE INDEX IF NOT EXISTS debit_spread_candidates_term_bucket_ts_idx ON spx.debit_spread_candidates_term (term_bucket, snapshot_ts DESC);

CREATE TABLE IF NOT EXISTS spx.debit_spread_labels_term (
  snapshot_ts TIMESTAMPTZ NOT NULL,
  expiration_date DATE NOT NULL,
  horizon_minutes INT NOT NULL,

  term_bucket TEXT NOT NULL,

  anchor_type TEXT NOT NULL,
  spread_type TEXT NOT NULL,

  debit_t NUMERIC,
  debit_tH NUMERIC,
  change NUMERIC,

  is_missing_future BOOLEAN NOT NULL DEFAULT FALSE,
  computed_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  PRIMARY KEY (snapshot_ts, expiration_date, horizon_minutes, anchor_type, spread_type)
);

CREATE INDEX IF NOT EXISTS debit_spread_labels_term_bucket_ts_idx ON spx.debit_spread_labels_term (term_bucket, snapshot_ts DESC);
"""


UPSERT_GEX_SQL = """
INSERT INTO spx.gex_levels_term (
  snapshot_ts, expiration_date,
  term_bucket, target_dte_days, dte_tolerance_days,
  call_wall, put_wall, magnet
) VALUES (
  %(snapshot_ts)s, %(expiration_date)s,
  %(term_bucket)s, %(target_dte_days)s, %(dte_tolerance_days)s,
  %(call_wall)s, %(put_wall)s, %(magnet)s
)
ON CONFLICT (snapshot_ts, expiration_date) DO UPDATE SET
  term_bucket = EXCLUDED.term_bucket,
  target_dte_days = EXCLUDED.target_dte_days,
  dte_tolerance_days = EXCLUDED.dte_tolerance_days,
  call_wall = EXCLUDED.call_wall,
  put_wall = EXCLUDED.put_wall,
  magnet = EXCLUDED.magnet,
  computed_at = now();
"""


UPSERT_CAND_SQL = """
INSERT INTO spx.debit_spread_candidates_term (
  snapshot_ts, expiration_date,
  term_bucket, target_dte_days, dte_tolerance_days,
  anchor_type, spread_type,
  spot,
  anchor_strike, k_long, k_short, long_symbol, short_symbol,
  debit_points, tradable, reject_reason
) VALUES (
  %(snapshot_ts)s, %(expiration_date)s,
  %(term_bucket)s, %(target_dte_days)s, %(dte_tolerance_days)s,
  %(anchor_type)s, %(spread_type)s,
  %(spot)s,
  %(anchor_strike)s, %(k_long)s, %(k_short)s, %(long_symbol)s, %(short_symbol)s,
  %(debit_points)s, %(tradable)s, %(reject_reason)s
)
ON CONFLICT (snapshot_ts, expiration_date, anchor_type, spread_type) DO UPDATE SET
  term_bucket = EXCLUDED.term_bucket,
  target_dte_days = EXCLUDED.target_dte_days,
  dte_tolerance_days = EXCLUDED.dte_tolerance_days,
  spot = EXCLUDED.spot,
  anchor_strike = EXCLUDED.anchor_strike,
  k_long = EXCLUDED.k_long,
  k_short = EXCLUDED.k_short,
  long_symbol = EXCLUDED.long_symbol,
  short_symbol = EXCLUDED.short_symbol,
  debit_points = EXCLUDED.debit_points,
  tradable = EXCLUDED.tradable,
  reject_reason = EXCLUDED.reject_reason,
  computed_at = now();
"""


UPSERT_LABEL_SQL = """
INSERT INTO spx.debit_spread_labels_term (
  snapshot_ts, expiration_date, horizon_minutes,
  term_bucket,
  anchor_type, spread_type,
  debit_t, debit_tH, change, is_missing_future
) VALUES (
  %(snapshot_ts)s, %(expiration_date)s, %(horizon_minutes)s,
  %(term_bucket)s,
  %(anchor_type)s, %(spread_type)s,
  %(debit_t)s, %(debit_tH)s, %(change)s, %(is_missing_future)s
)
ON CONFLICT (snapshot_ts, expiration_date, horizon_minutes, anchor_type, spread_type) DO UPDATE SET
  term_bucket = EXCLUDED.term_bucket,
  debit_t = EXCLUDED.debit_t,
  debit_tH = EXCLUDED.debit_tH,
  change = EXCLUDED.change,
  is_missing_future = EXCLUDED.is_missing_future,
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


def _median(vals: list[float]) -> float | None:
    if not vals:
        return None
    s = sorted(vals)
    n = len(s)
    mid = n // 2
    if n % 2 == 1:
        return float(s[mid])
    return 0.5 * (float(s[mid - 1]) + float(s[mid]))


def _mid_from_row(r: dict[str, Any]) -> float | None:
    m = _as_float(r.get("mid"))
    if m is not None:
        return m
    b = _as_float(r.get("bid"))
    a = _as_float(r.get("ask"))
    if b is not None and a is not None and a >= b:
        return 0.5 * (b + a)
    return None


def _fetch_chain_rows(conn: psycopg.Connection, snapshot_ts: datetime, expiration_date) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT option_symbol, side, strike,
                   bid, ask, mid,
                   iv, delta, gamma,
                   open_interest, volume,
                   underlying_price
            FROM spx.option_chain
            WHERE snapshot_ts = %s AND expiration_date = %s
            """,
            (snapshot_ts, expiration_date),
        )
        out: list[dict[str, Any]] = []
        for r in cur.fetchall():
            out.append(
                {
                    "option_symbol": r[0],
                    "side": r[1],
                    "strike": _as_float(r[2]),
                    "bid": _as_float(r[3]),
                    "ask": _as_float(r[4]),
                    "mid": _as_float(r[5]),
                    "iv": _as_float(r[6]),
                    "delta": _as_float(r[7]),
                    "gamma": _as_float(r[8]),
                    "open_interest": 0 if r[9] is None else int(r[9]),
                    "volume": None if r[10] is None else int(r[10]),
                    "underlying_price": _as_float(r[11]),
                }
            )
        return out


def compute_gex_levels(chain: list[dict[str, Any]]) -> dict[str, float | None]:
    by_strike: dict[float, dict[str, float]] = {}
    for r in chain:
        k = r.get("strike")
        if k is None:
            continue
        side = str(r.get("side") or "").lower()
        gamma = _as_float(r.get("gamma")) or 0.0
        oi = float(r.get("open_interest") or 0)

        d = by_strike.setdefault(float(k), {"call_gex": 0.0, "put_gex": 0.0})
        if side == "call":
            d["call_gex"] += gamma * oi
        elif side == "put":
            d["put_gex"] += gamma * oi

    if not by_strike:
        return {"call_wall": None, "put_wall": None, "magnet": None}

    def _argmax(items):
        best = None
        best_k = None
        for k, v in items:
            cand = (float(v), -float(k))
            if best is None or cand > best:
                best = cand
                best_k = k
        return float(best_k) if best_k is not None else None

    call_wall = _argmax(((k, v["call_gex"]) for k, v in by_strike.items()))
    put_wall = _argmax(((k, v["put_gex"]) for k, v in by_strike.items()))
    magnet = _argmax(((k, abs(v["call_gex"]) + abs(v["put_gex"])) for k, v in by_strike.items()))
    return {"call_wall": call_wall, "put_wall": put_wall, "magnet": magnet}


def _spot_from_chain(chain: list[dict[str, Any]]) -> float | None:
    vals = [c.get("underlying_price") for c in chain if c.get("underlying_price") is not None]
    return _median([float(v) for v in vals if v is not None])


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
            best_k = k
    return float(best_k) if best_k is not None else None


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


def _pick_contract(chain: list[dict[str, Any]], *, side: str, strike: float) -> dict[str, Any] | None:
    cands = []
    for r in chain:
        if str(r.get("side") or "").lower() != side:
            continue
        if r.get("strike") is None or float(r["strike"]) != float(strike):
            continue
        m = _mid_from_row(r)
        score = 0 if m is not None else 1
        cands.append((score, r.get("option_symbol") or "", m, r))
    if not cands:
        return None
    cands.sort()
    return cands[0][3]


def _debit_points(chain: list[dict[str, Any]], *, spread_type: str, k_long: float, k_short: float) -> tuple[float | None, str | None, str | None]:
    if spread_type == "CALL":
        long = _pick_contract(chain, side="call", strike=k_long)
        short = _pick_contract(chain, side="call", strike=k_short)
    else:
        long = _pick_contract(chain, side="put", strike=k_long)
        short = _pick_contract(chain, side="put", strike=k_short)

    if not long or not short:
        return None, (long or {}).get("option_symbol"), (short or {}).get("option_symbol")

    m_long = _mid_from_row(long)
    m_short = _mid_from_row(short)
    if m_long is None or m_short is None:
        return None, long.get("option_symbol"), short.get("option_symbol")

    return float(m_long - m_short), long.get("option_symbol"), short.get("option_symbol")


def _fetch_atm_spot_from_features(conn: psycopg.Connection, *, snapshot_ts: datetime, expiration_date, term_bucket: str) -> tuple[float | None, float | None]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT atm_strike, spot
            FROM spx.chain_features_term
            WHERE snapshot_ts = %s AND expiration_date = %s AND term_bucket = %s
            """,
            (snapshot_ts, expiration_date, term_bucket),
        )
        r = cur.fetchone()
        if not r:
            return None, None
        return _as_float(r[0]), _as_float(r[1])


def compute_candidates_for_snapshot(conn: psycopg.Connection, *, snapshot_ts: datetime, cfg: DebitSpreadTermConfig) -> int:
    picked = pick_expiration_for_target_dte(
        conn,
        snapshot_ts=snapshot_ts,
        target_dte_days=int(cfg.target_dte_days),
        dte_tolerance_days=int(cfg.dte_tolerance_days),
        tz_local=str(cfg.tz_local),
    )
    if not picked:
        return 0

    exp_date = picked.expiration_date

    chain = _fetch_chain_rows(conn, snapshot_ts, exp_date)
    if not chain:
        return 0

    strikes = sorted({float(r["strike"]) for r in chain if r.get("strike") is not None})
    if len(strikes) < 3:
        return 0

    atm_strike, spot = _fetch_atm_spot_from_features(conn, snapshot_ts=snapshot_ts, expiration_date=exp_date, term_bucket=cfg.term_bucket)
    if spot is None:
        spot = _spot_from_chain(chain)
    if atm_strike is None and spot is not None:
        atm_strike = _nearest_strike(strikes, float(spot))

    gex = compute_gex_levels(chain)

    # Upsert gex levels
    with conn.cursor() as cur:
        cur.execute(
            UPSERT_GEX_SQL,
            {
                "snapshot_ts": snapshot_ts,
                "expiration_date": exp_date,
                "term_bucket": cfg.term_bucket,
                "target_dte_days": int(cfg.target_dte_days),
                "dte_tolerance_days": int(cfg.dte_tolerance_days),
                "call_wall": gex["call_wall"],
                "put_wall": gex["put_wall"],
                "magnet": gex["magnet"],
            },
        )
        conn.commit()

    anchors: dict[str, float] = {}
    if atm_strike is not None:
        anchors["ATM"] = float(atm_strike)
    if gex.get("call_wall") is not None:
        anchors["CALL_WALL"] = float(gex["call_wall"])
    if gex.get("put_wall") is not None:
        anchors["PUT_WALL"] = float(gex["put_wall"])
    if gex.get("magnet") is not None:
        anchors["MAGNET"] = float(gex["magnet"])

    # Resolve anchors to actual strikes
    resolved: dict[str, float] = {}
    used: set[float] = set()
    for a, k in anchors.items():
        kk = _nearest_strike(strikes, float(k))
        if kk is None or kk in used:
            continue
        used.add(float(kk))
        resolved[a] = float(kk)

    upserted = 0

    for anchor_type, k_anchor in resolved.items():
        k_up = _next_up(strikes, k_anchor)
        k_dn = _next_dn(strikes, k_anchor)

        cand_call = {
            "snapshot_ts": snapshot_ts,
            "expiration_date": exp_date,
            "term_bucket": cfg.term_bucket,
            "target_dte_days": int(cfg.target_dte_days),
            "dte_tolerance_days": int(cfg.dte_tolerance_days),
            "anchor_type": anchor_type,
            "spread_type": "CALL",
            "spot": spot,
            "anchor_strike": k_anchor,
            "k_long": k_anchor,
            "k_short": k_up,
            "long_symbol": None,
            "short_symbol": None,
            "debit_points": None,
            "tradable": False,
            "reject_reason": None,
        }
        if k_up is None:
            cand_call["reject_reason"] = "no_adjacent_strike_up"
        else:
            debit, sym_long, sym_short = _debit_points(chain, spread_type="CALL", k_long=k_anchor, k_short=k_up)
            cand_call["debit_points"] = debit
            cand_call["long_symbol"] = sym_long
            cand_call["short_symbol"] = sym_short
            if debit is None:
                cand_call["reject_reason"] = "missing_quote"
            elif debit <= 0:
                cand_call["reject_reason"] = "debit_nonpositive"
            elif debit > float(cfg.max_debit_points):
                cand_call["reject_reason"] = "debit_gt_max"
            else:
                cand_call["tradable"] = True

        cand_put = {
            "snapshot_ts": snapshot_ts,
            "expiration_date": exp_date,
            "term_bucket": cfg.term_bucket,
            "target_dte_days": int(cfg.target_dte_days),
            "dte_tolerance_days": int(cfg.dte_tolerance_days),
            "anchor_type": anchor_type,
            "spread_type": "PUT",
            "spot": spot,
            "anchor_strike": k_anchor,
            "k_long": k_anchor,
            "k_short": k_dn,
            "long_symbol": None,
            "short_symbol": None,
            "debit_points": None,
            "tradable": False,
            "reject_reason": None,
        }
        if k_dn is None:
            cand_put["reject_reason"] = "no_adjacent_strike_dn"
        else:
            debit, sym_long, sym_short = _debit_points(chain, spread_type="PUT", k_long=k_anchor, k_short=k_dn)
            cand_put["debit_points"] = debit
            cand_put["long_symbol"] = sym_long
            cand_put["short_symbol"] = sym_short
            if debit is None:
                cand_put["reject_reason"] = "missing_quote"
            elif debit <= 0:
                cand_put["reject_reason"] = "debit_nonpositive"
            elif debit > float(cfg.max_debit_points):
                cand_put["reject_reason"] = "debit_gt_max"
            else:
                cand_put["tradable"] = True

        with conn.cursor() as cur:
            cur.execute(UPSERT_CAND_SQL, cand_call)
            cur.execute(UPSERT_CAND_SQL, cand_put)
            conn.commit()
            upserted += 2

    return upserted


def _debit_at_ts(conn: psycopg.Connection, *, snapshot_ts: datetime, expiration_date, long_symbol: str, short_symbol: str) -> float | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT option_symbol, bid, ask, mid
            FROM spx.option_chain
            WHERE snapshot_ts = %s
              AND expiration_date = %s
              AND option_symbol = ANY(%s)
            """,
            (snapshot_ts, expiration_date, [long_symbol, short_symbol]),
        )
        got: dict[str, dict[str, Any]] = {}
        for r in cur.fetchall():
            got[str(r[0])] = {"bid": r[1], "ask": r[2], "mid": r[3]}

    def _mid(x: dict[str, Any] | None) -> float | None:
        if not x:
            return None
        m = _as_float(x.get("mid"))
        if m is not None:
            return m
        b = _as_float(x.get("bid"))
        a = _as_float(x.get("ask"))
        if b is not None and a is not None and a >= b:
            return 0.5 * (b + a)
        return None

    m_long = _mid(got.get(long_symbol))
    m_short = _mid(got.get(short_symbol))
    if m_long is None or m_short is None:
        return None
    return float(m_long - m_short)


def _pick_future_snapshot_ts(conn: psycopg.Connection, *, expiration_date, ts_target: datetime, ts_max: datetime) -> datetime | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT snapshot_ts
            FROM spx.option_chain
            WHERE expiration_date = %s
              AND snapshot_ts >= %s
              AND snapshot_ts <= %s
            ORDER BY snapshot_ts ASC
            LIMIT 1
            """,
            (expiration_date, ts_target, ts_max),
        )
        r = cur.fetchone()
        return r[0] if r else None


def compute_labels_for_snapshot(conn: psycopg.Connection, *, snapshot_ts: datetime, expiration_date, cfg: DebitSpreadTermConfig) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT anchor_type, spread_type, long_symbol, short_symbol, debit_points
            FROM spx.debit_spread_candidates_term
            WHERE snapshot_ts = %s
              AND expiration_date = %s
              AND term_bucket = %s
            """,
            (snapshot_ts, expiration_date, cfg.term_bucket),
        )
        cands = cur.fetchall()

    if not cands:
        return 0

    upserted = 0

    for h in cfg.horizons_minutes:
        target = snapshot_ts + timedelta(minutes=int(h))
        max_ts = target + timedelta(minutes=int(cfg.max_future_lookahead_minutes))
        tH = _pick_future_snapshot_ts(conn, expiration_date=expiration_date, ts_target=target, ts_max=max_ts)

        for anchor_type, spread_type, long_sym, short_sym, debit_t in cands:
            debit_t_val = _as_float(debit_t)
            debit_tH = None
            missing = tH is None
            if (not missing) and long_sym and short_sym:
                debit_tH = _debit_at_ts(
                    conn,
                    snapshot_ts=tH,
                    expiration_date=expiration_date,
                    long_symbol=str(long_sym),
                    short_symbol=str(short_sym),
                )
                if debit_tH is None:
                    missing = True

            change = None
            if debit_t_val is not None and debit_tH is not None:
                change = float(debit_tH - debit_t_val)

            with conn.cursor() as cur:
                cur.execute(
                    UPSERT_LABEL_SQL,
                    {
                        "snapshot_ts": snapshot_ts,
                        "expiration_date": expiration_date,
                        "horizon_minutes": int(h),
                        "term_bucket": cfg.term_bucket,
                        "anchor_type": str(anchor_type),
                        "spread_type": str(spread_type),
                        "debit_t": debit_t_val,
                        "debit_tH": debit_tH,
                        "change": change,
                        "is_missing_future": bool(missing),
                    },
                )
                conn.commit()
                upserted += 1

    return upserted


def _feature_rows_missing_candidates(conn: psycopg.Connection, *, term_bucket: str, limit: int) -> list[tuple[datetime, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT f.snapshot_ts, f.expiration_date
            FROM spx.chain_features_term f
            WHERE f.term_bucket = %s
              AND NOT EXISTS (
                SELECT 1
                FROM spx.debit_spread_candidates_term c
                WHERE c.snapshot_ts = f.snapshot_ts
                  AND c.expiration_date = f.expiration_date
              )
            ORDER BY f.snapshot_ts ASC
            LIMIT %s
            """,
            (term_bucket, int(limit)),
        )
        return [(r[0], r[1]) for r in cur.fetchall()]


def _feature_rows_missing_labels(conn: psycopg.Connection, *, term_bucket: str, limit: int) -> list[tuple[datetime, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT c.snapshot_ts, c.expiration_date
            FROM spx.debit_spread_candidates_term c
            WHERE c.term_bucket = %s
              AND NOT EXISTS (
                SELECT 1
                FROM spx.debit_spread_labels_term l
                WHERE l.snapshot_ts = c.snapshot_ts
                  AND l.expiration_date = c.expiration_date
              )
            GROUP BY c.snapshot_ts, c.expiration_date
            ORDER BY c.snapshot_ts ASC
            LIMIT %s
            """,
            (term_bucket, int(limit)),
        )
        return [(r[0], r[1]) for r in cur.fetchall()]


def run_daemon(cfg: DebitSpreadTermConfig) -> None:
    with psycopg.connect(cfg.db_dsn) as conn:
        ensure_schema(conn)

    while True:
        did = False
        try:
            with psycopg.connect(cfg.db_dsn) as conn:
                for ts, exp in _feature_rows_missing_candidates(conn, term_bucket=cfg.term_bucket, limit=cfg.batch_limit):
                    n = compute_candidates_for_snapshot(conn, snapshot_ts=ts, cfg=cfg)
                    if n:
                        did = True

                for ts, exp in _feature_rows_missing_labels(conn, term_bucket=cfg.term_bucket, limit=cfg.batch_limit):
                    n = compute_labels_for_snapshot(conn, snapshot_ts=ts, expiration_date=exp, cfg=cfg)
                    if n:
                        did = True

        except Exception:
            pass

        time.sleep(cfg.poll_seconds if not did else 2.0)


def load_config_from_env() -> DebitSpreadTermConfig:
    db = os.getenv("DEBIT_DB_DSN", "").strip() or os.getenv("SPX_CHAIN_DATABASE_URL", "").strip()
    if not db:
        raise RuntimeError("DEBIT_DB_DSN (or SPX_CHAIN_DATABASE_URL) is required")

    tz_local = os.getenv("TZ_LOCAL", "America/Chicago").strip() or "America/Chicago"

    target = int(os.getenv("TARGET_DTE_DAYS", "7"))
    tol = int(os.getenv("DTE_TOLERANCE_DAYS", "2"))
    bucket = os.getenv("TERM_BUCKET", "").strip() or term_bucket_name(target_dte_days=target, dte_tolerance_days=tol)

    horizons_s = os.getenv("DEBIT_HORIZONS_MINUTES", "5760,10080").strip() or "5760,10080"
    horizons = tuple(int(x) for x in horizons_s.split(",") if x.strip())

    return DebitSpreadTermConfig(
        db_dsn=db,
        tz_local=tz_local,
        target_dte_days=target,
        dte_tolerance_days=tol,
        term_bucket=bucket,
        horizons_minutes=horizons,
        max_future_lookahead_minutes=int(os.getenv("MAX_FUTURE_LOOKAHEAD_MINUTES", "20160")),
        max_debit_points=float(os.getenv("MAX_DEBIT_POINTS", "5.0")),
        poll_seconds=float(os.getenv("DEBIT_POLL_SECONDS", "20")),
        batch_limit=int(os.getenv("DEBIT_BATCH_LIMIT", "200")),
    )
