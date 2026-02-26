from __future__ import annotations

import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

from zoneinfo import ZoneInfo

import psycopg


@dataclass(frozen=True)
class DebitSpreadConfig:
    db_dsn: str
    tz_local: str = "America/Chicago"

    horizons_minutes: tuple[int, ...] = (30,)  # default 30m
    max_future_lookahead_minutes: int = 120

    max_debit_points: float = 5.0

    poll_seconds: float = 20.0
    batch_limit: int = 200


ANCHOR_TYPES = ("ATM", "CALL_WALL", "PUT_WALL", "MAGNET")
SPREAD_TYPES = ("CALL", "PUT")


SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS spx;

CREATE TABLE IF NOT EXISTS spx.gex_levels_0dte (
  snapshot_ts TIMESTAMPTZ PRIMARY KEY,
  expiration_date DATE NOT NULL,
  call_wall NUMERIC,
  put_wall NUMERIC,
  magnet NUMERIC,
  computed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS spx.debit_spread_candidates_0dte (
  snapshot_ts TIMESTAMPTZ NOT NULL,
  anchor_type TEXT NOT NULL,
  spread_type TEXT NOT NULL,

  expiration_date DATE NOT NULL,
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
  PRIMARY KEY (snapshot_ts, anchor_type, spread_type)
);

CREATE INDEX IF NOT EXISTS debit_spread_candidates_ts_idx ON spx.debit_spread_candidates_0dte (snapshot_ts DESC);

CREATE TABLE IF NOT EXISTS spx.debit_spread_labels_0dte (
  snapshot_ts TIMESTAMPTZ NOT NULL,
  horizon_minutes INT NOT NULL,
  anchor_type TEXT NOT NULL,
  spread_type TEXT NOT NULL,

  debit_t NUMERIC,
  debit_tH NUMERIC,
  change NUMERIC,

  is_missing_future BOOLEAN NOT NULL DEFAULT FALSE,
  computed_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  PRIMARY KEY (snapshot_ts, horizon_minutes, anchor_type, spread_type)
);

CREATE INDEX IF NOT EXISTS debit_spread_labels_ts_idx ON spx.debit_spread_labels_0dte (snapshot_ts DESC);
"""


UPSERT_GEX_LEVELS_SQL = """
INSERT INTO spx.gex_levels_0dte (snapshot_ts, expiration_date, call_wall, put_wall, magnet)
VALUES (%(snapshot_ts)s, %(expiration_date)s, %(call_wall)s, %(put_wall)s, %(magnet)s)
ON CONFLICT (snapshot_ts) DO UPDATE SET
  expiration_date = EXCLUDED.expiration_date,
  call_wall = EXCLUDED.call_wall,
  put_wall = EXCLUDED.put_wall,
  magnet = EXCLUDED.magnet,
  computed_at = now();
"""


UPSERT_CAND_SQL = """
INSERT INTO spx.debit_spread_candidates_0dte (
  snapshot_ts, anchor_type, spread_type,
  expiration_date, spot,
  anchor_strike, k_long, k_short, long_symbol, short_symbol,
  debit_points, tradable, reject_reason
) VALUES (
  %(snapshot_ts)s, %(anchor_type)s, %(spread_type)s,
  %(expiration_date)s, %(spot)s,
  %(anchor_strike)s, %(k_long)s, %(k_short)s, %(long_symbol)s, %(short_symbol)s,
  %(debit_points)s, %(tradable)s, %(reject_reason)s
)
ON CONFLICT (snapshot_ts, anchor_type, spread_type) DO UPDATE SET
  expiration_date = EXCLUDED.expiration_date,
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
INSERT INTO spx.debit_spread_labels_0dte (
  snapshot_ts, horizon_minutes, anchor_type, spread_type,
  debit_t, debit_tH, change, is_missing_future
) VALUES (
  %(snapshot_ts)s, %(horizon_minutes)s, %(anchor_type)s, %(spread_type)s,
  %(debit_t)s, %(debit_tH)s, %(change)s, %(is_missing_future)s
)
ON CONFLICT (snapshot_ts, horizon_minutes, anchor_type, spread_type) DO UPDATE SET
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


def _local_trade_date(snapshot_ts: datetime, tz_local: str) -> datetime.date:
    tz = ZoneInfo(tz_local)
    return snapshot_ts.astimezone(tz).date()


def _spot_from_chain(chain: list[dict[str, Any]]) -> float | None:
    vals = [c.get("underlying_price") for c in chain if c.get("underlying_price") is not None]
    return _median([float(v) for v in vals if v is not None])


def _mid_from_row(r: dict[str, Any]) -> float | None:
    m = _as_float(r.get("mid"))
    if m is not None:
        return m
    b = _as_float(r.get("bid"))
    a = _as_float(r.get("ask"))
    if b is not None and a is not None and a >= b:
        return 0.5 * (b + a)
    return None


def _fetch_chain_rows(conn: psycopg.Connection, snapshot_ts: datetime, expiration_date: datetime.date) -> list[dict[str, Any]]:
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
    """Compute CALL_WALL / PUT_WALL / MAGNET from gamma*OI (0DTE chain rows)."""

    # Aggregate by strike
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

    # Deterministic tiebreaker: choose lower strike on equal value
    def _argmax(items: Iterable[tuple[float, float]]) -> float:
        best = None
        best_k = None
        for k, v in items:
            cand = (float(v), -float(k))  # maximize v, then minimize k
            if best is None or cand > best:
                best = cand
                best_k = k
        assert best_k is not None
        return float(best_k)

    call_wall = _argmax(((k, v["call_gex"]) for k, v in by_strike.items()))
    put_wall = _argmax(((k, v["put_gex"]) for k, v in by_strike.items()))
    magnet = _argmax(((k, abs(v["call_gex"]) + abs(v["put_gex"])) for k, v in by_strike.items()))

    return {"call_wall": call_wall, "put_wall": put_wall, "magnet": magnet}


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
    # spread_type: CALL or PUT
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


def compute_candidates_for_snapshot(
    conn: psycopg.Connection,
    *,
    snapshot_ts: datetime,
    tz_local: str,
    max_debit_points: float,
) -> int:
    exp_date = _local_trade_date(snapshot_ts, tz_local)

    chain = _fetch_chain_rows(conn, snapshot_ts, exp_date)
    if not chain:
        return 0

    strikes = sorted({float(r["strike"]) for r in chain if r.get("strike") is not None})
    if len(strikes) < 3:
        return 0

    spot = _spot_from_chain(chain)

    # ATM from chain_features_0dte when available (else nearest strike to spot)
    atm_strike = None
    with conn.cursor() as cur:
        cur.execute("SELECT atm_strike, spot, expiration_date FROM spx.chain_features_0dte WHERE snapshot_ts = %s", (snapshot_ts,))
        r = cur.fetchone()
        if r and r[0] is not None:
            atm_strike = float(r[0])
            if spot is None and r[1] is not None:
                spot = float(r[1])
        # Ensure 0DTE expiration is consistent with features if present
        if r and r[2] is not None:
            exp_date = r[2]

    if atm_strike is None and spot is not None:
        atm_strike = _nearest_strike(strikes, float(spot))

    gex = compute_gex_levels(chain)

    # Upsert gex levels for UI
    with conn.cursor() as cur:
        cur.execute(
            UPSERT_GEX_LEVELS_SQL,
            {
                "snapshot_ts": snapshot_ts,
                "expiration_date": exp_date,
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
        anchors["CALL_WALL"] = float(gex["call_wall"])  # already a strike
    if gex.get("put_wall") is not None:
        anchors["PUT_WALL"] = float(gex["put_wall"])
    if gex.get("magnet") is not None:
        anchors["MAGNET"] = float(gex["magnet"])

    # Resolve anchors to actual available strikes + de-dup
    resolved: dict[str, float] = {}
    used: set[float] = set()
    for a, k in anchors.items():
        kk = _nearest_strike(strikes, float(k))
        if kk is None:
            continue
        if kk in used:
            continue
        used.add(kk)
        resolved[a] = kk

    upserted = 0

    for anchor_type, k_anchor in resolved.items():
        k_up = _next_up(strikes, k_anchor)
        k_dn = _next_dn(strikes, k_anchor)

        # CALL debit: long call @ anchor, short call @ up
        cand_call = {
            "snapshot_ts": snapshot_ts,
            "anchor_type": anchor_type,
            "spread_type": "CALL",
            "expiration_date": exp_date,
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
            elif debit > float(max_debit_points):
                cand_call["reject_reason"] = "debit_gt_max"
            else:
                cand_call["tradable"] = True

        # PUT debit: long put @ anchor, short put @ down
        cand_put = {
            "snapshot_ts": snapshot_ts,
            "anchor_type": anchor_type,
            "spread_type": "PUT",
            "expiration_date": exp_date,
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
            elif debit > float(max_debit_points):
                cand_put["reject_reason"] = "debit_gt_max"
            else:
                cand_put["tradable"] = True

        with conn.cursor() as cur:
            cur.execute(UPSERT_CAND_SQL, cand_call)
            cur.execute(UPSERT_CAND_SQL, cand_put)
            conn.commit()
            upserted += 2

    return upserted


def _debit_at_ts(conn: psycopg.Connection, *, snapshot_ts: datetime, expiration_date: datetime.date, long_symbol: str, short_symbol: str) -> float | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT option_symbol, bid, ask, mid
            FROM spx.option_chain
            WHERE snapshot_ts = %s AND expiration_date = %s AND option_symbol = ANY(%s)
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


def compute_labels_for_snapshot(conn: psycopg.Connection, *, snapshot_ts: datetime, cfg: DebitSpreadConfig) -> int:
    # Need candidates at t
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT anchor_type, spread_type, expiration_date, long_symbol, short_symbol, debit_points
            FROM spx.debit_spread_candidates_0dte
            WHERE snapshot_ts = %s
            """,
            (snapshot_ts,),
        )
        cands = cur.fetchall()

    if not cands:
        return 0

    upserted = 0

    for h in cfg.horizons_minutes:
        target = snapshot_ts + timedelta(minutes=int(h))
        max_ts = target + timedelta(minutes=int(cfg.max_future_lookahead_minutes))

        # Find tH = earliest 0DTE feature snapshot at/after target within lookahead (event-time aligned).
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT snapshot_ts
                FROM spx.chain_features_0dte
                WHERE snapshot_ts >= %s AND snapshot_ts <= %s
                ORDER BY snapshot_ts ASC
                LIMIT 1
                """,
                (target, max_ts),
            )
            r = cur.fetchone()
            tH = r[0] if r else None

        for anchor_type, spread_type, exp_date, long_sym, short_sym, debit_t in cands:
            debit_t_val = _as_float(debit_t)
            debit_tH = None
            missing = tH is None
            if (not missing) and long_sym and short_sym:
                debit_tH = _debit_at_ts(conn, snapshot_ts=tH, expiration_date=exp_date, long_symbol=str(long_sym), short_symbol=str(short_sym))
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
                        "horizon_minutes": int(h),
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


def _candidate_snapshots_missing_candidates(conn: psycopg.Connection, *, limit: int, tz_local: str) -> list[datetime]:
    # Use chain_features_0dte as the driver of 0DTE snapshots.
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT f.snapshot_ts
            FROM spx.chain_features_0dte f
            LEFT JOIN spx.debit_spread_candidates_0dte c
              ON c.snapshot_ts = f.snapshot_ts
            WHERE c.snapshot_ts IS NULL
            ORDER BY f.snapshot_ts ASC
            LIMIT %s
            """,
            (int(limit),),
        )
        return [r[0] for r in cur.fetchall()]


def _candidate_snapshots_missing_labels(conn: psycopg.Connection, *, limit: int) -> list[datetime]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT c.snapshot_ts
            FROM spx.debit_spread_candidates_0dte c
            LEFT JOIN spx.debit_spread_labels_0dte l
              ON l.snapshot_ts = c.snapshot_ts
            GROUP BY c.snapshot_ts
            HAVING COUNT(l.horizon_minutes) = 0
            ORDER BY c.snapshot_ts ASC
            LIMIT %s
            """,
            (int(limit),),
        )
        return [r[0] for r in cur.fetchall()]


def run_daemon(cfg: DebitSpreadConfig) -> None:
    with psycopg.connect(cfg.db_dsn) as conn:
        ensure_schema(conn)

    while True:
        did = False
        try:
            with psycopg.connect(cfg.db_dsn) as conn:
                # Candidate generation (also computes gex levels)
                for ts in _candidate_snapshots_missing_candidates(conn, limit=cfg.batch_limit, tz_local=cfg.tz_local):
                    n = compute_candidates_for_snapshot(
                        conn,
                        snapshot_ts=ts,
                        tz_local=cfg.tz_local,
                        max_debit_points=cfg.max_debit_points,
                    )
                    if n:
                        did = True

                # Label generation
                for ts in _candidate_snapshots_missing_labels(conn, limit=cfg.batch_limit):
                    n = compute_labels_for_snapshot(conn, snapshot_ts=ts, cfg=cfg)
                    if n:
                        did = True

        except Exception:
            # Keep daemon alive; journalctl will show stack traces when run under systemd.
            pass

        time.sleep(cfg.poll_seconds if not did else 0.1)


def load_config_from_env() -> DebitSpreadConfig:
    db = os.getenv("DEBIT_DB_DSN", "").strip() or os.getenv("SPX_CHAIN_DATABASE_URL", "").strip()
    if not db:
        raise RuntimeError("DEBIT_DB_DSN (or SPX_CHAIN_DATABASE_URL) is required")

    tz_local = os.getenv("TZ_LOCAL", "America/Chicago").strip() or "America/Chicago"

    horizons_s = os.getenv("DEBIT_HORIZONS_MINUTES", "30").strip() or "30"
    horizons = tuple(int(x) for x in horizons_s.split(",") if x.strip())

    return DebitSpreadConfig(
        db_dsn=db,
        tz_local=tz_local,
        horizons_minutes=horizons,
        max_future_lookahead_minutes=int(os.getenv("MAX_FUTURE_LOOKAHEAD_MINUTES", "120")),
        max_debit_points=float(os.getenv("MAX_DEBIT_POINTS", "5.0")),
        poll_seconds=float(os.getenv("DEBIT_POLL_SECONDS", "20")),
        batch_limit=int(os.getenv("DEBIT_BATCH_LIMIT", "200")),
    )
