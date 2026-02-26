#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import sys

# Ensure repo root is on sys.path when running as a script
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _require(cond: bool, msg: str) -> None:
    if not cond:
        raise RuntimeError(msg)


def main() -> int:
    ap = argparse.ArgumentParser(description="Phase 2 smoke test: compute 0DTE features+labels from synthetic option_chain rows.")
    ap.add_argument(
        "--dsn",
        default=os.getenv("SPX_CHAIN_DATABASE_URL", ""),
        help="Postgres/Timescale DSN (default: env SPX_CHAIN_DATABASE_URL)",
    )
    ap.add_argument(
        "--tz-local",
        default=os.getenv("TZ_LOCAL", "America/Chicago"),
        help="Local TZ for 0DTE selection (default: America/Chicago)",
    )
    ap.add_argument(
        "--min-contracts",
        type=int,
        default=int(os.getenv("MIN_CONTRACTS", "50")),
        help="MIN_CONTRACTS threshold for low_quality (default: env MIN_CONTRACTS or 50)",
    )
    ap.add_argument(
        "--cleanup",
        action="store_true",
        help="Delete the synthetic rows after test (recommended)",
    )
    args = ap.parse_args()

    _require(bool(args.dsn), "Missing --dsn (or env SPX_CHAIN_DATABASE_URL)")

    import psycopg
    from zoneinfo import ZoneInfo

    from options_ai.phase2_builder import (
        compute_features_for_snapshot,
        compute_labels_for_snapshot,
        ensure_phase2_schema,
    )

    tz = ZoneInfo(args.tz_local)

    # Choose a deterministic Chicago-local date and time, and compute UTC timestamps.
    # 2030-01-02 10:00:00 America/Chicago -> winter CST (-06).
    t_local = datetime(2030, 1, 2, 10, 0, 0, tzinfo=tz)
    t = t_local.astimezone(timezone.utc)
    tH = (t_local + timedelta(minutes=15)).astimezone(timezone.utc)

    exp_date = t_local.date()  # 0DTE selection rule

    # Generate >= min_contracts contracts (calls+puts) with deterministic deltas.
    # We insert 25 strikes * 2 sides = 50 rows.
    strikes = [5700 + 10 * i for i in range(25)]

    def rows_for_ts(ts: datetime, *, atm_iv: float, skew: float) -> list[dict]:
        # skew_25d = iv_put_25d - iv_call_25d; we create 25d points by setting delta near +/-0.25
        # For simplicity:
        # - calls with delta ~ +0.25: iv_call_25d = atm_iv - skew/2
        # - puts  with delta ~ -0.25: iv_put_25d = atm_iv + skew/2
        iv_call_25d = atm_iv - skew / 2.0
        iv_put_25d = atm_iv + skew / 2.0

        out = []
        spot = 5805.0

        for i, k in enumerate(strikes):
            # call
            out.append(
                {
                    "snapshot_ts": ts,
                    "option_symbol": f"TEST20300102C{k}_{i}",
                    "underlying": "SPX",
                    "expiration_date": exp_date,
                    "side": "call",
                    "strike": float(k),
                    "first_traded_ts": None,
                    "updated_ts": None,
                    "dte": 0,
                    "bid": 1.0,
                    "bid_size": 1,
                    "mid": 1.1,
                    "ask": 1.2,
                    "ask_size": 1,
                    "last": None,
                    "open_interest": 10,
                    "volume": 1,
                    "in_the_money": None,
                    "intrinsic_value": None,
                    "extrinsic_value": None,
                    "underlying_price": spot,
                    "iv": float(iv_call_25d if k == 5900 else atm_iv),
                    "delta": float(0.25 if k == 5900 else 0.50),
                    "gamma": 0.01,
                    "theta": None,
                    "vega": None,
                }
            )
            # put
            out.append(
                {
                    "snapshot_ts": ts,
                    "option_symbol": f"TEST20300102P{k}_{i}",
                    "underlying": "SPX",
                    "expiration_date": exp_date,
                    "side": "put",
                    "strike": float(k),
                    "first_traded_ts": None,
                    "updated_ts": None,
                    "dte": 0,
                    "bid": 1.0,
                    "bid_size": 1,
                    "mid": 1.1,
                    "ask": 1.2,
                    "ask_size": 1,
                    "last": None,
                    "open_interest": 10,
                    "volume": 1,
                    "in_the_money": None,
                    "intrinsic_value": None,
                    "extrinsic_value": None,
                    "underlying_price": spot,
                    "iv": float(iv_put_25d if k == 5700 else atm_iv),
                    "delta": float(-0.25 if k == 5700 else -0.50),
                    "gamma": 0.01,
                    "theta": None,
                    "vega": None,
                }
            )

        return out

    # Design the outcome: ATM IV increases from t to t+15 => label_atm_iv_dir = +1
    # Skew decreases from t to t+15 => label_skew_25d_dir = -1
    rows_t = rows_for_ts(t, atm_iv=0.20, skew=0.04)
    rows_tH = rows_for_ts(tH, atm_iv=0.205, skew=0.035)

    with psycopg.connect(args.dsn) as conn:
        # ensure schema exists
        ensure_phase2_schema(conn)

        with conn.cursor() as cur:
            # Insert synthetic chain rows
            cur.executemany(
                """
                INSERT INTO spx.option_chain (
                  snapshot_ts, option_symbol, underlying, expiration_date, side, strike,
                  first_traded_ts, updated_ts, dte,
                  bid, bid_size, mid, ask, ask_size, last,
                  open_interest, volume,
                  in_the_money, intrinsic_value, extrinsic_value,
                  underlying_price, iv, delta, gamma, theta, vega
                ) VALUES (
                  %(snapshot_ts)s, %(option_symbol)s, %(underlying)s, %(expiration_date)s, %(side)s, %(strike)s,
                  %(first_traded_ts)s, %(updated_ts)s, %(dte)s,
                  %(bid)s, %(bid_size)s, %(mid)s, %(ask)s, %(ask_size)s, %(last)s,
                  %(open_interest)s, %(volume)s,
                  %(in_the_money)s, %(intrinsic_value)s, %(extrinsic_value)s,
                  %(underlying_price)s, %(iv)s, %(delta)s, %(gamma)s, %(theta)s, %(vega)s
                )
                ON CONFLICT (snapshot_ts, option_symbol) DO UPDATE SET
                  iv = EXCLUDED.iv,
                  delta = EXCLUDED.delta,
                  mid = EXCLUDED.mid,
                  bid = EXCLUDED.bid,
                  ask = EXCLUDED.ask,
                  underlying_price = EXCLUDED.underlying_price;
                """,
                rows_t + rows_tH,
            )
            conn.commit()

        # Compute features
        f1 = compute_features_for_snapshot(conn, snapshot_ts=t, tz_local=args.tz_local, min_contracts=args.min_contracts)
        f2 = compute_features_for_snapshot(conn, snapshot_ts=tH, tz_local=args.tz_local, min_contracts=args.min_contracts)
        _require(f1 is not None and f2 is not None, "Expected features to be computed (0DTE rows should exist)")

        # Compute labels for H=15 only
        n = compute_labels_for_snapshot(
            conn,
            snapshot_ts=t,
            horizons_minutes=[15],
            max_future_lookahead_minutes=120,
            label_eps_atm_iv=0.0025,
            label_eps_skew_25d=0.0025,
        )
        _require(n == 1, f"Expected 1 label upsert, got {n}")

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT label_atm_iv_dir, label_skew_25d_dir, is_missing_future, is_low_quality
                FROM spx.chain_labels_0dte
                WHERE snapshot_ts = %s AND horizon_minutes = 15
                """,
                (t,),
            )
            r = cur.fetchone()
            _require(r is not None, "Missing label row")

            label_atm, label_skew, miss, lowq = int(r[0]), int(r[1]), bool(r[2]), bool(r[3])
            _require(miss is False, "Expected is_missing_future=false")
            _require(lowq is False, "Expected is_low_quality=false")
            _require(label_atm == 1, f"Expected ATM IV dir label +1, got {label_atm}")
            _require(label_skew == -1, f"Expected skew dir label -1, got {label_skew}")

        if args.cleanup:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM spx.chain_labels_0dte WHERE snapshot_ts IN (%s,%s)", (t, tH))
                cur.execute("DELETE FROM spx.chain_features_0dte WHERE snapshot_ts IN (%s,%s)", (t, tH))
                cur.execute("DELETE FROM spx.option_chain WHERE snapshot_ts IN (%s,%s) AND option_symbol LIKE %s", (t, tH, 'TEST20300102%'))
                conn.commit()

    print("PASS phase2_smoke_test")
    print(f"  snapshot_ts(t): {t.isoformat()}")
    print(f"  snapshot_ts(t+15): {tH.isoformat()}")
    print(f"  expiration_date(0DTE): {exp_date.isoformat()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
