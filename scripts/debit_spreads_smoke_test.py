#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Ensure repo root on sys.path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _require(cond: bool, msg: str) -> None:
    if not cond:
        raise RuntimeError(msg)


def main() -> int:
    ap = argparse.ArgumentParser(description="Smoke test: multi-anchor debit spread candidates + labels.")
    ap.add_argument(
        "--dsn",
        default=os.getenv("SPX_CHAIN_DATABASE_URL", ""),
        help="Postgres/Timescale DSN (default: env SPX_CHAIN_DATABASE_URL)",
    )
    ap.add_argument("--cleanup", action="store_true", help="Delete synthetic rows after test")
    args = ap.parse_args()

    _require(bool(args.dsn), "Missing --dsn (or env SPX_CHAIN_DATABASE_URL)")

    import psycopg
    from zoneinfo import ZoneInfo

    from options_ai.phase2_builder import ensure_phase2_schema, compute_features_for_snapshot
    from options_ai.debit_spreads_builder import (
        DebitSpreadConfig,
        ensure_schema,
        compute_candidates_for_snapshot,
        compute_labels_for_snapshot,
    )

    tz = ZoneInfo("America/Chicago")
    t_local = datetime(2031, 1, 3, 10, 0, 0, tzinfo=tz)
    t = t_local.astimezone(timezone.utc)
    tH = (t_local + timedelta(minutes=30)).astimezone(timezone.utc)
    exp_date = t_local.date()

    # Build a chain with strikes 5700..5900 step 10.
    strikes = [5700 + 10 * i for i in range(21)]

    def rows(ts: datetime, *, debit_shift: float) -> list[dict]:
        out = []
        spot = 5805.0

        # Force call wall at 5860 via huge call oi
        call_wall_k = 5860
        # Force put wall at 5750 via huge put oi
        put_wall_k = 5750
        # Force magnet at 5810 via both sides big
        magnet_k = 5810

        for i, k in enumerate(strikes):
            # call
            oi_call = 10
            if k == call_wall_k:
                oi_call = 5000
            if k == magnet_k:
                oi_call = 3000

            # put
            oi_put = 10
            if k == put_wall_k:
                oi_put = 6000
            if k == magnet_k:
                oi_put = 3000

            # mid pricing: make adjacent call spread debit increase by +debit_shift at tH
            # Simple monotonic synthetic pricing so adjacent debit spreads have positive debit
            mid_call = 3.0 - 0.002 * float(k - 5700)  # calls cheaper at higher strikes
            mid_put = 2.0 + 0.002 * float(k - 5700)   # puts more expensive at higher strikes

            if ts == tH:
                # shift only some strikes so spreads change (not both legs equally)
                if k <= 5810:
                    mid_call += debit_shift
                if k >= 5810:
                    mid_put += debit_shift

            out.append(
                {
                    "snapshot_ts": ts,
                    "option_symbol": f"TSTC{k}_{i}",
                    "underlying": "SPX",
                    "expiration_date": exp_date,
                    "side": "call",
                    "strike": float(k),
                    "first_traded_ts": None,
                    "updated_ts": None,
                    "dte": 0,
                    "bid": mid_call - 0.05,
                    "bid_size": 1,
                    "mid": mid_call,
                    "ask": mid_call + 0.05,
                    "ask_size": 1,
                    "last": None,
                    "open_interest": oi_call,
                    "volume": 1,
                    "in_the_money": None,
                    "intrinsic_value": None,
                    "extrinsic_value": None,
                    "underlying_price": spot,
                    "iv": 0.20,
                    "delta": 0.50,
                    "gamma": 0.01,
                    "theta": None,
                    "vega": None,
                }
            )
            out.append(
                {
                    "snapshot_ts": ts,
                    "option_symbol": f"TSTP{k}_{i}",
                    "underlying": "SPX",
                    "expiration_date": exp_date,
                    "side": "put",
                    "strike": float(k),
                    "first_traded_ts": None,
                    "updated_ts": None,
                    "dte": 0,
                    "bid": mid_put - 0.05,
                    "bid_size": 1,
                    "mid": mid_put,
                    "ask": mid_put + 0.05,
                    "ask_size": 1,
                    "last": None,
                    "open_interest": oi_put,
                    "volume": 1,
                    "in_the_money": None,
                    "intrinsic_value": None,
                    "extrinsic_value": None,
                    "underlying_price": spot,
                    "iv": 0.20,
                    "delta": -0.50,
                    "gamma": 0.01,
                    "theta": None,
                    "vega": None,
                }
            )

        return out

    rows_t = rows(t, debit_shift=0.0)
    rows_tH = rows(tH, debit_shift=0.10)

    with psycopg.connect(args.dsn) as conn:
        ensure_phase2_schema(conn)
        ensure_schema(conn)

        with conn.cursor() as cur:
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
                  mid=EXCLUDED.mid, bid=EXCLUDED.bid, ask=EXCLUDED.ask,
                  open_interest=EXCLUDED.open_interest, gamma=EXCLUDED.gamma,
                  underlying_price=EXCLUDED.underlying_price;
                """,
                rows_t + rows_tH,
            )
            conn.commit()

        # Phase 2 features required for ATM anchor
        f1 = compute_features_for_snapshot(conn, snapshot_ts=t, tz_local="America/Chicago", min_contracts=10)
        f2 = compute_features_for_snapshot(conn, snapshot_ts=tH, tz_local="America/Chicago", min_contracts=10)
        _require(f1 is not None and f2 is not None, "Expected phase2 features")

        n_cand = compute_candidates_for_snapshot(conn, snapshot_ts=t, tz_local="America/Chicago", max_debit_points=5.0)
        _require(n_cand > 0, "Expected some candidates")

        cfg = DebitSpreadConfig(db_dsn=args.dsn, horizons_minutes=(30,))
        n_lab = compute_labels_for_snapshot(conn, snapshot_ts=t, cfg=cfg)
        _require(n_lab > 0, "Expected labels")

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT count(*)
                FROM spx.debit_spread_candidates_0dte
                WHERE snapshot_ts = %s
                """,
                (t,),
            )
            cand_count = int(cur.fetchone()[0])
            _require(cand_count >= 2, f"Expected >=2 candidates, got {cand_count}")

            # Ensure gex levels were computed
            cur.execute("SELECT call_wall, put_wall, magnet FROM spx.gex_levels_0dte WHERE snapshot_ts=%s", (t,))
            lev = cur.fetchone()
            _require(lev is not None, "Missing gex levels")

            # At least one label should have positive change due to our debit_shift at tH.
            cur.execute(
                """
                SELECT max(change)
                FROM spx.debit_spread_labels_0dte
                WHERE snapshot_ts=%s AND horizon_minutes=30 AND is_missing_future=false
                """,
                (t,),
            )
            mx = cur.fetchone()[0]
            _require(mx is not None and float(mx) > 0, f"Expected some positive label change, got {mx}")

        if args.cleanup:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM spx.debit_spread_labels_0dte WHERE snapshot_ts IN (%s,%s)", (t, tH))
                cur.execute("DELETE FROM spx.debit_spread_candidates_0dte WHERE snapshot_ts IN (%s,%s)", (t, tH))
                cur.execute("DELETE FROM spx.gex_levels_0dte WHERE snapshot_ts IN (%s,%s)", (t, tH))
                cur.execute("DELETE FROM spx.chain_labels_0dte WHERE snapshot_ts IN (%s,%s)", (t, tH))
                cur.execute("DELETE FROM spx.chain_features_0dte WHERE snapshot_ts IN (%s,%s)", (t, tH))
                cur.execute("DELETE FROM spx.option_chain WHERE snapshot_ts IN (%s,%s) AND option_symbol LIKE %s", (t, tH, 'TST%'))
                conn.commit()

    print("PASS debit_spreads_smoke_test")
    print(f"  snapshot_ts(t): {t.isoformat()}")
    print(f"  snapshot_ts(t+30): {tH.isoformat()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
