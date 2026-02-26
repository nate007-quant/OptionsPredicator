#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _require(cond: bool, msg: str) -> None:
    if not cond:
        raise RuntimeError(msg)


def main() -> int:
    ap = argparse.ArgumentParser(description="Smoke test: train debit spread ML model and score latest snapshot.")
    ap.add_argument(
        "--dsn",
        default=os.getenv("SPX_CHAIN_DATABASE_URL", ""),
        help="Postgres/Timescale DSN (default: env SPX_CHAIN_DATABASE_URL)",
    )
    ap.add_argument("--cleanup", action="store_true", help="Delete synthetic rows after test")
    args = ap.parse_args()

    _require(bool(args.dsn), "Missing --dsn")

    import psycopg
    from zoneinfo import ZoneInfo

    from options_ai.phase2_builder import ensure_phase2_schema, compute_features_for_snapshot
    from options_ai.debit_spreads_builder import ensure_schema as ensure_debit_schema, compute_candidates_for_snapshot
    from options_ai.debit_spread_ml import ensure_schema as ensure_ml_schema, DebitMLConfig, train_if_needed, score_latest_snapshot

    tz = ZoneInfo("America/Chicago")

    # Create many labeled samples (>= min_train_rows) by creating a sequence of snapshots.
    # We'll create 160 timestamps at 5-minute spacing; labels for H=30 will exist for the earlier ones.
    start_local = datetime(2032, 1, 4, 9, 30, 0, tzinfo=tz)
    exp_date = start_local.date()

    strikes = [5700 + 10 * i for i in range(21)]

    def chain_rows(ts: datetime, *, shift: float) -> list[dict]:
        out = []
        spot = 5805.0
        for i, k in enumerate(strikes):
            mid_call = 3.0 - 0.002 * float(k - 5700)
            mid_put = 2.0 + 0.002 * float(k - 5700)
            # time-varying to induce label changes
            mid_call += shift
            mid_put += shift

            out.append({
                "snapshot_ts": ts,
                "option_symbol": f"MLC{k}_{i}_{int(ts.timestamp())}",
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
                "open_interest": 100,
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
            })
            out.append({
                "snapshot_ts": ts,
                "option_symbol": f"MLP{k}_{i}_{int(ts.timestamp())}",
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
                "open_interest": 100,
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
            })
        return out

    # Build a monotonic shift so later snapshots have higher debit.
    snapshots_local = [start_local + timedelta(minutes=5 * i) for i in range(160)]
    snapshots = [dt.astimezone(timezone.utc) for dt in snapshots_local]

    with psycopg.connect(args.dsn) as conn:
        ensure_phase2_schema(conn)
        ensure_debit_schema(conn)
        ensure_ml_schema(conn)

        with conn.cursor() as cur:
            rows = []
            for j, ts in enumerate(snapshots):
                rows.extend(chain_rows(ts, shift=0.001 * j))
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
                rows,
            )
            conn.commit()

        # Phase 2 features and debit candidates for each snapshot
        for ts in snapshots:
            _ = compute_features_for_snapshot(conn, snapshot_ts=ts, tz_local="America/Chicago", min_contracts=10)
            _ = compute_candidates_for_snapshot(conn, snapshot_ts=ts, tz_local="America/Chicago", max_debit_points=5.0)
        conn.commit()

        # Generate debit labels for all snapshots (requires futures); we can do it by calling the builder's labels daemon indirectly
        from options_ai.debit_spreads_builder import DebitSpreadConfig, compute_labels_for_snapshot
        cfg = DebitSpreadConfig(db_dsn=args.dsn, horizons_minutes=(30,))
        for ts in snapshots:
            _ = compute_labels_for_snapshot(conn, snapshot_ts=ts, cfg=cfg)
        conn.commit()

        mlcfg = DebitMLConfig(db_dsn=args.dsn, horizon_minutes=30, min_train_rows=50, max_train_rows=50000, retrain_seconds=0, models_dir="/tmp/debit_ml_test_models", bigwin_mult_atm=1.1, bigwin_mult_wall=1.1)
        tm = train_if_needed(conn, mlcfg, force=True)
        _require(tm is not None, "Expected model to train")

        n = score_latest_snapshot(conn, mlcfg, tm)
        _require(n > 0, "Expected scores for latest snapshot")

        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM spx.debit_spread_scores_0dte WHERE horizon_minutes=30")
            ct = int(cur.fetchone()[0])
            _require(ct > 0, "Expected score rows")
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM spx.debit_spread_scores_0dte WHERE horizon_minutes=30 AND p_bigwin IS NOT NULL")
            pb = int(cur.fetchone()[0])
            _require(pb > 0, f"Expected some p_bigwin values, got {pb}")


        if args.cleanup:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM spx.debit_spread_scores_0dte WHERE snapshot_ts >= %s", (snapshots[0],))
                cur.execute("DELETE FROM spx.debit_spread_labels_0dte WHERE snapshot_ts >= %s", (snapshots[0],))
                cur.execute("DELETE FROM spx.debit_spread_candidates_0dte WHERE snapshot_ts >= %s", (snapshots[0],))
                cur.execute("DELETE FROM spx.gex_levels_0dte WHERE snapshot_ts >= %s", (snapshots[0],))
                cur.execute("DELETE FROM spx.chain_labels_0dte WHERE snapshot_ts >= %s", (snapshots[0],))
                cur.execute("DELETE FROM spx.chain_features_0dte WHERE snapshot_ts >= %s", (snapshots[0],))
                cur.execute("DELETE FROM spx.option_chain WHERE snapshot_ts >= %s AND option_symbol LIKE %s", (snapshots[0], 'ML%'))
                conn.commit()

    print("PASS debit_spreads_ml_smoke_test")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
