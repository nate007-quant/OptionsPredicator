#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _ok(msg: str) -> None:
    print(f"[OK]   {msg}")


def _warn(msg: str) -> None:
    print(f"[WARN] {msg}")


def _fail(msg: str) -> None:
    raise RuntimeError(msg)


def main() -> int:
    ap = argparse.ArgumentParser(description="Validate flow_phase3_v1 schema + data sanity")
    ap.add_argument("--dsn", default=os.getenv("SPX_CHAIN_DATABASE_URL", ""), help="Postgres DSN")
    ap.add_argument("--align", default=os.getenv("OUTCOME_ALIGN", "FirstOfDay"), help="Outcome align mode")
    ap.add_argument("--sample", type=int, default=1000, help="Sample rows for sanity checks")
    args = ap.parse_args()

    if not args.dsn:
        _fail("Missing --dsn (or SPX_CHAIN_DATABASE_URL)")

    import psycopg

    required_cols_0dte = {
        "itm_vol", "atm_vol", "otm_vol", "tot_vol", "d_tot_vol", "d_call_oi", "d_put_oi",
        "sma_spot_5", "sma_spot_20", "bb_mid_20", "bb_upper_20", "bb_lower_20", "bb_pctb_20", "rsi_14",
        "twap_spot_day", "vwap_chainweighted_spot_day", "rsi_osob_label", "bb_osob_label", "pcr_oi_osob_label",
        "flow_total_strikes", "flow_pct_bullish", "flow_pct_bearish", "flow_breadth", "flow_bucket_net_flow",
        "flow_bucket_robust_z", "flow_skew", "flow_confidence", "flow_atm_corridor_net", "flow_atm_corridor_frac",
        "flow_top3_share", "flow_top5_share", "flow_bias_summary", "flow_breadth_pass", "flow_bucketz_pass", "flow_live_ok_default",
    }

    with psycopg.connect(args.dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema='spx' AND table_name='chain_features_0dte'
            """)
            got_0dte = {r[0] for r in cur.fetchall()}
            missing = sorted(required_cols_0dte - got_0dte)
            if missing:
                _fail(f"Missing chain_features_0dte columns: {missing}")
            _ok("chain_features_0dte has required flow_phase3_v1 columns")

            cur.execute("""
                SELECT to_regclass('spx.chain_outcomes_underlying'), to_regclass('spx.chain_outcomes_atm_options')
            """)
            r = cur.fetchone()
            if not r or r[0] is None or r[1] is None:
                _fail("Outcome tables not found (chain_outcomes_underlying / chain_outcomes_atm_options)")
            _ok("Outcome tables exist")

            # range sanity
            cur.execute(
                """
                SELECT
                  SUM(CASE WHEN flow_pct_bullish < 0 OR flow_pct_bullish > 1 THEN 1 ELSE 0 END),
                  SUM(CASE WHEN flow_pct_bearish < 0 OR flow_pct_bearish > 1 THEN 1 ELSE 0 END),
                  SUM(CASE WHEN flow_breadth < 0 OR flow_breadth > 1 THEN 1 ELSE 0 END),
                  SUM(CASE WHEN flow_top3_share < 0 OR flow_top3_share > 1 THEN 1 ELSE 0 END),
                  SUM(CASE WHEN flow_top5_share < 0 OR flow_top5_share > 1 THEN 1 ELSE 0 END)
                FROM (
                  SELECT flow_pct_bullish, flow_pct_bearish, flow_breadth, flow_top3_share, flow_top5_share
                  FROM spx.chain_features_0dte
                  ORDER BY snapshot_ts DESC
                  LIMIT %s
                ) s
                """,
                (int(args.sample),),
            )
            bad = cur.fetchone()
            bad_n = sum(int(x or 0) for x in bad)
            if bad_n > 0:
                _warn(f"Flow range sanity found {bad_n} out-of-range values in sample={args.sample}")
            else:
                _ok("Flow range sanity checks passed")

            # no-leak proxy: outcome eval ts should be >= snapshot ts
            cur.execute(
                """
                SELECT COUNT(*)
                FROM spx.chain_outcomes_underlying
                WHERE align_mode=%s
                  AND eval_snapshot_ts IS NOT NULL
                  AND eval_snapshot_ts < snapshot_ts
                """,
                (args.align,),
            )
            leak_u = int(cur.fetchone()[0] or 0)
            if leak_u != 0:
                _fail(f"No-leak violation in chain_outcomes_underlying: {leak_u} rows with eval_snapshot_ts < snapshot_ts")
            _ok("No-leak check passed for chain_outcomes_underlying")

            cur.execute(
                """
                SELECT COUNT(*)
                FROM spx.chain_outcomes_atm_options
                WHERE align_mode=%s
                  AND eval_snapshot_ts IS NOT NULL
                  AND eval_snapshot_ts < snapshot_ts
                """,
                (args.align,),
            )
            leak_a = int(cur.fetchone()[0] or 0)
            if leak_a != 0:
                _fail(f"No-leak violation in chain_outcomes_atm_options: {leak_a} rows with eval_snapshot_ts < snapshot_ts")
            _ok("No-leak check passed for chain_outcomes_atm_options")

            cur.execute("SELECT COUNT(*) FROM spx.chain_outcomes_underlying WHERE align_mode=%s", (args.align,))
            n_u = int(cur.fetchone()[0] or 0)
            cur.execute("SELECT COUNT(*) FROM spx.chain_outcomes_atm_options WHERE align_mode=%s", (args.align,))
            n_a = int(cur.fetchone()[0] or 0)
            print(f"[INFO] outcomes_underlying rows ({args.align}): {n_u}")
            print(f"[INFO] outcomes_atm_options rows ({args.align}): {n_a}")

    print("PASS validate_flow_phase3_v1")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
