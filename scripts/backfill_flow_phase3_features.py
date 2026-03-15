#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _dsn(cli: str) -> str:
    return cli.strip() or os.getenv("SPX_CHAIN_DATABASE_URL", "").strip() or os.getenv("PHASE2_DB_DSN", "").strip()


def main() -> int:
    ap = argparse.ArgumentParser(description="Recompute flow_phase3 features/outcomes for existing snapshots")
    ap.add_argument("--dsn", default="", help="Postgres DSN")
    ap.add_argument("--limit", type=int, default=5000, help="How many recent snapshots to recompute")
    ap.add_argument("--tz-local", default=os.getenv("TZ_LOCAL", "America/Chicago"))
    ap.add_argument("--min-contracts", type=int, default=int(os.getenv("MIN_CONTRACTS", "50")))
    ap.add_argument("--include-term", action="store_true", help="Also recompute chain_features_term")
    ap.add_argument("--term-buckets", default="term_dte7t2,term_dte21t3", help="Comma-separated term buckets")
    ap.add_argument("--include-outcomes", action="store_true", help="Also recompute outcomes tables for selected snapshots")
    args = ap.parse_args()

    dsn = _dsn(args.dsn)
    if not dsn:
        raise RuntimeError("Missing --dsn / SPX_CHAIN_DATABASE_URL")

    import psycopg
    from options_ai.phase2_builder import Phase2Config, compute_features_for_snapshot
    from options_ai.phase2_term_builder import Phase2TermConfig, compute_features_for_snapshot as compute_term_features
    from options_ai.outcomes_phase3 import OutcomesConfig, compute_outcomes_for_snapshot

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT snapshot_ts
                FROM (
                  SELECT DISTINCT snapshot_ts
                  FROM spx.option_chain
                  ORDER BY snapshot_ts DESC
                  LIMIT %s
                ) q
                ORDER BY snapshot_ts ASC
                """,
                (int(args.limit),),
            )
            snaps = [r[0] for r in cur.fetchall()]

        cfg0 = Phase2Config(db_dsn=dsn)
        n0 = 0
        for ts in snaps:
            r = compute_features_for_snapshot(conn, snapshot_ts=ts, tz_local=args.tz_local, min_contracts=int(args.min_contracts), cfg=cfg0)
            if r:
                n0 += 1

        nt = 0
        if args.include_term:
            buckets = [b.strip() for b in str(args.term_buckets).split(',') if b.strip()]
            for b in buckets:
                if b.startswith('term_dte7'):
                    tcfg = Phase2TermConfig(db_dsn=dsn, term_bucket=b, target_dte_days=7, dte_tolerance_days=2)
                elif b.startswith('term_dte21'):
                    tcfg = Phase2TermConfig(db_dsn=dsn, term_bucket=b, target_dte_days=21, dte_tolerance_days=3)
                elif b.startswith('term_dte14'):
                    tcfg = Phase2TermConfig(db_dsn=dsn, term_bucket=b, target_dte_days=14, dte_tolerance_days=2)
                else:
                    tcfg = Phase2TermConfig(db_dsn=dsn, term_bucket=b)
                for ts in snaps:
                    rr = compute_term_features(conn, snapshot_ts=ts, cfg=tcfg)
                    if rr:
                        nt += 1

        no = 0
        if args.include_outcomes:
            ocfg = OutcomesConfig(
                align_mode=os.getenv("OUTCOME_ALIGN", "FirstOfDay").strip() or "FirstOfDay",
                horizons_td=tuple(int(x) for x in (os.getenv("OUTCOME_HORIZONS_TD", "5,10,21").split(',')) if x.strip()),
                flat_pct_band=float(os.getenv("FLAT_PCT_BAND", "0.0025")),
                tz_local=args.tz_local,
            )
            for ts in snaps:
                no += int(compute_outcomes_for_snapshot(conn, snapshot_ts=ts, cfg=ocfg) or 0)

    print(f"recomputed 0dte features: {n0}")
    print(f"recomputed term features: {nt}")
    print(f"recomputed outcomes upserts: {no}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
