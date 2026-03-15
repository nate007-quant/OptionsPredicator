#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sqlite3
from typing import Any

import psycopg

TABLES = [
    "predictions",
    "performance_summary",
    "system_events",
    "model_usage",
    "schema_migrations",
    "backtest_sampler_sessions",
    "portfolio_defs",
    "portfolio_backtest_sessions",
    "portfolio_group_runs",
    "portfolio_group_run_portfolios",
    "parameter_groups",
    "execution_account_controls",
    "execution_intents",
    "trade_runs",
    "risk_session_state",
    "reprice_policy",
    "audit_log",
    "broker_reconciliation_log",
    "incident_events",
    "order_events",
    "position_events",
    "tuning_profiles",
    "tuning_config_versions",
    "tuning_config_state",
    "tuning_audit_log",
    "tuning_job_state",
    "tuning_settings",
    "storage_metrics_samples",
]


def has_id_column_sqlite(con: sqlite3.Connection, table: str) -> bool:
    rows = con.execute(f"PRAGMA table_info({table})").fetchall()
    return any(str(r[1]) == "id" for r in rows)


def has_id_column_pg(cur: psycopg.Cursor, schema: str, table: str) -> bool:
    cur.execute(
        """
        SELECT 1 FROM information_schema.columns
        WHERE table_schema=%s AND table_name=%s AND column_name='id'
        """,
        (schema, table),
    )
    return cur.fetchone() is not None


def main() -> None:
    ap = argparse.ArgumentParser(description="Check sqlite vs postgres parity for migrated state tables")
    ap.add_argument("--sqlite", default="/mnt/options_ai/database/predictions.db")
    ap.add_argument("--pg", required=True)
    ap.add_argument("--schema", default="spx")
    ap.add_argument("--strict", action="store_true", help="exit non-zero if any mismatch")
    args = ap.parse_args()

    sq = sqlite3.connect(args.sqlite)
    sq.row_factory = sqlite3.Row

    mismatches: list[str] = []

    with psycopg.connect(args.pg) as pg:
        with pg.cursor() as cur:
            for t in TABLES:
                sc = int(sq.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0])
                cur.execute(f"SELECT COUNT(*) FROM {args.schema}.{t}")
                pc = int(cur.fetchone()[0])
                ok = sc == pc
                print(f"{t:32s} sqlite={sc:8d} pg={pc:8d} {'OK' if ok else 'MISMATCH'}")
                if not ok:
                    mismatches.append(f"count mismatch {t}: sqlite={sc} pg={pc}")

                if sc <= 10000 and has_id_column_sqlite(sq, t) and has_id_column_pg(cur, args.schema, t):
                    sids = {int(r[0]) for r in sq.execute(f"SELECT id FROM {t}").fetchall()}
                    cur.execute(f"SELECT id FROM {args.schema}.{t}")
                    pids = {int(r[0]) for r in cur.fetchall()}
                    if sids != pids:
                        miss = len(sids - pids)
                        extra = len(pids - sids)
                        mismatches.append(f"id-set mismatch {t}: missing_in_pg={miss} extra_in_pg={extra}")
                        print(f"  -> id-set mismatch missing_in_pg={miss} extra_in_pg={extra}")

    if mismatches:
        print("\nMISMATCHES:")
        for m in mismatches:
            print(f"- {m}")
        if args.strict:
            raise SystemExit(1)


if __name__ == "__main__":
    main()
