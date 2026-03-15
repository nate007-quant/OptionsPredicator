#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _lag_minutes(latest_chain, ts) -> float | None:
    if latest_chain is None or ts is None:
        return None
    try:
        return (latest_chain - ts).total_seconds() / 60.0
    except Exception:
        return None


def _fmt(v) -> str:
    if v is None:
        return "—"
    if isinstance(v, float):
        return f"{v:.1f}"
    return str(v)


def _load_task(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def snapshot(conn, *, window: int, align: str) -> dict:
    out = {"now": datetime.now(timezone.utc).replace(microsecond=0).isoformat()}
    with conn.cursor() as cur:
        def max_ts(table: str):
            cur.execute(f"SELECT max(snapshot_ts) FROM {table}")
            r = cur.fetchone()
            return r[0] if r else None

        chain = max_ts("spx.option_chain")
        feat = max_ts("spx.chain_features_0dte")
        term = max_ts("spx.chain_features_term")
        out_u = max_ts("spx.chain_outcomes_underlying")
        out_a = max_ts("spx.chain_outcomes_atm_options")

        out["latest_chain"] = chain
        out["lag_features_0dte_min"] = _lag_minutes(chain, feat)
        out["lag_features_term_min"] = _lag_minutes(chain, term)
        out["lag_outcomes_underlying_min"] = _lag_minutes(chain, out_u)
        out["lag_outcomes_atm_min"] = _lag_minutes(chain, out_a)

        cur.execute(
            """
            WITH f AS (
              SELECT snapshot_ts FROM spx.chain_features_0dte ORDER BY snapshot_ts DESC LIMIT %s
            )
            SELECT
              COUNT(*) AS n,
              SUM(CASE WHEN u.snapshot_ts IS NULL THEN 1 ELSE 0 END) AS miss_u,
              SUM(CASE WHEN a.snapshot_ts IS NULL THEN 1 ELSE 0 END) AS miss_a
            FROM f
            LEFT JOIN (SELECT DISTINCT snapshot_ts FROM spx.chain_outcomes_underlying WHERE align_mode=%s) u
              ON u.snapshot_ts=f.snapshot_ts
            LEFT JOIN (SELECT DISTINCT snapshot_ts FROM spx.chain_outcomes_atm_options WHERE align_mode=%s) a
              ON a.snapshot_ts=f.snapshot_ts
            """,
            (int(window), align, align),
        )
        r = cur.fetchone()
        out["window"] = int(window)
        out["missing_outcomes_underlying"] = int(r[1] or 0)
        out["missing_outcomes_atm"] = int(r[2] or 0)
        out["window_n"] = int(r[0] or 0)

    return out


def print_snapshot(s: dict, *, task_phase2: dict, task_debit_ml: dict) -> None:
    print(
        f"[{s['now']}] "
        f"lag feat0dte={_fmt(s.get('lag_features_0dte_min'))}m | "
        f"lag term={_fmt(s.get('lag_features_term_min'))}m | "
        f"lag out_u={_fmt(s.get('lag_outcomes_underlying_min'))}m | "
        f"lag out_atm={_fmt(s.get('lag_outcomes_atm_min'))}m | "
        f"missing out_u={s.get('missing_outcomes_underlying')}/{s.get('window_n')} | "
        f"missing out_atm={s.get('missing_outcomes_atm')}/{s.get('window_n')}"
    )
    if task_phase2:
        print(f"  task_phase2: stage={task_phase2.get('stage')} snapshot_ts={task_phase2.get('snapshot_ts')} started_at={task_phase2.get('started_at')}")
    if task_debit_ml:
        print(f"  task_debit_ml: stage={task_debit_ml.get('stage')} horizon={task_debit_ml.get('horizon_minutes')} started_at={task_debit_ml.get('started_at')}")


def main() -> int:
    ap = argparse.ArgumentParser(description="Monitor flow_phase3_v1 processing progress")
    ap.add_argument("--dsn", default=os.getenv("SPX_CHAIN_DATABASE_URL", ""), help="Postgres DSN")
    ap.add_argument("--align", default=os.getenv("OUTCOME_ALIGN", "FirstOfDay"), help="Outcome align mode")
    ap.add_argument("--window", type=int, default=500, help="Window for missing coverage")
    ap.add_argument("--interval", type=float, default=10.0, help="Poll interval seconds")
    ap.add_argument("--once", action="store_true", help="Run once and exit")
    ap.add_argument("--state-root", default=os.getenv("DATA_ROOT", "/mnt/options_ai"), help="State root (default /mnt/options_ai)")
    args = ap.parse_args()

    if not args.dsn:
        raise RuntimeError("Missing --dsn (or SPX_CHAIN_DATABASE_URL)")

    import psycopg

    state_root = Path(args.state_root) / "state"
    while True:
        with psycopg.connect(args.dsn) as conn:
            s = snapshot(conn, window=int(args.window), align=args.align)
        task_phase2 = _load_task(state_root / "task_phase2.json")
        task_debit_ml = _load_task(state_root / "task_debit_ml.json")
        print_snapshot(s, task_phase2=task_phase2, task_debit_ml=task_debit_ml)
        if args.once:
            break
        time.sleep(float(args.interval))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
