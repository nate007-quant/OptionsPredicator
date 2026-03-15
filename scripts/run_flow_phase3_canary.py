#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import asdict, replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _d(s: str) -> date:
    return date.fromisoformat(s)


def _auto_window(conn, days: int) -> tuple[date, date]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
              min((snapshot_ts AT TIME ZONE 'America/Chicago')::date),
              max((snapshot_ts AT TIME ZONE 'America/Chicago')::date)
            FROM spx.chain_features_0dte
            """
        )
        r = cur.fetchone()
        if not r or r[0] is None or r[1] is None:
            raise RuntimeError("No chain_features_0dte rows available for auto window")
        end = r[1]
        start = max(r[0], end - timedelta(days=max(1, int(days))))
        return start, end


def _summary_cmp(base: dict[str, Any], gate: dict[str, Any]) -> dict[str, Any]:
    keys = [
        "trades",
        "wins",
        "losses",
        "win_rate",
        "avg_roi",
        "avg_pnl_dollars",
        "cum_pnl_dollars",
        "max_drawdown_dollars",
        "profit_factor",
    ]
    out: dict[str, Any] = {}
    for k in keys:
        b = base.get(k)
        g = gate.get(k)
        d = None
        try:
            if b is not None and g is not None:
                d = float(g) - float(b)
        except Exception:
            d = None
        out[k] = {"baseline": b, "gated": g, "delta": d}
    return out


def _load_dsn(args_dsn: str) -> str:
    return (
        args_dsn.strip()
        or os.getenv("SPX_CHAIN_DATABASE_URL", "").strip()
        or os.getenv("PHASE2_DB_DSN", "").strip()
    )


def main() -> int:
    ap = argparse.ArgumentParser(description="Run flow_phase3 canary backtest comparison (baseline vs flow-gated)")
    ap.add_argument("--dsn", default="", help="Postgres DSN (or SPX_CHAIN_DATABASE_URL env)")
    ap.add_argument("--start", default="", help="Start day YYYY-MM-DD")
    ap.add_argument("--end", default="", help="End day YYYY-MM-DD")
    ap.add_argument("--auto-days", type=int, default=30, help="Auto window in calendar days when --start/--end not provided")
    ap.add_argument("--horizon", type=int, default=30, help="Horizon minutes")
    ap.add_argument("--max-debit", type=float, default=5.0, help="Max debit points")
    ap.add_argument("--min-p-bigwin", type=float, default=0.0)
    ap.add_argument("--min-pred-change", type=float, default=0.0)
    ap.add_argument("--gate-bucket-z", type=float, default=1.5)
    ap.add_argument("--gate-breadth", type=float, default=0.60)
    ap.add_argument("--gate-confidence", type=float, default=0.60)
    ap.add_argument("--gate-live-ok", action="store_true", help="Also require flow_live_ok_default")
    ap.add_argument("--out", default="", help="Write full JSON report to this file")
    args = ap.parse_args()

    dsn = _load_dsn(args.dsn)
    if not dsn:
        raise RuntimeError("Missing --dsn (or SPX_CHAIN_DATABASE_URL)")

    import psycopg
    from options_ai.backtest.debit_spreads import DebitBacktestConfig, run_backtest_debit_spreads

    with psycopg.connect(dsn) as conn:
        if args.start and args.end:
            start_day, end_day = _d(args.start), _d(args.end)
        else:
            start_day, end_day = _auto_window(conn, args.auto_days)

        base_cfg = DebitBacktestConfig(
            start_day=start_day,
            end_day=end_day,
            horizon_minutes=int(args.horizon),
            max_debit_points=float(args.max_debit),
            min_p_bigwin=float(args.min_p_bigwin),
            min_pred_change=float(args.min_pred_change),
            flow_gate_enabled=False,
            flow_live_ok_filter_enabled=False,
        )

        gate_cfg = replace(
            base_cfg,
            flow_gate_enabled=True,
            flow_live_ok_filter_enabled=bool(args.gate_live_ok),
            flow_gate_min_bucket_z=float(args.gate_bucket_z),
            flow_gate_min_breadth=float(args.gate_breadth),
            flow_gate_min_confidence=float(args.gate_confidence),
        )

        baseline = run_backtest_debit_spreads(conn, base_cfg)
        gated = run_backtest_debit_spreads(conn, gate_cfg)

    comp = _summary_cmp(baseline.get("summary", {}), gated.get("summary", {}))

    report = {
        "generated_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "window": {"start_day": start_day.isoformat(), "end_day": end_day.isoformat()},
        "baseline_config": baseline.get("config"),
        "gated_config": gated.get("config"),
        "summary_comparison": comp,
        "baseline_summary": baseline.get("summary"),
        "gated_summary": gated.get("summary"),
    }

    print("=== flow_phase3 canary comparison ===")
    print(json.dumps(report["summary_comparison"], indent=2, default=str))

    out_path = args.out.strip()
    if not out_path:
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        out_path = f"/mnt/options_ai/test_runs/flow_phase3_canary_{ts}.json"
    p = Path(out_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    print(f"wrote: {p}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
