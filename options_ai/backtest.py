from __future__ import annotations

import argparse
import json
from dataclasses import replace
from pathlib import Path
from typing import Any

from options_ai.ai.router import ModelRouter
from options_ai.ai.throttle import RateLimiter
from options_ai.config import load_config
from options_ai.db import db_path_from_url, init_db
from options_ai.processes.ingest import ingest_snapshot_file, parse_snapshot_filename
from options_ai.utils.paths import build_paths, ensure_runtime_dirs
from options_ai.utils.cache import sha256_file


def run_backtest(
    *,
    snapshots_dir: Path,
    out_jsonl: Path,
    limit: int | None = None,
) -> dict[str, Any]:
    cfg0 = load_config()
    cfg = replace(
        cfg0,
        backtest_mode=True,
        chart_enabled=False,
        chart_local_enabled=False,
        chart_remote_enabled=False,
        replay_mode=True,
    )

    paths = build_paths(cfg.data_root, cfg.ticker)
    ensure_runtime_dirs(paths)

    db_path = db_path_from_url(cfg.database_url)
    init_db(db_path, str(Path(__file__).resolve().parent / "db" / "schema.sql"))

    limiter = RateLimiter(
        max_per_minute=int(cfg.bootstrap_max_model_calls_per_min or 0),
        max_per_hour=int(cfg.bootstrap_max_model_calls_per_hour or 0),
    )
    router = ModelRouter(cfg, bootstrap_rate_limiter=limiter)

    files = [p for p in snapshots_dir.glob("*.json") if p.is_file()]
    files.sort(key=lambda p: parse_snapshot_filename(p.name).observed_dt_utc)

    if limit is not None and limit > 0:
        files = files[: int(limit)]

    out_jsonl.parent.mkdir(parents=True, exist_ok=True)
    state: dict[str, Any] = {}

    processed = 0
    skipped = 0
    with out_jsonl.open("w", encoding="utf-8") as f:
        for p in files:
            parsed = parse_snapshot_filename(p.name)
            h = sha256_file(p)
            res = ingest_snapshot_file(
                cfg=cfg,
                paths=paths,
                db_path=db_path,
                snapshot_path=p,
                snapshot_hash=h,
                router=router,
                state=state,
                bootstrap_mode=False,
                move_files=False,
            )
            rec = {
                "file": p.name,
                "snapshot_hash": h,
                "observed_ts_utc": parsed.observed_dt_utc.replace(microsecond=0).isoformat(),
                "processed": bool(res.processed),
                "prediction_id": res.prediction_id,
                "skipped_reason": res.skipped_reason,
            }
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            if res.processed:
                processed += 1
            else:
                skipped += 1

    return {"total_files": len(files), "processed": processed, "skipped": skipped, "out": str(out_jsonl)}


def main() -> None:
    ap = argparse.ArgumentParser(description="OptionsPredicator backtest runner (event-time causal mode)")
    ap.add_argument("snapshots_dir", type=str, help="Directory containing snapshot *.json files")
    ap.add_argument("--out", type=str, default="/mnt/options_ai/state/backtest_results.jsonl")
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()

    res = run_backtest(
        snapshots_dir=Path(args.snapshots_dir),
        out_jsonl=Path(args.out),
        limit=(args.limit or None),
    )
    print(json.dumps(res, indent=2))


if __name__ == "__main__":
    main()
