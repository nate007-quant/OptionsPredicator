from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from common import write_json


def write_daily_report(*, run_dir: Path, report: dict[str, Any]) -> Path:
    out = run_dir / "daily_summary.json"
    write_json(out, report)
    return out


def maybe_write_weekly_digest(*, artifacts_runs_dir: Path, latest_run_report: dict[str, Any], now_utc: datetime | None = None) -> Path | None:
    now = now_utc or datetime.now(timezone.utc)
    # weekly digest on Sunday UTC
    if now.weekday() != 6:
        return None

    summaries = []
    for d in sorted(artifacts_runs_dir.glob("*/daily_summary.json"))[-14:]:
        try:
            import json

            summaries.append(json.loads(d.read_text(encoding="utf-8")))
        except Exception:
            continue

    promoted = 0
    rejected = 0
    retired = 0
    reasons: dict[str, int] = {}
    for s in summaries:
        for row in s.get("strategies", []):
            st = row.get("state")
            if st in {"CANDIDATE", "PAPER", "LIVE"}:
                promoted += 1
            if st in {"TESTED", "SPECIFIED", "PROPOSED"}:
                rejected += 1
            if st == "RETIRED":
                retired += 1
            for r in row.get("reasons", []):
                reasons[str(r)] = reasons.get(str(r), 0) + 1

    digest = {
        "generated_at_utc": now.replace(microsecond=0).isoformat(),
        "window_runs": len(summaries),
        "promoted": promoted,
        "rejected": rejected,
        "retired": retired,
        "top_reasons": sorted(reasons.items(), key=lambda x: x[1], reverse=True)[:20],
    }
    out = artifacts_runs_dir / "weekly_digest.json"
    write_json(out, digest)
    return out
