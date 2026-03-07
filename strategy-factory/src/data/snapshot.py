from __future__ import annotations

from dataclasses import dataclass
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from common import sha256_text, utc_now_iso, write_json


@dataclass
class SnapshotResult:
    ok: bool
    snapshot_id: str
    degraded: bool
    reasons: list[str]


def create_data_snapshot(*, run_dir: Path) -> SnapshotResult:
    # MVP: record point-in-time metadata and basic freshness guards.
    now = utc_now_iso()
    reasons: list[str] = []
    degraded = False

    # These checks can be wired to real data stores in production.
    checks = {
        "pit_option_chain_available": True,
        "bid_ask_history_available": True,
        "oi_volume_available": True,
        "rates_dividends_available": True,
        "event_calendar_available": True,
        "no_lookahead_enforced": True,
    }

    if os.getenv("FORCE_DEGRADED_SNAPSHOT", "").strip().lower() in {"1", "true", "yes", "on"}:
        checks["pit_option_chain_available"] = False

    for k, v in checks.items():
        if not v:
            degraded = True
            reasons.append(f"missing_or_stale:{k}")

    sid = f"snap-{sha256_text(now)[:12]}"
    write_json(
        run_dir / "data_snapshot.json",
        {
            "snapshot_id": sid,
            "created_at_utc": now,
            "checks": checks,
            "degraded": degraded,
            "reasons": reasons,
        },
    )
    return SnapshotResult(ok=(not degraded), snapshot_id=sid, degraded=degraded, reasons=reasons)
