from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def parse_iso_utc(s: str) -> datetime | None:
    if not s or not isinstance(s, str):
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def extract_observed_dt_utc(snapshot: dict[str, Any]) -> datetime | None:
    """Extract observed timestamp from snapshot JSON.

    Tries common fields. Returns UTC datetime if found, else None.
    """

    if not isinstance(snapshot, dict):
        return None

    # Preferred explicit ISO fields
    for key in (
        "observed_ts_utc",
        "observed_utc",
        "observed_at",
        "observedAt",
        "timestamp",
        "ts_utc",
        "ts",
    ):
        v = snapshot.get(key)
        if isinstance(v, str):
            dt = parse_iso_utc(v)
            if dt is not None:
                return dt

    # Epoch seconds
    for key in ("observed_utc_epoch", "observed_epoch", "timestamp_epoch", "ts_epoch"):
        v = snapshot.get(key)
        if isinstance(v, (int, float)):
            try:
                return datetime.fromtimestamp(int(v), tz=timezone.utc)
            except Exception:
                pass

    return None
