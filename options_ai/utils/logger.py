from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def append_jsonl(path: Path, obj: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False, sort_keys=True))
        f.write("\n")


def log_daemon_event(logs_daemon_dir: Path, level: str, message: str, **fields: Any) -> None:
    event = {
        "ts": utc_now_iso(),
        "level": level,
        "message": message,
        **fields,
    }
    append_jsonl(logs_daemon_dir / "daemon.jsonl", event)


def log_prediction_event(logs_predictions_dir: Path, observed_date_yyyy_mm_dd: str, event: dict[str, Any]) -> None:
    # Stored as JSON Lines for safe appends.
    append_jsonl(logs_predictions_dir / f"{observed_date_yyyy_mm_dd}.json", event)


def log_analyzer_report(logs_analyzer_reports_dir: Path, observed_ts_compact: str, report: dict[str, Any]) -> None:
    # One file per observation for easier debugging.
    out = logs_analyzer_reports_dir / f"{observed_ts_compact}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
