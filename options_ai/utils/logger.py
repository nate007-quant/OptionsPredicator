from __future__ import annotations

import gzip
import json
import os
import traceback
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from options_ai.db import connect


LEVELS = {"DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40, "CRITICAL": 50}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def _ts() -> str:
    return _utc_now().isoformat().replace("+00:00", "Z")


def _level_num(level: str) -> int:
    return LEVELS.get(level.upper(), 20)


def _safe_json(obj: dict[str, Any]) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True)


@dataclass
class _RotatingFile:
    path: Path
    max_bytes: int = 10 * 1024 * 1024
    keep_days: int = 30

    def _should_rotate(self) -> bool:
        if not self.path.exists():
            return False
        try:
            if self.path.stat().st_size >= self.max_bytes:
                return True
        except Exception:
            pass
        # daily rotation: rotate if file mtime date != today
        try:
            m = datetime.fromtimestamp(self.path.stat().st_mtime, tz=timezone.utc).date()
            if m != _utc_now().date():
                return True
        except Exception:
            pass
        return False

    def _rotate(self) -> None:
        if not self.path.exists():
            return
        ts = _utc_now().strftime("%Y%m%d-%H%M%S")
        rotated = self.path.with_suffix(self.path.suffix + f".{ts}")
        try:
            self.path.rename(rotated)
        except Exception:
            return

        # compress
        gz_path = rotated.with_suffix(rotated.suffix + ".gz")
        try:
            with rotated.open("rb") as f_in, gzip.open(gz_path, "wb") as f_out:
                f_out.writelines(f_in)
            rotated.unlink(missing_ok=True)
        except Exception:
            # leave uncompressed if gzip fails
            pass

        # cleanup old
        self._cleanup()

    def _cleanup(self) -> None:
        cutoff = _utc_now() - timedelta(days=int(self.keep_days))
        parent = self.path.parent
        base = self.path.name
        for p in parent.glob(base + ".*.gz"):
            try:
                m = datetime.fromtimestamp(p.stat().st_mtime, tz=timezone.utc)
                if m < cutoff:
                    p.unlink(missing_ok=True)
            except Exception:
                continue

    def write_line(self, line: str) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if self._should_rotate():
            self._rotate()
        with self.path.open("a", encoding="utf-8") as f:
            f.write(line)
            f.write("\n")


class StructuredLogger:
    def __init__(
        self,
        *,
        logs_root: Path,
        db_path: str | None,
        min_level: str = "INFO",
    ):
        self.logs_root = logs_root
        self.db_path = db_path
        self.min_level = min_level.upper()

        self._files = {
            "system": _RotatingFile(logs_root / "system.log"),
            "errors": _RotatingFile(logs_root / "errors.log"),
            "model": _RotatingFile(logs_root / "model.log"),
            "bootstrap": _RotatingFile(logs_root / "bootstrap.log"),
            "routing": _RotatingFile(logs_root / "routing.log"),
            "scoring": _RotatingFile(logs_root / "scoring.log"),
            "performance": _RotatingFile(logs_root / "performance.log"),
        }

    def log(
        self,
        *,
        level: str,
        component: str,
        event: str,
        message: str,
        file_key: str = "system",
        snapshot_hash: str | None = None,
        model_used: str | None = None,
        details: dict[str, Any] | None = None,
        **fields: Any,
    ) -> None:
        lvl = level.upper()
        if _level_num(lvl) < _level_num(self.min_level):
            return

        rec: dict[str, Any] = {
            "timestamp": _ts(),
            "level": lvl,
            "component": component,
            "event": event,
            "message": message,
        }
        if snapshot_hash:
            rec["snapshot_hash"] = snapshot_hash
        if model_used:
            rec["model_used"] = model_used
        if details is not None:
            rec["details"] = details
        if fields:
            rec.update(fields)

        line = _safe_json(rec)

        # always write to system
        self._files["system"].write_line(line)

        # also route to specific file
        if file_key in self._files and file_key != "system":
            self._files[file_key].write_line(line)

        # errors also go to errors.log and DB
        if lvl in {"ERROR", "CRITICAL"}:
            self._files["errors"].write_line(line)
            self._insert_system_event(rec)

    def exception(
        self,
        *,
        level: str,
        component: str,
        event: str,
        message: str,
        file_key: str = "errors",
        snapshot_hash: str | None = None,
        model_used: str | None = None,
        exc: BaseException | None = None,
        **fields: Any,
    ) -> None:
        tb = traceback.format_exc() if exc is None else "".join(traceback.format_exception(exc))
        details = {"traceback": tb}
        self.log(
            level=level,
            component=component,
            event=event,
            message=message,
            file_key=file_key,
            snapshot_hash=snapshot_hash,
            model_used=model_used,
            details=details,
            **fields,
        )

    def _insert_system_event(self, rec: dict[str, Any]) -> None:
        if not self.db_path:
            return
        try:
            with connect(self.db_path) as conn:
                conn.execute(
                    "INSERT INTO system_events (timestamp, level, component, event, message, snapshot_hash, model_used, details_json) VALUES (?,?,?,?,?,?,?,?)",
                    (
                        rec.get("timestamp"),
                        rec.get("level"),
                        rec.get("component"),
                        rec.get("event"),
                        rec.get("message"),
                        rec.get("snapshot_hash"),
                        rec.get("model_used"),
                        json.dumps(rec.get("details") or {}, ensure_ascii=False, sort_keys=True),
                    ),
                )
        except Exception:
            # never crash on logging
            return


_LOGGER: StructuredLogger | None = None


def init_logger(*, logs_root: Path, db_path: str | None) -> StructuredLogger:
    global _LOGGER
    min_level = os.getenv("LOG_LEVEL", "INFO").strip().upper()
    _LOGGER = StructuredLogger(logs_root=logs_root, db_path=db_path, min_level=min_level)
    return _LOGGER


def get_logger() -> StructuredLogger | None:
    return _LOGGER


# Legacy wrappers (kept to reduce churn)

def log_daemon_event(logs_daemon_dir: Path, level: str, message: str, **fields: Any) -> None:
    root = logs_daemon_dir.parent
    lg = _LOGGER or init_logger(logs_root=root, db_path=None)

    lvl = level.upper() if level else "INFO"
    component = str(fields.pop("component", "Daemon"))
    event = message

    # map to log files
    file_key = "system"
    if lvl in {"ERROR", "CRITICAL"}:
        file_key = "errors"

    lg.log(level=lvl, component=component, event=event, message=event, file_key=file_key, **fields)


def log_prediction_event(logs_predictions_dir: Path, observed_date_yyyy_mm_dd: str, event: dict[str, Any]) -> None:
    # Keep old behavior for predictions daily log as JSONL.
    p = logs_predictions_dir / f"{observed_date_yyyy_mm_dd}.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("a", encoding="utf-8") as f:
        f.write(_safe_json(event))
        f.write("\n")


def log_analyzer_report(logs_analyzer_reports_dir: Path, observed_ts_compact: str, report: dict[str, Any]) -> None:
    out = logs_analyzer_reports_dir / f"{observed_ts_compact}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


# New helpers

def log_routing(paths: Any, *, level: str, event: str, message: str, snapshot_hash: str | None = None, **fields: Any) -> None:
    lg = _LOGGER or init_logger(logs_root=Path(paths.logs_predictions_dir).parents[1], db_path=None)
    lg.log(level=level, component="ModelRouter", event=event, message=message, file_key="routing", snapshot_hash=snapshot_hash, **fields)


def log_bootstrap(paths: Any, *, level: str, event: str, message: str, snapshot_hash: str | None = None, **fields: Any) -> None:
    lg = _LOGGER or init_logger(logs_root=Path(paths.logs_predictions_dir).parents[1], db_path=None)
    lg.log(level=level, component="Bootstrap", event=event, message=message, file_key="bootstrap", snapshot_hash=snapshot_hash, **fields)


def log_scoring(paths: Any, *, level: str, event: str, message: str, snapshot_hash: str | None = None, **fields: Any) -> None:
    lg = _LOGGER or init_logger(logs_root=Path(paths.logs_predictions_dir).parents[1], db_path=None)
    lg.log(level=level, component="Scoring", event=event, message=message, file_key="scoring", snapshot_hash=snapshot_hash, **fields)


def log_model(paths: Any, *, level: str, event: str, message: str, snapshot_hash: str | None = None, model_used: str | None = None, **fields: Any) -> None:
    lg = _LOGGER or init_logger(logs_root=Path(paths.logs_predictions_dir).parents[1], db_path=None)
    lg.log(level=level, component="Model", event=event, message=message, file_key="model", snapshot_hash=snapshot_hash, model_used=model_used, **fields)


def log_cache(paths: Any, *, level: str, event: str, message: str, **fields: Any) -> None:
    lg = _LOGGER or init_logger(logs_root=Path(paths.logs_predictions_dir).parents[1], db_path=None)
    lg.log(level=level, component="Cache", event=event, message=message, file_key="system", **fields)
