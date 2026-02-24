from __future__ import annotations

import os
import sqlite3
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse

from options_ai.config import load_config
from options_ai.runtime_overrides import (
    allowlist_public_spec,
    apply_overrides,
    load_overrides_file,
    validate_and_normalize_overrides,
    write_overrides_file_atomic,
)
from options_ai.utils_web.tail import tail_jsonl


CENTRAL_TZ = ZoneInfo("America/Chicago")


def _now_central_iso() -> str:
    return datetime.now(timezone.utc).astimezone(CENTRAL_TZ).replace(microsecond=0).isoformat()


def _to_central_iso(x: Any) -> Any:
    if x is None:
        return None
    if isinstance(x, datetime):
        dt = x
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(CENTRAL_TZ).replace(microsecond=0).isoformat()
    if isinstance(x, str):
        try:
            dt = datetime.fromisoformat(x.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(CENTRAL_TZ).replace(microsecond=0).isoformat()
        except Exception:
            return x
    return x


def _db_path_from_database_url(database_url: str) -> str:
    # expected form: sqlite:////abs/path/to.db
    if not database_url.startswith("sqlite:"):
        raise ValueError("only sqlite DATABASE_URL supported")
    p = database_url.replace("sqlite:", "", 1)
    while p.startswith("////"):
        p = p[1:]
    if not p.startswith("/"):
        raise ValueError("sqlite path must be absolute")
    return p


def _connect(db_path: str) -> sqlite3.Connection:
    con = sqlite3.connect(db_path, timeout=2.0)
    con.row_factory = sqlite3.Row
    return con


def _calc_metrics(rows: list[sqlite3.Row]) -> dict[str, Any]:
    counts = {
        "total_scored": 0,
        "correct": 0,
        "wrong_direction": 0,
        "correct_direction_wrong_magnitude": 0,
        "inconclusive": 0,
        "hi_total": 0,
        "hi_correct": 0,
    }

    for r in rows:
        res = r["result"]
        if res is None:
            continue
        counts["total_scored"] += 1
        if res == "correct":
            counts["correct"] += 1
        elif res == "wrong_direction":
            counts["wrong_direction"] += 1
        elif res == "correct_direction_wrong_magnitude":
            counts["correct_direction_wrong_magnitude"] += 1
        else:
            counts["inconclusive"] += 1

        # High confidence: treat >=0.8 as "high" (matches existing perf summary conventions)
        try:
            conf = float(r["confidence"])
        except Exception:
            conf = 0.0
        if conf >= 0.8:
            counts["hi_total"] += 1
            if res == "correct":
                counts["hi_correct"] += 1

    overall_accuracy = None
    if counts["total_scored"] > 0:
        overall_accuracy = counts["correct"] / counts["total_scored"]

    denom_excl = counts["total_scored"] - counts["inconclusive"]
    acc_excl = None
    if denom_excl > 0:
        acc_excl = counts["correct"] / denom_excl

    hi_acc = None
    if counts["hi_total"] > 0:
        hi_acc = counts["hi_correct"] / counts["hi_total"]

    return {
        **counts,
        "overall_accuracy": overall_accuracy,
        "accuracy_excluding_inconclusive": acc_excl,
        "hi_accuracy": hi_acc,
    }


def _central_day_key(iso_ts: str | None) -> str | None:
    if not iso_ts:
        return None
    try:
        dt = datetime.fromisoformat(str(iso_ts).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(CENTRAL_TZ).date().isoformat()
    except Exception:
        return None


def create_app() -> FastAPI:
    cfg = load_config()

    data_root = Path(os.getenv("DATA_ROOT", cfg.data_root or "/mnt/options_ai"))
    logs_root = data_root / "logs"
    overrides_path = data_root / "state" / "runtime_overrides.json"

    db_path = os.getenv("OPTIONS_AI_DB_PATH")
    if not db_path:
        db_path = _db_path_from_database_url(cfg.database_url)

    app = FastAPI(title="OptionsPredicator Dashboard API", version="0.1")

    @app.get("/", response_class=HTMLResponse)
    def index() -> str:
        html_path = Path(__file__).with_name("ui.html")
        return html_path.read_text(encoding="utf-8")

    @app.get("/api/health")
    def health() -> dict[str, Any]:
        return {"ok": True, "time": _now_central_iso(), "tz": "America/Chicago"}

    @app.get("/api/status/processing")
    def status_processing(limit: int = Query(50, ge=1, le=500)) -> dict[str, Any]:
        incoming_dir = data_root / "incoming" / "SPX"
        processed_dir = data_root / "processed" / "SPX" / "snapshots"

        queue_items = []
        if incoming_dir.exists():
            for p in sorted(incoming_dir.glob("*.json"))[-limit:]:
                try:
                    st = p.stat()
                    queue_items.append({"file": p.name, "size": st.st_size, "mtime": st.st_mtime})
                except Exception:
                    continue

        # Optional current task state file (daemon writes this if enabled)
        current_task_path = data_root / "state" / "current_task.json"
        processing_items: list[dict[str, Any]] = []
        if current_task_path.exists():
            try:
                import json

                obj = json.loads(current_task_path.read_text(encoding="utf-8"))
                if isinstance(obj, dict) and obj.get("file"):
                    started_at = _to_central_iso(obj.get("started_at"))
                    elapsed = None
                    try:
                        if obj.get("started_at"):
                            dt = datetime.fromisoformat(str(obj.get("started_at")).replace("Z", "+00:00"))
                            if dt.tzinfo is None:
                                dt = dt.replace(tzinfo=timezone.utc)
                            elapsed = max(0.0, (datetime.now(timezone.utc) - dt.astimezone(timezone.utc)).total_seconds())
                    except Exception:
                        elapsed = None
                    processing_items = [
                        {
                            "file": obj.get("file"),
                            "started_at": started_at,
                            "elapsed_seconds": elapsed,
                            "stage": obj.get("stage") or "unknown",
                            "snapshot_hash": obj.get("snapshot_hash"),
                            "model_used": obj.get("model_used"),
                            "pid": obj.get("pid"),
                        }
                    ]
            except Exception:
                pass

        with _connect(db_path) as con:
            cur = con.execute("SELECT COUNT(*) AS n FROM predictions")
            total_predictions = int(cur.fetchone()["n"])
            cur = con.execute("SELECT COUNT(*) AS n FROM predictions WHERE result IS NOT NULL")
            total_scored = int(cur.fetchone()["n"])
            cur = con.execute("SELECT COUNT(*) AS n FROM predictions WHERE result IS NULL")
            unscored = int(cur.fetchone()["n"])

        return {
            "incoming_dir": str(incoming_dir),
            "processed_dir": str(processed_dir),
            "queue": {"count": len(queue_items), "items": queue_items},
            "processing": {"count": len(processing_items), "items": processing_items},
            "counters": {"total_predictions": total_predictions, "total_scored": total_scored, "unscored": unscored},
            "tz": "America/Chicago",
        }

    @app.get("/api/metrics/daily")
    def metrics_daily(days: int = Query(30, ge=1, le=365)) -> dict[str, Any]:
        with _connect(db_path) as con:
            rows = con.execute(
                """
                SELECT timestamp, scored_at, result, confidence
                FROM predictions
                WHERE result IS NOT NULL
                ORDER BY COALESCE(scored_at, timestamp) DESC
                """
            ).fetchall()

        by_day: dict[str, list[sqlite3.Row]] = {}
        for r in rows:
            ts = r["scored_at"] or r["timestamp"]
            day = _central_day_key(ts)
            if not day:
                continue
            by_day.setdefault(day, []).append(r)

        series = []
        for day in sorted(by_day.keys(), reverse=True)[:days]:
            m = _calc_metrics(by_day[day])
            series.append({"day": day, **m})

        return {"days": days, "series": series, "tz": "America/Chicago"}

    @app.get("/api/metrics/rolling")
    def metrics_rolling(n: int = Query(50, ge=10, le=5000)) -> dict[str, Any]:
        with _connect(db_path) as con:
            rows = con.execute(
                """
                SELECT result, confidence, COALESCE(scored_at, timestamp) AS ts
                FROM predictions
                WHERE result IS NOT NULL
                ORDER BY ts DESC
                LIMIT ?
                """,
                (int(n),),
            ).fetchall()

        as_of = None
        if rows:
            as_of = _to_central_iso(rows[0]["ts"])

        m = _calc_metrics(rows)
        return {"n": n, "as_of": as_of, **m, "tz": "America/Chicago"}

    @app.get("/api/predictions/recent")
    def predictions_recent(limit: int = Query(100, ge=1, le=1000)) -> dict[str, Any]:
        with _connect(db_path) as con:
            rows = con.execute(
                """
                SELECT id, timestamp, predicted_direction, predicted_magnitude, confidence,
                       result, spot_price, scored_at
                FROM predictions
                ORDER BY timestamp DESC
                LIMIT ?
                """,
                (int(limit),),
            ).fetchall()

        items = []
        for r in rows:
            d = dict(r)
            d["timestamp"] = _to_central_iso(d.get("timestamp"))
            d["scored_at"] = _to_central_iso(d.get("scored_at"))
            items.append(d)

        return {"limit": limit, "items": items, "tz": "America/Chicago"}

    @app.get("/api/logs/tail")
    def logs_tail(
        name: str = Query(..., pattern="^(system|errors|model|routing|scoring|performance|bootstrap)$"),
        limit: int = Query(200, ge=10, le=2000),
    ) -> dict[str, Any]:
        path = logs_root / f"{name}.log"
        lines = tail_jsonl(path, limit=limit)

        # Normalize parsed timestamp fields if present
        for item in lines:
            parsed = item.get("parsed")
            if isinstance(parsed, dict) and parsed.get("timestamp"):
                parsed["timestamp"] = _to_central_iso(parsed.get("timestamp"))

        return {"name": name, "limit": limit, "lines": lines, "tz": "America/Chicago"}

    @app.get("/api/config")
    def get_config() -> dict[str, Any]:
        overrides = load_overrides_file(overrides_path)
        effective = apply_overrides(cfg, overrides)
        return {
            "base": asdict(cfg),
            "overrides": overrides,
            "effective": asdict(effective),
            "allowlist": allowlist_public_spec(),
            "overrides_path": str(overrides_path),
            "tz": "America/Chicago",
        }

    @app.patch("/api/config")
    def patch_config(patch: dict[str, Any]) -> dict[str, Any]:
        current = load_overrides_file(overrides_path)
        norm_patch = validate_and_normalize_overrides(patch)

        merged = dict(current)
        for k, v in norm_patch.items():
            if v is None:
                merged.pop(k, None)
            else:
                merged[k] = v

        write_overrides_file_atomic(overrides_path, merged)

        effective = apply_overrides(cfg, merged)
        return {
            "overrides": merged,
            "effective": asdict(effective),
            "allowlist": allowlist_public_spec(),
            "written_to": str(overrides_path),
            "tz": "America/Chicago",
        }

    return app


app = create_app()
