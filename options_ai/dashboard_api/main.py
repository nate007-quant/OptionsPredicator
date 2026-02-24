from __future__ import annotations

import os
import sqlite3
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from fastapi import FastAPI, Query, HTTPException
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
from options_ai.queries import fetch_tokens_summary, fetch_tokens_hourly_series


CENTRAL_TZ = ZoneInfo("America/Chicago")



def _safe_data_root(p: Path) -> Path:
    p = p.resolve()
    if str(p) in {"/", ""}:
        raise ValueError("DATA_ROOT unsafe")
    if not p.is_absolute():
        raise ValueError("DATA_ROOT must be absolute")
    return p


def _is_within(root: Path, child: Path) -> bool:
    try:
        child.resolve().relative_to(root)
        return True
    except Exception:
        return False


def _wipe_path(root: Path, rel: str, errors: list[str]) -> int:
    """Delete files/dirs under root/rel (contents only). Returns deleted count."""
    from shutil import rmtree

    deleted = 0
    target = (root / rel).resolve()
    if not _is_within(root, target):
        errors.append(f"refusing to delete outside data_root: {target}")
        return 0

    if not target.exists():
        return 0

    # If it's a file, unlink it.
    if target.is_file() or target.is_symlink():
        try:
            target.unlink(missing_ok=True)
            return 1
        except Exception as e:
            errors.append(f"unlink failed {target}: {e}")
            return 0

    # If it's a dir, delete contents.
    if target.is_dir():
        for child in list(target.iterdir()):
            try:
                if child.is_dir():
                    rmtree(child)
                else:
                    child.unlink(missing_ok=True)
                deleted += 1
            except Exception as e:
                errors.append(f"delete failed {child}: {e}")
        return deleted

    return 0

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
    def status_processing(
        page: int = Query(1, ge=1),
        page_size: int = Query(50, ge=10, le=200),
        order: str = Query("oldest", pattern="^(oldest|newest)$"),
    ) -> dict[str, Any]:
        incoming_dir = data_root / "incoming" / "SPX"
        processed_dir = data_root / "processed" / "SPX" / "snapshots"
        queue_items = []
        total_count = 0
        if incoming_dir.exists():
            items_all = []
            for p in incoming_dir.glob("*.json"):
                try:
                    st = p.stat()
                    items_all.append({"file": p.name, "size": st.st_size, "mtime": st.st_mtime})
                except Exception:
                    continue
            items_all.sort(key=lambda x: x["mtime"], reverse=(order == "newest"))
            total_count = len(items_all)
            start = (int(page) - 1) * int(page_size)
            end = start + int(page_size)
            queue_items = items_all[start:end]

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

        # Scoring health: newest snapshot_index key + oldest unscored prediction
        newest_snapshot = None
        try:
            import json as _json
            seen_path = data_root / "state" / "seen_files.json"
            if seen_path.exists():
                st = _json.loads(seen_path.read_text(encoding="utf-8"))
                keys = list((st.get("snapshot_index") or {}).keys())
                newest_snapshot = max(keys) if keys else None
        except Exception:
            newest_snapshot = None

        oldest_unscored = None
        try:
            cur = con.execute(
                "SELECT MIN(COALESCE(observed_ts_utc, timestamp)) AS ts FROM predictions WHERE result IS NULL"
            )
            oldest_unscored = cur.fetchone()["ts"]
        except Exception:
            oldest_unscored = None

        return {
            "incoming_dir": str(incoming_dir),
            "processed_dir": str(processed_dir),
            "queue": {"total_count": int(total_count), "page": int(page), "page_size": int(page_size), "order": order, "count": len(queue_items), "items": queue_items},
            "processing": {"count": len(processing_items), "items": processing_items},
            "counters": {"total_predictions": total_predictions, "total_scored": total_scored, "unscored": unscored},
            "snapshot_index_newest_ts": _to_central_iso(newest_snapshot) if newest_snapshot else None,
            "oldest_unscored_ts": _to_central_iso(oldest_unscored) if oldest_unscored else None,
            "tz": "America/Chicago",
        }

    @app.get("/api/metrics/daily")
    def metrics_daily(days: int = Query(30, ge=1, le=365)) -> dict[str, Any]:
        with _connect(db_path) as con:
            rows = con.execute(
                """
                SELECT observed_ts_utc, timestamp, result, confidence
                FROM predictions
                WHERE result IS NOT NULL AND model_provider != 'ml'
                ORDER BY COALESCE(observed_ts_utc, timestamp) DESC
                """
            ).fetchall()

        by_day: dict[str, list[sqlite3.Row]] = {}
        for r in rows:
            ts = r["observed_ts_utc"] or r["timestamp"]
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
                SELECT result, confidence, COALESCE(observed_ts_utc, timestamp) AS ts
                FROM predictions
                WHERE result IS NOT NULL AND model_provider != 'ml'
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

    
    @app.get("/api/ml/metrics/rolling")
    def ml_metrics_rolling(n: int = Query(50, ge=10, le=5000)) -> dict[str, Any]:
        with _connect(db_path) as con:
            rows = con.execute(
                """
                SELECT result, confidence, predicted_direction, predicted_magnitude, actual_move, COALESCE(observed_ts_utc, timestamp) AS ts
                FROM predictions
                WHERE result IS NOT NULL AND model_provider = 'ml'
                ORDER BY ts DESC
                LIMIT ?
                """,
                (int(n),),
            ).fetchall()

        as_of = _to_central_iso(rows[0]["ts"]) if rows else None
        m = _calc_metrics(rows)

        total = int(m.get('total_scored') or 0)
        actionable_total = 0
        actionable_correct = 0
        for r in rows:
            if (r['predicted_direction'] or '') != 'neutral':
                actionable_total += 1
                if r['result'] == 'correct':
                    actionable_correct += 1

        action_rate = (actionable_total / total) if total > 0 else None
        acc_actionable = (actionable_correct / actionable_total) if actionable_total > 0 else None

        return {
            'n': n,
            'as_of': as_of,
            **m,
            'action_rate': action_rate,
            'accuracy_actionable': acc_actionable,
            'actionable_total': actionable_total,
            'tz': 'America/Chicago',
        }

    @app.get("/api/ml/metrics/daily")
    def ml_metrics_daily(days: int = Query(30, ge=1, le=365)) -> dict[str, Any]:
        with _connect(db_path) as con:
            rows = con.execute(
                """
                SELECT observed_ts_utc, timestamp, result, confidence, predicted_direction
                FROM predictions
                WHERE result IS NOT NULL AND model_provider = 'ml'
                ORDER BY COALESCE(observed_ts_utc, timestamp) DESC
                """
            ).fetchall()

        by_day: dict[str, list[sqlite3.Row]] = {}
        for r in rows:
            ts = r['observed_ts_utc'] or r['timestamp']
            day = _central_day_key(ts)
            if not day:
                continue
            by_day.setdefault(day, []).append(r)

        series = []
        for day in sorted(by_day.keys(), reverse=True)[:days]:
            rs = by_day[day]
            m = _calc_metrics(rs)
            total = int(m.get('total_scored') or 0)
            actionable_total = 0
            actionable_correct = 0
            for r in rs:
                if (r['predicted_direction'] or '') != 'neutral':
                    actionable_total += 1
                    if r['result'] == 'correct':
                        actionable_correct += 1
            action_rate = (actionable_total / total) if total > 0 else None
            acc_actionable = (actionable_correct / actionable_total) if actionable_total > 0 else None
            series.append({'day': day, **m, 'action_rate': action_rate, 'accuracy_actionable': acc_actionable, 'actionable_total': actionable_total})

        return {'days': days, 'series': series, 'tz': 'America/Chicago'}
    
    def _series_buckets_query(*, provider_filter_sql: str, window_days: int, bucket_minutes: int) -> tuple[str, tuple[Any, ...]]:
        # bucket_start_utc as ISO-like UTC string (no offset); we convert for output later.
        bucket_seconds = int(bucket_minutes) * 60
        sql = f"""
            SELECT
              datetime((strftime('%s', COALESCE(observed_ts_utc, timestamp)) / ?) * ?, 'unixepoch') AS bucket_start_utc,
              COUNT(*) AS total_scored,
              SUM(CASE WHEN result='correct' THEN 1 ELSE 0 END) AS correct,
              SUM(CASE WHEN result='wrong_direction' THEN 1 ELSE 0 END) AS wrong_direction,
              SUM(CASE WHEN result='correct_direction_wrong_magnitude' THEN 1 ELSE 0 END) AS correct_direction_wrong_magnitude,
              SUM(CASE WHEN result NOT IN ('correct','wrong_direction','correct_direction_wrong_magnitude') THEN 1 ELSE 0 END) AS inconclusive,
              SUM(CASE WHEN predicted_direction != 'neutral' THEN 1 ELSE 0 END) AS actionable_total,
              SUM(CASE WHEN predicted_direction != 'neutral' AND result='correct' THEN 1 ELSE 0 END) AS actionable_correct
            FROM predictions
            WHERE result IS NOT NULL
              AND COALESCE(observed_ts_utc, timestamp) >= datetime(?, '-' || ? || ' days')
              AND {provider_filter_sql}
            GROUP BY bucket_start_utc
            ORDER BY bucket_start_utc ASC
        """
        params: tuple[Any, ...] = (bucket_seconds, bucket_seconds, now_utc, int(window_days))
        return sql, _params

    def _postprocess_bucket_row(r: sqlite3.Row, *, min_samples: int, include_action: bool) -> dict[str, Any]:
        total = int(r['total_scored'] or 0)
        correct = int(r['correct'] or 0)
        wrong_dir = int(r['wrong_direction'] or 0)
        cdwm = int(r['correct_direction_wrong_magnitude'] or 0)
        inconc = int(r['inconclusive'] or 0)

        overall = None
        excl = None
        if total >= int(min_samples) and total > 0:
            overall = correct / total
            denom_excl = total - inconc
            excl = (correct / denom_excl) if denom_excl > 0 else None

        actionable_total = int(r['actionable_total'] or 0) if include_action else 0
        actionable_correct = int(r['actionable_correct'] or 0) if include_action else 0
        action_rate = (actionable_total / total) if include_action and total > 0 else None
        acc_actionable = None
        if include_action and actionable_total >= int(min_samples) and actionable_total > 0:
            acc_actionable = actionable_correct / actionable_total

        return {
            'bucket_start': _to_central_iso(str(r['bucket_start_utc']) + '+00:00'),
            'total_scored': total,
            'correct': correct,
            'wrong_direction': wrong_dir,
            'correct_direction_wrong_magnitude': cdwm,
            'inconclusive': inconc,
            'overall_accuracy': overall,
            'accuracy_excluding_inconclusive': excl,
            'action_rate': action_rate,
            'actionable_total': actionable_total if include_action else None,
            'accuracy_actionable': acc_actionable,
        }


    @app.get("/api/metrics/series_buckets")
    def metrics_series_buckets(
        window_days: int = Query(15, ge=1, le=60),
        bucket_minutes: int = Query(15),
        min_samples: int = Query(5, ge=1, le=200),
    ) -> dict[str, Any]:
        if int(bucket_minutes) not in {5, 10, 15, 30, 60}:
            raise HTTPException(status_code=400, detail='bucket_minutes must be one of 5,10,15,30,60')

        now_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

        sql, _params = _series_buckets_query(
            provider_filter_sql="model_provider != 'ml'",
            window_days=int(window_days),
            bucket_minutes=int(bucket_minutes),
        )
        with _connect(db_path) as con:
            rows = con.execute(sql, (int(bucket_minutes) * 60, int(bucket_minutes) * 60, now_utc, int(window_days))).fetchall()

        series = [_postprocess_bucket_row(r, min_samples=int(min_samples), include_action=False) for r in rows]

        return {
            'window_days': int(window_days),
            'bucket_minutes': int(bucket_minutes),
            'min_samples': int(min_samples),
            'tz': 'America/Chicago',
            'series': series,
        }


    @app.get("/api/ml/metrics/series_buckets")
    def ml_metrics_series_buckets(
        window_days: int = Query(15, ge=1, le=60),
        bucket_minutes: int = Query(15),
        min_samples: int = Query(5, ge=1, le=200),
    ) -> dict[str, Any]:
        if int(bucket_minutes) not in {5, 10, 15, 30, 60}:
            raise HTTPException(status_code=400, detail='bucket_minutes must be one of 5,10,15,30,60')

        now_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

        sql, _params = _series_buckets_query(
            provider_filter_sql="model_provider = 'ml'",
            window_days=int(window_days),
            bucket_minutes=int(bucket_minutes),
        )
        with _connect(db_path) as con:
            rows = con.execute(sql, (int(bucket_minutes) * 60, int(bucket_minutes) * 60, now_utc, int(window_days))).fetchall()

        series = [_postprocess_bucket_row(r, min_samples=int(min_samples), include_action=True) for r in rows]

        return {
            'window_days': int(window_days),
            'bucket_minutes': int(bucket_minutes),
            'min_samples': int(min_samples),
            'tz': 'America/Chicago',
            'series': series,
        }

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

    
    @app.get("/api/usage/tokens")
    def usage_tokens(hours: int = Query(24, ge=1, le=168)) -> dict[str, Any]:
        now_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        summary = fetch_tokens_summary(db_path, now_ts_utc=now_utc)
        series = fetch_tokens_hourly_series(db_path, now_ts_utc=now_utc, hours=int(hours))
        # Convert hour bucket timestamps to Central for display
        for r in series:
            r["hour_bucket"] = _to_central_iso(r.get("hour_bucket"))
        return {"summary": summary, "series": series, "tz": "America/Chicago", "note": "Estimated from chars (DeepSeek endpoint doesn't return usage)"}
    
    @app.post("/api/admin/reset_all")
    def reset_all(body: dict[str, Any]) -> dict[str, Any]:
        # Gate
        if os.getenv("RESET_ENABLED", "").strip().lower() not in {"1", "true", "yes", "on"}:
            raise HTTPException(status_code=403, detail="RESET_ENABLED is false")

        # Must be paused
        overrides = load_overrides_file(overrides_path)
        effective = apply_overrides(cfg, overrides)
        if not bool(getattr(effective, "pause_processing", False)):
            raise HTTPException(status_code=409, detail="Reset requires PAUSE_PROCESSING=true")

        confirm = (body or {}).get("confirm")
        if confirm != "RESET ALL DATA":
            raise HTTPException(status_code=400, detail="Typed confirmation required: RESET ALL DATA")

        # Path safety
        root = _safe_data_root(data_root)

        lg = None
        try:
            from options_ai.utils.logger import get_logger
            lg = get_logger()
        except Exception:
            lg = None

        if lg:
            lg.warning(component="Admin", event="reset_started", message="full reset started", file_key="system", data_root=str(root))

        # DB truncate
        db_counts: dict[str, int] = {}
        with _connect(db_path) as con:
            con.execute("PRAGMA foreign_keys=OFF")
            for tbl in ("predictions", "performance_summary", "system_events", "model_usage"):
                try:
                    n = int(con.execute(f"SELECT COUNT(1) AS n FROM {tbl}").fetchone()["n"])
                    db_counts[tbl] = n
                    con.execute(f"DELETE FROM {tbl}")
                except Exception:
                    db_counts[tbl] = -1
            con.commit()

        # VACUUM
        vacuum_ran = False
        try:
            con2 = sqlite3.connect(db_path, timeout=30.0)
            con2.execute("VACUUM")
            con2.close()
            vacuum_ran = True
        except Exception as e:
            if lg:
                lg.error(component="Admin", event="reset_vacuum_failed", message="VACUUM failed", file_key="system", error=str(e))

        # Filesystem wipe
        errors: list[str] = []
        deleted = 0

        # state
        for rel in (
            "state/seen_files.json",
            "state/current_task.json",
            "state/bootstrap_checkpoint.json",
            "state/bootstrap_completed.json",
            "state/runtime_overrides.json",
        ):
            deleted += _wipe_path(root, rel, errors)

        # logs
        deleted += _wipe_path(root, "logs", errors)

        # cache
        deleted += _wipe_path(root, "cache/derived", errors)
        deleted += _wipe_path(root, "cache/model", errors)

        # processed + quarantine + incoming + historical
        deleted += _wipe_path(root, f"processed/{cfg.ticker}/snapshots", errors)
        deleted += _wipe_path(root, f"processed/{cfg.ticker}/charts", errors)
        deleted += _wipe_path(root, "quarantine/invalid_filenames", errors)
        deleted += _wipe_path(root, "quarantine/invalid_json", errors)
        deleted += _wipe_path(root, f"incoming/{cfg.ticker}", errors)
        deleted += _wipe_path(root, f"historical/{cfg.ticker}", errors)

        # ML artifacts (under DATA_ROOT by default)
        deleted += _wipe_path(root, "models", errors)

        if lg:
            lg.warning(component="Admin", event="reset_completed", message="full reset completed", file_key="system", data_root=str(root), deleted_files=int(deleted), errors=int(len(errors)))

        return {
            "ok": True,
            "db": db_counts,
            "files": {"deleted": int(deleted), "errors": int(len(errors)), "error_list": errors[:50]},
            "vacuum": {"ran": vacuum_ran},
            "data_root": str(root),
        }

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
