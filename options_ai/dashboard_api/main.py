from __future__ import annotations

import os
import sqlite3
from dataclasses import asdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

try:
    import psycopg
except Exception:  # pragma: no cover
    psycopg = None  # type: ignore

from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import HTMLResponse
from fastapi import Response
from fastapi.staticfiles import StaticFiles

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
from options_ai.backtest.debit_spreads import DebitBacktestConfig, run_backtest_debit_spreads


CENTRAL_TZ = ZoneInfo("America/Chicago")

def _pg_dsn() -> str | None:
    dsn = os.getenv("SPX_CHAIN_DATABASE_URL", "").strip()
    return dsn or None


def _pg_connect(dsn: str):
    if psycopg is None:
        raise HTTPException(status_code=500, detail="psycopg not installed on server")
    try:
        return psycopg.connect(dsn)
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"timescale connect failed: {e}")


def _anchor_policy_sets(policy_in: str | None = None) -> tuple[str, list[str] | None, list[str] | None]:
    """Directional anchor policy.

    Returns: (policy_name, call_allowed_anchors, put_allowed_anchors)

    Policies:
    - any: no restriction
    - opposite_wall: CALL spreads anchored at PUT_WALL/MAGNET/ATM; PUT spreads anchored at CALL_WALL/MAGNET/ATM
    - same_wall: CALL spreads anchored at CALL_WALL/MAGNET/ATM; PUT spreads anchored at PUT_WALL/MAGNET/ATM
    """
    policy = (policy_in or os.getenv('DEBIT_ANCHOR_POLICY', 'any')).strip().lower()
    if policy in {'', 'any'}:
        return 'any', None, None
    if policy == 'opposite_wall':
        return policy, ['PUT_WALL', 'MAGNET', 'ATM'], ['CALL_WALL', 'MAGNET', 'ATM']
    if policy == 'same_wall':
        return policy, ['CALL_WALL', 'MAGNET', 'ATM'], ['PUT_WALL', 'MAGNET', 'ATM']
    # fail closed to "any" but report policy
    return 'any', None, None




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


    # Backtest presets + run history (Option C)
    def _now_utc_iso() -> str:
        return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    def _ensure_backtest_tables() -> None:
        with _connect(db_path) as con:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS backtest_presets (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  strategy_key TEXT NOT NULL,
                  name TEXT NOT NULL,
                  params_json TEXT NOT NULL,
                  schema_version INTEGER NOT NULL DEFAULT 1,
                  created_at_utc TEXT NOT NULL,
                  updated_at_utc TEXT NOT NULL,
                  last_run_id INTEGER NULL,
                  last_run_at_utc TEXT NULL,
                  last_summary_json TEXT NULL,
                  UNIQUE(strategy_key, name)
                );
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS backtest_runs (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  strategy_key TEXT NOT NULL,
                  created_at_utc TEXT NOT NULL,
                  preset_id INTEGER NULL,
                  preset_name_at_run TEXT NULL,
                  params_json TEXT NOT NULL,
                  summary_json TEXT NOT NULL
                );
                """
            )
            con.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_backtest_runs_strategy_created
                ON backtest_runs(strategy_key, created_at_utc DESC);
                """
            )
            con.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_backtest_runs_preset_created
                ON backtest_runs(preset_id, created_at_utc DESC);
                """
            )
            con.commit()

    def _validate_preset_name(name: str) -> str:
        nm = (name or '').strip()
        if not nm:
            raise HTTPException(status_code=400, detail='name required')
        if len(nm) > 60:
            raise HTTPException(status_code=400, detail='name too long (max 60)')
        return nm

    _ensure_backtest_tables()

    app = FastAPI(title="Nate's Option Dashboard API", version="0.1")

    # Static assets for the UI (logos, etc.)
    static_dir = Path(__file__).with_name("static")
    static_dir.mkdir(parents=True, exist_ok=True)
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    @app.get("/", response_class=HTMLResponse)
    def index(response: Response) -> str:
        # Prevent stale UI JS/HTML from being cached between rapid deployments
        response.headers["Cache-Control"] = "no-store, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
        html_path = Path(__file__).with_name("ui.html")
        return html_path.read_text(encoding="utf-8")

    @app.get("/api/health")
    def health() -> dict[str, Any]:
        # light-weight status endpoint for UI app bar
        reset_enabled = os.getenv("RESET_ENABLED", "").strip().lower() in {"1", "true", "yes", "on"}
        overrides = load_overrides_file(overrides_path)
        effective = apply_overrides(cfg, overrides)
        paused = bool(getattr(effective, "pause_processing", False))
        return {
            "ok": True,
            "time": _now_central_iso(),
            "tz": "America/Chicago",
            "reset_enabled": reset_enabled,
            "paused": paused,
            "service": "options_ai_dashboard_api",
        }

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
    
    def _series_buckets_query(*, provider_filter_sql: str, now_utc: str, window_days: int, bucket_minutes: int) -> tuple[str, tuple[Any, ...]]:
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
        return sql, params

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
            now_utc=now_utc,
            window_days=int(window_days),
            bucket_minutes=int(bucket_minutes),
        )
        with _connect(db_path) as con:
            rows = con.execute(sql, _params).fetchall()

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
            now_utc=now_utc,
            window_days=int(window_days),
            bucket_minutes=int(bucket_minutes),
        )
        with _connect(db_path) as con:
            rows = con.execute(sql, _params).fetchall()

        series = [_postprocess_bucket_row(r, min_samples=int(min_samples), include_action=True) for r in rows]

        return {
            'window_days': int(window_days),
            'bucket_minutes': int(bucket_minutes),
            'min_samples': int(min_samples),
            'tz': 'America/Chicago',
            'series': series,
        }

    
    @app.get("/api/ml_eod/metrics/daily")
    def ml_eod_metrics_daily(
        days: int = Query(30, ge=1, le=365),
        variant: str = Query("lvl0", pattern="^(lvl0|lvl1)$"),
    ) -> dict[str, Any]:
        # model_version selection
        mv = cfg.eod_model_version_lvl0 if variant == 'lvl0' else cfg.eod_model_version_lvl1
        with _connect(db_path) as con:
            rows = con.execute(
                """
                SELECT trade_day, pred_dir, label_dir
                FROM eod_predictions
                WHERE label_dir IS NOT NULL AND model_version = ?
                ORDER BY trade_day DESC
                LIMIT ?
                """,
                (mv, int(days)),
            ).fetchall()

        by_day: dict[str, list[sqlite3.Row]] = {}
        for r in rows:
            by_day.setdefault(r['trade_day'], []).append(r)

        series = []
        for day in sorted(by_day.keys(), reverse=True):
            rs = by_day[day]
            total = len(rs)
            actionable_total = sum(1 for r in rs if (r['pred_dir'] or '') != 'neutral')
            correct = sum(1 for r in rs if (r['pred_dir'] or '') == (r['label_dir'] or ''))
            actionable_correct = sum(1 for r in rs if (r['pred_dir'] or '') != 'neutral' and (r['pred_dir'] or '') == (r['label_dir'] or ''))
            series.append({
                'day': day,
                'total_scored': total,
                'actionable_total': actionable_total,
                'action_rate': (actionable_total/total) if total else None,
                'overall_accuracy': (correct/total) if total else None,
                'accuracy_actionable': (actionable_correct/actionable_total) if actionable_total else None,
            })

        return {'days': int(days), 'variant': variant, 'model_version': mv, 'series': series, 'tz': 'America/Chicago'}

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

    


    # ---- Backtest Presets (server-persisted) ----

    @app.get('/api/backtest/presets')
    def backtest_presets_list(strategy_key: str = Query(...)) -> dict[str, Any]:
        with _connect(db_path) as con:
            rows = con.execute(
                """
                SELECT id, strategy_key, name, params_json, schema_version,
                       created_at_utc, updated_at_utc,
                       last_run_id, last_run_at_utc, last_summary_json
                FROM backtest_presets
                WHERE strategy_key = ?
                ORDER BY updated_at_utc DESC
                """,
                (strategy_key,),
            ).fetchall()

        items = []
        import json as _json
        for r in rows:
            params = None
            last_summary = None
            try:
                params = _json.loads(r['params_json'])
            except Exception:
                params = None
            try:
                last_summary = _json.loads(r['last_summary_json']) if r['last_summary_json'] else None
            except Exception:
                last_summary = None

            items.append({
                'id': int(r['id']),
                'strategy_key': r['strategy_key'],
                'name': r['name'],
                'params': params,
                'schema_version': int(r['schema_version'] or 1),
                'created_at_utc': r['created_at_utc'],
                'updated_at_utc': r['updated_at_utc'],
                'last_run_id': r['last_run_id'],
                'last_run_at_utc': r['last_run_at_utc'],
                'last_summary': last_summary,
            })

        return {'strategy_key': strategy_key, 'items': items}

    @app.post('/api/backtest/presets')
    def backtest_presets_create(body: dict[str, Any]) -> dict[str, Any]:
        import json as _json
        strategy_key = str((body or {}).get('strategy_key') or '').strip()
        if not strategy_key:
            raise HTTPException(status_code=400, detail='strategy_key required')
        name = _validate_preset_name(str((body or {}).get('name') or ''))
        params = (body or {}).get('params')
        if not isinstance(params, dict):
            raise HTTPException(status_code=400, detail='params must be an object')

        now = _now_utc_iso()
        params_json = _json.dumps(params, separators=(',', ':'), sort_keys=True)

        with _connect(db_path) as con:
            try:
                con.execute(
                    """
                    INSERT INTO backtest_presets(strategy_key, name, params_json, schema_version, created_at_utc, updated_at_utc)
                    VALUES(?, ?, ?, 1, ?, ?)
                    """,
                    (strategy_key, name, params_json, now, now),
                )
                con.commit()
            except sqlite3.IntegrityError:
                raise HTTPException(status_code=409, detail='preset name already exists for this strategy')

        return backtest_presets_list(strategy_key=strategy_key)

    @app.put('/api/backtest/presets/{preset_id}')
    def backtest_presets_update(preset_id: int, body: dict[str, Any]) -> dict[str, Any]:
        import json as _json
        now = _now_utc_iso()
        new_name = body.get('name') if isinstance(body, dict) else None
        new_params = body.get('params') if isinstance(body, dict) else None

        sets = []
        params: list[Any] = []
        if new_name is not None:
            nm = _validate_preset_name(str(new_name))
            sets.append('name = ?')
            params.append(nm)
        if new_params is not None:
            if not isinstance(new_params, dict):
                raise HTTPException(status_code=400, detail='params must be an object')
            sets.append('params_json = ?')
            params.append(_json.dumps(new_params, separators=(',', ':'), sort_keys=True))

        if not sets:
            raise HTTPException(status_code=400, detail='nothing to update')

        sets.append('updated_at_utc = ?')
        params.append(now)
        params.append(int(preset_id))

        with _connect(db_path) as con:
            r = con.execute('SELECT strategy_key FROM backtest_presets WHERE id=?', (int(preset_id),)).fetchone()
            if not r:
                raise HTTPException(status_code=404, detail='preset not found')
            strategy_key = r['strategy_key']

            try:
                con.execute(f"UPDATE backtest_presets SET {', '.join(sets)} WHERE id = ?", tuple(params))
                con.commit()
            except sqlite3.IntegrityError:
                raise HTTPException(status_code=409, detail='preset name already exists for this strategy')

        return backtest_presets_list(strategy_key=strategy_key)

    @app.delete('/api/backtest/presets/{preset_id}')
    def backtest_presets_delete(preset_id: int) -> dict[str, Any]:
        with _connect(db_path) as con:
            r = con.execute('SELECT strategy_key FROM backtest_presets WHERE id=?', (int(preset_id),)).fetchone()
            if not r:
                raise HTTPException(status_code=404, detail='preset not found')
            strategy_key = r['strategy_key']
            con.execute('DELETE FROM backtest_presets WHERE id=?', (int(preset_id),))
            con.commit()
        return backtest_presets_list(strategy_key=strategy_key)


    # ---- Backtest Runs (history grid) ----

    @app.get('/api/backtest/runs')
    def backtest_runs_list(
        strategy_key: str = Query(...),
        preset_id: int | None = Query(None),
        limit: int = Query(200, ge=1, le=2000),
    ) -> dict[str, Any]:
        import json as _json
        sql = """
            SELECT id, strategy_key, created_at_utc, preset_id, preset_name_at_run, params_json, summary_json
            FROM backtest_runs
            WHERE strategy_key = ?
        """
        params: list[Any] = [strategy_key]
        if preset_id is not None:
            sql += " AND preset_id = ?"
            params.append(int(preset_id))
        sql += " ORDER BY created_at_utc DESC LIMIT ?"
        params.append(int(limit))

        with _connect(db_path) as con:
            rows = con.execute(sql, tuple(params)).fetchall()

        items = []
        for r in rows:
            try:
                params_obj = _json.loads(r['params_json'])
            except Exception:
                params_obj = None
            try:
                summary_obj = _json.loads(r['summary_json'])
            except Exception:
                summary_obj = None
            items.append({
                'id': int(r['id']),
                'created_at_utc': r['created_at_utc'],
                'preset_id': r['preset_id'],
                'preset_name_at_run': r['preset_name_at_run'],
                'params': params_obj,
                'summary': summary_obj,
            })

        return {'strategy_key': strategy_key, 'preset_id': preset_id, 'limit': int(limit), 'items': items}

    @app.delete('/api/backtest/runs/{run_id}')
    def backtest_runs_delete(run_id: int) -> dict[str, Any]:
        with _connect(db_path) as con:
            r = con.execute('SELECT strategy_key FROM backtest_runs WHERE id=?', (int(run_id),)).fetchone()
            if not r:
                raise HTTPException(status_code=404, detail='run not found')
            strategy_key = r['strategy_key']
            con.execute('DELETE FROM backtest_runs WHERE id=?', (int(run_id),))
            con.commit()
        return {'ok': True, 'deleted': int(run_id), 'strategy_key': strategy_key}

    @app.post("/api/backtest/debit_spreads/run")
    def backtest_debit_spreads_run(payload: dict[str, Any]) -> dict[str, Any]:
        """Run a Timescale-backed backtest for debit spreads (0DTE default; optional target DTE)."""
        dsn = _pg_dsn()
        if not dsn:
            raise HTTPException(status_code=503, detail="SPX_CHAIN_DATABASE_URL not configured")

        try:
            start_day = date.fromisoformat(str(payload.get("start_day")))
            end_day = date.fromisoformat(str(payload.get("end_day")))
        except Exception:
            raise HTTPException(status_code=400, detail="start_day/end_day required as YYYY-MM-DD")

        cfg = DebitBacktestConfig(
            start_day=start_day,
            end_day=end_day,
            expiration_mode=str(payload.get("expiration_mode", "0dte")),
            target_dte_days=(
                int(payload.get("target_dte_days"))
                if payload.get("target_dte_days") not in (None, "")
                else None
            ),
            dte_tolerance_days=int(payload.get("dte_tolerance_days", 2)),
            horizon_minutes=int(payload.get("horizon_minutes", 30)),
            entry_mode=str(payload.get("entry_mode", "time_range")),
            session_start_ct=str(payload.get("session_start_ct", "08:30")),
            entry_first_n_minutes=int(payload.get("entry_first_n_minutes", 60)),
            entry_start_ct=str(payload.get("entry_start_ct", "08:40")),
            entry_end_ct=str(payload.get("entry_end_ct", "09:30")),
            max_trades_per_day=int(payload.get("max_trades_per_day", 1)),
            one_trade_at_a_time=bool(payload.get("one_trade_at_a_time", True)),
            anchor_mode=str(payload.get("anchor_mode", "ATM")),
            anchor_policy=str(payload.get("anchor_policy", os.getenv("DEBIT_ANCHOR_POLICY", "any"))),
            min_p_bigwin=float(payload.get("min_p_bigwin", 0.0)),
            min_pred_change=float(payload.get("min_pred_change", 0.0)),
            strategy_mode=str(payload.get("strategy_mode", "anchor_based")),
            enable_pw_trade=bool(payload.get("enable_pw_trade", True)),
            enable_cw_trade=bool(payload.get("enable_cw_trade", True)),
            long_leg_moneyness=str(payload.get("long_leg_moneyness", "ATM")),
            max_width_points=float(payload.get("max_width_points", 25)),
            min_width_points=float(payload.get("min_width_points", 5)),
            proximity_min_points=float(payload.get("proximity_min_points", 0)),
            proximity_max_points=float(payload.get("proximity_max_points", 30)),
            rotation_filter=str(payload.get("rotation_filter", "spot_delta_5m")),
            prefer_pw_on_tie=bool(payload.get("prefer_pw_on_tie", True)),
            short_put_offset_steps=int(payload.get("short_put_offset_steps", 0)),
            short_call_offset_steps=int(payload.get("short_call_offset_steps", 0)),
            allowed_spreads=tuple(payload.get("allowed_spreads", ["CALL", "PUT"])),
            max_debit_points=float(payload.get("max_debit_points", 5.0)),
            stop_loss_pct=float(payload.get("stop_loss_pct", 0.50)),
            take_profit_pct=float(payload.get("take_profit_pct", 2.00)),
            max_future_lookahead_minutes=int(payload.get("max_future_lookahead_minutes", 120)),
            price_mode=str(payload.get("price_mode", "mid")),
            tz_local=str(payload.get("tz_local", "America/Chicago")),
            include_missing_exits=bool(payload.get("include_missing_exits", False)),
        )

        with _pg_connect(dsn) as conn:
            result = run_backtest_debit_spreads(conn, cfg)

        # Persist run summary in SQLite for history/compare grid
        import json as _json
        now = _now_utc_iso()
        strategy_mode = str(payload.get('strategy_mode', 'anchor_based'))

        exp_mode = str(payload.get('expiration_mode', '0dte') or '0dte').strip().lower()
        if exp_mode == '0dte':
            exp_key = 'exp0dte'
        else:
            try:
                td = int(payload.get('target_dte_days') or 7)
            except Exception:
                td = 7
            try:
                tol = int(payload.get('dte_tolerance_days') or 2)
            except Exception:
                tol = 2
            exp_key = f'dte{td}t{tol}'

        strategy_key = f"debit_spreads:{strategy_mode}:{exp_key}"

        preset_id_in = payload.get('preset_id', None)
        preset_id_final: int | None = None
        preset_name_at_run: str | None = None

        # Store "pure" params (omit preset_id)
        params_payload = dict(payload or {})
        params_payload.pop('preset_id', None)

        summary = (result or {}).get('summary') or {}

        with _connect(db_path) as con:
            if preset_id_in is not None:
                try:
                    pid = int(preset_id_in)
                    row = con.execute('SELECT id, name FROM backtest_presets WHERE id=?', (pid,)).fetchone()
                    if row:
                        preset_id_final = int(row['id'])
                        preset_name_at_run = str(row['name'])
                except Exception:
                    preset_id_final = None
                    preset_name_at_run = None

            cur = con.execute(
                """
                INSERT INTO backtest_runs(strategy_key, created_at_utc, preset_id, preset_name_at_run, params_json, summary_json)
                VALUES(?, ?, ?, ?, ?, ?)
                """,
                (
                    strategy_key,
                    now,
                    preset_id_final,
                    preset_name_at_run,
                    _json.dumps(params_payload, separators=(',', ':'), sort_keys=True),
                    _json.dumps(summary, separators=(',', ':'), sort_keys=True),
                ),
            )
            run_id = int(cur.lastrowid)

            if preset_id_final is not None:
                con.execute(
                    """
                    UPDATE backtest_presets
                    SET last_run_id=?, last_run_at_utc=?, last_summary_json=?, updated_at_utc=?
                    WHERE id=?
                    """,
                    (
                        run_id,
                        now,
                        _json.dumps(summary, separators=(',', ':'), sort_keys=True),
                        now,
                        preset_id_final,
                    ),
                )
            con.commit()

        # Keep existing shape; add identifiers so UI can link/refresh
        if isinstance(result, dict):
            result['run_id'] = run_id
            result['preset_id'] = preset_id_final
        return result

    @app.get("/api/debit_spreads/top")
    def debit_spreads_top(limit: int = Query(12, ge=1, le=100)) -> dict[str, Any]:
        dsn = _pg_dsn()
        if not dsn:
            raise HTTPException(status_code=503, detail="SPX_CHAIN_DATABASE_URL not configured")


        anchor_policy, call_anchors, put_anchors = _anchor_policy_sets()

        with _pg_connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT max(snapshot_ts) FROM spx.debit_spread_candidates_0dte")
                r = cur.fetchone()
                latest = r[0] if r else None
                if latest is None:
                    return {"snapshot_ts": None, "levels": None, "candidates": [], "tz": "America/Chicago"}

                cur.execute("SELECT atm_strike, spot, expiration_date FROM spx.chain_features_0dte WHERE snapshot_ts=%s", (latest,))
                feat = cur.fetchone()
                atm_strike = float(feat[0]) if feat and feat[0] is not None else None
                spot = float(feat[1]) if feat and feat[1] is not None else None
                exp_date = feat[2] if feat and feat[2] is not None else None

                cur.execute("SELECT call_wall, put_wall, magnet FROM spx.gex_levels_0dte WHERE snapshot_ts=%s", (latest,))
                lev = cur.fetchone()
                levels = None
                if lev:
                    levels = {"atm": atm_strike, "call_wall": float(lev[0]) if lev[0] is not None else None, "put_wall": float(lev[1]) if lev[1] is not None else None, "magnet": float(lev[2]) if lev[2] is not None else None, "spot": spot, "expiration_date": str(exp_date) if exp_date is not None else None}
                else:
                    levels = {"atm": atm_strike, "call_wall": None, "put_wall": None, "magnet": None, "spot": spot, "expiration_date": str(exp_date) if exp_date is not None else None}

                # Join candidates with 30m labels if present; rank by label change desc when available.
                cur.execute(
                    """
                    SELECT
                      c.anchor_type, c.spread_type, c.anchor_strike,
                      c.k_long, c.k_short, c.debit_points,
                      c.long_symbol, c.short_symbol,
                      l.horizon_minutes, l.change, l.is_missing_future,
                      s.pred_change, s.p_bigwin
                    FROM spx.debit_spread_candidates_0dte c
                    LEFT JOIN spx.debit_spread_labels_0dte l
                      ON l.snapshot_ts = c.snapshot_ts
                     AND l.anchor_type = c.anchor_type
                     AND l.spread_type = c.spread_type
                     AND l.horizon_minutes = 30
                    LEFT JOIN spx.debit_spread_scores_0dte s
                      ON s.snapshot_ts = c.snapshot_ts
                     AND s.anchor_type = c.anchor_type
                     AND s.spread_type = c.spread_type
                     AND s.horizon_minutes = 30
                    WHERE c.snapshot_ts = %s
                      AND c.tradable = true
                      AND (
                        (%s::text[] IS NULL AND %s::text[] IS NULL)
                        OR (c.spread_type='CALL' AND c.anchor_type = ANY(%s::text[]))
                        OR (c.spread_type='PUT' AND c.anchor_type = ANY(%s::text[]))
                      )
                    ORDER BY
                      CASE WHEN s.p_bigwin IS NULL THEN 1 ELSE 0 END ASC,
                      s.p_bigwin DESC NULLS LAST,
                      CASE WHEN s.pred_change IS NULL THEN 1 ELSE 0 END ASC,
                      s.pred_change DESC NULLS LAST,
                      CASE WHEN l.change IS NULL THEN 1 ELSE 0 END ASC,
                      l.change DESC NULLS LAST,
                      c.debit_points ASC NULLS LAST
                    LIMIT %s
                    """,
                    (latest, call_anchors, put_anchors, call_anchors, put_anchors, int(limit)),
                )
                items = []
                for rr in cur.fetchall():
                    items.append({
                        "anchor_type": rr[0],
                        "spread_type": rr[1],
                        "anchor_strike": float(rr[2]) if rr[2] is not None else None,
                        "k_long": float(rr[3]) if rr[3] is not None else None,
                        "k_short": float(rr[4]) if rr[4] is not None else None,
                        "debit_points": float(rr[5]) if rr[5] is not None else None,
                        "long_symbol": rr[6],
                        "short_symbol": rr[7],
                        "horizon_minutes": int(rr[8]) if rr[8] is not None else 30,
                        "change": float(rr[9]) if rr[9] is not None else None,
                        "is_missing_future": bool(rr[10]) if rr[10] is not None else None,
                        "pred_change": float(rr[11]) if rr[11] is not None else None,
                        "p_bigwin": float(rr[12]) if rr[12] is not None else None,
                    })

        return {
            "snapshot_ts": _to_central_iso(latest),
            "levels": levels,
            "candidates": items,
            "tz": "America/Chicago",
        }


    

    @app.get("/api/debit_spreads/daily_pick")
    def debit_spreads_daily_pick(
        day_local: str | None = Query(None, description="YYYY-MM-DD in America/Chicago"),
        window_minutes: int = Query(int(os.getenv("DAILY_PICK_WINDOW_MINUTES", "30")), ge=5, le=180),
        session_start: str = Query(os.getenv("DAILY_PICK_SESSION_START_CT", "08:30"), description="CT time HH:MM"),
        min_p_bigwin: float = Query(float(os.getenv("DAILY_PICK_MIN_P_BIGWIN", "0.0")), ge=0.0, le=1.0),
        min_pred_change: float = Query(float(os.getenv("DAILY_PICK_MIN_PRED_CHANGE", "0.0"))),
        allowed_anchors: str | None = Query(os.getenv("DAILY_PICK_ALLOWED_ANCHORS", "" ) or None, description="comma list e.g. ATM,CALL_WALL,PUT_WALL,MAGNET"),
        allowed_spreads: str | None = Query(os.getenv("DAILY_PICK_ALLOWED_SPREAD_TYPES", "") or None, description="comma list e.g. CALL,PUT"),
    ) -> dict[str, Any]:
        """Pick a single best trade per day from the first N minutes of the session.

        Objective #2: rank by p_bigwin desc, pred_change desc, debit asc, and require pred_change > 0.
        """
        dsn = _pg_dsn()
        if not dsn:
            raise HTTPException(status_code=503, detail="SPX_CHAIN_DATABASE_URL not configured")

        # Determine day in CT
        if day_local:
            try:
                # validate format
                _ = datetime.fromisoformat(day_local)
            except Exception:
                raise HTTPException(status_code=400, detail="day_local must be YYYY-MM-DD")
            day_ct = day_local
        else:
            day_ct = datetime.now(tz=CENTRAL_TZ).date().isoformat()

        # Compute CT time window
        try:
            hh, mm = session_start.strip().split(":", 1)
            start_h = int(hh)
            start_m = int(mm)
        except Exception:
            raise HTTPException(status_code=400, detail="session_start must be HH:MM")

        start_time = f"{start_h:02d}:{start_m:02d}:00"
        # end time within same day
        dt0 = datetime(2000, 1, 1, start_h, start_m, 0)
        dt1 = dt0 + timedelta(minutes=int(window_minutes))
        end_time = dt1.time().strftime("%H:%M:%S")

        # Convert CT day + window into a UTC timestamp range so Postgres can use snapshot_ts indexes
        try:
            y, m, d = (int(x) for x in day_ct.split("-", 2))
        except Exception:
            raise HTTPException(status_code=400, detail="day_local must be YYYY-MM-DD")
        dt_start_ct = datetime(y, m, d, start_h, start_m, 0, tzinfo=CENTRAL_TZ)
        dt_end_ct = dt_start_ct + timedelta(minutes=int(window_minutes))
        dt_start_utc = dt_start_ct.astimezone(timezone.utc)
        dt_end_utc = dt_end_ct.astimezone(timezone.utc)

        anchors = None
        if allowed_anchors:
            anchors = [a.strip().upper() for a in allowed_anchors.split(",") if a.strip()]
        spreads = None
        if allowed_spreads:
            spreads = [s.strip().upper() for s in allowed_spreads.split(",") if s.strip()]

        anchor_policy, call_anchors, put_anchors = _anchor_policy_sets()

        with _pg_connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    WITH eligible AS (
                      SELECT
                        c.snapshot_ts,
                        c.anchor_type,
                        c.spread_type,
                        c.anchor_strike,
                        c.k_long,
                        c.k_short,
                        c.debit_points,
                        c.long_symbol,
                        c.short_symbol,
                        s.pred_change,
                        s.p_bigwin
                      FROM spx.debit_spread_candidates_0dte c
                      JOIN spx.chain_features_0dte f
                        ON f.snapshot_ts = c.snapshot_ts
                      JOIN spx.debit_spread_scores_0dte s
                        ON s.snapshot_ts = c.snapshot_ts
                       AND s.horizon_minutes = 30
                       AND s.anchor_type = c.anchor_type
                       AND s.spread_type = c.spread_type
                      WHERE c.tradable = true
                        AND f.low_quality = false
                        AND c.snapshot_ts >= %s
                        AND c.snapshot_ts < %s
                        AND s.p_bigwin IS NOT NULL
                        AND s.pred_change IS NOT NULL
                        AND s.p_bigwin >= %s
                        AND s.pred_change > 0
                        AND s.pred_change >= %s
                        AND (
                          (%s::text[] IS NULL AND %s::text[] IS NULL)
                          OR (c.spread_type='CALL' AND c.anchor_type = ANY(%s::text[]))
                          OR (c.spread_type='PUT' AND c.anchor_type = ANY(%s::text[]))
                        )
                    ),
                    ranked_per_snapshot AS (
                      SELECT
                        *,
                        ROW_NUMBER() OVER (
                          PARTITION BY snapshot_ts
                          ORDER BY
                            p_bigwin DESC NULLS LAST,
                            pred_change DESC NULLS LAST,
                            debit_points ASC NULLS LAST
                        ) AS rn_snap
                      FROM eligible
                      WHERE (%s::text[] IS NULL OR anchor_type = ANY(%s::text[]))
                        AND (%s::text[] IS NULL OR spread_type = ANY(%s::text[]))
                    ),
                    ranked_day AS (
                      SELECT
                        *,
                        ROW_NUMBER() OVER (
                          ORDER BY
                            p_bigwin DESC NULLS LAST,
                            pred_change DESC NULLS LAST,
                            debit_points ASC NULLS LAST,
                            snapshot_ts ASC
                        ) AS rn_day
                      FROM ranked_per_snapshot
                      WHERE rn_snap = 1
                    )
                    SELECT
                      snapshot_ts,
                      anchor_type,
                      spread_type,
                      anchor_strike,
                      k_long,
                      k_short,
                      debit_points,
                      long_symbol,
                      short_symbol,
                      pred_change,
                      p_bigwin
                    FROM ranked_day
                    WHERE rn_day = 1
                    LIMIT 1
""",
                    (
                        dt_start_utc,
                        dt_end_utc,
                        float(min_p_bigwin),
                        float(min_pred_change),
                        call_anchors,
                        put_anchors,
                        call_anchors,
                        put_anchors,
                        anchors,
                        anchors,
                        spreads,
                        spreads,
                    ),
                )
                r = cur.fetchone()

        pick = None
        if r:
            pick = {
                "snapshot_ts": _to_central_iso(r[0]),
                "anchor_type": r[1],
                "spread_type": r[2],
                "anchor_strike": float(r[3]) if r[3] is not None else None,
                "k_long": float(r[4]) if r[4] is not None else None,
                "k_short": float(r[5]) if r[5] is not None else None,
                "debit_points": float(r[6]) if r[6] is not None else None,
                "long_symbol": r[7],
                "short_symbol": r[8],
                "pred_change": float(r[9]) if r[9] is not None else None,
                "p_bigwin": float(r[10]) if r[10] is not None else None,
            }

        return {
            "day_local": day_ct,
            "session_start": start_time,
            "window_end": end_time,
            "criteria": {
                "objective": "p_bigwin desc, pred_change desc, debit asc",
                "require_pred_positive": True,
                "min_p_bigwin": float(min_p_bigwin),
                "min_pred_change": float(min_pred_change),
                "allowed_anchors": anchors,
                "allowed_spreads": spreads,
            },
            "pick": pick,
            "tz": "America/Chicago",
        }

    @app.get("/api/debit_spreads/history")
    def debit_spreads_history(
        limit: int = Query(100, ge=1, le=500),
        horizon_minutes: int = Query(30, ge=5, le=120),
        only_recommended: bool = Query(False),
    ) -> dict[str, Any]:
        """Historical realized debit spread outcomes.

        If only_recommended=true, returns **one** candidate per snapshot_ts (the "trade I'd take")
        based on current ranking: p_bigwin desc, pred_change desc, debit_points asc.
        """
        dsn = _pg_dsn()
        if not dsn:
            raise HTTPException(status_code=503, detail="SPX_CHAIN_DATABASE_URL not configured")

        mult_atm = float(os.getenv("DEBIT_BIGWIN_MULT_ATM", "2.0"))
        mult_wall = float(os.getenv("DEBIT_BIGWIN_MULT_WALL", "4.0"))

        anchor_policy, call_anchors, put_anchors = _anchor_policy_sets()

        with _pg_connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    WITH ranked AS (
                      SELECT
                        l.snapshot_ts,
                        l.anchor_type,
                        l.spread_type,
                        l.horizon_minutes,
                        l.debit_t,
                        l.debit_tH,
                        l.change,
                        c.debit_points,
                        c.k_long,
                        c.k_short,
                        s.pred_change,
                        s.p_bigwin,
                        ROW_NUMBER() OVER (
                          PARTITION BY l.snapshot_ts
                          ORDER BY
                            CASE WHEN s.p_bigwin IS NULL THEN 1 ELSE 0 END ASC,
                            s.p_bigwin DESC NULLS LAST,
                            CASE WHEN s.pred_change IS NULL THEN 1 ELSE 0 END ASC,
                            s.pred_change DESC NULLS LAST,
                            c.debit_points ASC NULLS LAST
                        ) AS rn
                      FROM spx.debit_spread_labels_0dte l
                      JOIN spx.debit_spread_candidates_0dte c
                        ON c.snapshot_ts = l.snapshot_ts
                       AND c.anchor_type = l.anchor_type
                       AND c.spread_type = l.spread_type
                      LEFT JOIN spx.debit_spread_scores_0dte s
                        ON s.snapshot_ts = l.snapshot_ts
                       AND s.anchor_type = l.anchor_type
                       AND s.spread_type = l.spread_type
                       AND s.horizon_minutes = l.horizon_minutes
                      WHERE l.horizon_minutes = %s
                        AND l.is_missing_future = false
                        AND c.tradable = true
                        AND (
                          (%s::text[] IS NULL AND %s::text[] IS NULL)
                          OR (c.spread_type='CALL' AND c.anchor_type = ANY(%s::text[]))
                          OR (c.spread_type='PUT' AND c.anchor_type = ANY(%s::text[]))
                        )
                    )
                    SELECT
                      snapshot_ts,
                      anchor_type,
                      spread_type,
                      horizon_minutes,
                      debit_t,
                      debit_tH,
                      change,
                      debit_points,
                      k_long,
                      k_short,
                      pred_change,
                      p_bigwin
                    FROM ranked
                    WHERE (NOT %s) OR rn = 1
                    ORDER BY snapshot_ts DESC
                    LIMIT %s
                    """,
                    (int(horizon_minutes), call_anchors, put_anchors, call_anchors, put_anchors, bool(only_recommended), int(limit)),
                )

                items = []
                for r in cur.fetchall():
                    snapshot_ts = r[0]
                    anchor_type = str(r[1])
                    spread_type = str(r[2])
                    debit_t = float(r[4]) if r[4] is not None else None
                    debit_tH = float(r[5]) if r[5] is not None else None
                    change = float(r[6]) if r[6] is not None else None

                    req_mult = mult_atm if anchor_type.upper() == 'ATM' else mult_wall

                    width = None
                    if r[8] is not None and r[9] is not None:
                        width = abs(float(r[9]) - float(r[8]))

                    bigwin = None
                    if debit_t is not None and debit_tH is not None and debit_t > 0:
                        bigwin = bool(debit_tH >= req_mult * debit_t)

                    bigwin_possible = None
                    if debit_t is not None and width is not None and debit_t > 0:
                        bigwin_possible = bool(width >= req_mult * debit_t)

                    roi = None
                    if change is not None and debit_t is not None and debit_t > 0:
                        roi = float(change) / float(debit_t)

                    items.append({
                        "snapshot_ts": _to_central_iso(snapshot_ts),
                        "anchor_type": anchor_type,
                        "spread_type": spread_type,
                        "horizon_minutes": int(r[3]) if r[3] is not None else int(horizon_minutes),
                        "debit_t": debit_t,
                        "debit_tH": debit_tH,
                        "change": change,
                        "roi": roi,
                        "req_mult": float(req_mult),
                        "bigwin": bigwin,
                        "bigwin_possible": bigwin_possible,
                        "debit_points": float(r[7]) if r[7] is not None else None,
                        "k_long": float(r[8]) if r[8] is not None else None,
                        "k_short": float(r[9]) if r[9] is not None else None,
                        "pred_change": float(r[10]) if r[10] is not None else None,
                        "p_bigwin": float(r[11]) if r[11] is not None else None,
                    })
        return {"items": items, "tz": "America/Chicago", "anchor_policy": anchor_policy}




    return app


app = create_app()
