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
from options_ai.backtest.executor import BacktestExecutor
from options_ai.backtest.registry import StrategyRegistry, params_hash
from options_ai.backtest.sampler_service import BacktestSamplerService
from options_ai.backtest.portfolio_backtest_service import PortfolioBacktestService
from options_ai.backtest.sqlite_migrations import migrate_backtest_schema, backfill_params_hash


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
    con = sqlite3.connect(db_path, timeout=30.0)
    con.row_factory = sqlite3.Row
    # WAL + busy timeout to mitigate locking during sampler runs
    try:
        con.execute('PRAGMA journal_mode=WAL;')
    except Exception:
        pass
    try:
        con.execute('PRAGMA busy_timeout=10000;')
    except Exception:
        pass
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
            # Migrations: add dedupe/hash + refinement latch + sampler sessions
            migrate_backtest_schema(con)
            # Backfill params_hash for old rows (params_json already canonical in this app)
            backfill_params_hash(con, hash_fn=lambda strategy_key, schema_version, params_json: params_hash(strategy_key=strategy_key, schema_version=int(schema_version), params_json_canonical=str(params_json)))
            con.commit()

    def _validate_preset_name(name: str) -> str:
        nm = (name or '').strip()
        if not nm:
            raise HTTPException(status_code=400, detail='name required')
        if len(nm) > 60:
            raise HTTPException(status_code=400, detail='name too long (max 60)')
        return nm

    _ensure_backtest_tables()

    # Backtest services
    strategy_registry = StrategyRegistry()
    backtest_executor = BacktestExecutor(db_path=db_path, connect_fn=_connect)
    sampler_service = BacktestSamplerService(db_path=db_path, connect_fn=_connect)
    portfolio_service = PortfolioBacktestService(db_path=db_path, connect_fn=_connect)

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


    @app.get('/api/status/pipelines')
    def status_pipelines(window: int = Query(500, ge=50, le=5000)) -> dict[str, Any]:
        """Return Timescale-backed pipeline progress + state cursors.

        This is used by the Processing tab to show how far each stage is behind the
        newest option_chain snapshot.
        """
        dsn = _pg_dsn()
        tz_local = os.getenv('TZ_LOCAL', 'America/Chicago').strip() or 'America/Chicago'
        out: dict[str, Any] = {
            'ok': True,
            'window': int(window),
            'ts': _now_central_iso(),
            'timescale': {'ok': False, 'error': None},
            'latest': {},
            'lags_minutes': {},
            'counts_recent': {},
            'labels_by_horizon': {},
            'scores_by_horizon': {},
            'state_files': {},
        }

        def _try_load_json_file(path: Path) -> Any:
            try:
                import json as _json
                return _json.loads(path.read_text(encoding='utf-8'))
            except Exception:
                return None

        # state cursor files (best-effort)
        try:
            sf = {}
            for name in sorted(list((data_root / 'state').glob('*backfill*.json')) + list((data_root / 'state').glob('task_*.json'))):
                try:
                    st = name.stat()
                    sf[name.name] = {
                        'path': str(name),
                        'mtime_utc': datetime.fromtimestamp(st.st_mtime, tz=timezone.utc).replace(microsecond=0).isoformat(),
                        'bytes': int(st.st_size),
                        'json': _try_load_json_file(name),
                    }
                except Exception:
                    continue
            out['state_files'] = sf
        except Exception:
            out['state_files'] = {}

        if not dsn:
            out['timescale'] = {'ok': False, 'error': 'SPX_CHAIN_DATABASE_URL not configured'}
            return out

        # Timescale progress
        try:
            with _pg_connect(dsn) as conn:
                with conn.cursor() as cur:
                    def max_ts(table: str) -> Any:
                        cur.execute(f"SELECT max(snapshot_ts) FROM {table}")
                        r = cur.fetchone()
                        return r[0] if r else None

                    def max_ts_le(table: str, le: Any) -> Any:
                        if le is None:
                            return max_ts(table)
                        cur.execute(f"SELECT max(snapshot_ts) FROM {table} WHERE snapshot_ts <= %s", (le,))
                        r = cur.fetchone()
                        return r[0] if r else None

                    latest_chain = max_ts('spx.option_chain')
                    latest_feat = max_ts_le('spx.chain_features_0dte', latest_chain)
                    latest_label = max_ts_le('spx.chain_labels_0dte', latest_chain)
                    latest_cand = max_ts_le('spx.debit_spread_candidates_0dte', latest_chain)
                    latest_dlbl = max_ts_le('spx.debit_spread_labels_0dte', latest_chain)
                    latest_score = max_ts_le('spx.debit_spread_scores_0dte', latest_chain)

                    out['latest'] = {
                        'option_chain': latest_chain,
                        'chain_features_0dte': latest_feat,
                        'chain_labels_0dte': latest_label,
                        'debit_candidates_0dte': latest_cand,
                        'debit_labels_0dte': latest_dlbl,
                        'debit_scores_0dte': latest_score,
                    }

                    # horizon breakdowns
                    cur.execute("SELECT horizon_minutes, max(snapshot_ts) FROM spx.chain_labels_0dte WHERE snapshot_ts <= %s GROUP BY horizon_minutes ORDER BY horizon_minutes", (latest_chain,))
                    out['labels_by_horizon'] = {int(r[0]): r[1] for r in cur.fetchall() if r and r[0] is not None}

                    cur.execute("SELECT horizon_minutes, max(snapshot_ts) FROM spx.debit_spread_scores_0dte WHERE snapshot_ts <= %s GROUP BY horizon_minutes ORDER BY horizon_minutes", (latest_chain,))
                    out['scores_by_horizon'] = {int(r[0]): r[1] for r in cur.fetchall() if r and r[0] is not None}

                    # recent window counts (approx backlog)
                    # Missing features for newest N distinct snapshots
                    cur.execute(
                        """
                        WITH oc AS (
                          -- Only count snapshots that actually have a 0DTE expiration for that local trade date.
                          SELECT DISTINCT snapshot_ts
                          FROM spx.option_chain
                          WHERE expiration_date = ((snapshot_ts AT TIME ZONE %s)::date)
                          ORDER BY snapshot_ts DESC
                          LIMIT %s
                        )
                        SELECT
                          COUNT(*) AS n,
                          SUM(CASE WHEN f.snapshot_ts IS NULL THEN 1 ELSE 0 END) AS missing
                        FROM oc
                        LEFT JOIN spx.chain_features_0dte f
                          ON f.snapshot_ts = oc.snapshot_ts
                        """,
                        (tz_local, int(window)),
                    )
                    r = cur.fetchone()
                    out['counts_recent']['features_missing'] = {'window': int(window), 'n': int(r[0] or 0), 'missing': int(r[1] or 0)}

                    # Missing any chain labels for newest N feature snapshots
                    cur.execute(
                        """
                        WITH f AS (
                          SELECT snapshot_ts
                          FROM spx.chain_features_0dte
                          ORDER BY snapshot_ts DESC
                          LIMIT %s
                        )
                        SELECT
                          COUNT(*) AS n,
                          SUM(CASE WHEN l.snapshot_ts IS NULL THEN 1 ELSE 0 END) AS missing
                        FROM f
                        LEFT JOIN (
                          SELECT DISTINCT snapshot_ts FROM spx.chain_labels_0dte
                        ) l
                          ON l.snapshot_ts = f.snapshot_ts
                        """,
                        (int(window),),
                    )
                    r = cur.fetchone()
                    out['counts_recent']['labels_missing_any'] = {'window': int(window), 'n': int(r[0] or 0), 'missing': int(r[1] or 0)}

                    # Missing any debit candidates for newest N feature snapshots
                    cur.execute(
                        """
                        WITH f AS (
                          SELECT snapshot_ts
                          FROM spx.chain_features_0dte
                          ORDER BY snapshot_ts DESC
                          LIMIT %s
                        )
                        SELECT
                          COUNT(*) AS n,
                          SUM(CASE WHEN c.snapshot_ts IS NULL THEN 1 ELSE 0 END) AS missing
                        FROM f
                        LEFT JOIN (
                          SELECT DISTINCT snapshot_ts FROM spx.debit_spread_candidates_0dte
                        ) c
                          ON c.snapshot_ts = f.snapshot_ts
                        """,
                        (int(window),),
                    )
                    r = cur.fetchone()
                    out['counts_recent']['debit_candidates_missing_any'] = {'window': int(window), 'n': int(r[0] or 0), 'missing': int(r[1] or 0)}

                    # Missing any debit scores for newest N candidates (any horizon)
                    cur.execute(
                        """
                        WITH c AS (
                          SELECT DISTINCT snapshot_ts
                          FROM spx.debit_spread_candidates_0dte
                          ORDER BY snapshot_ts DESC
                          LIMIT %s
                        )
                        SELECT
                          COUNT(*) AS n,
                          SUM(CASE WHEN s.snapshot_ts IS NULL THEN 1 ELSE 0 END) AS missing
                        FROM c
                        LEFT JOIN (
                          SELECT DISTINCT snapshot_ts FROM spx.debit_spread_scores_0dte
                        ) s
                          ON s.snapshot_ts = c.snapshot_ts
                        """,
                        (int(window),),
                    )
                    r = cur.fetchone()
                    out['counts_recent']['debit_scores_missing_any'] = {'window': int(window), 'n': int(r[0] or 0), 'missing': int(r[1] or 0)}

                    # Lag minutes vs latest option_chain
                    def lag_minutes(ts: Any) -> float | None:
                        if latest_chain is None or ts is None:
                            return None
                        try:
                            return (latest_chain - ts).total_seconds() / 60.0
                        except Exception:
                            return None

                    out['lags_minutes'] = {
                        'chain_features_0dte': lag_minutes(latest_feat),
                        'chain_labels_0dte': lag_minutes(latest_label),
                        'debit_candidates_0dte': lag_minutes(latest_cand),
                        'debit_labels_0dte': lag_minutes(latest_dlbl),
                        'debit_scores_0dte': lag_minutes(latest_score),
                    }



            # Derived cursors from state files (best-effort): shows what the backfill loops are currently working through.
            try:
                now_utc = datetime.now(timezone.utc)
                cursors: list[dict[str, Any]] = []

                def add_cursor(stage: str, file_key: str, key: str, ts_val: Any) -> None:
                    if ts_val is None:
                        return
                    # parse ts
                    ts = None
                    try:
                        if isinstance(ts_val, str):
                            ts = datetime.fromisoformat(ts_val.replace('Z', '+00:00'))
                        elif isinstance(ts_val, datetime):
                            ts = ts_val
                    except Exception:
                        ts = None
                    if ts is not None and ts.tzinfo is None:
                        ts = ts.replace(tzinfo=timezone.utc)

                    meta = (out.get('state_files') or {}).get(file_key) or {}
                    mtime_s = meta.get('mtime_utc')
                    mtime = None
                    try:
                        if isinstance(mtime_s, str):
                            mtime = datetime.fromisoformat(mtime_s.replace('Z', '+00:00'))
                    except Exception:
                        mtime = None

                    lag_chain = None
                    try:
                        if latest_chain is not None and ts is not None:
                            lag_chain = (latest_chain - ts).total_seconds() / 60.0
                    except Exception:
                        lag_chain = None

                    updating_recently = None
                    try:
                        if mtime is not None:
                            updating_recently = (now_utc - mtime).total_seconds() <= 15 * 60
                    except Exception:
                        updating_recently = None

                    cursors.append({
                        'stage': stage,
                        'file': file_key,
                        'key': key,
                        'cursor_ts': ts.isoformat().replace('+00:00', 'Z') if ts is not None else ts_val,
                        'lag_vs_latest_chain_minutes': lag_chain,
                        'state_mtime_utc': mtime_s,
                        'updating_recently': updating_recently,
                    })

                # Known state files
                for fk, meta in (out.get('state_files') or {}).items():
                    j = meta.get('json') if isinstance(meta, dict) else None
                    if not isinstance(j, dict):
                        continue
                    # Phase2 0DTE
                    if fk == 'phase2_0dte_backfill.json':
                        add_cursor('phase2_features_0dte', fk, 'features_cursor_ts', j.get('features_cursor_ts'))
                        add_cursor('phase2_labels_0dte', fk, 'labels_cursor_ts', j.get('labels_cursor_ts'))
                    # Debit candidates 0DTE
                    if fk == 'debit_0dte_backfill.json':
                        add_cursor('debit_candidates_0dte', fk, 'candidates_cursor_ts', j.get('candidates_cursor_ts'))
                    # Debit ML scores 0DTE (may have per-horizon cursors)
                    if fk == 'debit_ml_0dte_backfill.json':
                        for k, v in j.items():
                            if str(k).startswith('scores_cursor_'):
                                add_cursor('debit_scores_0dte', fk, str(k), v)
                    # Term builders
                    if fk.startswith('debit_term_backfill_'):
                        add_cursor('debit_candidates_term', fk, 'candidates_cursor_ts', j.get('candidates_cursor_ts'))
                    if fk.startswith('phase2_term_backfill_'):
                        # legacy key name
                        add_cursor('phase2_term', fk, 'cursor_ts', j.get('cursor_ts') or j.get('cursor'))

                cursors.sort(key=lambda x: (x.get('stage') or '', x.get('file') or '', x.get('key') or ''))
                out['cursors'] = cursors
            except Exception:
                out['cursors'] = []
            out['timescale'] = {'ok': True, 'error': None}
            return out
        except Exception as e:
            out['timescale'] = {'ok': False, 'error': str(e)}
            return out

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


    # ---- Backtest Strategies (metadata) ----

    @app.get('/api/backtest/strategies')
    def backtest_strategies() -> dict[str, Any]:
        return {'items': strategy_registry.list()}

    @app.get('/api/backtest/strategies/{strategy_id}/schema')
    def backtest_strategy_schema(strategy_id: str) -> dict[str, Any]:
        sdef = strategy_registry.get(strategy_id)
        return {'id': sdef.id, 'display_name': getattr(sdef, 'display_name', sdef.id), 'schema_version': int(getattr(sdef, 'schema_version', 1))}

    @app.get('/api/backtest/strategies/{strategy_id}/param_specs')
    def backtest_strategy_param_specs(strategy_id: str) -> dict[str, Any]:
        sdef = strategy_registry.get(strategy_id)
        items = []
        for sp in sdef.param_specs():
            items.append({
                'key': sp.key,
                'typ': sp.typ,
                'default': sp.default,
                'min': sp.min,
                'max': sp.max,
                'step': sp.step,
                'choices': sp.choices,
                'sweepable': bool(sp.sweepable),
                'refineable': bool(sp.refineable),
            })
        return {'strategy_id': strategy_id, 'items': items}


# ---- Sampler control ----

    @app.post('/api/backtest/sampler/start')
    def sampler_start(body: dict[str, Any]) -> dict[str, Any]:
        sid = str((body or {}).get('strategy_id') or 'debit_spreads')
        base_params = (body or {}).get('base_params')
        if not isinstance(base_params, dict):
            raise HTTPException(status_code=400, detail='base_params must be an object')
        budget = int((body or {}).get('budget') or 300)
        seed = (body or {}).get('seed')
        search_plan = (body or {}).get('search_plan')
        if search_plan is not None and not isinstance(search_plan, dict):
            raise HTTPException(status_code=400, detail='search_plan must be an object')
        return sampler_service.start(strategy_id=sid, base_params=base_params, budget=budget, seed=(int(seed) if seed not in (None,'') else None), search_plan=search_plan)

    @app.get('/api/backtest/sampler/status')
    def sampler_status(sampler_id: int | None = Query(None)) -> dict[str, Any]:
        st = sampler_service.status(sampler_id=int(sampler_id) if sampler_id is not None else None)
        if st is None:
            return {'sampler_id': sampler_id, 'status': None}
        return {
            'sampler_id': st.sampler_id,
            'status': st.status,
            'runs_completed': st.runs_completed,
            'duplicates_skipped': st.duplicates_skipped,
            'runs_failed': st.runs_failed,
            'precheck_rejected': getattr(st, 'precheck_rejected', 0),
            'last_activity_at_utc': getattr(st, 'last_activity_at_utc', None),
            'last_run_id': st.last_run_id,
        }

    @app.post('/api/backtest/sampler/stop')
    def sampler_stop(body: dict[str, Any]) -> dict[str, Any]:
        sid = (body or {}).get('sampler_id')
        if sid in (None, ''):
            raise HTTPException(status_code=400, detail='sampler_id required')
        return sampler_service.stop(sampler_id=int(sid))

    @app.post('/api/backtest/sampler/refine_from_run')
    def sampler_refine_from_run(body: dict[str, Any]) -> dict[str, Any]:
        rid = (body or {}).get('parent_run_id')
        if rid in (None, ''):
            raise HTTPException(status_code=400, detail='parent_run_id required')
        budget = int((body or {}).get('budget') or 200)
        rounds = int((body or {}).get('rounds') or 3)
        shrink = float((body or {}).get('shrink') or 0.5)
        return sampler_service.refine_from_run(parent_run_id=int(rid), budget=budget, rounds=rounds, shrink=shrink)






# ---- Portfolios (saved definitions) ----

    @app.get('/api/portfolios')
    def portfolios_list() -> dict[str, Any]:
        import json as _json
        with _connect(db_path) as con:
            rows = con.execute(
                """SELECT id,name,legs_json,created_at_utc,updated_at_utc
                   FROM portfolio_defs
                   ORDER BY updated_at_utc DESC, id DESC"""
            ).fetchall()
        items = []
        for r in rows:
            try:
                legs = _json.loads(r['legs_json'] or '[]')
            except Exception:
                legs = []
            items.append(
                {
                    'id': int(r['id']),
                    'name': str(r['name']),
                    'legs': legs,
                    'created_at_utc': str(r['created_at_utc']),
                    'updated_at_utc': str(r['updated_at_utc']),
                }
            )
        return {'items': items}

    @app.post('/api/portfolios')
    def portfolios_create(body: dict[str, Any]) -> dict[str, Any]:
        import json as _json
        name = str((body or {}).get('name') or '').strip()
        if not name:
            raise HTTPException(status_code=400, detail='name required')
        legs = (body or {}).get('legs')
        if legs is None:
            legs = []
        if not isinstance(legs, list):
            raise HTTPException(status_code=400, detail='legs must be a list')
        now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        with _connect(db_path) as con:
            cur = con.execute(
                """INSERT INTO portfolio_defs(name, legs_json, created_at_utc, updated_at_utc)
                   VALUES(?,?,?,?)""",
                (name, _json.dumps(legs, separators=(',', ':'), sort_keys=True), now, now),
            )
            pid = int(cur.lastrowid)
            con.commit()
        return {'id': pid, 'name': name, 'legs': legs}

    @app.get('/api/portfolios/{portfolio_id}')
    def portfolios_get(portfolio_id: int) -> dict[str, Any]:
        import json as _json
        with _connect(db_path) as con:
            r = con.execute(
                """SELECT id,name,legs_json,created_at_utc,updated_at_utc
                   FROM portfolio_defs WHERE id=?""",
                (int(portfolio_id),),
            ).fetchone()
        if not r:
            raise HTTPException(status_code=404, detail='portfolio not found')
        try:
            legs = _json.loads(r['legs_json'] or '[]')
        except Exception:
            legs = []
        return {
            'id': int(r['id']),
            'name': str(r['name']),
            'legs': legs,
            'created_at_utc': str(r['created_at_utc']),
            'updated_at_utc': str(r['updated_at_utc']),
        }

    @app.put('/api/portfolios/{portfolio_id}')
    def portfolios_update(portfolio_id: int, body: dict[str, Any]) -> dict[str, Any]:
        import json as _json
        name = (body or {}).get('name')
        legs = (body or {}).get('legs')
        if name is not None:
            name = str(name).strip()
            if not name:
                raise HTTPException(status_code=400, detail='name cannot be blank')
        if legs is not None and not isinstance(legs, list):
            raise HTTPException(status_code=400, detail='legs must be a list')

        now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        with _connect(db_path) as con:
            r = con.execute('SELECT id,name,legs_json FROM portfolio_defs WHERE id=?', (int(portfolio_id),)).fetchone()
            if not r:
                raise HTTPException(status_code=404, detail='portfolio not found')
            cur_name = str(r['name'])
            cur_legs_json = str(r['legs_json'] or '[]')

            new_name = cur_name if name is None else str(name)
            new_legs_json = cur_legs_json if legs is None else _json.dumps(legs, separators=(',', ':'), sort_keys=True)

            con.execute(
                'UPDATE portfolio_defs SET name=?, legs_json=?, updated_at_utc=? WHERE id=?',
                (new_name, new_legs_json, now, int(portfolio_id)),
            )
            con.commit()

        try:
            out_legs = _json.loads(new_legs_json or '[]')
        except Exception:
            out_legs = []
        return {'id': int(portfolio_id), 'name': new_name, 'legs': out_legs}

    @app.delete('/api/portfolios/{portfolio_id}')
    def portfolios_delete(portfolio_id: int) -> dict[str, Any]:
        with _connect(db_path) as con:
            r = con.execute('SELECT id FROM portfolio_defs WHERE id=?', (int(portfolio_id),)).fetchone()
            if not r:
                raise HTTPException(status_code=404, detail='portfolio not found')
            con.execute('DELETE FROM portfolio_defs WHERE id=?', (int(portfolio_id),))
            con.commit()
        return {'ok': True, 'deleted_id': int(portfolio_id)}


# ---- Portfolio Backtest (multi-leg) ----

    @app.post('/api/portfolio_backtest/start')
    def portfolio_backtest_start(body: dict[str, Any]) -> dict[str, Any]:
        legs = (body or {}).get('legs')
        if not isinstance(legs, list) or not legs:
            raise HTTPException(status_code=400, detail='legs must be a non-empty list')
        return portfolio_service.start(legs=legs)

    @app.get('/api/portfolio_backtest/status')
    def portfolio_backtest_status(session_id: int | None = Query(None)) -> dict[str, Any]:
        st = portfolio_service.status(session_id=int(session_id) if session_id is not None else None)
        if st is None:
            return {'session_id': session_id, 'status': None}
        return st

    @app.post('/api/portfolio_backtest/stop')
    def portfolio_backtest_stop(body: dict[str, Any]) -> dict[str, Any]:
        sid = (body or {}).get('session_id')
        if sid in (None, ''):
            raise HTTPException(status_code=400, detail='session_id required')
        return portfolio_service.stop(session_id=int(sid))


# ---- Backtest Runs (history grid) ----

    @app.get('/api/backtest/runs')
    def backtest_runs_list(
        strategy_key: str | None = Query(None),
        preset_id: int | None = Query(None),
        limit: int = Query(200, ge=1, le=2000),
        include_zero_trades: bool = Query(False),
    ) -> dict[str, Any]:
        import json as _json
        sql = """
            SELECT id, strategy_key, created_at_utc, preset_id, preset_name_at_run, params_json, summary_json,
                   COALESCE(refinement_launched,0) AS refinement_launched, refinement_sampler_id
            FROM backtest_runs
            WHERE 1=1
        """
        params: list[Any] = []
        if strategy_key is not None:
            sql += " AND strategy_key = ?"
            params.append(str(strategy_key))
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


            if not include_zero_trades and isinstance(summary_obj, dict):
                try:
                    t0 = summary_obj.get('trades')
                    if t0 is None:
                        t0 = summary_obj.get('n_trades')
                    if t0 is not None and int(t0) == 0:
                        continue
                except Exception:
                    pass
            items.append({
                'id': int(r['id']),
                'strategy_key': r['strategy_key'],
                'created_at_utc': r['created_at_utc'],
                'preset_id': r['preset_id'],
                'preset_name_at_run': r['preset_name_at_run'],
                'params': params_obj,
                'summary': summary_obj,
                'refinement_launched': bool(r['refinement_launched'] or 0),
                'refinement_sampler_id': (int(r['refinement_sampler_id']) if r['refinement_sampler_id'] is not None else None),
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

    

    @app.post('/api/backtest/runs/bulk_delete')
    def backtest_runs_bulk_delete(payload: dict[str, Any]) -> dict[str, Any]:
        ids = payload.get('ids')
        if not isinstance(ids, list) or not ids:
            raise HTTPException(status_code=400, detail='ids must be a non-empty list')
        if len(ids) > 5000:
            raise HTTPException(status_code=400, detail='too many ids (max 5000)')
        norm: list[int] = []
        invalid: list[Any] = []
        for x in ids:
            try:
                norm.append(int(x))
            except Exception:
                invalid.append(x)

        if not norm:
            raise HTTPException(status_code=400, detail={'message': 'no valid ids', 'invalid_count': len(invalid)})

        q = ','.join(['?'] * len(norm))
        with _connect(db_path) as con:
            cur = con.execute(f'DELETE FROM backtest_runs WHERE id IN ({q})', tuple(norm))
            con.commit()

        deleted = int(cur.rowcount if cur.rowcount is not None else 0)
        return {'ok': True, 'deleted_count': deleted, 'requested': len(norm)}

    @app.post("/api/backtest/debit_spreads/run")
    def backtest_debit_spreads_run(payload: dict[str, Any]) -> dict[str, Any]:
        """Run a Timescale-backed backtest for debit spreads (debit or credit).

        Persisted to SQLite backtest_runs with global dedupe by (strategy_key, schema_version, params_hash).
        """
        import json as _json

        pid_in = (payload or {}).get('preset_id', None)
        preset_id_final: int | None = None
        preset_name_at_run: str | None = None

        with _connect(db_path) as con:
            if pid_in is not None:
                try:
                    pid = int(pid_in)
                    row = con.execute('SELECT id, name FROM backtest_presets WHERE id=?', (pid,)).fetchone()
                    if row:
                        preset_id_final = int(row['id'])
                        preset_name_at_run = str(row['name'])
                except Exception:
                    preset_id_final = None
                    preset_name_at_run = None

        force_run = bool((payload or {}).get('force_run') or False)

        result = backtest_executor.execute_and_persist(
            strategy_id='debit_spreads',
            payload=dict(payload or {}),
            preset_id=preset_id_final,
            preset_name_at_run=preset_name_at_run,
            strict=True,
            force_run=force_run,
        )

        # Back-compat for UI: legacy flag name
        if isinstance(result, dict):
            result['run_duplicate'] = bool(result.get('duplicate_skipped'))

        # Update preset last_run even if duplicate (link to existing run_id)
        if preset_id_final is not None:
            run_id = int(result.get('run_id')) if isinstance(result, dict) and result.get('run_id') else None
            if run_id is not None:
                with _connect(db_path) as con:
                    row = con.execute('SELECT summary_json FROM backtest_runs WHERE id=?', (run_id,)).fetchone()
                    summary_json = row['summary_json'] if row else _json.dumps((result or {}).get('summary') or {}, separators=(',', ':'), sort_keys=True)
                    now = _now_utc_iso()
                    con.execute(
                        """
                        UPDATE backtest_presets
                        SET last_run_id=?, last_run_at_utc=?, last_summary_json=?, updated_at_utc=?
                        WHERE id=?
                        """,
                        (run_id, now, summary_json, now, preset_id_final),
                    )
                    con.commit()

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
