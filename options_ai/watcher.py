from __future__ import annotations

import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from options_ai.ai.router import ModelRouter
from options_ai.ai.throttle import RateLimiter
from options_ai.config import Config
from options_ai.processes.ingest import IngestResult, ingest_snapshot_file
from options_ai.processes.scorer import score_due_predictions
from options_ai.ml_eod.pipeline import maybe_generate_today
from options_ai.queries import fetch_total_predictions
from options_ai.runtime_overrides import apply_overrides, load_overrides_file
from options_ai.utils.cache import sha256_file
from options_ai.utils.logger import get_logger, log_bootstrap, log_daemon_event
from options_ai.utils.paths import build_paths, ensure_runtime_dirs


@dataclass
class _FileSeen:
    size: int
    last_change_ts: float


def _load_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return default
    except Exception:
        return default


def _save_json_atomic(path: Path, obj: Any) -> None:
    """Best-effort atomic JSON write.

    Some filesystems (notably certain FUSE/CIFS/NTFS mounts) can raise
    EPERM on os.replace() if the destination file is being read. In that
    case we fall back to an in-place truncate+write, which is not fully
    atomic but keeps the daemon functional.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(obj, ensure_ascii=False, sort_keys=True, indent=2)
    tmp = path.with_suffix(path.suffix + ".tmp")
    try:
        tmp.write_text(payload, encoding="utf-8")
        os.replace(tmp, path)
        return
    except PermissionError:
        # Fall back below
        pass
    except OSError as e:
        # EPERM/EXDEV/etc. -> fall back
        if getattr(e, "errno", None) not in {1, 18}:
            raise
    try:
        with path.open("w", encoding="utf-8") as f:
            f.write(payload)
            f.write("\n")
            f.flush()
            try:
                os.fsync(f.fileno())
            except Exception:
                pass
    finally:
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass


def _load_seen_state(state_path: Path) -> dict[str, Any]:
    return _load_json(state_path, {"snapshot_index": {}, "snapshot_index_by_ticker": {}})


def _sha256_file(path: Path) -> str:
    return sha256_file(path)


def _list_candidate_snapshots(dir_path: Path) -> list[Path]:
    if not dir_path.exists():
        return []
    out: list[Path] = []
    # Fast, non-recursive top-level scan only. Avoid full sort on large dirs.
    try:
        with os.scandir(dir_path) as it:
            for ent in it:
                try:
                    if not ent.is_file():
                        continue
                    n = ent.name
                    if not n.endswith('.json') or n.endswith('.tmp'):
                        continue
                    out.append(Path(ent.path))
                except Exception:
                    continue
    except Exception:
        return []
    return out




def _normalize_ticker_name(v: str | None, default: str = "SPX") -> str:
    t = str(v or "").strip().upper()
    if not t:
        return str(default or "SPX").upper()
    allowed = set("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-")
    if any(ch not in allowed for ch in t):
        return str(default or "SPX").upper()
    return t


def _load_processing_tickers(state_dir: Path, default_ticker: str) -> list[str]:
    p = state_dir / "processing_tickers.json"
    out: list[str] = []
    try:
        obj = _load_json(p, {})
        raw = obj.get("tickers") if isinstance(obj, dict) else None
        if isinstance(raw, list):
            for x in raw:
                out.append(_normalize_ticker_name(x, default_ticker))
    except Exception:
        pass
    out.append(_normalize_ticker_name(default_ticker, default_ticker))
    return sorted(set(out))


def _incoming_dir_for_ticker(cfg: Config, paths: Any, ticker: str) -> Path:
    market_root = Path(os.getenv("MARKET_DATA_ROOT", "/mnt/MarketData")).expanduser()
    p = market_root / ticker
    if p.exists():
        return p
    return Path(paths.data_root) / "incoming" / ticker


def _candidate_entries(cfg: Config, paths: Any) -> list[tuple[Path, Any, bool]]:
    tickers = _load_processing_tickers(Path(paths.state_dir), cfg.ticker)
    entries: list[tuple[Path, Any, bool]] = []
    seen: set[str] = set()

    for t in tickers:
        tpaths = build_paths(str(paths.data_root), t)
        ensure_runtime_dirs(tpaths)

        dirs: list[tuple[Path, bool]] = []
        if cfg.replay_mode:
            dirs = [(Path(tpaths.historical_dir), False)]
        else:
            dirs = [(_incoming_dir_for_ticker(cfg, paths, t), True)]
            if (cfg.reprocess_mode or "none").lower() != "none":
                dirs.append((Path(tpaths.processed_snapshots_dir), False))

        for d, is_incoming in dirs:
            for p in _list_candidate_snapshots(d):
                k = str(p.resolve()) if p.exists() else str(p)
                if k in seen:
                    continue
                seen.add(k)
                entries.append((p, tpaths, is_incoming))

    entries.sort(key=lambda x: str(x[0]))
    return entries

def _run_bootstrap_if_needed(cfg: Config, paths: Any, db_path: str, state: dict[str, Any], router: ModelRouter) -> None:
    if cfg.replay_mode:
        return

    if not cfg.bootstrap_enable:
        return

    total = fetch_total_predictions(db_path)
    if total != 0:
        return

    hist_dir = Path(paths.historical_dir)
    files = _list_candidate_snapshots(hist_dir)
    if not files:
        return

    checkpoint_path = Path(paths.state_dir) / "bootstrap_checkpoint.json"
    completed_path = Path(paths.state_dir) / "bootstrap_completed.json"

    if completed_path.exists():
        return

    checkpoint = _load_json(checkpoint_path, {"last_file": None})
    last_file = checkpoint.get("last_file")

    log_bootstrap(paths, level="INFO", event="bootstrap_start", message="bootstrap start", total_files=len(files), last_file=last_file)

    for p in files:
        if last_file and p.name <= str(last_file):
            continue
        try:
            h = _sha256_file(p)

            ingest_snapshot_file(
                cfg=cfg,
                paths=paths,
                db_path=db_path,
                snapshot_path=p,
                snapshot_hash=h,
                router=router,
                state=state,
                bootstrap_mode=True,
                move_files=False,
            )

            # score as we go (historical timestamps are always eligible)
            score_due_predictions(cfg=cfg, paths=paths, db_path=db_path, state=state)

            _save_json_atomic(checkpoint_path, {"last_file": p.name})
        except Exception as e:
            log_bootstrap(paths, level="ERROR", event="bootstrap_file_error", message="bootstrap file error", file=str(p), error=str(e))

    _save_json_atomic(completed_path, {"completed_at": time.time()})
    log_bootstrap(paths, level="INFO", event="bootstrap_complete", message="bootstrap complete")


def _task_state_path(paths: Any) -> Path:
    return Path(paths.state_dir) / "current_task.json"


def _write_current_task(paths: Any, obj: dict[str, Any] | None) -> None:
    path = _task_state_path(paths)
    try:
        if obj is None:
            if path.exists():
                path.unlink(missing_ok=True)
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(obj, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
        os.replace(tmp, path)
    except Exception:
        pass


def _should_rebuild_router(prev: Config, cur: Config) -> bool:
    keys = [
        "model_force_local",
        "model_force_remote",
        "local_model_enabled",
        "local_model_endpoint",
        "local_model_name",
        "local_model_timeout_seconds",
        "local_model_max_retries",
        "remote_model_name",
        "chart_enabled",
        "chart_local_enabled",
        "chart_remote_enabled",
    ]
    for k in keys:
        if getattr(prev, k) != getattr(cur, k):
            return True
    return False


def run_daemon(cfg: Config, paths: Any, db_path: str) -> None:
    # Load base config once; apply runtime overrides per loop iteration.
    base_cfg = cfg

    state_path = Path(paths.state_dir) / "seen_files.json"
    state = _load_seen_state(state_path)

    file_sizes: dict[str, _FileSeen] = {}
    backoff: dict[str, float] = {}

    limiter = RateLimiter(
        max_per_minute=int(base_cfg.bootstrap_max_model_calls_per_min or 0),
        max_per_hour=int(base_cfg.bootstrap_max_model_calls_per_hour or 0),
    )

    # Router initialized from base config; may be rebuilt if overrides change routing knobs.
    router = ModelRouter(base_cfg, bootstrap_rate_limiter=limiter)

    # Bootstrap backtest on first run
    _run_bootstrap_if_needed(base_cfg, paths, db_path, state, router)

    overrides_path = Path(paths.state_dir) / "runtime_overrides.json"
    last_overrides: dict[str, Any] = {}

    cfg_effective = base_cfg

    def _merge_local_state(local_state: dict[str, Any]) -> None:
        state.setdefault("snapshot_index", {})
        state.setdefault("snapshot_index_by_ticker", {})

        for k, v in (local_state.get("snapshot_index") or {}).items():
            state["snapshot_index"][k] = v

        by_t = local_state.get("snapshot_index_by_ticker") or {}
        if isinstance(by_t, dict):
            for t, idx in by_t.items():
                if not isinstance(idx, dict):
                    continue
                dst = state["snapshot_index_by_ticker"].setdefault(str(t).upper(), {})
                for k, v in idx.items():
                    dst[k] = v

    def _process_entry(entry: tuple[Path, Any, bool], cfg_live: Config) -> dict[str, Any]:
        p, ticker_paths, is_incoming = entry
        file_hash = _sha256_file(p)
        ticker = str(getattr(ticker_paths, "ticker", cfg_live.ticker)).upper()

        _write_current_task(
            paths,
            {
                "file": p.name,
                "snapshot_hash": file_hash,
                "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "stage": "ingest",
                "ticker": ticker,
                "pid": os.getpid(),
            },
        )

        local_state: dict[str, Any] = {"snapshot_index": {}, "snapshot_index_by_ticker": {}}
        ingest_res: IngestResult = ingest_snapshot_file(
            cfg=cfg_live,
            paths=ticker_paths,
            db_path=db_path,
            snapshot_path=p,
            snapshot_hash=file_hash,
            router=router,
            state=local_state,
            bootstrap_mode=False,
            move_files=is_incoming and (not cfg_live.replay_mode),
        )

        _write_current_task(
            paths,
            {
                "file": p.name,
                "snapshot_hash": file_hash,
                "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "stage": "done",
                "ticker": ticker,
                "pid": os.getpid(),
                "processed": bool(ingest_res.processed),
                "prediction_id": ingest_res.prediction_id,
                "skipped_reason": ingest_res.skipped_reason,
            },
        )

        return {
            "ok": True,
            "path": p,
            "ticker_paths": ticker_paths,
            "is_incoming": bool(is_incoming),
            "file_hash": file_hash,
            "ingest_res": ingest_res,
            "local_state": local_state,
        }

    while True:
        try:
            overrides = load_overrides_file(overrides_path)
            if overrides != last_overrides:
                lg = get_logger()
                changed_keys = sorted(set(overrides.keys()) ^ set(last_overrides.keys()))
                if lg:
                    lg.info(
                        component="Config",
                        event="runtime_overrides_changed",
                        message="runtime overrides changed",
                        file_key="system",
                        changed_keys=changed_keys,
                        overrides=overrides,
                    )
                last_overrides = overrides

            new_effective = apply_overrides(base_cfg, overrides)
            if _should_rebuild_router(cfg_effective, new_effective):
                router = ModelRouter(new_effective, bootstrap_rate_limiter=limiter)
            cfg_effective = new_effective

            if bool(cfg_effective.pause_processing):
                # paused: do not ingest or score; keep loop alive for dashboard/runtime changes
                time.sleep(cfg_effective.watch_poll_seconds)
                continue

            score_due_predictions(cfg=cfg_effective, paths=paths, db_path=db_path, state=state)

            entries = _candidate_entries(cfg_effective, paths)
            ready_entries: list[tuple[Path, Any, bool]] = []

            # Pre-filter for stable files/backoff in single thread
            for p, ticker_paths, is_incoming in entries:
                now = time.time()
                next_ts = backoff.get(str(p))
                if next_ts is not None and now < next_ts:
                    continue

                if is_incoming:
                    st = p.stat()
                    prev = file_sizes.get(str(p))
                    if prev is None:
                        file_sizes[str(p)] = _FileSeen(size=st.st_size, last_change_ts=now)
                        continue
                    if st.st_size != prev.size:
                        file_sizes[str(p)] = _FileSeen(size=st.st_size, last_change_ts=now)
                        continue
                    if now - prev.last_change_ts < cfg_effective.file_stable_seconds:
                        continue

                ready_entries.append((p, ticker_paths, is_incoming))

            processed_any = False
            mode = str(getattr(cfg_effective, "multi_ticker_mode", "single_loop") or "single_loop").strip().lower()
            max_workers = max(1, int(getattr(cfg_effective, "multi_ticker_max_workers", 2) or 1))
            use_parallel = mode == "parallel" and max_workers > 1 and len(ready_entries) > 1

            if use_parallel:
                with ThreadPoolExecutor(max_workers=min(max_workers, len(ready_entries))) as ex:
                    futures = {ex.submit(_process_entry, e, cfg_effective): e for e in ready_entries}
                    for fut in as_completed(futures):
                        p, ticker_paths, is_incoming = futures[fut]
                        try:
                            out = fut.result()
                            ingest_res = out["ingest_res"]
                            _merge_local_state(out["local_state"])
                            _save_json_atomic(state_path, state)

                            processed_any = processed_any or bool(ingest_res.processed)

                            if is_incoming and (not ingest_res.processed) and (ingest_res.skipped_reason or "").startswith("duplicate"):
                                try:
                                    dest = Path(ticker_paths.processed_snapshots_dir) / p.name
                                    dest.parent.mkdir(parents=True, exist_ok=True)
                                    if dest.exists():
                                        p.unlink(missing_ok=True)
                                    else:
                                        p.replace(dest)
                                except Exception:
                                    pass
                        except Exception as e:
                            delay = backoff.get(str(p) + ":delay", cfg_effective.watch_poll_seconds)
                            delay = min(max(delay * 2, cfg_effective.watch_poll_seconds), 60.0)
                            backoff[str(p) + ":delay"] = delay
                            backoff[str(p)] = time.time() + delay
                            lg = get_logger()
                            if lg:
                                lg.exception(
                                    level="ERROR",
                                    component="Watcher",
                                    event="snapshot_process_error",
                                    message="snapshot process error",
                                    file_key="errors",
                                    exc=e,
                                    file=str(p),
                                    backoff_seconds=delay,
                                )
                            else:
                                log_daemon_event(ticker_paths.logs_daemon_dir, "error", "snapshot_process_error", file=str(p), error=str(e), backoff_seconds=delay)
                            _write_current_task(
                                paths,
                                {
                                    "file": p.name,
                                    "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                                    "stage": "error",
                                    "ticker": str(getattr(ticker_paths, "ticker", cfg_effective.ticker)).upper(),
                                    "pid": os.getpid(),
                                    "error": str(e),
                                },
                            )
            else:
                for entry in ready_entries:
                    p, ticker_paths, is_incoming = entry
                    try:
                        out = _process_entry(entry, cfg_effective)
                        ingest_res = out["ingest_res"]
                        _merge_local_state(out["local_state"])
                        _save_json_atomic(state_path, state)

                        processed_any = processed_any or bool(ingest_res.processed)

                        if is_incoming and (not ingest_res.processed) and (ingest_res.skipped_reason or "").startswith("duplicate"):
                            try:
                                dest = Path(ticker_paths.processed_snapshots_dir) / p.name
                                dest.parent.mkdir(parents=True, exist_ok=True)
                                if dest.exists():
                                    p.unlink(missing_ok=True)
                                else:
                                    p.replace(dest)
                            except Exception:
                                pass
                    except Exception as e:
                        delay = backoff.get(str(p) + ":delay", cfg_effective.watch_poll_seconds)
                        delay = min(max(delay * 2, cfg_effective.watch_poll_seconds), 60.0)
                        backoff[str(p) + ":delay"] = delay
                        backoff[str(p)] = time.time() + delay
                        lg = get_logger()
                        if lg:
                            lg.exception(
                                level="ERROR",
                                component="Watcher",
                                event="snapshot_process_error",
                                message="snapshot process error",
                                file_key="errors",
                                exc=e,
                                file=str(p),
                                backoff_seconds=delay,
                            )
                        else:
                            log_daemon_event(ticker_paths.logs_daemon_dir, "error", "snapshot_process_error", file=str(p), error=str(e), backoff_seconds=delay)

                        _write_current_task(
                            paths,
                            {
                                "file": p.name,
                                "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                                "stage": "error",
                                "ticker": str(getattr(ticker_paths, "ticker", cfg_effective.ticker)).upper(),
                                "pid": os.getpid(),
                                "error": str(e),
                            },
                        )

            try:
                maybe_generate_today(cfg_effective, db_path)
            except Exception:
                pass

            time.sleep(cfg_effective.watch_poll_seconds if not processed_any else 1.0)

        except Exception as e:
            lg = get_logger()
            if lg:
                lg.exception(level="CRITICAL", component="Watcher", event="watch_loop_error", message="watch loop error", file_key="errors", exc=e)
            else:
                log_daemon_event(paths.logs_daemon_dir, "error", "watch_loop_error", error=str(e))
            time.sleep(2.0)
