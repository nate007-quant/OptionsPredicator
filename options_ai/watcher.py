from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from options_ai.ai.codex_client import CodexClient
from options_ai.ai.throttle import RateLimiter, ThrottledCodexClient
from options_ai.config import Config
from options_ai.processes.ingest import IngestResult, ingest_snapshot_file
from options_ai.processes.scorer import score_due_predictions
from options_ai.queries import fetch_total_predictions, hash_exists
from options_ai.utils.cache import sha256_file
from options_ai.utils.logger import log_daemon_event


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
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, sort_keys=True, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def _load_seen_state(state_path: Path) -> dict[str, Any]:
    return _load_json(state_path, {"snapshot_index": {}})


def _sha256_file(path: Path) -> str:
    # compatibility wrapper
    return sha256_file(path)


def _list_candidate_snapshots(dir_path: Path) -> list[Path]:
    if not dir_path.exists():
        return []
    return sorted([p for p in dir_path.glob("*.json") if p.is_file() and not p.name.endswith(".tmp")])


def _build_codex(cfg: Config, throttled: bool) -> CodexClient | None:
    if not cfg.openai_api_key:
        return None
    if throttled:
        limiter = RateLimiter(
            max_per_minute=int(cfg.bootstrap_max_model_calls_per_min or 0),
            max_per_hour=int(cfg.bootstrap_max_model_calls_per_hour or 0),
        )
        return ThrottledCodexClient(api_key=cfg.openai_api_key, model=cfg.codex_model, limiter=limiter)
    return CodexClient(api_key=cfg.openai_api_key, model=cfg.codex_model)


def _run_bootstrap_if_needed(cfg: Config, paths: Any, db_path: str, state: dict[str, Any]) -> None:
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

    codex = _build_codex(cfg, throttled=True)

    log_daemon_event(paths.logs_daemon_dir, "info", "bootstrap_start", total_files=len(files), last_file=last_file)

    for p in files:
        if last_file and p.name <= str(last_file):
            continue
        try:
            h = _sha256_file(p)
            if hash_exists(db_path, h, cfg.prompt_version):
                _save_json_atomic(checkpoint_path, {"last_file": p.name})
                continue

            ingest_snapshot_file(
                cfg=cfg,
                paths=paths,
                db_path=db_path,
                snapshot_path=p,
                snapshot_hash=h,
                codex=codex,
                state=state,
                bootstrap_mode=True,
                move_files=False,
            )

            # score as we go (historical timestamps are always eligible)
            score_due_predictions(cfg=cfg, paths=paths, db_path=db_path, state=state)

            _save_json_atomic(checkpoint_path, {"last_file": p.name})
        except Exception as e:
            log_daemon_event(paths.logs_daemon_dir, "error", "bootstrap_file_error", file=str(p), error=str(e))
            # continue; checkpoint keeps progress up to last success

    _save_json_atomic(completed_path, {"completed_at": time.time()})
    log_daemon_event(paths.logs_daemon_dir, "info", "bootstrap_complete")


def run_daemon(cfg: Config, paths: Any, db_path: str) -> None:
    """Long-running loop. Polling watcher (inotify optional in future)."""

    state_path = Path(paths.state_dir) / "seen_files.json"
    state = _load_seen_state(state_path)

    file_sizes: dict[str, _FileSeen] = {}
    backoff: dict[str, float] = {}  # filepath -> next_attempt_ts

    codex = _build_codex(cfg, throttled=False)

    # Bootstrap backtest on first run (empty DB)
    _run_bootstrap_if_needed(cfg, paths, db_path, state)

    while True:
        try:
            # Attempt scoring periodically, even if no new snapshots.
            score_due_predictions(cfg=cfg, paths=paths, db_path=db_path, state=state)

            dirs: list[Path] = []
            if cfg.replay_mode:
                dirs = [Path(paths.historical_dir)]
            else:
                dirs = [Path(paths.incoming_snapshots_dir)]
                if (cfg.reprocess_mode or "none").lower() != "none":
                    dirs.append(Path(paths.processed_snapshots_dir))

            candidates: list[Path] = []
            for d in dirs:
                candidates.extend(_list_candidate_snapshots(d))

            processed_any = False

            for p in sorted(set(candidates)):
                now = time.time()

                # Backoff per file
                next_ts = backoff.get(str(p))
                if next_ts is not None and now < next_ts:
                    continue

                # Only enforce stable-size check for incoming dir.
                is_incoming = Path(paths.incoming_snapshots_dir) in p.parents

                if is_incoming:
                    st = p.stat()
                    prev = file_sizes.get(str(p))
                    if prev is None:
                        file_sizes[str(p)] = _FileSeen(size=st.st_size, last_change_ts=now)
                        continue
                    if st.st_size != prev.size:
                        file_sizes[str(p)] = _FileSeen(size=st.st_size, last_change_ts=now)
                        continue
                    if now - prev.last_change_ts < cfg.file_stable_seconds:
                        continue

                file_hash = _sha256_file(p)

                if hash_exists(db_path, file_hash, cfg.prompt_version):
                    # already processed for this prompt_version
                    if is_incoming:
                        try:
                            dest = Path(paths.processed_snapshots_dir) / p.name
                            dest.parent.mkdir(parents=True, exist_ok=True)
                            if dest.exists():
                                p.unlink(missing_ok=True)
                            else:
                                p.replace(dest)
                        except Exception:
                            pass
                    continue

                try:
                    ingest_res: IngestResult = ingest_snapshot_file(
                        cfg=cfg,
                        paths=paths,
                        db_path=db_path,
                        snapshot_path=p,
                        snapshot_hash=file_hash,
                        codex=codex,
                        state=state,
                        bootstrap_mode=False,
                        move_files=is_incoming and (not cfg.replay_mode),
                    )
                    processed_any = processed_any or ingest_res.processed

                    _save_json_atomic(state_path, state)

                except Exception as e:
                    delay = backoff.get(str(p) + ":delay", cfg.watch_poll_seconds)
                    delay = min(max(delay * 2, cfg.watch_poll_seconds), 60.0)
                    backoff[str(p) + ":delay"] = delay
                    backoff[str(p)] = time.time() + delay
                    log_daemon_event(
                        paths.logs_daemon_dir,
                        "error",
                        "snapshot_process_error",
                        file=str(p),
                        error=str(e),
                        backoff_seconds=delay,
                    )

            time.sleep(cfg.watch_poll_seconds if not processed_any else 0.1)

        except Exception as e:
            log_daemon_event(paths.logs_daemon_dir, "error", "watch_loop_error", error=str(e))
            time.sleep(2.0)
