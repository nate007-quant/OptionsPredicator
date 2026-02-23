from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from pathlib import Path
from typing import Any

from options_ai.ai.codex_client import CodexClient
from options_ai.config import Config
from options_ai.processes.ingest import IngestResult, ingest_snapshot_file
from options_ai.processes.scorer import score_due_predictions
from options_ai.queries import hash_exists
from options_ai.utils.logger import log_daemon_event


@dataclass
class _FileSeen:
    size: int
    last_change_ts: float


def _load_seen_state(state_path: Path) -> dict[str, Any]:
    try:
        return json.loads(state_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {"seen_hashes": [], "snapshot_index": {}}
    except Exception:
        return {"seen_hashes": [], "snapshot_index": {}}


def _save_seen_state(state_path: Path, state: dict[str, Any]) -> None:
    state_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = state_path.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, sort_keys=True, indent=2), encoding="utf-8")
    os.replace(tmp, state_path)


def _sha256_file(path: Path) -> str:
    h = sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _list_candidate_snapshots(dir_path: Path) -> list[Path]:
    if not dir_path.exists():
        return []
    return sorted([p for p in dir_path.glob("*.json") if p.is_file() and not p.name.endswith(".tmp")])


def run_daemon(cfg: Config, paths: Any, db_path: str) -> None:
    """Long-running loop. Polling watcher (inotify optional in future)."""

    # state
    state_path = Path(paths.state_dir) / "seen_files.json"
    state = _load_seen_state(state_path)

    file_sizes: dict[str, _FileSeen] = {}
    backoff: dict[str, float] = {}  # filepath -> next_attempt_ts

    codex: CodexClient | None = None
    if not cfg.replay_mode:
        # In replay mode, allow running without OPENAI_API_KEY (for deterministic tests).
        if cfg.openai_api_key:
            codex = CodexClient(api_key=cfg.openai_api_key, model=cfg.codex_model)
    else:
        if cfg.openai_api_key:
            codex = CodexClient(api_key=cfg.openai_api_key, model=cfg.codex_model)

    # main loop
    while True:
        try:
            if cfg.replay_mode:
                snapshots_dir = Path(paths.historical_dir)
            else:
                snapshots_dir = Path(paths.incoming_snapshots_dir)

            candidates = _list_candidate_snapshots(snapshots_dir)

            # Attempt scoring periodically, even if no new snapshots.
            score_due_predictions(
                cfg=cfg,
                paths=paths,
                db_path=db_path,
                state=state,
            )

            processed_any = False

            for p in candidates:
                # Basic backoff on failures
                now = time.time()
                next_ts = backoff.get(str(p))
                if next_ts is not None and now < next_ts:
                    continue

                st = p.stat()
                prev = file_sizes.get(str(p))
                if prev is None:
                    file_sizes[str(p)] = _FileSeen(size=st.st_size, last_change_ts=now)
                    continue

                if st.st_size != prev.size:
                    file_sizes[str(p)] = _FileSeen(size=st.st_size, last_change_ts=now)
                    continue

                # stable?
                if now - prev.last_change_ts < cfg.file_stable_seconds:
                    continue

                # Compute hash and idempotency check
                file_hash = _sha256_file(p)

                if hash_exists(db_path, file_hash):
                    # already processed
                    state.setdefault("seen_hashes", [])
                    if file_hash not in state["seen_hashes"]:
                        state["seen_hashes"].append(file_hash)
                        _save_seen_state(state_path, state)
                    try:
                        # move duplicates to processed dir for cleanliness
                        dest = Path(paths.processed_snapshots_dir) / p.name
                        if dest.exists():
                            p.unlink(missing_ok=True)
                        else:
                            p.replace(dest)
                    except Exception:
                        pass
                    continue

                # Ingest + prediction
                try:
                    ingest_res: IngestResult = ingest_snapshot_file(
                        cfg=cfg,
                        paths=paths,
                        db_path=db_path,
                        snapshot_path=p,
                        snapshot_hash=file_hash,
                        codex=codex,
                        state=state,
                    )
                    processed_any = processed_any or ingest_res.processed

                    # persist state after successful ingest (includes snapshot_index updates)
                    _save_seen_state(state_path, state)

                except Exception as e:
                    # exponential backoff per file
                    delay = backoff.get(str(p) + ":delay", cfg.watch_poll_seconds)
                    delay = min(max(delay * 2, cfg.watch_poll_seconds), 60.0)
                    backoff[str(p) + ":delay"] = delay
                    backoff[str(p)] = time.time() + delay
                    log_daemon_event(paths.logs_daemon_dir, "error", "snapshot_process_error", file=str(p), error=str(e), backoff_seconds=delay)

            if cfg.replay_mode and not candidates:
                # nothing left in historical; sleep a bit
                time.sleep(cfg.watch_poll_seconds)
            else:
                # normal watch
                time.sleep(cfg.watch_poll_seconds if not processed_any else 0.1)

        except Exception as e:
            log_daemon_event(paths.logs_daemon_dir, "error", "watch_loop_error", error=str(e))
            time.sleep(2.0)
