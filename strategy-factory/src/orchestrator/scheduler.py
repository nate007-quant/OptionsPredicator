from __future__ import annotations

import os
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

from common import load_yaml_like
from orchestrator.pipeline import PipelineOptions, run_pipeline


@dataclass
class SchedulerConfig:
    timezone: str
    default_run_time_utc: str
    lock_file: Path
    max_attempts: int
    retry_delay_seconds: int
    max_runtime_seconds: int
    skip_if_active: bool


def load_scheduler_config(config_path: Path, repo_root: Path) -> SchedulerConfig:
    cfg = load_yaml_like(config_path)
    retries = cfg.get("retries") or {}
    runtime = cfg.get("runtime") or {}
    lock_rel = str(runtime.get("lock_file") or "strategy-factory/artifacts/runs/.factory.lock")
    return SchedulerConfig(
        timezone=str(cfg.get("timezone") or "UTC"),
        default_run_time_utc=str(cfg.get("default_run_time_utc") or "00:05"),
        lock_file=(repo_root / lock_rel),
        max_attempts=int(retries.get("max_attempts") or 2),
        retry_delay_seconds=int(retries.get("retry_delay_seconds") or 60),
        max_runtime_seconds=int(runtime.get("max_runtime_seconds") or 7200),
        skip_if_active=bool(runtime.get("skip_if_active") if runtime.get("skip_if_active") is not None else True),
    )


@contextmanager
def lock_guard(lock_path: Path, *, skip_if_active: bool) -> Iterator[bool]:
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    fd = None
    try:
        flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
        try:
            fd = os.open(str(lock_path), flags)
            os.write(fd, str(os.getpid()).encode("utf-8"))
            yield True
        except FileExistsError:
            if skip_if_active:
                yield False
            else:
                raise
    finally:
        if fd is not None:
            os.close(fd)
            try:
                lock_path.unlink(missing_ok=True)
            except Exception:
                pass


def run_once_with_retry(*, repo_root: Path, explicit_live_approval: bool = False) -> dict[str, Any] | None:
    cfg = load_scheduler_config(repo_root / "strategy-factory" / "configs" / "scheduler.yaml", repo_root)
    with lock_guard(cfg.lock_file, skip_if_active=cfg.skip_if_active) as acquired:
        if not acquired:
            return None

        last_exc: Exception | None = None
        start = time.time()
        for i in range(1, cfg.max_attempts + 1):
            if time.time() - start > cfg.max_runtime_seconds:
                raise TimeoutError("max runtime cutoff reached")
            try:
                return run_pipeline(repo_root=repo_root, opts=PipelineOptions(explicit_live_approval=explicit_live_approval))
            except Exception as e:
                last_exc = e
                if i < cfg.max_attempts:
                    time.sleep(cfg.retry_delay_seconds)
                    continue
                raise
        if last_exc:
            raise last_exc
        return None
