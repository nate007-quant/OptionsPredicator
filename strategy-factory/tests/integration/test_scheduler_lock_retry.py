from __future__ import annotations

from pathlib import Path

import orchestrator.scheduler as sch


def test_lock_skip_if_active(tmp_path: Path):
    lock = tmp_path / "x.lock"
    lock.parent.mkdir(parents=True, exist_ok=True)
    lock.write_text("1")

    with sch.lock_guard(lock, skip_if_active=True) as ok:
        assert ok is False


def test_retry_logic(tmp_path: Path, monkeypatch):
    repo = tmp_path / "repo"
    (repo / "strategy-factory" / "configs").mkdir(parents=True)
    (repo / "strategy-factory" / "configs" / "scheduler.yaml").write_text(
        """
timezone: UTC
schedule_cron: "5 0 * * *"
default_run_time_utc: "00:05"
retries:
  max_attempts: 2
  retry_delay_seconds: 0
runtime:
  max_runtime_seconds: 100
  lock_file: "strategy-factory/artifacts/runs/.factory.lock"
  skip_if_active: true
""".strip()
    )

    calls = {"n": 0}

    def fake_run_pipeline(**kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("boom")
        return {"run_id": "ok"}

    monkeypatch.setattr(sch, "run_pipeline", fake_run_pipeline)
    out = sch.run_once_with_retry(repo_root=repo)
    assert out and out["run_id"] == "ok"
    assert calls["n"] == 2
