from __future__ import annotations

import json
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path

from options_ai.config import Config
from options_ai.db import db_path_from_url, init_db
from options_ai.processes.ingest import ingest_snapshot_file
from options_ai.utils.paths import build_paths, ensure_runtime_dirs


def _hash_file(p: Path) -> str:
    h = sha256()
    h.update(p.read_bytes())
    return h.hexdigest()


def _write_snapshot(p: Path, exp_epoch: int) -> None:
    snap = {
        "s": "ok",
        "optionSymbol": ["SPXW_000000C", "SPXW_000000P"],
        "underlying": ["SPX", "SPX"],
        "expiration": [exp_epoch, exp_epoch],
        "side": ["call", "put"],
        "strike": [6900, 6900],
        "bid": [1.0, 1.0],
        "ask": [2.0, 2.0],
        "openInterest": [10, 20],
        "volume": [1, 2],
        "iv": [0.2, 0.21],
        "delta": [0.5, -0.5],
        "gamma": [0.01, 0.01],
    }
    p.write_text(json.dumps(snap), encoding="utf-8")


def test_ingest_is_idempotent_by_hash(tmp_path: Path):
    data_root = tmp_path / "data_root"
    cfg = Config(
        openai_api_key="",
        database_url=f"sqlite:////{tmp_path}/predictions.db",
        ticker="SPX",
        data_root=str(data_root),
        replay_mode=True,
    )

    paths = build_paths(cfg.data_root, cfg.ticker)
    ensure_runtime_dirs(paths)

    db_path = db_path_from_url(cfg.database_url)
    schema_path = Path(__file__).resolve().parents[1] / "options_ai" / "db" / "schema.sql"
    init_db(db_path, str(schema_path))

    name = "SPX-6903.63-2026-02-20-20260220-145520.json"
    snap_path = Path(paths.historical_dir) / name
    exp_epoch = int(datetime(2026, 2, 20, tzinfo=timezone.utc).timestamp())
    _write_snapshot(snap_path, exp_epoch)

    state: dict = {}
    h = _hash_file(snap_path)

    r1 = ingest_snapshot_file(cfg=cfg, paths=paths, db_path=db_path, snapshot_path=snap_path, snapshot_hash=h, router=None, state=state)
    assert r1.processed is True

    r2 = ingest_snapshot_file(cfg=cfg, paths=paths, db_path=db_path, snapshot_path=snap_path, snapshot_hash=h, router=None, state=state)
    assert r2.processed is False
    assert r2.skipped_reason == "duplicate_hash_prompt_model"
