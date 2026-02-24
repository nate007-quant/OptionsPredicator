from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from options_ai.config import Config
from options_ai.db import db_path_from_url, init_db
from options_ai.processes.ingest import ingest_snapshot_file
from options_ai.utils.paths import build_paths, ensure_runtime_dirs
from options_ai.utils.cache import sha256_file


def _write_snapshot(path: Path, exp_epoch: int, *, observed_utc: str) -> None:
    # minimal valid snapshot per validate_snapshot_json
    n = 2
    snap = {
        "s": "ok",
        "observed_utc": observed_utc,
        "optionSymbol": ["SPXW260220C06000000", "SPXW260220P06000000"],
        "underlying": ["SPX", "SPX"],
        "side": ["call", "put"],
        "strike": [6000.0, 6000.0],
        "expiration": [exp_epoch, exp_epoch],
        "bid": [1.0, 1.0],
        "ask": [2.0, 2.0],
        "openInterest": [100, 100],
        "volume": [10, 10],
        "iv": [0.2, 0.2],
        "delta": [0.5, -0.5],
        "gamma": [0.01, 0.01],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(snap), encoding="utf-8")


def test_ingest_uses_json_observed_time_over_filename(tmp_path: Path):
    data_root = tmp_path / "data_root"
    cfg = Config(
        openai_api_key="",
        database_url=f"sqlite:////{tmp_path}/predictions.db",
        ticker="SPX",
        data_root=str(data_root),
        replay_mode=True,
        llm_enabled=False,
        ml_enabled=False,
    )

    paths = build_paths(cfg.data_root, cfg.ticker)
    ensure_runtime_dirs(paths)

    db_path = db_path_from_url(cfg.database_url)
    schema_path = Path(__file__).resolve().parents[1] / "options_ai" / "db" / "schema.sql"
    init_db(db_path, str(schema_path))

    # filename time: 14:55:20Z (from name)
    name = "SPX-6903.63-2026-02-20-20260220-145520.json"
    snap_path = Path(paths.historical_dir) / name
    exp_epoch = int(datetime(2026, 2, 20, tzinfo=timezone.utc).timestamp())

    # JSON observed time: 20:55:20Z (6 hours later)
    _write_snapshot(snap_path, exp_epoch, observed_utc="2026-02-20T20:55:20Z")
    h = sha256_file(snap_path)

    res = ingest_snapshot_file(cfg=cfg, paths=paths, db_path=db_path, snapshot_path=snap_path, snapshot_hash=h, router=None, state={})
    assert res.processed is True

    import sqlite3

    con = sqlite3.connect(db_path)
    row = con.execute("SELECT observed_ts_utc FROM predictions ORDER BY id DESC LIMIT 1").fetchone()
    assert row is not None
    assert row[0].startswith("2026-02-20T20:55:20")
