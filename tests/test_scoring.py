from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from options_ai.config import Config
from options_ai.db import db_path_from_url, init_db
from options_ai.processes.scorer import score_due_predictions
from options_ai.queries import insert_prediction
from options_ai.utils.paths import build_paths, ensure_runtime_dirs


def test_scoring_updates_prediction_row(tmp_path: Path):
    data_root = tmp_path / "data_root"
    cfg = Config(
        openai_api_key="",
        database_url=f"sqlite:////{tmp_path}/predictions.db",
        ticker="SPX",
        data_root=str(data_root),
        outcome_delay_minutes=15,
        replay_mode=True,
    )

    paths = build_paths(cfg.data_root, cfg.ticker)
    ensure_runtime_dirs(paths)

    db_path = db_path_from_url(cfg.database_url)
    schema_path = Path(__file__).resolve().parents[1] / "options_ai" / "db" / "schema.sql"
    init_db(db_path, str(schema_path))

    # Create a prediction 20 minutes ago, so it's eligible for scoring
    t0 = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(minutes=20)

    pred_row = {
        "timestamp": t0.isoformat(),
        "ticker": "SPX",
        "expiration_date": "2026-02-20",
        "source_snapshot_file": "SPX-100.0-2026-02-20-20260220-000000.json",
        "source_snapshot_hash": "x" * 64,
        "chart_file": None,
        "spot_price": 100.0,
        "signals_used": json.dumps({"computed": {}, "model_signals_used": ["test"]}),
        "chart_description": None,
        "predicted_direction": "bullish",
        "predicted_magnitude": 0.002,
        "confidence": 0.9,
        "strategy_suggested": "",
        "reasoning": "test",
        "prompt_version": "v2.2.0",
        "model_used": "deepseek-r1:8b",
        "model_provider": "local",
        "routing_reason": "test",
        "price_at_prediction": 100.0,
    }
    pred_id = insert_prediction(db_path, pred_row)
    assert pred_id is not None
    assert isinstance(pred_id, int)

    # Provide an outcome snapshot at/after t0 + 15m
    outcome_ts = (t0 + timedelta(minutes=15)).isoformat()
    state = {
        "snapshot_index": {
            outcome_ts: {"spot": 100.2, "file": "later.json"},
        }
    }

    scored = score_due_predictions(cfg=cfg, paths=paths, db_path=db_path, state=state)
    assert scored == 1

    # Verify DB updated
    import sqlite3

    conn = sqlite3.connect(db_path)
    try:
        cur = conn.execute("SELECT result, actual_move, price_at_outcome FROM predictions WHERE id=?", (pred_id,))
        row = cur.fetchone()
        assert row is not None
        result, actual_move, price_at_outcome = row
        assert result == "correct"
        assert abs(actual_move - 0.002) < 1e-9
        assert abs(price_at_outcome - 100.2) < 1e-9
    finally:
        conn.close()
