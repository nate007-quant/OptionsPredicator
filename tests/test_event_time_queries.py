from __future__ import annotations

import json
from pathlib import Path

import pytest

from options_ai.db import init_db
from options_ai.queries import fetch_gex_levels_before, fetch_recent_predictions_before, insert_prediction


def _init_tmp_db(tmp_path: Path) -> str:
    db_path = str(tmp_path / "pred.db")
    schema = Path(__file__).resolve().parents[1] / "options_ai" / "db" / "schema.sql"
    init_db(db_path, str(schema))
    return db_path


def test_fetch_recent_predictions_before_filters(tmp_path: Path):
    db_path = _init_tmp_db(tmp_path)

    # Insert three predictions at t1, t2, t3
    def ins(obs: str, d: str):
        row = {
            "timestamp": obs,
            "observed_ts_utc": obs,
            "outcome_ts_utc": obs,
            "ticker": "SPX",
            "expiration_date": "2026-01-01",
            "source_snapshot_file": "x.json",
            "source_snapshot_hash": obs,
            "chart_file": None,
            "spot_price": 6000.0,
            "signals_used": "{}",
            "chart_description": None,
            "predicted_direction": d,
            "predicted_magnitude": 0.1,
            "confidence": 0.5,
            "strategy_suggested": "",
            "reasoning": "",
            "prompt_version": "v",
            "model_used": "m",
            "model_provider": "local",
            "routing_reason": "r",
            "price_at_prediction": 6000.0,
        }
        assert insert_prediction(db_path, row) is not None

    ins("2026-02-24T10:00:00+00:00", "neutral")
    ins("2026-02-24T10:05:00+00:00", "bullish")
    ins("2026-02-24T10:10:00+00:00", "bearish")

    out = fetch_recent_predictions_before(db_path, "2026-02-24T10:10:00+00:00", limit=10)
    assert [r["predicted_direction"] for r in out] == ["bullish", "neutral"]


def test_fetch_gex_levels_before(tmp_path: Path):
    db_path = _init_tmp_db(tmp_path)

    def ins(obs: str, call_wall: float | None):
        signals = {
            "computed": {
                "gex": {"levels": {"call_wall": call_wall, "put_wall": None, "magnet": None, "flip": None}}
            }
        }
        row = {
            "timestamp": obs,
            "observed_ts_utc": obs,
            "outcome_ts_utc": obs,
            "ticker": "SPX",
            "expiration_date": "2026-01-01",
            "source_snapshot_file": "x.json",
            "source_snapshot_hash": obs + str(call_wall),
            "chart_file": None,
            "spot_price": 6000.0,
            "signals_used": json.dumps(signals),
            "chart_description": None,
            "predicted_direction": "neutral",
            "predicted_magnitude": 0.0,
            "confidence": 0.0,
            "strategy_suggested": "",
            "reasoning": "",
            "prompt_version": "v",
            "model_used": "m",
            "model_provider": "local",
            "routing_reason": "r",
            "price_at_prediction": 6000.0,
        }
        insert_prediction(db_path, row)

    ins("2026-02-24T09:59:00+00:00", 6100.0)
    ins("2026-02-24T10:01:00+00:00", 6110.0)

    levels = fetch_gex_levels_before(
        db_path,
        observed_ts_utc="2026-02-24T10:02:00+00:00",
        day_key_utc="2026-02-24",
        limit=10,
    )
    assert levels[:2] == [6110.0, 6100.0]
