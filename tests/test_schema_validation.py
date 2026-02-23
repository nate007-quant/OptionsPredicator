from __future__ import annotations

from datetime import datetime, timezone

import pytest

from options_ai.processes.ingest import parse_snapshot_filename, validate_snapshot_json


def _min_snapshot(exp_epoch: int) -> dict:
    return {
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


def test_filename_parsing_and_json_validation_passes():
    name = "SPX-6903.63-2026-02-20-20260220-145520.json"
    parsed = parse_snapshot_filename(name)
    assert parsed.ticker == "SPX"
    assert parsed.expiration_date == "2026-02-20"

    exp_epoch = int(datetime(2026, 2, 20, tzinfo=timezone.utc).timestamp())
    snap = _min_snapshot(exp_epoch)
    validate_snapshot_json(snap, parsed)


def test_side_validation_fails():
    name = "SPX-6903.63-2026-02-20-20260220-145520.json"
    parsed = parse_snapshot_filename(name)

    exp_epoch = int(datetime(2026, 2, 20, tzinfo=timezone.utc).timestamp())
    snap = _min_snapshot(exp_epoch)
    snap["side"][0] = "CALL"

    with pytest.raises(ValueError):
        validate_snapshot_json(snap, parsed)
