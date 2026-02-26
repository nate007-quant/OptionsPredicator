from __future__ import annotations

from datetime import timezone

import pytest

from options_ai.spx_chain_ingester import parse_chain_filename, validate_chain_json, build_rows


def test_parse_chain_filename_tz_to_utc() -> None:
    parsed = parse_chain_filename(
        "SPX-5940.17-2025-03-28-20250303-100058.json",
        filename_tz="America/Chicago",
    )
    assert parsed.underlying == "SPX"
    assert abs(parsed.underlying_price - 5940.17) < 1e-9
    assert parsed.expiration_date.isoformat() == "2025-03-28"
    assert parsed.snapshot_ts_utc.tzinfo == timezone.utc


def test_validate_chain_json_alignment() -> None:
    snap = {
        "status": "ok",
        "optionSymbol": ["A", "B"],
        "bid": [1.0, 2.0],
        "ask": [1.1, 2.1],
    }
    assert validate_chain_json(snap) == 2

    snap_bad = {"status": "ok", "optionSymbol": ["A", "B"], "bid": [1.0]}
    with pytest.raises(ValueError):
        validate_chain_json(snap_bad)


def test_build_rows_minimal() -> None:
    parsed = parse_chain_filename(
        "SPX-5940.17-2025-03-28-20250303-100058.json",
        filename_tz="America/Chicago",
    )
    snap = {
        "status": "ok",
        "optionSymbol": ["SYM1"],
        "underlyingPrice": [5950.0],
        "side": ["call"],
        "strike": [6000],
        "bid": [1.0],
        "ask": [1.2],
    }
    n = validate_chain_json(snap)
    rows = build_rows(snap, n=n, parsed=parsed)
    assert len(rows) == 1
    assert rows[0]["option_symbol"] == "SYM1"
    assert rows[0]["expiration_date"].isoformat() == "2025-03-28"
    assert rows[0]["underlying_price"] == 5950.0
