from __future__ import annotations

from datetime import timezone

import pytest

from options_ai.spx_chain_ingester import build_rows, parse_chain_filename, validate_chain_json


def test_parse_chain_filename_tz_to_utc() -> None:
    parsed = parse_chain_filename(
        "SPX-5940.17-2025-03-28-20250303-100058.json",
        filename_tz="America/Chicago",
    )
    assert parsed.underlying == "SPX"
    assert parsed.underlying_price is not None
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


def test_parse_and_build_rows_unknown_spot_token_prefers_json_underlying_price() -> None:
    parsed = parse_chain_filename(
        "SPX-Unknown-2026-04-10-20260226-145112.json",
        filename_tz="America/Chicago",
    )
    assert parsed.underlying == "SPX"
    assert parsed.underlying_price is None

    snap = {
        "status": "ok",
        "optionSymbol": ["SYM1"],
        "underlyingPrice": [6123.45],
        "side": ["call"],
        "strike": [6000],
    }
    n = validate_chain_json(snap)
    rows = build_rows(snap, n=n, parsed=parsed)
    assert rows[0]["underlying_price"] == 6123.45


def test_underlying_normalization_spxw_to_spx() -> None:
    parsed = parse_chain_filename(
        "SPXW-5940.17-2025-03-28-20250303-100058.json",
        filename_tz="America/Chicago",
    )
    assert parsed.underlying == "SPX"

    snap = {
        "status": "ok",
        "optionSymbol": ["SYM1"],
        "underlying": ["SPXW"],
        "underlyingPrice": [5950.0],
    }
    n = validate_chain_json(snap)
    rows = build_rows(snap, n=n, parsed=parsed)
    assert rows[0]["underlying"] == "SPX"


def test_nan_spot_token_treated_as_missing() -> None:
    parsed = parse_chain_filename(
        "SPX-NaN-2025-03-28-20250303-100058.json",
        filename_tz="America/Chicago",
    )
    assert parsed.underlying == "SPX"
    assert parsed.underlying_price is None

    snap = {
        "status": "ok",
        "optionSymbol": ["SYM1"],
        "underlyingPrice": [6050.0],
    }
    n = validate_chain_json(snap)
    rows = build_rows(snap, n=n, parsed=parsed)
    assert rows[0]["underlying_price"] == 6050.0


def test_underlying_price_null_rows_fill_with_snapshot_median() -> None:
    parsed = parse_chain_filename(
        "SPX-Unknown-2026-04-10-20260226-145112.json",
        filename_tz="America/Chicago",
    )

    snap = {
        "status": "ok",
        "optionSymbol": ["A", "B", "C"],
        "underlyingPrice": [None, 6000.0, None],
    }
    n = validate_chain_json(snap)
    rows = build_rows(snap, n=n, parsed=parsed)
    assert [r["underlying_price"] for r in rows] == [6000.0, 6000.0, 6000.0]


def test_missing_underlying_price_and_missing_numeric_filename_spot_errors() -> None:
    parsed = parse_chain_filename(
        "SPX-Unknown-2026-04-10-20260226-145112.json",
        filename_tz="America/Chicago",
    )

    snap = {
        "status": "ok",
        "optionSymbol": ["SYM1"],
        # underlyingPrice intentionally missing
    }
    n = validate_chain_json(snap)
    with pytest.raises(ValueError):
        build_rows(snap, n=n, parsed=parsed)
