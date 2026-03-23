from __future__ import annotations

from datetime import datetime, timezone

from options_ai.processes.scorer import _find_outcome_price


def test_find_outcome_price_uses_ticker_index_when_available() -> None:
    target = datetime(2026, 3, 23, 10, 0, tzinfo=timezone.utc)
    state = {
        "snapshot_index": {
            "2026-03-23T10:01:00+00:00": {"spot": 6100.0, "ticker": "SPX"},
        },
        "snapshot_index_by_ticker": {
            "SPX": {
                "2026-03-23T10:01:00+00:00": {"spot": 6100.0, "ticker": "SPX"},
            },
            "NDX": {
                "2026-03-23T10:01:00+00:00": {"spot": 20100.0, "ticker": "NDX"},
            },
        },
    }

    spx, _ = _find_outcome_price(state, target, ticker="SPX")
    ndx, _ = _find_outcome_price(state, target, ticker="NDX")

    assert spx == 6100.0
    assert ndx == 20100.0
