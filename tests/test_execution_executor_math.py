from __future__ import annotations

from options_ai.execution.executor import compute_reprice_limit, is_entry_window_open


def test_compute_reprice_limit_debit_caps_concession() -> None:
    # base 1.00, step 0.05, attempt #5 would imply +0.20 but cap is 0.15
    p = compute_reprice_limit(
        base_limit=1.00,
        attempt_num=5,
        price_effect="DEBIT",
        step=0.05,
        max_total_concession=0.15,
    )
    assert p == 1.15


def test_compute_reprice_limit_credit_decreases_price() -> None:
    p = compute_reprice_limit(
        base_limit=1.20,
        attempt_num=3,
        price_effect="CREDIT",
        step=0.05,
        max_total_concession=0.15,
    )
    assert p == 1.10


def test_is_entry_window_open() -> None:
    assert is_entry_window_open(now_hhmm="08:45", first_trade_time_ct="08:30", last_new_entry_time_ct="09:30") is True
    assert is_entry_window_open(now_hhmm="08:10", first_trade_time_ct="08:30", last_new_entry_time_ct="09:30") is False
    assert is_entry_window_open(now_hhmm="09:45", first_trade_time_ct="08:30", last_new_entry_time_ct="09:30") is False
