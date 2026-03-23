from __future__ import annotations

from options_ai.ai.prompts import (
    chart_extraction_user_prompt,
    prediction_system_local,
    prediction_system_remote,
    prediction_user_prompt,
)


def test_prompts_support_custom_ticker() -> None:
    chart = chart_extraction_user_prompt(ticker="NDX")
    assert "NDX" in chart

    sys_remote = prediction_system_remote(ticker="NDX")
    sys_local = prediction_system_local(ticker="NDX")
    assert "NDX" in sys_remote
    assert "NDX" in sys_local

    user = prediction_user_prompt(
        snapshot_summary={"ticker": "NDX"},
        signals={},
        chart_description=None,
        recent_predictions=[],
        performance_summary=None,
        min_confidence=0.65,
        ticker="NDX",
    )
    assert "for NDX" in user
