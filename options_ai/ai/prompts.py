from __future__ import annotations

import json
from typing import Any


CHART_EXTRACTION_SYSTEM = (
    "You are a chart describer. Describe only what is visible in the chart image. "
    "No prediction. No speculation. No options references. "
    "If something is unclear, say 'not clearly visible'. Produce 4–6 sentences."
)


def chart_extraction_user_prompt() -> str:
    return (
        "Describe the SPX chart image. Only visible facts (price direction, candles/bars, support/resistance lines if visible, volume if shown). "
        "No prediction, no speculation, no options discussion. 4–6 sentences."
    )


PREDICTION_SYSTEM = (
    "You are an SPX-only short-horizon prediction engine. Source of truth is the snapshot JSON summary and deterministic signals provided. "
    "Do not assume any data not provided. Use UTC timestamps. "
    "Self-calibration: if the provided performance summary or similar-condition accuracy indicates <45% accuracy with sample >=5, output neutral. "
    "If confidence < MIN_CONFIDENCE, strategy_suggested must be an empty string. "
    "Output MUST be JSON only and MUST match the required schema exactly. No markdown."
)


def prediction_user_prompt(
    *,
    snapshot_summary: dict[str, Any],
    signals: dict[str, Any],
    chart_description: str | None,
    recent_predictions: list[dict[str, Any]],
    performance_summary: dict[str, Any] | None,
    min_confidence: float,
) -> str:
    payload = {
        "snapshot_summary": snapshot_summary,
        "signals": signals,
        "chart_description": chart_description,
        "recent_predictions": recent_predictions,
        "performance_summary": performance_summary,
        "rules": {
            "min_confidence": min_confidence,
            "output_schema": {
                "predicted_direction": "bullish|bearish|neutral",
                "predicted_magnitude": "decimal",
                "confidence": "0-1",
                "strategy_suggested": "string",
                "signals_used": "array[string]",
                "reasoning": "2-3 sentences",
            },
        },
    }
    return (
        "Generate the next-15-minute directional prediction for SPX. "
        "Return JSON only. Required schema: {predicted_direction, predicted_magnitude, confidence, strategy_suggested, signals_used, reasoning}. "
        "Use the provided inputs strictly.\n\nINPUTS:\n"
        + json.dumps(payload, ensure_ascii=False)
    )
