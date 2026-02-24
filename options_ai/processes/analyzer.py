from __future__ import annotations

import json
from typing import Any, Literal

from pydantic import BaseModel, Field, ValidationError

from options_ai.ai.codex_client import CodexClient, safe_json_loads_tolerant
from options_ai.ai.prompts import (
    prediction_user_prompt,
    CHART_EXTRACTION_SYSTEM,
    chart_extraction_user_prompt,
)


Direction = Literal["bullish", "bearish", "neutral"]


class PredictionOut(BaseModel):
    predicted_direction: Direction
    predicted_magnitude: float
    confidence: float = Field(ge=0.0, le=1.0)
    strategy_suggested: str
    signals_used: list[str]
    reasoning: str


def validate_prediction_obj(obj: Any, min_confidence: float) -> dict[str, Any]:
    m = PredictionOut.model_validate(obj)
    out = m.model_dump()

    # Enforce local rule: if confidence < min_confidence -> strategy must be empty.
    if out["confidence"] < min_confidence:
        out["strategy_suggested"] = ""

    return out


def run_chart_extraction_if_available(
    *,
    codex: CodexClient,
    chart_png_path: str | None,
    chart_max_output_tokens: int | None = None,
) -> tuple[str | None, dict[str, Any] | None]:
    if not chart_png_path:
        return None, None

    desc, report = codex.extract_chart_description(
        chart_png_path,
        system_prompt=CHART_EXTRACTION_SYSTEM,
        user_prompt=chart_extraction_user_prompt(),
        max_output_tokens=chart_max_output_tokens,
    )
    return desc, report


def run_prediction(
    *,
    codex: CodexClient,
    system_prompt: str,
    snapshot_summary: dict[str, Any],
    signals: dict[str, Any],
    chart_description: str | None,
    recent_predictions: list[dict[str, Any]],
    performance_summary: dict[str, Any] | None,
    min_confidence: float,
    prediction_max_output_tokens: int | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    user_prompt = prediction_user_prompt(
        snapshot_summary=snapshot_summary,
        signals=signals,
        chart_description=chart_description,
        recent_predictions=recent_predictions,
        performance_summary=performance_summary,
        min_confidence=min_confidence,
    )

    report: dict[str, Any] = {"prompt_chars": len(user_prompt)}
    raw_text, rep0 = codex.generate_prediction(
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        max_output_tokens=prediction_max_output_tokens,
    )
    try:
        report.update(rep0 or {})
    except Exception:
        report["provider_report"] = rep0

    # Tolerant JSON extraction first to avoid a second model call.
    last_err: str | None = None
    for attempt in range(2):
        try:
            obj = safe_json_loads_tolerant(raw_text)
            validated = validate_prediction_obj(obj, min_confidence=min_confidence)
            report["validated"] = validated
            report["attempts"] = attempt + 1
            report["raw_text"] = raw_text
            return validated, report
        except (json.JSONDecodeError, ValidationError) as e:
            last_err = str(e)
            if attempt == 0:
                # Retry once with an explicit correction instruction.
                correction = (
                    "Your previous response failed JSON parsing or schema validation. "
                    "Return ONLY a valid JSON object matching the required keys and types. "
                    "No surrounding text."
                )
                raw_text, report2 = codex.generate_prediction(
                    system_prompt=system_prompt,
                    user_prompt=user_prompt + "\n\n" + correction,
                    max_output_tokens=prediction_max_output_tokens,
                )
                report["retry_report"] = report2
                continue
            break

    report["parse_error"] = last_err
    report["raw_text"] = raw_text
    raise ValueError(f"Model output invalid after retry: {last_err}")
