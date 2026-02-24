from __future__ import annotations

import json
from typing import Any, Literal

from pydantic import BaseModel, Field, ValidationError

from options_ai.ai.codex_client import CodexClient, safe_json_loads
from options_ai.ai.prompts import (
    PREDICTION_SYSTEM,
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
) -> tuple[str | None, dict[str, Any] | None]:
    if not chart_png_path:
        return None, None

    desc, report = codex.extract_chart_description(
        chart_png_path,
        system_prompt=CHART_EXTRACTION_SYSTEM,
        user_prompt=chart_extraction_user_prompt(),
    )
    return desc, report


def run_prediction(
    *,
    codex: CodexClient,
    snapshot_summary: dict[str, Any],
    signals: dict[str, Any],
    chart_description: str | None,
    recent_predictions: list[dict[str, Any]],
    performance_summary: dict[str, Any] | None,
    min_confidence: float,
) -> tuple[dict[str, Any], dict[str, Any]]:
    user_prompt = prediction_user_prompt(
        snapshot_summary=snapshot_summary,
        signals=signals,
        chart_description=chart_description,
        recent_predictions=recent_predictions,
        performance_summary=performance_summary,
        min_confidence=min_confidence,
    )

    report = {"prompt_chars": len(user_prompt)}
    raw_text, rep0 = codex.generate_prediction(system_prompt=PREDICTION_SYSTEM, user_prompt=user_prompt)
    try:
        report.update(rep0 or {})
    except Exception:
        report["provider_report"] = rep0

    # Validate strict JSON, retry once
    last_err: str | None = None
    for attempt in range(2):
        try:
            obj = safe_json_loads(raw_text)
            validated = validate_prediction_obj(obj, min_confidence=min_confidence)
            report["validated"] = validated
            report["attempts"] = attempt + 1
            report["raw_text"] = raw_text
            return validated, report
        except (json.JSONDecodeError, ValidationError) as e:
            last_err = str(e)
            if attempt == 0:
                # one retry with an explicit correction instruction
                correction = (
                    "Your previous response failed strict JSON schema validation. "
                    "Return ONLY a valid JSON object matching the required keys and types."
                )
                raw_text, report2 = codex.generate_prediction(
                    system_prompt=PREDICTION_SYSTEM,
                    user_prompt=user_prompt + "\n\n" + correction,
                )
                report["retry_report"] = report2
                continue
            break

    report["parse_error"] = last_err
    report["raw_text"] = raw_text
    raise ValueError(f"Model output invalid after retry: {last_err}")
