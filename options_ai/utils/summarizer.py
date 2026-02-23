from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from typing import Any

from options_ai.queries import fetch_scored_predictions, fetch_total_predictions


@dataclass(frozen=True)
class PerformanceSummary:
    generated_at: str
    total_predictions: int
    total_scored: int
    overall_accuracy: float | None
    summary_json: dict[str, Any]


def build_performance_summary(db_path: str) -> PerformanceSummary:
    total_predictions = fetch_total_predictions(db_path)
    rows = fetch_scored_predictions(db_path)

    total_scored = len(rows)

    counts: dict[str, int] = {}
    correct = 0
    scored_non_inconclusive = 0
    correct_non_inconclusive = 0

    hi_total = 0
    hi_correct = 0

    for r in rows:
        res = r["result"]
        counts[res] = counts.get(res, 0) + 1
        if res == "correct":
            correct += 1
        if res != "inconclusive":
            scored_non_inconclusive += 1
            if res == "correct":
                correct_non_inconclusive += 1

        conf = r.get("confidence")
        if conf is not None and conf > 0.75:
            hi_total += 1
            if res == "correct":
                hi_correct += 1

    overall_accuracy = (correct / total_scored) if total_scored else None
    accuracy_ex_inconclusive = (
        correct_non_inconclusive / scored_non_inconclusive if scored_non_inconclusive else None
    )
    hi_accuracy = (hi_correct / hi_total) if hi_total else None

    summary = {
        "counts": counts,
        "accuracy_excluding_inconclusive": accuracy_ex_inconclusive,
        "high_confidence": {
            "threshold": 0.75,
            "total": hi_total,
            "correct": hi_correct,
            "accuracy": hi_accuracy,
        },
    }

    return PerformanceSummary(
        generated_at=datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        total_predictions=total_predictions,
        total_scored=total_scored,
        overall_accuracy=overall_accuracy,
        summary_json=summary,
    )


def performance_summary_to_row(ps: PerformanceSummary) -> dict[str, Any]:
    return {
        "generated_at": ps.generated_at,
        "total_predictions": ps.total_predictions,
        "total_scored": ps.total_scored,
        "overall_accuracy": ps.overall_accuracy,
        "summary_json": json.dumps(ps.summary_json, sort_keys=True),
    }
