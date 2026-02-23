from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


Result = Literal[
    "correct",
    "wrong_direction",
    "correct_direction_wrong_magnitude",
    "inconclusive",
]


@dataclass(frozen=True)
class ScoreOutput:
    actual_move: float
    result: Result


def score_prediction(
    predicted_direction: str,
    predicted_magnitude: float,
    price_at_prediction: float,
    price_at_outcome: float,
    flat_threshold: float = 0.001,
    magnitude_tolerance: float = 0.002,
) -> ScoreOutput:
    if price_at_prediction <= 0:
        raise ValueError("price_at_prediction must be > 0")

    actual_move = (price_at_outcome - price_at_prediction) / price_at_prediction

    if abs(actual_move) < flat_threshold:
        return ScoreOutput(actual_move=actual_move, result="inconclusive")

    # direction correctness
    actual_dir = "bullish" if actual_move > 0 else "bearish"
    if predicted_direction == "neutral":
        # treat neutral as wrong unless market flat, which is already handled
        return ScoreOutput(actual_move=actual_move, result="wrong_direction")

    if predicted_direction != actual_dir:
        return ScoreOutput(actual_move=actual_move, result="wrong_direction")

    # magnitude correctness (predicted_magnitude is a decimal fraction, e.g. 0.002)
    if abs(abs(actual_move) - abs(predicted_magnitude)) <= magnitude_tolerance:
        return ScoreOutput(actual_move=actual_move, result="correct")

    return ScoreOutput(actual_move=actual_move, result="correct_direction_wrong_magnitude")


def simulate_pnl(predicted_direction: str, actual_move: float) -> float:
    if predicted_direction == "bullish":
        return actual_move
    if predicted_direction == "bearish":
        return -actual_move
    return 0.0
