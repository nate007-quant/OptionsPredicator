from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from common import LIFECYCLE_STATES


TRANSITIONS: dict[str, set[str]] = {
    "PROPOSED": {"SPECIFIED", "RETIRED"},
    "SPECIFIED": {"TESTED", "RETIRED"},
    "TESTED": {"ROBUST", "RETIRED"},
    "ROBUST": {"CANDIDATE", "RETIRED"},
    "CANDIDATE": {"PAPER", "RETIRED"},
    "PAPER": {"LIVE", "RETIRED"},
    "LIVE": {"RETIRED", "PAPER"},
    "RETIRED": set(),
}


@dataclass
class TransitionResult:
    allowed: bool
    reason: str


def validate_transition(current: str, target: str, *, approval_live: bool, severe_breach: bool = False) -> TransitionResult:
    if current not in LIFECYCLE_STATES or target not in LIFECYCLE_STATES:
        return TransitionResult(False, "unknown state")

    if severe_breach and target == "RETIRED":
        return TransitionResult(True, "severe breach retirement")

    if target not in TRANSITIONS.get(current, set()):
        return TransitionResult(False, f"invalid transition {current}->{target}")

    if current == "PAPER" and target == "LIVE" and not approval_live:
        return TransitionResult(False, "PAPER->LIVE requires explicit approval")

    return TransitionResult(True, "ok")


def next_state_for_test_outcome(*, spec_valid: bool, backtest_ok: bool, gates_ok: bool) -> str:
    if not spec_valid:
        return "PROPOSED"
    if spec_valid and not backtest_ok:
        return "SPECIFIED"
    if backtest_ok and not gates_ok:
        return "TESTED"
    return "ROBUST"
