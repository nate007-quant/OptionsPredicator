from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class Idea:
    idea_id: str
    structure_type: str
    description: str
    seed_params: dict[str, Any]


def generate_ideas(*, run_id: str, max_ideas: int = 6) -> list[Idea]:
    # Deterministic starter set (options-native, includes multi-leg ideas).
    base = [
        ("VERTICAL", "ATM debit call spread, morning window", {"spread_style": "debit", "option_type": "CALL"}),
        ("VERTICAL", "ATM debit put spread, morning window", {"spread_style": "debit", "option_type": "PUT"}),
        ("IRON_CONDOR", "short condor with tight wings and event blackout", {"spread_style": "credit"}),
        ("STRANGLE", "long strangle around IV compression", {"spread_style": "debit"}),
        ("CALENDAR", "calendar spread with DTE stagger", {"spread_style": "debit"}),
        ("BUTTERFLY", "broken-wing butterfly with capped risk", {"spread_style": "debit"}),
    ]
    out: list[Idea] = []
    for i, (st, desc, params) in enumerate(base[: max_ideas]):
        out.append(Idea(idea_id=f"{run_id}-idea-{i+1}", structure_type=st, description=desc, seed_params=params))
    return out
