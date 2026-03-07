from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


@dataclass
class ValidationResult:
    ok: bool
    errors: list[str]
    warnings: list[str]


class BacktestAdapter(Protocol):
    def validateSpec(self, spec: dict[str, Any]) -> ValidationResult:  # noqa: N802
        ...

    def runBacktest(self, request: dict[str, Any]) -> dict[str, Any]:  # noqa: N802
        ...

    def runStress(self, request: dict[str, Any], scenario: str) -> dict[str, Any]:  # noqa: N802
        ...

    def supports(self, feature: str) -> bool:
        ...
