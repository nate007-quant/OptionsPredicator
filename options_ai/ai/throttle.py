from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass
from typing import Any

from options_ai.ai.codex_client import CodexClient


@dataclass
class RateLimiter:
    max_per_minute: int = 0  # 0 = unlimited
    max_per_hour: int = 0  # 0 = unlimited

    def __post_init__(self) -> None:
        self._min_calls: deque[float] = deque()
        self._hour_calls: deque[float] = deque()

    def _prune(self, now: float) -> None:
        # keep only last 60s / 3600s
        while self._min_calls and now - self._min_calls[0] >= 60.0:
            self._min_calls.popleft()
        while self._hour_calls and now - self._hour_calls[0] >= 3600.0:
            self._hour_calls.popleft()

    def acquire(self) -> None:
        """Blocks until a call is allowed, then records the call."""
        while True:
            now = time.time()
            self._prune(now)

            min_ok = (self.max_per_minute <= 0) or (len(self._min_calls) < self.max_per_minute)
            hour_ok = (self.max_per_hour <= 0) or (len(self._hour_calls) < self.max_per_hour)

            if min_ok and hour_ok:
                self._min_calls.append(now)
                self._hour_calls.append(now)
                return

            # compute sleep until next slot
            sleeps = []
            if self.max_per_minute > 0 and self._min_calls:
                sleeps.append(max(0.01, 60.0 - (now - self._min_calls[0])))
            if self.max_per_hour > 0 and self._hour_calls:
                sleeps.append(max(0.01, 3600.0 - (now - self._hour_calls[0])))
            time.sleep(min(sleeps) if sleeps else 0.1)


class ThrottledCodexClient(CodexClient):
    def __init__(self, *, token_manager, model: str, limiter: RateLimiter):
        super().__init__(token_manager=token_manager, model=model)
        self._limiter = limiter

    def extract_chart_description(self, png_path: str, system_prompt: str, user_prompt: str) -> tuple[str, dict[str, Any]]:
        self._limiter.acquire()
        return super().extract_chart_description(png_path, system_prompt, user_prompt)

    def generate_prediction(self, system_prompt: str, user_prompt: str) -> tuple[str, dict[str, Any]]:
        self._limiter.acquire()
        return super().generate_prediction(system_prompt, user_prompt)
