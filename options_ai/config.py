from __future__ import annotations

from dataclasses import dataclass
import os

from dotenv import load_dotenv


@dataclass(frozen=True)
class Config:
    openai_api_key: str
    database_url: str
    ticker: str
    data_root: str

    # optional tuning
    min_confidence: float = 0.65
    outcome_delay_minutes: int = 15
    history_records: int = 10
    similar_conditions_n: int = 3
    file_stable_seconds: int = 2
    watch_poll_seconds: float = 1.0
    replay_mode: bool = False

    codex_model: str = "gpt-5.2-codex"
    prompt_version: str = "v1.3"


def _get_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in {"1", "true", "yes", "y", "on"}


def load_config() -> Config:
    load_dotenv(override=False)

    openai_api_key = os.getenv("OPENAI_API_KEY", "").strip()
    database_url = os.getenv("DATABASE_URL", "").strip()
    ticker = os.getenv("TICKER", "SPX").strip().upper()
    data_root = os.getenv("DATA_ROOT", "/mnt/options_ai").strip()

    if ticker != "SPX":
        raise RuntimeError(f"v1 supports SPX only (TICKER={ticker!r})")

    if not database_url:
        raise RuntimeError("DATABASE_URL is required")

    # OPENAI_API_KEY is required for live runs; tests may omit it.

    return Config(
        openai_api_key=openai_api_key,
        database_url=database_url,
        ticker=ticker,
        data_root=data_root,
        min_confidence=float(os.getenv("MIN_CONFIDENCE", "0.65")),
        outcome_delay_minutes=int(os.getenv("OUTCOME_DELAY", "15")),
        history_records=int(os.getenv("HISTORY_RECORDS", "10")),
        similar_conditions_n=int(os.getenv("SIMILAR_CONDITIONS_N", "3")),
        file_stable_seconds=int(os.getenv("FILE_STABLE_SECONDS", "2")),
        watch_poll_seconds=float(os.getenv("WATCH_POLL_SECONDS", "1")),
        replay_mode=_get_bool("REPLAY_MODE", False),
        codex_model=os.getenv("CODEX_MODEL", "gpt-5.2-codex").strip(),
        prompt_version=os.getenv("PROMPT_VERSION", "v1.3").strip(),
    )
