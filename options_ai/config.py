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

    # OAuth (v2.0)
    oauth_client_id: str = ""
    oauth_client_secret: str = ""
    oauth_token_url: str = ""
    oauth_scope: str = ""
    oauth_audience: str = ""
    oauth_refresh_margin_seconds: int = 60
    oauth_cache_path: str = "/mnt/options_ai/state/oauth_token.json"

    # optional tuning

    min_confidence: float = 0.65
    outcome_delay_minutes: int = 15
    history_records: int = 10
    local_history_records: int = 3
    similar_conditions_n: int = 3

    # Backtesting / event-time (v2.5)
    backtest_mode: bool = False
    backtest_disable_chart: bool = True

    # Tokens estimation (v2.6)
    tokens_per_char: float = 0.25
    tokens_estimation_mode: str = "chars"  # chars|tokenizer (future)

    # GEX prompt compression (v2.3+)
    gex_neighbor_strikes: int = 2
    gex_topk_abs_strikes: int = 0
    gex_sticky_day_max: int = 20
    file_stable_seconds: int = 2
    watch_poll_seconds: float = 1.0
    replay_mode: bool = False

    # bootstrap (v1.5+)
    bootstrap_enable: bool = True
    bootstrap_max_model_calls_per_min: int = 0  # 0 = unlimited
    bootstrap_max_model_calls_per_hour: int = 0  # 0 = unlimited

    # caching / reprocessing (v1.6)
    reprocess_mode: str = "none"  # none|from_model|from_summary|from_signals|full

    # Model routing (v2.2)
    model_force_local: bool = False
    model_force_remote: bool = False

    remote_model_name: str = "openai-codex/gpt-5.2"

    local_model_enabled: bool = True
    local_model_endpoint: str = "http://192.168.86.24:11434/v1"
    local_model_name: str = "deepseek-r1:8b"
    local_model_timeout_seconds: int = 60
    local_model_max_retries: int = 2

    # Prompt variants (v2.4)
    use_compact_system_prompt_local: bool = True

    # Output token caps (v2.4)
    prediction_max_output_tokens: int = 300
    chart_max_output_tokens: int = 160

    # Chart extraction gating (v2.4)
    chart_enabled: bool = False
    chart_local_enabled: bool = False
    chart_remote_enabled: bool = True

    codex_model: str = "gpt-5.2-codex"
    prompt_version: str = "v2.3.0"


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

    # OAuth (v2.0)
    oauth_client_id = os.getenv("OAUTH_CLIENT_ID", "").strip()
    oauth_client_secret = os.getenv("OAUTH_CLIENT_SECRET", "").strip()
    oauth_token_url = os.getenv("OAUTH_TOKEN_URL", "").strip()
    oauth_scope = os.getenv("OAUTH_SCOPE", "").strip()
    oauth_audience = os.getenv("OAUTH_AUDIENCE", "").strip()
    oauth_refresh_margin_seconds = int(os.getenv("OAUTH_REFRESH_MARGIN_SECONDS", "60"))
    oauth_cache_path = os.getenv("OAUTH_CACHE_PATH", f"{data_root}/state/oauth_token.json").strip()

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
        oauth_client_id=oauth_client_id,
        oauth_client_secret=oauth_client_secret,
        oauth_token_url=oauth_token_url,
        oauth_scope=oauth_scope,
        oauth_audience=oauth_audience,
        oauth_refresh_margin_seconds=oauth_refresh_margin_seconds,
        oauth_cache_path=oauth_cache_path,
        min_confidence=float(os.getenv("MIN_CONFIDENCE", "0.65")),
        outcome_delay_minutes=int(os.getenv("OUTCOME_DELAY", "15")),
        history_records=int(os.getenv("HISTORY_RECORDS", "10")),
        local_history_records=int(os.getenv("LOCAL_HISTORY_RECORDS", "3")),
        similar_conditions_n=int(os.getenv("SIMILAR_CONDITIONS_N", "3")),
        backtest_mode=_get_bool("BACKTEST_MODE", False),
        backtest_disable_chart=_get_bool("BACKTEST_DISABLE_CHART", True),
        tokens_per_char=float(os.getenv("TOKENS_PER_CHAR", "0.25")),
        tokens_estimation_mode=os.getenv("TOKENS_ESTIMATION_MODE", "chars").strip().lower(),
        gex_neighbor_strikes=int(os.getenv("GEX_NEIGHBOR_STRIKES", "2")),
        gex_topk_abs_strikes=int(os.getenv("GEX_TOPK_ABS_STRIKES", "0")),
        gex_sticky_day_max=int(os.getenv("GEX_STICKY_DAY_MAX", "20")),
        file_stable_seconds=int(os.getenv("FILE_STABLE_SECONDS", "2")),
        watch_poll_seconds=float(os.getenv("WATCH_POLL_SECONDS", "1")),
        replay_mode=_get_bool("REPLAY_MODE", False),
        bootstrap_enable=_get_bool("BOOTSTRAP_ENABLE", True),
        bootstrap_max_model_calls_per_min=int(os.getenv("BOOTSTRAP_MAX_MODEL_CALLS_PER_MIN", "0")),
        bootstrap_max_model_calls_per_hour=int(os.getenv("BOOTSTRAP_MAX_MODEL_CALLS_PER_HOUR", "0")),
        reprocess_mode=os.getenv("REPROCESS_MODE", "none").strip().lower(),
        model_force_local=_get_bool("MODEL_FORCE_LOCAL", False),
        model_force_remote=_get_bool("MODEL_FORCE_REMOTE", False),
        remote_model_name=os.getenv("REMOTE_MODEL_NAME", "openai-codex/gpt-5.2").strip(),
        local_model_enabled=_get_bool("LOCAL_MODEL_ENABLED", True),
        local_model_endpoint=os.getenv("LOCAL_MODEL_ENDPOINT", "http://192.168.86.24:11434/v1").strip(),
        local_model_name=os.getenv("LOCAL_MODEL_NAME", "deepseek-r1:8b").strip(),
        local_model_timeout_seconds=int(os.getenv("LOCAL_MODEL_TIMEOUT_SECONDS", "60")),
        local_model_max_retries=int(os.getenv("LOCAL_MODEL_MAX_RETRIES", "2")),
        use_compact_system_prompt_local=_get_bool("USE_COMPACT_SYSTEM_PROMPT_LOCAL", True),
        prediction_max_output_tokens=int(os.getenv("PREDICTION_MAX_OUTPUT_TOKENS", "300")),
        chart_max_output_tokens=int(os.getenv("CHART_MAX_OUTPUT_TOKENS", "160")),
        chart_enabled=_get_bool("CHART_ENABLED", False),
        chart_local_enabled=_get_bool("CHART_LOCAL_ENABLED", False),
        chart_remote_enabled=_get_bool("CHART_REMOTE_ENABLED", True),
        codex_model=os.getenv("CODEX_MODEL", "gpt-5.2-codex").strip(),
        prompt_version=os.getenv("PROMPT_VERSION", "v2.3.0").strip(),
    )
