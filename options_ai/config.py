from __future__ import annotations

from dataclasses import dataclass
import json
import os

from dotenv import load_dotenv


@dataclass(frozen=True)
class Config:
    openai_api_key: str
    database_url: str
    ticker: str
    data_root: str

    # Execution (v1)
    trading_enabled: bool = False
    broker_name: str = "tastytrade"
    broker_env: str = "sandbox"  # sandbox|live
    tasty_base_url: str = "https://api.cert.tastyworks.com"
    tasty_streamer_url: str = ""

    # Risk/session (v1)
    max_daily_loss_usd: float = 300.0
    session_tz: str = "America/Chicago"
    force_close_minutes_before_end: int = 15

    # Reprice defaults (v1)
    reprice_max_attempts: int = 3
    reprice_step: float = 0.05
    reprice_interval_seconds: int = 25
    reprice_max_total_concession: float = 0.15

    execution_intent_poll_seconds: int = 30
    execution_poll_seconds: int = 5
    execution_monitor_poll_seconds: int = 10
    execution_risk_guard_poll_seconds: int = 15

    # Execution hardening v1.1
    target_api_version: str = "20260307"
    close_only_mode: bool = False
    max_reject_streak: int = 5
    max_streamer_downtime_seconds: int = 120
    max_allowed_entry_slippage_abs: float = 0.15
    max_position_mismatch_count: int = 3
    reconcile_interval_seconds: int = 10
    pretrade_required_checks: bool = True
    require_complex_exit_orders: bool = True
    require_broker_external_identifier: bool = True
    startup_reconcile_required: bool = True
    strict_quarantine_requires_operator_clear: bool = True
    live_armed: bool = False

    # Dual-target execution mode
    dual_env_execution_enabled: bool = True
    live_execution_enabled: bool = False
    tasty_sandbox_base_url: str = "https://api.cert.tastyworks.com"
    tasty_live_base_url: str = "https://api.tastyworks.com"
    tasty_sandbox_streamer_url: str = ""
    tasty_live_streamer_url: str = ""
    tasty_sandbox_account_number: str = ""
    tasty_live_account_number: str = ""

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

    # ML EOD (early 60m -> cash close) (v2.8)
    eod_ml_enabled: bool = False
    eod_asof_minutes: int = 60
    eod_levels_asof_snapshot_index: int = 0
    eod_model_version_lvl0: str = "ml_eod60_lvl0_v1"
    eod_model_version_lvl1: str = "ml_eod60_lvl1_v1"
    eod_features_version: str = "ml_eod_features_v1"
    open_snapshot_max_delay_min: int = 10
    close_snapshot_tolerance_min: int = 10
    eod_early_window_minutes: int = 60
    reject_lookahead_minutes: int = 30
    min_near_pts: float = 5.0
    k_near: float = 0.25
    min_reject_pts: float = 8.0
    k_reject: float = 0.40
    min_eod_band_pts: float = 8.0
    k_eod: float = 0.35
    eod_action_threshold: float = 0.85
    # ML runtime toggles + params (v2.7)
    ml_enabled: bool = False
    llm_enabled: bool = True
    ml_models_dir: str = "/mnt/options_ai/models"
    ml_model_version: str = "ml_v1"
    ml_features_version: str = "ml_features_v1"
    ml_action_threshold: float = 0.85
    ml_min_neutral_band_pts: float = 3.0
    ml_k_em: float = 0.20

    # Regime routing (v2.9)
    regime_enabled: bool = True
    regime_version: str = "regime_v1"
    regime_hysteresis_snapshots: int = 2
    regime_model_map_json: str = "{}"
    regime_fallback_model_version: str = "ml_v1"
    regime_min_confidence_for_routing: float = 0.55

    # GEX prompt compression (v2.3+)
    gex_neighbor_strikes: int = 2
    gex_topk_abs_strikes: int = 0
    gex_sticky_day_max: int = 20
    file_stable_seconds: int = 2
    watch_poll_seconds: float = 1.0
    pause_processing: bool = False
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

    # Flow phase3 v1 knobs
    flow_max_strike_dist_pct: float = 0.40
    flow_use_moneyness_weight: bool = True
    flow_use_gaussian: bool = True
    flow_winsorize_bucket: bool = True
    flow_confirm_fast_win: int = 10
    flow_confirm_slow_win: int = 60
    flow_history_per_strike: int = 600
    flow_bucket_z_window: int = 60
    flow_min_breadth: float = 0.60
    flow_min_bucket_z: float = 1.5
    flow_conf_min: float = 0.60
    flow_atm_corridor_pct: float = 0.01

    outcome_align: str = "FirstOfDay"
    outcome_horizons_td: str = "5,10,21"
    flat_pct_band: float = 0.0025

    # Backtest strategy gate (flow-based; disabled by default)
    backtest_flow_gate_enabled: bool = False
    backtest_flow_live_ok_filter_enabled: bool = False
    backtest_flow_gate_min_bucket_z: float = 1.5
    backtest_flow_gate_min_breadth: float = 0.60
    backtest_flow_gate_min_confidence: float = 0.60

    codex_model: str = "gpt-5.2-codex"
    prompt_version: str = "v2.3.0"


def _get_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in {"1", "true", "yes", "y", "on"}


def _get_json_object_string(name: str, default: str = "{}") -> str:
    raw = os.getenv(name, default)
    try:
        obj = json.loads(raw or "{}")
        if not isinstance(obj, dict):
            return default
        return json.dumps(obj, sort_keys=True)
    except Exception:
        return default


def load_config() -> Config:
    load_dotenv(override=False)

    openai_api_key = os.getenv("OPENAI_API_KEY", "").strip()
    database_url = os.getenv("DATABASE_URL", "").strip()
    ticker = os.getenv("TICKER", "SPX").strip().upper()
    data_root = os.getenv("DATA_ROOT", "/mnt/options_ai").strip()

    # Execution (v1)
    trading_enabled = _get_bool("TRADING_ENABLED", False)
    broker_name = os.getenv("BROKER_NAME", "tastytrade").strip().lower() or "tastytrade"
    broker_env = os.getenv("BROKER_ENV", "sandbox").strip().lower() or "sandbox"
    if broker_env not in {"sandbox", "live"}:
        broker_env = "sandbox"
    tasty_base_url = os.getenv("TASTY_BASE_URL", "https://api.cert.tastyworks.com").strip()
    tasty_streamer_url = os.getenv("TASTY_STREAMER_URL", "").strip()

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

    return Config(
        openai_api_key=openai_api_key,
        database_url=database_url,
        ticker=ticker,
        data_root=data_root,
        trading_enabled=trading_enabled,
        broker_name=broker_name,
        broker_env=broker_env,
        tasty_base_url=tasty_base_url,
        tasty_streamer_url=tasty_streamer_url,
        max_daily_loss_usd=float(os.getenv("MAX_DAILY_LOSS_USD", "300")),
        session_tz=os.getenv("SESSION_TZ", "America/Chicago").strip(),
        force_close_minutes_before_end=int(os.getenv("FORCE_CLOSE_MINUTES_BEFORE_END", "15")),
        reprice_max_attempts=int(os.getenv("REPRICE_MAX_ATTEMPTS", "3")),
        reprice_step=float(os.getenv("REPRICE_STEP", "0.05")),
        reprice_interval_seconds=int(os.getenv("REPRICE_INTERVAL_SECONDS", "25")),
        reprice_max_total_concession=float(os.getenv("REPRICE_MAX_TOTAL_CONCESSION", "0.15")),
        execution_intent_poll_seconds=int(os.getenv("EXECUTION_INTENT_POLL_SECONDS", "30")),
        execution_poll_seconds=int(os.getenv("EXECUTION_POLL_SECONDS", "5")),
        execution_monitor_poll_seconds=int(os.getenv("EXECUTION_MONITOR_POLL_SECONDS", "10")),
        execution_risk_guard_poll_seconds=int(os.getenv("EXECUTION_RISK_GUARD_POLL_SECONDS", "15")),
        target_api_version=os.getenv("TARGET_API_VERSION", "20260307").strip(),
        close_only_mode=_get_bool("CLOSE_ONLY_MODE", False),
        max_reject_streak=int(os.getenv("MAX_REJECT_STREAK", "5")),
        max_streamer_downtime_seconds=int(os.getenv("MAX_STREAMER_DOWNTIME_SECONDS", "120")),
        max_allowed_entry_slippage_abs=float(os.getenv("MAX_ALLOWED_ENTRY_SLIPPAGE_ABS", "0.15")),
        max_position_mismatch_count=int(os.getenv("MAX_POSITION_MISMATCH_COUNT", "3")),
        reconcile_interval_seconds=int(os.getenv("RECONCILE_INTERVAL_SECONDS", "10")),
        pretrade_required_checks=_get_bool("PRETRADE_REQUIRED_CHECKS", True),
        require_complex_exit_orders=_get_bool("REQUIRE_COMPLEX_EXIT_ORDERS", True),
        require_broker_external_identifier=_get_bool("REQUIRE_BROKER_EXTERNAL_IDENTIFIER", True),
        startup_reconcile_required=_get_bool("STARTUP_RECONCILE_REQUIRED", True),
        strict_quarantine_requires_operator_clear=_get_bool("STRICT_QUARANTINE_REQUIRES_OPERATOR_CLEAR", True),
        live_armed=_get_bool("LIVE_ARMED", False),
        dual_env_execution_enabled=_get_bool("DUAL_ENV_EXECUTION_ENABLED", True),
        live_execution_enabled=_get_bool("LIVE_EXECUTION_ENABLED", False),
        tasty_sandbox_base_url=os.getenv("TASTY_SANDBOX_BASE_URL", "https://api.cert.tastyworks.com").strip(),
        tasty_live_base_url=os.getenv("TASTY_LIVE_BASE_URL", "https://api.tastyworks.com").strip(),
        tasty_sandbox_streamer_url=os.getenv("TASTY_SANDBOX_STREAMER_URL", "").strip(),
        tasty_live_streamer_url=os.getenv("TASTY_LIVE_STREAMER_URL", "").strip(),
        tasty_sandbox_account_number=os.getenv("TASTY_SANDBOX_ACCOUNT_NUMBER", "").strip(),
        tasty_live_account_number=os.getenv("TASTY_LIVE_ACCOUNT_NUMBER", "").strip(),
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
        eod_ml_enabled=_get_bool("EOD_ML_ENABLED", False),
        eod_asof_minutes=int(os.getenv("EOD_ASOF_MINUTES", "60")),
        eod_levels_asof_snapshot_index=int(os.getenv("LEVELS_ASOF_SNAPSHOT_INDEX", "0")),
        eod_model_version_lvl0=os.getenv("EOD_MODEL_VERSION_LVL0", "ml_eod60_lvl0_v1").strip(),
        eod_model_version_lvl1=os.getenv("EOD_MODEL_VERSION_LVL1", "ml_eod60_lvl1_v1").strip(),
        eod_features_version=os.getenv("EOD_FEATURES_VERSION", "ml_eod_features_v1").strip(),
        open_snapshot_max_delay_min=int(os.getenv("OPEN_SNAPSHOT_MAX_DELAY_MIN", "10")),
        close_snapshot_tolerance_min=int(os.getenv("CLOSE_SNAPSHOT_TOLERANCE_MIN", "10")),
        eod_early_window_minutes=int(os.getenv("EOD_EARLY_WINDOW_MINUTES", "60")),
        reject_lookahead_minutes=int(os.getenv("REJECT_LOOKAHEAD_MINUTES", "30")),
        min_near_pts=float(os.getenv("MIN_NEAR_PTS", "5.0")),
        k_near=float(os.getenv("K_NEAR", "0.25")),
        min_reject_pts=float(os.getenv("MIN_REJECT_PTS", "8.0")),
        k_reject=float(os.getenv("K_REJECT", "0.40")),
        min_eod_band_pts=float(os.getenv("MIN_EOD_BAND_PTS", "8.0")),
        k_eod=float(os.getenv("K_EOD", "0.35")),
        eod_action_threshold=float(os.getenv("EOD_ACTION_THRESHOLD", "0.85")),
        ml_enabled=_get_bool("ML_ENABLED", False),
        llm_enabled=_get_bool("LLM_ENABLED", True),
        ml_models_dir=os.getenv("ML_MODELS_DIR", "/mnt/options_ai/models").strip(),
        ml_model_version=os.getenv("ML_MODEL_VERSION", "ml_v1").strip(),
        ml_features_version=os.getenv("ML_FEATURES_VERSION", "ml_features_v1").strip(),
        ml_action_threshold=float(os.getenv("ML_ACTION_THRESHOLD", "0.85")),
        ml_min_neutral_band_pts=float(os.getenv("ML_MIN_NEUTRAL_BAND_PTS", "3.0")),
        ml_k_em=float(os.getenv("ML_K_EM", "0.20")),
        regime_enabled=_get_bool("REGIME_ENABLED", True),
        regime_version=os.getenv("REGIME_VERSION", "regime_v1").strip() or "regime_v1",
        regime_hysteresis_snapshots=int(os.getenv("REGIME_HYSTERESIS_SNAPSHOTS", "2")),
        regime_model_map_json=_get_json_object_string("REGIME_MODEL_MAP_JSON", "{}"),
        regime_fallback_model_version=os.getenv("REGIME_FALLBACK_MODEL_VERSION", "ml_v1").strip() or "ml_v1",
        regime_min_confidence_for_routing=float(os.getenv("REGIME_MIN_CONFIDENCE_FOR_ROUTING", "0.55")),
        gex_neighbor_strikes=int(os.getenv("GEX_NEIGHBOR_STRIKES", "2")),
        gex_topk_abs_strikes=int(os.getenv("GEX_TOPK_ABS_STRIKES", "0")),
        gex_sticky_day_max=int(os.getenv("GEX_STICKY_DAY_MAX", "20")),
        file_stable_seconds=int(os.getenv("FILE_STABLE_SECONDS", "2")),
        watch_poll_seconds=float(os.getenv("WATCH_POLL_SECONDS", "1")),
        pause_processing=_get_bool("PAUSE_PROCESSING", False),
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
        flow_max_strike_dist_pct=float(os.getenv("FLOW_MAX_STRIKE_DIST_PCT", "0.40")),
        flow_use_moneyness_weight=_get_bool("FLOW_USE_MONEYNESS_WEIGHT", True),
        flow_use_gaussian=_get_bool("FLOW_USE_GAUSSIAN", True),
        flow_winsorize_bucket=_get_bool("FLOW_WINSORIZE_BUCKET", True),
        flow_confirm_fast_win=int(os.getenv("FLOW_CONFIRM_FAST_WIN", "10")),
        flow_confirm_slow_win=int(os.getenv("FLOW_CONFIRM_SLOW_WIN", "60")),
        flow_history_per_strike=int(os.getenv("FLOW_HISTORY_PER_STRIKE", "600")),
        flow_bucket_z_window=int(os.getenv("FLOW_BUCKET_Z_WINDOW", "60")),
        flow_min_breadth=float(os.getenv("FLOW_MIN_BREADTH", "0.60")),
        flow_min_bucket_z=float(os.getenv("FLOW_MIN_BUCKET_Z", "1.5")),
        flow_conf_min=float(os.getenv("FLOW_CONF_MIN", "0.60")),
        flow_atm_corridor_pct=float(os.getenv("FLOW_ATM_CORRIDOR_PCT", "0.01")),
        outcome_align=os.getenv("OUTCOME_ALIGN", "FirstOfDay").strip() or "FirstOfDay",
        outcome_horizons_td=os.getenv("OUTCOME_HORIZONS_TD", "5,10,21").strip() or "5,10,21",
        flat_pct_band=float(os.getenv("FLAT_PCT_BAND", "0.0025")),
        backtest_flow_gate_enabled=_get_bool("BACKTEST_FLOW_GATE_ENABLED", False),
        backtest_flow_live_ok_filter_enabled=_get_bool("BACKTEST_FLOW_LIVE_OK_FILTER_ENABLED", False),
        backtest_flow_gate_min_bucket_z=float(os.getenv("BACKTEST_FLOW_GATE_MIN_BUCKET_Z", "1.5")),
        backtest_flow_gate_min_breadth=float(os.getenv("BACKTEST_FLOW_GATE_MIN_BREADTH", "0.60")),
        backtest_flow_gate_min_confidence=float(os.getenv("BACKTEST_FLOW_GATE_MIN_CONFIDENCE", "0.60")),
        codex_model=os.getenv("CODEX_MODEL", "gpt-5.2-codex").strip(),
        prompt_version=os.getenv("PROMPT_VERSION", "v2.3.0").strip(),
    )
