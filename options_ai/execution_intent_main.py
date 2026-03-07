from __future__ import annotations

import time
from pathlib import Path

from options_ai.config import load_config
from options_ai.db import db_path_from_url, init_db
from options_ai.execution.intent_builder import ExecutionIntentBuilder
from options_ai.utils.logger import get_logger, init_logger, log_daemon_event
from options_ai.utils.paths import build_paths, ensure_runtime_dirs


def main() -> None:
    cfg = load_config()
    paths = build_paths(cfg.data_root, cfg.ticker)
    ensure_runtime_dirs(paths)

    db_path = db_path_from_url(cfg.database_url)
    schema_path = Path(__file__).parent / "db" / "schema.sql"
    init_db(db_path, schema_sql_path=str(schema_path))

    init_logger(logs_root=paths.data_root / "logs", db_path=db_path)

    poll_s = max(1, int(getattr(cfg, "execution_intent_poll_seconds", 30)))

    log_daemon_event(
        paths.logs_daemon_dir,
        "info",
        "execution_intent_builder_start",
        broker_name=cfg.broker_name,
        broker_env=cfg.broker_env,
        trading_enabled=cfg.trading_enabled,
        poll_seconds=poll_s,
    )

    if cfg.trading_enabled:
        log_daemon_event(
            paths.logs_daemon_dir,
            "warn",
            "trading_enabled_startup_warning",
            broker_name=cfg.broker_name,
            broker_env=cfg.broker_env,
            message_detail="TRADING_ENABLED=true. Intent builder will continue writing pending intents.",
        )

    b = ExecutionIntentBuilder(
        db_path=db_path,
        environment=cfg.broker_env,
        broker_name=cfg.broker_name,
    )

    try:
        while True:
            st = b.build_once(limit=200)
            if int(st.get("inserted") or 0) > 0:
                log_daemon_event(
                    paths.logs_daemon_dir,
                    "info",
                    "execution_intent_builder_poll",
                    **st,
                )
            time.sleep(poll_s)
    except KeyboardInterrupt:
        log_daemon_event(paths.logs_daemon_dir, "info", "execution_intent_builder_stop_keyboard")
    except Exception as e:
        lg = get_logger()
        if lg:
            lg.exception(level="CRITICAL", component="ExecutionIntentBuilder", event="execution_intent_builder_crash", message="execution intent builder crashed", file_key="errors", exc=e)
        else:
            log_daemon_event(paths.logs_daemon_dir, "error", "execution_intent_builder_crash", error=str(e))
        raise


if __name__ == "__main__":
    main()
