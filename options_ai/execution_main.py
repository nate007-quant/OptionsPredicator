from __future__ import annotations

import time
from pathlib import Path

from options_ai.config import load_config
from options_ai.db import db_path_from_url, init_db
from options_ai.execution.executor import ExecutionExecutor, RepricePolicy
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

    poll_s = max(1, int(getattr(cfg, "execution_poll_seconds", 5)))

    log_daemon_event(
        paths.logs_daemon_dir,
        "info",
        "execution_executor_start",
        trading_enabled=cfg.trading_enabled,
        broker_name=cfg.broker_name,
        broker_env=cfg.broker_env,
        poll_seconds=poll_s,
    )

    if cfg.trading_enabled:
        log_daemon_event(
            paths.logs_daemon_dir,
            "warn",
            "trading_enabled_startup_warning",
            trading_enabled=cfg.trading_enabled,
            broker_name=cfg.broker_name,
            broker_env=cfg.broker_env,
            message_detail="TRADING_ENABLED=true. Executor will submit orders to broker unless dry-run is forced.",
        )

    ex = ExecutionExecutor(
        db_path=db_path,
        environment=cfg.broker_env,
        broker_name=cfg.broker_name,
        session_tz=cfg.session_tz,
        trading_enabled=cfg.trading_enabled,
        max_daily_loss_usd=cfg.max_daily_loss_usd,
        reprice_defaults=RepricePolicy(
            max_attempts=cfg.reprice_max_attempts,
            step=cfg.reprice_step,
            interval_seconds=cfg.reprice_interval_seconds,
            max_total_concession=cfg.reprice_max_total_concession,
        ),
        close_only_mode=cfg.close_only_mode,
        pretrade_required_checks=cfg.pretrade_required_checks,
        require_complex_exit_orders=cfg.require_complex_exit_orders,
        require_broker_external_identifier=cfg.require_broker_external_identifier,
        max_reject_streak=cfg.max_reject_streak,
    )

    try:
        while True:
            st = ex.process_once(limit=25)
            if int(st.get("processed") or 0) > 0:
                log_daemon_event(paths.logs_daemon_dir, "info", "execution_executor_poll", **st)
            time.sleep(poll_s)
    except KeyboardInterrupt:
        log_daemon_event(paths.logs_daemon_dir, "info", "execution_executor_stop_keyboard")
    except Exception as e:
        lg = get_logger()
        if lg:
            lg.exception(level="CRITICAL", component="ExecutionExecutor", event="execution_executor_crash", message="execution executor crashed", file_key="errors", exc=e)
        else:
            log_daemon_event(paths.logs_daemon_dir, "error", "execution_executor_crash", error=str(e))
        raise


if __name__ == "__main__":
    main()
