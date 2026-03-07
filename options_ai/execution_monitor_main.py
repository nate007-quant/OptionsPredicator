from __future__ import annotations

import time
from pathlib import Path

from options_ai.brokers.tastytrade.client import TastytradeClient
from options_ai.config import load_config
from options_ai.db import db_path_from_url, init_db
from options_ai.execution.monitor import ExecutionMonitor
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

    poll_s = max(1, int(getattr(cfg, "execution_monitor_poll_seconds", 10)))

    log_daemon_event(
        paths.logs_daemon_dir,
        "info",
        "execution_monitor_start",
        trading_enabled=cfg.trading_enabled,
        broker_name=cfg.broker_name,
        broker_env=cfg.broker_env,
        poll_seconds=poll_s,
    )
    client_sandbox = TastytradeClient(
        base_url=(cfg.tasty_sandbox_base_url or cfg.tasty_base_url),
        streamer_url=(cfg.tasty_sandbox_streamer_url or cfg.tasty_streamer_url),
        environment='sandbox',
        account_number=(cfg.tasty_sandbox_account_number or None),
        dry_run=(not cfg.trading_enabled),
        target_api_version=cfg.target_api_version,
    )
    mon_sandbox = ExecutionMonitor(
        db_path=db_path,
        environment='sandbox',
        broker_name=cfg.broker_name,
        account_number=(client_sandbox.account_number or ""),
        client=client_sandbox,
        max_position_mismatch_count=cfg.max_position_mismatch_count,
        max_streamer_downtime_seconds=cfg.max_streamer_downtime_seconds,
    )

    client_live = TastytradeClient(
        base_url=(cfg.tasty_live_base_url or cfg.tasty_base_url),
        streamer_url=(cfg.tasty_live_streamer_url or cfg.tasty_streamer_url),
        environment='live',
        account_number=(cfg.tasty_live_account_number or None),
        dry_run=(not (cfg.trading_enabled and cfg.live_execution_enabled and cfg.live_armed)),
        target_api_version=cfg.target_api_version,
    )
    mon_live = ExecutionMonitor(
        db_path=db_path,
        environment='live',
        broker_name=cfg.broker_name,
        account_number=(client_live.account_number or ""),
        client=client_live,
        max_position_mismatch_count=cfg.max_position_mismatch_count,
        max_streamer_downtime_seconds=cfg.max_streamer_downtime_seconds,
    )

    try:
        while True:
            st_sbx = mon_sandbox.process_once(limit=200)
            if int(st_sbx.get("updated") or 0) > 0 or int(st_sbx.get("errors") or 0) > 0:
                log_daemon_event(paths.logs_daemon_dir, "info", "execution_monitor_poll", env='sandbox', **st_sbx)

            if cfg.live_execution_enabled:
                st_live = mon_live.process_once(limit=200)
                if int(st_live.get("updated") or 0) > 0 or int(st_live.get("errors") or 0) > 0:
                    log_daemon_event(paths.logs_daemon_dir, "info", "execution_monitor_poll", env='live', **st_live)

            time.sleep(poll_s)
    except KeyboardInterrupt:
        log_daemon_event(paths.logs_daemon_dir, "info", "execution_monitor_stop_keyboard")
    except Exception as e:
        lg = get_logger()
        if lg:
            lg.exception(level="CRITICAL", component="ExecutionMonitor", event="execution_monitor_crash", message="execution monitor crashed", file_key="errors", exc=e)
        else:
            log_daemon_event(paths.logs_daemon_dir, "error", "execution_monitor_crash", error=str(e))
        raise


if __name__ == "__main__":
    main()
