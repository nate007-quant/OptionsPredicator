from __future__ import annotations

import time
from pathlib import Path

from options_ai.brokers.tastytrade.client import TastytradeClient
from options_ai.config import load_config
from options_ai.db import db_path_from_url, init_db
from options_ai.execution.risk_guard import RiskGuard
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

    poll_s = max(1, int(getattr(cfg, "execution_risk_guard_poll_seconds", 15)))

    log_daemon_event(
        paths.logs_daemon_dir,
        "info",
        "execution_risk_guard_start",
        trading_enabled=cfg.trading_enabled,
        broker_name=cfg.broker_name,
        broker_env=cfg.broker_env,
        poll_seconds=poll_s,
        session_tz=cfg.session_tz,
        max_daily_loss_usd=cfg.max_daily_loss_usd,
        force_close_minutes_before_end=cfg.force_close_minutes_before_end,
    )

    client = TastytradeClient(
        base_url=cfg.tasty_base_url,
        streamer_url=cfg.tasty_streamer_url,
        environment=cfg.broker_env,
        dry_run=(not cfg.trading_enabled),
        target_api_version=cfg.target_api_version,
    )

    rg = RiskGuard(
        db_path=db_path,
        environment=cfg.broker_env,
        broker_name=cfg.broker_name,
        session_tz=cfg.session_tz,
        max_daily_loss_usd=cfg.max_daily_loss_usd,
        force_close_minutes_before_end=cfg.force_close_minutes_before_end,
        trading_enabled=cfg.trading_enabled,
        client=client,
        account_number=(client.account_number or ""),
    )

    try:
        while True:
            st = rg.process_once()
            if int(st.get("force_closed") or 0) > 0 or int(st.get("errors") or 0) > 0 or int(st.get("blocked_new_entries") or 0) > 0:
                log_daemon_event(paths.logs_daemon_dir, "info", "execution_risk_guard_poll", **st)
            time.sleep(poll_s)
    except KeyboardInterrupt:
        log_daemon_event(paths.logs_daemon_dir, "info", "execution_risk_guard_stop_keyboard")
    except Exception as e:
        lg = get_logger()
        if lg:
            lg.exception(level="CRITICAL", component="RiskGuard", event="execution_risk_guard_crash", message="execution risk guard crashed", file_key="errors", exc=e)
        else:
            log_daemon_event(paths.logs_daemon_dir, "error", "execution_risk_guard_crash", error=str(e))
        raise


if __name__ == "__main__":
    main()
