from __future__ import annotations

import time
from pathlib import Path

from options_ai.config import load_config
from options_ai.db import db_path_from_url, init_db
from options_ai.utils.logger import get_logger, init_logger, log_daemon_event
from options_ai.utils.paths import build_paths, ensure_runtime_dirs
from options_ai.watcher import run_daemon


def main() -> None:
    cfg = load_config()
    paths = build_paths(cfg.data_root, cfg.ticker)
    ensure_runtime_dirs(paths)

    # v2.0: API key auth is deprecated; ignore if present.
    import os
    if os.getenv("OPENAI_API_KEY"):
        log_daemon_event(paths.logs_daemon_dir, "warn", "openai_api_key_ignored", message_detail="OPENAI_API_KEY is ignored in v2.0; configure OAuth env vars instead")

    db_path = db_path_from_url(cfg.database_url)
    schema_path = Path(__file__).parent / "db" / "schema.sql"
    init_db(db_path, schema_sql_path=str(schema_path))

    # v2.3 structured logging (also writes ERROR/CRITICAL to DB system_events)
    init_logger(logs_root=paths.data_root / "logs", db_path=db_path)

    log_daemon_event(
        paths.logs_daemon_dir,
        "info",
        "daemon_start",
        ticker=cfg.ticker,
        data_root=cfg.data_root,
        replay_mode=cfg.replay_mode,
    )

    try:
        run_daemon(cfg, paths, db_path)
    except KeyboardInterrupt:
        log_daemon_event(paths.logs_daemon_dir, "info", "daemon_stop_keyboard")
    except Exception as e:
        lg = get_logger()
        if lg:
            lg.exception(level="CRITICAL", component="Daemon", event="daemon_crash", message="daemon crashed", file_key="errors", exc=e)
        else:
            log_daemon_event(paths.logs_daemon_dir, "error", "daemon_crash", error=str(e))
        time.sleep(0.2)
        raise


if __name__ == "__main__":
    main()
