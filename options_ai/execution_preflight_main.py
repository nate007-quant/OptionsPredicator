from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

from options_ai.config import load_config
from options_ai.db import db_path_from_url, init_db


def _check_env(cfg) -> list[str]:
    errs: list[str] = []
    if str(cfg.broker_name).lower() != 'tastytrade':
        errs.append('BROKER_NAME must be tastytrade')
    if str(cfg.broker_env).lower() not in {'sandbox', 'live'}:
        errs.append('BROKER_ENV must be sandbox|live')
    if not str(cfg.tasty_base_url or '').strip():
        errs.append('TASTY_BASE_URL missing')

    if str(cfg.broker_env).lower() == 'live':
        if not cfg.trading_enabled:
            errs.append('live mode set but TRADING_ENABLED=false')
        if not cfg.live_armed:
            errs.append('LIVE_ARMED=false (live interlock active)')

    if cfg.pretrade_required_checks is not True:
        errs.append('PRETRADE_REQUIRED_CHECKS should be true')
    if cfg.require_complex_exit_orders is not True:
        errs.append('REQUIRE_COMPLEX_EXIT_ORDERS should be true')
    if cfg.require_broker_external_identifier is not True:
        errs.append('REQUIRE_BROKER_EXTERNAL_IDENTIFIER should be true')
    return errs


def _check_db(db_path: str) -> list[str]:
    errs: list[str] = []
    con = sqlite3.connect(db_path)
    try:
        needed = [
            'execution_intents',
            'trade_runs',
            'order_events',
            'position_events',
            'risk_session_state',
            'reprice_policy',
            'audit_log',
            'broker_reconciliation_log',
            'incident_events',
        ]
        names = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        miss = [t for t in needed if t not in names]
        if miss:
            errs.append(f'missing tables: {miss}')
    finally:
        con.close()
    return errs


def main() -> None:
    cfg = load_config()
    db_path = db_path_from_url(cfg.database_url)
    schema_path = Path(__file__).parent / 'db' / 'schema.sql'
    init_db(db_path, schema_sql_path=str(schema_path))

    errs = []
    errs.extend(_check_env(cfg))
    errs.extend(_check_db(db_path))

    result = {
        'ok': len(errs) == 0,
        'broker_env': cfg.broker_env,
        'trading_enabled': cfg.trading_enabled,
        'live_armed': cfg.live_armed,
        'errors': errs,
    }
    print(json.dumps(result, indent=2, sort_keys=True))
    if errs:
        sys.exit(2)


if __name__ == '__main__':
    main()
