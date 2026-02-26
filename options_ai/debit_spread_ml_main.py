from __future__ import annotations

import time

from options_ai.debit_spread_ml import load_config_from_env, run_daemon


def main() -> None:
    cfg = load_config_from_env()
    try:
        run_daemon(cfg)
    except KeyboardInterrupt:
        time.sleep(0.1)


if __name__ == "__main__":
    main()
