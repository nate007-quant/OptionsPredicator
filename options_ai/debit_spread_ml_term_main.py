from __future__ import annotations

from options_ai.debit_spread_ml_term import load_config_from_env, run_daemon


def main() -> None:
    cfg = load_config_from_env()
    run_daemon(cfg)


if __name__ == "__main__":
    main()
