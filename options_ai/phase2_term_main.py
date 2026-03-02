from __future__ import annotations

from options_ai.phase2_term_builder import load_config_from_env, run_daemon


def main() -> None:
    cfg = load_config_from_env()
    run_daemon(cfg)


if __name__ == "__main__":
    main()
