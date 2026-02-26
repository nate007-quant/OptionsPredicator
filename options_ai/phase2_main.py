from __future__ import annotations

import time

from options_ai.phase2_builder import load_phase2_config_from_env, run_phase2_daemon


def main() -> None:
    cfg = load_phase2_config_from_env()
    try:
        run_phase2_daemon(cfg)
    except KeyboardInterrupt:
        time.sleep(0.1)


if __name__ == "__main__":
    main()
