from __future__ import annotations

import time

from options_ai.spx_chain_ingester import load_chain_ingest_config_from_env, run_chain_ingest_daemon


def main() -> None:
    cfg = load_chain_ingest_config_from_env()
    try:
        run_chain_ingest_daemon(cfg)
    except KeyboardInterrupt:
        time.sleep(0.1)


if __name__ == "__main__":
    main()
