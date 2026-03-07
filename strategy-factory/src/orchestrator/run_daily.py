from __future__ import annotations

import argparse
from pathlib import Path

from orchestrator.scheduler import run_once_with_retry


def main() -> None:
    ap = argparse.ArgumentParser(description="Run Strategy Factory daily pipeline once")
    ap.add_argument("--repo-root", default=str(Path(__file__).resolve().parents[3]))
    ap.add_argument("--approve-live", action="store_true", help="Allow PAPER->LIVE transitions for this run")
    args = ap.parse_args()

    out = run_once_with_retry(repo_root=Path(args.repo_root), explicit_live_approval=bool(args.approve_live))
    if out is None:
        print("SKIPPED_ACTIVE_LOCK")
    else:
        print(f"RUN_OK {out.get('run_id')}")


if __name__ == "__main__":
    main()
