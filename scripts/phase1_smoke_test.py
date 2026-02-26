#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

# Ensure repo root is on sys.path when running as a script
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _require(cond: bool, msg: str) -> None:
    if not cond:
        raise RuntimeError(msg)


def main() -> int:
    ap = argparse.ArgumentParser(description="Phase 1 smoke test: ingest one SPX chain JSON into Timescale and archive/copy.")
    ap.add_argument(
        "--dsn",
        default=os.getenv("SPX_CHAIN_DATABASE_URL", ""),
        help="Postgres/Timescale DSN (default: env SPX_CHAIN_DATABASE_URL)",
    )
    ap.add_argument(
        "--archive-root",
        default=os.getenv("ARCHIVE_ROOT", "/mnt/options_ai"),
        help="Writable archive root (default: env ARCHIVE_ROOT or /mnt/options_ai)",
    )
    ap.add_argument(
        "--filename-tz",
        default=os.getenv("FILENAME_TZ", "America/Chicago"),
        help="Timezone used in snapshot filenames (default: America/Chicago)",
    )
    args = ap.parse_args()

    _require(bool(args.dsn), "Missing --dsn (or env SPX_CHAIN_DATABASE_URL)")

    from options_ai.spx_chain_ingester import ChainIngestConfig, ingest_one_file

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    archive_root = Path(args.archive_root) / "test_runs" / f"phase1_{run_id}"
    archive_root.mkdir(parents=True, exist_ok=True)

    # Create a read-only simulated INPUT_DIR
    tmpdir = Path(tempfile.mkdtemp(prefix="spx_phase1_ro_"))
    try:
        fname = "SPX-5940.17-2025-03-28-20250303-100058.json"
        p = tmpdir / fname

        snap = {
            "status": "ok",
            "optionSymbol": ["TESTSYM1"],
            "underlying": ["SPX"],
            "expiration": [1743110400],  # 2025-03-28T00:00:00Z
            "side": ["call"],
            "strike": [6000],
            "bid": [1.0],
            "ask": [1.2],
            "openInterest": [10],
            "volume": [1],
            "iv": [0.10],
            "delta": [0.50],
            "gamma": [0.01],
            "updated": [1741016956],
            "underlyingPrice": [5940.17],
        }
        p.write_text(json.dumps(snap), encoding="utf-8")

        # Make directory & file read-only to force copy+processed.log behavior.
        # Note: depending on FS, this may or may not prevent rename; we still accept either.
        os.chmod(tmpdir, 0o555)
        os.chmod(p, 0o444)

        processed_log = archive_root / "state" / "processed.log"

        cfg = ChainIngestConfig(
            input_dir=tmpdir,
            archive_root=archive_root,
            database_url=args.dsn,
            filename_tz=args.filename_tz,
            poll_seconds=0.1,
            file_stable_seconds=0.0,
            processed_log_path=processed_log,
        )

        ingest_one_file(cfg, p)

        day_dir = archive_root / "archive" / "20250303"
        archived = day_dir / fname
        _require(archived.exists(), f"Expected archived file at {archived}")

        # processed.log should be written if move fails; if move succeeded, original is gone.
        if p.exists():
            _require(processed_log.exists(), "Expected processed.log to exist when source cannot be moved")
            txt = processed_log.read_text(encoding="utf-8")
            _require(fname in txt, "Expected processed.log to contain filename")

        print("PASS phase1_smoke_test")
        print(f"  archived: {archived}")
        print(f"  run_id: {run_id}")
        return 0

    finally:
        # Best-effort cleanup of tmpdir permissions
        try:
            os.chmod(tmpdir, 0o755)
        except Exception:
            pass
        try:
            for fp in tmpdir.glob("*"):
                try:
                    os.chmod(fp, 0o644)
                except Exception:
                    pass
        except Exception:
            pass
        shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
