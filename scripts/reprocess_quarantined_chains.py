#!/usr/bin/env python3
"""Reprocess quarantined chain JSON files from ARCHIVE_ROOT/bad into Timescale.

This is intended for one-off backfills after ingestion code is fixed.

Defaults:
- Reads files from:  $ARCHIVE_ROOT/bad (ARCHIVE_ROOT defaults to /mnt/options_ai)
- Processes only:    SPX-Unknown-*.json (override with --glob)

Safe to re-run: successful files are moved to ARCHIVE_ROOT/archive/YYYYMMDD/.
"""

from __future__ import annotations


import argparse
import json
import sys
import time
from pathlib import Path

# Ensure project root is on sys.path when run as a script (sys.path[0]=scripts/)
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import psycopg

from options_ai.spx_chain_ingester import (
    UPSERT_SQL,
    _archive_dest,
    _safe_move_or_copy,
    build_rows,
    ensure_timescale_schema,
    load_chain_ingest_config_from_env,
    parse_chain_filename,
    validate_chain_json,
)


def _eprint(*a: object) -> None:
    print(*a, file=sys.stderr, flush=True)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--glob", default="SPX-Unknown-*.json", help="filename glob under bad dir")
    ap.add_argument("--limit", type=int, default=0, help="stop after N files (0=all)")
    ap.add_argument("--sleep", type=float, default=0.0, help="sleep seconds between files")
    ap.add_argument("--commit-every", type=int, default=1, help="commit every N files")
    args = ap.parse_args()

    cfg = load_chain_ingest_config_from_env()

    bad_dir = cfg.archive_root / "bad"
    if not bad_dir.exists():
        _eprint(f"bad dir not found: {bad_dir}")
        return 2

    files = sorted(bad_dir.glob(args.glob))
    if args.limit and args.limit > 0:
        files = files[: int(args.limit)]

    print(
        json.dumps(
            {
                "event": "reprocess_start",
                "bad_dir": str(bad_dir),
                "glob": args.glob,
                "n_files": len(files),
                "archive_root": str(cfg.archive_root),
                "filename_tz": cfg.filename_tz,
            }
        ),
        flush=True,
    )

    ensure_timescale_schema(cfg.database_url)

    ok = 0
    fail = 0
    t0 = time.time()

    with psycopg.connect(cfg.database_url) as conn:
        with conn.cursor() as cur:
            cur.execute("BEGIN")

            for idx, path in enumerate(files, start=1):
                fn = path.name
                try:
                    parsed = parse_chain_filename(fn, filename_tz=cfg.filename_tz)
                    raw = path.read_text(encoding="utf-8-sig")  # tolerate UTF-8 BOM
                    snap = json.loads(raw)
                    if not isinstance(snap, dict):
                        raise ValueError("snapshot JSON root must be an object")

                    n = validate_chain_json(snap)
                    rows = build_rows(snap, n=n, parsed=parsed)

                    cur.executemany(UPSERT_SQL, rows)

                    # Move file to archive post-upsert.
                    dest = _archive_dest(cfg, parsed, fn)
                    _safe_move_or_copy(path, dest)

                    # Remove old error marker if present.
                    try:
                        errp = (cfg.archive_root / "bad" / f"{fn}.error.txt")
                        if errp.exists():
                            errp.unlink(missing_ok=True)
                    except Exception:
                        pass

                    ok += 1

                except Exception as e:
                    fail += 1
                    # Keep file in bad; write/overwrite error.
                    try:
                        errp = cfg.archive_root / "bad" / f"{fn}.error.txt"
                        errp.write_text(str(e).strip() + "\n", encoding="utf-8")
                    except Exception:
                        pass

                    print(json.dumps({"event": "reprocess_fail", "file": fn, "error": str(e)[:500]}), flush=True)

                # periodic commit
                if int(args.commit_every) <= 1 or (idx % int(args.commit_every) == 0):
                    conn.commit()
                    cur.execute("BEGIN")

                if idx % 50 == 0:
                    dt = time.time() - t0
                    rate = ok / dt if dt > 0 else None
                    print(
                        json.dumps(
                            {
                                "event": "reprocess_progress",
                                "done": idx,
                                "total": len(files),
                                "ok": ok,
                                "fail": fail,
                                "rate_files_per_sec": rate,
                            }
                        ),
                        flush=True,
                    )

                if args.sleep and args.sleep > 0:
                    time.sleep(float(args.sleep))

            conn.commit()

    dt = time.time() - t0
    print(
        json.dumps(
            {
                "event": "reprocess_done",
                "ok": ok,
                "fail": fail,
                "seconds": dt,
                "rate_files_per_sec": (ok / dt if dt > 0 else None),
            }
        ),
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
