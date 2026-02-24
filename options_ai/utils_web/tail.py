from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def tail_lines(path: Path, limit: int = 200, chunk_size: int = 8192) -> list[str]:
    """Tail the last N lines of a text file without reading the whole file."""

    limit = max(1, int(limit))
    if limit <= 0:
        return []

    try:
        with path.open("rb") as f:
            f.seek(0, 2)
            size = f.tell()
            buf = b""
            pos = size
            lines: list[bytes] = []

            while pos > 0 and len(lines) <= limit:
                read_size = min(chunk_size, pos)
                pos -= read_size
                f.seek(pos)
                data = f.read(read_size)
                buf = data + buf
                lines = buf.splitlines()

            # Take last N lines
            tail = lines[-limit:]
            return [b.decode("utf-8", errors="replace") for b in tail]
    except FileNotFoundError:
        return []


def tail_jsonl(path: Path, limit: int = 200) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for line in tail_lines(path, limit=limit):
        raw = line.rstrip("\n")
        item: dict[str, Any] = {"raw": raw}
        try:
            item["parsed"] = json.loads(raw)
        except Exception:
            item["parsed"] = None
        out.append(item)
    return out
