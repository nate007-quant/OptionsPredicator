from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def write_task_state(path: str | Path, obj: dict[str, Any] | None) -> None:
    """Best-effort atomic write of a small JSON task-state file."""
    try:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        if obj is None:
            try:
                p.unlink(missing_ok=True)
            except Exception:
                pass
            return
        payload = json.dumps(obj, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
        tmp = p.with_suffix(p.suffix + ".tmp")
        try:
            tmp.write_text(payload, encoding="utf-8")
            os.replace(tmp, p)
            return
        except Exception:
            try:
                with p.open("w", encoding="utf-8") as f:
                    f.write(payload)
                    f.flush()
                    try:
                        os.fsync(f.fileno())
                    except Exception:
                        pass
            finally:
                try:
                    tmp.unlink(missing_ok=True)
                except Exception:
                    pass
    except Exception:
        return
