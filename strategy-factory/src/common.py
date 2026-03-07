from __future__ import annotations

import hashlib
import json
import os

try:
    import yaml  # type: ignore
except Exception:  # pragma: no cover
    yaml = None
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


LIFECYCLE_STATES = [
    "PROPOSED",
    "SPECIFIED",
    "TESTED",
    "ROBUST",
    "CANDIDATE",
    "PAPER",
    "LIVE",
    "RETIRED",
]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def canonical_json(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"))


def sha256_text(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def sha256_obj(obj: Any) -> str:
    return sha256_text(canonical_json(obj))


def ensure_dir(p: Path) -> Path:
    p.mkdir(parents=True, exist_ok=True)
    return p


def append_ndjson(path: Path, row: dict[str, Any]) -> None:
    ensure_dir(path.parent)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, sort_keys=True) + "\n")


def write_json(path: Path, obj: Any) -> None:
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, sort_keys=True)


def read_json(path: Path, default: Any = None) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def parse_bool(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return False
    return str(v).strip().lower() in {"1", "true", "yes", "on"}


def _parse_scalar(v: str) -> Any:
    s = v.strip()
    if s in {"true", "True"}:
        return True
    if s in {"false", "False"}:
        return False
    if s in {"null", "None", "~"}:
        return None
    try:
        if "." in s:
            return float(s)
        return int(s)
    except Exception:
        return s.strip('"').strip("'")


def load_yaml_like(path: Path) -> dict[str, Any]:
    """Load YAML config with safe parser; fallback parser if PyYAML unavailable."""
    text = path.read_text(encoding="utf-8")
    if yaml is not None:
        out = yaml.safe_load(text)
        if not isinstance(out, dict):
            raise ValueError(f"config root must be object: {path}")
        return out
    lines = [ln.rstrip() for ln in text.splitlines() if ln.strip() and not ln.strip().startswith("#")]

    root: dict[str, Any] = {}
    stack: list[tuple[int, Any]] = [(-1, root)]

    for ln in lines:
        indent = len(ln) - len(ln.lstrip(" "))
        content = ln.strip()

        while len(stack) > 1 and indent <= stack[-1][0]:
            stack.pop()

        parent = stack[-1][1]

        if content.startswith("- "):
            item = content[2:].strip()
            if not isinstance(parent, list):
                raise ValueError(f"invalid list placement in {path}: {ln}")
            parent.append(_parse_scalar(item))
            continue

        if ":" not in content:
            raise ValueError(f"invalid line in {path}: {ln}")

        k, v = content.split(":", 1)
        key = k.strip()
        val = v.strip()

        if val == "":
            # infer next structure by peeking next line
            # default to dict; if next non-empty same deeper with '- ' treat as list
            inferred: Any = {}
            if isinstance(parent, dict):
                parent[key] = inferred
                stack.append((indent, inferred))
            else:
                raise ValueError(f"mapping key in non-dict parent for {path}: {ln}")
        else:
            if isinstance(parent, dict):
                parent[key] = _parse_scalar(val)
            else:
                raise ValueError(f"mapping key in non-dict parent for {path}: {ln}")

        # convert empty dict to list if next line is list item
        # lightweight post-fixup during parse
        if isinstance(parent, dict) and key in parent and parent[key] == {}:
            # check next not easy in streaming; handled in second pass below
            pass

    # second pass: convert dicts with numeric keys impossible; we also need explicit lists from '-'
    # Simpler: reparsing with small heuristic for keys known to be lists.
    # For this project configs, only explicit list keys are stress_scenarios/actions.
    for list_key in ("actions", "stress_scenarios"):
        _normalize_list_key(root, list_key)
    return root


def _normalize_list_key(node: Any, target_key: str) -> None:
    if isinstance(node, dict):
        for k, v in list(node.items()):
            if k == target_key and isinstance(v, dict) and not v:
                node[k] = []
            _normalize_list_key(v, target_key)


@dataclass
class FactoryContext:
    repo_root: Path
    factory_root: Path
    configs_dir: Path
    schemas_dir: Path
    artifacts_dir: Path
    run_id: str
    run_dir: Path
    code_version: str
