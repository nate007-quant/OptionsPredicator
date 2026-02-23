from __future__ import annotations

import json
import os
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Any


SCHEMA_VERSION = "1.0"
SIGNALS_VERSION = "1.1"  # bumped for GEX inclusion
GEX_VERSION = "1.0"
SUMMARY_VERSION = "1.0"


def sha256_bytes(b: bytes) -> str:
    h = sha256()
    h.update(b)
    return h.hexdigest()


def sha256_file(path: Path) -> str:
    return sha256_bytes(path.read_bytes())


def _safe_name(s: str) -> str:
    # safe for filenames
    return "".join(c if (c.isalnum() or c in {"-", "_", "."}) else "_" for c in s)


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def save_json_atomic(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, sort_keys=True, indent=2), encoding="utf-8")
    os.replace(tmp, path)


@dataclass(frozen=True)
class DerivedCache:
    normalized_rows: list[dict[str, Any]]
    signals: dict[str, Any]
    snapshot_summary: dict[str, Any]

    schema_version: str = SCHEMA_VERSION
    signals_version: str = SIGNALS_VERSION
    gex_version: str = GEX_VERSION
    summary_version: str = SUMMARY_VERSION


def derived_cache_dir(paths: Any, snapshot_hash: str) -> Path:
    return Path(paths.cache_derived_dir) / snapshot_hash


def derived_cache_path(paths: Any, snapshot_hash: str) -> Path:
    return derived_cache_dir(paths, snapshot_hash) / "derived.json"


def load_derived_cache(paths: Any, snapshot_hash: str) -> DerivedCache | None:
    p = derived_cache_path(paths, snapshot_hash)
    if not p.exists():
        return None
    try:
        obj = load_json(p)
        if not isinstance(obj, dict):
            return None
        # Version gating
        if obj.get("schema_version") != SCHEMA_VERSION:
            return None
        if obj.get("signals_version") != SIGNALS_VERSION:
            return None
        if obj.get("gex_version") != GEX_VERSION:
            return None
        if obj.get("summary_version") != SUMMARY_VERSION:
            return None
        return DerivedCache(
            normalized_rows=list(obj.get("normalized_rows") or []),
            signals=dict(obj.get("signals") or {}),
            snapshot_summary=dict(obj.get("snapshot_summary") or {}),
        )
    except Exception:
        return None


def save_derived_cache(paths: Any, snapshot_hash: str, cache: DerivedCache) -> None:
    p = derived_cache_path(paths, snapshot_hash)
    payload = {
        "schema_version": cache.schema_version,
        "signals_version": cache.signals_version,
        "gex_version": cache.gex_version,
        "summary_version": cache.summary_version,
        "normalized_rows": cache.normalized_rows,
        "signals": cache.signals,
        "snapshot_summary": cache.snapshot_summary,
    }
    save_json_atomic(p, payload)


def model_cache_path(
    paths: Any,
    snapshot_hash: str,
    *,
    chart_hash: str | None,
    prompt_version: str,
    model_id: str,
    kind: str,
) -> Path:
    # kind: prediction | chart
    d = Path(paths.cache_model_dir) / snapshot_hash
    ch = chart_hash or "nochart"
    name = _safe_name(f"{kind}__{model_id}__{prompt_version}__{ch}.json")
    return d / name


def load_model_cache(paths: Any, snapshot_hash: str, *, chart_hash: str | None, prompt_version: str, model_id: str, kind: str) -> dict[str, Any] | None:
    p = model_cache_path(paths, snapshot_hash, chart_hash=chart_hash, prompt_version=prompt_version, model_id=model_id, kind=kind)
    if not p.exists():
        return None
    try:
        obj = load_json(p)
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def save_model_cache(paths: Any, snapshot_hash: str, *, chart_hash: str | None, prompt_version: str, model_id: str, kind: str, payload: dict[str, Any]) -> None:
    p = model_cache_path(paths, snapshot_hash, chart_hash=chart_hash, prompt_version=prompt_version, model_id=model_id, kind=kind)
    save_json_atomic(p, payload)
