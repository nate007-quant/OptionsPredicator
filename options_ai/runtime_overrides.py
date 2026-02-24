from __future__ import annotations

import json
import os
from dataclasses import replace
from pathlib import Path
from typing import Any

from options_ai.config import Config
from options_ai.utils.logger import get_logger


# Phase-1 allowlist (spec v1) + ML toggles (v2.7)
ALLOWLIST: dict[str, Any] = {
    # Required reprocessing controls
    "REPROCESS_MODE": {"type": "enum", "values": ["none", "from_model", "from_summary", "from_signals", "full"]},

    # Operational knobs
    "WATCH_POLL_SECONDS": {"type": "float", "min": 0.05, "max": 60.0},
    "FILE_STABLE_SECONDS": {"type": "int", "min": 0, "max": 60},
    "OUTCOME_DELAY": {"type": "int", "min": 1, "max": 240},

    # Routing toggles
    "MODEL_FORCE_LOCAL": {"type": "bool"},
    "MODEL_FORCE_REMOTE": {"type": "bool"},
    "LOCAL_MODEL_ENABLED": {"type": "bool"},
    "CHART_ENABLED": {"type": "bool"},
    "CHART_LOCAL_ENABLED": {"type": "bool"},
    "CHART_REMOTE_ENABLED": {"type": "bool"},

    # ML/LLM toggles
    "ML_ENABLED": {"type": "bool"},
    "LLM_ENABLED": {"type": "bool"},

    # Optional experiment/versioning
    "PROMPT_VERSION": {"type": "str", "min_len": 1, "max_len": 64},
}


def _coerce_bool(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    if isinstance(v, str):
        return v.strip().lower() in {"1", "true", "yes", "y", "on"}
    raise ValueError("invalid bool")


def validate_and_normalize_overrides(incoming: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(incoming, dict):
        raise ValueError("overrides must be a JSON object")

    normalized: dict[str, Any] = {}
    for k, v in incoming.items():
        if k not in ALLOWLIST:
            raise ValueError(f"override key not allowed: {k}")

        if v is None:
            normalized[k] = None
            continue

        spec = ALLOWLIST[k]
        t = spec.get("type")
        try:
            if t == "enum":
                if not isinstance(v, str):
                    raise ValueError("must be string")
                vv = v.strip().lower()
                if vv not in set(spec.get("values") or []):
                    raise ValueError("invalid enum value")
                normalized[k] = vv
            elif t == "float":
                vv = float(v)
                if "min" in spec and vv < float(spec["min"]):
                    raise ValueError("below min")
                if "max" in spec and vv > float(spec["max"]):
                    raise ValueError("above max")
                normalized[k] = vv
            elif t == "int":
                vv = int(v)
                if "min" in spec and vv < int(spec["min"]):
                    raise ValueError("below min")
                if "max" in spec and vv > int(spec["max"]):
                    raise ValueError("above max")
                normalized[k] = vv
            elif t == "bool":
                normalized[k] = _coerce_bool(v)
            elif t == "str":
                if not isinstance(v, str):
                    raise ValueError("must be string")
                vv = v.strip()
                if len(vv) < int(spec.get("min_len", 0)):
                    raise ValueError("too short")
                if len(vv) > int(spec.get("max_len", 10_000)):
                    raise ValueError("too long")
                normalized[k] = vv
            else:
                raise ValueError("unknown allowlist spec")
        except Exception as e:
            raise ValueError(f"invalid value for {k}: {e}")

    return normalized


def load_overrides_file(path: Path) -> dict[str, Any]:
    try:
        raw = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return {}
    except Exception:
        return {}

    try:
        obj = json.loads(raw)
    except Exception:
        lg = get_logger()
        if lg:
            lg.exception(
                level="WARNING",
                component="Config",
                event="runtime_overrides_invalid_json",
                message="runtime overrides file invalid JSON; ignoring",
                file_key="system",
                path=str(path),
            )
        return {}

    if not isinstance(obj, dict):
        return {}

    try:
        norm = validate_and_normalize_overrides(obj)
    except Exception as e:
        lg = get_logger()
        if lg:
            lg.exception(
                level="WARNING",
                component="Config",
                event="runtime_overrides_invalid",
                message="runtime overrides invalid; ignoring",
                file_key="system",
                path=str(path),
                error=str(e),
            )
        return {}

    return {k: v for k, v in norm.items() if v is not None}


def write_overrides_file_atomic(path: Path, overrides: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(overrides, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def apply_overrides(base_cfg: Config, overrides: dict[str, Any]) -> Config:
    fields: dict[str, Any] = {}

    if "REPROCESS_MODE" in overrides:
        fields["reprocess_mode"] = str(overrides["REPROCESS_MODE"]).strip().lower()
    if "WATCH_POLL_SECONDS" in overrides:
        fields["watch_poll_seconds"] = float(overrides["WATCH_POLL_SECONDS"])
    if "FILE_STABLE_SECONDS" in overrides:
        fields["file_stable_seconds"] = int(overrides["FILE_STABLE_SECONDS"])
    if "OUTCOME_DELAY" in overrides:
        fields["outcome_delay_minutes"] = int(overrides["OUTCOME_DELAY"])

    if "MODEL_FORCE_LOCAL" in overrides:
        fields["model_force_local"] = bool(overrides["MODEL_FORCE_LOCAL"])
    if "MODEL_FORCE_REMOTE" in overrides:
        fields["model_force_remote"] = bool(overrides["MODEL_FORCE_REMOTE"])
    if "LOCAL_MODEL_ENABLED" in overrides:
        fields["local_model_enabled"] = bool(overrides["LOCAL_MODEL_ENABLED"])

    if "CHART_ENABLED" in overrides:
        fields["chart_enabled"] = bool(overrides["CHART_ENABLED"])
    if "CHART_LOCAL_ENABLED" in overrides:
        fields["chart_local_enabled"] = bool(overrides["CHART_LOCAL_ENABLED"])
    if "CHART_REMOTE_ENABLED" in overrides:
        fields["chart_remote_enabled"] = bool(overrides["CHART_REMOTE_ENABLED"])

    if "ML_ENABLED" in overrides:
        fields["ml_enabled"] = bool(overrides["ML_ENABLED"])
    if "LLM_ENABLED" in overrides:
        fields["llm_enabled"] = bool(overrides["LLM_ENABLED"])

    if "PROMPT_VERSION" in overrides:
        fields["prompt_version"] = str(overrides["PROMPT_VERSION"]).strip()

    if not fields:
        return base_cfg

    return replace(base_cfg, **fields)


def allowlist_public_spec() -> dict[str, Any]:
    return ALLOWLIST
