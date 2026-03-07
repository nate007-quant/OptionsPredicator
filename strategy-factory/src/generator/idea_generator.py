from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any


@dataclass
class Idea:
    idea_id: str
    structure_type: str
    description: str
    seed_params: dict[str, Any]


def _deterministic_ideas(*, run_id: str, max_ideas: int = 6) -> list[Idea]:
    base = [
        ("VERTICAL", "ATM debit call spread, morning window", {"spread_style": "debit", "option_type": "CALL"}),
        ("VERTICAL", "ATM debit put spread, morning window", {"spread_style": "debit", "option_type": "PUT"}),
        ("IRON_CONDOR", "short condor with tight wings and event blackout", {"spread_style": "credit"}),
        ("STRANGLE", "long strangle around IV compression", {"spread_style": "debit"}),
        ("CALENDAR", "calendar spread with DTE stagger", {"spread_style": "debit"}),
        ("BUTTERFLY", "broken-wing butterfly with capped risk", {"spread_style": "debit"}),
    ]
    out: list[Idea] = []
    for i, (st, desc, params) in enumerate(base[: max_ideas]):
        out.append(Idea(idea_id=f"{run_id}-idea-{i+1}", structure_type=st, description=desc, seed_params=params))
    return out


def _parse_bool(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return False
    return str(v).strip().lower() in {"1", "true", "yes", "on"}


def _generate_ai_ideas(
    *,
    run_id: str,
    max_ideas: int,
    model: str,
    temperature: float,
    timeout_seconds: int,
    approved_structures: list[str],
) -> list[Idea]:
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        return []

    try:
        from openai import OpenAI  # type: ignore
    except Exception:
        return []

    prompt = {
        "task": "Generate options strategy ideas for autonomous backtest factory.",
        "constraints": {
            "options_only": True,
            "allowed_structures": approved_structures,
            "max_ideas": int(max_ideas),
            "must_be_machine_runnable": True,
            "include_seed_params": ["spread_style", "option_type", "iv_rank_min", "iv_rank_max", "take_profit_pct", "stop_loss_pct"],
        },
        "output_schema": {
            "ideas": [
                {
                    "structure_type": "VERTICAL",
                    "description": "...",
                    "seed_params": {"spread_style": "debit", "option_type": "CALL"},
                }
            ]
        },
        "output_format": "JSON only",
    }

    client = OpenAI(api_key=api_key, timeout=float(timeout_seconds))
    # Use chat.completions for broad compatibility with installed SDKs.
    resp = client.chat.completions.create(
        model=str(model),
        temperature=float(temperature),
        messages=[
            {"role": "system", "content": "You are an options strategy ideation model. Return strict JSON only."},
            {"role": "user", "content": json.dumps(prompt)},
        ],
    )

    text = (resp.choices[0].message.content or "").strip() if resp.choices else ""
    if not text:
        return []

    # best-effort extract JSON
    if text.startswith("```"):
        text = text.strip("`")
        text = text.replace("json", "", 1).strip()

    try:
        obj = json.loads(text)
    except Exception:
        return []

    ideas_raw = obj.get("ideas") if isinstance(obj, dict) else None
    if not isinstance(ideas_raw, list):
        return []

    out: list[Idea] = []
    for i, it in enumerate(ideas_raw[:max_ideas]):
        if not isinstance(it, dict):
            continue
        st = str(it.get("structure_type") or "VERTICAL").upper().strip()
        if st not in set(approved_structures):
            continue
        desc = str(it.get("description") or f"AI idea {i+1}").strip()
        seed = it.get("seed_params") if isinstance(it.get("seed_params"), dict) else {}
        out.append(Idea(idea_id=f"{run_id}-ai-{i+1}", structure_type=st, description=desc, seed_params=seed))

    return out


def generate_ideas(*, run_id: str, max_ideas: int = 6, generator_cfg: dict[str, Any] | None = None) -> list[Idea]:
    """Generate ideas with optional AI augmentation.

    Behavior:
      - deterministic ideas are always available as fallback.
      - AI ideas are enabled only when both config + env allow it.
    """
    cfg = generator_cfg or {}
    ai_cfg = cfg.get("ai") or {}
    approved_structures = list((cfg.get("approved_structures") or [
        "SINGLE",
        "VERTICAL",
        "STRADDLE",
        "STRANGLE",
        "IRON_CONDOR",
        "CALENDAR",
        "BUTTERFLY",
        "RATIO",
    ]))

    ai_enabled_cfg = _parse_bool(ai_cfg.get("enabled"))
    ai_enabled_env = _parse_bool(os.getenv("STRATEGY_FACTORY_AI_ENABLED", "false"))
    ai_enabled = ai_enabled_cfg or ai_enabled_env

    deterministic = _deterministic_ideas(run_id=run_id, max_ideas=max_ideas)
    if not ai_enabled:
        return deterministic

    ai_ideas = _generate_ai_ideas(
        run_id=run_id,
        max_ideas=min(int(ai_cfg.get("max_ideas_from_ai") or max_ideas), max_ideas),
        model=str(ai_cfg.get("model") or os.getenv("STRATEGY_FACTORY_AI_MODEL", "gpt-5.2")),
        temperature=float(ai_cfg.get("temperature") or 0.3),
        timeout_seconds=int(ai_cfg.get("timeout_seconds") or 30),
        approved_structures=approved_structures,
    )

    # Merge AI + deterministic fallback, dedup by (structure, description).
    merged: list[Idea] = []
    seen: set[tuple[str, str]] = set()
    for it in ai_ideas + deterministic:
        key = (it.structure_type, it.description)
        if key in seen:
            continue
        seen.add(key)
        merged.append(it)
        if len(merged) >= max_ideas:
            break
    return merged
