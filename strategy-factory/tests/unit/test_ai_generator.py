from __future__ import annotations

from generator.idea_generator import generate_ideas


def test_generate_ideas_fallback_without_ai(monkeypatch):
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.setenv("STRATEGY_FACTORY_AI_ENABLED", "false")
    ideas = generate_ideas(run_id="r1", max_ideas=4, generator_cfg={"ai": {"enabled": False}})
    assert len(ideas) == 4
    assert all(i.idea_id.startswith("r1-") for i in ideas)
