from __future__ import annotations

from generator.idea_generator import _resolve_openai_auth


def test_resolve_auth_prefers_api_key_in_auto(monkeypatch):
    monkeypatch.setenv("STRATEGY_FACTORY_AI_AUTH_MODE", "auto")
    monkeypatch.setenv("OPENAI_API_KEY", "k-test")
    monkeypatch.setenv("OPENAI_OAUTH_ACCESS_TOKEN", "tok-test")
    tok, src = _resolve_openai_auth()
    assert tok == "k-test"
    assert src == "api_key"


def test_resolve_auth_uses_oauth_in_oauth_mode(monkeypatch):
    monkeypatch.setenv("STRATEGY_FACTORY_AI_AUTH_MODE", "oauth")
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.setenv("OPENAI_OAUTH_ACCESS_TOKEN", "tok-test")
    tok, src = _resolve_openai_auth()
    assert tok == "tok-test"
    assert src == "oauth_access_token"
