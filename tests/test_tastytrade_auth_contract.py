from __future__ import annotations

from options_ai.brokers.tastytrade.client import TastytradeClient


def test_authorization_header_value_adds_bearer_for_bare_token() -> None:
    assert TastytradeClient._authorization_header_value("token123") == "Bearer token123"


def test_authorization_header_value_keeps_existing_bearer_case_insensitive() -> None:
    assert TastytradeClient._authorization_header_value("bearer token123") == "bearer token123"


def test_authorization_header_value_trims_outer_whitespace() -> None:
    assert TastytradeClient._authorization_header_value("  token123  ") == "Bearer token123"
