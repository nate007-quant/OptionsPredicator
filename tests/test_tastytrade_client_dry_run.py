from __future__ import annotations

from options_ai.brokers.tastytrade.client import OptionLeg, OrderDTO, TastytradeClient


def test_place_order_dry_run_returns_payload() -> None:
    c = TastytradeClient(base_url="https://api.cert.tastyworks.com", dry_run=True)
    dto = OrderDTO(
        account_number="ABC123",
        underlying="SPX",
        quantity=1,
        price_effect="DEBIT",
        limit_price=1.00,
        legs=[
            OptionLeg(symbol="SPXW  260101C06000000", quantity=1, side="BUY", effect="OPEN"),
            OptionLeg(symbol="SPXW  260101C06050000", quantity=1, side="SELL", effect="OPEN"),
        ],
    )
    out = c.place_order(dto)
    assert out["ok"] is True
    assert out["dry_run"] is True
    assert out["payload"]["price-effect"] == "Debit"


def test_headers_use_raw_session_token() -> None:
    c = TastytradeClient(base_url="https://api.cert.tastyworks.com", dry_run=True, session_token="abc123")
    h = c._headers()
    assert h["Authorization"] == "abc123"


def test_headers_preserve_prefixed_session_token() -> None:
    c = TastytradeClient(base_url="https://api.cert.tastyworks.com", dry_run=True, session_token="Bearer abc123")
    h = c._headers()
    assert h["Authorization"] == "Bearer abc123"
