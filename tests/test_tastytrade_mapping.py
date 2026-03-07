from __future__ import annotations

import pytest

from options_ai.brokers.tastytrade.client import (
    OptionLeg,
    OrderDTO,
    TastytradeMappingError,
    map_order_dto_to_tasty_payload,
    normalize_limit_price,
    validate_spread_legs,
)


def test_normalize_limit_price_abs_and_round() -> None:
    p = normalize_limit_price(raw_price=-1.234, effect="CREDIT")
    assert float(p) == 1.23

    p2 = normalize_limit_price(raw_price=1.235, effect="DEBIT")
    assert float(p2) == 1.24


def test_validate_spread_legs_requires_buy_and_sell() -> None:
    legs = [
        OptionLeg(symbol="SPXW  260101C06000000", quantity=1, side="BUY", effect="OPEN"),
        OptionLeg(symbol="SPXW  260101C06050000", quantity=1, side="BUY", effect="OPEN"),
    ]
    with pytest.raises(TastytradeMappingError):
        validate_spread_legs(legs)


def test_validate_spread_legs_rejects_duplicate_symbol() -> None:
    legs = [
        OptionLeg(symbol="SPXW  260101C06000000", quantity=1, side="BUY", effect="OPEN"),
        OptionLeg(symbol="SPXW  260101C06000000", quantity=1, side="SELL", effect="OPEN"),
    ]
    with pytest.raises(TastytradeMappingError):
        validate_spread_legs(legs)


def test_map_order_payload_debit_spread() -> None:
    dto = OrderDTO(
        account_number="ABC123",
        underlying="SPX",
        quantity=1,
        price_effect="DEBIT",
        limit_price=0.85,
        legs=[
            OptionLeg(symbol="SPXW  260101C06000000", quantity=1, side="BUY", effect="OPEN"),
            OptionLeg(symbol="SPXW  260101C06050000", quantity=1, side="SELL", effect="OPEN"),
        ],
        client_order_id="intent-1",
    )

    payload = map_order_dto_to_tasty_payload(dto)
    assert payload["price-effect"] == "Debit"
    assert payload["price"] == "0.85"
    assert payload["client-order-id"] == "intent-1"
    assert len(payload["legs"]) == 2
    assert payload["legs"][0]["action"] == "Buy to Open"
    assert payload["legs"][1]["action"] == "Sell to Open"
