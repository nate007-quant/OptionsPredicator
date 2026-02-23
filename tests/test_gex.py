from __future__ import annotations

from options_ai.utils.gex import compute_gex_structure
from options_ai.utils.signals import OptionRow


def test_gex_structure_walls_magnet_flip():
    spot = 100.0

    # Make 3 strikes, with simple gamma/oi so we can reason about signs.
    rows = [
        # strike 95: net negative (puts)
        OptionRow(
            optionSymbol="P95",
            side="put",
            strike=95.0,
            expiration_epoch=0,
            bid=None,
            ask=None,
            mid=None,
            openInterest=100,
            volume=None,
            iv=None,
            delta=None,
            gamma=0.01,
        ),
        # strike 100: net positive (calls)
        OptionRow(
            optionSymbol="C100",
            side="call",
            strike=100.0,
            expiration_epoch=0,
            bid=None,
            ask=None,
            mid=None,
            openInterest=200,
            volume=None,
            iv=None,
            delta=None,
            gamma=0.01,
        ),
        # strike 105: small positive
        OptionRow(
            optionSymbol="C105",
            side="call",
            strike=105.0,
            expiration_epoch=0,
            bid=None,
            ask=None,
            mid=None,
            openInterest=50,
            volume=None,
            iv=None,
            delta=None,
            gamma=0.01,
        ),
    ]

    g = compute_gex_structure(rows, spot)

    assert g.call_wall_strike == 100.0
    assert g.put_wall_strike == 95.0

    # Magnet within +/-1% (99-101) -> only strike 100 qualifies
    assert g.magnet_strike == 100.0

    # Flip should be defined
    assert g.flip_strike in {95.0, 100.0, 105.0}

    assert g.regime_label in {"positive_gamma", "negative_gamma"}
