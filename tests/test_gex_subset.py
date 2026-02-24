from __future__ import annotations

from options_ai.utils.gex_subset import build_compact_gex


def test_gex_subset_caps_and_includes_levels():
    # Create synthetic strike maps
    net = {str(4000 + i * 5): float(i) for i in range(200)}
    abs_map = {k: abs(v) for k, v in net.items()}

    g = build_compact_gex(
        spot=5000.0,
        net_map=net,
        abs_map=abs_map,
        call_wall=5100.0,
        put_wall=4900.0,
        magnet=5000.0,
        flip=5050.0,
        dist_call_wall=0.02,
        dist_put_wall=0.02,
        dist_magnet=0.0,
        dist_flip=0.01,
        regime_label="positive_gamma",
        neighbors_each_level=2,
        topk_abs=10,
        sticky_strikes=[4800.0, 5200.0],
        sticky_day_max=20,
    )

    strikes = g.subset["strikes"]
    assert 5100.0 in strikes
    assert 4900.0 in strikes
    assert 5000.0 in strikes
    assert 5050.0 in strikes

    # rough cap: levels + neighbors + topk + sticky (dedup can reduce)
    assert len(strikes) <= (4 + 4 * (2 * 2 + 1) + 10 + 20)
