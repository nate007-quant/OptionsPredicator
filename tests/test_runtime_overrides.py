from __future__ import annotations

import os

import pytest

from options_ai.runtime_overrides import apply_overrides, validate_and_normalize_overrides
from options_ai.config import load_config


def test_validate_rejects_unknown_key():
    with pytest.raises(ValueError):
        validate_and_normalize_overrides({"DATABASE_URL": "sqlite:///x"})


def test_validate_reprocess_mode_enum():
    ok = validate_and_normalize_overrides({"REPROCESS_MODE": "full"})
    assert ok["REPROCESS_MODE"] == "full"

    with pytest.raises(ValueError):
        validate_and_normalize_overrides({"REPROCESS_MODE": "nope"})


def test_validate_null_removes():
    ok = validate_and_normalize_overrides({"REPROCESS_MODE": None})
    assert ok["REPROCESS_MODE"] is None


def test_apply_overrides_maps_fields(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("DATABASE_URL", "sqlite:////tmp/test.db")
    cfg = load_config()
    eff = apply_overrides(cfg, {"REPROCESS_MODE": "full", "WATCH_POLL_SECONDS": 0.5, "OUTCOME_DELAY": 20})
    assert eff.reprocess_mode == "full"
    assert abs(eff.watch_poll_seconds - 0.5) < 1e-9
    assert eff.outcome_delay_minutes == 20
