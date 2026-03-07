from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from options_ai.db import init_db


def test_execution_endpoints_smoke(tmp_path: Path, monkeypatch):
    data_root = tmp_path / "data"
    data_root.mkdir(parents=True, exist_ok=True)

    db_path = tmp_path / "predictions.db"
    schema = Path(__file__).resolve().parents[1] / "options_ai" / "db" / "schema.sql"
    init_db(str(db_path), str(schema))

    monkeypatch.setenv("DATABASE_URL", f"sqlite:////{db_path}")
    monkeypatch.setenv("DATA_ROOT", str(data_root))

    from options_ai.dashboard_api.main import create_app

    app = create_app()
    client = TestClient(app)

    r1 = client.get("/api/execution/intents")
    assert r1.status_code == 200

    r2 = client.get("/api/execution/trades/open")
    assert r2.status_code == 200

    r3 = client.get("/api/execution/reprice-policy?underlying=SPX")
    assert r3.status_code == 200
    body = r3.json()
    assert body["underlying"] == "SPX"

    r4 = client.put(
        "/api/execution/reprice-policy",
        json={"underlying": "SPX", "max_attempts": 0},
    )
    assert r4.status_code == 400

    r5 = client.post("/api/execution/kill-switch", json={"block_new_entries": True, "reason": "test"})
    assert r5.status_code == 200
    assert r5.json()["block_new_entries"] is True

    r6 = client.get("/api/execution/risk-session")
    assert r6.status_code == 200
