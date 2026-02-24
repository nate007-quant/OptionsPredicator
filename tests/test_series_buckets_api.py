from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from options_ai.db import init_db


def test_series_buckets_endpoints_return_200(tmp_path: Path, monkeypatch):
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

    r = client.get("/api/metrics/series_buckets?window_days=1&bucket_minutes=15&min_samples=1")
    assert r.status_code == 200
    j = r.json()
    assert "series" in j

    r2 = client.get("/api/ml/metrics/series_buckets?window_days=1&bucket_minutes=15&min_samples=1")
    assert r2.status_code == 200
    j2 = r2.json()
    assert "series" in j2
