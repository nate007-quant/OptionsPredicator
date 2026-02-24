from __future__ import annotations

from pathlib import Path

from options_ai.db import init_db


def test_eod_predictions_table_created(tmp_path: Path):
    db_path = str(tmp_path / "predictions.db")
    schema = Path(__file__).resolve().parents[1] / "options_ai" / "db" / "schema.sql"
    init_db(db_path, str(schema))

    import sqlite3

    con = sqlite3.connect(db_path)
    tables = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    assert "eod_predictions" in tables
