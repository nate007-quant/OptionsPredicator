import sqlite3

from options_ai.backtest.portfolio_backtest_service import PortfolioBacktestService


def _connect(db_path: str):
    con = sqlite3.connect(db_path)
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS portfolio_backtest_sessions(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          created_at_utc TEXT,
          started_at_utc TEXT,
          stopped_at_utc TEXT,
          status TEXT NOT NULL,
          legs_json TEXT NOT NULL,
          legs_total INTEGER NOT NULL DEFAULT 0,
          legs_completed INTEGER NOT NULL DEFAULT 0,
          legs_failed INTEGER NOT NULL DEFAULT 0,
          cancel_requested INTEGER NOT NULL DEFAULT 0,
          last_activity_at_utc TEXT,
          combined_summary_json TEXT,
          combined_equity_json TEXT,
          legs_summaries_json TEXT
        )
        """
    )
    con.commit()
    return con


def test_force_stop_marks_stopped_immediately(tmp_path):
    db = tmp_path / "state.db"
    with _connect(str(db)) as con:
        con.execute(
            """
            INSERT INTO portfolio_backtest_sessions(
              created_at_utc, started_at_utc, status, legs_json, legs_total, cancel_requested, last_activity_at_utc
            ) VALUES('2026-01-01T00:00:00+00:00','2026-01-01T00:00:00+00:00','running','[]',0,0,'2026-01-01T00:00:00+00:00')
            """
        )
        con.commit()

    svc = PortfolioBacktestService(db_path=str(db), connect_fn=_connect)
    out = svc.stop(session_id=1, force=True)
    assert out["status"] == "stopped"
    assert out["forced"] is True

    with _connect(str(db)) as con:
        row = con.execute("SELECT status,cancel_requested,stopped_at_utc FROM portfolio_backtest_sessions WHERE id=1").fetchone()
    assert row[0] == "stopped"
    assert int(row[1]) == 1
    assert row[2]
