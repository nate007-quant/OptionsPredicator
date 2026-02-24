from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from options_ai.db import connect


def hash_exists(db_path: str, source_snapshot_hash: str, prompt_version: str, model_used: str) -> bool:
    with connect(db_path) as conn:
        cur = conn.execute(
            "SELECT 1 FROM predictions WHERE source_snapshot_hash = ? AND prompt_version = ? AND model_used = ? LIMIT 1",
            (source_snapshot_hash, prompt_version, model_used),
        )
        return cur.fetchone() is not None



def insert_prediction(db_path: str, row: dict[str, Any]) -> int | None:
    """Insert prediction row.

    v2.3: idempotent on UNIQUE(source_snapshot_hash, prompt_version, model_used).
    Returns:
      - new row id if inserted
      - None if ignored due to uniqueness (duplicate)
    """

    cols = [
        "timestamp",
        "ticker",
        "expiration_date",
        "source_snapshot_file",
        "source_snapshot_hash",
        "chart_file",
        "spot_price",
        "signals_used",
        "chart_description",
        "predicted_direction",
        "predicted_magnitude",
        "confidence",
        "strategy_suggested",
        "reasoning",
        "prompt_version",
        "model_used",
        "model_provider",
        "routing_reason",
        "price_at_prediction",
    ]

    values = [row.get(c) for c in cols]

    placeholders = ",".join(["?"] * len(cols))
    col_sql = ",".join(cols)

    with connect(db_path) as conn:
        before = conn.total_changes
        cur = conn.execute(
            f"INSERT OR IGNORE INTO predictions ({col_sql}) VALUES ({placeholders})",
            tuple(values),
        )
        after = conn.total_changes
        inserted = after > before
        if not inserted:
            return None
        return int(cur.lastrowid)
def fetch_recent_predictions(db_path: str, limit: int) -> list[dict[str, Any]]:
    with connect(db_path) as conn:
        cur = conn.execute(
            "SELECT id, timestamp, predicted_direction, predicted_magnitude, confidence, result, signals_used "
            "FROM predictions ORDER BY timestamp DESC LIMIT ?",
            (int(limit),),
        )
        out = []
        for r in cur.fetchall():
            d = dict(r)
            try:
                d["signals_used"] = json.loads(d["signals_used"]) if d.get("signals_used") else None
            except Exception:
                pass
            out.append(d)
        return out


def fetch_latest_performance_summary(db_path: str) -> dict[str, Any] | None:
    with connect(db_path) as conn:
        cur = conn.execute(
            "SELECT generated_at, total_predictions, total_scored, overall_accuracy, summary_json "
            "FROM performance_summary ORDER BY generated_at DESC LIMIT 1"
        )
        r = cur.fetchone()
        if not r:
            return None
        d = dict(r)
        try:
            d["summary_json"] = json.loads(d["summary_json"]) if d.get("summary_json") else None
        except Exception:
            pass
        return d


def fetch_total_predictions(db_path: str) -> int:
    with connect(db_path) as conn:
        cur = conn.execute("SELECT COUNT(1) AS n FROM predictions")
        return int(cur.fetchone()[0])


def fetch_scored_predictions(db_path: str) -> list[dict[str, Any]]:
    with connect(db_path) as conn:
        cur = conn.execute(
            "SELECT result, confidence FROM predictions WHERE result IS NOT NULL"
        )
        return [dict(r) for r in cur.fetchall()]


def fetch_eligible_to_score(db_path: str, cutoff_ts_iso: str) -> list[dict[str, Any]]:
    with connect(db_path) as conn:
        cur = conn.execute(
            "SELECT id, timestamp, predicted_direction, predicted_magnitude, spot_price "
            "FROM predictions WHERE result IS NULL AND timestamp <= ?",
            (cutoff_ts_iso,),
        )
        return [dict(r) for r in cur.fetchall()]


def update_scoring(
    db_path: str,
    pred_id: int,
    price_at_prediction: float,
    price_at_outcome: float,
    actual_move: float,
    result: str,
    pnl_simulated: float,
    outcome_notes: str,
) -> None:
    scored_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    with connect(db_path) as conn:
        conn.execute(
            "UPDATE predictions SET price_at_prediction=?, price_at_outcome=?, actual_move=?, result=?, pnl_simulated=?, outcome_notes=?, scored_at=? "
            "WHERE id=?",
            (
                float(price_at_prediction),
                float(price_at_outcome),
                float(actual_move),
                result,
                float(pnl_simulated),
                outcome_notes,
                scored_at,
                int(pred_id),
            ),
        )


def insert_performance_summary(db_path: str, row: dict[str, Any]) -> int:
    cols = ["generated_at", "total_predictions", "total_scored", "overall_accuracy", "summary_json"]
    placeholders = ",".join(["?"] * len(cols))
    with connect(db_path) as conn:
        cur = conn.execute(
            f"INSERT INTO performance_summary ({','.join(cols)}) VALUES ({placeholders})",
            (
                row.get("generated_at"),
                int(row.get("total_predictions") or 0),
                int(row.get("total_scored") or 0),
                row.get("overall_accuracy"),
                row.get("summary_json") or "{}",
            ),
        )
        return int(cur.lastrowid)
