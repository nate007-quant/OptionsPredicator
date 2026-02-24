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
        "observed_ts_utc",
        "outcome_ts_utc",
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
    """Legacy: latest predictions by timestamp."""

    with connect(db_path) as conn:
        cur = conn.execute(
            "SELECT id, timestamp, predicted_direction, predicted_magnitude, confidence, result, spot_price "
            "FROM predictions ORDER BY timestamp DESC LIMIT ?",
            (int(limit),),
        )
        return [dict(r) for r in cur.fetchall()]


def fetch_recent_predictions_before(db_path: str, observed_ts_utc: str, limit: int) -> list[dict[str, Any]]:
    """Event-time recent predictions: strictly before T (no future leakage)."""

    with connect(db_path) as conn:
        cur = conn.execute(
            "SELECT id, observed_ts_utc, predicted_direction, predicted_magnitude, confidence, result, spot_price "
            "FROM predictions "
            "WHERE observed_ts_utc < ? "
            "ORDER BY observed_ts_utc DESC LIMIT ?",
            (observed_ts_utc, int(limit)),
        )
        out = []
        for r in cur.fetchall():
            d = dict(r)
            # provide 'timestamp' key expected by some callers
            d.setdefault("timestamp", d.get("observed_ts_utc"))
            out.append(d)
        return out


def fetch_gex_levels_before(
    db_path: str,
    observed_ts_utc: str,
    day_key_utc: str,
    limit: int = 50,
) -> list[float]:
    """Fetch prior GEX level strikes for the same UTC day, strictly before T.

    We parse the stored signals_used JSON (signals_used contains a JSON object with a 'computed' section).
    Returns a de-duped list in reverse-chronological order (most recent first).
    """

    with connect(db_path) as conn:
        cur = conn.execute(
            "SELECT observed_ts_utc, signals_used FROM predictions "
            "WHERE observed_ts_utc < ? AND substr(observed_ts_utc, 1, 10) = ? "
            "ORDER BY observed_ts_utc DESC LIMIT ?",
            (observed_ts_utc, day_key_utc, int(limit)),
        )
        rows = cur.fetchall()

    strikes: list[float] = []
    for r in rows:
        try:
            payload = json.loads(r["signals_used"])
            computed = payload.get("computed") or {}
            gex = computed.get("gex") or {}
            levels = gex.get("levels") or {}
            for k in ("call_wall", "put_wall", "magnet", "flip"):
                v = levels.get(k)
                if v is None:
                    continue
                try:
                    strikes.append(float(v))
                except Exception:
                    continue
        except Exception:
            continue

    # De-dup preserving order (most recent first)
    seen = set()
    out: list[float] = []
    for s in strikes:
        if s in seen:
            continue
        seen.add(s)
        out.append(s)
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
        cur = conn.execute("SELECT result, confidence FROM predictions WHERE result IS NOT NULL")
        return [dict(r) for r in cur.fetchall()]


def fetch_eligible_to_score(db_path: str, cutoff_ts_iso: str) -> list[dict[str, Any]]:
    """Fetch predictions eligible to be scored.

    Uses outcome_ts_utc when available; falls back to legacy timestamp comparison.
    cutoff_ts_iso is an ISO-8601 UTC time (now), and eligible means outcome_ts_utc <= cutoff.
    """

    with connect(db_path) as conn:
        # If outcome_ts_utc exists, use it; otherwise fallback to timestamp.
        try:
            cur = conn.execute(
                "SELECT id, timestamp, predicted_direction, predicted_magnitude, spot_price, observed_ts_utc, outcome_ts_utc "
                "FROM predictions "
                "WHERE result IS NULL AND COALESCE(outcome_ts_utc, datetime(timestamp, '+15 minutes')) <= ?",
                (cutoff_ts_iso,),
            )
        except Exception:
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
