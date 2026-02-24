from __future__ import annotations

import json
import math
from datetime import datetime, timedelta, timezone
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
        "features_version",
        "features_json",
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


def insert_model_usage(db_path: str, row: dict[str, Any]) -> int | None:
    cols = [
        "ts_utc",
        "observed_ts_utc",
        "snapshot_hash",
        "kind",
        "model_used",
        "model_provider",
        "prompt_chars",
        "output_chars",
        "latency_ms",
        "input_tokens",
        "output_tokens",
        "total_tokens",
        "est_input_tokens",
        "est_output_tokens",
        "est_total_tokens",
    ]
    values = [row.get(c) for c in cols]
    placeholders = ",".join(["?"] * len(cols))

    with connect(db_path) as conn:
        cur = conn.execute(
            f"INSERT INTO model_usage ({','.join(cols)}) VALUES ({placeholders})",
            tuple(values),
        )
        return int(cur.lastrowid)


def estimate_tokens_from_chars(prompt_chars: int, output_chars: int, tokens_per_char: float) -> tuple[int, int, int]:
    est_in = int(math.ceil(float(prompt_chars or 0) * float(tokens_per_char)))
    est_out = int(math.ceil(float(output_chars or 0) * float(tokens_per_char)))
    return est_in, est_out, est_in + est_out


def fetch_tokens_summary(db_path: str, *, now_ts_utc: str) -> dict[str, Any]:
    """Return last-60m + previous-60m token sums and avg per file."""

    # We use SQLite datetime() for simple windowing. now_ts_utc should be ISO string.
    with connect(db_path) as conn:
        cur = conn.execute(
            """
            SELECT
              SUM(CASE WHEN ts_utc >= datetime(?, '-60 minutes') THEN est_total_tokens ELSE 0 END) AS sum_last60,
              COUNT(DISTINCT CASE WHEN ts_utc >= datetime(?, '-60 minutes') THEN snapshot_hash ELSE NULL END) AS files_last60,
              SUM(CASE WHEN ts_utc >= datetime(?, '-120 minutes') AND ts_utc < datetime(?, '-60 minutes') THEN est_total_tokens ELSE 0 END) AS sum_prev60
            FROM model_usage
            """,
            (now_ts_utc, now_ts_utc, now_ts_utc, now_ts_utc),
        )
        r = cur.fetchone()
        sum_last60 = int(r[0] or 0)
        files_last60 = int(r[1] or 0)
        sum_prev60 = int(r[2] or 0)

    avg_per_file = (sum_last60 / files_last60) if files_last60 > 0 else None
    trend_pct = None
    if sum_prev60 > 0:
        trend_pct = (sum_last60 - sum_prev60) / float(sum_prev60)

    return {
        "est_total_tokens_last_60m": sum_last60,
        "avg_est_tokens_per_file_last_60m": avg_per_file,
        "trend_vs_prev_hour_pct": trend_pct,
        "prev_hour_est_total_tokens": sum_prev60,
        "files_last_60m": files_last60,
    }


def fetch_tokens_hourly_series(db_path: str, *, now_ts_utc: str, hours: int = 24) -> list[dict[str, Any]]:
    hours = max(1, int(hours))
    with connect(db_path) as conn:
        cur = conn.execute(
            """
            SELECT strftime('%Y-%m-%dT%H:00:00+00:00', ts_utc) AS hour_bucket,
                   SUM(est_total_tokens) AS tokens
            FROM model_usage
            WHERE ts_utc >= datetime(?, '-' || ? || ' hours')
            GROUP BY hour_bucket
            ORDER BY hour_bucket DESC
            LIMIT ?
            """,
            (now_ts_utc, int(hours), int(hours)),
        )
        rows = [dict(r) for r in cur.fetchall()]

    # Ensure descending order by default; caller can reverse.
    for r in rows:
        r["tokens"] = int(r.get("tokens") or 0)
    return rows


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
    """Fetch prior GEX level strikes for the same UTC day, strictly before T."""

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
        cur = conn.execute(
            "SELECT result, confidence, predicted_direction FROM predictions WHERE result IS NOT NULL"
        )
        return [dict(r) for r in cur.fetchall()]


def fetch_eligible_to_score(db_path: str, cutoff_ts_iso: str) -> list[dict[str, Any]]:
    """Fetch predictions eligible to be scored.

    Uses outcome_ts_utc when available; falls back to legacy timestamp comparison.
    cutoff_ts_iso is an ISO-8601 UTC time (now), and eligible means outcome_ts_utc <= cutoff.
    """

    with connect(db_path) as conn:
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
