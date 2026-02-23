from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from options_ai.config import Config
from options_ai.queries import fetch_eligible_to_score, insert_performance_summary, update_scoring
from options_ai.utils.logger import log_daemon_event
from options_ai.utils.scoring import score_prediction, simulate_pnl
from options_ai.utils.summarizer import build_performance_summary, performance_summary_to_row


def _parse_ts(ts_iso: str) -> datetime:
    return datetime.fromisoformat(ts_iso.replace("Z", "+00:00")).astimezone(timezone.utc)


def _ensure_snapshot_index(state: dict[str, Any], paths: Any) -> None:
    """Ensure state['snapshot_index'] exists; if missing/empty, attempt a light rebuild by scanning filenames."""
    state.setdefault("snapshot_index", {})
    if state["snapshot_index"]:
        return

    # scan a few directories for snapshots
    dirs = [
        Path(paths.processed_snapshots_dir),
        Path(paths.incoming_snapshots_dir),
        Path(paths.historical_dir),
    ]

    from options_ai.processes.ingest import parse_snapshot_filename

    idx: dict[str, Any] = {}
    for d in dirs:
        if not d.exists():
            continue
        for p in d.glob("*.json"):
            try:
                parsed = parse_snapshot_filename(p.name)
                obs_iso = parsed.observed_dt_utc.replace(microsecond=0).isoformat()
                idx[obs_iso] = {"spot": parsed.spot_price, "file": p.name}
            except Exception:
                continue

    state["snapshot_index"] = idx


def _find_outcome_price(state: dict[str, Any], target_dt: datetime) -> tuple[float | None, str | None]:
    idx = state.get("snapshot_index") or {}
    if not idx:
        return None, None

    best_dt: datetime | None = None
    best_spot: float | None = None
    best_key: str | None = None

    for obs_iso, payload in idx.items():
        try:
            obs_dt = _parse_ts(obs_iso)
        except Exception:
            continue
        if obs_dt < target_dt:
            continue
        try:
            spot = float(payload.get("spot"))
        except Exception:
            continue
        if best_dt is None or obs_dt < best_dt:
            best_dt = obs_dt
            best_spot = spot
            best_key = obs_iso

    return best_spot, best_key


def score_due_predictions(*, cfg: Config, paths: Any, db_path: str, state: dict[str, Any]) -> int:
    _ensure_snapshot_index(state, paths)

    now = datetime.now(timezone.utc).replace(microsecond=0)
    cutoff = now - timedelta(minutes=int(cfg.outcome_delay_minutes))
    eligible = fetch_eligible_to_score(db_path, cutoff_ts_iso=cutoff.isoformat())

    scored = 0
    for p in eligible:
        try:
            pred_ts = _parse_ts(p["timestamp"]).replace(microsecond=0)
            target_ts = pred_ts + timedelta(minutes=int(cfg.outcome_delay_minutes))

            price_outcome, matched_obs = _find_outcome_price(state, target_ts)
            if price_outcome is None:
                continue

            price_pred = float(p.get("spot_price") or 0.0)
            s = score_prediction(
                predicted_direction=p["predicted_direction"],
                predicted_magnitude=float(p["predicted_magnitude"]),
                price_at_prediction=price_pred,
                price_at_outcome=float(price_outcome),
            )
            pnl = simulate_pnl(p["predicted_direction"], s.actual_move)

            update_scoring(
                db_path,
                pred_id=int(p["id"]),
                price_at_prediction=price_pred,
                price_at_outcome=float(price_outcome),
                actual_move=float(s.actual_move),
                result=s.result,
                pnl_simulated=float(pnl),
                outcome_notes=f"outcome_from_snapshot_ts={matched_obs}",
            )
            scored += 1
        except Exception as e:
            log_daemon_event(paths.logs_daemon_dir, "error", "scoring_error", pred_id=p.get("id"), error=str(e))

    if scored:
        ps = build_performance_summary(db_path)
        insert_performance_summary(db_path, performance_summary_to_row(ps))
        log_daemon_event(paths.logs_daemon_dir, "info", "performance_summary_refreshed", total_scored=ps.total_scored)

    return scored
