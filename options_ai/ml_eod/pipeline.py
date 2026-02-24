from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

from zoneinfo import ZoneInfo

from options_ai.config import Config
from options_ai.db import connect
from options_ai.ml_eod.calendar import CENTRAL_TZ, is_trading_day, session_times, to_central
from options_ai.ml_eod.features import build_eod_features, _em_abs
from options_ai.ml_eod.infer import infer as eod_infer
from options_ai.ml_eod.labeling import compute_band_eod, eod_direction_label, level_event_labels
from options_ai.ml_eod.store import load_bundle
from options_ai.queries import upsert_eod_prediction


def _parse_iso(ts: str) -> datetime:
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _snapshots_for_day(db_path: str, trade_day_ct: date) -> list[dict[str, Any]]:
    start_ct = datetime.combine(trade_day_ct, datetime.min.time(), tzinfo=CENTRAL_TZ)
    end_ct = start_ct.replace(hour=23, minute=59, second=59)
    start_utc = start_ct.astimezone(timezone.utc).replace(microsecond=0).isoformat()
    end_utc = end_ct.astimezone(timezone.utc).replace(microsecond=0).isoformat()

    with connect(db_path) as conn:
        rows = conn.execute(
            """
            WITH one AS (
              SELECT MIN(id) AS id
              FROM predictions
              WHERE COALESCE(observed_ts_utc, timestamp) >= ?
                AND COALESCE(observed_ts_utc, timestamp) <= ?
              GROUP BY source_snapshot_hash
            )
            SELECT COALESCE(observed_ts_utc, timestamp) AS ts,
                   spot_price, signals_used
            FROM predictions
            WHERE id IN (SELECT id FROM one)
            ORDER BY ts ASC
            """,
            (start_utc, end_utc),
        ).fetchall()
        return [dict(r) for r in rows]


def _extract_computed(signals_used: str) -> dict[str, Any]:
    try:
        obj = json.loads(signals_used)
    except Exception:
        return {}
    if isinstance(obj, dict) and isinstance(obj.get("computed"), dict):
        return obj["computed"]
    return {}


def _levels_from_computed(computed: dict[str, Any]) -> tuple[dict[str, Any], str | None, float | None, float | None]:
    gex = computed.get("gex") if isinstance(computed.get("gex"), dict) else {}
    levels = gex.get("levels") if isinstance(gex.get("levels"), dict) else {}
    regime = gex.get("regime_label") if isinstance(gex.get("regime_label"), str) else None

    atm_iv = computed.get("atm_iv")
    em_abs = computed.get("expected_move_abs")
    try:
        atm_iv = float(atm_iv) if atm_iv is not None else None
    except Exception:
        atm_iv = None
    try:
        em_abs = float(em_abs) if em_abs is not None else None
    except Exception:
        em_abs = None

    return levels, regime, atm_iv, em_abs


def build_and_upsert_for_day(
    *,
    cfg: Config,
    db_path: str,
    trade_day_ct: date,
    levels_asof_snapshot_index: int,
    model_version: str,
) -> bool:
    if not is_trading_day(trade_day_ct):
        return False

    st = session_times(trade_day_ct=trade_day_ct, early_window_minutes=int(cfg.eod_early_window_minutes))
    rows = _snapshots_for_day(db_path, trade_day_ct)
    if not rows:
        return False

    snaps: list[tuple[datetime, float, dict[str, Any]]] = []
    for r in rows:
        try:
            t_ct = to_central(_parse_iso(str(r["ts"])))
            snaps.append((t_ct, float(r["spot_price"]), _extract_computed(str(r["signals_used"]))))
        except Exception:
            continue

    after_open = [x for x in snaps if x[0] >= st.open_ct]
    if not after_open:
        return False

    open_snap = after_open[0]
    open_delay_min = (open_snap[0] - st.open_ct).total_seconds() / 60.0
    if open_delay_min > float(cfg.open_snapshot_max_delay_min):
        return False

    early_snaps = [x for x in after_open if x[0] <= st.early_end_ct]
    if not early_snaps:
        return False

    if int(levels_asof_snapshot_index) >= len(early_snaps):
        return False

    early_end_snap = min(early_snaps, key=lambda x: abs((x[0] - st.early_end_ct).total_seconds()))
    levels_snap = early_snaps[int(levels_asof_snapshot_index)]

    levels, regime, atm_iv, em_abs_15m = _levels_from_computed(levels_snap[2])
    if atm_iv is None:
        return False

    open_price = float(open_snap[1])
    early_end_price = float(early_end_snap[1])
    spot_asof = float(levels_snap[1])

    # close snapshot last <= close within tolerance
    close_price = None
    before_close = [x for x in snaps if x[0] <= st.close_ct]
    if before_close:
        close_snap = before_close[-1]
        if (st.close_ct - close_snap[0]).total_seconds() / 60.0 <= float(cfg.close_snapshot_tolerance_min):
            close_price = float(close_snap[1])

    # fallback nearest within tolerance
    if close_price is None:
        candidates = [x for x in snaps if abs((x[0] - st.close_ct).total_seconds()) / 60.0 <= float(cfg.close_snapshot_tolerance_min)]
        if candidates:
            close_price = float(min(candidates, key=lambda x: abs((x[0] - st.close_ct).total_seconds()))[1])

    # Build features
    feats = build_eod_features(
        trade_day=st.trade_day,
        open_price=open_price,
        early_end_price=early_end_price,
        early_prices=[(t, p) for t, p, _ in early_snaps],
        atm_iv=float(atm_iv),
        spot_asof=spot_asof,
        expected_move_abs_15m=float(em_abs_15m or 0.0),
        levels=levels,
        gex_regime_label=regime,
        early_window_minutes=int(cfg.eod_early_window_minutes),
    )

    # Label fields if close exists
    label_dir = None
    label_move = None
    label_band = None
    label_events = None

    em_60 = _em_abs(spot_asof, float(atm_iv), float(cfg.eod_early_window_minutes))
    em_30 = _em_abs(spot_asof, float(atm_iv), 30.0)

    if close_price is not None:
        label_move = float(close_price - open_price)
        label_band = float(compute_band_eod(spot=spot_asof, atm_iv=float(atm_iv), min_eod_band_pts=float(cfg.min_eod_band_pts), k_eod=float(cfg.k_eod)))
        label_dir = eod_direction_label(move_eod_pts=float(label_move), band_eod_pts=float(label_band))
        le: dict[str, Any] = {}
        for level_name in ("magnet", "call_wall", "put_wall"):
            obj = levels.get(level_name)
            strike = float(obj.get("strike")) if isinstance(obj, dict) and obj.get("strike") is not None else 0.0
            if not strike:
                continue
            le[level_name] = level_event_labels(
                early_prices=[(t, p) for t, p, _ in early_snaps],
                strike=strike,
                expected_move_abs_60m=em_60,
                expected_move_abs_30m=em_30,
                min_near_pts=float(cfg.min_near_pts),
                k_near=float(cfg.k_near),
                min_reject_pts=float(cfg.min_reject_pts),
                k_reject=float(cfg.k_reject),
                reject_lookahead_minutes=int(cfg.reject_lookahead_minutes),
            )
        label_events = le

    # Inference if model exists
    pred_dir = "neutral"
    pred_conf = 0.0
    pred_move_pts = 0.0
    p_action = 0.0
    event_probs: dict[str, float] = {}

    band_eod = float(label_band) if label_band is not None else float(compute_band_eod(spot=spot_asof, atm_iv=float(atm_iv), min_eod_band_pts=float(cfg.min_eod_band_pts), k_eod=float(cfg.k_eod)))

    try:
        bundle = load_bundle(str(cfg.ml_models_dir), str(model_version))
        feature_keys = list(bundle.meta.get("feature_keys") or sorted(feats.features.keys()))
        inf = eod_infer(bundle=bundle, features=feats.features, feature_keys=feature_keys, action_threshold=float(cfg.eod_action_threshold), band_eod_pts=band_eod)
        pred_dir = inf.pred_dir
        pred_conf = float(inf.pred_conf)
        pred_move_pts = float(inf.pred_move_pts)
        p_action = float(inf.p_action)
        event_probs = dict(inf.event_probs or {})
    except Exception:
        pass

    created_at_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    upsert_eod_prediction(
        db_path,
        {
            "trade_day": st.trade_day,
            "asof_minutes": int(cfg.eod_asof_minutes),
            "levels_asof_snapshot_index": int(levels_asof_snapshot_index),
            "model_version": str(model_version),
            "created_at_utc": created_at_utc,
            "open_price": open_price,
            "early_end_price": early_end_price,
            "close_price": close_price,
            "levels_json": json.dumps({"levels": levels, "regime_label": regime}, sort_keys=True),
            "features_version": str(cfg.eod_features_version),
            "features_json": json.dumps(feats.features, sort_keys=True, separators=(",", ":")),
            "pred_dir": pred_dir,
            "pred_conf": float(pred_conf),
            "pred_move_pts": float(pred_move_pts),
            "p_action": float(p_action),
            "event_probs_json": json.dumps(event_probs, sort_keys=True),
            "label_dir": label_dir,
            "label_move_pts": label_move,
            "label_band_pts": label_band,
            "label_events_json": json.dumps(label_events, sort_keys=True) if label_events is not None else None,
            "scored_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat() if label_dir is not None else None,
        },
    )

    return True


def maybe_generate_today(cfg: Config, db_path: str) -> None:
    if not bool(cfg.eod_ml_enabled):
        return
    if bool(cfg.backtest_mode) or bool(cfg.replay_mode):
        return

    now_ct = datetime.now(timezone.utc).astimezone(CENTRAL_TZ)
    td = now_ct.date()
    if not is_trading_day(td):
        return

    st = session_times(trade_day_ct=td, early_window_minutes=int(cfg.eod_early_window_minutes))
    if now_ct < st.early_end_ct:
        return

    # Build both variants
    build_and_upsert_for_day(cfg=cfg, db_path=db_path, trade_day_ct=td, levels_asof_snapshot_index=0, model_version=str(cfg.eod_model_version_lvl0))
    build_and_upsert_for_day(cfg=cfg, db_path=db_path, trade_day_ct=td, levels_asof_snapshot_index=1, model_version=str(cfg.eod_model_version_lvl1))


