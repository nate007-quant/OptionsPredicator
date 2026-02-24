from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

from options_ai.config import Config, load_config
from options_ai.db import connect, db_path_from_url, init_db
from options_ai.ml_eod.calendar import CENTRAL_TZ, is_trading_day, session_times, to_central
from options_ai.ml_eod.features import EodFeatures, build_eod_features, _em_abs
from options_ai.ml_eod.labeling import EodLabels, compute_band_eod, eod_direction_label, level_event_labels
from options_ai.ml_eod.store import load_bundle
from options_ai.ml_eod.infer import infer as eod_infer


def _parse_iso(ts: str) -> datetime:
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _snapshot_rows_for_day(db_path: str, trade_day_ct: date) -> list[dict[str, Any]]:
    # Pull one row per snapshot_hash by choosing MIN(id) within the day window.
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
            SELECT id, source_snapshot_hash, COALESCE(observed_ts_utc, timestamp) AS ts,
                   spot_price, signals_used
            FROM predictions
            WHERE id IN (SELECT id FROM one)
            ORDER BY ts ASC
            """,
            (start_utc, end_utc),
        ).fetchall()
        return [dict(r) for r in rows]


def _extract_levels(signals_used_json: str) -> tuple[dict[str, Any], str | None, float | None, float | None]:
    # returns (levels, regime_label, atm_iv, expected_move_abs_15m)
    try:
        obj = json.loads(signals_used_json)
    except Exception:
        return {}, None, None, None
    comp = obj.get("computed") if isinstance(obj, dict) else None
    if not isinstance(comp, dict):
        return {}, None, None, None

    gex = comp.get("gex") if isinstance(comp.get("gex"), dict) else {}
    levels = gex.get("levels") if isinstance(gex.get("levels"), dict) else {}
    regime_label = gex.get("regime_label") if isinstance(gex.get("regime_label"), str) else None

    atm_iv = comp.get("atm_iv")
    em_abs = comp.get("expected_move_abs")
    try:
        atm_iv = float(atm_iv) if atm_iv is not None else None
    except Exception:
        atm_iv = None
    try:
        em_abs = float(em_abs) if em_abs is not None else None
    except Exception:
        em_abs = None

    return levels, regime_label, atm_iv, em_abs


@dataclass(frozen=True)
class EodDayBuild:
    trade_day: str
    levels_asof_snapshot_index: int
    open_price: float
    early_end_price: float
    close_price: float | None
    levels_json: dict[str, Any]
    features: EodFeatures
    labels: EodLabels | None


def build_day(
    *,
    cfg: Config,
    db_path: str,
    trade_day_ct: date,
    levels_asof_snapshot_index: int,
) -> EodDayBuild | None:
    if not is_trading_day(trade_day_ct):
        return None

    st = session_times(trade_day_ct=trade_day_ct, early_window_minutes=int(cfg.eod_early_window_minutes))

    rows = _snapshot_rows_for_day(db_path, trade_day_ct)
    if not rows:
        return None

    # Convert to central timestamps
    snaps: list[tuple[datetime, float, dict[str, Any], str]] = []
    for r in rows:
        try:
            t_utc = _parse_iso(str(r["ts"]))
            t_ct = to_central(t_utc)
            snaps.append((t_ct, float(r["spot_price"]), json.loads(r["signals_used"]), str(r["source_snapshot_hash"])))
        except Exception:
            continue

    # open snapshot: first >= 08:30
    after_open = [x for x in snaps if x[0] >= st.open_ct]
    if not after_open:
        return None

    open_snap = after_open[0]
    open_delay_min = (open_snap[0] - st.open_ct).total_seconds() / 60.0
    if open_delay_min > float(cfg.open_snapshot_max_delay_min):
        return None

    # early window snaps up to early_end
    early_snaps = [x for x in after_open if x[0] <= st.early_end_ct]
    if not early_snaps:
        return None

    # early_end snapshot: closest to T_early_end
    early_end_snap = min(early_snaps, key=lambda x: abs((x[0] - st.early_end_ct).total_seconds()))

    # levels-as-of snapshot index
    if levels_asof_snapshot_index >= len(early_snaps):
        return None
    levels_snap = early_snaps[int(levels_asof_snapshot_index)]

    # Close snapshot: last snapshot <= 15:00, within tolerance
    close_snap = None
    before_close = [x for x in snaps if x[0] <= st.close_ct]
    if before_close:
        close_snap = before_close[-1]
        if (st.close_ct - close_snap[0]).total_seconds() / 60.0 > float(cfg.close_snapshot_tolerance_min):
            close_snap = None

    # fallback: nearest within tolerance (either side)
    if close_snap is None:
        candidates = [x for x in snaps if abs((x[0] - st.close_ct).total_seconds()) / 60.0 <= float(cfg.close_snapshot_tolerance_min)]
        if candidates:
            close_snap = min(candidates, key=lambda x: abs((x[0] - st.close_ct).total_seconds()))

    levels, regime, atm_iv, em_abs_15m = _extract_levels(json.dumps(levels_snap[2].get("computed") if isinstance(levels_snap[2], dict) else {}))
    # Above is messy due to storage structure; parse directly from signals_used wrapper when present.
    if isinstance(levels_snap[2], dict) and isinstance(levels_snap[2].get("computed"), dict):
        computed = levels_snap[2]["computed"]
        gex = computed.get("gex") if isinstance(computed.get("gex"), dict) else {}
        levels = gex.get("levels") if isinstance(gex.get("levels"), dict) else {}
        regime = gex.get("regime_label") if isinstance(gex.get("regime_label"), str) else None
        try:
            atm_iv = float(computed.get("atm_iv")) if computed.get("atm_iv") is not None else None
        except Exception:
            atm_iv = None
        try:
            em_abs_15m = float(computed.get("expected_move_abs")) if computed.get("expected_move_abs") is not None else None
        except Exception:
            em_abs_15m = None

    if atm_iv is None:
        return None

    open_price = float(open_snap[1])
    early_end_price = float(early_end_snap[1])
    spot_asof = float(levels_snap[1])

    # EM scaled for 60 and 30
    em_60 = _em_abs(spot_asof, float(atm_iv), float(cfg.eod_early_window_minutes))
    em_30 = _em_abs(spot_asof, float(atm_iv), 30.0)

    feats = build_eod_features(
        trade_day=st.trade_day,
        open_price=open_price,
        early_end_price=early_end_price,
        early_prices=[(t, p) for t, p, _, _ in early_snaps],
        atm_iv=float(atm_iv),
        spot_asof=spot_asof,
        expected_move_abs_15m=float(em_abs_15m or 0.0),
        levels=levels,
        gex_regime_label=regime,
        early_window_minutes=int(cfg.eod_early_window_minutes),
    )

    # Labels if close exists
    labels = None
    close_price = float(close_snap[1]) if close_snap is not None else None
    if close_price is not None:
        move_eod = close_price - open_price
        band_eod = compute_band_eod(spot=spot_asof, atm_iv=float(atm_iv), min_eod_band_pts=float(cfg.min_eod_band_pts), k_eod=float(cfg.k_eod))
        dir_eod = eod_direction_label(move_eod_pts=move_eod, band_eod_pts=band_eod)

        level_events: dict[str, dict[str, int]] = {}
        for level_name in ("magnet", "call_wall", "put_wall"):
            obj = levels.get(level_name)
            strike = float(obj.get("strike")) if isinstance(obj, dict) and obj.get("strike") is not None else 0.0
            if not strike:
                continue
            level_events[level_name] = level_event_labels(
                early_prices=[(t, p) for t, p, _, _ in early_snaps],
                strike=strike,
                expected_move_abs_60m=em_60,
                expected_move_abs_30m=em_30,
                min_near_pts=float(cfg.min_near_pts),
                k_near=float(cfg.k_near),
                min_reject_pts=float(cfg.min_reject_pts),
                k_reject=float(cfg.k_reject),
                reject_lookahead_minutes=int(cfg.reject_lookahead_minutes),
            )

        labels = EodLabels(dir_eod=dir_eod, move_eod_pts=float(move_eod), band_eod_pts=float(band_eod), level_events=level_events)

    return EodDayBuild(
        trade_day=st.trade_day,
        levels_asof_snapshot_index=int(levels_asof_snapshot_index),
        open_price=open_price,
        early_end_price=early_end_price,
        close_price=close_price,
        levels_json={"levels": levels, "regime_label": regime},
        features=feats,
        labels=labels,
    )


def main() -> None:
    ap = argparse.ArgumentParser(description="Build EOD early-window dataset rows")
    ap.add_argument("--start", required=True, help="YYYY-MM-DD (Central trade_day)")
    ap.add_argument("--end", required=True, help="YYYY-MM-DD (Central trade_day)")
    ap.add_argument("--levels", default="both", choices=["0", "1", "both"])
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    cfg = load_config()
    db_path = db_path_from_url(cfg.database_url)
    init_db(db_path, str(__import__("pathlib").Path(__file__).resolve().parents[1] / "db" / "schema.sql"))

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)

    days = []
    d = start
    while d <= end:
        days.append(d)
        d = d + __import__("datetime").timedelta(days=1)

    levels_idx = [0, 1] if args.levels == "both" else [int(args.levels)]

    out = []
    for day in days:
        for li in levels_idx:
            b = build_day(cfg=cfg, db_path=db_path, trade_day_ct=day, levels_asof_snapshot_index=li)
            if b is None:
                continue
            out.append({
                "trade_day": b.trade_day,
                "lvl": li,
                "open": b.open_price,
                "early_end": b.early_end_price,
                "close": b.close_price,
                "has_labels": b.labels is not None,
            })

    print(json.dumps({"rows": out, "n": len(out)}, indent=2))


if __name__ == "__main__":
    main()
