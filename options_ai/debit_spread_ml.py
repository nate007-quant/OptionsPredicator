from __future__ import annotations

import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import psycopg

from options_ai.utils.task_state import utc_now_iso, write_task_state
from sklearn.linear_model import Ridge, LogisticRegression


ANCHOR_TYPES = ("ATM", "CALL_WALL", "PUT_WALL", "MAGNET")
SPREAD_TYPES = ("CALL", "PUT")


@dataclass(frozen=True)
class DebitMLConfig:
    db_dsn: str

    horizon_minutes: int = 30

    bigwin_mult_atm: float = 2.0
    bigwin_mult_wall: float = 4.0

    # training controls
    min_train_rows: int = 300
    max_train_rows: int = 50000
    retrain_seconds: int = 15 * 60

    # scoring loop
    poll_seconds: float = 20.0

    models_dir: str = "/mnt/options_ai/models/debit_spread"
    model_version: str = "debit_ridge_v2_flow"


SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS spx;

CREATE TABLE IF NOT EXISTS spx.debit_spread_scores_0dte (
  snapshot_ts TIMESTAMPTZ NOT NULL,
  horizon_minutes INT NOT NULL,
  anchor_type TEXT NOT NULL,
  spread_type TEXT NOT NULL,

  pred_change NUMERIC,
  p_bigwin NUMERIC,
  model_version TEXT NOT NULL,
  trained_at TIMESTAMPTZ,
  computed_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  PRIMARY KEY (snapshot_ts, horizon_minutes, anchor_type, spread_type)
);

CREATE INDEX IF NOT EXISTS debit_spread_scores_ts_idx ON spx.debit_spread_scores_0dte (snapshot_ts DESC);

-- Forward-compatible: add new columns if the table already existed
ALTER TABLE spx.debit_spread_scores_0dte ADD COLUMN IF NOT EXISTS p_bigwin NUMERIC;
"""


UPSERT_SCORE_SQL = """
INSERT INTO spx.debit_spread_scores_0dte (
  snapshot_ts, horizon_minutes, anchor_type, spread_type,
  pred_change, p_bigwin, model_version, trained_at
) VALUES (
  %(snapshot_ts)s, %(horizon_minutes)s, %(anchor_type)s, %(spread_type)s,
  %(pred_change)s, %(p_bigwin)s, %(model_version)s, %(trained_at)s
)
ON CONFLICT (snapshot_ts, horizon_minutes, anchor_type, spread_type) DO UPDATE SET
  pred_change = EXCLUDED.pred_change,
  p_bigwin = EXCLUDED.p_bigwin,
  model_version = EXCLUDED.model_version,
  trained_at = EXCLUDED.trained_at,
  computed_at = now();
"""


def ensure_schema(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)
        conn.commit()


def _as_float(x: Any) -> float | None:
    if x is None:
        return None
    try:
        return float(x)
    except Exception:
        return None


def _bigwin_required_mult(anchor_type: str, *, mult_atm: float, mult_wall: float) -> float:
    a = (anchor_type or '').upper()
    if a == 'ATM':
        return float(mult_atm)
    # CALL_WALL, PUT_WALL, MAGNET treated as wall-style
    return float(mult_wall)


def _onehot(val: str, choices: tuple[str, ...]) -> list[float]:
    v = (val or "").upper()
    return [1.0 if v == c else 0.0 for c in choices]


_NUM_FEATURES = [
    "debit_points",
    "width",
    "anchor_to_spot",
    "abs_anchor_to_spot",
    "spot",
    "atm_iv",
    "skew_25d",
    "bf_25d",
    "pcr_volume",
    "pcr_oi",
    "contract_count",
    "valid_iv_count",
    "valid_mid_count",
    "itm_vol",
    "atm_vol",
    "otm_vol",
    "tot_vol",
    "d_tot_vol",
    "d_call_oi",
    "d_put_oi",
    "sma_spot_5",
    "sma_spot_20",
    "bb_pctb_20",
    "rsi_14",
    "twap_spot_day",
    "vwap_chainweighted_spot_day",
    "flow_pct_bullish",
    "flow_pct_bearish",
    "flow_breadth",
    "flow_bucket_net_flow",
    "flow_bucket_robust_z",
    "flow_skew",
    "flow_confidence",
    "flow_atm_corridor_net",
    "flow_atm_corridor_frac",
    "flow_top3_share",
    "flow_top5_share",
]


def _row_to_features(r: dict[str, Any]) -> tuple[np.ndarray, list[str]]:
    anchor_type = str(r.get("anchor_type") or "")
    spread_type = str(r.get("spread_type") or "")

    # numeric inputs
    debit = _as_float(r.get("debit_points"))
    k_long = _as_float(r.get("k_long"))
    k_short = _as_float(r.get("k_short"))
    anchor_strike = _as_float(r.get("anchor_strike"))
    spot = _as_float(r.get("spot"))

    width = None
    if k_long is not None and k_short is not None:
        width = abs(float(k_short) - float(k_long))

    anchor_to_spot = None
    abs_anchor_to_spot = None
    if anchor_strike is not None and spot is not None:
        anchor_to_spot = float(anchor_strike - spot)
        abs_anchor_to_spot = abs(anchor_to_spot)

    feat_map = {
        "debit_points": debit,
        "width": width,
        "anchor_to_spot": anchor_to_spot,
        "abs_anchor_to_spot": abs_anchor_to_spot,
        "spot": spot,
        "atm_iv": _as_float(r.get("atm_iv")),
        "skew_25d": _as_float(r.get("skew_25d")),
        "bf_25d": _as_float(r.get("bf_25d")),
        "pcr_volume": _as_float(r.get("pcr_volume")),
        "pcr_oi": _as_float(r.get("pcr_oi")),
        "contract_count": _as_float(r.get("contract_count")),
        "valid_iv_count": _as_float(r.get("valid_iv_count")),
        "valid_mid_count": _as_float(r.get("valid_mid_count")),
        "itm_vol": _as_float(r.get("itm_vol")),
        "atm_vol": _as_float(r.get("atm_vol")),
        "otm_vol": _as_float(r.get("otm_vol")),
        "tot_vol": _as_float(r.get("tot_vol")),
        "d_tot_vol": _as_float(r.get("d_tot_vol")),
        "d_call_oi": _as_float(r.get("d_call_oi")),
        "d_put_oi": _as_float(r.get("d_put_oi")),
        "sma_spot_5": _as_float(r.get("sma_spot_5")),
        "sma_spot_20": _as_float(r.get("sma_spot_20")),
        "bb_pctb_20": _as_float(r.get("bb_pctb_20")),
        "rsi_14": _as_float(r.get("rsi_14")),
        "twap_spot_day": _as_float(r.get("twap_spot_day")),
        "vwap_chainweighted_spot_day": _as_float(r.get("vwap_chainweighted_spot_day")),
        "flow_pct_bullish": _as_float(r.get("flow_pct_bullish")),
        "flow_pct_bearish": _as_float(r.get("flow_pct_bearish")),
        "flow_breadth": _as_float(r.get("flow_breadth")),
        "flow_bucket_net_flow": _as_float(r.get("flow_bucket_net_flow")),
        "flow_bucket_robust_z": _as_float(r.get("flow_bucket_robust_z")),
        "flow_skew": _as_float(r.get("flow_skew")),
        "flow_confidence": _as_float(r.get("flow_confidence")),
        "flow_atm_corridor_net": _as_float(r.get("flow_atm_corridor_net")),
        "flow_atm_corridor_frac": _as_float(r.get("flow_atm_corridor_frac")),
        "flow_top3_share": _as_float(r.get("flow_top3_share")),
        "flow_top5_share": _as_float(r.get("flow_top5_share")),
    }

    nums = [feat_map[k] for k in _NUM_FEATURES]
    cats = _onehot(anchor_type, ANCHOR_TYPES) + _onehot(spread_type, SPREAD_TYPES)

    x = np.array([(0.0 if v is None else float(v)) for v in nums] + cats, dtype=np.float64)

    feature_names = _NUM_FEATURES + [f"anchor_{c}" for c in ANCHOR_TYPES] + [f"spread_{c}" for c in SPREAD_TYPES]
    return x, feature_names


def _compute_impute_values(X_raw: np.ndarray, mask: np.ndarray) -> np.ndarray:
    """Median imputation for numeric features; categorical one-hots are already 0/1."""
    # X_raw already has None converted to 0; use mask to know what was missing.
    # For simplicity we keep 0 for missing; but for stability, compute medians where not missing.
    impute = np.zeros(X_raw.shape[1], dtype=np.float64)
    for j in range(X_raw.shape[1]):
        col = X_raw[:, j]
        m = mask[:, j]
        present = col[~m]
        if present.size == 0:
            impute[j] = 0.0
        else:
            impute[j] = float(np.median(present))
    return impute


def _apply_impute(X_raw: np.ndarray, mask: np.ndarray, impute: np.ndarray) -> np.ndarray:
    X = X_raw.copy()
    for j in range(X.shape[1]):
        X[mask[:, j], j] = impute[j]
    return X


def _fetch_training_rows(conn: psycopg.Connection, *, horizon_minutes: int, limit: int) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
              l.change, l.debit_t, l.debit_tH,
              c.anchor_type, c.spread_type,
              c.debit_points, c.anchor_strike, c.k_long, c.k_short,
              f.spot, f.atm_iv, f.skew_25d, f.bf_25d, f.pcr_volume, f.pcr_oi,
              f.contract_count, f.valid_iv_count, f.valid_mid_count,
              f.itm_vol, f.atm_vol, f.otm_vol, f.tot_vol,
              f.d_tot_vol, f.d_call_oi, f.d_put_oi,
              f.sma_spot_5, f.sma_spot_20, f.bb_pctb_20, f.rsi_14,
              f.twap_spot_day, f.vwap_chainweighted_spot_day,
              f.flow_pct_bullish, f.flow_pct_bearish, f.flow_breadth,
              f.flow_bucket_net_flow, f.flow_bucket_robust_z, f.flow_skew, f.flow_confidence,
              f.flow_atm_corridor_net, f.flow_atm_corridor_frac, f.flow_top3_share, f.flow_top5_share
            FROM spx.debit_spread_labels_0dte l
            JOIN spx.debit_spread_candidates_0dte c
              ON c.snapshot_ts = l.snapshot_ts
             AND c.anchor_type = l.anchor_type
             AND c.spread_type = l.spread_type
            JOIN spx.chain_features_0dte f
              ON f.snapshot_ts = c.snapshot_ts
            WHERE l.horizon_minutes = %s
              AND l.is_missing_future = false
              AND l.change IS NOT NULL
              AND c.tradable = true
              AND f.low_quality = false
            ORDER BY l.snapshot_ts DESC
            LIMIT %s
            """,
            (int(horizon_minutes), int(limit)),
        )
        rows = []
        for r in cur.fetchall():
            rows.append(
                {
                    "y": _as_float(r[0]),
                    "debit_t": _as_float(r[1]),
                    "debit_tH": _as_float(r[2]),
                    "anchor_type": r[3],
                    "spread_type": r[4],
                    "debit_points": _as_float(r[5]),
                    "anchor_strike": _as_float(r[6]),
                    "k_long": _as_float(r[7]),
                    "k_short": _as_float(r[8]),
                    "spot": _as_float(r[9]),
                    "atm_iv": _as_float(r[10]),
                    "skew_25d": _as_float(r[11]),
                    "bf_25d": _as_float(r[12]),
                    "pcr_volume": _as_float(r[13]),
                    "pcr_oi": _as_float(r[14]),
                    "contract_count": _as_float(r[15]),
                    "valid_iv_count": _as_float(r[16]),
                    "valid_mid_count": _as_float(r[17]),
                    "itm_vol": _as_float(r[18]),
                    "atm_vol": _as_float(r[19]),
                    "otm_vol": _as_float(r[20]),
                    "tot_vol": _as_float(r[21]),
                    "d_tot_vol": _as_float(r[22]),
                    "d_call_oi": _as_float(r[23]),
                    "d_put_oi": _as_float(r[24]),
                    "sma_spot_5": _as_float(r[25]),
                    "sma_spot_20": _as_float(r[26]),
                    "bb_pctb_20": _as_float(r[27]),
                    "rsi_14": _as_float(r[28]),
                    "twap_spot_day": _as_float(r[29]),
                    "vwap_chainweighted_spot_day": _as_float(r[30]),
                    "flow_pct_bullish": _as_float(r[31]),
                    "flow_pct_bearish": _as_float(r[32]),
                    "flow_breadth": _as_float(r[33]),
                    "flow_bucket_net_flow": _as_float(r[34]),
                    "flow_bucket_robust_z": _as_float(r[35]),
                    "flow_skew": _as_float(r[36]),
                    "flow_confidence": _as_float(r[37]),
                    "flow_atm_corridor_net": _as_float(r[38]),
                    "flow_atm_corridor_frac": _as_float(r[39]),
                    "flow_top3_share": _as_float(r[40]),
                    "flow_top5_share": _as_float(r[41]),
                }
            )
        return rows


@dataclass
class _TrainedModel:
    model: Ridge
    clf: LogisticRegression | None
    impute: np.ndarray
    feature_names: list[str]
    trained_at: datetime
    horizon_minutes: int
    model_version: str


def _model_path(cfg: DebitMLConfig) -> Path:
    return Path(cfg.models_dir) / f"{cfg.model_version}_h{int(cfg.horizon_minutes)}.joblib"


def train_if_needed(conn: psycopg.Connection, cfg: DebitMLConfig, *, force: bool = False) -> _TrainedModel | None:
    p = _model_path(cfg)
    p.parent.mkdir(parents=True, exist_ok=True)

    if not force and p.exists():
        try:
            obj = joblib.load(p)
            if isinstance(obj, dict) and obj.get("model_version") == cfg.model_version and int(obj.get("horizon_minutes")) == int(cfg.horizon_minutes):
                trained_at = obj.get("trained_at")
                if isinstance(trained_at, datetime):
                    age = (datetime.now(timezone.utc) - trained_at).total_seconds()
                    if age < float(cfg.retrain_seconds):
                        return _TrainedModel(
                            model=obj["model"],
                            clf=obj.get("clf"),
                            impute=obj["impute"],
                            feature_names=list(obj.get("feature_names") or []),
                            trained_at=trained_at,
                            horizon_minutes=int(cfg.horizon_minutes),
                            model_version=str(cfg.model_version),
                        )
            if not did:
                write_task_state(task_path, None)
        except Exception:
            pass

    rows = _fetch_training_rows(conn, horizon_minutes=int(cfg.horizon_minutes), limit=int(cfg.max_train_rows))
    rows = [r for r in rows if r.get("y") is not None]

    if len(rows) < int(cfg.min_train_rows):
        return None

    # Build X_raw + mask for imputation.
    X_list = []
    mask_list = []
    y_list = []
    y_bigwin = []
    feat_names: list[str] | None = None

    for r in rows:
        # Preserve missingness mask for numeric portion (before None->0)
        # We'll treat all missing numeric fields as masked.
        nums = []
        for k in _NUM_FEATURES:
            v = r.get(k)
            nums.append(v)
        num_mask = [v is None for v in nums]

        x, names = _row_to_features(r)
        if feat_names is None:
            feat_names = names

        # mask vector length = full X; only numeric features masked.
        m = np.zeros_like(x, dtype=bool)
        m[: len(_NUM_FEATURES)] = np.array(num_mask, dtype=bool)

        X_list.append(x)
        mask_list.append(m)
        y_list.append(float(r["y"]))

        debit_t = _as_float(r.get("debit_t"))
        debit_tH = _as_float(r.get("debit_tH"))
        mult = _bigwin_required_mult(str(r.get("anchor_type") or ""), mult_atm=cfg.bigwin_mult_atm, mult_wall=cfg.bigwin_mult_wall)
        big = 0
        if debit_t is not None and debit_tH is not None and debit_t > 0:
            big = 1 if float(debit_tH) >= float(mult) * float(debit_t) else 0
        y_bigwin.append(int(big))

    X_raw = np.vstack(X_list)
    mask = np.vstack(mask_list)
    y = np.array(y_list, dtype=np.float64)

    impute = _compute_impute_values(X_raw, mask)
    X = _apply_impute(X_raw, mask, impute)

    model = Ridge(alpha=1.0, random_state=0)
    model.fit(X, y)

    clf = None
    try:
        ys = np.array(y_bigwin, dtype=np.int64)
        if ys.min() != ys.max():
            clf = LogisticRegression(max_iter=2000)
            clf.fit(X, ys)
    except Exception:
        clf = None

    trained_at = datetime.now(timezone.utc).replace(microsecond=0)

    obj = {
        "model": model,
        "clf": clf,
        "impute": impute,
        "feature_names": feat_names or [],
        "trained_at": trained_at,
        "horizon_minutes": int(cfg.horizon_minutes),
        "model_version": str(cfg.model_version),
    }

    joblib.dump(obj, p)

    return _TrainedModel(
        model=model,
        clf=clf,
        impute=impute,
        feature_names=feat_names or [],
        trained_at=trained_at,
        horizon_minutes=int(cfg.horizon_minutes),
        model_version=str(cfg.model_version),
    )




def _latest_candidate_snapshot_ts(conn: psycopg.Connection) -> datetime | None:
    """Return the latest snapshot_ts present in debit_spread_candidates_0dte."""
    with conn.cursor() as cur:
        cur.execute("SELECT max(snapshot_ts) FROM spx.debit_spread_candidates_0dte")
        r = cur.fetchone()
        return r[0] if r else None


def score_latest_snapshot(conn: psycopg.Connection, cfg: DebitMLConfig, tm: _TrainedModel) -> int:
    """Score tradable candidates for the latest snapshot and upsert into scores table."""

    latest = _latest_candidate_snapshot_ts(conn)
    if latest is None:
        return 0

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
              c.anchor_type, c.spread_type,
              c.debit_points, c.anchor_strike, c.k_long, c.k_short,
              f.spot, f.atm_iv, f.skew_25d, f.bf_25d, f.pcr_volume, f.pcr_oi,
              f.contract_count, f.valid_iv_count, f.valid_mid_count,
              f.itm_vol, f.atm_vol, f.otm_vol, f.tot_vol,
              f.d_tot_vol, f.d_call_oi, f.d_put_oi,
              f.sma_spot_5, f.sma_spot_20, f.bb_pctb_20, f.rsi_14,
              f.twap_spot_day, f.vwap_chainweighted_spot_day,
              f.flow_pct_bullish, f.flow_pct_bearish, f.flow_breadth,
              f.flow_bucket_net_flow, f.flow_bucket_robust_z, f.flow_skew, f.flow_confidence,
              f.flow_atm_corridor_net, f.flow_atm_corridor_frac, f.flow_top3_share, f.flow_top5_share
            FROM spx.debit_spread_candidates_0dte c
            JOIN spx.chain_features_0dte f
              ON f.snapshot_ts = c.snapshot_ts
            WHERE c.snapshot_ts = %s
              AND c.tradable = true
              AND f.low_quality = false
            """,
            (latest,),
        )
        rows = cur.fetchall()

    if not rows:
        return 0

    X_list = []
    mask_list = []
    keys = []

    for rr in rows:
        d = {
            "anchor_type": rr[0],
            "spread_type": rr[1],
            "debit_points": _as_float(rr[2]),
            "anchor_strike": _as_float(rr[3]),
            "k_long": _as_float(rr[4]),
            "k_short": _as_float(rr[5]),
            "spot": _as_float(rr[6]),
            "atm_iv": _as_float(rr[7]),
            "skew_25d": _as_float(rr[8]),
            "bf_25d": _as_float(rr[9]),
            "pcr_volume": _as_float(rr[10]),
            "pcr_oi": _as_float(rr[11]),
            "contract_count": _as_float(rr[12]),
            "valid_iv_count": _as_float(rr[13]),
            "valid_mid_count": _as_float(rr[14]),
            "itm_vol": _as_float(rr[15]),
            "atm_vol": _as_float(rr[16]),
            "otm_vol": _as_float(rr[17]),
            "tot_vol": _as_float(rr[18]),
            "d_tot_vol": _as_float(rr[19]),
            "d_call_oi": _as_float(rr[20]),
            "d_put_oi": _as_float(rr[21]),
            "sma_spot_5": _as_float(rr[22]),
            "sma_spot_20": _as_float(rr[23]),
            "bb_pctb_20": _as_float(rr[24]),
            "rsi_14": _as_float(rr[25]),
            "twap_spot_day": _as_float(rr[26]),
            "vwap_chainweighted_spot_day": _as_float(rr[27]),
            "flow_pct_bullish": _as_float(rr[28]),
            "flow_pct_bearish": _as_float(rr[29]),
            "flow_breadth": _as_float(rr[30]),
            "flow_bucket_net_flow": _as_float(rr[31]),
            "flow_bucket_robust_z": _as_float(rr[32]),
            "flow_skew": _as_float(rr[33]),
            "flow_confidence": _as_float(rr[34]),
            "flow_atm_corridor_net": _as_float(rr[35]),
            "flow_atm_corridor_frac": _as_float(rr[36]),
            "flow_top3_share": _as_float(rr[37]),
            "flow_top5_share": _as_float(rr[38]),
        }

        nums = [d.get(k) for k in _NUM_FEATURES]
        num_mask = [v is None for v in nums]

        x, _names = _row_to_features(d)
        m = np.zeros_like(x, dtype=bool)
        m[: len(_NUM_FEATURES)] = np.array(num_mask, dtype=bool)

        X_list.append(x)
        mask_list.append(m)
        keys.append((str(d["anchor_type"]), str(d["spread_type"])))

    X_raw = np.vstack(X_list)
    mask = np.vstack(mask_list)
    X = _apply_impute(X_raw, mask, tm.impute)

    preds = tm.model.predict(X)
    p_big = None
    if tm.clf is not None:
        try:
            p_big = tm.clf.predict_proba(X)[:, 1]
        except Exception:
            p_big = None

    upserted = 0
    with conn.cursor() as cur:
        for idx, ((anchor_type, spread_type), pred) in enumerate(zip(keys, preds, strict=True)):
            pb = float(p_big[idx]) if p_big is not None else None
            cur.execute(
                UPSERT_SCORE_SQL,
                {
                    "snapshot_ts": latest,
                    "horizon_minutes": int(cfg.horizon_minutes),
                    "anchor_type": anchor_type,
                    "spread_type": spread_type,
                    "pred_change": float(pred),
                    "p_bigwin": pb,
                    "model_version": str(cfg.model_version),
                    "trained_at": tm.trained_at,
                },
            )
            upserted += 1
        conn.commit()

    return upserted


def _snapshots_missing_scores(conn: psycopg.Connection, *, horizon_minutes: int, model_version: str, limit: int) -> list[datetime]:
    """Return recent snapshot_ts values that need scores for the active model version."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT c.snapshot_ts
            FROM spx.debit_spread_candidates_0dte c
            JOIN spx.chain_features_0dte f
              ON f.snapshot_ts = c.snapshot_ts
            WHERE c.tradable = true
              AND f.low_quality = false
              AND NOT EXISTS (
                SELECT 1
                FROM spx.debit_spread_scores_0dte s
                WHERE s.snapshot_ts = c.snapshot_ts
                  AND s.horizon_minutes = %s
                  AND s.model_version = %s
              )
            ORDER BY c.snapshot_ts DESC
            LIMIT %s
            """,
            (int(horizon_minutes), str(model_version), int(limit)),
        )
        return [r[0] for r in cur.fetchall()]


def _score_snapshot(conn: psycopg.Connection, cfg: DebitMLConfig, tm: _TrainedModel, *, snapshot_ts: datetime) -> int:
    """Score tradable candidates for a specific snapshot_ts."""

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
              c.anchor_type, c.spread_type,
              c.debit_points, c.anchor_strike, c.k_long, c.k_short,
              f.spot, f.atm_iv, f.skew_25d, f.bf_25d, f.pcr_volume, f.pcr_oi,
              f.contract_count, f.valid_iv_count, f.valid_mid_count,
              f.itm_vol, f.atm_vol, f.otm_vol, f.tot_vol,
              f.d_tot_vol, f.d_call_oi, f.d_put_oi,
              f.sma_spot_5, f.sma_spot_20, f.bb_pctb_20, f.rsi_14,
              f.twap_spot_day, f.vwap_chainweighted_spot_day,
              f.flow_pct_bullish, f.flow_pct_bearish, f.flow_breadth,
              f.flow_bucket_net_flow, f.flow_bucket_robust_z, f.flow_skew, f.flow_confidence,
              f.flow_atm_corridor_net, f.flow_atm_corridor_frac, f.flow_top3_share, f.flow_top5_share
            FROM spx.debit_spread_candidates_0dte c
            JOIN spx.chain_features_0dte f
              ON f.snapshot_ts = c.snapshot_ts
            WHERE c.snapshot_ts = %s
              AND c.tradable = true
              AND f.low_quality = false
            """,
            (snapshot_ts,),
        )
        rows = cur.fetchall()

    if not rows:
        return 0

    X_list = []
    keys = []

    for rr in rows:
        d = {
            "anchor_type": rr[0],
            "spread_type": rr[1],
            "debit_points": _as_float(rr[2]),
            "anchor_strike": _as_float(rr[3]),
            "k_long": _as_float(rr[4]),
            "k_short": _as_float(rr[5]),
            "spot": _as_float(rr[6]),
            "atm_iv": _as_float(rr[7]),
            "skew_25d": _as_float(rr[8]),
            "bf_25d": _as_float(rr[9]),
            "pcr_volume": _as_float(rr[10]),
            "pcr_oi": _as_float(rr[11]),
            "contract_count": _as_float(rr[12]),
            "valid_iv_count": _as_float(rr[13]),
            "valid_mid_count": _as_float(rr[14]),
            "itm_vol": _as_float(rr[15]),
            "atm_vol": _as_float(rr[16]),
            "otm_vol": _as_float(rr[17]),
            "tot_vol": _as_float(rr[18]),
            "d_tot_vol": _as_float(rr[19]),
            "d_call_oi": _as_float(rr[20]),
            "d_put_oi": _as_float(rr[21]),
            "sma_spot_5": _as_float(rr[22]),
            "sma_spot_20": _as_float(rr[23]),
            "bb_pctb_20": _as_float(rr[24]),
            "rsi_14": _as_float(rr[25]),
            "twap_spot_day": _as_float(rr[26]),
            "vwap_chainweighted_spot_day": _as_float(rr[27]),
            "flow_pct_bullish": _as_float(rr[28]),
            "flow_pct_bearish": _as_float(rr[29]),
            "flow_breadth": _as_float(rr[30]),
            "flow_bucket_net_flow": _as_float(rr[31]),
            "flow_bucket_robust_z": _as_float(rr[32]),
            "flow_skew": _as_float(rr[33]),
            "flow_confidence": _as_float(rr[34]),
            "flow_atm_corridor_net": _as_float(rr[35]),
            "flow_atm_corridor_frac": _as_float(rr[36]),
            "flow_top3_share": _as_float(rr[37]),
            "flow_top5_share": _as_float(rr[38]),
        }

        nums = [d.get(k) for k in _NUM_FEATURES]
        num_mask = [v is None for v in nums]

        x, _names = _row_to_features(d)
        m = np.zeros_like(x, dtype=bool)
        m[: len(_NUM_FEATURES)] = np.array(num_mask, dtype=bool)

        X_list.append(_apply_impute(x.reshape(1, -1), m.reshape(1, -1), tm.impute)[0])
        keys.append((str(d["anchor_type"]), str(d["spread_type"])))

    X = np.vstack(X_list)

    preds = tm.model.predict(X)
    p_big = None
    if tm.clf is not None:
        try:
            p_big = tm.clf.predict_proba(X)[:, 1]
        except Exception:
            p_big = None

    upserted = 0
    with conn.cursor() as cur:
        for idx, ((anchor_type, spread_type), pred) in enumerate(zip(keys, preds, strict=True)):
            pb = float(p_big[idx]) if p_big is not None else None
            cur.execute(
                UPSERT_SCORE_SQL,
                {
                    "snapshot_ts": snapshot_ts,
                    "horizon_minutes": int(cfg.horizon_minutes),
                    "anchor_type": anchor_type,
                    "spread_type": spread_type,
                    "pred_change": float(pred),
                    "p_bigwin": pb,
                    "model_version": str(cfg.model_version),
                    "trained_at": tm.trained_at,
                },
            )
            upserted += 1
        conn.commit()

    return upserted


def score_recent_backfill(conn: psycopg.Connection, cfg: DebitMLConfig, tm: _TrainedModel, *, limit: int = 200) -> int:
    """Backfill scores for recent snapshots that are missing scores."""
    total = 0
    for ts in _snapshots_missing_scores(conn, horizon_minutes=int(cfg.horizon_minutes), model_version=str(cfg.model_version), limit=int(limit)):
        total += _score_snapshot(conn, cfg, tm, snapshot_ts=ts)
    return total


def run_daemon(cfg: DebitMLConfig) -> None:
    task_path = os.getenv('DEBIT_ML_TASK_PATH', '/mnt/options_ai/state/task_debit_ml.json')
    last_train_ts = 0.0
    last_scored_snapshot_ts: datetime | None = None
    last_scored_trained_at: datetime | None = None

    with psycopg.connect(cfg.db_dsn) as conn:
        ensure_schema(conn)

    while True:
        did = False
        try:
            with psycopg.connect(cfg.db_dsn) as conn:
                now = time.time()
                force = (now - last_train_ts) >= float(cfg.retrain_seconds)
                write_task_state(task_path, {"stage": "ml_train_if_needed", "force": bool(force), "started_at": utc_now_iso()})
                tm = train_if_needed(conn, cfg, force=force)
                if tm is not None:
                    last_train_ts = now

                    # Avoid tight upsert loops: only rescore the latest snapshot when it advances
                    # (or when the model was retrained, in which case we refresh once).
                    latest = _latest_candidate_snapshot_ts(conn)
                    if latest is not None:
                        should_score_latest = (
                            latest != last_scored_snapshot_ts
                            or tm.trained_at != last_scored_trained_at
                        )
                        if should_score_latest:
                            write_task_state(task_path, {"stage": "ml_score_latest", "snapshot_ts": latest.isoformat().replace("+00:00","Z"), "horizon_minutes": int(cfg.horizon_minutes), "started_at": utc_now_iso()})
                            n = _score_snapshot(conn, cfg, tm, snapshot_ts=latest)
                            last_scored_snapshot_ts = latest
                            last_scored_trained_at = tm.trained_at
                        else:
                            n = 0
                    else:
                        n = 0

                    write_task_state(task_path, {"stage": "ml_score_backfill", "horizon_minutes": int(cfg.horizon_minutes), "limit": 200, "started_at": utc_now_iso()})
                    n2 = score_recent_backfill(conn, cfg, tm, limit=200)
                    if n or n2:
                        did = True
        except Exception:
            write_task_state(task_path, {"stage": "error", "at": utc_now_iso()})
            pass

        time.sleep(cfg.poll_seconds if not did else 0.2)


def load_config_from_env() -> DebitMLConfig:
    dsn = os.getenv("DEBIT_ML_DB_DSN", "").strip() or os.getenv("SPX_CHAIN_DATABASE_URL", "").strip()
    if not dsn:
        raise RuntimeError("DEBIT_ML_DB_DSN (or SPX_CHAIN_DATABASE_URL) is required")

    return DebitMLConfig(
        db_dsn=dsn,
        horizon_minutes=int(os.getenv("DEBIT_ML_HORIZON_MINUTES", "30")),
        bigwin_mult_atm=float(os.getenv("DEBIT_BIGWIN_MULT_ATM", "2.0")),
        bigwin_mult_wall=float(os.getenv("DEBIT_BIGWIN_MULT_WALL", "4.0")),
        min_train_rows=int(os.getenv("DEBIT_ML_MIN_TRAIN_ROWS", "300")),
        max_train_rows=int(os.getenv("DEBIT_ML_MAX_TRAIN_ROWS", "50000")),
        retrain_seconds=int(os.getenv("DEBIT_ML_RETRAIN_SECONDS", "900")),
        poll_seconds=float(os.getenv("DEBIT_ML_POLL_SECONDS", "20")),
        models_dir=os.getenv("DEBIT_ML_MODELS_DIR", "/mnt/options_ai/models/debit_spread"),
        model_version=os.getenv("DEBIT_ML_MODEL_VERSION", "debit_ridge_v2_flow"),
    )
