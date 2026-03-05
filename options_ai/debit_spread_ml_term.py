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
from sklearn.linear_model import LogisticRegression, Ridge

from options_ai.term_expiration import term_bucket_name


ANCHOR_TYPES = ("ATM", "CALL_WALL", "PUT_WALL", "MAGNET")
SPREAD_TYPES = ("CALL", "PUT")


@dataclass(frozen=True)
class DebitMLTermConfig:
    db_dsn: str

    term_bucket: str
    target_dte_days: int
    dte_tolerance_days: int

    horizons_minutes: tuple[int, ...] = (5760, 10080)

    bigwin_mult_atm: float = 2.0
    bigwin_mult_wall: float = 4.0

    min_train_rows: int = 100
    max_train_rows: int = 50000
    retrain_seconds: int = 60 * 60

    poll_seconds: float = 30.0

    models_dir: str = "/mnt/options_ai/models/debit_spread_term"
    model_version: str = "debit_ridge_term_dte7t2_v1"


SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS spx;

CREATE TABLE IF NOT EXISTS spx.debit_spread_scores_term (
  snapshot_ts TIMESTAMPTZ NOT NULL,
  expiration_date DATE NOT NULL,
  horizon_minutes INT NOT NULL,

  term_bucket TEXT NOT NULL,

  anchor_type TEXT NOT NULL,
  spread_type TEXT NOT NULL,

  pred_change NUMERIC,
  p_bigwin NUMERIC,
  model_version TEXT NOT NULL,
  trained_at TIMESTAMPTZ,
  computed_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  PRIMARY KEY (snapshot_ts, expiration_date, horizon_minutes, anchor_type, spread_type)
);

CREATE INDEX IF NOT EXISTS debit_spread_scores_term_bucket_ts_idx ON spx.debit_spread_scores_term (term_bucket, snapshot_ts DESC);
"""


UPSERT_SCORE_SQL = """
INSERT INTO spx.debit_spread_scores_term (
  snapshot_ts, expiration_date, horizon_minutes,
  term_bucket,
  anchor_type, spread_type,
  pred_change, p_bigwin, model_version, trained_at
) VALUES (
  %(snapshot_ts)s, %(expiration_date)s, %(horizon_minutes)s,
  %(term_bucket)s,
  %(anchor_type)s, %(spread_type)s,
  %(pred_change)s, %(p_bigwin)s, %(model_version)s, %(trained_at)s
)
ON CONFLICT (snapshot_ts, expiration_date, horizon_minutes, anchor_type, spread_type) DO UPDATE SET
  term_bucket = EXCLUDED.term_bucket,
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
    a = (anchor_type or "").upper()
    if a == "ATM":
        return float(mult_atm)
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
]


def _row_to_features(r: dict[str, Any]) -> tuple[np.ndarray, list[str]]:
    nums = []
    names = []
    for k in _NUM_FEATURES:
        v = r.get(k)
        nums.append(0.0 if v is None else float(v))
        names.append(k)

    # categoricals
    anchor_oh = _onehot(str(r.get("anchor_type") or ""), ANCHOR_TYPES)
    spread_oh = _onehot(str(r.get("spread_type") or ""), SPREAD_TYPES)

    x = np.array(nums + anchor_oh + spread_oh, dtype=np.float64)
    names = names + [f"anchor_{c}" for c in ANCHOR_TYPES] + [f"spread_{c}" for c in SPREAD_TYPES]
    return x, names


def _compute_impute_values(X_raw: np.ndarray, mask: np.ndarray) -> np.ndarray:
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


def _fetch_training_rows(conn: psycopg.Connection, *, term_bucket: str, horizon_minutes: int, limit: int) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
              l.change, l.debit_t, l.debit_tH,
              c.anchor_type, c.spread_type,
              c.debit_points, c.anchor_strike, c.k_long, c.k_short,
              f.spot, f.atm_iv, f.skew_25d, f.bf_25d, f.pcr_volume, f.pcr_oi,
              f.contract_count, f.valid_iv_count, f.valid_mid_count,
              c.expiration_date
            FROM spx.debit_spread_labels_term l
            JOIN spx.debit_spread_candidates_term c
              ON c.snapshot_ts = l.snapshot_ts
             AND c.expiration_date = l.expiration_date
             AND c.anchor_type = l.anchor_type
             AND c.spread_type = l.spread_type
            JOIN spx.chain_features_term f
              ON f.snapshot_ts = c.snapshot_ts
             AND f.expiration_date = c.expiration_date
             AND f.term_bucket = %s
            WHERE l.term_bucket = %s
              AND l.horizon_minutes = %s
              AND l.is_missing_future = false
              AND l.change IS NOT NULL
              AND c.tradable = true
              AND f.low_quality = false
            ORDER BY l.snapshot_ts DESC
            LIMIT %s
            """,
            (term_bucket, term_bucket, int(horizon_minutes), int(limit)),
        )
        rows = []
        for r in cur.fetchall():
            # derived numeric
            k_long = _as_float(r[7])
            k_short = _as_float(r[8])
            width = abs(float(k_short) - float(k_long)) if k_long is not None and k_short is not None else None
            anchor_strike = _as_float(r[6])
            spot = _as_float(r[9])
            anchor_to_spot = (float(anchor_strike) - float(spot)) if anchor_strike is not None and spot is not None else None
            rows.append(
                {
                    "y": _as_float(r[0]),
                    "debit_t": _as_float(r[1]),
                    "debit_tH": _as_float(r[2]),
                    "anchor_type": r[3],
                    "spread_type": r[4],
                    "debit_points": _as_float(r[5]),
                    "anchor_strike": anchor_strike,
                    "k_long": k_long,
                    "k_short": k_short,
                    "width": width,
                    "spot": spot,
                    "anchor_to_spot": anchor_to_spot,
                    "abs_anchor_to_spot": abs(anchor_to_spot) if anchor_to_spot is not None else None,
                    "atm_iv": _as_float(r[10]),
                    "skew_25d": _as_float(r[11]),
                    "bf_25d": _as_float(r[12]),
                    "pcr_volume": _as_float(r[13]),
                    "pcr_oi": _as_float(r[14]),
                    "contract_count": _as_float(r[15]),
                    "valid_iv_count": _as_float(r[16]),
                    "valid_mid_count": _as_float(r[17]),
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


def _model_path(cfg: DebitMLTermConfig, *, horizon_minutes: int) -> Path:
    p = Path(cfg.models_dir) / cfg.term_bucket
    return p / f"{cfg.model_version}_h{int(horizon_minutes)}.joblib"


def train_if_needed(conn: psycopg.Connection, cfg: DebitMLTermConfig, *, horizon_minutes: int, force: bool = False) -> _TrainedModel | None:
    p = _model_path(cfg, horizon_minutes=horizon_minutes)
    p.parent.mkdir(parents=True, exist_ok=True)

    if not force and p.exists():
        try:
            obj = joblib.load(p)
            if isinstance(obj, dict) and obj.get("model_version") == cfg.model_version and int(obj.get("horizon_minutes")) == int(horizon_minutes):
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
                            horizon_minutes=int(horizon_minutes),
                            model_version=str(cfg.model_version),
                        )
        except Exception:
            pass

    rows = _fetch_training_rows(conn, term_bucket=cfg.term_bucket, horizon_minutes=int(horizon_minutes), limit=int(cfg.max_train_rows))
    rows = [r for r in rows if r.get("y") is not None]

    if len(rows) < int(cfg.min_train_rows):
        return None

    X_list = []
    mask_list = []
    y_list = []
    y_bigwin = []
    feat_names: list[str] | None = None

    for r in rows:
        nums = [r.get(k) for k in _NUM_FEATURES]
        num_mask = [v is None for v in nums]

        x, names = _row_to_features(r)
        if feat_names is None:
            feat_names = names

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
        "horizon_minutes": int(horizon_minutes),
        "model_version": str(cfg.model_version),
        "term_bucket": str(cfg.term_bucket),
    }
    joblib.dump(obj, p)

    return _TrainedModel(
        model=model,
        clf=clf,
        impute=impute,
        feature_names=feat_names or [],
        trained_at=trained_at,
        horizon_minutes=int(horizon_minutes),
        model_version=str(cfg.model_version),
    )


def _keys_missing_scores(conn: psycopg.Connection, *, term_bucket: str, horizon_minutes: int, limit: int) -> list[tuple[datetime, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT c.snapshot_ts, c.expiration_date
            FROM spx.debit_spread_candidates_term c
            JOIN spx.chain_features_term f
              ON f.snapshot_ts = c.snapshot_ts
             AND f.expiration_date = c.expiration_date
             AND f.term_bucket = %s
            WHERE c.term_bucket = %s
              AND c.tradable = true
              AND f.low_quality = false
              AND NOT EXISTS (
                SELECT 1
                FROM spx.debit_spread_scores_term s
                WHERE s.snapshot_ts = c.snapshot_ts
                  AND s.expiration_date = c.expiration_date
                  AND s.horizon_minutes = %s
              )
            ORDER BY c.snapshot_ts DESC
            LIMIT %s
            """,
            (term_bucket, term_bucket, int(horizon_minutes), int(limit)),
        )
        return [(r[0], r[1]) for r in cur.fetchall()]


def _score_one(conn: psycopg.Connection, cfg: DebitMLTermConfig, tm: _TrainedModel, *, snapshot_ts: datetime, expiration_date) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
              c.anchor_type, c.spread_type,
              c.debit_points, c.anchor_strike, c.k_long, c.k_short,
              f.spot, f.atm_iv, f.skew_25d, f.bf_25d, f.pcr_volume, f.pcr_oi,
              f.contract_count, f.valid_iv_count, f.valid_mid_count
            FROM spx.debit_spread_candidates_term c
            JOIN spx.chain_features_term f
              ON f.snapshot_ts = c.snapshot_ts
             AND f.expiration_date = c.expiration_date
             AND f.term_bucket = %s
            WHERE c.snapshot_ts = %s
              AND c.expiration_date = %s
              AND c.term_bucket = %s
              AND c.tradable = true
              AND f.low_quality = false
            """,
            (cfg.term_bucket, snapshot_ts, expiration_date, cfg.term_bucket),
        )
        rows = cur.fetchall()

    if not rows:
        return 0

    X_list = []
    mask_list = []
    keys = []

    for rr in rows:
        anchor_type = str(rr[0])
        spread_type = str(rr[1])
        debit_points = _as_float(rr[2])
        anchor_strike = _as_float(rr[3])
        k_long = _as_float(rr[4])
        k_short = _as_float(rr[5])
        width = abs(float(k_short) - float(k_long)) if k_long is not None and k_short is not None else None
        spot = _as_float(rr[6])
        anchor_to_spot = (float(anchor_strike) - float(spot)) if anchor_strike is not None and spot is not None else None

        d = {
            "anchor_type": anchor_type,
            "spread_type": spread_type,
            "debit_points": debit_points,
            "width": width,
            "anchor_to_spot": anchor_to_spot,
            "abs_anchor_to_spot": abs(anchor_to_spot) if anchor_to_spot is not None else None,
            "spot": spot,
            "atm_iv": _as_float(rr[7]),
            "skew_25d": _as_float(rr[8]),
            "bf_25d": _as_float(rr[9]),
            "pcr_volume": _as_float(rr[10]),
            "pcr_oi": _as_float(rr[11]),
            "contract_count": _as_float(rr[12]),
            "valid_iv_count": _as_float(rr[13]),
            "valid_mid_count": _as_float(rr[14]),
        }

        nums = [d.get(k) for k in _NUM_FEATURES]
        num_mask = [v is None for v in nums]

        x, _names = _row_to_features(d)
        m = np.zeros_like(x, dtype=bool)
        m[: len(_NUM_FEATURES)] = np.array(num_mask, dtype=bool)

        X_list.append(x)
        mask_list.append(m)
        keys.append((anchor_type, spread_type))

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
                    "snapshot_ts": snapshot_ts,
                    "expiration_date": expiration_date,
                    "horizon_minutes": int(tm.horizon_minutes),
                    "term_bucket": cfg.term_bucket,
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


def run_daemon(cfg: DebitMLTermConfig) -> None:
    with psycopg.connect(cfg.db_dsn) as conn:
        ensure_schema(conn)

    trained: dict[int, _TrainedModel] = {}

    while True:
        did = False
        try:
            with psycopg.connect(cfg.db_dsn) as conn:
                for h in cfg.horizons_minutes:
                    tm = train_if_needed(conn, cfg, horizon_minutes=int(h))
                    if tm is None:
                        continue
                    trained[int(h)] = tm

                    # backfill missing scores
                    for ts, exp in _keys_missing_scores(conn, term_bucket=cfg.term_bucket, horizon_minutes=int(h), limit=50):
                        n = _score_one(conn, cfg, tm, snapshot_ts=ts, expiration_date=exp)
                        if n:
                            did = True

        except Exception:
            pass

        time.sleep(cfg.poll_seconds if not did else 2.0)


def load_config_from_env() -> DebitMLTermConfig:
    db = os.getenv("DEBIT_ML_DB_DSN", "").strip() or os.getenv("SPX_CHAIN_DATABASE_URL", "").strip()
    if not db:
        raise RuntimeError("DEBIT_ML_DB_DSN (or SPX_CHAIN_DATABASE_URL) is required")

    target = int(os.getenv("TARGET_DTE_DAYS", "7"))
    tol = int(os.getenv("DTE_TOLERANCE_DAYS", "2"))
    bucket = os.getenv("TERM_BUCKET", "").strip() or term_bucket_name(target_dte_days=target, dte_tolerance_days=tol)

    horizons_s = os.getenv("DEBIT_ML_HORIZONS_MINUTES", "5760,10080").strip() or "5760,10080"
    horizons = tuple(int(x) for x in horizons_s.split(",") if x.strip())

    mv = os.getenv("DEBIT_ML_MODEL_VERSION", "").strip() or f"debit_ridge_{bucket}_v1"

    return DebitMLTermConfig(
        db_dsn=db,
        term_bucket=bucket,
        target_dte_days=target,
        dte_tolerance_days=tol,
        horizons_minutes=horizons,
        bigwin_mult_atm=float(os.getenv("DEBIT_ML_BIGWIN_MULT_ATM", "2.0")),
        bigwin_mult_wall=float(os.getenv("DEBIT_ML_BIGWIN_MULT_WALL", "4.0")),
        min_train_rows=int(os.getenv("DEBIT_ML_MIN_TRAIN_ROWS", "100")),
        max_train_rows=int(os.getenv("DEBIT_ML_MAX_TRAIN_ROWS", "50000")),
        retrain_seconds=int(os.getenv("DEBIT_ML_RETRAIN_SECONDS", "3600")),
        poll_seconds=float(os.getenv("DEBIT_ML_POLL_SECONDS", "60")),
        models_dir=os.getenv("DEBIT_ML_MODELS_DIR", "/mnt/options_ai/models/debit_spread_term"),
        model_version=mv,
    )
