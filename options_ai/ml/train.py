from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import joblib
import numpy as np
from sklearn.ensemble import HistGradientBoostingClassifier, HistGradientBoostingRegressor

from options_ai.db import connect


def _load_rows(db_path: str) -> list[dict[str, Any]]:
    with connect(db_path) as conn:
        cur = conn.execute(
            "SELECT observed_ts_utc, features_json, features_version, spot_price, price_at_outcome, price_at_prediction, actual_move, predicted_direction, result, model_provider "
            "FROM predictions WHERE result IS NOT NULL AND features_json IS NOT NULL AND (model_provider IS NULL OR model_provider != 'ml') "
            "ORDER BY observed_ts_utc ASC"
        )
        return [dict(r) for r in cur.fetchall()]


def main() -> None:
    ap = argparse.ArgumentParser(description="Train conservative ML bundle (classifier + regressor)")
    ap.add_argument("--db", required=True)
    ap.add_argument("--out", required=True, help="Output directory for model bundle")
    ap.add_argument("--min-neutral-band-pts", type=float, default=3.0)
    ap.add_argument("--k-em", type=float, default=0.20)
    args = ap.parse_args()

    rows = _load_rows(args.db)
    if not rows:
        raise SystemExit("no training rows")

    # feature keys from first row
    f0 = json.loads(rows[0]["features_json"])
    feature_keys = sorted([k for k in f0.keys()])

    X = []
    y_action = []
    y_move = []

    for r in rows:
        feats = json.loads(r["features_json"])
        x = [float(feats.get(k, 0.0)) for k in feature_keys]
        X.append(x)

        # regression target: actual move in points
        move = r.get("actual_move")
        if move is None:
            # fallback
            try:
                move = float(r.get("price_at_outcome")) - float(r.get("price_at_prediction"))
            except Exception:
                move = 0.0
        y_move.append(float(move))

        # neutral band based on expected_move_abs feature
        em = float(feats.get("expected_move_abs", 0.0))
        neutral_band = max(float(args.min_neutral_band_pts), float(args.k_em) * em)
        y_action.append(1 if abs(float(move)) >= neutral_band else 0)

    X = np.asarray(X, dtype=float)
    y_action = np.asarray(y_action, dtype=int)
    y_move = np.asarray(y_move, dtype=float)

    # time split 80/20
    n = len(X)
    cut = int(n * 0.8)
    X_train, X_test = X[:cut], X[cut:]
    ya_train, ya_test = y_action[:cut], y_action[cut:]
    ym_train, ym_test = y_move[:cut], y_move[cut:]

    clf = HistGradientBoostingClassifier(max_depth=3)
    reg = HistGradientBoostingRegressor(max_depth=3)

    clf.fit(X_train, ya_train)
    reg.fit(X_train, ym_train)

    # simple metrics
    p = clf.predict_proba(X_test)[:, 1]
    pred_action = (p >= 0.5).astype(int)
    action_acc = float((pred_action == ya_test).mean()) if len(ya_test) else None

    pred_move = reg.predict(X_test)
    mae = float(np.mean(np.abs(pred_move - ym_test))) if len(ym_test) else None

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    joblib.dump(clf, out_dir / "classifier.joblib")
    joblib.dump(reg, out_dir / "regressor.joblib")

    meta = {
        "feature_keys": feature_keys,
        "min_neutral_band_pts": float(args.min_neutral_band_pts),
        "k_em": float(args.k_em),
        "metrics": {"action_acc": action_acc, "mae_pts": mae},
        "n_rows": int(n),
    }
    (out_dir / "meta.json").write_text(json.dumps(meta, indent=2) + "\n", encoding="utf-8")

    print(json.dumps(meta, indent=2))


if __name__ == "__main__":
    main()
