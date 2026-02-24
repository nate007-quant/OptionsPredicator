from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import joblib


@dataclass(frozen=True)
class ModelBundle:
    classifier: Any
    regressor: Any
    meta: dict[str, Any]


def load_bundle(models_dir: str, model_version: str) -> ModelBundle:
    root = Path(models_dir) / str(model_version)
    clf_path = root / "classifier.joblib"
    reg_path = root / "regressor.joblib"
    meta_path = root / "meta.json"

    if not clf_path.exists() or not reg_path.exists() or not meta_path.exists():
        raise FileNotFoundError(f"model bundle missing files under {root}")

    clf = joblib.load(clf_path)
    reg = joblib.load(reg_path)
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    return ModelBundle(classifier=clf, regressor=reg, meta=meta)
