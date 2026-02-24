from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import joblib


def _root(models_dir: str, model_version: str) -> Path:
    return Path(models_dir) / str(model_version)


@dataclass(frozen=True)
class EodModelBundle:
    meta: dict[str, Any]
    action_clf: Any
    dir_clf: Any | None
    move_reg: Any
    event_clfs: dict[str, Any]  # key: level_event_name


def save_bundle(models_dir: str, model_version: str, bundle: EodModelBundle) -> None:
    root = _root(models_dir, model_version)
    root.mkdir(parents=True, exist_ok=True)

    (root / "meta.json").write_text(json.dumps(bundle.meta, indent=2) + "\n", encoding="utf-8")
    joblib.dump(bundle.action_clf, root / "action_classifier.joblib")
    joblib.dump(bundle.move_reg, root / "move_regressor.joblib")
    joblib.dump(bundle.event_clfs, root / "event_classifiers.joblib")
    if bundle.dir_clf is not None:
        joblib.dump(bundle.dir_clf, root / "direction_classifier.joblib")


def load_bundle(models_dir: str, model_version: str) -> EodModelBundle:
    root = _root(models_dir, model_version)
    meta = json.loads((root / "meta.json").read_text(encoding="utf-8"))
    action_clf = joblib.load(root / "action_classifier.joblib")
    move_reg = joblib.load(root / "move_regressor.joblib")
    event_clfs = joblib.load(root / "event_classifiers.joblib")

    dir_path = root / "direction_classifier.joblib"
    dir_clf = joblib.load(dir_path) if dir_path.exists() else None

    return EodModelBundle(meta=meta, action_clf=action_clf, dir_clf=dir_clf, move_reg=move_reg, event_clfs=event_clfs)
