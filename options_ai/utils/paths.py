from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class RuntimePaths:
    data_root: Path
    ticker: str

    incoming_snapshots_dir: Path
    incoming_charts_dir: Path

    processed_snapshots_dir: Path
    processed_charts_dir: Path

    database_dir: Path
    logs_predictions_dir: Path
    logs_analyzer_reports_dir: Path
    logs_daemon_dir: Path

    state_dir: Path
    quarantine_invalid_filenames_dir: Path
    quarantine_invalid_json_dir: Path

    historical_dir: Path

    cache_dir: Path
    cache_derived_dir: Path
    cache_model_dir: Path


def build_paths(data_root: str, ticker: str) -> RuntimePaths:
    root = Path(data_root)

    incoming_snapshots_dir = root / "incoming" / ticker
    incoming_charts_dir = incoming_snapshots_dir / "charts"

    processed_snapshots_dir = root / "processed" / ticker / "snapshots"
    processed_charts_dir = root / "processed" / ticker / "charts"

    database_dir = root / "database"

    logs_predictions_dir = root / "logs" / "predictions"
    logs_analyzer_reports_dir = root / "logs" / "analyzer_reports"
    logs_daemon_dir = root / "logs" / "daemon"

    state_dir = root / "state"

    quarantine_invalid_filenames_dir = root / "quarantine" / "invalid_filenames"
    quarantine_invalid_json_dir = root / "quarantine" / "invalid_json"

    historical_dir = root / "historical" / ticker

    cache_dir = root / "cache"
    cache_derived_dir = cache_dir / "derived"
    cache_model_dir = cache_dir / "model"

    return RuntimePaths(
        data_root=root,
        ticker=ticker,
        incoming_snapshots_dir=incoming_snapshots_dir,
        incoming_charts_dir=incoming_charts_dir,
        processed_snapshots_dir=processed_snapshots_dir,
        processed_charts_dir=processed_charts_dir,
        database_dir=database_dir,
        logs_predictions_dir=logs_predictions_dir,
        logs_analyzer_reports_dir=logs_analyzer_reports_dir,
        logs_daemon_dir=logs_daemon_dir,
        state_dir=state_dir,
        quarantine_invalid_filenames_dir=quarantine_invalid_filenames_dir,
        quarantine_invalid_json_dir=quarantine_invalid_json_dir,
        historical_dir=historical_dir,
        cache_dir=cache_dir,
        cache_derived_dir=cache_derived_dir,
        cache_model_dir=cache_model_dir,
    )


def ensure_runtime_dirs(paths: RuntimePaths) -> None:
    dirs = [
        paths.incoming_snapshots_dir,
        paths.incoming_charts_dir,
        paths.processed_snapshots_dir,
        paths.processed_charts_dir,
        paths.database_dir,
        paths.logs_predictions_dir,
        paths.logs_analyzer_reports_dir,
        paths.logs_daemon_dir,
        paths.state_dir,
        paths.quarantine_invalid_filenames_dir,
        paths.quarantine_invalid_json_dir,
        paths.historical_dir,
        paths.cache_dir,
        paths.cache_derived_dir,
        paths.cache_model_dir,
    ]
    for d in dirs:
        d.mkdir(parents=True, exist_ok=True)
