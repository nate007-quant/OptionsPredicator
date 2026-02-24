from __future__ import annotations

import json
import traceback
import re
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from options_ai.ai.codex_client import CodexClient
from options_ai.ai.router import ModelRouter, try_models
from options_ai.config import Config
from options_ai.processes.analyzer import run_chart_extraction_if_available, run_prediction
from options_ai.queries import (
    fetch_latest_performance_summary,
    fetch_recent_predictions,
    hash_exists,
    insert_prediction,
)
from options_ai.utils.cache import (
    DerivedCache,
    load_derived_cache,
    load_model_cache,
    save_derived_cache,
    save_model_cache,
    sha256_file,
)
from options_ai.utils.logger import (
    get_logger,
    log_analyzer_report,
    log_daemon_event,
    log_model,
    log_prediction_event,
    log_routing,
)
from options_ai.utils.signals import OptionRow, compute_all_signals
from options_ai.utils.gex_subset import build_compact_gex


FILENAME_RE = re.compile(
    r"^(?P<ticker>[A-Z]+)-(?P<spot>\d+(?:\.\d+)?)-(?P<expY>\d{4})-(?P<expM>\d{2})-(?P<expD>\d{2})-(?P<obsDate>\d{8})-(?P<obsTime>\d{6})\.json$"
)


@dataclass(frozen=True)
class ParsedFilename:
    ticker: str
    spot_price: float
    expiration_date: str  # YYYY-MM-DD
    observed_dt_utc: datetime
    observed_date_compact: str  # YYYYMMDD
    observed_time_compact: str  # HHMMSS


@dataclass(frozen=True)
class IngestResult:
    processed: bool
    prediction_id: int | None = None
    skipped_reason: str | None = None


def parse_snapshot_filename(name: str) -> ParsedFilename:
    m = FILENAME_RE.match(name)
    if not m:
        raise ValueError("filename does not match required SPX snapshot pattern")

    ticker = m.group("ticker").upper()
    if ticker != "SPX":
        raise ValueError(f"ticker must be SPX (got {ticker!r})")

    spot_price = float(m.group("spot"))
    if spot_price <= 0:
        raise ValueError("spot_price must be > 0")

    expY, expM, expD = int(m.group("expY")), int(m.group("expM")), int(m.group("expD"))
    expiration = date(expY, expM, expD)
    expiration_date = expiration.isoformat()

    obsDate = m.group("obsDate")
    obsTime = m.group("obsTime")
    observed_dt_utc = datetime.strptime(obsDate + obsTime, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)

    if expiration < observed_dt_utc.date():
        raise ValueError("expiration_date must be >= observed_date")

    return ParsedFilename(
        ticker=ticker,
        spot_price=spot_price,
        expiration_date=expiration_date,
        observed_dt_utc=observed_dt_utc,
        observed_date_compact=obsDate,
        observed_time_compact=obsTime,
    )


def _normalize_underlying(u: str) -> str:
    x = (u or "").strip().upper()
    if x in {"SPX", "SPXW"}:
        return "SPX"
    return x


REQUIRED_ARRAY_FIELDS = [
    "optionSymbol",
    "underlying",
    "expiration",
    "side",
    "strike",
    "bid",
    "ask",
    "openInterest",
    "volume",
    "iv",
    "delta",
    "gamma",
]


def validate_snapshot_json(snapshot: dict[str, Any], parsed: ParsedFilename) -> None:
    if snapshot.get("s") != "ok":
        raise ValueError("snapshot.s must equal 'ok'")

    # observed_utc validation if present
    obs_from_json: datetime | None = None
    if isinstance(snapshot.get("observed_utc"), str):
        try:
            obs_from_json = datetime.fromisoformat(snapshot["observed_utc"].replace("Z", "+00:00")).astimezone(timezone.utc)
        except Exception:
            raise ValueError("snapshot.observed_utc is not parseable")
    elif isinstance(snapshot.get("observed_utc_epoch"), (int, float)):
        obs_from_json = datetime.fromtimestamp(int(snapshot["observed_utc_epoch"]), tz=timezone.utc)

    if obs_from_json is not None:
        delta = abs((obs_from_json - parsed.observed_dt_utc).total_seconds())
        if delta > 2.0:
            raise ValueError("observed_utc mismatch vs filename exceeds tolerance")

    # arrays exist + alignment
    n: int | None = None
    for k in REQUIRED_ARRAY_FIELDS:
        v = snapshot.get(k)
        if not isinstance(v, list):
            raise ValueError(f"snapshot.{k} must be an array")
        if n is None:
            n = len(v)
        if len(v) != n:
            raise ValueError("required arrays must have equal length")

    if not n or n <= 0:
        raise ValueError("snapshot arrays must have N > 0")

    # expiration identical
    exp_arr = snapshot["expiration"]
    exp0 = exp_arr[0]
    if not isinstance(exp0, int):
        raise ValueError("expiration values must be int epoch seconds")
    for e in exp_arr:
        if e != exp0:
            raise ValueError("expiration values must be identical across rows")

    # validate expiration date matches filename
    exp_dt = datetime.fromtimestamp(int(exp0), tz=timezone.utc)
    if exp_dt.date().isoformat() != parsed.expiration_date:
        raise ValueError("expiration epoch-derived date does not match filename expiration date")

    # underlying identical after normalization
    under_arr = snapshot["underlying"]
    u0 = _normalize_underlying(str(under_arr[0]))
    for u in under_arr:
        if _normalize_underlying(str(u)) != u0:
            raise ValueError("underlying values must be identical across rows")
    if u0 != "SPX":
        raise ValueError("underlying must resolve to SPX")

    # side values
    for s in snapshot["side"]:
        if s not in {"call", "put"}:
            raise ValueError("side must be 'call' or 'put'")


def normalize_rows(snapshot: dict[str, Any]) -> list[OptionRow]:
    n = len(snapshot["optionSymbol"])

    mids = snapshot.get("mid") if isinstance(snapshot.get("mid"), list) else None

    rows: list[OptionRow] = []
    for i in range(n):
        bid = snapshot["bid"][i]
        ask = snapshot["ask"][i]

        mid_val: float | None = None
        if mids is not None:
            try:
                mid_val = None if mids[i] is None else float(mids[i])
            except Exception:
                mid_val = None
        if mid_val is None and bid is not None and ask is not None:
            try:
                mid_val = (float(bid) + float(ask)) / 2.0
            except Exception:
                mid_val = None

        rows.append(
            OptionRow(
                optionSymbol=str(snapshot["optionSymbol"][i]),
                side=str(snapshot["side"][i]),
                strike=float(snapshot["strike"][i]),
                expiration_epoch=int(snapshot["expiration"][i]),
                bid=None if bid is None else float(bid),
                ask=None if ask is None else float(ask),
                mid=mid_val,
                openInterest=None if snapshot["openInterest"][i] is None else int(snapshot["openInterest"][i]),
                volume=None if snapshot["volume"][i] is None else int(snapshot["volume"][i]),
                iv=None if snapshot["iv"][i] is None else float(snapshot["iv"][i]),
                delta=None if snapshot["delta"][i] is None else float(snapshot["delta"][i]),
                gamma=None if snapshot["gamma"][i] is None else float(snapshot["gamma"][i]),
            )
        )

    return rows


def _row_to_dict(r: OptionRow) -> dict[str, Any]:
    return asdict(r)


def _dict_to_row(d: dict[str, Any]) -> OptionRow:
    return OptionRow(**d)


def build_snapshot_summary(parsed: ParsedFilename, rows: list[OptionRow], snapshot: dict[str, Any]) -> dict[str, Any]:
    strikes = sorted({r.strike for r in rows})
    return {
        "ticker": parsed.ticker,
        "observed_utc": parsed.observed_dt_utc.replace(microsecond=0).isoformat(),
        "expiration_date": parsed.expiration_date,
        "spot_price": parsed.spot_price,
        "contracts": {
            "n": len(rows),
            "strikes_min": float(strikes[0]) if strikes else None,
            "strikes_max": float(strikes[-1]) if strikes else None,
        },
        "has_ohlcv": bool(snapshot.get("ohlcv")),
    }


def _chart_path_if_exists(paths: Any, parsed: ParsedFilename) -> str | None:
    # spec example: SPX_chart_YYYYMMDD_HHMMSS.png
    name = f"{parsed.ticker}_chart_{parsed.observed_date_compact}_{parsed.observed_time_compact}.png"
    p = Path(paths.incoming_charts_dir) / name
    return str(p) if p.exists() else None


def ingest_snapshot_file(
    *,
    cfg: Config,
    paths: Any,
    db_path: str,
    snapshot_path: Path,
    snapshot_hash: str,
    router: ModelRouter | None,
    state: dict[str, Any],
    bootstrap_mode: bool = False,
    move_files: bool | None = None,
) -> IngestResult:
    """Ingest one snapshot JSON file, using caching and prompt-version idempotency (v1.6)."""

    # idempotency: (hash,prompt_version)
    if False:  # idempotency handled after model routing
        pass
        return IngestResult(processed=False, skipped_reason="duplicate_hash_prompt")

    # Default move behavior: move only in live incoming mode.
    if move_files is None:
        move_files = (not cfg.replay_mode) and (not bootstrap_mode)

    try:
        parsed = parse_snapshot_filename(snapshot_path.name)
    except Exception as e:
        log_daemon_event(paths.logs_daemon_dir, "error", "invalid_filename", file=str(snapshot_path), error=str(e))
        # quarantine only for live incoming
        if move_files:
            q = Path(paths.quarantine_invalid_filenames_dir) / snapshot_path.name
            q.parent.mkdir(parents=True, exist_ok=True)
            try:
                snapshot_path.replace(q)
            except Exception:
                pass
        return IngestResult(processed=False, skipped_reason="invalid_filename")

    # Load + validate JSON
    try:
        raw = snapshot_path.read_text(encoding="utf-8")
        snapshot = json.loads(raw)
        if not isinstance(snapshot, dict):
            raise ValueError("snapshot JSON root must be object")
        validate_snapshot_json(snapshot, parsed)
    except Exception as e:
        log_daemon_event(paths.logs_daemon_dir, "error", "invalid_json", file=str(snapshot_path), error=str(e))
        if move_files:
            q = Path(paths.quarantine_invalid_json_dir) / snapshot_path.name
            q.parent.mkdir(parents=True, exist_ok=True)
            try:
                snapshot_path.replace(q)
            except Exception:
                pass
        return IngestResult(processed=False, skipped_reason="invalid_json")

    # Stage caching controls
    mode = (cfg.reprocess_mode or "none").lower()
    if mode not in {"none", "from_model", "from_summary", "from_signals", "full"}:
        mode = "none"

    derived: DerivedCache | None = None
    if mode != "full":
        derived = load_derived_cache(paths, snapshot_hash)

    # S1 Normalize rows
    rows: list[OptionRow]
    if derived is not None and mode in {"none", "from_model", "from_summary", "from_signals"}:
        try:
            rows = [_dict_to_row(d) for d in derived.normalized_rows]
        except Exception:
            rows = normalize_rows(snapshot)
    else:
        rows = normalize_rows(snapshot)

    def _coerce_spot(val):
        # Accept common provider shapes:
        # - number/string
        # - list/array where the first element is the spot repeated per row
        if val is None:
            return None
        if isinstance(val, list):
            for x in val:
                if x is None:
                    continue
                try:
                    return float(x)
                except Exception:
                    continue
            return None
        try:
            return float(val)
        except Exception:
            return None

    spot = (
        _coerce_spot(snapshot.get("spot_price"))
        or _coerce_spot(snapshot.get("underlyingPrice"))
        or float(parsed.spot_price)
    )
    price_series_raw = snapshot.get("ohlcv")

    # S2+S3 Signals (includes GEX)
    if derived is not None and mode in {"none", "from_model", "from_summary"}:
        signals = dict(derived.signals)
    else:
        signals = compute_all_signals(rows, spot, price_series_raw)

    # --- Reduce LLM tokens: compact GEX + keep sticky day levels (v2.3+)
    day_key = parsed.observed_dt_utc.date().isoformat()
    state.setdefault("day_levels", {})
    state["day_levels"].setdefault(day_key, {"sticky_strikes": []})
    sticky_list = list(state["day_levels"][day_key].get("sticky_strikes") or [])

    # Extract full GEX maps + level strikes
    net_map = dict(signals.get("gex_net_by_strike") or {})
    abs_map = dict(signals.get("gex_abs_by_strike") or {})

    call_wall = signals.get("gex_call_wall_strike")
    put_wall = signals.get("gex_put_wall_strike")
    magnet = signals.get("gex_magnet_strike")
    flip = signals.get("gex_flip_strike")

    # Update sticky strikes for the day: include any level strike seen earlier today (sticky-day)
    for lv in [call_wall, put_wall, magnet, flip]:
        if lv is None:
            continue
        try:
            sticky_list.append(float(lv))
        except Exception:
            pass
    # de-dup (preserve recency) + cap (keep most recent)
    seen = set()
    sticky_unique_rev = []
    for s in reversed(sticky_list):
        try:
            sf = float(s)
        except Exception:
            continue
        if sf in seen:
            continue
        seen.add(sf)
        sticky_unique_rev.append(sf)
        if int(cfg.gex_sticky_day_max or 0) and len(sticky_unique_rev) >= int(cfg.gex_sticky_day_max or 0):
            break
    sticky_unique = list(reversed(sticky_unique_rev))

    state["day_levels"][day_key]["sticky_strikes"] = sticky_unique

    # Build compact model-facing signals
    compact_gex = build_compact_gex(
        spot=float(spot),
        net_map=net_map,
        abs_map=abs_map,
        call_wall=float(call_wall) if call_wall is not None else None,
        put_wall=float(put_wall) if put_wall is not None else None,
        magnet=float(magnet) if magnet is not None else None,
        flip=float(flip) if flip is not None else None,
        dist_call_wall=signals.get("gex_distance_to_call_wall"),
        dist_put_wall=signals.get("gex_distance_to_put_wall"),
        dist_magnet=signals.get("gex_distance_to_magnet"),
        dist_flip=signals.get("gex_distance_to_flip"),
        regime_label=signals.get("gex_regime_label"),
        neighbors_each_level=int(cfg.gex_neighbor_strikes or 0),
        topk_abs=int(cfg.gex_topk_abs_strikes or 0),
        sticky_strikes=sticky_unique,
        sticky_day_max=int(cfg.gex_sticky_day_max or 0),
    )

    # Strip large per-strike maps + move GEX into structured compact block
    model_signals = {k: v for k, v in signals.items() if k not in {
        "gex_net_by_strike",
        "gex_abs_by_strike",
        "gex_call_wall_strike",
        "gex_put_wall_strike",
        "gex_magnet_strike",
        "gex_flip_strike",
        "gex_regime_label",
        "gex_distance_to_call_wall",
        "gex_distance_to_put_wall",
        "gex_distance_to_magnet",
        "gex_distance_to_flip",
    }}
    model_signals["gex"] = {
        "levels": compact_gex.levels,
        "regime_label": compact_gex.regime_label,
        "subset": compact_gex.subset,
    }

    # S4 Snapshot summary
    if derived is not None and mode in {"none", "from_model"}:
        snapshot_summary = dict(derived.snapshot_summary)
    else:
        snapshot_summary = build_snapshot_summary(parsed, rows, snapshot)

    # Write derived cache if missing or forced recompute occurred
    if mode != "from_model":
        try:
            save_derived_cache(
                paths,
                snapshot_hash,
                DerivedCache(
                    normalized_rows=[_row_to_dict(r) for r in rows],
                    signals=model_signals,
                    snapshot_summary=snapshot_summary,
                ),
            )
        except Exception:
            pass

    chart_path = _chart_path_if_exists(paths, parsed)
    chart_hash: str | None = None
    if chart_path and Path(chart_path).exists():
        try:
            chart_hash = sha256_file(Path(chart_path))
        except Exception:
            chart_hash = None

    # Phase 1: chart extraction (optional). Disabled by default during bootstrap.
    chart_description: str | None = None
    chart_report: dict[str, Any] | None = None

    candidates = router.candidates(bootstrap_mode=bootstrap_mode) if router is not None else []
    try:
        log_routing(
            paths,
            level="INFO",
            event="routing_candidates",
            message="computed routing candidates",
            snapshot_hash=snapshot_hash,
            candidates=[{"provider": c.provider, "model_used": c.model_used, "reason": c.routing_reason} for c in candidates],
            bootstrap_mode=bool(bootstrap_mode),
        )
    except Exception:
        pass


    # Chart extraction routing + caching (chart is optional; failures should not stop prediction).
    if not bootstrap_mode and chart_path and candidates:
        for c in candidates:
            cached = load_model_cache(
                paths,
                snapshot_hash,
                chart_hash=chart_hash,
                prompt_version=cfg.prompt_version,
                model_id=c.model_used,
                kind="chart",
            )
            if cached and isinstance(cached.get("chart_description"), str):
                chart_description = str(cached.get("chart_description"))
                chart_report = dict(cached.get("report") or {})
                chart_report.update({
                    "cache_hit": True,
                    "model_used": c.model_used,
                    "model_provider": c.provider,
                    "routing_reason": c.routing_reason,
                })
                break

            try:
                desc, rep, model_used, provider, reason = try_models(
                    [c],
                    fn_name="chart_extraction",
                    fn=lambda client: run_chart_extraction_if_available(codex=client, chart_png_path=chart_path),
                    local_max_retries=int(cfg.local_model_max_retries or 0),
                )
                chart_description = desc
                chart_report = rep
                try:
                    save_model_cache(
                        paths,
                        snapshot_hash,
                        chart_hash=chart_hash,
                        prompt_version=cfg.prompt_version,
                        model_id=model_used,
                        kind="chart",
                        payload={"chart_description": chart_description, "report": chart_report},
                    )
                except Exception:
                    pass
                break
            except Exception as e:
                lg = get_logger()
                if lg:
                    lg.exception(
                        level="ERROR",
                        component="Model",
                        event="chart_extraction_failed",
                        message="chart extraction failed",
                        file_key="model",
                        snapshot_hash=snapshot_hash,
                        model_used=c.model_used,
                        exc=e,
                        model_provider=c.provider,
                        routing_reason=c.routing_reason,
                        error=str(e),
                    )
                else:
                    log_model(
                        paths,
                        level="ERROR",
                        event="chart_extraction_failed",
                        message="chart extraction failed",
                        snapshot_hash=snapshot_hash,
                        model_used=c.model_used,
                        model_provider=c.provider,
                        routing_reason=c.routing_reason,
                        error=str(e),
                    )
                continue

        if chart_report is not None:
            log_analyzer_report(
                Path(paths.logs_analyzer_reports_dir),
                observed_ts_compact=parsed.observed_date_compact + parsed.observed_time_compact,
                report={"phase": "chart_extraction", **chart_report},
            )

    # Context for prediction
    recent_predictions = fetch_recent_predictions(db_path, limit=cfg.history_records)
    perf_summary = fetch_latest_performance_summary(db_path)

    # Phase 2: prediction (cached + routed)
    pred_obj: dict[str, Any] | None = None
    pred_report: dict[str, Any] | None = None
    model_used = "none"
    model_provider = "none"
    routing_reason = "no_model_available"

    # Filter out candidates already inserted (idempotency per model)
    candidates_to_try = []
    for c in candidates:
        try:
            if hash_exists(db_path, snapshot_hash, cfg.prompt_version, c.model_used):
                continue
        except Exception:
            pass
        candidates_to_try.append(c)

    # If all routed candidates already exist in DB, skip processing (idempotent).
    if candidates and not candidates_to_try:
        return IngestResult(processed=False, skipped_reason="duplicate_hash_prompt_model")

    # If there are no candidates (no remote/local available), ensure idempotency for the placeholder model.
    if not candidates and hash_exists(db_path, snapshot_hash, cfg.prompt_version, "none"):
        return IngestResult(processed=False, skipped_reason="duplicate_hash_prompt_model")

    for c in candidates_to_try:
        cached = load_model_cache(
            paths,
            snapshot_hash,
            chart_hash=chart_hash,
            prompt_version=cfg.prompt_version,
            model_id=c.model_used,
            kind="prediction",
        )
        if cached and isinstance(cached.get("validated"), dict):
            pred_obj = dict(cached["validated"])
            pred_report = dict(cached.get("report") or {})
            pred_report.update({
                "cache_hit": True,
                "model_used": c.model_used,
                "model_provider": c.provider,
                "routing_reason": c.routing_reason,
            })
            model_used, model_provider, routing_reason = c.model_used, c.provider, c.routing_reason
            try:
                log_routing(paths, level="INFO", event="model_selected", message="selected model from cache", snapshot_hash=snapshot_hash, model_used=model_used, model_provider=model_provider, routing_reason=routing_reason)
            except Exception:
                pass
            break

        try:
            val, rep, mu, mp, rr = try_models(
                [c],
                fn_name="prediction",
                fn=lambda client: run_prediction(
                    codex=client,
                    snapshot_summary=snapshot_summary,
                    signals=model_signals,
                    chart_description=chart_description,
                    recent_predictions=recent_predictions,
                    performance_summary=perf_summary,
                    min_confidence=cfg.min_confidence,
                ),
                local_max_retries=int(cfg.local_model_max_retries or 0),
            )
            pred_obj = val
            pred_report = rep
            model_used, model_provider, routing_reason = mu, mp, rr
            try:
                log_routing(paths, level="INFO", event="model_selected", message="selected model", snapshot_hash=snapshot_hash, model_used=model_used, model_provider=model_provider, routing_reason=routing_reason)
            except Exception:
                pass
            try:
                save_model_cache(
                    paths,
                    snapshot_hash,
                    chart_hash=chart_hash,
                    prompt_version=cfg.prompt_version,
                    model_id=model_used,
                    kind="prediction",
                    payload={"validated": pred_obj, "report": pred_report},
                )
            except Exception:
                pass
            break
        except Exception as e:
            lg = get_logger()
            if lg:
                lg.exception(
                    level="ERROR",
                    component="Model",
                    event="prediction_failed",
                    message="prediction failed",
                    file_key="model",
                    snapshot_hash=snapshot_hash,
                    model_used=c.model_used,
                    exc=e,
                    model_provider=c.provider,
                    routing_reason=c.routing_reason,
                    error=str(e),
                )
            else:
                log_model(
                    paths,
                    level="ERROR",
                    event="prediction_failed",
                    message="prediction failed",
                    snapshot_hash=snapshot_hash,
                    model_used=c.model_used,
                    model_provider=c.provider,
                    routing_reason=c.routing_reason,
                    error=str(e),
                )
            continue

    if pred_obj is None:
        pred_obj = {
            "predicted_direction": "neutral",
            "predicted_magnitude": 0.0,
            "confidence": 0.0,
            "strategy_suggested": "",
            "signals_used": ["no_model_available"],
            "reasoning": "No model provider available; prediction skipped.",
        }
        pred_report = {"cache_hit": False, "model_used": model_used, "model_provider": model_provider, "routing_reason": routing_reason}
    # Local self-calibration enforcement (hard rule, deterministic).
    try:
        if perf_summary and perf_summary.get("overall_accuracy") is not None and (perf_summary.get("total_scored") or 0) >= 5:
            if float(perf_summary["overall_accuracy"]) < 0.45:
                pred_obj = {
                    **pred_obj,
                    "predicted_direction": "neutral",
                    "predicted_magnitude": 0.0,
                    "strategy_suggested": "",
                    "reasoning": (
                        pred_obj.get("reasoning", "")
                        + " Self-calibration: recent accuracy below threshold; output forced neutral."
                    ).strip(),
                }
                pred_report["postprocess"] = {
                    "forced_neutral": True,
                    "reason": "overall_accuracy_below_0.45_with_sample>=5",
                }
    except Exception:
        pass

    log_analyzer_report(
        Path(paths.logs_analyzer_reports_dir),
        observed_ts_compact=parsed.observed_date_compact + parsed.observed_time_compact,
        report={"phase": "prediction", **(pred_report or {})},
    )

    signals_payload = {
        "computed": model_signals,
        "model_signals_used": pred_obj.get("signals_used"),
    }

    row = {
        "timestamp": parsed.observed_dt_utc.replace(microsecond=0).isoformat(),
        "ticker": parsed.ticker,
        "expiration_date": parsed.expiration_date,
        "source_snapshot_file": snapshot_path.name,
        "source_snapshot_hash": snapshot_hash,
        "chart_file": Path(chart_path).name if chart_path else None,
        "spot_price": float(spot),
        "signals_used": json.dumps(signals_payload, sort_keys=True),
        "chart_description": chart_description,
        "predicted_direction": pred_obj["predicted_direction"],
        "predicted_magnitude": float(pred_obj["predicted_magnitude"]),
        "confidence": float(pred_obj["confidence"]),
        "strategy_suggested": pred_obj.get("strategy_suggested", "") or "",
        "reasoning": pred_obj.get("reasoning", ""),
        "prompt_version": cfg.prompt_version,
        "model_used": model_used,
        "model_provider": model_provider,
        "routing_reason": routing_reason,
        "price_at_prediction": float(spot),
    }

    pred_id = insert_prediction(db_path, row)

    if pred_id is None:
        # Duplicate (snapshot_hash, prompt_version, model_used)
        try:
            log_routing(
                paths,
                level="INFO",
                event="duplicate_prediction",
                message="prediction already exists; skipping",
                snapshot_hash=snapshot_hash,
                model_used=model_used,
                model_provider=model_provider,
                routing_reason=routing_reason,
            )
        except Exception:
            pass
        return IngestResult(processed=False, skipped_reason="duplicate_hash_prompt_model")


    # Update state snapshot index (for scoring)
    state.setdefault("snapshot_index", {})
    obs_iso = parsed.observed_dt_utc.replace(microsecond=0).isoformat()
    state["snapshot_index"][obs_iso] = {
        "spot": float(spot),
        "file": snapshot_path.name,
    }

    # Append daily prediction log
    log_prediction_event(
        Path(paths.logs_predictions_dir),
        observed_date_yyyy_mm_dd=parsed.observed_dt_utc.date().isoformat(),
        event={
            "prediction_id": pred_id,
            "timestamp": row["timestamp"],
            "ticker": row["ticker"],
            "expiration_date": row["expiration_date"],
            "spot_price": row["spot_price"],
            "predicted_direction": row["predicted_direction"],
            "predicted_magnitude": row["predicted_magnitude"],
            "confidence": row["confidence"],
            "strategy_suggested": row["strategy_suggested"],
        },
    )

    # Move processed files only for live (non-replay) incoming directory
    if move_files:
        try:
            dest = Path(paths.processed_snapshots_dir) / snapshot_path.name
            dest.parent.mkdir(parents=True, exist_ok=True)
            snapshot_path.replace(dest)
        except Exception as e:
            log_daemon_event(paths.logs_daemon_dir, "error", "move_processed_snapshot_failed", file=str(snapshot_path), error=str(e))

        if chart_path:
            try:
                chart_src = Path(chart_path)
                chart_dest = Path(paths.processed_charts_dir) / chart_src.name
                chart_dest.parent.mkdir(parents=True, exist_ok=True)
                chart_src.replace(chart_dest)
            except Exception as e:
                log_daemon_event(paths.logs_daemon_dir, "error", "move_processed_chart_failed", file=str(chart_path), error=str(e))

    return IngestResult(processed=True, prediction_id=pred_id)
