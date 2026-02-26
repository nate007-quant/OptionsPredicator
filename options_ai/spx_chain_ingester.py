from __future__ import annotations

import json
import os
import re
import shutil
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable

try:
    from zoneinfo import ZoneInfo  # py3.9+
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore

try:
    import psycopg
except Exception:  # pragma: no cover
    psycopg = None  # type: ignore


# Example filename:
#   SPX-5940.17-2025-03-28-20250303-100058.json
# Notes:
# - Some sources may emit stray spaces; we strip them.
FILENAME_RE = re.compile(
    r"^(?P<underlying>[A-Z]+)-(?P<spot>\d+(?:\.\d+)?)-(?P<exp>\d{4}-\d{2}-\d{2})-(?P<obsDate>\d{8})-(?P<obsTime>\d{6})\.json$"
)


@dataclass(frozen=True)
class ParsedChainFilename:
    underlying: str
    underlying_price: float
    expiration_date: date
    snapshot_local: datetime
    snapshot_ts_utc: datetime


@dataclass(frozen=True)
class ChainIngestConfig:
    input_dir: Path
    archive_root: Path
    database_url: str
    filename_tz: str = "America/Chicago"

    poll_seconds: float = 0.5
    file_stable_seconds: float = 1.0

    # If we cannot rename/remove files in input_dir, keep a processed log.
    processed_log_path: Path | None = None


def parse_chain_filename(name: str, *, filename_tz: str) -> ParsedChainFilename:
    clean = name.strip().replace(" ", "")
    m = FILENAME_RE.match(clean)
    if not m:
        raise ValueError(f"invalid filename format: {name!r}")

    underlying = m.group("underlying").upper()
    spot = float(m.group("spot"))
    exp = date.fromisoformat(m.group("exp"))

    if ZoneInfo is None:
        raise RuntimeError("zoneinfo not available; install tzdata or use Python 3.9+")

    tz = ZoneInfo(filename_tz)
    snapshot_local = datetime.strptime(m.group("obsDate") + m.group("obsTime"), "%Y%m%d%H%M%S").replace(tzinfo=tz)
    snapshot_ts_utc = snapshot_local.astimezone(timezone.utc)

    return ParsedChainFilename(
        underlying=underlying,
        underlying_price=spot,
        expiration_date=exp,
        snapshot_local=snapshot_local,
        snapshot_ts_utc=snapshot_ts_utc,
    )


def _epoch_seconds_to_utc_ts(x: Any) -> datetime | None:
    if x is None:
        return None
    try:
        return datetime.fromtimestamp(int(x), tz=timezone.utc)
    except Exception:
        return None


def _get_status(snapshot: dict[str, Any]) -> str | None:
    # Accept common shapes:
    # - {"status": "ok"}
    # - {"s": "ok"}
    for k in ("status", "s"):
        v = snapshot.get(k)
        if isinstance(v, str):
            return v.strip().lower()
    return None


def validate_chain_json(snapshot: dict[str, Any]) -> int:
    st = _get_status(snapshot)
    if st is not None and st != "ok":
        raise ValueError(f"snapshot status not ok: {st!r}")

    if not isinstance(snapshot.get("optionSymbol"), list):
        raise ValueError("snapshot.optionSymbol must be an array")

    n = len(snapshot["optionSymbol"])
    if n <= 0:
        raise ValueError("snapshot has no rows (N=0)")

    # Any present array must align to N.
    for k, v in snapshot.items():
        if isinstance(v, list) and len(v) != n:
            raise ValueError(f"array field length mismatch: {k} len={len(v)} expected={n}")

    return n


def _ensure_dirs(cfg: ChainIngestConfig) -> None:
    (cfg.archive_root / "archive").mkdir(parents=True, exist_ok=True)
    (cfg.archive_root / "bad").mkdir(parents=True, exist_ok=True)
    (cfg.archive_root / "state").mkdir(parents=True, exist_ok=True)


SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS spx;

CREATE TABLE IF NOT EXISTS spx.option_chain (
  snapshot_ts TIMESTAMPTZ NOT NULL,
  option_symbol TEXT NOT NULL,
  underlying TEXT,
  expiration_date DATE,
  side TEXT,
  strike NUMERIC,
  first_traded_ts TIMESTAMPTZ,
  updated_ts TIMESTAMPTZ,
  dte INT,
  bid NUMERIC,
  bid_size INT,
  mid NUMERIC,
  ask NUMERIC,
  ask_size INT,
  last NUMERIC,
  open_interest INT,
  volume INT,
  in_the_money BOOLEAN,
  intrinsic_value NUMERIC,
  extrinsic_value NUMERIC,
  underlying_price NUMERIC,
  iv NUMERIC,
  delta NUMERIC,
  gamma NUMERIC,
  theta NUMERIC,
  vega NUMERIC,
  PRIMARY KEY (snapshot_ts, option_symbol)
);

-- Best-effort hypertable creation (requires timescaledb extension).
DO $$
BEGIN
  PERFORM create_hypertable('spx.option_chain','snapshot_ts', if_not_exists => TRUE);
EXCEPTION
  WHEN undefined_function THEN
    -- timescaledb extension not installed / no privileges
    NULL;
  WHEN insufficient_privilege THEN
    NULL;
END$$;

CREATE INDEX IF NOT EXISTS option_chain_symbol_ts_idx ON spx.option_chain (option_symbol, snapshot_ts DESC);
CREATE INDEX IF NOT EXISTS option_chain_exp_ts_idx ON spx.option_chain (expiration_date, snapshot_ts DESC);
"""


UPSERT_SQL = """
INSERT INTO spx.option_chain (
  snapshot_ts,
  option_symbol,
  underlying,
  expiration_date,
  side,
  strike,
  first_traded_ts,
  updated_ts,
  dte,
  bid,
  bid_size,
  mid,
  ask,
  ask_size,
  last,
  open_interest,
  volume,
  in_the_money,
  intrinsic_value,
  extrinsic_value,
  underlying_price,
  iv,
  delta,
  gamma,
  theta,
  vega
) VALUES (
  %(snapshot_ts)s,
  %(option_symbol)s,
  %(underlying)s,
  %(expiration_date)s,
  %(side)s,
  %(strike)s,
  %(first_traded_ts)s,
  %(updated_ts)s,
  %(dte)s,
  %(bid)s,
  %(bid_size)s,
  %(mid)s,
  %(ask)s,
  %(ask_size)s,
  %(last)s,
  %(open_interest)s,
  %(volume)s,
  %(in_the_money)s,
  %(intrinsic_value)s,
  %(extrinsic_value)s,
  %(underlying_price)s,
  %(iv)s,
  %(delta)s,
  %(gamma)s,
  %(theta)s,
  %(vega)s
)
ON CONFLICT (snapshot_ts, option_symbol) DO UPDATE SET
  underlying = EXCLUDED.underlying,
  expiration_date = EXCLUDED.expiration_date,
  side = EXCLUDED.side,
  strike = EXCLUDED.strike,
  first_traded_ts = EXCLUDED.first_traded_ts,
  updated_ts = EXCLUDED.updated_ts,
  dte = EXCLUDED.dte,
  bid = EXCLUDED.bid,
  bid_size = EXCLUDED.bid_size,
  mid = EXCLUDED.mid,
  ask = EXCLUDED.ask,
  ask_size = EXCLUDED.ask_size,
  last = EXCLUDED.last,
  open_interest = EXCLUDED.open_interest,
  volume = EXCLUDED.volume,
  in_the_money = EXCLUDED.in_the_money,
  intrinsic_value = EXCLUDED.intrinsic_value,
  extrinsic_value = EXCLUDED.extrinsic_value,
  underlying_price = EXCLUDED.underlying_price,
  iv = EXCLUDED.iv,
  delta = EXCLUDED.delta,
  gamma = EXCLUDED.gamma,
  theta = EXCLUDED.theta,
  vega = EXCLUDED.vega;
"""


def _as_num(x: Any) -> float | None:
    if x is None:
        return None
    try:
        return float(x)
    except Exception:
        return None


def _as_int(x: Any) -> int | None:
    if x is None:
        return None
    try:
        return int(x)
    except Exception:
        return None


def _as_bool(x: Any) -> bool | None:
    if x is None:
        return None
    if isinstance(x, bool):
        return x
    if isinstance(x, (int, float)):
        return bool(int(x))
    if isinstance(x, str):
        t = x.strip().lower()
        if t in {"true", "t", "1", "yes", "y"}:
            return True
        if t in {"false", "f", "0", "no", "n"}:
            return False
    return None


def _arr(snapshot: dict[str, Any], key: str, n: int) -> list[Any] | None:
    v = snapshot.get(key)
    if v is None:
        return None
    if not isinstance(v, list) or len(v) != n:
        # validate_chain_json already checked, but stay defensive.
        raise ValueError(f"field {key} must be array of length {n}")
    return v


def build_rows(
    snapshot: dict[str, Any],
    *,
    n: int,
    parsed: ParsedChainFilename,
) -> list[dict[str, Any]]:
    option_symbol = _arr(snapshot, "optionSymbol", n) or []

    # Common arrays (all optional except optionSymbol)
    underlying = _arr(snapshot, "underlying", n)
    side = _arr(snapshot, "side", n)
    strike = _arr(snapshot, "strike", n)

    first_traded = _arr(snapshot, "firstTraded", n)
    updated = _arr(snapshot, "updated", n)
    dte = _arr(snapshot, "dte", n)

    bid = _arr(snapshot, "bid", n)
    bid_size = _arr(snapshot, "bidSize", n)
    mid = _arr(snapshot, "mid", n)
    ask = _arr(snapshot, "ask", n)
    ask_size = _arr(snapshot, "askSize", n)
    last = _arr(snapshot, "last", n)

    open_interest = _arr(snapshot, "openInterest", n)
    volume = _arr(snapshot, "volume", n)

    in_the_money = _arr(snapshot, "inTheMoney", n)
    intrinsic_value = _arr(snapshot, "intrinsicValue", n)
    extrinsic_value = _arr(snapshot, "extrinsicValue", n)

    underlying_price = _arr(snapshot, "underlyingPrice", n)

    iv = _arr(snapshot, "iv", n)
    delta = _arr(snapshot, "delta", n)
    gamma = _arr(snapshot, "gamma", n)
    theta = _arr(snapshot, "theta", n)
    vega = _arr(snapshot, "vega", n)

    out: list[dict[str, Any]] = []
    for i in range(n):
        out.append(
            {
                "snapshot_ts": parsed.snapshot_ts_utc,
                "option_symbol": str(option_symbol[i]),
                "underlying": (str(underlying[i]) if underlying is not None and underlying[i] is not None else parsed.underlying),
                "expiration_date": parsed.expiration_date,
                "side": (str(side[i]) if side is not None and side[i] is not None else None),
                "strike": (_as_num(strike[i]) if strike is not None else None),
                "first_traded_ts": (_epoch_seconds_to_utc_ts(first_traded[i]) if first_traded is not None else None),
                "updated_ts": (_epoch_seconds_to_utc_ts(updated[i]) if updated is not None else None),
                "dte": (_as_int(dte[i]) if dte is not None else None),
                "bid": (_as_num(bid[i]) if bid is not None else None),
                "bid_size": (_as_int(bid_size[i]) if bid_size is not None else None),
                "mid": (_as_num(mid[i]) if mid is not None else None),
                "ask": (_as_num(ask[i]) if ask is not None else None),
                "ask_size": (_as_int(ask_size[i]) if ask_size is not None else None),
                "last": (_as_num(last[i]) if last is not None else None),
                "open_interest": (_as_int(open_interest[i]) if open_interest is not None else None),
                "volume": (_as_int(volume[i]) if volume is not None else None),
                "in_the_money": (_as_bool(in_the_money[i]) if in_the_money is not None else None),
                "intrinsic_value": (_as_num(intrinsic_value[i]) if intrinsic_value is not None else None),
                "extrinsic_value": (_as_num(extrinsic_value[i]) if extrinsic_value is not None else None),
                "underlying_price": (
                    _as_num(underlying_price[i])
                    if underlying_price is not None
                    else float(parsed.underlying_price)
                ),
                "iv": (_as_num(iv[i]) if iv is not None else None),
                "delta": (_as_num(delta[i]) if delta is not None else None),
                "gamma": (_as_num(gamma[i]) if gamma is not None else None),
                "theta": (_as_num(theta[i]) if theta is not None else None),
                "vega": (_as_num(vega[i]) if vega is not None else None),
            }
        )

    return out


def _load_processed_set(path: Path) -> set[str]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
        return {ln.strip() for ln in lines if ln.strip()}
    except FileNotFoundError:
        return set()
    except Exception:
        return set()


def _append_processed(path: Path, filename: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(filename)
        f.write("\n")


def _archive_dest(cfg: ChainIngestConfig, parsed: ParsedChainFilename, filename: str) -> Path:
    day = parsed.snapshot_ts_utc.strftime("%Y%m%d")
    return cfg.archive_root / "archive" / day / filename


def _bad_dest(cfg: ChainIngestConfig, filename: str) -> Path:
    return cfg.archive_root / "bad" / filename


def _write_error(cfg: ChainIngestConfig, filename: str, err: str) -> None:
    p = cfg.archive_root / "bad" / f"{filename}.error.txt"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(err.strip() + "\n", encoding="utf-8")


def _safe_move_or_copy(src: Path, dest: Path) -> bool:
    dest.parent.mkdir(parents=True, exist_ok=True)
    try:
        src.replace(dest)
        return True
    except Exception:
        # fallback to copy
        try:
            shutil.copy2(src, dest)
            return False
        except Exception:
            return False


def ensure_timescale_schema(database_url: str) -> None:
    if psycopg is None:
        raise RuntimeError("psycopg is required for Timescale/Postgres ingestion")

    with psycopg.connect(database_url, autocommit=True) as conn:
        # Best-effort extension creation: may require superuser.
        try:
            conn.execute("CREATE EXTENSION IF NOT EXISTS timescaledb;")
        except Exception:
            pass
        conn.execute(SCHEMA_SQL)


def ingest_one_file(cfg: ChainIngestConfig, path: Path) -> None:
    if psycopg is None:
        raise RuntimeError("psycopg is required for Timescale/Postgres ingestion")

    parsed = parse_chain_filename(path.name, filename_tz=cfg.filename_tz)

    raw = path.read_text(encoding="utf-8")
    snap = json.loads(raw)
    if not isinstance(snap, dict):
        raise ValueError("snapshot JSON root must be an object")

    n = validate_chain_json(snap)
    rows = build_rows(snap, n=n, parsed=parsed)

    with psycopg.connect(cfg.database_url) as conn:
        with conn.cursor() as cur:
            cur.execute("BEGIN")
            cur.executemany(UPSERT_SQL, rows)
            conn.commit()

    # Post-commit archive
    dest = _archive_dest(cfg, parsed, path.name)
    moved = _safe_move_or_copy(path, dest)

    # If we had to copy (no permission to move/delete), record it.
    if (not moved) and cfg.processed_log_path is not None:
        _append_processed(cfg.processed_log_path, path.name)


def quarantine_file(cfg: ChainIngestConfig, path: Path, err: str) -> None:
    _write_error(cfg, path.name, err)
    dest = _bad_dest(cfg, path.name)
    moved = _safe_move_or_copy(path, dest)
    if (not moved) and cfg.processed_log_path is not None:
        _append_processed(cfg.processed_log_path, path.name)


def _list_json_files(input_dir: Path) -> list[Path]:
    if not input_dir.exists():
        return []
    return sorted([p for p in input_dir.glob("*.json") if p.is_file()])


def run_chain_ingest_daemon(cfg: ChainIngestConfig) -> None:
    _ensure_dirs(cfg)
    ensure_timescale_schema(cfg.database_url)

    processed: set[str] = set()
    if cfg.processed_log_path is not None:
        processed = _load_processed_set(cfg.processed_log_path)

    seen_sizes: dict[str, tuple[int, float]] = {}

    while True:
        files = _list_json_files(cfg.input_dir)
        did_any = False

        for p in files:
            if p.name in processed:
                continue

            try:
                st = p.stat()
            except FileNotFoundError:
                continue

            prev = seen_sizes.get(p.name)
            now = time.time()
            if prev is None or prev[0] != st.st_size:
                seen_sizes[p.name] = (st.st_size, now)
                continue

            if now - prev[1] < cfg.file_stable_seconds:
                continue

            # stable; process
            try:
                ingest_one_file(cfg, p)
                did_any = True
                processed.add(p.name)
            except Exception as e:
                quarantine_file(cfg, p, err=str(e))
                did_any = True
                processed.add(p.name)

        time.sleep(cfg.poll_seconds if not did_any else 0.05)


def load_chain_ingest_config_from_env() -> ChainIngestConfig:
    input_dir = Path(os.getenv("INPUT_DIR", "/mnt/SPX")).expanduser()
    archive_root = Path(os.getenv("ARCHIVE_ROOT", "/mnt/options_ai")).expanduser()
    database_url = os.getenv("SPX_CHAIN_DATABASE_URL", os.getenv("DATABASE_URL", "")).strip()
    filename_tz = os.getenv("FILENAME_TZ", "America/Chicago").strip()

    if not database_url:
        raise RuntimeError("SPX_CHAIN_DATABASE_URL is required")

    processed_log_path = archive_root / "state" / "processed.log"

    return ChainIngestConfig(
        input_dir=input_dir,
        archive_root=archive_root,
        database_url=database_url,
        filename_tz=filename_tz,
        poll_seconds=float(os.getenv("CHAIN_WATCH_POLL_SECONDS", "0.5")),
        file_stable_seconds=float(os.getenv("CHAIN_FILE_STABLE_SECONDS", "1.0")),
        processed_log_path=processed_log_path,
    )
