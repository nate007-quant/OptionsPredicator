from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime, timezone
from typing import Any


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


ALLOWED_ANCHORS = ["ATM", "CALL_WALL", "PUT_WALL", "MAGNET"]
ALLOWED_SPREADS = ["CALL", "PUT"]
ALLOWED_POLICIES = ["any", "opposite_wall", "same_wall"]

FIELD_META: dict[str, dict[str, Any]] = {
    "MAX_DEBIT_POINTS": {"type": "number", "min": 0.1, "max": 25.0, "default": 5.0, "what": "Maximum debit paid for a spread entry (points).", "up": "Fewer trades; generally higher quality but may miss opportunities.", "down": "More trades; can include weaker setups."},
    "DEBIT_HORIZONS_MINUTES": {"type": "csv_int", "min": 1, "max": 10080, "default": "30", "what": "Label horizon(s) in minutes used for training outcomes.", "up": "Targets slower moves; less reactive.", "down": "Targets faster moves; more reactive/noisy."},
    "DEBIT_BIGWIN_MULT_ATM": {"type": "number", "min": 1.0, "max": 10.0, "default": 2.0, "what": "Big-win multiplier threshold for ATM anchors.", "up": "Harder positive class; fewer big-win labels.", "down": "Easier positive class; more big-win labels."},
    "DEBIT_BIGWIN_MULT_WALL": {"type": "number", "min": 1.0, "max": 10.0, "default": 2.5, "what": "Big-win multiplier threshold for wall/magnet anchors.", "up": "Harder positive class; fewer big-win labels.", "down": "Easier positive class; more big-win labels."},
    "DAILY_PICK_MIN_P_BIGWIN": {"type": "number", "min": 0.0, "max": 1.0, "default": 0.0, "what": "Minimum model p(bigwin) filter for pick candidate selection.", "up": "Stricter picks; lower trade count.", "down": "Looser picks; higher trade count."},
    "DAILY_PICK_MIN_PRED_CHANGE": {"type": "number", "min": -10.0, "max": 10.0, "default": 0.0, "what": "Minimum predicted change filter for pick candidate selection.", "up": "Requires stronger model conviction.", "down": "Allows weaker/negative predictions."},
    "DAILY_PICK_ALLOWED_ANCHORS": {"type": "csv_enum", "choices": ALLOWED_ANCHORS, "default": "ATM,CALL_WALL,PUT_WALL,MAGNET", "what": "Anchor types allowed for candidate selection.", "up": "(More restrictive set) narrows candidate universe.", "down": "(Broader set) increases candidate universe."},
    "DAILY_PICK_ALLOWED_SPREAD_TYPES": {"type": "csv_enum", "choices": ALLOWED_SPREADS, "default": "CALL,PUT", "what": "Spread directions allowed for candidate selection.", "up": "(More restrictive set) fewer directional choices.", "down": "(Broader set) more directional choices."},
    "DEBIT_ANCHOR_POLICY": {"type": "enum", "choices": ALLOWED_POLICIES, "default": "any", "what": "Anchor-policy filter mapping direction to allowed anchors.", "up": "N/A", "down": "N/A"},
    "DEBIT_ML_RETRAIN_SECONDS": {"type": "int", "min": 60, "max": 86400, "default": 900, "what": "Auto-retrain interval in seconds for ML service.", "up": "Less frequent retrains; more stale risk.", "down": "More frequent retrains; higher compute usage."},
}

PRESETS: dict[str, dict[str, Any]] = {
    "Conservative": {"MAX_DEBIT_POINTS": 4.0, "DEBIT_HORIZONS_MINUTES": "30", "DEBIT_BIGWIN_MULT_ATM": 2.2, "DEBIT_BIGWIN_MULT_WALL": 2.8, "DAILY_PICK_MIN_P_BIGWIN": 0.05, "DAILY_PICK_MIN_PRED_CHANGE": 0.02, "DAILY_PICK_ALLOWED_ANCHORS": "ATM,PUT_WALL,CALL_WALL", "DAILY_PICK_ALLOWED_SPREAD_TYPES": "CALL,PUT", "DEBIT_ANCHOR_POLICY": "opposite_wall", "DEBIT_ML_RETRAIN_SECONDS": 1800},
    "Balanced": {"MAX_DEBIT_POINTS": 5.0, "DEBIT_HORIZONS_MINUTES": "30", "DEBIT_BIGWIN_MULT_ATM": 2.0, "DEBIT_BIGWIN_MULT_WALL": 2.5, "DAILY_PICK_MIN_P_BIGWIN": 0.0, "DAILY_PICK_MIN_PRED_CHANGE": 0.0, "DAILY_PICK_ALLOWED_ANCHORS": "ATM,CALL_WALL,PUT_WALL,MAGNET", "DAILY_PICK_ALLOWED_SPREAD_TYPES": "CALL,PUT", "DEBIT_ANCHOR_POLICY": "any", "DEBIT_ML_RETRAIN_SECONDS": 900},
    "Aggressive": {"MAX_DEBIT_POINTS": 7.5, "DEBIT_HORIZONS_MINUTES": "15,30", "DEBIT_BIGWIN_MULT_ATM": 1.8, "DEBIT_BIGWIN_MULT_WALL": 2.0, "DAILY_PICK_MIN_P_BIGWIN": 0.0, "DAILY_PICK_MIN_PRED_CHANGE": -0.05, "DAILY_PICK_ALLOWED_ANCHORS": "ATM,CALL_WALL,PUT_WALL,MAGNET", "DAILY_PICK_ALLOWED_SPREAD_TYPES": "CALL,PUT", "DEBIT_ANCHOR_POLICY": "any", "DEBIT_ML_RETRAIN_SECONDS": 600},
}

FORBIDDEN_KEYS = {"TRADING_ENABLED", "LIVE_ARMED", "LIVE_EXECUTION_ENABLED", "BROKER_SESSION", "BROKER_AUTH", "ORDER_ACTION"}
FORBIDDEN_PREFIXES = ("TASTY_", "BROKER_", "DB_", "PGPASSWORD", "SPX_CHAIN_DATABASE_URL")

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS tuning_profiles (
  name TEXT PRIMARY KEY,
  config_json TEXT NOT NULL,
  built_in INTEGER NOT NULL DEFAULT 0,
  version INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS tuning_config_versions (
  version INTEGER PRIMARY KEY AUTOINCREMENT,
  config_json TEXT NOT NULL,
  actor TEXT,
  action TEXT,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS tuning_config_state (
  id INTEGER PRIMARY KEY CHECK (id=1),
  current_version INTEGER,
  current_config_json TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  updated_by TEXT
);
CREATE TABLE IF NOT EXISTS tuning_audit_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts TEXT NOT NULL,
  actor TEXT,
  action TEXT NOT NULL,
  old_values_json TEXT,
  new_values_json TEXT,
  result TEXT NOT NULL,
  error_detail TEXT,
  meta_json TEXT
);
CREATE TABLE IF NOT EXISTS tuning_job_state (
  id INTEGER PRIMARY KEY CHECK (id=1),
  job_id TEXT,
  action TEXT,
  status TEXT,
  progress TEXT,
  started_at TEXT,
  finished_at TEXT,
  duration_sec REAL,
  error_text TEXT
);
CREATE TABLE IF NOT EXISTS tuning_settings (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
"""


def _json(x: Any) -> str:
    return json.dumps(x, separators=(",", ":"), sort_keys=True)


def ensure_schema(con: sqlite3.Connection) -> None:
    con.executescript(SCHEMA_SQL)
    con.commit()


def audit(con: sqlite3.Connection, *, actor: str, action: str, old_values: Any, new_values: Any, result: str, error_detail: str | None = None, meta: Any = None) -> None:
    con.execute("INSERT INTO tuning_audit_log(ts, actor, action, old_values_json, new_values_json, result, error_detail, meta_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", (utc_now_iso(), actor, action, _json(old_values) if old_values is not None else None, _json(new_values) if new_values is not None else None, str(result), error_detail, _json(meta) if meta is not None else None))
    con.commit()


def field_specs() -> dict[str, Any]:
    return {"fields": FIELD_META, "presets": PRESETS}


def _env_defaults() -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k, m in FIELD_META.items():
        raw = os.getenv(k, "")
        out[k] = m["default"] if raw == "" else raw
    return normalize_config(out)[0]


def get_current_config(con: sqlite3.Connection) -> dict[str, Any]:
    row = con.execute("SELECT current_config_json FROM tuning_config_state WHERE id=1").fetchone()
    if row and row[0]:
        try:
            return normalize_config(json.loads(row[0]))[0]
        except Exception:
            pass
    cfg = _env_defaults()
    con.execute("INSERT OR REPLACE INTO tuning_config_state(id, current_version, current_config_json, updated_at, updated_by) VALUES (1, NULL, ?, ?, ?)", (_json(cfg), utc_now_iso(), "system"))
    con.commit()
    return cfg


def _parse_csv(s: str) -> list[str]:
    return [x.strip() for x in str(s).split(",") if x.strip()]


def normalize_config(payload: dict[str, Any]) -> tuple[dict[str, Any], dict[str, str]]:
    errors: dict[str, str] = {}
    norm: dict[str, Any] = {}
    extra = [k for k in payload.keys() if k not in FIELD_META]
    for k in extra:
        errors[k] = "Unknown key"

    for k, m in FIELD_META.items():
        v = payload.get(k, m["default"])
        typ = m["type"]
        try:
            if typ == "number":
                x = float(v)
                if x < float(m["min"]) or x > float(m["max"]):
                    raise ValueError(f"Must be between {m['min']} and {m['max']}")
                norm[k] = x
            elif typ == "int":
                x = int(v)
                if x < int(m["min"]) or x > int(m["max"]):
                    raise ValueError(f"Must be between {m['min']} and {m['max']}")
                norm[k] = x
            elif typ == "enum":
                s = str(v).strip().lower()
                choices = [str(x).lower() for x in m["choices"]]
                if s not in choices:
                    raise ValueError(f"Must be one of {m['choices']}")
                norm[k] = s
            elif typ == "csv_enum":
                vals = [x.upper() for x in _parse_csv(str(v))]
                allowed = set([str(x).upper() for x in m["choices"]])
                if not vals:
                    raise ValueError("At least one value is required")
                bad = [x for x in vals if x not in allowed]
                if bad:
                    raise ValueError(f"Invalid values: {bad}")
                norm[k] = ",".join(dict.fromkeys(vals))
            elif typ == "csv_int":
                parts = _parse_csv(str(v))
                if not parts:
                    raise ValueError("At least one horizon is required")
                vals: list[int] = []
                for p in parts:
                    i = int(p)
                    if i < int(m["min"]) or i > int(m["max"]):
                        raise ValueError(f"Each horizon must be between {m['min']} and {m['max']}")
                    vals.append(i)
                norm[k] = ",".join(str(x) for x in dict.fromkeys(vals))
            else:
                norm[k] = v
        except Exception as e:
            errors[k] = str(e)

    try:
        if float(norm.get("DEBIT_BIGWIN_MULT_WALL", 0)) < float(norm.get("DEBIT_BIGWIN_MULT_ATM", 0)):
            errors["DEBIT_BIGWIN_MULT_WALL"] = "Wall multiplier should be >= ATM multiplier"
    except Exception:
        pass
    return norm, errors


def reject_forbidden(payload: dict[str, Any]) -> list[str]:
    bad: list[str] = []
    for k in payload.keys():
        kk = str(k)
        if kk in FORBIDDEN_KEYS:
            bad.append(kk)
            continue
        for p in FORBIDDEN_PREFIXES:
            if kk.upper().startswith(p):
                bad.append(kk)
                break
    return bad


def set_current_config(con: sqlite3.Connection, cfg: dict[str, Any], *, actor: str, action: str) -> int:
    cur = get_current_config(con)
    ts = utc_now_iso()
    c = con.execute("INSERT INTO tuning_config_versions(config_json, actor, action, created_at) VALUES (?, ?, ?, ?)", (_json(cfg), actor, action, ts))
    ver = int(c.lastrowid)
    con.execute("INSERT OR REPLACE INTO tuning_config_state(id, current_version, current_config_json, updated_at, updated_by) VALUES (1, ?, ?, ?, ?)", (ver, _json(cfg), ts, actor))
    con.commit()
    audit(con, actor=actor, action=action, old_values=cur, new_values=cfg, result="success")
    return ver


def list_profiles(con: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = con.execute("SELECT name, config_json, built_in, version, created_at, updated_at FROM tuning_profiles ORDER BY built_in DESC, name ASC").fetchall()
    return [{"name": r[0], "config": json.loads(r[1]), "built_in": bool(r[2]), "version": int(r[3]), "created_at": r[4], "updated_at": r[5]} for r in rows]


def seed_builtin_profiles(con: sqlite3.Connection) -> None:
    ts = utc_now_iso()
    for name, cfg in PRESETS.items():
        con.execute("INSERT INTO tuning_profiles(name, config_json, built_in, version, created_at, updated_at) VALUES (?, ?, 1, 1, ?, ?) ON CONFLICT(name) DO NOTHING", (name, _json(cfg), ts, ts))
    con.commit()


def upsert_profile(con: sqlite3.Connection, *, name: str, config: dict[str, Any], actor: str) -> None:
    now = utc_now_iso()
    row = con.execute("SELECT config_json, version FROM tuning_profiles WHERE name=?", (name,)).fetchone()
    if row:
        old = json.loads(row[0])
        ver = int(row[1]) + 1
        con.execute("UPDATE tuning_profiles SET config_json=?, version=?, updated_at=?, built_in=0 WHERE name=?", (_json(config), ver, now, name))
        con.commit()
        audit(con, actor=actor, action="profile.update", old_values=old, new_values=config, result="success", meta={"name": name})
    else:
        con.execute("INSERT INTO tuning_profiles(name, config_json, built_in, version, created_at, updated_at) VALUES (?, ?, 0, 1, ?, ?)", (name, _json(config), now, now))
        con.commit()
        audit(con, actor=actor, action="profile.create", old_values=None, new_values=config, result="success", meta={"name": name})


def apply_profile(con: sqlite3.Connection, *, name: str, actor: str) -> int:
    row = con.execute("SELECT config_json FROM tuning_profiles WHERE name=?", (name,)).fetchone()
    if not row:
        raise ValueError("Profile not found")
    return set_current_config(con, json.loads(row[0]), actor=actor, action=f"profile.apply:{name}")


def rollback_to_version(con: sqlite3.Connection, *, version: int, actor: str) -> int:
    row = con.execute("SELECT config_json FROM tuning_config_versions WHERE version=?", (int(version),)).fetchone()
    if not row:
        raise ValueError("Version not found")
    return set_current_config(con, json.loads(row[0]), actor=actor, action=f"rollback:{version}")


def list_audit(con: sqlite3.Connection, *, limit: int = 200) -> list[dict[str, Any]]:
    rows = con.execute("SELECT id, ts, actor, action, old_values_json, new_values_json, result, error_detail, meta_json FROM tuning_audit_log ORDER BY id DESC LIMIT ?", (int(limit),)).fetchall()
    out: list[dict[str, Any]] = []
    for r in rows:
        out.append({"id": int(r[0]), "timestamp": r[1], "actor": r[2], "action": r[3], "old_values": json.loads(r[4]) if r[4] else None, "new_values": json.loads(r[5]) if r[5] else None, "result": r[6], "error_detail": r[7], "meta": json.loads(r[8]) if r[8] else None})
    return out


def set_job_state(con: sqlite3.Connection, **kwargs: Any) -> None:
    cur = con.execute("SELECT job_id, action, status, progress, started_at, finished_at, duration_sec, error_text FROM tuning_job_state WHERE id=1").fetchone()
    old = {"job_id": cur[0] if cur else None, "action": cur[1] if cur else None, "status": cur[2] if cur else None, "progress": cur[3] if cur else None, "started_at": cur[4] if cur else None, "finished_at": cur[5] if cur else None, "duration_sec": cur[6] if cur else None, "error_text": cur[7] if cur else None}
    old.update(kwargs)
    con.execute("INSERT OR REPLACE INTO tuning_job_state(id, job_id, action, status, progress, started_at, finished_at, duration_sec, error_text) VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?)", (old["job_id"], old["action"], old["status"], old["progress"], old["started_at"], old["finished_at"], old["duration_sec"], old["error_text"]))
    con.commit()


def get_job_state(con: sqlite3.Connection) -> dict[str, Any]:
    row = con.execute("SELECT job_id, action, status, progress, started_at, finished_at, duration_sec, error_text FROM tuning_job_state WHERE id=1").fetchone()
    if not row:
        return {"job_id": None, "action": None, "status": "idle", "progress": None, "started_at": None, "finished_at": None, "duration_sec": None, "error_text": None}
    return {"job_id": row[0], "action": row[1], "status": row[2] or "idle", "progress": row[3], "started_at": row[4], "finished_at": row[5], "duration_sec": row[6], "error_text": row[7]}


def get_auto_retrain_enabled(con: sqlite3.Connection) -> bool:
    row = con.execute("SELECT value FROM tuning_settings WHERE key='auto_retrain_enabled'").fetchone()
    return True if not row else (str(row[0]).strip().lower() in {"1", "true", "yes", "on"})


def set_auto_retrain_enabled(con: sqlite3.Connection, enabled: bool) -> None:
    con.execute("INSERT OR REPLACE INTO tuning_settings(key, value, updated_at) VALUES('auto_retrain_enabled', ?, ?)", ("1" if enabled else "0", utc_now_iso()))
    con.commit()


def config_to_env_overrides(cfg: dict[str, Any]) -> dict[str, str]:
    out = {k: str(v) for k, v in cfg.items() if k in FIELD_META}
    out.setdefault("DEBIT_ANCHOR_POLICY", str(cfg.get("DEBIT_ANCHOR_POLICY", "any")))
    return out
