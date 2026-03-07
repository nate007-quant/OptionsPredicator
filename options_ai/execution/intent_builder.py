from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _canonical_json(obj: Any) -> str:
    return json.dumps(obj, separators=(",", ":"), sort_keys=True)


def _parse_json_maybe(s: Any, default: Any) -> Any:
    if s is None:
        return default
    try:
        return json.loads(str(s))
    except Exception:
        return default


@dataclass
class IntentBuildStats:
    scanned: int = 0
    qualified: int = 0
    inserted: int = 0
    existing: int = 0
    skipped: int = 0


class ExecutionIntentBuilder:
    """Build pending execution intents from qualified backtest runs.

    Phase-2 bridge implementation:
      - source rows: backtest_runs
      - qualifier: trades > 0
      - idempotency: deterministic idempotency_key per source run + canonical params
      - upsert intent in `pending` status
    """

    def __init__(self, *, db_path: str, connect_fn: Any | None = None, environment: str = "sandbox", broker_name: str = "tastytrade") -> None:
        self.db_path = str(db_path)
        self._connect_fn = connect_fn
        self.environment = str(environment or "sandbox")
        self.broker_name = str(broker_name or "tastytrade")

    def _connect(self):
        if self._connect_fn is not None:
            return self._connect_fn(self.db_path)
        con = sqlite3.connect(self.db_path, timeout=5.0)
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA journal_mode=WAL;")
        con.execute("PRAGMA busy_timeout=5000;")
        return con

    @staticmethod
    def _entry_window_ct(params: dict[str, Any]) -> tuple[str | None, str | None]:
        mode = str((params or {}).get("entry_mode") or "time_range").strip().lower()
        if mode == "time_range":
            return (
                (str(params.get("entry_start_ct")) if params.get("entry_start_ct") else None),
                (str(params.get("entry_end_ct")) if params.get("entry_end_ct") else None),
            )

        # fallback for first_n_minutes mode
        start = str((params or {}).get("session_start_ct") or "08:30")
        mins = int((params or {}).get("entry_first_n_minutes") or 60)
        try:
            hh, mm = start.split(":")
            total = int(hh) * 60 + int(mm) + mins
            end = f"{total // 60:02d}:{total % 60:02d}"
        except Exception:
            end = None
        return (start, end)

    @staticmethod
    def _tp_sl_from_params(params: dict[str, Any]) -> dict[str, Any]:
        p = params or {}
        tp = p.get("take_profit_pct")
        if tp is None:
            tp = p.get("credit_take_profit_pct")

        sl = p.get("stop_loss_pct")
        if sl is None:
            sl = p.get("credit_stop_loss_mult")

        out: dict[str, Any] = {
            "take_profit_pct": (float(tp) if tp is not None else None),
            "stop_loss": (float(sl) if sl is not None else None),
            "stop_loss_kind": ("pct" if p.get("stop_loss_pct") is not None else ("mult" if p.get("credit_stop_loss_mult") is not None else None)),
        }
        return out

    @staticmethod
    def _idempotency_key(*, source_type: str, source_id: int, strategy_key: str, params: dict[str, Any]) -> str:
        canon = _canonical_json(params)
        raw = f"{source_type}|{int(source_id)}|{strategy_key}|{canon}".encode("utf-8")
        return hashlib.sha256(raw).hexdigest()

    def _intent_payload_from_run(self, *, run_row: sqlite3.Row, params: dict[str, Any], summary: dict[str, Any]) -> dict[str, Any]:
        first_ct, last_ct = self._entry_window_ct(params)
        payload = {
            "source": {
                "type": "backtest_run",
                "id": int(run_row["id"]),
                "created_at_utc": str(run_row["created_at_utc"]),
            },
            "strategy_key": str(run_row["strategy_key"]),
            "params": params,
            "summary": summary,
            "entry_window_ct": {
                "first_trade_time_ct": first_ct,
                "last_new_entry_time_ct": last_ct,
            },
            "risk": self._tp_sl_from_params(params),
        }
        return payload

    def build_once(self, *, limit: int = 100) -> dict[str, Any]:
        stats = IntentBuildStats()
        now = _now_utc_iso()

        with self._connect() as con:
            rows = con.execute(
                """
                SELECT id, strategy_key, created_at_utc, params_json, summary_json
                FROM backtest_runs
                ORDER BY id DESC
                LIMIT ?
                """,
                (int(limit),),
            ).fetchall()

            for r in rows:
                stats.scanned += 1
                params = _parse_json_maybe(r["params_json"], {})
                summary = _parse_json_maybe(r["summary_json"], {})

                trades = int((summary or {}).get("trades") or 0)
                if trades <= 0:
                    stats.skipped += 1
                    continue

                stats.qualified += 1

                intent_payload = self._intent_payload_from_run(run_row=r, params=params, summary=summary)
                idem = self._idempotency_key(
                    source_type="backtest_run",
                    source_id=int(r["id"]),
                    strategy_key=str(r["strategy_key"]),
                    params=params,
                )

                ex0 = con.execute(
                    "SELECT id FROM execution_intents WHERE idempotency_key=?",
                    (idem,),
                ).fetchone()
                if ex0 is not None:
                    con.execute(
                        "UPDATE execution_intents SET updated_at_utc=? WHERE idempotency_key=?",
                        (now, idem),
                    )
                    stats.existing += 1
                else:
                    con.execute(
                        """
                        INSERT INTO execution_intents(
                          created_at_utc, updated_at_utc,
                          environment, broker_name,
                          status, strategy_key, symbol,
                          candidate_ref, idempotency_key, intent_payload_json, error
                        )
                        VALUES(?,?,?,?,?,?,?,?,?,?,NULL)
                        """,
                        (
                            now,
                            now,
                            self.environment,
                            self.broker_name,
                            "pending",
                            str(r["strategy_key"]),
                            "SPX",
                            f"backtest_run:{int(r['id'])}",
                            idem,
                            _canonical_json(intent_payload),
                        ),
                    )
                    stats.inserted += 1

            con.commit()

        return {
            "scanned": stats.scanned,
            "qualified": stats.qualified,
            "inserted": stats.inserted,
            "existing": stats.existing,
            "skipped": stats.skipped,
        }
