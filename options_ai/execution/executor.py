from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

from options_ai.brokers.tastytrade.client import OptionLeg, OrderDTO, TastytradeClient


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _parse_json(s: Any, default: Any) -> Any:
    if s is None:
        return default
    try:
        return json.loads(str(s))
    except Exception:
        return default


def _hhmm_now(tz_name: str) -> str:
    tz = ZoneInfo(str(tz_name or "America/Chicago"))
    now = datetime.now(timezone.utc).astimezone(tz)
    return now.strftime("%H:%M")


def _hhmm_to_minutes(x: str | None) -> int | None:
    if not x:
        return None
    try:
        hh, mm = str(x).split(":")
        return int(hh) * 60 + int(mm)
    except Exception:
        return None


def is_entry_window_open(*, now_hhmm: str, first_trade_time_ct: str | None, last_new_entry_time_ct: str | None) -> bool:
    cur = _hhmm_to_minutes(now_hhmm)
    if cur is None:
        return False
    lo = _hhmm_to_minutes(first_trade_time_ct)
    hi = _hhmm_to_minutes(last_new_entry_time_ct)
    if lo is None and hi is None:
        return True
    if lo is not None and cur < lo:
        return False
    if hi is not None and cur > hi:
        return False
    return True


def compute_reprice_limit(
    *,
    base_limit: float,
    attempt_num: int,
    price_effect: str,
    step: float,
    max_total_concession: float,
) -> float:
    """Compute repriced limit per attempt with cap.

    - DEBIT: concession means paying more -> price increases.
    - CREDIT: concession means accepting less -> price decreases.
    """
    n = max(1, int(attempt_num))
    base = float(base_limit)
    st = max(0.0, float(step))
    cap = max(0.0, float(max_total_concession))
    concession = min(cap, st * float(n - 1))

    eff = str(price_effect or "DEBIT").upper()
    if eff == "CREDIT":
        out = base - concession
    else:
        out = base + concession

    # keep positive and cents precision
    out = round(max(0.01, out), 2)
    return out


@dataclass
class RepricePolicy:
    max_attempts: int
    step: float
    interval_seconds: int
    max_total_concession: float


class ExecutionExecutor:
    def __init__(
        self,
        *,
        db_path: str,
        environment: str,
        broker_name: str,
        session_tz: str,
        trading_enabled: bool,
        max_daily_loss_usd: float,
        reprice_defaults: RepricePolicy,
        connect_fn: Any | None = None,
        client: TastytradeClient | None = None,
    ) -> None:
        self.db_path = str(db_path)
        self.environment = str(environment or "sandbox")
        self.broker_name = str(broker_name or "tastytrade")
        self.session_tz = str(session_tz or "America/Chicago")
        self.trading_enabled = bool(trading_enabled)
        self.max_daily_loss_usd = float(max_daily_loss_usd)
        self.reprice_defaults = reprice_defaults
        self._connect_fn = connect_fn
        self.client = client or TastytradeClient(environment=self.environment, dry_run=(not self.trading_enabled))

    def _connect(self):
        if self._connect_fn is not None:
            return self._connect_fn(self.db_path)
        con = sqlite3.connect(self.db_path, timeout=5.0)
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA journal_mode=WAL;")
        con.execute("PRAGMA busy_timeout=5000;")
        return con

    def _risk_blocked(self, con: sqlite3.Connection) -> bool:
        tz = ZoneInfo(self.session_tz)
        sess_day = datetime.now(timezone.utc).astimezone(tz).date().isoformat()
        row = con.execute(
            """
            SELECT block_new_entries, realized_pnl_usd, unrealized_pnl_usd, max_daily_loss_usd
            FROM risk_session_state
            WHERE environment=? AND broker_name=? AND session_day_local=?
            """,
            (self.environment, self.broker_name, sess_day),
        ).fetchone()
        if row is None:
            return False
        if int(row[0] or 0) == 1:
            return True

        # Soft guard even if block flag not yet set.
        realized = float(row[1] or 0.0)
        unreal = float(row[2] or 0.0)
        max_loss = float(row[3] or self.max_daily_loss_usd)
        pnl = realized + unreal
        return pnl <= -abs(max_loss)

    def _load_reprice_policy(self, con: sqlite3.Connection, underlying: str) -> RepricePolicy:
        row = con.execute(
            """
            SELECT max_attempts, step, interval_seconds, max_total_concession
            FROM reprice_policy
            WHERE environment=? AND underlying=?
            """,
            (self.environment, str(underlying or "SPX").upper()),
        ).fetchone()
        if row is None:
            return self.reprice_defaults
        return RepricePolicy(
            max_attempts=max(1, int(row[0] or self.reprice_defaults.max_attempts)),
            step=float(row[1] or self.reprice_defaults.step),
            interval_seconds=max(1, int(row[2] or self.reprice_defaults.interval_seconds)),
            max_total_concession=float(row[3] or self.reprice_defaults.max_total_concession),
        )

    @staticmethod
    def _extract_leg_symbols(params: dict[str, Any]) -> tuple[str | None, str | None]:
        p = params or {}
        long_sym = p.get("long_leg_symbol") or p.get("long_symbol") or p.get("buy_symbol")
        short_sym = p.get("short_leg_symbol") or p.get("short_symbol") or p.get("sell_symbol")
        if long_sym is not None:
            long_sym = str(long_sym).strip().upper() or None
        if short_sym is not None:
            short_sym = str(short_sym).strip().upper() or None
        return long_sym, short_sym

    @staticmethod
    def _price_effect_from_params(params: dict[str, Any]) -> str:
        style = str((params or {}).get("spread_style") or "debit").strip().lower()
        return "CREDIT" if style == "credit" else "DEBIT"

    @staticmethod
    def _base_limit_from_params(params: dict[str, Any], *, effect: str) -> float:
        p = params or {}
        # explicit limit first
        v = p.get("entry_limit")
        if v is not None:
            try:
                return round(abs(float(v)), 2)
            except Exception:
                pass

        # fallback from strategy knobs
        if effect == "CREDIT":
            v = p.get("credit_target")
            if v is not None:
                try:
                    return round(abs(float(v)), 2)
                except Exception:
                    pass
            # modest default
            return 0.50

        v = p.get("max_debit_points")
        if v is not None:
            try:
                # start inside cap by default
                return round(max(0.05, min(float(v), 1.00)), 2)
            except Exception:
                pass
        return 0.50

    def _record_order_event(
        self,
        con: sqlite3.Connection,
        *,
        trade_run_id: int | None,
        execution_intent_id: int,
        order_id: str | None,
        event_type: str,
        status: str | None,
        payload: Any,
    ) -> None:
        con.execute(
            """
            INSERT INTO order_events(created_at_utc, environment, broker_name, trade_run_id, execution_intent_id, order_id, event_type, status, raw_payload_json)
            VALUES(?,?,?,?,?,?,?,?,?)
            """,
            (
                _now_utc_iso(),
                self.environment,
                self.broker_name,
                (int(trade_run_id) if trade_run_id is not None else None),
                int(execution_intent_id),
                (str(order_id) if order_id is not None else None),
                str(event_type),
                (str(status) if status is not None else None),
                json.dumps(payload, separators=(",", ":"), sort_keys=True),
            ),
        )

    def _mark_intent(self, con: sqlite3.Connection, *, intent_id: int, status: str, error: str | None = None) -> None:
        con.execute(
            "UPDATE execution_intents SET status=?, error=?, updated_at_utc=? WHERE id=?",
            (str(status), (str(error) if error is not None else None), _now_utc_iso(), int(intent_id)),
        )

    def _create_trade_run(self, con: sqlite3.Connection, *, intent_id: int, payload: dict[str, Any]) -> int:
        params = (payload.get("params") or {}) if isinstance(payload, dict) else {}
        cur = con.execute(
            """
            INSERT INTO trade_runs(
              created_at_utc, updated_at_utc, environment, broker_name,
              execution_intent_id, status, underlying, side, qty, open_reason, run_payload_json
            )
            VALUES(?,?,?,?,?,'opening',?,?,?,?,?)
            """,
            (
                _now_utc_iso(),
                _now_utc_iso(),
                self.environment,
                self.broker_name,
                int(intent_id),
                str((payload.get("symbol") if isinstance(payload, dict) else None) or "SPX"),
                str(self._price_effect_from_params(params)),
                1,
                "intent_executor",
                json.dumps(payload, separators=(",", ":"), sort_keys=True),
            ),
        )
        return int(cur.lastrowid)

    def process_once(self, *, limit: int = 25) -> dict[str, Any]:
        out = {
            "scanned": 0,
            "processed": 0,
            "filled": 0,
            "working": 0,
            "blocked": 0,
            "expired": 0,
            "rejected": 0,
            "errors": 0,
        }

        with self._connect() as con:
            intents = con.execute(
                """
                SELECT id, strategy_key, symbol, intent_payload_json
                FROM execution_intents
                WHERE status='pending' AND environment=? AND broker_name=?
                ORDER BY id ASC
                LIMIT ?
                """,
                (self.environment, self.broker_name, int(limit)),
            ).fetchall()

            for it in intents:
                out["scanned"] += 1
                iid = int(it["id"])

                try:
                    payload = _parse_json(it["intent_payload_json"], {})
                    params = (payload.get("params") or {}) if isinstance(payload, dict) else {}
                    ew = (payload.get("entry_window_ct") or {}) if isinstance(payload, dict) else {}

                    # entry window gate
                    now_hhmm = _hhmm_now(self.session_tz)
                    if not is_entry_window_open(
                        now_hhmm=now_hhmm,
                        first_trade_time_ct=ew.get("first_trade_time_ct"),
                        last_new_entry_time_ct=ew.get("last_new_entry_time_ct"),
                    ):
                        # If before start, keep pending. If after end, expire.
                        lo = _hhmm_to_minutes(ew.get("first_trade_time_ct"))
                        cur = _hhmm_to_minutes(now_hhmm)
                        hi = _hhmm_to_minutes(ew.get("last_new_entry_time_ct"))
                        if lo is not None and cur is not None and cur < lo:
                            continue
                        self._mark_intent(con, intent_id=iid, status="expired", error="outside entry window")
                        out["expired"] += 1
                        out["processed"] += 1
                        continue

                    # daily loss/risk gate
                    if self._risk_blocked(con):
                        self._mark_intent(con, intent_id=iid, status="blocked", error="risk gate blocked new entries")
                        out["blocked"] += 1
                        out["processed"] += 1
                        continue

                    self._mark_intent(con, intent_id=iid, status="submitting", error=None)
                    trade_run_id = self._create_trade_run(con, intent_id=iid, payload=payload)

                    long_sym, short_sym = self._extract_leg_symbols(params)
                    if not long_sym or not short_sym:
                        self._record_order_event(
                            con,
                            trade_run_id=trade_run_id,
                            execution_intent_id=iid,
                            order_id=None,
                            event_type="validation_failed",
                            status="rejected",
                            payload={"error": "missing long/short leg symbols in intent params"},
                        )
                        self._mark_intent(con, intent_id=iid, status="rejected", error="missing long/short leg symbols")
                        con.execute(
                            "UPDATE trade_runs SET status='rejected', updated_at_utc=?, close_reason=? WHERE id=?",
                            (_now_utc_iso(), "validation_failed", int(trade_run_id)),
                        )
                        out["rejected"] += 1
                        out["processed"] += 1
                        continue

                    effect = self._price_effect_from_params(params)
                    base_limit = self._base_limit_from_params(params, effect=effect)
                    policy = self._load_reprice_policy(con, str(it["symbol"] or "SPX"))

                    order_id: str | None = None
                    filled = False

                    for attempt in range(1, max(1, int(policy.max_attempts)) + 1):
                        limit_px = compute_reprice_limit(
                            base_limit=base_limit,
                            attempt_num=attempt,
                            price_effect=effect,
                            step=policy.step,
                            max_total_concession=policy.max_total_concession,
                        )

                        dto = OrderDTO(
                            account_number=(self.client.account_number or ""),
                            underlying=str(it["symbol"] or "SPX"),
                            quantity=1,
                            price_effect=("CREDIT" if effect == "CREDIT" else "DEBIT"),
                            limit_price=limit_px,
                            legs=[
                                OptionLeg(symbol=long_sym, quantity=1, side="BUY", effect="OPEN"),
                                OptionLeg(symbol=short_sym, quantity=1, side="SELL", effect="OPEN"),
                            ],
                            client_order_id=f"intent-{iid}-a{attempt}",
                        )

                        if attempt == 1:
                            resp = self.client.place_order(dto, dry_run=(not self.trading_enabled))
                        else:
                            if order_id:
                                resp = self.client.replace_order(
                                    account_number=(self.client.account_number or ""),
                                    order_id=order_id,
                                    dto=dto,
                                    dry_run=(not self.trading_enabled),
                                )
                            else:
                                resp = self.client.place_order(dto, dry_run=(not self.trading_enabled))

                        order_id = str((resp.get("data") or {}).get("id") or resp.get("order-id") or order_id or "") or None

                        self._record_order_event(
                            con,
                            trade_run_id=trade_run_id,
                            execution_intent_id=iid,
                            order_id=order_id,
                            event_type="submit_attempt",
                            status=("filled" if (not self.trading_enabled) else "working"),
                            payload={
                                "attempt": attempt,
                                "limit_price": limit_px,
                                "response": resp,
                            },
                        )

                        # Dry-run mode simulates immediate fill on first accepted submit.
                        if not self.trading_enabled:
                            filled = True
                            break

                        # Live mode: keep it simple for v1 (mark working after first submit).
                        filled = False
                        if attempt < int(policy.max_attempts):
                            time.sleep(max(1, int(policy.interval_seconds)))

                    if filled:
                        # place OCO exits from candidate TP/SL params
                        tp = (payload.get("risk") or {}).get("take_profit_pct")
                        sl = (payload.get("risk") or {}).get("stop_loss")
                        oco_resp = {"skipped": True, "reason": "no tp/sl params"}
                        if tp is not None or sl is not None:
                            # For v1 placeholder, use same symbols with CLOSE effects and nominal prices.
                            tp_dto = OrderDTO(
                                account_number=(self.client.account_number or ""),
                                underlying=str(it["symbol"] or "SPX"),
                                quantity=1,
                                price_effect=("CREDIT" if effect == "DEBIT" else "DEBIT"),
                                limit_price=max(0.01, float(base_limit) + 0.10),
                                legs=[
                                    OptionLeg(symbol=long_sym, quantity=1, side="SELL", effect="CLOSE"),
                                    OptionLeg(symbol=short_sym, quantity=1, side="BUY", effect="CLOSE"),
                                ],
                                client_order_id=f"intent-{iid}-tp",
                            )
                            sl_dto = OrderDTO(
                                account_number=(self.client.account_number or ""),
                                underlying=str(it["symbol"] or "SPX"),
                                quantity=1,
                                price_effect=("DEBIT" if effect == "DEBIT" else "CREDIT"),
                                limit_price=max(0.01, float(base_limit) - 0.10),
                                legs=[
                                    OptionLeg(symbol=long_sym, quantity=1, side="SELL", effect="CLOSE"),
                                    OptionLeg(symbol=short_sym, quantity=1, side="BUY", effect="CLOSE"),
                                ],
                                client_order_id=f"intent-{iid}-sl",
                            )
                            oco_resp = self.client.place_oco_exits(
                                account_number=(self.client.account_number or ""),
                                take_profit=tp_dto,
                                stop_loss=sl_dto,
                                dry_run=(not self.trading_enabled),
                            )

                        self._record_order_event(
                            con,
                            trade_run_id=trade_run_id,
                            execution_intent_id=iid,
                            order_id=order_id,
                            event_type="oco_armed",
                            status="accepted",
                            payload={"response": oco_resp},
                        )

                        self._mark_intent(con, intent_id=iid, status="filled", error=None)
                        con.execute(
                            """
                            UPDATE trade_runs
                            SET status='open', entry_order_id=?, opened_at_utc=?, updated_at_utc=?
                            WHERE id=?
                            """,
                            (order_id, _now_utc_iso(), _now_utc_iso(), int(trade_run_id)),
                        )
                        out["filled"] += 1
                    else:
                        self._mark_intent(con, intent_id=iid, status="working", error=None)
                        con.execute(
                            "UPDATE trade_runs SET status='open', entry_order_id=?, opened_at_utc=?, updated_at_utc=? WHERE id=?",
                            (order_id, _now_utc_iso(), _now_utc_iso(), int(trade_run_id)),
                        )
                        out["working"] += 1

                    out["processed"] += 1
                except Exception as e:
                    self._mark_intent(con, intent_id=iid, status="error", error=str(e))
                    self._record_order_event(
                        con,
                        trade_run_id=None,
                        execution_intent_id=iid,
                        order_id=None,
                        event_type="executor_error",
                        status="error",
                        payload={"error": str(e)},
                    )
                    out["errors"] += 1
                    out["processed"] += 1

            con.commit()

        return out
