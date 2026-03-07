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
        close_only_mode: bool = False,
        pretrade_required_checks: bool = True,
        require_complex_exit_orders: bool = True,
        require_broker_external_identifier: bool = True,
        max_reject_streak: int = 5,
        max_allowed_entry_slippage_abs: float = 0.15,
        startup_reconcile_required: bool = True,
        strict_quarantine_requires_operator_clear: bool = True,
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
        self.close_only_mode = bool(close_only_mode)
        self.pretrade_required_checks = bool(pretrade_required_checks)
        self.require_complex_exit_orders = bool(require_complex_exit_orders)
        self.require_broker_external_identifier = bool(require_broker_external_identifier)
        self.max_reject_streak = max(1, int(max_reject_streak))
        self.max_allowed_entry_slippage_abs = max(0.0, float(max_allowed_entry_slippage_abs))
        self.startup_reconcile_required = bool(startup_reconcile_required)
        self.strict_quarantine_requires_operator_clear = bool(strict_quarantine_requires_operator_clear)

    def _connect(self):
        if self._connect_fn is not None:
            return self._connect_fn(self.db_path)
        con = sqlite3.connect(self.db_path, timeout=5.0)
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA journal_mode=WAL;")
        con.execute("PRAGMA busy_timeout=5000;")
        return con

    def startup_reconcile_ready(self) -> tuple[bool, dict[str, Any]]:
        if not self.startup_reconcile_required:
            return True, {'required': False}
        with self._connect() as con:
            r = con.execute(
                """
                SELECT id, snapshot_ts, resolved_bool, diff_json
                FROM broker_reconciliation_log
                WHERE environment=? AND broker_name=?
                ORDER BY id DESC LIMIT 1
                """,
                (self.environment, self.broker_name),
            ).fetchone()
        if r is None:
            return False, {'required': True, 'reason': 'no_reconciliation_snapshot'}
        ok = bool(int(r['resolved_bool'] or 0))
        return ok, {
            'required': True,
            'snapshot_id': int(r['id']),
            'snapshot_ts': str(r['snapshot_ts']),
            'resolved_bool': bool(int(r['resolved_bool'] or 0)),
        }

    def _strict_quarantine_active(self, con: sqlite3.Connection) -> bool:
        if not self.strict_quarantine_requires_operator_clear:
            return False
        tz = ZoneInfo(self.session_tz)
        sess_day = datetime.now(timezone.utc).astimezone(tz).date().isoformat()
        r = con.execute(
            """
            SELECT block_new_entries, reason
            FROM risk_session_state
            WHERE environment=? AND broker_name=? AND session_day_local=?
            """,
            (self.environment, self.broker_name, sess_day),
        ).fetchone()
        if not r:
            return False
        blocked = bool(int(r[0] or 0))
        reason = str(r[1] or '')
        return blocked and reason.startswith('strict_quarantine_')

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
    def _intent_qty(params: dict[str, Any]) -> int:
        p = params or {}
        for k in ('qty', 'quantity', 'contracts'):
            if p.get(k) is not None:
                try:
                    return max(1, int(p.get(k)))
                except Exception:
                    pass
        return 1

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

    def _record_incident(
        self,
        con: sqlite3.Connection,
        *,
        severity: str,
        incident_type: str,
        trade_run_id: int | None,
        execution_intent_id: int | None,
        details: dict[str, Any],
    ) -> None:
        con.execute(
            """
            INSERT INTO incident_events(created_at_utc, environment, broker_name, severity, incident_type, trade_run_id, execution_intent_id, details_json)
            VALUES(?,?,?,?,?,?,?,?)
            """,
            (
                _now_utc_iso(),
                self.environment,
                self.broker_name,
                str(severity),
                str(incident_type),
                (int(trade_run_id) if trade_run_id is not None else None),
                (int(execution_intent_id) if execution_intent_id is not None else None),
                json.dumps(details, separators=(",", ":"), sort_keys=True),
            ),
        )

    def _mark_intent(
        self,
        con: sqlite3.Connection,
        *,
        intent_id: int,
        status: str,
        error: str | None = None,
        broker_external_id: str | None = None,
        precheck_status: str | None = None,
        precheck_payload: dict[str, Any] | None = None,
        risk_gate_status: str | None = None,
        quarantine_reason: str | None = None,
    ) -> None:
        con.execute(
            """
            UPDATE execution_intents
            SET status=?, error=?, updated_at_utc=?,
                broker_external_id=COALESCE(?, broker_external_id),
                precheck_status=COALESCE(?, precheck_status),
                precheck_payload_json=COALESCE(?, precheck_payload_json),
                risk_gate_status=COALESCE(?, risk_gate_status),
                quarantine_reason=COALESCE(?, quarantine_reason)
            WHERE id=?
            """,
            (
                str(status),
                (str(error) if error is not None else None),
                _now_utc_iso(),
                (str(broker_external_id) if broker_external_id is not None else None),
                (str(precheck_status) if precheck_status is not None else None),
                (json.dumps(precheck_payload, separators=(",", ":"), sort_keys=True) if precheck_payload is not None else None),
                (str(risk_gate_status) if risk_gate_status is not None else None),
                (str(quarantine_reason) if quarantine_reason is not None else None),
                int(intent_id),
            ),
        )

    def _create_trade_run(self, con: sqlite3.Connection, *, intent_id: int, payload: dict[str, Any], qty: int = 1) -> int:
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
                int(max(1, qty)),
                "intent_executor",
                json.dumps(payload, separators=(",", ":"), sort_keys=True),
            ),
        )
        return int(cur.lastrowid)

    def _current_reject_streak(self, con: sqlite3.Connection) -> int:
        rows = con.execute(
            """
            SELECT status FROM execution_intents
            WHERE environment=? AND broker_name=?
            ORDER BY id DESC LIMIT ?
            """,
            (self.environment, self.broker_name, int(self.max_reject_streak)),
        ).fetchall()
        streak = 0
        for r in rows:
            st = str(r[0] or '').lower()
            if st in {'rejected', 'error', 'precheck_failed'}:
                streak += 1
            else:
                break
        return streak

    def _slippage_breaker_triggered(self, con: sqlite3.Connection) -> tuple[bool, dict[str, Any]]:
        rows = con.execute(
            """
            SELECT execution_intent_id, raw_payload_json
            FROM order_events
            WHERE environment=? AND broker_name=? AND event_type='submit_attempt'
            ORDER BY id DESC LIMIT 200
            """,
            (self.environment, self.broker_name),
        ).fetchall()
        by_i: dict[int, list[float]] = {}
        for r in rows:
            iid = int(r[0] or 0)
            if iid <= 0:
                continue
            try:
                obj = json.loads(r[1] or '{}')
                px = float(obj.get('limit_price') or 0.0)
                if px > 0:
                    by_i.setdefault(iid, []).append(px)
            except Exception:
                pass
        max_conc = 0.0
        offenders = 0
        for arr in by_i.values():
            if len(arr) < 2:
                continue
            conc = abs(float(arr[0]) - float(arr[-1]))
            max_conc = max(max_conc, conc)
            if conc > self.max_allowed_entry_slippage_abs:
                offenders += 1
        trig = offenders >= 1
        return trig, {'offenders': offenders, 'max_concession': max_conc, 'threshold': self.max_allowed_entry_slippage_abs}

    def _run_prechecks(self, con: sqlite3.Connection, *, intent_id: int, dto: OrderDTO, long_sym: str, short_sym: str) -> tuple[bool, dict[str, Any], str | None]:
        checks: dict[str, Any] = {}

        # contract/symbology validation
        checks['contract_validation'] = {
            'ok': bool(long_sym and short_sym and long_sym != short_sym),
            'long_symbol': long_sym,
            'short_symbol': short_sym,
        }

        # order dry-run
        try:
            r = self.client.place_order(dto, dry_run=True)
            checks['order_dry_run'] = {'ok': True, 'response': r}
        except Exception as e:
            checks['order_dry_run'] = {'ok': False, 'error': str(e)}

        # margin dry-run
        if hasattr(self.client, 'margin_check'):
            try:
                mr = self.client.margin_check(dto=dto, dry_run=True)
                checks['margin_dry_run'] = {'ok': True, 'response': mr}
            except Exception as e:
                checks['margin_dry_run'] = {'ok': False, 'error': str(e)}
        else:
            checks['margin_dry_run'] = {'ok': True, 'skipped': 'unavailable'}

        # trading status
        if hasattr(self.client, 'get_trading_status'):
            try:
                ts = self.client.get_trading_status()
                ok = bool((ts or {}).get('ok', True))
                checks['trading_status_check'] = {'ok': ok, 'response': ts}
            except Exception as e:
                checks['trading_status_check'] = {'ok': False, 'error': str(e)}
        else:
            checks['trading_status_check'] = {'ok': True, 'skipped': 'unavailable'}

        # position limit check (simple open trade cap)
        row = con.execute(
            "SELECT COUNT(*) FROM trade_runs WHERE environment=? AND broker_name=? AND status IN ('opening','open','closing')",
            (self.environment, self.broker_name),
        ).fetchone()
        open_n = int((row[0] if row else 0) or 0)
        checks['position_limit_check'] = {'ok': open_n < 50, 'open_count': open_n, 'limit': 50}

        # reject streak circuit breaker
        streak = self._current_reject_streak(con)
        checks['reject_streak_breaker'] = {'ok': streak < self.max_reject_streak, 'streak': streak, 'max_reject_streak': self.max_reject_streak}

        # slippage breach circuit breaker (based on recent execution concessions)
        sb, sb_meta = self._slippage_breaker_triggered(con)
        checks['slippage_breach_breaker'] = {'ok': (not sb), **sb_meta}

        failed = [k for k, v in checks.items() if not bool((v or {}).get('ok', False))]
        if self.pretrade_required_checks and failed:
            return False, checks, ','.join(failed)
        return True, checks, None

    def process_once(self, *, limit: int = 25) -> dict[str, Any]:
        out = {
            "scanned": 0,
            "processed": 0,
            "filled": 0,
            "working": 0,
            "blocked": 0,
            "expired": 0,
            "rejected": 0,
            "precheck_failed": 0,
            "quarantined": 0,
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
                    qty = self._intent_qty(params)
                    ew = (payload.get("entry_window_ct") or {}) if isinstance(payload, dict) else {}

                    # close-only control plane gate
                    if self.close_only_mode:
                        self._mark_intent(con, intent_id=iid, status="QUARANTINED", error="close_only_mode", quarantine_reason="close_only_mode")
                        out["quarantined"] += 1
                        out["processed"] += 1
                        continue

                    # strict quarantine gate (requires operator clear)
                    if self._strict_quarantine_active(con):
                        self._mark_intent(con, intent_id=iid, status="QUARANTINED", error="strict_quarantine_active", quarantine_reason="strict_quarantine_active")
                        out["quarantined"] += 1
                        out["processed"] += 1
                        continue

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
                        self._mark_intent(con, intent_id=iid, status="blocked", error="risk gate blocked new entries", risk_gate_status="blocked")
                        out["blocked"] += 1
                        out["processed"] += 1
                        continue

                    self._mark_intent(con, intent_id=iid, status="PRECHECK_PENDING", error=None, precheck_status="running")
                    trade_run_id = self._create_trade_run(con, intent_id=iid, payload=payload, qty=qty)

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

                    broker_external_id = f"ei-{iid}-{int(time.time())}"
                    if self.require_broker_external_identifier and (not broker_external_id):
                        self._mark_intent(con, intent_id=iid, status="PRECHECK_FAILED", error="missing broker_external_id", precheck_status="failed")
                        self._record_incident(con, severity="error", incident_type="precheck_failed", trade_run_id=trade_run_id, execution_intent_id=iid, details={"reason": pre_fail, "checks": pre_payload})
                        out["precheck_failed"] += 1
                        out["processed"] += 1
                        continue

                    pre_dto = OrderDTO(
                        account_number=(self.client.account_number or ""),
                        underlying=str(it["symbol"] or "SPX"),
                        quantity=qty,
                        price_effect=("CREDIT" if effect == "CREDIT" else "DEBIT"),
                        limit_price=base_limit,
                        legs=[
                            OptionLeg(symbol=long_sym, quantity=qty, side="BUY", effect="OPEN"),
                            OptionLeg(symbol=short_sym, quantity=qty, side="SELL", effect="OPEN"),
                        ],
                        client_order_id=broker_external_id,
                    )
                    ok_pre, pre_payload, pre_fail = self._run_prechecks(con, intent_id=iid, dto=pre_dto, long_sym=long_sym, short_sym=short_sym)
                    if not ok_pre:
                        self._mark_intent(
                            con,
                            intent_id=iid,
                            status="PRECHECK_FAILED",
                            error=(pre_fail or "precheck failed"),
                            broker_external_id=broker_external_id,
                            precheck_status="failed",
                            precheck_payload=pre_payload,
                            quarantine_reason=(pre_fail or "precheck_failed"),
                        )
                        self._record_order_event(
                            con,
                            trade_run_id=trade_run_id,
                            execution_intent_id=iid,
                            order_id=None,
                            event_type="precheck_failed",
                            status="rejected",
                            payload={"checks": pre_payload, "reason": pre_fail},
                        )
                        self._record_incident(con, severity="error", incident_type="precheck_failed", trade_run_id=trade_run_id, execution_intent_id=iid, details={"reason": pre_fail, "checks": pre_payload})
                        out["precheck_failed"] += 1
                        out["processed"] += 1
                        continue

                    self._mark_intent(
                        con,
                        intent_id=iid,
                        status="submitting",
                        error=None,
                        broker_external_id=broker_external_id,
                        precheck_status="passed",
                        precheck_payload=pre_payload,
                        risk_gate_status="passed",
                    )
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
                            quantity=qty,
                            price_effect=("CREDIT" if effect == "CREDIT" else "DEBIT"),
                            limit_price=limit_px,
                            legs=[
                                OptionLeg(symbol=long_sym, quantity=qty, side="BUY", effect="OPEN"),
                                OptionLeg(symbol=short_sym, quantity=qty, side="SELL", effect="OPEN"),
                            ],
                            client_order_id=f"{broker_external_id}-a{attempt}",
                        )

                        if attempt == 1:
                            resp = self.client.place_order_with_warning_reconfirm(dto, dry_run=(not self.trading_enabled)) if hasattr(self.client, 'place_order_with_warning_reconfirm') else self.client.place_order(dto, dry_run=(not self.trading_enabled))
                            if isinstance(resp, dict) and ('reconfirm' in resp):
                                self._mark_intent(con, intent_id=iid, status='WARNING_RECONFIRM_REQUIRED', error=None)
                                self._record_order_event(
                                    con,
                                    trade_run_id=trade_run_id,
                                    execution_intent_id=iid,
                                    order_id=None,
                                    event_type='warning_reconfirm_completed',
                                    status='accepted',
                                    payload=resp,
                                )
                                # proceed using reconfirm payload
                                rr = resp.get('reconfirm')
                                if isinstance(rr, dict):
                                    resp = rr
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
                        filled_qty = qty
                        try:
                            fd = (resp.get('data') if isinstance(resp, dict) else None) or {}
                            fq = fd.get('filled-quantity') or fd.get('filled_quantity') or fd.get('filled_qty')
                            if fq is not None:
                                filled_qty = max(1, int(float(fq)))
                        except Exception:
                            filled_qty = qty
                        tp = (payload.get("risk") or {}).get("take_profit_pct")
                        sl = (payload.get("risk") or {}).get("stop_loss")
                        oco_resp = {"skipped": True, "reason": "no tp/sl params"}
                        if tp is not None or sl is not None:
                            # For v1 placeholder, use same symbols with CLOSE effects and nominal prices.
                            tp_dto = OrderDTO(
                                account_number=(self.client.account_number or ""),
                                underlying=str(it["symbol"] or "SPX"),
                                quantity=qty,
                                price_effect=("CREDIT" if effect == "DEBIT" else "DEBIT"),
                                limit_price=max(0.01, float(base_limit) + 0.10),
                                legs=[
                                    OptionLeg(symbol=long_sym, quantity=filled_qty, side="SELL", effect="CLOSE"),
                                    OptionLeg(symbol=short_sym, quantity=filled_qty, side="BUY", effect="CLOSE"),
                                ],
                                client_order_id=f"intent-{iid}-tp",
                            )
                            sl_dto = OrderDTO(
                                account_number=(self.client.account_number or ""),
                                underlying=str(it["symbol"] or "SPX"),
                                quantity=qty,
                                price_effect=("DEBIT" if effect == "DEBIT" else "CREDIT"),
                                limit_price=max(0.01, float(base_limit) - 0.10),
                                legs=[
                                    OptionLeg(symbol=long_sym, quantity=filled_qty, side="SELL", effect="CLOSE"),
                                    OptionLeg(symbol=short_sym, quantity=filled_qty, side="BUY", effect="CLOSE"),
                                ],
                                client_order_id=f"intent-{iid}-sl",
                            )
                            if self.require_complex_exit_orders and hasattr(self.client, "submit_complex_order"):
                                oco_resp = self.client.submit_complex_order(
                                    account_number=(self.client.account_number or ""),
                                    payload={"order-type": "OCO", "orders": [
                                        {"price": f"{tp_dto.limit_price:.2f}", "side": "take_profit", "qty": filled_qty},
                                        {"price": f"{sl_dto.limit_price:.2f}", "side": "stop_loss", "qty": filled_qty},
                                    ]},
                                    dry_run=(not self.trading_enabled),
                                )
                            else:
                                oco_resp = self.client.place_oco_exits(
                                    account_number=(self.client.account_number or ""),
                                    take_profit=tp_dto,
                                    stop_loss=sl_dto,
                                    dry_run=(not self.trading_enabled),
                                )

                        armed_ok = not bool((oco_resp or {}).get("skipped", False))
                        self._record_order_event(
                            con,
                            trade_run_id=trade_run_id,
                            execution_intent_id=iid,
                            order_id=order_id,
                            event_type=("oco_armed" if armed_ok else "PROTECTION_ARMING_FAILED"),
                            status=("accepted" if armed_ok else "failed"),
                            payload={"response": oco_resp},
                        )

                        if (not armed_ok) and self.require_complex_exit_orders:
                            # hardening policy: immediate auto-close attempt
                            try:
                                cl = self.client.close_position(
                                    account_number=(self.client.account_number or ""),
                                    symbol=str(it["symbol"] or "SPX"),
                                    quantity=qty,
                                    dry_run=(not self.trading_enabled),
                                )
                                self._record_order_event(
                                    con,
                                    trade_run_id=trade_run_id,
                                    execution_intent_id=iid,
                                    order_id=None,
                                    event_type="auto_close_on_protection_fail",
                                    status="accepted",
                                    payload={"response": cl},
                                )
                                con.execute(
                                    "UPDATE trade_runs SET status='closed', close_reason='PROTECTION_ARMING_FAILED', protection_state='failed', close_mode='force_close', updated_at_utc=?, closed_at_utc=? WHERE id=?",
                                    (_now_utc_iso(), _now_utc_iso(), int(trade_run_id)),
                                )
                                self._mark_intent(con, intent_id=iid, status="PROTECTION_ARMING_FAILED", error="protection arming failed")
                                self._record_incident(con, severity="critical", incident_type="protection_arming_failed", trade_run_id=trade_run_id, execution_intent_id=iid, details={"auto_close_attempted": True})
                                out["errors"] += 1
                                out["processed"] += 1
                                continue
                            except Exception as e:
                                self._mark_intent(con, intent_id=iid, status="PROTECTION_ARMING_FAILED", error=str(e))
                                self._record_incident(con, severity="critical", incident_type="protection_arming_failed", trade_run_id=trade_run_id, execution_intent_id=iid, details={"auto_close_attempted": False, "error": str(e)})
                                out["errors"] += 1
                                out["processed"] += 1
                                continue

                        self._mark_intent(con, intent_id=iid, status="filled", error=None)
                        con.execute(
                            """
                            UPDATE trade_runs
                            SET status='open', entry_order_id=?, complex_exit_order_id=?, protection_state=?, opened_at_utc=?, updated_at_utc=?
                            WHERE id=?
                            """,
                            (order_id, str(((oco_resp.get('data') or {}).get('id') if isinstance(oco_resp, dict) else '') or '') or None, ('partial' if (armed_ok and int(filled_qty) < int(qty)) else ('armed' if armed_ok else 'missing')), _now_utc_iso(), _now_utc_iso(), int(trade_run_id)),
                        )
                        out["filled"] += 1
                    else:
                        self._mark_intent(con, intent_id=iid, status="working", error=None)
                        con.execute(
                            "UPDATE trade_runs SET status='open', entry_order_id=?, protection_state='missing', opened_at_utc=?, updated_at_utc=? WHERE id=?",
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
