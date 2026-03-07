from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _json(x: Any) -> str:
    return json.dumps(x, separators=(",", ":"), sort_keys=True)


def resolve_session_close_ct(*, now_utc: datetime, session_tz: str) -> datetime:
    """Resolve session close (calendar-aware when exchange_calendars is available).

    Primary: XNYS session close converted to session_tz.
    Fallback: local date 15:00 in session_tz.
    """
    tz = ZoneInfo(str(session_tz or "America/Chicago"))
    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=timezone.utc)

    try:
        import pandas as pd  # type: ignore
        import exchange_calendars as xc  # type: ignore

        cal = xc.get_calendar("XNYS")
        ts = pd.Timestamp(now_utc)
        sess = cal.minute_to_session(ts, direction="previous")
        close_utc = cal.session_close(sess).to_pydatetime()
        if close_utc.tzinfo is None:
            close_utc = close_utc.replace(tzinfo=timezone.utc)
        return close_utc.astimezone(tz)
    except Exception:
        local = now_utc.astimezone(tz)
        return local.replace(hour=15, minute=0, second=0, microsecond=0)


def should_force_close_now(*, now_utc: datetime, session_tz: str, force_close_minutes_before_end: int) -> bool:
    close_local = resolve_session_close_ct(now_utc=now_utc, session_tz=session_tz)
    trigger = close_local - timedelta(minutes=max(0, int(force_close_minutes_before_end)))
    now_local = now_utc.astimezone(ZoneInfo(session_tz))
    return now_local >= trigger


@dataclass
class RiskStats:
    updated_session: int = 0
    blocked_new_entries: int = 0
    force_closed: int = 0
    errors: int = 0


class RiskGuard:
    def __init__(
        self,
        *,
        db_path: str,
        environment: str,
        broker_name: str,
        session_tz: str,
        max_daily_loss_usd: float,
        force_close_minutes_before_end: int,
        trading_enabled: bool,
        client: Any,
        account_number: str,
        connect_fn: Any | None = None,
    ) -> None:
        self.db_path = str(db_path)
        self.environment = str(environment or "sandbox")
        self.broker_name = str(broker_name or "tastytrade")
        self.session_tz = str(session_tz or "America/Chicago")
        self.max_daily_loss_usd = float(max_daily_loss_usd)
        self.force_close_minutes_before_end = int(force_close_minutes_before_end)
        self.trading_enabled = bool(trading_enabled)
        self.client = client
        self.account_number = str(account_number or "")
        self._connect_fn = connect_fn

    def _connect(self):
        if self._connect_fn is not None:
            return self._connect_fn(self.db_path)
        con = sqlite3.connect(self.db_path, timeout=5.0)
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA journal_mode=WAL;")
        con.execute("PRAGMA busy_timeout=5000;")
        return con

    def _record_order_event(
        self,
        con: sqlite3.Connection,
        *,
        trade_run_id: int | None,
        execution_intent_id: int | None,
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
                (int(execution_intent_id) if execution_intent_id is not None else None),
                (str(order_id) if order_id is not None else None),
                str(event_type),
                (str(status) if status is not None else None),
                _json(payload),
            ),
        )

    def _audit(self, con: sqlite3.Connection, *, action: str, entity_type: str, entity_id: str, details: dict[str, Any]) -> None:
        con.execute(
            """
            INSERT INTO audit_log(created_at_utc, environment, actor, action, entity_type, entity_id, details_json)
            VALUES(?,?,?,?,?,?,?)
            """,
            (
                _now_utc_iso(),
                self.environment,
                "risk_guard",
                str(action),
                str(entity_type),
                str(entity_id),
                _json(details),
            ),
        )

    def _update_risk_session_state(self, con: sqlite3.Connection) -> tuple[float, float, bool]:
        tz = ZoneInfo(self.session_tz)
        now_local = datetime.now(timezone.utc).astimezone(tz)
        sess_day = now_local.date().isoformat()

        row = con.execute(
            """
            SELECT
              COALESCE(SUM(CASE WHEN status='closed' THEN pnl_realized_usd ELSE 0 END),0) AS realized,
              COALESCE(SUM(CASE WHEN status IN ('open','opening','closing') THEN pnl_unrealized_usd ELSE 0 END),0) AS unrealized
            FROM trade_runs
            WHERE environment=? AND broker_name=?
            """,
            (self.environment, self.broker_name),
        ).fetchone()

        realized = float((row[0] if row is not None else 0.0) or 0.0)
        unrealized = float((row[1] if row is not None else 0.0) or 0.0)
        total = realized + unrealized
        blocked = total <= -abs(self.max_daily_loss_usd)

        con.execute(
            """
            INSERT INTO risk_session_state(
              created_at_utc, updated_at_utc, environment, broker_name,
              session_day_local, session_tz,
              realized_pnl_usd, unrealized_pnl_usd,
              max_daily_loss_usd, block_new_entries, reason
            )
            VALUES(?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(environment, broker_name, session_day_local) DO UPDATE SET
              updated_at_utc=excluded.updated_at_utc,
              realized_pnl_usd=excluded.realized_pnl_usd,
              unrealized_pnl_usd=excluded.unrealized_pnl_usd,
              max_daily_loss_usd=excluded.max_daily_loss_usd,
              block_new_entries=excluded.block_new_entries,
              reason=excluded.reason
            """,
            (
                _now_utc_iso(),
                _now_utc_iso(),
                self.environment,
                self.broker_name,
                sess_day,
                self.session_tz,
                realized,
                unrealized,
                float(self.max_daily_loss_usd),
                1 if blocked else 0,
                ("daily_loss_breach" if blocked else None),
            ),
        )

        return realized, unrealized, blocked

    def _already_force_closed_today(self, con: sqlite3.Connection) -> bool:
        tz = ZoneInfo(self.session_tz)
        sess_day = datetime.now(timezone.utc).astimezone(tz).date().isoformat()
        r = con.execute(
            """
            SELECT id FROM audit_log
            WHERE environment=? AND actor='risk_guard' AND action='force_close_session_end'
              AND entity_type='session' AND entity_id=?
            ORDER BY id DESC LIMIT 1
            """,
            (self.environment, sess_day),
        ).fetchone()
        return r is not None

    def _force_close_positions(self, con: sqlite3.Connection) -> int:
        rows = con.execute(
            """
            SELECT id, execution_intent_id, underlying, qty, exit_order_id, complex_exit_order_id
            FROM trade_runs
            WHERE environment=? AND broker_name=? AND status IN ('open','opening','closing')
            ORDER BY id ASC
            """,
            (self.environment, self.broker_name),
        ).fetchall()

        n = 0
        for r in rows:
            rid = int(r[0])
            intent_id = int(r[1] or 0) or None
            underlying = str(r[2] or "SPX")
            qty = int(r[3] or 1)
            exit_order_id = str(r[4]) if r[4] is not None else None
            complex_exit_order_id = str(r[5]) if r[5] is not None else None

            # cancel existing protective exit if present
            cancel_resp = None
            if complex_exit_order_id:
                try:
                    cancel_resp = self.client.cancel_complex_order(
                        account_number=self.account_number,
                        complex_order_id=complex_exit_order_id,
                        dry_run=(not self.trading_enabled),
                    )
                    self._record_order_event(
                        con,
                        trade_run_id=rid,
                        execution_intent_id=intent_id,
                        order_id=complex_exit_order_id,
                        event_type="force_close_cancel_complex_exit",
                        status="accepted",
                        payload={"response": cancel_resp},
                    )
                except Exception as e:
                    self._record_order_event(
                        con,
                        trade_run_id=rid,
                        execution_intent_id=intent_id,
                        order_id=complex_exit_order_id,
                        event_type="force_close_cancel_complex_exit_error",
                        status="error",
                        payload={"error": str(e)},
                    )
            elif exit_order_id:
                try:
                    cancel_resp = self.client.cancel_order(
                        account_number=self.account_number,
                        order_id=exit_order_id,
                        dry_run=(not self.trading_enabled),
                    )
                    self._record_order_event(
                        con,
                        trade_run_id=rid,
                        execution_intent_id=intent_id,
                        order_id=exit_order_id,
                        event_type="force_close_cancel_resting_exit",
                        status="accepted",
                        payload={"response": cancel_resp},
                    )
                except Exception as e:
                    self._record_order_event(
                        con,
                        trade_run_id=rid,
                        execution_intent_id=intent_id,
                        order_id=exit_order_id,
                        event_type="force_close_cancel_resting_exit_error",
                        status="error",
                        payload={"error": str(e)},
                    )

            close_resp = self.client.close_position(
                account_number=self.account_number,
                symbol=underlying,
                quantity=max(1, qty),
                dry_run=(not self.trading_enabled),
            )
            self._record_order_event(
                con,
                trade_run_id=rid,
                execution_intent_id=intent_id,
                order_id=None,
                event_type="force_close_position",
                status="accepted",
                payload={"response": close_resp},
            )

            con.execute(
                """
                UPDATE trade_runs
                SET status='closed', close_reason='FORCE_CLOSE_SESSION_END', closed_at_utc=?, updated_at_utc=?
                WHERE id=?
                """,
                (_now_utc_iso(), _now_utc_iso(), rid),
            )
            n += 1

        return n

    def process_once(self) -> dict[str, Any]:
        st = RiskStats()
        with self._connect() as con:
            try:
                _, _, blocked = self._update_risk_session_state(con)
                st.updated_session = 1
                if blocked:
                    st.blocked_new_entries = 1
            except Exception:
                st.errors += 1

            now_utc = datetime.now(timezone.utc)
            if should_force_close_now(
                now_utc=now_utc,
                session_tz=self.session_tz,
                force_close_minutes_before_end=self.force_close_minutes_before_end,
            ) and (not self._already_force_closed_today(con)):
                try:
                    n = self._force_close_positions(con)
                    st.force_closed = int(n)
                    tz = ZoneInfo(self.session_tz)
                    sess_day = now_utc.astimezone(tz).date().isoformat()
                    self._audit(
                        con,
                        action="force_close_session_end",
                        entity_type="session",
                        entity_id=sess_day,
                        details={
                            "force_close_minutes_before_end": self.force_close_minutes_before_end,
                            "trades_closed": int(n),
                        },
                    )
                except Exception as e:
                    st.errors += 1
                    self._audit(
                        con,
                        action="force_close_session_end_error",
                        entity_type="session",
                        entity_id="unknown",
                        details={"error": str(e)},
                    )

            con.commit()

        return {
            "updated_session": st.updated_session,
            "blocked_new_entries": st.blocked_new_entries,
            "force_closed": st.force_closed,
            "errors": st.errors,
        }
