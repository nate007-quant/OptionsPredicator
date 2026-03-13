from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _parse_json(s: Any, default: Any) -> Any:
    if s is None:
        return default
    try:
        return json.loads(str(s))
    except Exception:
        return default


def _json(x: Any) -> str:
    return json.dumps(x, separators=(",", ":"), sort_keys=True)


@dataclass
class MonitorStats:
    scanned: int = 0
    updated: int = 0
    order_events_written: int = 0
    position_events_written: int = 0
    missing_protection_alerts: int = 0
    errors: int = 0


class ExecutionMonitor:
    """Execution monitor worker.

    Responsibilities (Phase 5):
      - reconcile open/working trade runs with broker updates
      - persist order/position updates into order_events / position_events
      - detect missing protective exits (OCO) and alert via audit_log
      - support stream-event ingestion via `ingest_stream_event(...)`
    """

    def __init__(
        self,
        *,
        db_path: str,
        environment: str,
        broker_name: str,
        account_number: str,
        client: Any,
        connect_fn: Any | None = None,
        rearm_missing_protection: bool = False,
        max_position_mismatch_count: int = 3,
        max_streamer_downtime_seconds: int = 120,
    ) -> None:
        self.db_path = str(db_path)
        self.environment = str(environment or "sandbox")
        self.broker_name = str(broker_name or "tastytrade")
        self.account_number = str(account_number or "")
        self.client = client
        self._connect_fn = connect_fn
        self.rearm_missing_protection = bool(rearm_missing_protection)
        self.max_position_mismatch_count = max(1, int(max_position_mismatch_count))
        self.max_streamer_downtime_seconds = max(5, int(max_streamer_downtime_seconds))

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

    def _record_position_event(
        self,
        con: sqlite3.Connection,
        *,
        trade_run_id: int | None,
        position_key: str | None,
        event_type: str,
        qty: float | None,
        price: float | None,
        pnl_unrealized_usd: float | None,
        pnl_realized_usd: float | None,
        payload: Any,
    ) -> None:
        con.execute(
            """
            INSERT INTO position_events(created_at_utc, environment, broker_name, trade_run_id, position_key, event_type, qty, price, pnl_unrealized_usd, pnl_realized_usd, raw_payload_json)
            VALUES(?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                _now_utc_iso(),
                self.environment,
                self.broker_name,
                (int(trade_run_id) if trade_run_id is not None else None),
                (str(position_key) if position_key is not None else None),
                str(event_type),
                (float(qty) if qty is not None else None),
                (float(price) if price is not None else None),
                (float(pnl_unrealized_usd) if pnl_unrealized_usd is not None else None),
                (float(pnl_realized_usd) if pnl_realized_usd is not None else None),
                _json(payload),
            ),
        )

    def _audit(self, con: sqlite3.Connection, *, action: str, entity_type: str, entity_id: str, details: dict[str, Any]) -> None:
        try:
            con.execute(
                """
                INSERT INTO audit_log(created_at_utc, environment, actor, action, entity_type, entity_id, details_json)
                VALUES(?,?,?,?,?,?,?)
                """,
                (
                    _now_utc_iso(),
                    self.environment,
                    "execution_monitor",
                    str(action),
                    str(entity_type),
                    str(entity_id),
                    _json(details),
                ),
            )
        except sqlite3.OperationalError:
            # Do not crash monitor on transient SQLite lock contention.
            pass

    def _incident(
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
                _json(details),
            ),
        )

    def ingest_stream_event(self, event: dict[str, Any]) -> dict[str, Any]:
        """Persist one stream event (if broker stream integration forwards events here)."""
        et = str((event or {}).get("type") or "stream_event")
        with self._connect() as con:
            self._record_order_event(
                con,
                trade_run_id=(event or {}).get("trade_run_id"),
                execution_intent_id=(event or {}).get("execution_intent_id"),
                order_id=(event or {}).get("order_id"),
                event_type=f"stream:{et}",
                status=str((event or {}).get("status") or "update"),
                payload=event,
            )
            con.commit()
        return {"ok": True, "event_type": et}

    def _has_protective_exit(self, con: sqlite3.Connection, trade_run_id: int) -> bool:
        r = con.execute(
            """
            SELECT id FROM order_events
            WHERE trade_run_id=? AND event_type IN ('oco_armed','protective_exit_armed')
            ORDER BY id DESC LIMIT 1
            """,
            (int(trade_run_id),),
        ).fetchone()
        return r is not None


    def _already_alerted_missing_protection(self, con: sqlite3.Connection, trade_run_id: int) -> bool:
        r = con.execute(
            """
            SELECT id FROM audit_log
            WHERE environment=? AND actor='execution_monitor' AND action='missing_protective_exit_detected'
              AND entity_type='trade_run' AND entity_id=?
            ORDER BY id DESC LIMIT 1
            """,
            (self.environment, str(int(trade_run_id))),
        ).fetchone()
        return r is not None

    def _extract_list(self, payload: Any) -> list[dict[str, Any]]:
        if isinstance(payload, list):
            return [x for x in payload if isinstance(x, dict)]
        if isinstance(payload, dict):
            d = payload.get("data")
            if isinstance(d, list):
                return [x for x in d if isinstance(x, dict)]
            if isinstance(d, dict):
                for k in ("items", "orders", "positions"):
                    v = d.get(k)
                    if isinstance(v, list):
                        return [x for x in v if isinstance(x, dict)]
            for k in ("items", "orders", "positions"):
                v = payload.get(k)
                if isinstance(v, list):
                    return [x for x in v if isinstance(x, dict)]
        return []

    def _streamer_down_breaker(self, con: sqlite3.Connection) -> tuple[bool, dict[str, Any]]:
        row = con.execute(
            """
            SELECT created_at_utc FROM order_events
            WHERE environment=? AND broker_name=? AND event_type LIKE 'stream:%'
            ORDER BY id DESC LIMIT 1
            """,
            (self.environment, self.broker_name),
        ).fetchone()
        if not row:
            return False, {'last_stream_event_utc': None, 'downtime_seconds': None, 'threshold_seconds': self.max_streamer_downtime_seconds}
        try:
            ts = datetime.fromisoformat(str(row[0]).replace('Z', '+00:00'))
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            dt = (datetime.now(timezone.utc) - ts.astimezone(timezone.utc)).total_seconds()
        except Exception:
            dt = 0.0
        return (dt > float(self.max_streamer_downtime_seconds)), {
            'last_stream_event_utc': str(row[0]),
            'downtime_seconds': float(dt),
            'threshold_seconds': self.max_streamer_downtime_seconds,
        }

    def _recent_unresolved_mismatch_count(self, con: sqlite3.Connection) -> int:
        row = con.execute(
            """
            SELECT COUNT(*)
            FROM (
              SELECT resolved_bool
              FROM broker_reconciliation_log
              WHERE environment=? AND broker_name=?
              ORDER BY id DESC
              LIMIT 200
            ) t
            WHERE COALESCE(t.resolved_bool, 0)=0
            """,
            (self.environment, self.broker_name),
        ).fetchone()
        return int((row[0] if row else 0) or 0)

    def process_once(self, *, limit: int = 200) -> dict[str, Any]:
        st = MonitorStats()

        with self._connect() as con:
            rows = con.execute(
                """
                SELECT id, execution_intent_id, status, underlying, entry_order_id, run_payload_json
                FROM trade_runs
                WHERE environment=? AND broker_name=? AND status IN ('opening','open','closing')
                ORDER BY id ASC
                LIMIT ?
                """,
                (self.environment, self.broker_name, int(limit)),
            ).fetchall()

            # REST sync (disconnect safety)
            orders_payload = None
            positions_payload = None
            try:
                if hasattr(self.client, "get_orders"):
                    orders_payload = self.client.get_orders(account_number=self.account_number, status=None)
            except Exception as e:
                st.errors += 1
                self._audit(con, action="rest_sync_orders_error", entity_type="monitor", entity_id="global", details={"error": str(e)})

            try:
                if hasattr(self.client, "get_positions"):
                    positions_payload = self.client.get_positions(account_number=self.account_number)
            except Exception as e:
                st.errors += 1
                self._audit(con, action="rest_sync_positions_error", entity_type="monitor", entity_id="global", details={"error": str(e)})

            orders = self._extract_list(orders_payload)
            positions = self._extract_list(positions_payload)

            # reconciliation snapshot
            open_runs = len(rows)
            pos_count = len(positions)
            mismatch = 1 if (open_runs > 0 and pos_count == 0) else 0
            resolved = 0 if mismatch else 1
            con.execute(
                """
                INSERT INTO broker_reconciliation_log(snapshot_ts, environment, broker_name, open_orders_json, open_positions_json, diff_json, resolved_bool)
                VALUES(?,?,?,?,?,?,?)
                """,
                (
                    _now_utc_iso(),
                    self.environment,
                    self.broker_name,
                    _json({'items': orders}),
                    _json({'items': positions}),
                    _json({'open_trade_runs': open_runs, 'positions_count': pos_count, 'mismatch': bool(mismatch)}),
                    int(resolved),
                ),
            )

            # stream-down breaker
            stream_down, stream_meta = self._streamer_down_breaker(con)
            if stream_down:
                self._incident(
                    con,
                    severity='error',
                    incident_type='strict_quarantine_stream_down',
                    trade_run_id=None,
                    execution_intent_id=None,
                    details=stream_meta,
                )
                try:
                    from zoneinfo import ZoneInfo
                    tz = ZoneInfo('America/Chicago')
                    day_local = datetime.now(timezone.utc).astimezone(tz).date().isoformat()
                    now = _now_utc_iso()
                    con.execute(
                        """
                        INSERT INTO risk_session_state(
                          created_at_utc, updated_at_utc, environment, broker_name,
                          session_day_local, session_tz,
                          realized_pnl_usd, unrealized_pnl_usd,
                          max_daily_loss_usd, block_new_entries, reason
                        ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
                        ON CONFLICT(environment, broker_name, session_day_local) DO UPDATE SET
                          updated_at_utc=excluded.updated_at_utc,
                          block_new_entries=1,
                          reason='strict_quarantine_stream_down'
                        """,
                        (now, now, self.environment, self.broker_name, day_local, 'America/Chicago', 0.0, 0.0, 300.0, 1, 'strict_quarantine_stream_down'),
                    )
                except Exception:
                    pass

            if mismatch:
                self._incident(
                    con,
                    severity='error',
                    incident_type='position_mismatch',
                    trade_run_id=None,
                    execution_intent_id=None,
                    details={'open_trade_runs': open_runs, 'positions_count': pos_count},
                )
                # unresolved mismatch => enter close-only behavior by blocking new entries
                try:
                    from zoneinfo import ZoneInfo
                    tz = ZoneInfo('America/Chicago')
                    day_local = datetime.now(timezone.utc).astimezone(tz).date().isoformat()
                    now = _now_utc_iso()
                    con.execute(
                        """
                        INSERT INTO risk_session_state(
                          created_at_utc, updated_at_utc, environment, broker_name,
                          session_day_local, session_tz,
                          realized_pnl_usd, unrealized_pnl_usd,
                          max_daily_loss_usd, block_new_entries, reason
                        ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
                        ON CONFLICT(environment, broker_name, session_day_local) DO UPDATE SET
                          updated_at_utc=excluded.updated_at_utc,
                          block_new_entries=1,
                          reason='position_mismatch_close_only'
                        """,
                        (now, now, self.environment, self.broker_name, day_local, 'America/Chicago', 0.0, 0.0, 300.0, 1, 'position_mismatch_close_only'),
                    )
                except Exception:
                    pass

            # mismatch breaker threshold (only escalate while currently mismatched)
            mm_count = self._recent_unresolved_mismatch_count(con)
            if mismatch and (mm_count >= self.max_position_mismatch_count):
                self._incident(
                    con,
                    severity='error',
                    incident_type='position_mismatch_breaker',
                    trade_run_id=None,
                    execution_intent_id=None,
                    details={'unresolved_mismatch_count': mm_count, 'threshold': self.max_position_mismatch_count, 'operator_clear_required': True},
                )
                try:
                    from zoneinfo import ZoneInfo
                    tz = ZoneInfo('America/Chicago')
                    day_local = datetime.now(timezone.utc).astimezone(tz).date().isoformat()
                    now = _now_utc_iso()
                    con.execute(
                        """
                        INSERT INTO risk_session_state(
                          created_at_utc, updated_at_utc, environment, broker_name,
                          session_day_local, session_tz,
                          realized_pnl_usd, unrealized_pnl_usd,
                          max_daily_loss_usd, block_new_entries, reason
                        ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
                        ON CONFLICT(environment, broker_name, session_day_local) DO UPDATE SET
                          updated_at_utc=excluded.updated_at_utc,
                          block_new_entries=1,
                          reason='strict_quarantine_position_mismatch'
                        """,
                        (now, now, self.environment, self.broker_name, day_local, 'America/Chicago', 0.0, 0.0, 300.0, 1, 'strict_quarantine_position_mismatch'),
                    )
                except Exception:
                    pass

            for tr in rows:
                st.scanned += 1
                trade_run_id = int(tr["id"])
                intent_id = int(tr["execution_intent_id"] or 0) or None
                run_payload = _parse_json(tr["run_payload_json"], {})
                underlying = str(tr["underlying"] or "")

                # Persist order snapshot event (scoped by symbol when available)
                scoped_orders = [o for o in orders if not underlying or str(o.get("underlying-symbol") or o.get("underlying") or "").upper() == underlying.upper()]
                self._record_order_event(
                    con,
                    trade_run_id=trade_run_id,
                    execution_intent_id=intent_id,
                    order_id=(tr["entry_order_id"] if tr["entry_order_id"] else None),
                    event_type="rest_sync_orders",
                    status="synced",
                    payload={"orders": scoped_orders},
                )
                st.order_events_written += 1

                # Persist position snapshot event (best-effort)
                scoped_positions = [p for p in positions if not underlying or str(p.get("underlying-symbol") or p.get("underlying") or p.get("symbol") or "").upper().startswith(underlying.upper())]
                self._record_position_event(
                    con,
                    trade_run_id=trade_run_id,
                    position_key=underlying or None,
                    event_type="rest_sync_positions",
                    qty=None,
                    price=None,
                    pnl_unrealized_usd=None,
                    pnl_realized_usd=None,
                    payload={"positions": scoped_positions},
                )
                st.position_events_written += 1

                tr_status = str(tr["status"] or "")
                if tr_status == 'closing' and len(scoped_positions) == 0:
                    con.execute(
                        "UPDATE trade_runs SET status='closed', close_reason=COALESCE(close_reason,'operator_flatten_complete'), closed_at_utc=COALESCE(closed_at_utc, ?), updated_at_utc=? WHERE id=?",
                        (_now_utc_iso(), _now_utc_iso(), trade_run_id),
                    )
                    self._record_order_event(
                        con,
                        trade_run_id=trade_run_id,
                        execution_intent_id=intent_id,
                        order_id=None,
                        event_type="close_detected_from_positions",
                        status="filled",
                        payload={"positions_count": 0},
                    )

                if (not self._has_protective_exit(con, trade_run_id)) and (not self._already_alerted_missing_protection(con, trade_run_id)):
                    self._audit(
                        con,
                        action="missing_protective_exit_detected",
                        entity_type="trade_run",
                        entity_id=str(trade_run_id),
                        details={
                            "trade_run_id": trade_run_id,
                            "environment": self.environment,
                            "broker_name": self.broker_name,
                            "rearm_attempted": bool(self.rearm_missing_protection),
                            "run_payload": run_payload,
                        },
                    )
                    st.missing_protection_alerts += 1

                con.execute("UPDATE trade_runs SET updated_at_utc=? WHERE id=?", (_now_utc_iso(), trade_run_id))
                st.updated += 1

            con.commit()

        return {
            "scanned": st.scanned,
            "updated": st.updated,
            "order_events_written": st.order_events_written,
            "position_events_written": st.position_events_written,
            "missing_protection_alerts": st.missing_protection_alerts,
            "errors": st.errors,
        }
