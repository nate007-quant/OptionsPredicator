from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timezone, timedelta
from zoneinfo import ZoneInfo

import exchange_calendars as xc


CENTRAL_TZ = ZoneInfo("America/Chicago")


@dataclass(frozen=True)
class SessionTimes:
    trade_day: str  # YYYY-MM-DD (Central)
    open_ct: datetime
    early_end_ct: datetime
    close_ct: datetime


def is_trading_day(trade_day_ct: date) -> bool:
    cal = xc.get_calendar("XNYS")
    ds = trade_day_ct.isoformat()
    return ds in set(str(x) for x in cal.sessions_in_range(ds, ds))


def session_times(
    *,
    trade_day_ct: date,
    early_window_minutes: int,
) -> SessionTimes:
    open_ct = datetime.combine(trade_day_ct, time(8, 30), tzinfo=CENTRAL_TZ)
    early_end_ct = open_ct + timedelta(minutes=int(early_window_minutes))
    close_ct = datetime.combine(trade_day_ct, time(15, 0), tzinfo=CENTRAL_TZ)
    return SessionTimes(
        trade_day=trade_day_ct.isoformat(),
        open_ct=open_ct,
        early_end_ct=early_end_ct,
        close_ct=close_ct,
    )


def to_central(dt_utc: datetime) -> datetime:
    if dt_utc.tzinfo is None:
        dt_utc = dt_utc.replace(tzinfo=timezone.utc)
    return dt_utc.astimezone(CENTRAL_TZ)
