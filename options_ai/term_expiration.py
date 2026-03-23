from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Literal

import psycopg
from zoneinfo import ZoneInfo


@dataclass(frozen=True)
class PickedExpiration:
    expiration_date: date
    dte_days: int
    dte_diff: int


def pick_expiration_for_target_dte(
    conn: psycopg.Connection,
    *,
    snapshot_ts: datetime,
    target_dte_days: int,
    dte_tolerance_days: int,
    tz_local: str,
    underlying: str = "SPX",
) -> PickedExpiration | None:
    """Pick one expiration for a snapshot_ts closest to target DTE.

    DTE is computed in *local trade date* terms:
      dte_days = (expiration_date - (snapshot_ts AT TIME ZONE tz_local)::date)

    Returns None when no expiration is within tolerance.
    """

    with conn.cursor() as cur:
        cur.execute(
            """
            WITH exps AS (
              SELECT DISTINCT expiration_date
              FROM spx.option_chain
              WHERE snapshot_ts = %s
                AND expiration_date IS NOT NULL
                AND UPPER(underlying) = %s
            )
            SELECT
              expiration_date,
              ((expiration_date - (%s AT TIME ZONE %s)::date))::int AS dte_days,
              ABS(((expiration_date - (%s AT TIME ZONE %s)::date))::int - %s)::int AS dte_diff
            FROM exps
            WHERE ((expiration_date - (%s AT TIME ZONE %s)::date))::int >= 0
            ORDER BY dte_diff ASC, expiration_date ASC
            LIMIT 1
            """,
            (
                snapshot_ts,
                str(underlying).upper(),
                snapshot_ts,
                tz_local,
                snapshot_ts,
                tz_local,
                int(target_dte_days),
                snapshot_ts,
                tz_local,
            ),
        )
        r = cur.fetchone()
        if not r:
            return None

        exp = r[0]
        dte_days = int(r[1]) if r[1] is not None else None
        dte_diff = int(r[2]) if r[2] is not None else None

        if exp is None or dte_days is None or dte_diff is None:
            return None

        if dte_diff > int(dte_tolerance_days):
            return None

        return PickedExpiration(expiration_date=exp, dte_days=int(dte_days), dte_diff=int(dte_diff))


def _current_week_friday_local_date(*, snapshot_ts: datetime, tz_local: str) -> date:
    tz = ZoneInfo(str(tz_local or "America/Chicago"))
    local_dt = snapshot_ts.astimezone(tz)
    # Monday=0 ... Friday=4
    days_to_friday = max(0, 4 - int(local_dt.weekday()))
    return (local_dt.date() + timedelta(days=days_to_friday))


def pick_current_week_friday_expiration(
    conn: psycopg.Connection,
    *,
    snapshot_ts: datetime,
    tz_local: str,
    underlying: str = "SPX",
) -> PickedExpiration | None:
    """Pick expiration that exactly matches current week's Friday (local).

    Returns None if that Friday expiration is not present in the chain snapshot.
    """
    friday = _current_week_friday_local_date(snapshot_ts=snapshot_ts, tz_local=tz_local)

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
              expiration_date,
              ((expiration_date - (%s AT TIME ZONE %s)::date))::int AS dte_days
            FROM spx.option_chain
            WHERE snapshot_ts = %s
              AND expiration_date = %s
              AND UPPER(underlying) = %s
            GROUP BY expiration_date
            LIMIT 1
            """,
            (snapshot_ts, tz_local, snapshot_ts, friday, str(underlying).upper()),
        )
        r = cur.fetchone()
        if not r:
            return None
        exp = r[0]
        dte_days = int(r[1]) if r[1] is not None else None
        if exp is None or dte_days is None:
            return None
        return PickedExpiration(expiration_date=exp, dte_days=int(dte_days), dte_diff=0)


def pick_expiration(
    conn: psycopg.Connection,
    *,
    snapshot_ts: datetime,
    expiration_mode: Literal["target_dte", "current_week_friday"] = "target_dte",
    target_dte_days: int,
    dte_tolerance_days: int,
    tz_local: str,
    underlying: str = "SPX",
) -> PickedExpiration | None:
    mode = str(expiration_mode or "target_dte").strip().lower()
    if mode == "current_week_friday":
        return pick_current_week_friday_expiration(conn, snapshot_ts=snapshot_ts, tz_local=tz_local, underlying=underlying)
    return pick_expiration_for_target_dte(
        conn,
        snapshot_ts=snapshot_ts,
        target_dte_days=target_dte_days,
        dte_tolerance_days=dte_tolerance_days,
        tz_local=tz_local,
        underlying=underlying,
    )


def term_bucket_name(*, target_dte_days: int, dte_tolerance_days: int) -> str:
    return f"term_dte{int(target_dte_days)}t{int(dte_tolerance_days)}"
