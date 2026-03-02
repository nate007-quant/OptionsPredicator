from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

import psycopg


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


def term_bucket_name(*, target_dte_days: int, dte_tolerance_days: int) -> str:
    return f"term_dte{int(target_dte_days)}t{int(dte_tolerance_days)}"
