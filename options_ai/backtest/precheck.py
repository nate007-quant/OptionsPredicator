from __future__ import annotations

from dataclasses import dataclass
from datetime import date, time

try:
    import psycopg
except Exception:  # pragma: no cover
    psycopg = None  # type: ignore

from options_ai.backtest.debit_spreads import DebitBacktestConfig


@dataclass(frozen=True)
class PrecheckResult:
    ok: bool
    reason: str | None
    sampled_days: int
    sampled_snapshots: int
    est_candidates: int
    scores_available_for_horizon: bool


def _sample_days(start_day: date, end_day: date, max_days: int) -> list[date]:
    if max_days <= 0:
        return []
    days: list[date] = []
    cur = start_day
    while cur <= end_day:
        days.append(cur)
        cur = date.fromordinal(cur.toordinal() + 1)

    if len(days) <= max_days:
        return days

    # evenly spaced sample
    if max_days == 1:
        return [days[len(days) // 2]]

    idxs = [int(round(i * (len(days) - 1) / float(max_days - 1))) for i in range(max_days)]
    out: list[date] = []
    seen = set()
    for ix in idxs:
        ix = max(0, min(ix, len(days) - 1))
        if ix in seen:
            continue
        seen.add(ix)
        out.append(days[ix])
    return out


def precheck_candidates_0dte(
    conn: "psycopg.Connection",
    *,
    cfg: DebitBacktestConfig,
    sample_days: int = 5,
    snapshots_per_day: int = 3,
    min_est_candidates: int = 1,
) -> PrecheckResult:
    """Cheap viability precheck for 0DTE candidates.

    This precheck is intentionally conservative: it only rejects when it is very likely that
    the configuration will yield 0 trades.

    - samples a few days in range
    - samples a few snapshots per day from the entry window
    - counts candidate rows that match basic constraints
    """

    if psycopg is None:
        return PrecheckResult(True, None, 0, 0, 0, False)

    if cfg.expiration_mode != "0dte":
        return PrecheckResult(True, None, 0, 0, 0, True)

    # Scores availability for horizon
    scores_available = False
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM spx.debit_spread_scores_0dte WHERE horizon_minutes=%s LIMIT 1",
                (int(cfg.horizon_minutes),),
            )
            scores_available = cur.fetchone() is not None
    except Exception:
        scores_available = False

    # entry window
    def _parse_hhmm(v: str) -> tuple[int, int]:
        hh, mm = v.strip().split(":")
        return int(hh), int(mm)

    if cfg.entry_mode == "first_n_minutes":
        sh, sm = _parse_hhmm(cfg.session_start_ct)
        start_t = time(hour=sh, minute=sm)
        total = sh * 60 + sm + int(cfg.entry_first_n_minutes)
        end_t = time(hour=(total // 60) % 24, minute=(total % 60))
    else:
        h1, m1 = _parse_hhmm(cfg.entry_start_ct)
        h2, m2 = _parse_hhmm(cfg.entry_end_ct)
        start_t = time(hour=h1, minute=m1)
        end_t = time(hour=h2, minute=m2)

    days = _sample_days(cfg.start_day, cfg.end_day, int(sample_days))
    if not days:
        return PrecheckResult(False, "no_days", 0, 0, 0, scores_available)

    # Import here to avoid circular imports at module import time
    from options_ai.backtest.debit_spreads import _anchor_mode_allowed, _fetch_snapshots_in_window

    snaps = []
    for d in days:
        ss = _fetch_snapshots_in_window(conn, day_local=d, start_t=start_t, end_t=end_t, tz_local=cfg.tz_local)
        if not ss:
            continue
        picks = []
        picks.append(ss[0])
        if snapshots_per_day > 1 and len(ss) > 2:
            picks.append(ss[len(ss) // 2])
        if snapshots_per_day > 2 and len(ss) > 1:
            picks.append(ss[-1])
        snaps.extend(picks[: int(snapshots_per_day)])

    # unique
    uniq_snaps = list(dict.fromkeys(snaps))

    if not uniq_snaps:
        return PrecheckResult(False, "no_snapshots_in_entry_window", len(days), 0, 0, scores_available)

    allowed_anchors = _anchor_mode_allowed(cfg.anchor_mode)

    use_p = float(cfg.min_p_bigwin) > 0.0
    use_pred = float(cfg.min_pred_change) > 0.0

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*)
            FROM spx.debit_spread_candidates_0dte c
            JOIN spx.chain_features_0dte f
              ON f.snapshot_ts = c.snapshot_ts
            LEFT JOIN spx.debit_spread_scores_0dte s
              ON s.snapshot_ts = c.snapshot_ts
             AND s.horizon_minutes = %s
             AND s.anchor_type = c.anchor_type
             AND s.spread_type = c.spread_type
            WHERE c.snapshot_ts = ANY(%s)
              AND c.tradable = true
              AND f.low_quality = false
              AND c.debit_points > 0
              AND c.debit_points <= %s
              AND c.anchor_type = ANY(%s)
              AND c.spread_type = ANY(%s)
              AND (%s = false OR (s.p_bigwin IS NOT NULL AND s.p_bigwin >= %s))
              AND (%s = false OR (s.pred_change IS NOT NULL AND s.pred_change >= %s))
            """,
            (
                int(cfg.horizon_minutes),
                uniq_snaps,
                float(cfg.max_debit_points),
                allowed_anchors,
                list(cfg.allowed_spreads),
                bool(use_p),
                float(cfg.min_p_bigwin),
                bool(use_pred),
                float(cfg.min_pred_change),
            ),
        )
        est = int(cur.fetchone()[0] or 0)

    if est < int(min_est_candidates):
        return PrecheckResult(
            False,
            "no_candidates_estimate",
            sampled_days=len(days),
            sampled_snapshots=len(uniq_snaps),
            est_candidates=est,
            scores_available_for_horizon=scores_available,
        )

    return PrecheckResult(True, None, len(days), len(uniq_snaps), est, scores_available)
