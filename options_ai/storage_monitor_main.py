from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from pathlib import Path

from options_ai.db_compat import connect_compat

try:
    import psycopg
except Exception:  # pragma: no cover
    psycopg = None  # type: ignore


def _now_minute_iso() -> str:
    return datetime.now(timezone.utc).replace(second=0, microsecond=0).isoformat()


def _db_path_from_database_url(database_url: str) -> str:
    d = (database_url or '').strip()
    if d.startswith('postgresql://') or d.startswith('postgres://'):
        return d
    if d.startswith('sqlite:///'):
        return d.replace('sqlite:///', '/', 1)
    if d.startswith('sqlite:////'):
        return d.replace('sqlite://', '', 1)
    raise RuntimeError('DATABASE_URL must be sqlite:///... or postgres://...')


def _pg_size(dsn: str) -> tuple[int | None, str | None]:
    if not dsn or psycopg is None:
        return None, None
    try:
        with psycopg.connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT current_database()')
                rn = cur.fetchone()
                dbn = str(rn[0]) if rn and rn[0] is not None else None
                cur.execute('SELECT pg_database_size(current_database())')
                rr = cur.fetchone()
                return (int(rr[0] or 0) if rr else 0), dbn
    except Exception:
        return None, None


def _ensure_table(con) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS storage_metrics_samples (
          id BIGSERIAL PRIMARY KEY,
          sample_minute_utc TEXT NOT NULL UNIQUE,
          postgres_bytes BIGINT,
          timescale_bytes BIGINT,
          disk_used_bytes BIGINT,
          disk_free_bytes BIGINT,
          postgres_db_name TEXT,
          timescale_db_name TEXT
        )
        """
    )
    con.execute("CREATE INDEX IF NOT EXISTS idx_storage_metrics_samples_minute ON storage_metrics_samples(sample_minute_utc DESC)")
    con.commit()


def main() -> None:

    database_url = os.getenv('DATABASE_URL', '').strip()
    db_path = _db_path_from_database_url(database_url)

    data_root = Path(os.getenv('OPTIONS_AI_DATA_ROOT', '/mnt/options_ai'))
    poll_s = max(15, int(os.getenv('STORAGE_MONITOR_POLL_SECONDS', '60') or '60'))

    postgres_dsn = os.getenv('POSTGRES_DATABASE_URL', '').strip() or os.getenv('PRIMARY_POSTGRES_DATABASE_URL', '').strip()
    if not postgres_dsn and database_url.startswith('postgres'):
        postgres_dsn = database_url

    timescale_dsn = os.getenv('TIMESCALE_DATABASE_URL', '').strip() or os.getenv('SPX_CHAIN_DATABASE_URL', '').strip()

    while True:
        sample_ts = _now_minute_iso()
        pg_bytes, pg_name = _pg_size(postgres_dsn)
        ts_bytes, ts_name = _pg_size(timescale_dsn)

        if pg_bytes is None and not postgres_dsn:
            try:
                pg_bytes = int(Path(str(db_path)).stat().st_size)
            except Exception:
                pg_bytes = None

        used_b = free_b = None
        try:
            st = os.statvfs(str(data_root))
            total_b = int(st.f_frsize * st.f_blocks)
            free_b = int(st.f_frsize * st.f_bavail)
            used_b = int(total_b - free_b)
        except Exception:
            pass

        with connect_compat(str(db_path), timeout=30.0) as con:
            _ensure_table(con)
            con.execute(
                """
                INSERT INTO storage_metrics_samples(
                  sample_minute_utc, postgres_bytes, timescale_bytes,
                  disk_used_bytes, disk_free_bytes, postgres_db_name, timescale_db_name
                ) VALUES(?,?,?,?,?,?,?)
                ON CONFLICT (sample_minute_utc)
                DO UPDATE SET
                  postgres_bytes=EXCLUDED.postgres_bytes,
                  timescale_bytes=EXCLUDED.timescale_bytes,
                  disk_used_bytes=EXCLUDED.disk_used_bytes,
                  disk_free_bytes=EXCLUDED.disk_free_bytes,
                  postgres_db_name=EXCLUDED.postgres_db_name,
                  timescale_db_name=EXCLUDED.timescale_db_name
                """,
                (sample_ts, pg_bytes, ts_bytes, used_b, free_b, pg_name, ts_name),
            )
            con.commit()

        time.sleep(poll_s)


if __name__ == '__main__':
    main()
