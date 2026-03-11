from __future__ import annotations

import os
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path

try:
    import psycopg
except Exception:  # pragma: no cover
    psycopg = None  # type: ignore


def _now_minute_iso() -> str:
    return datetime.now(timezone.utc).replace(second=0, microsecond=0).isoformat()


def _db_path_from_database_url(database_url: str) -> Path:
    d = (database_url or '').strip()
    if d.startswith('sqlite:///'):
        return Path(d.replace('sqlite:///', '/', 1))
    if d.startswith('sqlite:////'):
        return Path(d.replace('sqlite://', '', 1))
    raise RuntimeError('DATABASE_URL must be sqlite:///... for storage monitor local state db')


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


def _ensure_table(con: sqlite3.Connection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS storage_metrics_samples (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          sample_minute_utc TEXT NOT NULL UNIQUE,
          postgres_bytes INTEGER,
          timescale_bytes INTEGER,
          disk_used_bytes INTEGER,
          disk_free_bytes INTEGER,
          postgres_db_name TEXT,
          timescale_db_name TEXT
        )
        """
    )
    con.execute("CREATE INDEX IF NOT EXISTS idx_storage_metrics_samples_minute ON storage_metrics_samples(sample_minute_utc DESC)")
    cols = {str(r[1]) for r in con.execute('PRAGMA table_info(storage_metrics_samples)').fetchall()}
    if 'postgres_db_name' not in cols:
        con.execute('ALTER TABLE storage_metrics_samples ADD COLUMN postgres_db_name TEXT')
    if 'timescale_db_name' not in cols:
        con.execute('ALTER TABLE storage_metrics_samples ADD COLUMN timescale_db_name TEXT')
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
                pg_bytes = int(db_path.stat().st_size)
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

        with sqlite3.connect(str(db_path)) as con:
            _ensure_table(con)
            con.execute(
                """
                INSERT OR REPLACE INTO storage_metrics_samples(
                  sample_minute_utc, postgres_bytes, timescale_bytes,
                  disk_used_bytes, disk_free_bytes, postgres_db_name, timescale_db_name
                ) VALUES(?,?,?,?,?,?,?)
                """,
                (sample_ts, pg_bytes, ts_bytes, used_b, free_b, pg_name, ts_name),
            )
            con.commit()

        time.sleep(poll_s)


if __name__ == '__main__':
    main()
