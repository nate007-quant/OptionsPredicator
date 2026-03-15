from __future__ import annotations

from typing import Any
import threading
import time

try:
    import psycopg
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover
    psycopg = None  # type: ignore
    dict_row = None  # type: ignore

_PG_CONN_MAX = 16
_PG_CONN_SEM = threading.BoundedSemaphore(_PG_CONN_MAX)


class FlexRow(dict):
    """dict row with sqlite-like int indexing support."""

    def __getitem__(self, key: Any) -> Any:  # type: ignore[override]
        if isinstance(key, int):
            vals = list(self.values())
            return vals[key]
        return super().__getitem__(key)


class PgCursorCompat:
    def __init__(self, cur: Any, owner: "PgConnCompat"):
        self._cur = cur
        self._owner = owner

    @staticmethod
    def _q(sql: str) -> str:
        return str(sql).replace("?", "%s")

    def execute(self, sql: str, params: tuple[Any, ...] | list[Any] | None = None):
        self._cur.execute(self._q(sql), tuple(params or ()))
        try:
            rc = int(getattr(self._cur, 'rowcount', 0) or 0)
            if rc > 0:
                self._owner.total_changes += rc
        except Exception:
            pass
        return self

    def executemany(self, sql: str, seq: list[tuple[Any, ...]]):
        self._cur.executemany(self._q(sql), seq)
        try:
            rc = int(getattr(self._cur, 'rowcount', 0) or 0)
            if rc > 0:
                self._owner.total_changes += rc
        except Exception:
            pass
        return self

    @property
    def lastrowid(self):
        try:
            c2 = self._owner._con.cursor()
            c2.execute('SELECT LASTVAL()')
            r = c2.fetchone()
            if isinstance(r, dict):
                return r.get('lastval') or next(iter(r.values()))
            return r[0]
        except Exception:
            return None

    def fetchone(self):
        r = self._cur.fetchone()
        if r is None:
            return None
        if isinstance(r, dict):
            return FlexRow(r)
        return r

    def fetchall(self):
        rows = self._cur.fetchall()
        out = []
        for r in rows:
            if isinstance(r, dict):
                out.append(FlexRow(r))
            else:
                out.append(r)
        return out


class PgConnCompat:
    def __init__(self, dsn: str):
        if psycopg is None:
            raise RuntimeError("psycopg not installed")
        self._sem_acquired = False
        got = _PG_CONN_SEM.acquire(timeout=8.0)
        if not got:
            raise RuntimeError('postgres connection gate busy (too many concurrent requests)')
        self._sem_acquired = True
        last_err = None
        for delay in (0.0, 0.15, 0.35, 0.7):
            if delay > 0:
                time.sleep(delay)
            try:
                self._con = psycopg.connect(dsn, row_factory=dict_row)
                break
            except Exception as e:
                last_err = e
        else:
            if self._sem_acquired:
                _PG_CONN_SEM.release()
                self._sem_acquired = False
            raise last_err if last_err else RuntimeError('failed to connect postgres')
        self.row_factory = FlexRow
        self.total_changes = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            if exc_type is None:
                self._con.commit()
            else:
                self._con.rollback()
        finally:
            try:
                self._con.close()
            finally:
                if self._sem_acquired:
                    _PG_CONN_SEM.release()
                    self._sem_acquired = False

    def execute(self, sql: str, params: tuple[Any, ...] | list[Any] | None = None):
        cur = self._con.cursor()
        cc = PgCursorCompat(cur, self)
        return cc.execute(sql, params)

    def executescript(self, script: str):
        cur = self._con.cursor()
        parts = [x.strip() for x in str(script or '').split(';') if x.strip()]
        for stmt in parts:
            cur.execute(stmt)
        return self

    def commit(self):
        self._con.commit()

    def close(self):
        try:
            self._con.close()
        finally:
            if getattr(self, '_sem_acquired', False):
                _PG_CONN_SEM.release()
                self._sem_acquired = False


def connect_compat(db_dsn_or_path: str, *, timeout: float = 5.0):
    _ = timeout
    target = str(db_dsn_or_path or "").strip()
    if not (target.startswith("postgresql://") or target.startswith("postgres://")):
        raise RuntimeError(f"Postgres DSN required, got: {target!r}")
    return PgConnCompat(target)
