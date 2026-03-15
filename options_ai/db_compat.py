from __future__ import annotations

import sqlite3
from typing import Any

try:
    import psycopg
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover
    psycopg = None  # type: ignore
    dict_row = None  # type: ignore


class FlexRow(dict):
    """dict row with sqlite-like int indexing support."""

    def __getitem__(self, key: Any) -> Any:  # type: ignore[override]
        if isinstance(key, int):
            try:
                return list(self.values())[key]
            except Exception as e:
                raise KeyError(key) from e
        return super().__getitem__(key)


class PgCursorCompat:
    def __init__(self, cur: Any):
        self._cur = cur

    @staticmethod
    def _q(sql: str) -> str:
        return str(sql).replace("?", "%s")

    def execute(self, sql: str, params: tuple[Any, ...] | list[Any] | None = None):
        self._cur.execute(self._q(sql), tuple(params or ()))
        return self

    def executemany(self, sql: str, seq: list[tuple[Any, ...]]):
        self._cur.executemany(self._q(sql), seq)
        return self

    @property
    def lastrowid(self):
        try:
            return self._cur.fetchone()[0]
        except Exception:
            return None

    def fetchone(self):
        r = self._cur.fetchone()
        if r is None:
            return None
        if isinstance(r, dict):
            return FlexRow(r)
        if isinstance(r, tuple):
            return r
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
    def __init__(self, dsn: str, timeout: float = 5.0):
        if psycopg is None:
            raise RuntimeError("psycopg not installed")
        self._con = psycopg.connect(dsn, row_factory=dict_row)
        self.row_factory = FlexRow

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            if exc_type is None:
                self._con.commit()
            else:
                self._con.rollback()
        finally:
            self._con.close()

    def execute(self, sql: str, params: tuple[Any, ...] | list[Any] | None = None):
        cur = self._con.cursor()
        cc = PgCursorCompat(cur)
        return cc.execute(sql, params)

    def commit(self):
        self._con.commit()

    def close(self):
        self._con.close()


def connect_compat(db_dsn_or_path: str, *, timeout: float = 5.0):
    target = str(db_dsn_or_path or "").strip()
    if target.startswith("postgresql://") or target.startswith("postgres://"):
        return PgConnCompat(target, timeout=timeout)

    con = sqlite3.connect(target, timeout=float(timeout))
    con.row_factory = sqlite3.Row
    try:
        con.execute("PRAGMA journal_mode=WAL;")
    except Exception:
        pass
    try:
        con.execute("PRAGMA busy_timeout=5000;")
    except Exception:
        pass
    return con
