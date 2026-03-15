from __future__ import annotations


def ensure_execution_hardening_schema(con) -> None:
    """Postgres runtime already provisioned; keep function as no-op for compatibility."""
    _ = con
    return
