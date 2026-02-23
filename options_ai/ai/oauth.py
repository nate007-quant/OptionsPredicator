from __future__ import annotations

import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx

from options_ai.utils.cache import save_json_atomic, load_json


class OAuthUnavailable(RuntimeError):
    pass


@dataclass(frozen=True)
class OAuthConfig:
    client_id: str
    client_secret: str
    token_url: str
    scope: str
    audience: str | None = None

    refresh_margin_seconds: int = 60
    cache_path: str = "/mnt/options_ai/state/oauth_token.json"

    max_retries: int = 5
    disable_seconds_after_failure: int = 60


class OAuthTokenManager:
    """Client-credentials OAuth token fetcher with disk cache.

    - Never logs tokens.
    - Caches token to disk.
    - Refreshes when within refresh_margin_seconds.
    - After repeated failures, becomes temporarily unavailable (disabled_until).
    """

    def __init__(self, cfg: OAuthConfig):
        self.cfg = cfg
        self._disabled_until: float | None = None
        self._last_error_ts: float | None = None

    def is_configured(self) -> bool:
        return bool(self.cfg.client_id and self.cfg.client_secret and self.cfg.token_url and self.cfg.scope)

    def _now(self) -> float:
        return time.time()

    def _cache_path(self) -> Path:
        return Path(self.cfg.cache_path)

    def _load_cache(self) -> dict[str, Any] | None:
        p = self._cache_path()
        if not p.exists():
            return None
        try:
            obj = load_json(p)
            return obj if isinstance(obj, dict) else None
        except Exception:
            return None

    def _save_cache(self, access_token: str, expires_at: float) -> None:
        p = self._cache_path()
        payload = {
            "access_token": access_token,
            "expires_at": float(expires_at),
        }
        save_json_atomic(p, payload)
        # Best-effort permission hardening
        try:
            os.chmod(p, 0o600)
        except Exception:
            pass

    def _cache_valid(self, obj: dict[str, Any]) -> bool:
        try:
            token = obj.get("access_token")
            exp = float(obj.get("expires_at"))
            if not token or exp <= 0:
                return False
            return self._now() < (exp - float(self.cfg.refresh_margin_seconds))
        except Exception:
            return False

    def get_access_token(self) -> str:
        if not self.is_configured():
            raise OAuthUnavailable("OAuth not configured (missing client_id/client_secret/token_url/scope)")

        now = self._now()
        if self._disabled_until is not None and now < self._disabled_until:
            raise OAuthUnavailable("OAuth temporarily disabled after repeated token failures")

        cached = self._load_cache()
        if cached and self._cache_valid(cached):
            return str(cached["access_token"])

        # Request new token with retries/backoff.
        delay = 1.0
        last_err: str | None = None
        for attempt in range(1, int(self.cfg.max_retries) + 1):
            try:
                token, exp = self._request_token()
                self._save_cache(token, exp)
                self._disabled_until = None
                return token
            except Exception as e:
                last_err = str(e)
                # exponential backoff
                if attempt < int(self.cfg.max_retries):
                    time.sleep(delay)
                    delay = min(delay * 2.0, 16.0)

        # Failure: pause model usage for a while
        self._disabled_until = self._now() + float(self.cfg.disable_seconds_after_failure)
        self._last_error_ts = self._now()
        raise OAuthUnavailable(f"OAuth token request failed after retries: {last_err}")

    def _request_token(self) -> tuple[str, float]:
        # TLS required by spec: enforce https
        if not self.cfg.token_url.lower().startswith("https://"):
            raise OAuthUnavailable("OAUTH_TOKEN_URL must be https")

        data: dict[str, str] = {
            "grant_type": "client_credentials",
            "client_id": self.cfg.client_id,
            "client_secret": self.cfg.client_secret,
            "scope": self.cfg.scope,
        }
        if self.cfg.audience:
            data["audience"] = self.cfg.audience

        with httpx.Client(timeout=20.0) as client:
            resp = client.post(self.cfg.token_url, data=data)

        if resp.status_code >= 400:
            raise OAuthUnavailable(f"token endpoint HTTP {resp.status_code}")

        try:
            payload = resp.json()
        except Exception:
            raise OAuthUnavailable("token endpoint returned non-JSON")

        token = payload.get("access_token")
        if not token:
            raise OAuthUnavailable("token response missing access_token")

        # expires_in preferred; fallback to expires_at
        now = self._now()
        if payload.get("expires_in") is not None:
            try:
                exp = now + float(payload["expires_in"])
            except Exception:
                exp = now + 3600.0
        elif payload.get("expires_at") is not None:
            exp = float(payload["expires_at"])
        else:
            exp = now + 3600.0

        return str(token), float(exp)
