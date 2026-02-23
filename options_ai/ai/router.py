from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from options_ai.ai.codex_client import CodexClient
from options_ai.ai.oauth import OAuthConfig, OAuthTokenManager, OAuthUnavailable
from options_ai.ai.throttle import RateLimiter, ThrottledCodexClient
from options_ai.config import Config


Provider = Literal["remote", "local"]


@dataclass(frozen=True)
class RoutedClient:
    provider: Provider
    model_used: str
    routing_reason: str
    client: CodexClient


class ModelRouter:
    def __init__(self, cfg: Config, *, bootstrap_rate_limiter: RateLimiter | None = None):
        self.cfg = cfg
        self.bootstrap_rate_limiter = bootstrap_rate_limiter

        oauth_cfg = OAuthConfig(
            client_id=cfg.oauth_client_id,
            client_secret=cfg.oauth_client_secret,
            token_url=cfg.oauth_token_url,
            scope=cfg.oauth_scope,
            audience=cfg.oauth_audience or None,
            refresh_margin_seconds=int(cfg.oauth_refresh_margin_seconds or 60),
            cache_path=str(cfg.oauth_cache_path),
        )
        self.oauth_manager = OAuthTokenManager(oauth_cfg)

        # Local client is always constructible; availability depends on LOCAL_MODEL_ENABLED.
        self.local_client = CodexClient(
            model=cfg.local_model_name,
            base_url=cfg.local_model_endpoint,
            static_api_key="local",
            timeout_seconds=int(cfg.local_model_timeout_seconds or 60),
        )

        self.remote_client = CodexClient(
            model=cfg.remote_model_name,
            token_manager=self.oauth_manager,
            timeout_seconds=60,
        )

    def _remote_client_for_mode(self, *, bootstrap_mode: bool) -> CodexClient:
        if bootstrap_mode and self.bootstrap_rate_limiter is not None:
            return ThrottledCodexClient(
                token_manager=self.oauth_manager,
                model=self.cfg.remote_model_name,
                limiter=self.bootstrap_rate_limiter,
            )
        return self.remote_client

    def oauth_configured(self) -> bool:
        return self.oauth_manager.is_configured()

    def local_enabled(self) -> bool:
        return bool(self.cfg.local_model_enabled)

    def candidates(self, *, bootstrap_mode: bool) -> list[RoutedClient]:
        """Return ordered candidates per v2.2 routing rules."""

        local_ok = self.local_enabled()
        oauth_cfg_ok = self.oauth_configured()
        remote_client = self._remote_client_for_mode(bootstrap_mode=bootstrap_mode)

        if self.cfg.model_force_local:
            return (
                [
                    RoutedClient(
                        provider="local",
                        model_used=self.cfg.local_model_name,
                        routing_reason="MODEL_FORCE_LOCAL",
                        client=self.local_client,
                    )
                ]
                if local_ok
                else []
            )

        if self.cfg.model_force_remote:
            return [
                RoutedClient(
                    provider="remote",
                    model_used=self.cfg.remote_model_name,
                    routing_reason="MODEL_FORCE_REMOTE",
                    client=remote_client,
                )
            ]

        # Prefer remote if OAuth works right now.
        if oauth_cfg_ok:
            try:
                self.oauth_manager.get_access_token()
                candidates: list[RoutedClient] = [
                    RoutedClient(
                        provider="remote",
                        model_used=self.cfg.remote_model_name,
                        routing_reason="oauth_ok",
                        client=remote_client,
                    )
                ]
                if local_ok:
                    candidates.append(
                        RoutedClient(
                            provider="local",
                            model_used=self.cfg.local_model_name,
                            routing_reason="fallback_local",
                            client=self.local_client,
                        )
                    )
                return candidates
            except OAuthUnavailable:
                pass

        # No OAuth or OAuth failed => local
        candidates: list[RoutedClient] = []
        if local_ok:
            candidates.append(
                RoutedClient(
                    provider="local",
                    model_used=self.cfg.local_model_name,
                    routing_reason="oauth_unavailable",
                    client=self.local_client,
                )
            )
        if oauth_cfg_ok:
            candidates.append(
                RoutedClient(
                    provider="remote",
                    model_used=self.cfg.remote_model_name,
                    routing_reason="fallback_remote",
                    client=remote_client,
                )
            )
        return candidates


def try_models(
    candidates: list[RoutedClient],
    *,
    fn_name: str,
    fn: Any,
    local_max_retries: int = 0,
) -> tuple[Any, dict[str, Any], str, str, str]:
    """Try model candidates in order.

    Returns: (value, report, model_used, model_provider, routing_reason)
    """

    last_err: str | None = None
    for c in candidates:
        retries = 0
        if c.provider == "local" and local_max_retries and local_max_retries > 0:
            retries = int(local_max_retries)

        attempts = 1 + retries
        for _ in range(attempts):
            try:
                val, rep = fn(c.client)
                rep = dict(rep or {})
                rep["model_used"] = c.model_used
                rep["model_provider"] = c.provider
                rep["routing_reason"] = c.routing_reason
                rep["fn"] = fn_name
                return val, rep, c.model_used, c.provider, c.routing_reason
            except Exception as e:
                last_err = str(e)
                continue

    raise RuntimeError(f"all model providers failed for {fn_name}: {last_err}")
