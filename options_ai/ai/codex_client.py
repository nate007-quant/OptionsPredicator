from __future__ import annotations

import base64
import json
from pathlib import Path
from typing import Any

from openai import OpenAI

from options_ai.ai.oauth import OAuthTokenManager


def _response_text(resp: Any) -> str:
    """Best-effort extraction of plain text across OpenAI/compat response shapes."""

    if resp is None:
        return ""

    # New "responses" API
    if hasattr(resp, "output_text") and isinstance(resp.output_text, str):
        return resp.output_text

    try:
        chunks: list[str] = []
        for item in getattr(resp, "output", []) or []:
            for c in getattr(item, "content", []) or []:
                c_type = getattr(c, "type", None)
                c_text = getattr(c, "text", None)
                if c_type in {"output_text", "text"} and c_text:
                    chunks.append(c_text)
        if chunks:
            return "\n".join(chunks)
    except Exception:
        pass

    # ChatCompletions shape
    try:
        return resp.choices[0].message.content
    except Exception:
        return "" if resp is None else str(resp)


class CodexClient:
    """OpenAI-compatible client wrapper.

    Supports:
      - Remote OAuth (token_manager provided)
      - Local OpenAI-compatible endpoints (base_url provided)

    For local endpoints, api_key is a dummy value (no auth).
    """

    def __init__(
        self,
        *,
        model: str,
        token_manager: OAuthTokenManager | None = None,
        base_url: str | None = None,
        static_api_key: str | None = None,
        timeout_seconds: int | None = None,
    ):
        self.model = model
        self.token_manager = token_manager
        self.base_url = base_url
        self.static_api_key = static_api_key
        self.timeout_seconds = timeout_seconds

    def _client(self) -> OpenAI:
        api_key = self.static_api_key
        if self.token_manager is not None:
            api_key = self.token_manager.get_access_token()
        if not api_key:
            # openai lib requires a non-empty key string; for unauth local endpoints use a dummy.
            api_key = "local"

        kwargs: dict[str, Any] = {"api_key": api_key}
        if self.base_url:
            kwargs["base_url"] = self.base_url
        if self.timeout_seconds is not None:
            # openai-python supports float seconds or httpx.Timeout.
            kwargs["timeout"] = float(self.timeout_seconds)

        return OpenAI(**kwargs)

    def extract_chart_description(
        self,
        png_path: str,
        system_prompt: str,
        user_prompt: str,
        *,
        max_output_tokens: int | None = None,
    ) -> tuple[str, dict[str, Any]]:
        img_bytes = Path(png_path).read_bytes()
        b64 = base64.b64encode(img_bytes).decode("ascii")
        data_url = f"data:image/png;base64,{b64}"

        client = self._client()
        kwargs: dict[str, Any] = {}
        if max_output_tokens is not None:
            kwargs["max_output_tokens"] = int(max_output_tokens)

        resp = client.responses.create(
            model=self.model,
            input=[
                {"role": "system", "content": system_prompt},
                {
                    "role": "user",
                    "content": [
                        {"type": "input_text", "text": user_prompt},
                        {"type": "input_image", "image_url": data_url},
                    ],
                },
            ],
            **kwargs,
        )
        text = _response_text(resp).strip()
        report = {
            "method": "responses",
            "raw": getattr(resp, "model_dump", lambda: {})() if hasattr(resp, "model_dump") else str(resp),
        }
        return text, report

    def generate_prediction(
        self,
        system_prompt: str,
        user_prompt: str,
        *,
        max_output_tokens: int | None = None,
    ) -> tuple[str, dict[str, Any]]:
        client = self._client()
        kwargs: dict[str, Any] = {}
        if max_output_tokens is not None:
            kwargs["max_output_tokens"] = int(max_output_tokens)

        resp = client.responses.create(
            model=self.model,
            input=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            **kwargs,
        )
        text = _response_text(resp).strip()
        report: dict[str, Any] = {
            "method": "responses",
            "raw": getattr(resp, "model_dump", lambda: {})() if hasattr(resp, "model_dump") else str(resp),
        }

        # Some OpenAI-compatible local endpoints implement /v1/chat/completions but not /v1/responses.
        # In that case the SDK may return an object that parses but has no text content. Fallback.
        if not text and self.base_url:
            chat_kwargs: dict[str, Any] = {}
            if max_output_tokens is not None:
                chat_kwargs["max_tokens"] = int(max_output_tokens)

            chat_resp = client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                **chat_kwargs,
            )
            text = _response_text(chat_resp).strip()
            report["method"] = "chat_completions_fallback"
            report["fallback_raw"] = (
                getattr(chat_resp, "model_dump", lambda: {})() if hasattr(chat_resp, "model_dump") else str(chat_resp)
            )

        return text, report


def safe_json_loads(s: str) -> Any:
    """Strict JSON loader."""

    s = (s or "").strip()
    return json.loads(s)


def safe_json_loads_tolerant(s: str) -> Any:
    """Tolerant JSON extraction for models that may emit extra text.

    Strategy:
      - Strip whitespace
      - If direct json.loads works, return
      - Otherwise, take substring from first '{' to last '}' and try again

    We intentionally do NOT attempt lossy repairs (e.g. trailing commas), because we
    want to avoid silent corruption.
    """

    s = (s or "").strip()
    if not s:
        raise json.JSONDecodeError("empty", s, 0)

    try:
        return json.loads(s)
    except json.JSONDecodeError:
        pass

    start = s.find("{")
    end = s.rfind("}")
    if start == -1 or end == -1 or end <= start:
        raise json.JSONDecodeError("no_json_object_found", s, 0)

    sub = s[start : end + 1].strip()
    return json.loads(sub)
