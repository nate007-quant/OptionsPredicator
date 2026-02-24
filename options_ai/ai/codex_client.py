from __future__ import annotations

import base64
import json
import time
from pathlib import Path
from typing import Any

from openai import OpenAI

from options_ai.ai.oauth import OAuthTokenManager


def _response_text(resp: Any) -> str:
    if resp is None:
        return ""
    if hasattr(resp, "output_text") and isinstance(resp.output_text, str):
        return resp.output_text
    try:
        chunks = []
        for item in resp.output:
            for c in getattr(item, "content", []) or []:
                if getattr(c, "type", None) in {"output_text", "text"} and getattr(c, "text", None):
                    chunks.append(c.text)
        if chunks:
            return "\n".join(chunks)
    except Exception:
        pass
    try:
        return resp.choices[0].message.content
    except Exception:
        return str(resp)


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

        # openai-python currently takes timeout via httpx internally; keep simple.
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

        t0 = time.time()
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
        latency_ms = int((time.time() - t0) * 1000)
        text = _response_text(resp).strip()
        report = {"raw": getattr(resp, "model_dump", lambda: {})()} if hasattr(resp, "model_dump") else {"raw": str(resp)}
        report["latency_ms"] = latency_ms
        report["output_chars"] = len(text)
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

        t0 = time.time()
        resp = client.responses.create(
            model=self.model,
            input=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            **kwargs,
        )
        latency_ms = int((time.time() - t0) * 1000)
        text = _response_text(resp).strip()
        report = {"raw": getattr(resp, "model_dump", lambda: {})()} if hasattr(resp, "model_dump") else {"raw": str(resp)}
        report["latency_ms"] = latency_ms
        report["output_chars"] = len(text)
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
