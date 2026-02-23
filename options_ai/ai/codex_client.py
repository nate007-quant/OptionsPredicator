from __future__ import annotations

import base64
import json
from pathlib import Path
from typing import Any

from openai import OpenAI


def _response_text(resp: Any) -> str:
    # openai-python has changed shapes a few times; try common access patterns.
    if resp is None:
        return ""
    if hasattr(resp, "output_text") and isinstance(resp.output_text, str):
        return resp.output_text
    # responses API
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
    # chat completions fallback
    try:
        return resp.choices[0].message.content
    except Exception:
        return str(resp)


class CodexClient:
    def __init__(self, api_key: str, model: str):
        if not api_key:
            raise RuntimeError("OPENAI_API_KEY is required for Codex calls")
        self.client = OpenAI(api_key=api_key)
        self.model = model

    def extract_chart_description(self, png_path: str, system_prompt: str, user_prompt: str) -> tuple[str, dict[str, Any]]:
        img_bytes = Path(png_path).read_bytes()
        b64 = base64.b64encode(img_bytes).decode("ascii")
        data_url = f"data:image/png;base64,{b64}"

        resp = self.client.responses.create(
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
        )
        text = _response_text(resp).strip()
        report = {"raw": getattr(resp, "model_dump", lambda: {} )()} if hasattr(resp, "model_dump") else {"raw": str(resp)}
        return text, report

    def generate_prediction(self, system_prompt: str, user_prompt: str) -> tuple[str, dict[str, Any]]:
        resp = self.client.responses.create(
            model=self.model,
            input=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        )
        text = _response_text(resp).strip()
        report = {"raw": getattr(resp, "model_dump", lambda: {} )()} if hasattr(resp, "model_dump") else {"raw": str(resp)}
        return text, report


def safe_json_loads(s: str) -> Any:
    # strict JSON only; no markdown. Attempt to isolate first JSON object if extra whitespace.
    s = s.strip()
    return json.loads(s)
