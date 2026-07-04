"""Tiny Ollama API client for the MSH AI explainer."""

from __future__ import annotations

import json
import os
from urllib import request

DEFAULT_MODEL = os.environ.get("MSH_AI_MODEL", "qwen2.5-coder:7b")
DEFAULT_BASE_URL = os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434")


class OllamaError(RuntimeError):
    """Raised when the local Ollama API call fails."""


def chat(prompt: str, system_prompt: str, model: str = DEFAULT_MODEL, base_url: str = DEFAULT_BASE_URL) -> str:
    """Send a non-streaming chat request to Ollama."""
    payload = {
        "model": model,
        "stream": False,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt},
        ],
    }
    data = json.dumps(payload).encode("utf-8")
    req = request.Request(
        f"{base_url.rstrip('/')}/api/chat",
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=120) as response:
            result = json.loads(response.read().decode("utf-8"))
    except Exception as exc:  # pragma: no cover - depends on local Ollama runtime
        raise OllamaError(f"Ollama request failed: {exc}") from exc

    message = result.get("message", {})
    content = message.get("content")
    if not content:
        raise OllamaError(f"Ollama response did not include message content: {result}")
    return content.strip()
