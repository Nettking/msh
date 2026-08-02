"""Tiny private Ollama API client for MSH language-model providers."""

from __future__ import annotations

import json
import os
from urllib import error, request

DEFAULT_MODEL = os.environ.get("MSH_AI_MODEL", "qwen2.5-coder:7b")
DEFAULT_BASE_URL = os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434")


class OllamaError(RuntimeError):
    """Structured local Ollama failure without leaking the configured endpoint."""

    def __init__(self, message: str, *, code: str = "ollama-request-failed") -> None:
        self.code = code
        super().__init__(message)


def _http_error(status: int) -> OllamaError:
    if status in {401, 403}:
        return OllamaError(
            "Ollama rejected the request authorization.",
            code="ollama-authorization",
        )
    if status == 404:
        return OllamaError(
            "Ollama could not find the requested model or API resource.",
            code="ollama-model-unavailable",
        )
    if status in {408, 504}:
        return OllamaError("Ollama request timed out.", code="ollama-timeout")
    if status in {429, 502, 503}:
        return OllamaError(
            "Ollama is temporarily overloaded or unavailable.",
            code="ollama-overloaded",
        )
    if 400 <= status < 500:
        return OllamaError(
            "Ollama rejected the request format.",
            code="ollama-protocol-error",
        )
    return OllamaError(
        "Ollama is temporarily unavailable.",
        code="ollama-unavailable",
    )


def chat(
    prompt: str,
    system_prompt: str,
    model: str = DEFAULT_MODEL,
    base_url: str = DEFAULT_BASE_URL,
    *,
    timeout_seconds: float | None = 120,
) -> str:
    """Send a bounded non-streaming chat request to a private Ollama endpoint."""
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
    timeout = 120 if timeout_seconds is None else max(0.001, float(timeout_seconds))
    try:
        with request.urlopen(req, timeout=timeout) as response:
            raw = response.read()
    except error.HTTPError as exc:  # pragma: no cover - local runtime dependent
        raise _http_error(exc.code) from exc
    except TimeoutError as exc:  # pragma: no cover
        raise OllamaError("Ollama request timed out.", code="ollama-timeout") from exc
    except error.URLError as exc:  # pragma: no cover
        reason = getattr(exc, "reason", None)
        if isinstance(reason, TimeoutError):
            raise OllamaError("Ollama request timed out.", code="ollama-timeout") from exc
        raise OllamaError(
            "Ollama is unavailable on the configured private connection.",
            code="ollama-unavailable",
        ) from exc
    except OSError as exc:  # pragma: no cover
        raise OllamaError(
            "Ollama is unavailable on the configured private connection.",
            code="ollama-unavailable",
        ) from exc

    try:
        result = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise OllamaError(
            "Ollama returned an invalid response.",
            code="ollama-invalid-response",
        ) from exc
    if not isinstance(result, dict):
        raise OllamaError(
            "Ollama returned an invalid response.",
            code="ollama-invalid-response",
        )
    message = result.get("message")
    content = message.get("content") if isinstance(message, dict) else None
    if not isinstance(content, str) or not content.strip():
        raise OllamaError(
            "Ollama response did not include message content.",
            code="ollama-invalid-response",
        )
    return content.strip()
