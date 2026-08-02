from __future__ import annotations

import io
from urllib import error

import pytest

from catalog.ai import ollama_client
from catalog.ai.ollama_client import OllamaError


class FakeResponse:
    def __init__(self, content: bytes) -> None:
        self.content = content

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self) -> bytes:
        return self.content


def test_ollama_chat_returns_content_and_uses_bounded_timeout(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_urlopen(req, timeout):
        captured["url"] = req.full_url
        captured["timeout"] = timeout
        return FakeResponse(b'{"message":{"content":" answer "}}')

    monkeypatch.setattr(ollama_client.request, "urlopen", fake_urlopen)

    result = ollama_client.chat(
        "prompt",
        "system",
        model="model-a",
        base_url="http://private-provider:11434",
        timeout_seconds=2.5,
    )

    assert result == "answer"
    assert captured == {
        "url": "http://private-provider:11434/api/chat",
        "timeout": 2.5,
    }


@pytest.mark.parametrize(
    ("status", "code"),
    [
        (401, "ollama-authorization"),
        (403, "ollama-authorization"),
        (404, "ollama-model-unavailable"),
        (408, "ollama-timeout"),
        (429, "ollama-overloaded"),
        (503, "ollama-overloaded"),
        (400, "ollama-protocol-error"),
        (500, "ollama-unavailable"),
    ],
)
def test_ollama_http_failures_are_structured_without_endpoint_leak(
    monkeypatch,
    status: int,
    code: str,
) -> None:
    def fake_urlopen(req, timeout):
        raise error.HTTPError(
            req.full_url,
            status,
            "failure",
            hdrs=None,
            fp=io.BytesIO(b"private diagnostic"),
        )

    monkeypatch.setattr(ollama_client.request, "urlopen", fake_urlopen)

    with pytest.raises(OllamaError) as failed:
        ollama_client.chat(
            "prompt",
            "system",
            base_url="http://192.168.1.50:11434",
        )

    assert failed.value.code == code
    assert "192.168.1.50" not in str(failed.value)
    assert "private diagnostic" not in str(failed.value)


def test_ollama_timeout_is_structured(monkeypatch) -> None:
    monkeypatch.setattr(
        ollama_client.request,
        "urlopen",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(TimeoutError()),
    )

    with pytest.raises(OllamaError) as failed:
        ollama_client.chat("prompt", "system")

    assert failed.value.code == "ollama-timeout"


@pytest.mark.parametrize(
    "content",
    [
        b"not-json",
        b"[]",
        b"{}",
        b'{"message":{}}',
        b'{"message":{"content":""}}',
    ],
)
def test_ollama_invalid_response_fails_closed(monkeypatch, content: bytes) -> None:
    monkeypatch.setattr(
        ollama_client.request,
        "urlopen",
        lambda *_args, **_kwargs: FakeResponse(content),
    )

    with pytest.raises(OllamaError) as failed:
        ollama_client.chat("prompt", "system")

    assert failed.value.code == "ollama-invalid-response"
