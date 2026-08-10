from __future__ import annotations

from dataclasses import dataclass, field
import threading

from flask import Flask

from catalog.ai.runtime import (
    AIRuntimePolicy,
    LANGUAGE_MODEL_PROTOCOL,
    LANGUAGE_MODEL_PROTOCOL_VERSION,
    LanguageModelRuntime,
)
from catalog.ai.runtime_contracts import AIModality, AIRuntimeRequest
from catalog.flask_app import ai_routes
from catalog.flask_app.ai_routes import ai_web
from catalog.flask_app.routes import web

SESSION = ai_routes.AI_RUNTIME_SESSION_ID


@dataclass
class FakeProvider:
    capability_id: str
    display_name: str
    models: tuple[str, ...]
    response: str
    node_id: str
    session_id: str = SESSION
    modalities: tuple[str, ...] = (AIModality.TEXT.value,)
    max_concurrent_jobs: int = 1
    supports_timeout: bool = True
    supports_cancellation: bool = True
    protocol: str = LANGUAGE_MODEL_PROTOCOL
    protocol_version: str = LANGUAGE_MODEL_PROTOCOL_VERSION
    calls: list[AIRuntimeRequest] = field(default_factory=list)

    def invoke(
        self,
        request: AIRuntimeRequest,
        *,
        timeout_seconds: float | None,
        cancellation_event: threading.Event | None,
    ) -> str:
        del timeout_seconds, cancellation_event
        self.calls.append(request)
        return self.response


class FakeChunk:
    path = "README.md"

    def source_label(self) -> str:
        return "README.md:1-2"


def _app() -> Flask:
    app = Flask(__name__, template_folder="../templates", static_folder="../static")
    app.secret_key = "test"
    app.register_blueprint(web)
    app.register_blueprint(ai_web)
    return app


def _grounding(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(ai_routes, "repo_root_from", lambda: tmp_path)
    monkeypatch.setattr(ai_routes, "load_or_build_chunks", lambda _root: [])
    monkeypatch.setattr(ai_routes, "build_symbols", lambda _root: {})
    monkeypatch.setattr(
        ai_routes,
        "retrieve",
        lambda *_args, **_kwargs: [FakeChunk()],
    )
    monkeypatch.setattr(
        ai_routes,
        "format_context",
        lambda _chunks: "retrieved context",
    )
    monkeypatch.setattr(
        ai_routes,
        "build_prompt",
        lambda *_args, **_kwargs: "logical prompt",
    )


def test_existing_ai_answer_uses_multi_provider_logical_runtime(
    monkeypatch,
    tmp_path,
) -> None:
    _grounding(monkeypatch, tmp_path)
    first = FakeProvider(
        capability_id="provider-small",
        display_name="Small provider",
        models=("model-small",),
        response="wrong",
        node_id="node-small",
    )
    second = FakeProvider(
        capability_id="provider-large",
        display_name="Large provider",
        models=("model-large",),
        response="selected answer",
        node_id="node-large",
    )
    runtime = LanguageModelRuntime(
        session_id=SESSION,
        providers=(first, second),
        policy=AIRuntimePolicy(max_fallbacks=0),
    )

    result = ai_routes._answer_question(
        "How does FCP work?",
        model="model-large",
        base_url="http://private-provider:11434",
        provider_name="Configured provider",
        dry_run=False,
        extractive=False,
        runtime=runtime,
    )

    assert result["error"] == ""
    assert result["selected_provider_name"] == "Large provider"
    assert result["selection_status"]["reason"] == (
        "highest-ranked-eligible-provider"
    )
    assert result["selection_status"]["rejected"]["provider-small"] == [
        "requirement-mismatch:requirements.models"
    ]
    assert not first.calls
    assert len(second.calls) == 1
    assert "private-provider" not in repr(result)


def test_ai_json_reports_selected_provider_and_reason_without_endpoint(
    monkeypatch,
) -> None:
    private_url = "http://192.168.1.50:11434"
    monkeypatch.setattr(
        ai_routes,
        "_ai_defaults",
        lambda: ("model-large", private_url, "Configured laptop"),
    )
    monkeypatch.setattr(
        ai_routes,
        "_answer_question",
        lambda *_args, **_kwargs: {
            "answer": "answer",
            "context": "context",
            "sources": ["README.md:1-2"],
            "error": "",
            "error_code": "",
            "selection_status": {
                "decision": "selected",
                "selected_capability_id": "provider-large",
                "selected_provider_name": "Large provider",
                "eligible_capability_ids": ["provider-large"],
                "rejected": {},
                "reason": "highest-ranked-eligible-provider",
                "evaluated_at": "2026-08-01T12:00:00Z",
            },
            "selected_provider_name": "Large provider",
            "provider_attempts": [],
            "provider_status": [
                {
                    "capability_id": "provider-large",
                    "provider_name": "Large provider",
                    "status": "ready",
                    "models": ["model-large"],
                    "modalities": ["text"],
                    "active_jobs": 0,
                    "max_concurrent_jobs": 1,
                    "queue_depth": 0,
                    "supports_timeout": True,
                    "supports_cancellation": True,
                }
            ],
        },
    )

    response = _app().test_client().post(
        "/ai/ask",
        data={"question": "How?", "model": "model-large"},
        headers={"Accept": "application/json"},
    )
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["provider_name"] == "Large provider"
    assert payload["selection_status"]["reason"] == (
        "highest-ranked-eligible-provider"
    )
    assert private_url not in response.get_data(as_text=True)
    assert "base_url" not in payload["provider_status"][0]


def test_ai_page_never_renders_private_ollama_endpoint(monkeypatch) -> None:
    private_url = "http://192.168.1.50:11434"
    monkeypatch.setattr(
        ai_routes,
        "_ai_defaults",
        lambda: ("model-a", private_url, "Lab laptop"),
    )

    response = _app().test_client().get("/ai")
    html = response.get_data(as_text=True)

    assert response.status_code == 200
    assert "Lab laptop" in html
    assert "Provider decision" not in html
    assert private_url not in html
    assert "192.168.1.50" not in html
    assert "ollama_base_url" not in html
