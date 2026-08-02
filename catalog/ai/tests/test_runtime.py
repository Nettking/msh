from __future__ import annotations

import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone

import pytest

from catalog.ai.ollama_provider import OllamaLanguageModelProvider
from catalog.ai.runtime import (
    AIProviderInvocationError,
    AIRuntimePolicy,
    LANGUAGE_MODEL_PROTOCOL,
    LANGUAGE_MODEL_PROTOCOL_VERSION,
    LanguageModelRuntime,
)
from catalog.ai.runtime_contracts import (
    AIModality,
    AIProviderFailureClass,
    AIRuntimeError,
    AIRuntimeRequest,
)
from catalog.capabilities.provider_reports import ProviderSelectionPolicy, ProviderStatus
from catalog.federation.errors import FederationValidationError

SESSION = "session-ai"
NOW = datetime(2026, 8, 1, 12, 0, tzinfo=timezone.utc)


def _request(
    *,
    request_id: str = "request-1",
    session_id: str = SESSION,
    model: str = "model-a",
    modality: AIModality = AIModality.TEXT,
    timeout_seconds: int = 3,
) -> AIRuntimeRequest:
    return AIRuntimeRequest(
        request_id=request_id,
        session_id=session_id,
        idempotency_key=request_id,
        model=model,
        modality=modality,
        prompt="Explain MSH.",
        system_prompt="Use repository evidence.",
        timeout_seconds=timeout_seconds,
    )


@dataclass
class FakeProvider:
    capability_id: str
    display_name: str
    models: tuple[str, ...]
    modalities: tuple[str, ...] = (AIModality.TEXT.value,)
    node_id: str = "node-a"
    session_id: str = SESSION
    max_concurrent_jobs: int = 1
    supports_timeout: bool = True
    supports_cancellation: bool = True
    protocol: str = LANGUAGE_MODEL_PROTOCOL
    protocol_version: str = LANGUAGE_MODEL_PROTOCOL_VERSION
    response: str = "answer"
    failure: AIProviderInvocationError | None = None
    started: threading.Event | None = None
    release: threading.Event | None = None
    calls: list[tuple[str, float | None, bool]] = field(default_factory=list)

    def invoke(
        self,
        request: AIRuntimeRequest,
        *,
        timeout_seconds: float | None,
        cancellation_event: threading.Event | None,
    ) -> str:
        self.calls.append(
            (
                request.request_id,
                timeout_seconds,
                cancellation_event is not None,
            )
        )
        if self.started is not None:
            self.started.set()
        if self.release is not None:
            assert self.release.wait(timeout=5)
        if self.failure is not None:
            raise self.failure
        if cancellation_event is not None and cancellation_event.is_set():
            raise AIProviderInvocationError(
                "provider-request-cancelled",
                "cancelled",
                failure_class=AIProviderFailureClass.CANCELLED,
            )
        return self.response


def test_two_ai_providers_select_requested_model() -> None:
    first = FakeProvider(
        capability_id="provider-a",
        display_name="Provider A",
        models=("model-a",),
        response="from-a",
    )
    second = FakeProvider(
        capability_id="provider-b",
        display_name="Provider B",
        models=("model-b",),
        node_id="node-b",
        response="from-b",
    )
    runtime = LanguageModelRuntime(session_id=SESSION, providers=(first, second))

    result = runtime.execute(_request(model="model-b"))

    assert result.content == "from-b"
    assert result.provider_capability_id == "provider-b"
    assert result.selection.eligible_capability_ids == ("provider-b",)
    assert "provider-a" in result.selection.rejected
    assert not first.calls
    assert len(second.calls) == 1


def test_modality_requirement_selects_vision_provider() -> None:
    text = FakeProvider(
        capability_id="provider-text",
        display_name="Text provider",
        models=("model-a",),
    )
    vision = FakeProvider(
        capability_id="provider-vision",
        display_name="Vision provider",
        models=("model-a",),
        modalities=(AIModality.TEXT.value, AIModality.VISION.value),
        node_id="node-vision",
        response="vision-answer",
    )
    runtime = LanguageModelRuntime(session_id=SESSION, providers=(text, vision))

    result = runtime.execute(
        _request(model="model-a", modality=AIModality.VISION)
    )

    assert result.provider_capability_id == "provider-vision"
    assert result.content == "vision-answer"
    assert not text.calls


def test_unavailable_provider_is_filtered_out() -> None:
    first = FakeProvider(
        capability_id="provider-a",
        display_name="Provider A",
        models=("model-a",),
    )
    second = FakeProvider(
        capability_id="provider-b",
        display_name="Provider B",
        models=("model-a",),
        node_id="node-b",
        response="healthy",
    )
    runtime = LanguageModelRuntime(session_id=SESSION, providers=(first, second))
    runtime.set_provider_status("provider-a", ProviderStatus.UNAVAILABLE)

    result = runtime.execute(_request())

    assert result.provider_capability_id == "provider-b"
    assert result.selection.rejected["provider-a"] == (
        "provider-status-unavailable",
    )
    assert not first.calls


def test_deterministic_policy_prefers_explicit_node() -> None:
    first = FakeProvider(
        capability_id="provider-a",
        display_name="Provider A",
        models=("model-a",),
        node_id="node-a",
        response="a",
    )
    second = FakeProvider(
        capability_id="provider-b",
        display_name="Provider B",
        models=("model-a",),
        node_id="node-b",
        response="b",
    )
    runtime = LanguageModelRuntime(
        session_id=SESSION,
        providers=(first, second),
        policy=AIRuntimePolicy(
            selection_policy=ProviderSelectionPolicy(
                preferred_node_id="node-b"
            ),
            max_fallbacks=0,
        ),
    )

    result = runtime.execute(_request())

    assert result.provider_capability_id == "provider-b"
    assert result.selection.reason == "preferred-node-provider-selected"


def test_fallback_occurs_only_for_explicitly_allowed_error() -> None:
    first = FakeProvider(
        capability_id="provider-a",
        display_name="Provider A",
        models=("model-a",),
        failure=AIProviderInvocationError(
            "provider-unavailable",
            "temporarily unavailable",
            failure_class=AIProviderFailureClass.TRANSIENT,
            retryable=True,
        ),
    )
    second = FakeProvider(
        capability_id="provider-b",
        display_name="Provider B",
        models=("model-a",),
        node_id="node-b",
        response="fallback-answer",
    )
    runtime = LanguageModelRuntime(
        session_id=SESSION,
        providers=(first, second),
        policy=AIRuntimePolicy(
            max_fallbacks=1,
            fallback_error_codes=("provider-unavailable",),
        ),
    )

    result = runtime.execute(_request())

    assert result.content == "fallback-answer"
    assert [attempt.capability_id for attempt in result.attempts] == [
        "provider-a",
        "provider-b",
    ]
    assert [attempt.outcome for attempt in result.attempts] == [
        "failed",
        "succeeded",
    ]


@pytest.mark.parametrize(
    ("code", "failure_class"),
    [
        ("provider-security-error", AIProviderFailureClass.SECURITY),
        ("provider-authorization-error", AIProviderFailureClass.AUTHORIZATION),
        ("provider-protocol-error", AIProviderFailureClass.PROTOCOL),
        ("provider-invalid-response", AIProviderFailureClass.INVALID_RESPONSE),
    ],
)
def test_security_protocol_and_authorization_failures_never_fallback(
    code: str,
    failure_class: AIProviderFailureClass,
) -> None:
    first = FakeProvider(
        capability_id="provider-a",
        display_name="Provider A",
        models=("model-a",),
        failure=AIProviderInvocationError(
            code,
            "fail closed",
            failure_class=failure_class,
            retryable=True,
        ),
    )
    second = FakeProvider(
        capability_id="provider-b",
        display_name="Provider B",
        models=("model-a",),
        node_id="node-b",
        response="must-not-run",
    )
    runtime = LanguageModelRuntime(
        session_id=SESSION,
        providers=(first, second),
        policy=AIRuntimePolicy(
            max_fallbacks=1,
            fallback_error_codes=(code,),
        ),
    )

    with pytest.raises(AIRuntimeError) as failed:
        runtime.execute(_request())

    assert failed.value.code == code
    assert len(first.calls) == 1
    assert not second.calls


def test_non_allowlisted_transient_failure_does_not_fallback() -> None:
    first = FakeProvider(
        capability_id="provider-a",
        display_name="Provider A",
        models=("model-a",),
        failure=AIProviderInvocationError(
            "provider-network-reset",
            "network reset",
            failure_class=AIProviderFailureClass.TRANSIENT,
            retryable=True,
        ),
    )
    second = FakeProvider(
        capability_id="provider-b",
        display_name="Provider B",
        models=("model-a",),
        node_id="node-b",
    )
    runtime = LanguageModelRuntime(
        session_id=SESSION,
        providers=(first, second),
        policy=AIRuntimePolicy(
            max_fallbacks=1,
            fallback_error_codes=("provider-unavailable",),
        ),
    )

    with pytest.raises(AIRuntimeError) as failed:
        runtime.execute(_request())

    assert failed.value.code == "provider-network-reset"
    assert not second.calls


def test_bounded_queue_rejects_overload() -> None:
    started = threading.Event()
    release = threading.Event()
    provider = FakeProvider(
        capability_id="provider-a",
        display_name="Provider A",
        models=("model-a",),
        started=started,
        release=release,
    )
    runtime = LanguageModelRuntime(
        session_id=SESSION,
        providers=(provider,),
        policy=AIRuntimePolicy(max_pending_requests=1, max_fallbacks=0),
    )
    outcomes: list[str] = []

    def run_first() -> None:
        outcomes.append(runtime.execute(_request()).content)

    thread = threading.Thread(target=run_first)
    thread.start()
    assert started.wait(timeout=5)
    try:
        with pytest.raises(AIRuntimeError) as full:
            runtime.execute(_request(request_id="request-2"))
        assert full.value.code == "ai-queue-full"
        assert full.value.failure_class is AIProviderFailureClass.OVERLOADED
    finally:
        release.set()
        thread.join(timeout=5)

    assert outcomes == ["answer"]


def test_cancellation_before_scheduling_fails_closed() -> None:
    provider = FakeProvider(
        capability_id="provider-a",
        display_name="Provider A",
        models=("model-a",),
    )
    runtime = LanguageModelRuntime(session_id=SESSION, providers=(provider,))
    cancelled = threading.Event()
    cancelled.set()

    with pytest.raises(AIRuntimeError) as error:
        runtime.execute(_request(), cancellation_event=cancelled)

    assert error.value.code == "ai-request-cancelled"
    assert not provider.calls


def test_cross_session_request_is_rejected_before_provider_access() -> None:
    provider = FakeProvider(
        capability_id="provider-a",
        display_name="Provider A",
        models=("model-a",),
    )
    runtime = LanguageModelRuntime(session_id=SESSION, providers=(provider,))

    with pytest.raises(AIRuntimeError) as error:
        runtime.execute(_request(session_id="other-session"))

    assert error.value.code == "cross-session-ai-request"
    assert error.value.failure_class is AIProviderFailureClass.AUTHORIZATION
    assert not provider.calls


def test_safe_status_surface_never_contains_endpoint_or_credentials() -> None:
    provider = OllamaLanguageModelProvider(
        session_id=SESSION,
        display_name="Lab laptop",
        base_url="http://192.168.1.50:11434",
        models=("model-a",),
        chat_callable=lambda **_kwargs: "answer",
    )
    runtime = LanguageModelRuntime(session_id=SESSION, providers=(provider,))

    status = runtime.provider_status()
    rendered = repr(status)

    assert status[0]["provider_name"] == "Lab laptop"
    assert "base_url" not in status[0]
    assert "192.168.1.50" not in rendered
    assert "11434" not in rendered


def test_provider_display_name_cannot_be_an_endpoint() -> None:
    provider = FakeProvider(
        capability_id="provider-a",
        display_name="http://192.168.1.50:11434",
        models=("model-a",),
    )

    with pytest.raises(FederationValidationError) as unsafe:
        LanguageModelRuntime(session_id=SESSION, providers=(provider,))

    assert unsafe.value.code == "unsafe-provider-name"


def test_incompatible_provider_protocol_is_not_silently_downgraded() -> None:
    provider = FakeProvider(
        capability_id="provider-a",
        display_name="Provider A",
        models=("model-a",),
        protocol_version="2.0",
    )
    runtime = LanguageModelRuntime(session_id=SESSION, providers=(provider,))

    with pytest.raises(AIRuntimeError) as error:
        runtime.execute(_request())

    assert error.value.code == "no-eligible-ai-provider"
    assert error.value.selection is not None
    assert error.value.selection.rejected["provider-a"] == (
        "capability-protocol-version-incompatible",
    )
    assert not provider.calls


def test_ollama_adapter_passes_bounded_timeout_without_advertising_url() -> None:
    captured: dict[str, object] = {}

    def fake_chat(**kwargs) -> str:
        captured.update(kwargs)
        return "ollama-answer"

    provider = OllamaLanguageModelProvider(
        session_id=SESSION,
        display_name="Local workstation",
        base_url="http://10.0.0.8:11434",
        models=("model-a",),
        chat_callable=fake_chat,
    )
    runtime = LanguageModelRuntime(session_id=SESSION, providers=(provider,))

    result = runtime.execute(_request(timeout_seconds=2))

    assert result.content == "ollama-answer"
    assert captured["base_url"] == "http://10.0.0.8:11434"
    assert 0 < float(captured["timeout_seconds"]) <= 2
    assert "10.0.0.8" not in repr(result.to_dict())
