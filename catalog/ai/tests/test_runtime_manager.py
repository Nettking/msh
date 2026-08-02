from __future__ import annotations

from dataclasses import dataclass
import threading

import pytest

from catalog.ai.runtime import (
    AIRuntimePolicy,
    LANGUAGE_MODEL_PROTOCOL,
    LANGUAGE_MODEL_PROTOCOL_VERSION,
)
from catalog.ai.runtime_contracts import AIModality, AIRuntimeRequest
from catalog.ai.runtime_manager import ConfiguredLanguageModelRuntimeManager
from catalog.federation.errors import FederationValidationError

SESSION = "session-manager"


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

    def invoke(
        self,
        request: AIRuntimeRequest,
        *,
        timeout_seconds: float | None,
        cancellation_event: threading.Event | None,
    ) -> str:
        del request, timeout_seconds, cancellation_event
        return self.response


def _request(model: str) -> AIRuntimeRequest:
    return AIRuntimeRequest(
        request_id=f"request-{model}",
        session_id=SESSION,
        idempotency_key=f"request-{model}",
        model=model,
        modality=AIModality.TEXT,
        prompt="prompt",
        system_prompt="system",
        timeout_seconds=5,
    )


def test_manager_reuses_runtime_for_same_configuration() -> None:
    manager = ConfiguredLanguageModelRuntimeManager(
        session_id=SESSION,
        policy=AIRuntimePolicy(max_pending_requests=4, max_fallbacks=0),
    )
    fake_chat = lambda **_kwargs: "configured"

    first = manager.runtime_for(
        model="model-a",
        base_url="http://private-a:11434",
        provider_name="Configured provider",
        chat_callable=fake_chat,
    )
    second = manager.runtime_for(
        model="model-a",
        base_url="http://private-a:11434",
        provider_name="Configured provider",
        chat_callable=fake_chat,
    )

    assert first is second


def test_model_change_keeps_one_scheduler_and_provider_capacity() -> None:
    manager = ConfiguredLanguageModelRuntimeManager(
        session_id=SESSION,
        policy=AIRuntimePolicy(max_pending_requests=4, max_fallbacks=0),
    )
    first_started = threading.Event()
    release_first = threading.Event()
    second_started = threading.Event()
    calls: list[str] = []
    outcomes: list[str] = []
    errors: list[BaseException] = []

    def fake_chat(**kwargs) -> str:
        model = str(kwargs["model"])
        calls.append(model)
        if model == "model-a":
            first_started.set()
            assert release_first.wait(timeout=5)
        else:
            second_started.set()
        return f"answer-{model}"

    first = manager.runtime_for(
        model="model-a",
        base_url="http://private-a:11434",
        provider_name="Configured provider",
        chat_callable=fake_chat,
    )

    def execute(runtime, model: str) -> None:
        try:
            outcomes.append(runtime.execute(_request(model)).content)
        except BaseException as error:  # pragma: no cover - asserted below
            errors.append(error)

    first_thread = threading.Thread(target=execute, args=(first, "model-a"))
    first_thread.start()
    assert first_started.wait(timeout=5)

    second = manager.runtime_for(
        model="model-b",
        base_url="http://private-a:11434",
        provider_name="Configured provider",
        chat_callable=fake_chat,
    )
    assert second is first
    assert second.provider_status()[0]["models"] == ["model-a", "model-b"]

    second_thread = threading.Thread(target=execute, args=(second, "model-b"))
    second_thread.start()
    assert not second_started.wait(timeout=0.2)

    release_first.set()
    first_thread.join(timeout=5)
    second_thread.join(timeout=5)

    assert not first_thread.is_alive()
    assert not second_thread.is_alive()
    assert not errors
    assert second_started.is_set()
    assert calls == ["model-a", "model-b"]
    assert sorted(outcomes) == ["answer-model-a", "answer-model-b"]


def test_registered_provider_participates_in_normal_runtime_selection() -> None:
    manager = ConfiguredLanguageModelRuntimeManager(session_id=SESSION)
    manager.register(
        FakeProvider(
            capability_id="provider-b",
            display_name="Provider B",
            models=("model-b",),
            response="from-b",
            node_id="node-b",
        )
    )
    runtime = manager.runtime_for(
        model="model-a",
        base_url="http://private-a:11434",
        provider_name="Configured provider",
        chat_callable=lambda **_kwargs: "from-a",
    )

    result = runtime.execute(_request("model-b"))

    assert result.provider_capability_id == "provider-b"
    assert result.content == "from-b"
    assert manager.additional_provider_ids() == ("provider-b",)


def test_configuration_change_rebuilds_runtime_but_keeps_registered_providers() -> None:
    manager = ConfiguredLanguageModelRuntimeManager(session_id=SESSION)
    manager.register(
        FakeProvider(
            capability_id="provider-b",
            display_name="Provider B",
            models=("model-b",),
            response="from-b",
            node_id="node-b",
        )
    )
    fake_chat = lambda **_kwargs: "configured"
    first = manager.runtime_for(
        model="model-a",
        base_url="http://private-a:11434",
        provider_name="Configured provider",
        chat_callable=fake_chat,
    )
    second = manager.runtime_for(
        model="model-c",
        base_url="http://private-c:11434",
        provider_name="New configured provider",
        chat_callable=fake_chat,
    )

    assert first is not second
    assert second.execute(_request("model-b")).content == "from-b"
    assert "private-c" not in repr(second.provider_status())


def test_manager_rejects_cross_session_provider() -> None:
    manager = ConfiguredLanguageModelRuntimeManager(session_id=SESSION)
    provider = FakeProvider(
        capability_id="provider-other",
        display_name="Other provider",
        models=("model-a",),
        response="answer",
        node_id="node-other",
        session_id="other-session",
    )

    with pytest.raises(FederationValidationError) as rejected:
        manager.register(provider)

    assert rejected.value.code == "cross-session-ai-provider"


def test_unregister_removes_provider_from_future_runtimes() -> None:
    manager = ConfiguredLanguageModelRuntimeManager(session_id=SESSION)
    manager.register(
        FakeProvider(
            capability_id="provider-b",
            display_name="Provider B",
            models=("model-b",),
            response="from-b",
            node_id="node-b",
        )
    )
    assert manager.unregister("provider-b") is True
    assert manager.unregister("provider-b") is False
    runtime = manager.runtime_for(
        model="model-a",
        base_url="http://private-a:11434",
        provider_name="Configured provider",
        chat_callable=lambda **_kwargs: "from-a",
    )

    with pytest.raises(Exception) as missing:
        runtime.execute(_request("model-b"))

    assert getattr(missing.value, "code", None) == "no-eligible-ai-provider"
