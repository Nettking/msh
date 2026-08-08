from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from flask import Flask

from catalog.ai.ollama_client import DEFAULT_BASE_URL
from catalog.ai.ollama_provider import OllamaLanguageModelProvider
from catalog.ai.runtime_manager import ConfiguredLanguageModelRuntimeManager
from catalog.ai.runtime_contracts import AIModality, AIRuntimeRequest
from catalog.capabilities.contributions import AICandidateSource, AIContributionAdapter
from catalog.federation.onboarding_models import (
    ContributionActivationState,
    DeviceInspectionSnapshot,
)
from catalog.flask_app import ai_routes
from catalog.flask_app.services.capability_contribution_components import (
    default_components,
)

NOW = datetime(2026, 8, 6, 9, 30, tzinfo=timezone.utc)


def _inspection() -> DeviceInspectionSnapshot:
    return DeviceInspectionSnapshot(
        device_id="device-one",
        revision=1,
        os_family="linux",
        architecture="x86_64",
        resource_observations={},
        detected_services=("ollama-configured",),
        registered_handlers=("msh-system-summary",),
        detected_data_sources=(),
        recommended_benchmark_ids=("benchmark.ai.ollama-inference.v1",),
        warnings=(),
        created_at=NOW,
        expires_at=NOW + timedelta(minutes=15),
    )


def _legacy_setup() -> dict[str, object]:
    return {
        "configured": True,
        "user_setup_complete": True,
        "deployment_mode": "web-workbench",
        "ai_enabled": False,
        "ai_provider_mode": "local",
        "ai_model": "llama3.2:3b",
        "ollama_base_url": "http://ollama:11434",
    }


def test_legacy_ai_disabled_does_not_hide_configured_ai_explainer(
    monkeypatch,
) -> None:
    monkeypatch.setenv("MSH_AI_MODEL", "llama3.2:3b")
    monkeypatch.setenv("OLLAMA_BASE_URL", "http://ollama:11434")

    app = Flask(__name__)
    app.config["CAPABILITY_ONBOARDING_AI_RUNTIME_MANAGER"] = object()

    with app.app_context():
        sources, adapters = default_components(
            onboarding_service=object(),  # type: ignore[arg-type]
            setup_loader=_legacy_setup,
        )
        ai_sources = tuple(
            source for source in sources if isinstance(source, AICandidateSource)
        )
        ai_adapters = tuple(
            adapter for adapter in adapters if isinstance(adapter, AIContributionAdapter)
        )
        assert len(ai_sources) == 1
        assert len(ai_adapters) == 1

        descriptors = ai_sources[0].descriptors(_inspection())

    assert len(descriptors) == 1
    descriptor = descriptors[0]
    assert descriptor.logical_service_id == "ollama-configured"
    assert descriptor.capability_type == "language-model"
    assert descriptor.display_label == "AI Explainer — This computer"
    assert descriptor.missing_prerequisites == ()


def test_ai_explainer_enable_maps_opaque_federation_identity_to_runtime_id(
    monkeypatch,
) -> None:
    monkeypatch.setenv("MSH_AI_MODEL", "llama3.2:3b")
    monkeypatch.setenv("OLLAMA_BASE_URL", "http://ollama:11434")

    opaque_node_id = "node-AbC_DEf-0123456789abcdefghijklmnopqrstuvwxyz"
    context = SimpleNamespace(
        credentials=SimpleNamespace(
            identity=SimpleNamespace(node_id=opaque_node_id)
        )
    )
    onboarding = SimpleNamespace(authorized_context=lambda: context)
    manager = ConfiguredLanguageModelRuntimeManager(session_id="local-ai")
    candidate = SimpleNamespace(
        capability_type="language-model",
        capacity_envelope={"provider_id": "ollama-configured"},
        display_label="AI Explainer — This computer",
    )

    app = Flask(__name__)
    app.config["CAPABILITY_ONBOARDING_AI_RUNTIME_MANAGER"] = manager
    with app.app_context():
        _sources, adapters = default_components(
            onboarding_service=onboarding,  # type: ignore[arg-type]
            setup_loader=_legacy_setup,
        )
        ai_adapter = next(
            adapter for adapter in adapters if isinstance(adapter, AIContributionAdapter)
        )
        outcome = ai_adapter.enable(candidate)  # type: ignore[arg-type]

    assert outcome.activation_state is ContributionActivationState.ACTIVE
    assert manager.additional_provider_ids() == ("ollama-configured",)
    provider = manager.additional_providers()[0]
    assert provider.node_id.startswith("node-federation-")
    assert provider.node_id != opaque_node_id
    assert provider.node_id == provider.node_id.casefold()


def _active_manager() -> ConfiguredLanguageModelRuntimeManager:
    manager = ConfiguredLanguageModelRuntimeManager(session_id="local-ai")
    manager.register(
        OllamaLanguageModelProvider(
            session_id="local-ai",
            display_name="AI Explainer — This computer",
            base_url="http://private-active-provider:11434",
            models=("llama3.2:3b",),
            capability_id="ollama-configured",
            node_id="node-active-ai",
            chat_callable=lambda **_kwargs: "capability-first answer",
        )
    )
    return manager


def test_active_capability_provider_replaces_stale_default_runtime(
    monkeypatch,
) -> None:
    manager = _active_manager()
    monkeypatch.setattr(ai_routes, "AI_RUNTIME_MANAGER", manager)
    monkeypatch.setattr(ai_routes, "_ACTIVE_RUNTIME", None)
    monkeypatch.setattr(ai_routes, "_ACTIVE_RUNTIME_KEY", None)

    model, base_url, provider_name = ai_routes._ai_defaults()
    runtime = ai_routes._build_ai_runtime(
        model=model,
        base_url=base_url,
        provider_name=provider_name,
    )

    assert (model, base_url, provider_name) == (
        "llama3.2:3b",
        DEFAULT_BASE_URL,
        "AI Explainer — This computer",
    )
    assert runtime.provider_status() == (
        {
            "capability_id": "ollama-configured",
            "provider_name": "AI Explainer — This computer",
            "status": "ready",
            "models": ["llama3.2:3b"],
            "modalities": ["text"],
            "active_jobs": 0,
            "max_concurrent_jobs": 1,
            "queue_depth": 0,
            "supports_timeout": True,
            "supports_cancellation": False,
        },
    )

    result = runtime.execute(
        AIRuntimeRequest(
            request_id="request-active-provider",
            session_id="local-ai",
            idempotency_key="request-active-provider",
            model="llama3.2:3b",
            modality=AIModality.TEXT,
            prompt="Explain MSH",
            system_prompt="Answer from context.",
            timeout_seconds=5,
        )
    )
    assert result.content == "capability-first answer"
    assert result.provider_capability_id == "ollama-configured"


def test_active_capability_provider_overrides_stale_startup_navigation_flag(
    monkeypatch,
) -> None:
    manager = _active_manager()
    monkeypatch.setattr(ai_routes, "AI_RUNTIME_MANAGER", manager)
    monkeypatch.setattr(
        ai_routes,
        "get_capability_startup_transition_service",
        lambda: SimpleNamespace(
            capability_flags=lambda: {
                "completed": True,
                "language_model": False,
                "runtime": True,
                "workbench": True,
            }
        ),
    )

    injected = ai_routes.inject_live_ai_capability_startup_flags()

    assert injected["capability_startup_flags"]["language_model"] is True


def test_disabled_capability_provider_hides_stale_enabled_navigation_flag(
    monkeypatch,
) -> None:
    manager = ConfiguredLanguageModelRuntimeManager(session_id="local-ai")
    monkeypatch.setattr(ai_routes, "AI_RUNTIME_MANAGER", manager)
    monkeypatch.setattr(
        ai_routes,
        "get_capability_startup_transition_service",
        lambda: SimpleNamespace(
            capability_flags=lambda: {
                "completed": True,
                "language_model": True,
                "runtime": True,
                "workbench": True,
            }
        ),
    )

    injected = ai_routes.inject_live_ai_capability_startup_flags()

    assert injected["capability_startup_flags"]["language_model"] is False
