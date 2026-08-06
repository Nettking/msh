from __future__ import annotations

from datetime import datetime, timedelta, timezone

from flask import Flask

from catalog.capabilities.contributions import AICandidateSource, AIContributionAdapter
from catalog.federation.onboarding_models import DeviceInspectionSnapshot
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


def test_legacy_ai_disabled_does_not_hide_configured_ai_explainer(
    monkeypatch,
) -> None:
    monkeypatch.setenv("MSH_AI_MODEL", "llama3.2:3b")
    monkeypatch.setenv("OLLAMA_BASE_URL", "http://ollama:11434")

    app = Flask(__name__)
    app.config["CAPABILITY_ONBOARDING_AI_RUNTIME_MANAGER"] = object()
    legacy_setup = {
        "configured": True,
        "user_setup_complete": True,
        "deployment_mode": "web-workbench",
        "ai_enabled": False,
        "ai_provider_mode": "local",
        "ai_model": "llama3.2:3b",
        "ollama_base_url": "http://ollama:11434",
    }

    with app.app_context():
        sources, adapters = default_components(
            onboarding_service=object(),  # type: ignore[arg-type]
            setup_loader=lambda: legacy_setup,
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
