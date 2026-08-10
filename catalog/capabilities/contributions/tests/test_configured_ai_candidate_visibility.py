from __future__ import annotations

from datetime import datetime, timedelta, timezone

from catalog.capabilities.contributions import AICandidateSource, AICandidateSpec
from catalog.federation.onboarding_models import DeviceInspectionSnapshot

NOW = datetime(2026, 8, 3, 18, 0, tzinfo=timezone.utc)


def _inspection(*, detected: bool) -> DeviceInspectionSnapshot:
    return DeviceInspectionSnapshot(
        device_id="device-one",
        revision=1,
        os_family="windows",
        architecture="x86_64",
        resource_observations={},
        detected_services=("ollama-configured",) if detected else (),
        registered_handlers=(),
        detected_data_sources=(),
        recommended_benchmark_ids=(),
        warnings=(),
        created_at=NOW,
        expires_at=NOW + timedelta(minutes=10),
    )


def _source() -> AICandidateSource:
    return AICandidateSource(
        {
            "ollama-configured": AICandidateSpec(
                service_id="ollama-configured",
                protocol="fcp-language-model",
                display_label="Configured Ollama model",
                capacity_envelope={"model": "qwen2.5:7b"},
            )
        }
    )


def test_configured_ai_remains_visible_when_inspection_cannot_detect_it() -> None:
    descriptors = _source().descriptors(_inspection(detected=False))

    assert len(descriptors) == 1
    assert descriptors[0].logical_service_id == "ollama-configured"
    assert descriptors[0].missing_prerequisites == (
        "configured-service-not-detected",
    )


def test_detected_ai_candidate_has_no_synthetic_missing_prerequisite() -> None:
    descriptors = _source().descriptors(_inspection(detected=True))

    assert len(descriptors) == 1
    assert descriptors[0].missing_prerequisites == ()
