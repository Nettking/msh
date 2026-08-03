from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from flask import Flask

from catalog.federation.projections import (
    BenchmarkResultsAdapter,
    BenchmarkSnapshot,
    DeviceRecord,
    FederationAuthoritySnapshot,
    FederationProjectionService,
    JobAuthoritySnapshot,
    OnboardingSnapshot,
    ProjectionAdapters,
    ProviderSnapshot,
    StorageAuthoritySnapshot,
)
from catalog.flask_app.services import federation_projection_service as composition

NOW = datetime(2026, 8, 3, 17, 30, tzinfo=timezone.utc)


class _StaticAdapter:
    def __init__(self, snapshot: object) -> None:
        self._snapshot = snapshot

    def snapshot(self) -> object:
        return self._snapshot


def _summary(payload: dict[str, object], label: str) -> dict[str, object]:
    cards = payload["summary_cards"]
    assert isinstance(cards, list)
    return next(card for card in cards if card["label"] == label)


def test_empty_optional_surfaces_are_not_false_degraded_states() -> None:
    onboarding = OnboardingSnapshot(
        available=True,
        reason_code="current",
        federation_id="federation-local",
        connection_state="connected",
        trusted=True,
        device_id="node-local",
    )
    federation = FederationAuthoritySnapshot(
        available=True,
        reason_code="current",
        label="Local federation",
        state="active",
        revision=2,
        devices=(
            DeviceRecord(
                "node-local",
                "This MSH device",
                "unknown",
                None,
                0,
            ),
        ),
    )
    service = FederationProjectionService(
        ProjectionAdapters(
            onboarding=_StaticAdapter(onboarding),
            providers=_StaticAdapter(ProviderSnapshot(True, "not-configured")),
            benchmarks=_StaticAdapter(BenchmarkSnapshot(True, "current")),
            federation=_StaticAdapter(federation),
            storage=_StaticAdapter(
                StorageAuthoritySnapshot(True, "not-configured")
            ),
            jobs=_StaticAdapter(JobAuthoritySnapshot(True, "not-configured")),
        ),
        now=lambda: NOW,
    )

    payload = service.overview().to_dict()

    assert payload["state"] == "connected"
    assert payload["state_label"] == "Connected"
    assert payload["degraded"]["visible"] is False
    assert payload["recommended_action"]["url"] == "/onboarding?step=inspect"
    trusted = _summary(payload, "Trusted devices")
    assert trusted["detail"] == "1 connected now"
    assert trusted["state_label"] == "Healthy"
    benchmarks = _summary(payload, "Benchmark freshness")
    assert benchmarks == {
        "label": "Benchmark freshness",
        "value": "0 results",
        "detail": "Run inspection and review the recommended checks.",
        "state": "empty",
        "state_label": "Not run",
    }
    assert payload["device"]["state_label"] == "Connected"
    assert payload["devices"]["items"][0]["state_label"] == "Connected"


def test_production_composition_reads_live_cfi_services(
    monkeypatch,
) -> None:
    class _Coordinator:
        store = SimpleNamespace(
            get_session=lambda _session_id: SimpleNamespace(
                display_name="Physical test federation",
                state="active",
                revision=1,
            )
        )

        @staticmethod
        def status(*, actor_node_id: str, cursor: str | None = None) -> object:
            assert actor_node_id == "node-local"
            assert cursor is None
            return {
                "nodes": [
                    {
                        "node_id": "node-local",
                        "display_name": "This MSH device",
                        "connection_state": "unknown",
                    }
                ],
                "capabilities": [],
                "pagination": {"has_more": False, "next_cursor": None},
            }

        @staticmethod
        def replay_page(**_kwargs) -> tuple[tuple[object, ...], int]:
            return (
                (
                    SimpleNamespace(
                        revision=1,
                        event_type="node.joined",
                        occurred_at=NOW,
                        payload={"node_id": "node-local"},
                    ),
                ),
                1,
            )

    binding = SimpleNamespace(
        federation_id="federation-local",
        internal_session_id="session-private",
        device_id="node-local",
        state="connected",
        trusted=True,
    )
    context = SimpleNamespace(
        binding=binding,
        credentials=SimpleNamespace(
            identity=SimpleNamespace(node_id="node-local")
        ),
        coordinator=_Coordinator(),
    )
    inspection = SimpleNamespace(
        device_id="node-local",
        revision=4,
        expires_at=NOW + timedelta(hours=1),
        os_family="windows",
        architecture="x86-64",
        resource_observations={},
        detected_services=("ollama-configured",),
        registered_handlers=(),
        detected_data_sources=(),
        warnings=(),
    )
    benchmark = SimpleNamespace(
        run_id="run-ai-current",
        device_id="node-local",
        benchmark_id="ollama-response",
        target_service_id="ollama-configured",
        state="passed",
        recommendation="recommended",
        finished_at=NOW,
        expires_at=NOW + timedelta(hours=1),
        metrics={},
        diagnostics=(),
    )
    candidate = SimpleNamespace(
        candidate_id="candidate-ai",
        device_id="node-local",
        capability_type="language-model",
        capability_protocol="msh-ai",
        display_label="Configured language model",
        capacity_envelope={},
        missing_prerequisites=(),
        policy_state="allowed",
    )
    intent = SimpleNamespace(
        candidate_id="candidate-ai",
        desired_state="enabled",
        activation_state="active",
        reason=None,
    )

    monkeypatch.setattr(composition, "_onboarding_context", lambda: context)
    monkeypatch.setattr(
        composition,
        "get_capability_inspection_service",
        lambda: SimpleNamespace(load=lambda: inspection),
    )
    monkeypatch.setattr(
        composition,
        "get_capability_benchmark_service",
        lambda: SimpleNamespace(list_results=lambda: (benchmark,)),
    )
    monkeypatch.setattr(
        composition,
        "get_capability_contribution_service",
        lambda: SimpleNamespace(
            recommend=lambda *, require_benchmark_review: (candidate,),
            intents=lambda: (intent,),
        ),
    )
    monkeypatch.setattr(
        composition,
        "BenchmarkResultsAdapter",
        lambda store: BenchmarkResultsAdapter(store, now=lambda: NOW),
    )

    app = Flask(__name__)
    with app.app_context():
        payload = composition.get_federation_projection_service().overview().to_dict()

    assert payload["state"] == "connected"
    assert payload["degraded"]["visible"] is False
    assert payload["device"]["details"][1]["value"] == "Revision 4"
    assert payload["benchmarks"]["items"][0]["label"] == "Ollama Response"
    assert payload["contributions"][0]["label"] == "Configured language model"
    assert payload["contributions"][0]["activation_state"] == "active"
    assert _summary(payload, "Trusted devices")["detail"] == "1 connected now"
    assert "session-private" not in str(payload)