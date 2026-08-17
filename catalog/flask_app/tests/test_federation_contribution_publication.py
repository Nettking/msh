from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from catalog.federation.models import CapabilityStatus
from catalog.federation.onboarding_models import BenchmarkRecommendation, BenchmarkState
from catalog.flask_app.services import federation_contribution_publication as publication
from catalog.flask_app.services.federation_contribution_publication import (
    plan_contribution_publications,
)

NOW = datetime(2026, 8, 8, 18, 30, tzinfo=timezone.utc)


def _candidate(
    candidate_id: str,
    capability_type: str,
    protocol: str,
    *,
    version: str = "1.0",
) -> SimpleNamespace:
    return SimpleNamespace(
        candidate_id=candidate_id,
        device_id="node-a",
        capability_type=capability_type,
        capability_protocol=protocol,
        display_label=capability_type.replace("-", " ").title(),
        capacity_envelope={"protocol_version": version},
    )


def _intent(
    candidate_id: str,
    activation_state: str,
    decision_revision: int,
) -> SimpleNamespace:
    return SimpleNamespace(
        candidate_id=candidate_id,
        device_id="node-a",
        activation_state=activation_state,
        decision_revision=decision_revision,
        decided_at=NOW + timedelta(seconds=decision_revision),
    )


def test_all_local_candidates_are_shared_as_metadata_without_granting_readiness() -> None:
    publications = plan_contribution_publications(
        session_id="session-a",
        node_id="node-a",
        candidates=(
            _candidate("candidate-ai", "language-model", "fcp-ai"),
            _candidate("candidate-compute", "compute", "fcp-compute-handler"),
            _candidate("candidate-storage", "storage", "fcp-storage-candidate"),
        ),
        intents=(
            _intent("candidate-ai", "active", 3),
            _intent("candidate-compute", "pending", 2),
        ),
        authorized_status={"capabilities": []},
        now=NOW,
    )

    by_id = {
        item.announcement.capability_id: item.announcement
        for item in publications
    }
    assert by_id["candidate-ai"].status is CapabilityStatus.READY
    assert by_id["candidate-compute"].status is CapabilityStatus.REGISTERING
    assert by_id["candidate-storage"].status is CapabilityStatus.DISABLED
    assert all(
        item.properties["kind"] == "capability-first-candidate"
        for item in by_id.values()
    )
    assert all("endpoint" not in str(item.properties).casefold() for item in by_id.values())


def test_matching_shared_metadata_is_not_reannounced() -> None:
    candidate = _candidate("candidate-ai", "language-model", "fcp-ai")
    intent = _intent("candidate-ai", "active", 3)
    initial = plan_contribution_publications(
        session_id="session-a",
        node_id="node-a",
        candidates=(candidate,),
        intents=(intent,),
        authorized_status={"capabilities": []},
        now=NOW,
    )[0].announcement

    publications = plan_contribution_publications(
        session_id="session-a",
        node_id="node-a",
        candidates=(candidate,),
        intents=(intent,),
        authorized_status={
            "capabilities": [
                {
                    "session_id": initial.session_id,
                    "capability_id": initial.capability_id,
                    "node_id": initial.node_id,
                    "type": initial.type,
                    "protocol": initial.protocol,
                    "protocol_version": initial.protocol_version,
                    "status": initial.status.value,
                    "properties": initial.properties,
                }
            ]
        },
        now=NOW + timedelta(minutes=1),
    )

    assert publications == ()


def test_missing_previously_published_candidate_fails_closed() -> None:
    publications = plan_contribution_publications(
        session_id="session-a",
        node_id="node-a",
        candidates=(),
        intents=(),
        authorized_status={
            "capabilities": [
                {
                    "session_id": "session-a",
                    "capability_id": "candidate-ai",
                    "node_id": "node-a",
                    "type": "language-model",
                    "protocol": "fcp-ai",
                    "protocol_version": "1.0",
                    "status": "ready",
                    "properties": {
                        "kind": "capability-first-candidate",
                        "label": "Language Model",
                    },
                },
                {
                    "session_id": "session-a",
                    "capability_id": "unrelated-provider",
                    "node_id": "node-a",
                    "type": "compute",
                    "protocol": "fcp-compute",
                    "protocol_version": "1.0",
                    "status": "ready",
                    "properties": {},
                },
            ]
        },
        now=NOW,
    )

    assert len(publications) == 1
    assert publications[0].announcement.capability_id == "candidate-ai"
    assert publications[0].announcement.status is CapabilityStatus.UNAVAILABLE


def _attestation_fixture(*, capability_type: str = "language-model"):
    snapshot = SimpleNamespace(device_id="node-a", revision=7)
    benchmark_result = SimpleNamespace(
        device_id="node-a",
        run_id="benchmark-green-1",
        benchmark_id="benchmark-ai",
        target_service_id="candidate-ai",
        state=BenchmarkState.PASSED,
        recommendation=BenchmarkRecommendation.RECOMMENDED,
    )
    benchmark_item = SimpleNamespace(
        benchmark_id="benchmark-ai",
        target_service_id="candidate-ai",
        runnable=True,
        definition=object(),
        dependency_inputs={},
    )
    benchmark_service = SimpleNamespace(
        plan=lambda _snapshot: (benchmark_item,),
        list_results=lambda: (benchmark_result,),
    )
    contribution_service = SimpleNamespace(
        benchmark_service=benchmark_service,
        _authorized_snapshot=lambda **_kwargs: (snapshot, True),
    )
    candidate = SimpleNamespace(
        candidate_id="candidate-ai",
        device_id="node-a",
        capability_type=capability_type,
        capability_protocol="fcp-ai",
        display_label="Language model",
        capacity_envelope={"protocol_version": "1.0"},
        inspection_revision=7,
        benchmark_run_ids=("benchmark-green-1",),
        missing_prerequisites=(),
        policy_state="allowed",
    )
    intent = SimpleNamespace(
        candidate_id="candidate-ai",
        device_id="node-a",
        activation_state="pending",
        desired_state="enabled",
        policy_state="allowed",
        decision_revision=5,
        decided_at=NOW,
    )
    return contribution_service, candidate, intent, benchmark_result


def test_auto_enrollment_attestation_requires_current_green_benchmark(monkeypatch) -> None:
    service, candidate, intent, result = _attestation_fixture()
    monkeypatch.setattr(
        publication,
        "evaluate_result",
        lambda *_args, **_kwargs: SimpleNamespace(current=True),
    )

    evidence = publication._auto_enrollment_attestations(
        contribution_service=service,
        candidates=(candidate,),
        intents=(intent,),
        node_id="node-a",
        now=NOW,
    )
    assert evidence["candidate-ai"]["benchmark_state"] == "green"
    assert evidence["candidate-ai"]["benchmark_run_ids"] == ["benchmark-green-1"]

    result.state = BenchmarkState.FAILED
    assert publication._auto_enrollment_attestations(
        contribution_service=service,
        candidates=(candidate,),
        intents=(intent,),
        node_id="node-a",
        now=NOW,
    ) == {}

    result.state = BenchmarkState.PASSED
    monkeypatch.setattr(
        publication,
        "evaluate_result",
        lambda *_args, **_kwargs: SimpleNamespace(current=False),
    )
    assert publication._auto_enrollment_attestations(
        contribution_service=service,
        candidates=(candidate,),
        intents=(intent,),
        node_id="node-a",
        now=NOW,
    ) == {}


def test_storage_never_receives_auto_enrollment_attestation(monkeypatch) -> None:
    service, candidate, intent, _result = _attestation_fixture(
        capability_type="storage"
    )
    monkeypatch.setattr(
        publication,
        "evaluate_result",
        lambda *_args, **_kwargs: SimpleNamespace(current=True),
    )
    assert publication._auto_enrollment_attestations(
        contribution_service=service,
        candidates=(candidate,),
        intents=(intent,),
        node_id="node-a",
        now=NOW,
    ) == {}
