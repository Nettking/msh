from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from catalog.capabilities.storage_authority_enrollment import (
    STORAGE_AUTHORITY_PROPERTY,
    parse_storage_authority_evidence,
)
from catalog.federation.models import CapabilityStatus
from catalog.federation.onboarding_models import (
    BenchmarkRecommendation,
    BenchmarkState,
)
from catalog.federation.storage_protocol import (
    STORAGE_PROTOCOL,
    STORAGE_PROTOCOL_VERSION,
)
from catalog.flask_app.services import federation_contribution_publication as publication

NOW = datetime(2026, 8, 19, 18, 30, tzinfo=timezone.utc)
NODE_ID = "node-a"
SESSION_ID = "session-a"
CANDIDATE_ID = "candidate-77e6228e93fa5a79580649a62ee1493d"
LOCAL_PROVIDER_ID = "fcp-local-data-storage"
BENCHMARK_ID = "benchmark.storage.candidate-roundtrip.v1"
RUN_ID = "storage-green-1"


def _candidate(*, policy_state: str = "approval-required") -> SimpleNamespace:
    return SimpleNamespace(
        candidate_id=CANDIDATE_ID,
        device_id=NODE_ID,
        capability_type="storage",
        capability_protocol=STORAGE_PROTOCOL,
        display_label="Local FCP data storage",
        policy_state=policy_state,
        missing_prerequisites=(),
        inspection_revision=9,
        benchmark_run_ids=(RUN_ID,),
        capacity_envelope={
            "provider_id": LOCAL_PROVIDER_ID,
            "protocol_version": STORAGE_PROTOCOL_VERSION,
        },
    )


def _intent(*, activation_state: str = "active") -> SimpleNamespace:
    return SimpleNamespace(
        candidate_id=CANDIDATE_ID,
        device_id=NODE_ID,
        desired_state="enabled",
        policy_state="allowed",
        activation_state=activation_state,
        decision_revision=3,
        decided_at=NOW,
    )


def _contribution_service() -> SimpleNamespace:
    snapshot = SimpleNamespace(device_id=NODE_ID, revision=9)
    item = SimpleNamespace(
        benchmark_id=BENCHMARK_ID,
        target_service_id=LOCAL_PROVIDER_ID,
        runnable=True,
        definition=SimpleNamespace(),
        dependency_inputs={},
    )
    result = SimpleNamespace(
        device_id=NODE_ID,
        run_id=RUN_ID,
        benchmark_id=BENCHMARK_ID,
        target_service_id=LOCAL_PROVIDER_ID,
        state=BenchmarkState.PASSED,
        recommendation=BenchmarkRecommendation.USABLE,
    )
    benchmark_service = SimpleNamespace(
        plan=lambda _snapshot: (item,),
        list_results=lambda: (result,),
    )
    return SimpleNamespace(
        benchmark_service=benchmark_service,
        _authorized_snapshot=lambda **_kwargs: (snapshot, True),
    )


def test_assigned_active_storage_publishes_strict_authority_attestation(monkeypatch) -> None:
    """Reproduce the physical PC1 state: allowed+active intent, approval-required projection."""

    monkeypatch.setattr(
        publication,
        "evaluate_result",
        lambda *_args, **_kwargs: SimpleNamespace(current=True),
    )
    candidate = _candidate()
    intent = _intent()

    attestations = publication._auto_enrollment_attestations(
        contribution_service=_contribution_service(),
        candidates=(candidate,),
        intents=(intent,),
        node_id=NODE_ID,
        now=NOW,
    )

    assert STORAGE_AUTHORITY_PROPERTY in attestations[CANDIDATE_ID]

    planned = publication.plan_contribution_publications(
        session_id=SESSION_ID,
        node_id=NODE_ID,
        candidates=(candidate,),
        intents=(intent,),
        authorized_status={
            "capabilities": [
                {
                    "session_id": SESSION_ID,
                    "capability_id": CANDIDATE_ID,
                    "node_id": NODE_ID,
                    "type": "storage",
                    "protocol": STORAGE_PROTOCOL,
                    "protocol_version": STORAGE_PROTOCOL_VERSION,
                    "status": "ready",
                    "properties": {
                        "kind": "capability-first-candidate",
                        "candidate_id": CANDIDATE_ID,
                        "label": "Local FCP data storage",
                    },
                }
            ]
        },
        now=NOW,
        attestation_by_candidate_id=attestations,
    )

    assert len(planned) == 1
    announcement = planned[0].announcement
    assert announcement.status is CapabilityStatus.READY
    evidence = parse_storage_authority_evidence(announcement)
    assert evidence is not None
    assert evidence.benchmark_run_ids == (RUN_ID,)


def test_storage_attestation_still_fails_closed_until_local_assignment_is_active(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        publication,
        "evaluate_result",
        lambda *_args, **_kwargs: SimpleNamespace(current=True),
    )

    attestations = publication._auto_enrollment_attestations(
        contribution_service=_contribution_service(),
        candidates=(_candidate(),),
        intents=(_intent(activation_state="pending"),),
        node_id=NODE_ID,
        now=NOW,
    )

    assert attestations == {}


def test_storage_attestation_rejects_blocked_candidate_policy(monkeypatch) -> None:
    monkeypatch.setattr(
        publication,
        "evaluate_result",
        lambda *_args, **_kwargs: SimpleNamespace(current=True),
    )

    attestations = publication._auto_enrollment_attestations(
        contribution_service=_contribution_service(),
        candidates=(_candidate(policy_state="blocked"),),
        intents=(_intent(),),
        node_id=NODE_ID,
        now=NOW,
    )

    assert attestations == {}
