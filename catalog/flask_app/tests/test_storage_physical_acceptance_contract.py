from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from flask import Flask

from catalog.capabilities.benchmarking.adapters.storage import StorageCandidateAdapter
from catalog.capabilities.contributions.candidates import _candidate_id
from catalog.capabilities.contributions.types import LocalContributionDescriptor
from catalog.capabilities.operator_surface import ProviderActivationState
from catalog.capabilities.storage_authority_enrollment import (
    STORAGE_AUTHORITY_PROPERTY,
    STORAGE_AUTHORITY_SCHEMA,
    parse_storage_authority_evidence,
)
from catalog.federation.models import CapabilityAnnouncement, CapabilityStatus
from catalog.federation.storage_protocol import (
    STORAGE_PROTOCOL,
    STORAGE_PROTOCOL_VERSION,
)
from catalog.flask_app.services.federation_contribution_publication import (
    plan_contribution_publications,
)
from catalog.flask_app.services.local_capability_candidates import _build_bundle
from catalog.flask_app.services.pending_contribution_approval import (
    CapabilityFirstProviderOperatorSurface,
)

NOW = datetime(2026, 8, 19, 8, 50, tzinfo=timezone.utc)
_LEGACY_STORAGE_CANDIDATE_PROTOCOL = "fcp-storage-candidate"


def _storage_authority_attestation() -> dict[str, dict[str, object]]:
    return {
        STORAGE_AUTHORITY_PROPERTY: {
            "schema": STORAGE_AUTHORITY_SCHEMA,
            "eligible": True,
            "benchmark_state": "green",
            "provider_id": "storage-provider-a",
            "group_id": "fcp-local-storage",
            "role": "primary",
            "inspection_revision": 1,
            "decision_revision": 1,
            "benchmark_run_ids": ["storage-green-1"],
            "desired_state": "enabled",
            "policy_state": "allowed",
        }
    }


def test_builtin_storage_candidate_matches_storage_authority_protocol(tmp_path) -> None:
    app = Flask(__name__)
    app.config["CAPABILITY_ONBOARDING_STORAGE_PROBE_DIRECTORY"] = str(tmp_path / "probe")
    with app.app_context():
        spec = _build_bundle().storage_spec

    assert spec.protocol == STORAGE_PROTOCOL
    assert spec.capacity_envelope["protocol_version"] == STORAGE_PROTOCOL_VERSION
    assert StorageCandidateAdapter.definition.capability_protocol == STORAGE_PROTOCOL

    announcement = CapabilityAnnouncement(
        capability_id="candidate-storage",
        node_id="node-a",
        session_id="session-a",
        type="storage",
        protocol=spec.protocol,
        protocol_version=str(spec.capacity_envelope["protocol_version"]),
        status=CapabilityStatus.READY,
        properties={
            "kind": "capability-first-candidate",
            "candidate_id": "fcp-local-data-storage",
            **_storage_authority_attestation(),
        },
        announced_at=NOW,
    )

    assert parse_storage_authority_evidence(announcement) is not None


def _storage_descriptors(tmp_path):
    app = Flask(__name__)
    app.config["CAPABILITY_ONBOARDING_STORAGE_PROBE_DIRECTORY"] = str(tmp_path / "probe")
    with app.app_context():
        spec = _build_bundle().storage_spec

    shared = {
        "logical_service_id": spec.provider_id,
        "capability_type": "storage",
        "display_label": spec.display_label,
        "capacity_envelope": dict(spec.capacity_envelope),
    }
    return (
        spec,
        LocalContributionDescriptor(
            capability_protocol=_LEGACY_STORAGE_CANDIDATE_PROTOCOL,
            **shared,
        ),
        LocalContributionDescriptor(
            capability_protocol=STORAGE_PROTOCOL,
            **shared,
        ),
    )


def test_storage_protocol_correction_preserves_existing_candidate_identity(tmp_path) -> None:
    _spec, legacy, corrected = _storage_descriptors(tmp_path)

    assert _candidate_id("node-a", corrected) == _candidate_id("node-a", legacy)


def test_existing_storage_capability_upgrades_in_place_to_authority_protocol(tmp_path) -> None:
    spec, legacy, corrected = _storage_descriptors(tmp_path)
    candidate_id = _candidate_id("node-a", legacy)
    assert _candidate_id("node-a", corrected) == candidate_id

    candidate = SimpleNamespace(
        candidate_id=candidate_id,
        device_id="node-a",
        capability_type="storage",
        capability_protocol=STORAGE_PROTOCOL,
        display_label=spec.display_label,
        capacity_envelope=dict(spec.capacity_envelope),
    )
    intent = SimpleNamespace(
        candidate_id=candidate_id,
        device_id="node-a",
        activation_state="active",
        decision_revision=1,
        decided_at=NOW,
    )
    authorized_status = {
        "capabilities": [
            {
                "session_id": "session-a",
                "capability_id": candidate_id,
                "node_id": "node-a",
                "type": "storage",
                "protocol": _LEGACY_STORAGE_CANDIDATE_PROTOCOL,
                "protocol_version": "1.0",
                "status": "ready",
                "properties": {
                    "kind": "capability-first-candidate",
                    "candidate_id": candidate_id,
                },
            }
        ]
    }

    planned = plan_contribution_publications(
        session_id="session-a",
        node_id="node-a",
        candidates=(candidate,),
        intents=(intent,),
        authorized_status=authorized_status,
        now=NOW,
        attestation_by_candidate_id={
            candidate_id: _storage_authority_attestation(),
        },
    )

    assert len(planned) == 1
    announcement = planned[0].announcement
    assert announcement.capability_id == candidate_id
    assert announcement.protocol == STORAGE_PROTOCOL
    assert announcement.protocol_version == STORAGE_PROTOCOL_VERSION
    assert announcement.status is CapabilityStatus.READY
    assert parse_storage_authority_evidence(announcement) is not None


def test_storage_capabilities_never_offer_generic_provider_actions() -> None:
    for capability_type, protocol, version, expected_reason in (
        ("storage", STORAGE_PROTOCOL, STORAGE_PROTOCOL_VERSION, "storage-control-plane-separate"),
        ("storage-control", "fcp.storage-control", "1", "logical-storage-authority-separate"),
    ):
        announcement = CapabilityAnnouncement(
            capability_id=f"candidate-{capability_type}",
            node_id="node-a",
            session_id="session-a",
            type=capability_type,
            protocol=protocol,
            protocol_version=version,
            status=CapabilityStatus.UNAVAILABLE,
            properties={},
            announced_at=NOW,
        )

        assert CapabilityFirstProviderOperatorSurface._allowed_actions(
            is_owner=True,
            announcement=announcement,
            enrollment=None,
        ) == ()

        surface = object.__new__(CapabilityFirstProviderOperatorSurface)
        state, reason, compatible = surface._activation(
            capability_id=announcement.capability_id,
            capability_type=capability_type,
            health_state=None,  # ignored for storage-owned authority
            enrollment=None,
        )
        assert state is ProviderActivationState.NOT_APPLICABLE
        assert reason == expected_reason
        assert compatible is None
