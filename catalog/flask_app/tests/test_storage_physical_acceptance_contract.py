from __future__ import annotations

from datetime import datetime, timezone

from flask import Flask

from catalog.capabilities.operator_surface import ProviderActivationState
from catalog.capabilities.storage_authority_enrollment import (
    STORAGE_AUTHORITY_SCHEMA,
    parse_storage_authority_evidence,
)
from catalog.federation.models import CapabilityAnnouncement, CapabilityStatus
from catalog.federation.storage_protocol import (
    STORAGE_PROTOCOL,
    STORAGE_PROTOCOL_VERSION,
)
from catalog.flask_app.services.local_capability_candidates import _build_bundle
from catalog.flask_app.services.pending_contribution_approval import (
    CapabilityFirstProviderOperatorSurface,
)

NOW = datetime(2026, 8, 19, 8, 50, tzinfo=timezone.utc)


def test_builtin_storage_candidate_matches_storage_authority_protocol(tmp_path) -> None:
    app = Flask(__name__)
    app.config["CAPABILITY_ONBOARDING_STORAGE_PROBE_DIRECTORY"] = str(tmp_path / "probe")
    with app.app_context():
        spec = _build_bundle().storage_spec

    assert spec.protocol == STORAGE_PROTOCOL
    assert spec.capacity_envelope["protocol_version"] == STORAGE_PROTOCOL_VERSION

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
            "storage_authority": {
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
            },
        },
        announced_at=NOW,
    )

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
