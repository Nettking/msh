from __future__ import annotations

from types import SimpleNamespace

from catalog.federation.projections.onboarding_adapter import (
    OnboardingContractsAdapter,
)


def test_active_contribution_projects_persisted_allowed_policy() -> None:
    candidate = SimpleNamespace(
        candidate_id="candidate-storage",
        device_id="node-local",
        capability_type="storage",
        capability_protocol="msh-storage-candidate",
        display_label="Local MSH data storage",
        capacity_envelope={"provider_id": "msh-local-data-storage"},
        missing_prerequisites=(),
        policy_state="approval-required",
    )
    intent = SimpleNamespace(
        candidate_id="candidate-storage",
        policy_state="allowed",
        desired_state="enabled",
        activation_state="active",
        reason=(
            "Authorized by the federation creator and assigned as the initial "
            "storage primary."
        ),
    )
    binding = SimpleNamespace(
        federation_id="federation-local",
        device_id="node-local",
        state="connected",
        trusted=True,
    )

    snapshot = OnboardingContractsAdapter(
        binding=binding,
        candidates=(candidate,),
        intents=(intent,),
    ).snapshot()

    assert snapshot.available is True
    assert len(snapshot.contributions) == 1
    contribution = snapshot.contributions[0]
    assert contribution.policy_state == "allowed"
    assert contribution.desired_state == "enabled"
    assert contribution.activation_state == "active"
