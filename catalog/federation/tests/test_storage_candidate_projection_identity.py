from __future__ import annotations

from datetime import datetime, timezone

from catalog.federation.projections import (
    ContributionRecord,
    FederationProjectionService,
    OnboardingSnapshot,
    ProjectionAdapters,
    StorageAuthoritySnapshot,
    StorageGroupRecord,
    StorageProviderRecord,
)

NOW = datetime(2026, 8, 11, 9, tzinfo=timezone.utc)


class StaticAdapter:
    def __init__(self, value: object) -> None:
        self.value = value

    def snapshot(self) -> object:
        return self.value


def _candidate(provider_id: str) -> ContributionRecord:
    return ContributionRecord(
        candidate_id="candidate-storage-decision-id",
        device_id="node-owner",
        capability_type="storage",
        protocol="fcp-storage-candidate",
        label="Local FCP data storage",
        capacity={
            "kind": "host-mounted-local-data",
            "provider_id": provider_id,
        },
        missing_prerequisites=(),
        policy_state="allowed",
        desired_state="enabled",
        activation_state="active",
        reason="Authorized by the federation creator.",
    )


def _service(candidate_provider_id: str) -> FederationProjectionService:
    onboarding = OnboardingSnapshot(
        available=True,
        reason_code="current",
        federation_id="federation-one",
        connection_state="connected",
        trusted=True,
        device_id="node-owner",
        contributions=(_candidate(candidate_provider_id),),
    )
    storage = StorageAuthoritySnapshot(
        available=True,
        reason_code="current",
        revision=7,
        providers=(
            StorageProviderRecord(
                "fcp-local-data-storage",
                "node-owner",
                "fcp-storage-v1",
                "1.0",
                "ready",
                True,
                True,
                ("primary",),
            ),
        ),
        groups=(
            StorageGroupRecord(
                "fcp-local-storage",
                "fcp-local-data-storage",
                (),
                True,
            ),
        ),
    )
    return FederationProjectionService(
        ProjectionAdapters(
            onboarding=StaticAdapter(onboarding),
            storage=StaticAdapter(storage),
        ),
        now=lambda: NOW,
    )


def test_authoritative_provider_suppresses_different_candidate_decision_id() -> None:
    page = _service("fcp-local-data-storage").storage().to_dict()

    keys = {item["key"] for item in page["items"]}
    assert "fcp-local-storage" in keys
    assert "fcp-local-data-storage" in keys
    assert "candidate-storage-decision-id" not in keys
    assert not any(
        item["state_label"] == "Candidate only"
        for item in page["items"]
    )


def test_unassigned_provider_identity_remains_visible_as_candidate_only() -> None:
    page = _service("different-storage-provider").storage().to_dict()

    candidates = [
        item
        for item in page["items"]
        if item["key"] == "candidate-storage-decision-id"
    ]
    assert len(candidates) == 1
    assert candidates[0]["state"] == "candidate"
    assert candidates[0]["state_label"] == "Candidate only"
