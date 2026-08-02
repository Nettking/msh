from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from catalog.capabilities.contributions import (
    AICandidateSource,
    AICandidateSpec,
    AIContributionAdapter,
    ComputeCandidateSource,
    ComputeContributionAdapter,
    ContributionCandidateGenerator,
    ContributionPolicyEvaluator,
    ContributionService,
    RecorderCandidateSource,
    RecorderContributionAdapter,
    SQLiteContributionIntentStore,
    StorageCandidateSource,
    StorageCandidateSpec,
    StorageContributionAdapter,
)
from catalog.federation.onboarding_models import (
    ContributionActivationState,
    ContributionDesiredState,
    ContributionPolicyState,
    DeviceInspectionSnapshot,
)

NOW = datetime(2026, 8, 2, 20, tzinfo=timezone.utc)


@dataclass(frozen=True)
class Descriptor:
    handler_id: str = "safe-handler"
    capability_type: str = "synthetic-compute"
    protocol: str = "msh-synthetic"
    protocol_version: str = "1.0"
    descriptor_fingerprint: str = "sha256:" + "c" * 64


@dataclass(frozen=True)
class Binding:
    descriptor: Descriptor = Descriptor()


class Inventory:
    def __init__(self) -> None:
        self.binding = Binding()

    def list_bindings(self):
        return (self.binding,)

    def get(self, handler_id):
        return self.binding if handler_id == "safe-handler" else None


class RecorderAuthority:
    def __init__(self) -> None:
        self.enabled = False
        self.calls = []

    def set_enabled(self, enabled, settings):
        self.enabled = enabled
        self.calls.append(enabled)
        return True, "recorder state changed"

    def status(self, settings):
        return {"requested_enabled": self.enabled}


def snapshot() -> DeviceInspectionSnapshot:
    return DeviceInspectionSnapshot(
        device_id="device-1",
        revision=1,
        os_family="linux",
        architecture="x86_64",
        resource_observations={},
        detected_services=("ai-local", "storage-local"),
        registered_handlers=("safe-handler",),
        detected_data_sources=("machine-one",),
        recommended_benchmark_ids=(),
        warnings=(),
        created_at=NOW,
        expires_at=NOW + timedelta(minutes=10),
    )


def environment(tmp_path):
    current = [NOW]
    recorder = RecorderAuthority()
    ai_active = set()
    compute_active = set()
    storage_assigned = set()
    fences = {"compute": [], "storage": []}
    inventory = Inventory()
    generator = ContributionCandidateGenerator(
        sources=(
            RecorderCandidateSource(),
            AICandidateSource(
                {
                    "ai-local": AICandidateSpec(
                        "ai-local", "ollama", "Local AI", {}
                    )
                }
            ),
            ComputeCandidateSource(inventory),
            StorageCandidateSource(
                {
                    "storage-local": StorageCandidateSpec(
                        "storage-local", "msh-storage", "Local storage", {}
                    )
                }
            ),
        ),
        now=lambda: current[0],
    )
    service = ContributionService(
        generator=generator,
        store=SQLiteContributionIntentStore(tmp_path / "intents.sqlite3"),
        policy=ContributionPolicyEvaluator(),
        adapters=(
            RecorderContributionAdapter(recorder, settings_provider=lambda: object()),
            AIContributionAdapter(
                enable_provider=lambda candidate: ai_active.add(
                    candidate.capacity_envelope["provider_id"]
                ),
                disable_provider=ai_active.discard,
                is_provider_active=lambda provider_id: provider_id in ai_active,
            ),
            ComputeContributionAdapter(
                inventory,
                activate_binding=lambda binding: compute_active.add(
                    (
                        binding.descriptor.handler_id,
                        binding.descriptor.descriptor_fingerprint,
                    )
                ),
                fence_handler=lambda handler_id, fingerprint: (
                    fences["compute"].append((handler_id, fingerprint)),
                    compute_active.discard((handler_id, fingerprint)),
                ),
                is_handler_active=lambda handler_id, fingerprint: (
                    (handler_id, fingerprint) in compute_active
                ),
            ),
            StorageContributionAdapter(
                is_assigned=lambda provider_id: provider_id in storage_assigned,
                fence_candidate=lambda provider_id: fences["storage"].append(
                    provider_id
                ),
            ),
        ),
        now=lambda: current[0],
    )
    candidates = {
        candidate.capability_type: candidate
        for candidate in service.recommend(snapshot(), ())
    }
    return (
        service,
        candidates,
        recorder,
        ai_active,
        compute_active,
        storage_assigned,
        fences,
        current,
    )


def test_simultaneous_capabilities_are_independent_and_ai_grants_nothing_else(
    tmp_path,
) -> None:
    (
        service,
        candidates,
        recorder,
        ai_active,
        compute_active,
        storage_assigned,
        _,
        _,
    ) = environment(tmp_path)

    ai = service.enable(candidates["language-model"].candidate_id)
    assert ai.activation_state is ContributionActivationState.ACTIVE
    assert ai_active == {"ai-local"}
    assert not compute_active
    assert not storage_assigned
    assert recorder.calls == []

    recorder_intent = service.enable(candidates["recorder"].candidate_id)
    compute = service.enable(candidates["compute"].candidate_id)
    assert recorder_intent.activation_state is ContributionActivationState.ACTIVE
    assert compute.activation_state is ContributionActivationState.ACTIVE
    assert recorder.enabled
    assert compute_active
    assert ai_active == {"ai-local"}


def test_storage_is_candidate_only_until_control_plane_assignment(tmp_path) -> None:
    service, candidates, _, _, _, storage_assigned, _, _ = environment(tmp_path)
    storage = candidates["storage"]

    pending = service.enable(storage.candidate_id)
    assert pending.policy_state is ContributionPolicyState.APPROVAL_REQUIRED
    assert pending.activation_state is ContributionActivationState.PENDING
    assert not storage_assigned

    storage_assigned.add("storage-local")
    reconciled = {item.candidate_id: item for item in service.reconcile()}
    active = reconciled[storage.candidate_id]
    assert active.policy_state is ContributionPolicyState.ALLOWED
    assert active.activation_state is ContributionActivationState.ACTIVE


def test_disable_and_suspend_fence_future_use_without_membership_deletion(tmp_path) -> None:
    service, candidates, recorder, ai_active, compute_active, _, fences, _ = environment(
        tmp_path
    )
    membership = {"device-1"}
    compute_id = candidates["compute"].candidate_id
    ai_id = candidates["language-model"].candidate_id

    service.enable(compute_id)
    service.enable(ai_id)
    disabled = service.disable(compute_id)
    suspended = service.suspend(ai_id, reason="operator suspension")

    assert disabled.desired_state is ContributionDesiredState.DISABLED
    assert disabled.activation_state is ContributionActivationState.INACTIVE
    assert fences["compute"]
    assert not compute_active
    assert suspended.activation_state is ContributionActivationState.SUSPENDED
    assert not ai_active
    assert membership == {"device-1"}
    assert recorder.calls == []


def test_expired_evidence_suspends_and_still_allows_disable(tmp_path) -> None:
    service, candidates, _, _, compute_active, _, fences, current = environment(
        tmp_path
    )
    compute_id = candidates["compute"].candidate_id
    service.enable(compute_id)

    current[0] = NOW + timedelta(minutes=11)
    reconciled = {item.candidate_id: item for item in service.reconcile()}

    assert reconciled[compute_id].activation_state is ContributionActivationState.SUSPENDED
    assert not compute_active
    assert fences["compute"]

    disabled = service.disable(compute_id)
    assert disabled.desired_state is ContributionDesiredState.DISABLED
    assert disabled.activation_state is ContributionActivationState.INACTIVE
