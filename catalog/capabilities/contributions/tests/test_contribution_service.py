from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

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
    protocol: str = "fcp-synthetic"
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
        self.calls: list[bool] = []

    def set_enabled(self, enabled, settings):
        self.enabled = enabled
        self.calls.append(enabled)
        return True, "recorder state changed"

    def status(self, settings):
        return {"requested_enabled": self.enabled}


class Harness:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.current = [NOW]
        self.recorder = RecorderAuthority()
        self.ai_active: set[str] = set()
        self.compute_active: set[tuple[str, str]] = set()
        self.storage_assigned: set[str] = set()
        self.compute_fences: list[tuple[str, str]] = []
        self.storage_fences: list[str] = []
        self.membership = {"device-1"}
        self.inventory = Inventory()

    def build(self):
        generator = ContributionCandidateGenerator(
            sources=(
                RecorderCandidateSource(),
                AICandidateSource(
                    {
                        "ai-local": AICandidateSpec(
                            "ai-local",
                            "ollama",
                            "Local AI",
                            {},
                        )
                    }
                ),
                ComputeCandidateSource(self.inventory),
                StorageCandidateSource(
                    {
                        "storage-local": StorageCandidateSpec(
                            "storage-local",
                            "fcp-storage",
                            "Local storage",
                            {},
                        )
                    }
                ),
            ),
            now=lambda: self.current[0],
        )
        store = SQLiteContributionIntentStore(self.path)
        service = ContributionService(
            generator=generator,
            store=store,
            policy=ContributionPolicyEvaluator(),
            adapters=(
                RecorderContributionAdapter(
                    self.recorder,
                    settings_provider=lambda: object(),
                ),
                AIContributionAdapter(
                    enable_provider=lambda candidate: self.ai_active.add(
                        candidate.capacity_envelope["provider_id"]
                    ),
                    disable_provider=self.ai_active.discard,
                    is_provider_active=lambda provider_id: (
                        provider_id in self.ai_active
                    ),
                ),
                ComputeContributionAdapter(
                    self.inventory,
                    activate_binding=self._activate_compute,
                    fence_handler=self._fence_compute,
                    is_handler_active=lambda handler_id, fingerprint: (
                        (handler_id, fingerprint) in self.compute_active
                    ),
                ),
                StorageContributionAdapter(
                    is_assigned=lambda provider_id: (
                        provider_id in self.storage_assigned
                    ),
                    fence_candidate=self.storage_fences.append,
                ),
            ),
            now=lambda: self.current[0],
        )
        candidates = {
            candidate.capability_type: candidate
            for candidate in service.recommend(snapshot(), ())
        }
        return service, candidates, store

    def reset_runtime(self) -> None:
        self.recorder.enabled = False
        self.recorder.calls.clear()
        self.ai_active.clear()
        self.compute_active.clear()
        self.compute_fences.clear()
        self.storage_fences.clear()

    def _activate_compute(self, binding) -> None:
        self.compute_active.add(
            (
                binding.descriptor.handler_id,
                binding.descriptor.descriptor_fingerprint,
            )
        )

    def _fence_compute(self, handler_id: str, fingerprint: str) -> None:
        identity = (handler_id, fingerprint)
        self.compute_fences.append(identity)
        self.compute_active.discard(identity)


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


def test_simultaneous_capabilities_are_independent_and_ai_grants_nothing_else(
    tmp_path,
) -> None:
    harness = Harness(tmp_path / "intents.sqlite3")
    service, candidates, store = harness.build()
    try:
        ai = service.enable(candidates["language-model"].candidate_id)
        assert ai.activation_state is ContributionActivationState.ACTIVE
        assert harness.ai_active == {"ai-local"}
        assert not harness.compute_active
        assert not harness.storage_assigned
        assert harness.recorder.calls == []

        recorder_intent = service.enable(candidates["recorder"].candidate_id)
        compute = service.enable(candidates["compute"].candidate_id)
        assert recorder_intent.activation_state is ContributionActivationState.ACTIVE
        assert compute.activation_state is ContributionActivationState.ACTIVE
        assert harness.recorder.enabled
        assert harness.compute_active
        assert harness.ai_active == {"ai-local"}
    finally:
        store.close()


def test_storage_is_candidate_only_until_control_plane_assignment(tmp_path) -> None:
    harness = Harness(tmp_path / "intents.sqlite3")
    service, candidates, store = harness.build()
    try:
        storage = candidates["storage"]
        pending = service.enable(storage.candidate_id)
        assert pending.policy_state is ContributionPolicyState.APPROVAL_REQUIRED
        assert pending.activation_state is ContributionActivationState.PENDING
        assert not harness.storage_assigned

        harness.storage_assigned.add("storage-local")
        reconciled = {item.candidate_id: item for item in service.reconcile()}
        active = reconciled[storage.candidate_id]
        assert active.policy_state is ContributionPolicyState.ALLOWED
        assert active.activation_state is ContributionActivationState.ACTIVE
    finally:
        store.close()


def test_disable_and_suspend_fence_future_use_without_membership_deletion(
    tmp_path,
) -> None:
    harness = Harness(tmp_path / "intents.sqlite3")
    service, candidates, store = harness.build()
    try:
        membership_before = set(harness.membership)
        compute_id = candidates["compute"].candidate_id
        ai_id = candidates["language-model"].candidate_id

        service.enable(compute_id)
        service.enable(ai_id)
        disabled = service.disable(compute_id)
        suspended = service.suspend(ai_id, reason="operator suspension")

        assert disabled.desired_state is ContributionDesiredState.DISABLED
        assert disabled.activation_state is ContributionActivationState.INACTIVE
        assert harness.compute_fences
        assert not harness.compute_active
        assert suspended.desired_state is ContributionDesiredState.ENABLED
        assert suspended.activation_state is ContributionActivationState.SUSPENDED
        assert not harness.ai_active
        assert harness.membership == membership_before
        assert harness.recorder.calls == []
    finally:
        store.close()


def test_expired_evidence_suspends_and_still_allows_disable(tmp_path) -> None:
    harness = Harness(tmp_path / "intents.sqlite3")
    service, candidates, store = harness.build()
    try:
        compute_id = candidates["compute"].candidate_id
        service.enable(compute_id)

        harness.current[0] = NOW + timedelta(minutes=11)
        reconciled = {item.candidate_id: item for item in service.reconcile()}

        assert reconciled[compute_id].activation_state is (
            ContributionActivationState.SUSPENDED
        )
        assert not harness.compute_active
        assert harness.compute_fences

        disabled = service.disable(compute_id)
        assert disabled.desired_state is ContributionDesiredState.DISABLED
        assert disabled.activation_state is ContributionActivationState.INACTIVE
    finally:
        store.close()


def test_suspend_before_enable_does_not_create_enabled_intent(tmp_path) -> None:
    harness = Harness(tmp_path / "intents.sqlite3")
    service, candidates, store = harness.build()
    try:
        candidate_id = candidates["language-model"].candidate_id
        suspended = service.suspend(candidate_id, reason="pre-activation safety fence")

        assert suspended.desired_state is ContributionDesiredState.ASK_LATER
        assert suspended.policy_state is ContributionPolicyState.NOT_EVALUATED
        assert suspended.activation_state is ContributionActivationState.SUSPENDED
        assert store.get(candidate_id) == suspended
        assert not harness.ai_active
    finally:
        store.close()


def test_restart_reconcile_does_not_activate_never_enabled_candidate(tmp_path) -> None:
    harness = Harness(tmp_path / "intents.sqlite3")
    membership_before = set(harness.membership)
    service, candidates, store = harness.build()
    candidate_id = candidates["language-model"].candidate_id
    service.suspend(candidate_id, reason="pre-activation safety fence")
    store.close()

    harness.reset_runtime()
    restarted, _, restarted_store = harness.build()
    try:
        reconciled = {item.candidate_id: item for item in restarted.reconcile()}
        current = reconciled[candidate_id]

        assert current.desired_state is ContributionDesiredState.ASK_LATER
        assert current.activation_state is ContributionActivationState.INACTIVE
        assert not harness.ai_active
        assert harness.membership == membership_before
    finally:
        restarted_store.close()


def test_restart_reconcile_restores_explicitly_enabled_contribution(tmp_path) -> None:
    harness = Harness(tmp_path / "intents.sqlite3")
    membership_before = set(harness.membership)
    service, candidates, store = harness.build()
    candidate_id = candidates["language-model"].candidate_id
    enabled = service.enable(candidate_id)
    assert enabled.desired_state is ContributionDesiredState.ENABLED
    assert enabled.activation_state is ContributionActivationState.ACTIVE
    store.close()

    harness.reset_runtime()
    restarted, _, restarted_store = harness.build()
    try:
        assert not harness.ai_active
        reconciled = {item.candidate_id: item for item in restarted.reconcile()}
        restored = reconciled[candidate_id]

        assert restored.desired_state is ContributionDesiredState.ENABLED
        assert restored.policy_state is ContributionPolicyState.ALLOWED
        assert restored.activation_state is ContributionActivationState.ACTIVE
        assert harness.ai_active == {"ai-local"}
        assert harness.membership == membership_before
    finally:
        restarted_store.close()


def test_disabled_and_ask_later_remain_fenced_after_restart(tmp_path) -> None:
    harness = Harness(tmp_path / "intents.sqlite3")
    membership_before = set(harness.membership)
    service, candidates, store = harness.build()
    compute_id = candidates["compute"].candidate_id
    ai_id = candidates["language-model"].candidate_id

    service.enable(compute_id)
    service.enable(ai_id)
    service.disable(compute_id)
    service.ask_later(ai_id)
    store.close()

    harness.reset_runtime()
    restarted, _, restarted_store = harness.build()
    try:
        reconciled = {item.candidate_id: item for item in restarted.reconcile()}

        assert reconciled[compute_id].desired_state is (
            ContributionDesiredState.DISABLED
        )
        assert reconciled[compute_id].activation_state is (
            ContributionActivationState.INACTIVE
        )
        assert reconciled[ai_id].desired_state is ContributionDesiredState.ASK_LATER
        assert reconciled[ai_id].activation_state is (
            ContributionActivationState.INACTIVE
        )
        assert harness.compute_fences
        assert not harness.compute_active
        assert not harness.ai_active
        assert harness.membership == membership_before
    finally:
        restarted_store.close()


def test_restart_reconciliation_does_not_change_membership(tmp_path) -> None:
    harness = Harness(tmp_path / "intents.sqlite3")
    membership_before = set(harness.membership)
    service, candidates, store = harness.build()
    candidate_id = candidates["recorder"].candidate_id

    service.enable(candidate_id)
    service.suspend(candidate_id, reason="temporary recorder fence")
    assert harness.membership == membership_before
    store.close()

    harness.reset_runtime()
    restarted, _, restarted_store = harness.build()
    try:
        restarted.reconcile()
        assert harness.membership == membership_before
    finally:
        restarted_store.close()
