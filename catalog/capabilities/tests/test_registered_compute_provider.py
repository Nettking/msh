from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from catalog.capabilities.analysis.contracts import (
    analysis_capability_requirement,
    analysis_provider_attributes,
    analysis_retry_policy,
    analysis_timeout_policy,
)
from catalog.capabilities.job_store import SQLiteJobStore
from catalog.capabilities.jobs import JobContract
from catalog.capabilities.provider_enrollment import (
    FederatedProviderEnrollmentService,
    ProviderEnrollmentState,
    SQLiteProviderEnrollmentStore,
)
from catalog.capabilities.provider_health import (
    FederatedProviderHealthService,
    SQLiteProviderHealthStore,
)
from catalog.capabilities.provider_reports import ProviderStatus
from catalog.capabilities.provider_selection import select_provider
from catalog.capabilities.registered_compute_provider import RegisteredComputeProviderRuntime
from catalog.capabilities.worker_activation import LocalComputeHandlerDescriptor
from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.errors import FederationOperationError, FederationValidationError
from catalog.federation.onboarding_models import (
    ContributionActivationState,
    ContributionCandidate,
    ContributionDesiredState,
    ContributionIntent,
    ContributionPolicyState,
)
from catalog.node.identity import IdentityStore

NOW = datetime(2026, 8, 31, 12, 30, tzinfo=timezone.utc)
SESSION_ID = "session-compute-provider"
HANDLER_ID = "background-analysis"
PROTOCOL = "fcp-background-analysis"


def _descriptor(*, revision: int = 1) -> LocalComputeHandlerDescriptor:
    return LocalComputeHandlerDescriptor(
        handler_id=HANDLER_ID,
        capability_type="background-analysis",
        protocol=PROTOCOL,
        protocol_version="1.0",
        attributes=analysis_provider_attributes(handler_revision=revision),
    )


def _candidate(
    node_id: str,
    *,
    descriptor_revision: int = 1,
    inspection_revision: int = 1,
) -> ContributionCandidate:
    descriptor = _descriptor(revision=descriptor_revision)
    return ContributionCandidate(
        candidate_id="candidate-compute-provider",
        device_id=node_id,
        capability_type="compute",
        capability_protocol=descriptor.protocol,
        display_label="Background analysis",
        inspection_revision=inspection_revision,
        benchmark_run_ids=(),
        capacity_envelope={
            "kind": "registered-compute-handler",
            "handler_id": descriptor.handler_id,
            "handler_capability_type": descriptor.capability_type,
            "protocol_version": descriptor.protocol_version,
            "descriptor_fingerprint": descriptor.descriptor_fingerprint,
            "handler_attributes": descriptor.attributes,
        },
        missing_prerequisites=(),
        policy_state=ContributionPolicyState.ALLOWED,
        created_at=NOW,
        expires_at=NOW + timedelta(minutes=15),
    )


def _intent(node_id: str, *, active: bool, revision: int) -> ContributionIntent:
    return ContributionIntent(
        candidate_id="candidate-compute-provider",
        device_id=node_id,
        desired_state=(
            ContributionDesiredState.ENABLED
            if active
            else ContributionDesiredState.DISABLED
        ),
        decision_revision=revision,
        policy_state=ContributionPolicyState.ALLOWED,
        activation_state=(
            ContributionActivationState.ACTIVE
            if active
            else ContributionActivationState.INACTIVE
        ),
        reason=None,
        decided_at=NOW + timedelta(seconds=revision - 1),
    )


def _environment(tmp_path: Path):
    current = [NOW]
    coordinator = SessionCoordinator(
        tmp_path / "coordinator.sqlite3",
        clock=lambda: current[0],
    )
    owner = IdentityStore(tmp_path / "owner", display_name="Owner").create(now=NOW)
    provider = IdentityStore(tmp_path / "provider", display_name="Provider").create(now=NOW)
    for credentials in (owner, provider):
        token = coordinator.create_enrollment_token(ttl_seconds=60, max_uses=1)["token"]
        coordinator.enroll_node(credentials.identity, token=token)
    coordinator.create_session(
        actor_node_id=owner.identity.node_id,
        display_name="Compute provider integration",
        request_id="create-session",
        session_id=SESSION_ID,
    )
    invitation = coordinator.create_invitation(
        session_id=SESSION_ID,
        actor_node_id=owner.identity.node_id,
        ttl_seconds=60,
        max_uses=1,
        request_id="invite-provider",
    )
    coordinator.join_session(
        node_id=provider.identity.node_id,
        token=invitation["token"],
        request_id="join-provider",
        expected_session_id=SESSION_ID,
    )
    enrollment_service = FederatedProviderEnrollmentService(
        coordinator,
        SQLiteProviderEnrollmentStore(tmp_path / "enrollment.sqlite3"),
        clock=lambda: current[0],
    )
    health_service = FederatedProviderHealthService(
        enrollment_service,
        SQLiteProviderHealthStore(tmp_path / "health.sqlite3"),
        clock=lambda: current[0],
    )
    runtime = RegisteredComputeProviderRuntime(
        enrollments=enrollment_service,
        health=health_service,
        clock=lambda: current[0],
    )
    return current, coordinator, owner, provider, enrollment_service, health_service, runtime


def _job() -> JobContract:
    return JobContract(
        job_id="job-compute-provider",
        session_id=SESSION_ID,
        request_id="request-compute-provider",
        idempotency_key="compute-provider-integration",
        capability=analysis_capability_requirement(),
        inputs=(),
        outputs=(),
        retry_policy=analysis_retry_policy(),
        timeout_policy=analysis_timeout_policy(),
    )


def test_active_compute_contribution_reaches_health_selection_and_ownership(
    tmp_path: Path,
) -> None:
    (
        current,
        _coordinator,
        owner,
        provider,
        enrollments,
        health,
        runtime,
    ) = _environment(tmp_path)
    candidate = _candidate(provider.identity.node_id)
    active = _intent(provider.identity.node_id, active=True, revision=1)

    binding, announcement, enrollment = runtime.reconcile_contribution(
        candidate,
        active,
        session_id=SESSION_ID,
        node_id=provider.identity.node_id,
    )
    assert announcement.status.value == "ready"
    assert enrollment is not None
    assert enrollment.state is ProviderEnrollmentState.PENDING
    assert enrollment.capability_id == binding.capability_id
    assert binding.descriptor.attributes == analysis_provider_attributes(handler_revision=1)

    current[0] += timedelta(seconds=10)
    replay_binding, replay_announcement, replay_enrollment = runtime.reconcile_contribution(
        candidate,
        active,
        session_id=SESSION_ID,
        node_id=provider.identity.node_id,
    )
    assert replay_binding == binding
    assert replay_announcement == announcement
    assert replay_enrollment == enrollment
    assert replay_enrollment.revision == 1

    with pytest.raises(FederationOperationError) as before_approval:
        runtime.publish_health(binding)
    assert before_approval.value.code == "provider-enrollment-ineligible"

    approved = enrollments.approve(
        session_id=SESSION_ID,
        capability_id=binding.capability_id,
        actor_node_id=owner.identity.node_id,
        command_id="approve-compute-provider",
        expected_revision=enrollment.revision,
    )
    assert approved.state is ProviderEnrollmentState.APPROVED

    health_record = runtime.publish_health(
        binding,
        status=ProviderStatus.READY,
        max_concurrent_jobs=2,
        utilization_millis=100,
    )
    assert health_record.report.capability_id == binding.capability_id
    assert health_record.report.attributes == analysis_provider_attributes(handler_revision=1)
    reports = health.fresh_reports(
        session_id=SESSION_ID,
        actor_node_id=owner.identity.node_id,
        capability_type="background-analysis",
    )
    assert reports == (health_record.report,)

    job = _job()
    selection = select_provider(job, reports, evaluated_at=current[0])
    assert selection.decision == "selected"
    assert selection.selected_capability_id == binding.capability_id

    store = SQLiteJobStore(tmp_path / "jobs.sqlite3")
    submitted = store.submit(job, coordinator_id=owner.identity.node_id, now=current[0])
    queued = store.queue(
        job.job_id,
        coordinator_id=owner.identity.node_id,
        command_id="queue-compute-provider",
        expected_revision=submitted.snapshot.revision,
        now=current[0],
    )
    assert queued.snapshot.ownership is None
    claimed = store.claim(
        job.job_id,
        coordinator_id=owner.identity.node_id,
        owner_provider_id=binding.capability_id,
        attempt_id="attempt-compute-provider",
        lease_id="lease-compute-provider",
        command_id="claim-compute-provider",
        expected_revision=queued.snapshot.revision,
        lease_expires_at=current[0] + timedelta(minutes=5),
        now=current[0],
    )
    assert claimed.snapshot.ownership is not None
    assert claimed.snapshot.ownership.owner_provider_id == binding.capability_id

    current[0] = NOW + timedelta(seconds=20)
    disabled = _intent(provider.identity.node_id, active=False, revision=2)
    rebound, disabled_announcement, disabled_enrollment = runtime.reconcile_contribution(
        candidate,
        disabled,
        session_id=SESSION_ID,
        node_id=provider.identity.node_id,
    )
    assert rebound == binding
    assert disabled_announcement.status.value == "disabled"
    assert disabled_enrollment is None
    assert health.fresh_reports(
        session_id=SESSION_ID,
        actor_node_id=owner.identity.node_id,
        capability_type="background-analysis",
    ) == ()

    current[0] = NOW + timedelta(seconds=30)
    reenabled = _intent(provider.identity.node_id, active=True, revision=3)
    _, reenabled_announcement, reenabled_enrollment = runtime.reconcile_contribution(
        candidate,
        reenabled,
        session_id=SESSION_ID,
        node_id=provider.identity.node_id,
    )
    assert reenabled_announcement.status.value == "ready"
    assert reenabled_enrollment is not None
    assert reenabled_enrollment.state is ProviderEnrollmentState.APPROVED
    assert health.fresh_reports(
        session_id=SESSION_ID,
        actor_node_id=owner.identity.node_id,
        capability_type="background-analysis",
    ) == ()

    republished = runtime.publish_health(
        binding,
        provider_generation=2,
    )
    assert republished.provider_generation == 2
    assert health.fresh_reports(
        session_id=SESSION_ID,
        actor_node_id=owner.identity.node_id,
        capability_type="background-analysis",
    ) == (republished.report,)


def test_descriptor_change_fences_old_provider_before_new_binding_is_eligible(
    tmp_path: Path,
) -> None:
    (
        current,
        coordinator,
        owner,
        provider,
        enrollments,
        health,
        runtime,
    ) = _environment(tmp_path)
    old_candidate = _candidate(provider.identity.node_id)
    old_intent = _intent(provider.identity.node_id, active=True, revision=1)
    old_binding, _old_announcement, old_enrollment = runtime.reconcile_contribution(
        old_candidate,
        old_intent,
        session_id=SESSION_ID,
        node_id=provider.identity.node_id,
    )
    assert old_enrollment is not None
    old_approved = enrollments.approve(
        session_id=SESSION_ID,
        capability_id=old_binding.capability_id,
        actor_node_id=owner.identity.node_id,
        command_id="approve-old-compute-provider",
        expected_revision=old_enrollment.revision,
    )
    assert old_approved.state is ProviderEnrollmentState.APPROVED
    old_health = runtime.publish_health(old_binding)
    assert health.fresh_reports(
        session_id=SESSION_ID,
        actor_node_id=owner.identity.node_id,
        capability_type="background-analysis",
    ) == (old_health.report,)

    current[0] = NOW + timedelta(seconds=10)
    replacement_candidate = _candidate(
        provider.identity.node_id,
        descriptor_revision=2,
        inspection_revision=2,
    )
    replacement_intent = _intent(provider.identity.node_id, active=True, revision=2)
    new_binding, new_announcement, new_enrollment = runtime.reconcile_contribution(
        replacement_candidate,
        replacement_intent,
        session_id=SESSION_ID,
        node_id=provider.identity.node_id,
    )

    assert new_binding.capability_id != old_binding.capability_id
    assert new_announcement.status.value == "ready"
    assert new_enrollment is not None
    assert new_enrollment.state is ProviderEnrollmentState.PENDING

    announcements = {
        item.capability_id: item
        for item in coordinator.store.list_capabilities(session_id=SESSION_ID)
    }
    assert announcements[old_binding.capability_id].status.value == "disabled"
    assert announcements[new_binding.capability_id].status.value == "ready"

    old_record = enrollments.store.get(
        session_id=SESSION_ID,
        capability_id=old_binding.capability_id,
    )
    assert old_record is not None
    assert old_record.state is ProviderEnrollmentState.SUSPENDED
    assert old_record.announcement_status.value == "disabled"

    reports = health.fresh_reports(
        session_id=SESSION_ID,
        actor_node_id=owner.identity.node_id,
        capability_type="background-analysis",
    )
    assert reports == ()
    selection = select_provider(_job(), reports, evaluated_at=current[0])
    assert selection.decision == "no-eligible-provider"

    with pytest.raises(FederationValidationError) as stale_replay:
        runtime.reconcile_contribution(
            old_candidate,
            _intent(provider.identity.node_id, active=True, revision=3),
            session_id=SESSION_ID,
            node_id=provider.identity.node_id,
        )
    assert stale_replay.value.code == "stale-registered-compute-binding"
    announcements_after_stale = {
        item.capability_id: item
        for item in coordinator.store.list_capabilities(session_id=SESSION_ID)
    }
    assert announcements_after_stale[new_binding.capability_id].status.value == "ready"
    assert announcements_after_stale[old_binding.capability_id].status.value == "disabled"

    new_approved = enrollments.approve(
        session_id=SESSION_ID,
        capability_id=new_binding.capability_id,
        actor_node_id=owner.identity.node_id,
        command_id="approve-new-compute-provider",
        expected_revision=new_enrollment.revision,
    )
    assert new_approved.state is ProviderEnrollmentState.APPROVED
    new_health = runtime.publish_health(new_binding)
    reports = health.fresh_reports(
        session_id=SESSION_ID,
        actor_node_id=owner.identity.node_id,
        capability_type="background-analysis",
    )
    assert reports == (new_health.report,)
    selection = select_provider(_job(), reports, evaluated_at=current[0])
    assert selection.decision == "selected"
    assert selection.selected_capability_id == new_binding.capability_id


def test_binding_rejects_attributes_that_do_not_match_descriptor_fingerprint(
    tmp_path: Path,
) -> None:
    (
        _current,
        _coordinator,
        _owner,
        provider,
        _enrollments,
        _health,
        runtime,
    ) = _environment(tmp_path)
    candidate = _candidate(provider.identity.node_id)
    candidate.capacity_envelope["handler_attributes"] = analysis_provider_attributes(
        handler_revision=99
    )

    with pytest.raises(FederationValidationError) as mismatch:
        runtime.reconcile_contribution(
            candidate,
            _intent(provider.identity.node_id, active=True, revision=1),
            session_id=SESSION_ID,
            node_id=provider.identity.node_id,
        )
    assert mismatch.value.code == "compute-handler-fingerprint-mismatch"
