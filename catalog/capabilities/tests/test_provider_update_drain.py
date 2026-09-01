from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from catalog.capabilities.job_store import SQLiteJobStore
from catalog.capabilities.jobs import (
    ArtifactReference,
    AttemptStatus,
    CapabilityRequirement,
    JobContract,
    RetryPolicy,
    TimeoutPolicy,
)
from catalog.capabilities.provider_reports import ProviderStatus
from catalog.capabilities.provider_selection import evaluate_provider_candidate
from catalog.capabilities.provider_update_drain import (
    DurableProviderQuiescenceQuery,
    ProviderUpdateDrainState,
    SQLiteProviderUpdateDrainStore,
)
from catalog.capabilities.registered_compute_provider import (
    RegisteredComputeProviderBinding,
    RegisteredComputeProviderRuntime,
)
from catalog.capabilities.worker_activation import LocalComputeHandlerDescriptor
from catalog.federation.errors import FederationValidationError

NOW = datetime(2026, 9, 1, 12, 0, tzinfo=timezone.utc)
SESSION_ID = "session-update-drain"
NODE_ID = "node-provider"
PROVIDER_ID = "provider-capability"
COORDINATOR_ID = "node-coordinator"


def _sha256(character: str) -> str:
    return "sha256:" + character * 64


def _job(job_id: str = "job-drain") -> JobContract:
    return JobContract(
        job_id=job_id,
        session_id=SESSION_ID,
        request_id=f"request-{job_id}",
        idempotency_key=f"{SESSION_ID}:request-{job_id}",
        capability=CapabilityRequirement(
            capability_type="background-analysis",
            protocol="fcp-background-analysis",
            protocol_version="1.0",
            requirements={"kind": "drain-test"},
        ),
        inputs=(
            ArtifactReference(
                reference_id=f"{job_id}-input",
                session_id=SESSION_ID,
                schema_name="fcp.test-input.v1",
                media_type="application/json",
                content_hash=_sha256("a"),
                size_bytes=16,
            ),
        ),
        outputs=(
            ArtifactReference(
                reference_id=f"{job_id}-output",
                session_id=SESSION_ID,
                schema_name="fcp.test-output.v1",
                media_type="application/json",
            ),
        ),
        retry_policy=RetryPolicy(
            max_attempts=2,
            backoff_seconds=(1,),
            retryable_error_codes=("worker-lost",),
        ),
        timeout_policy=TimeoutPolicy(
            overall_timeout_seconds=600,
            queue_timeout_seconds=60,
            start_timeout_seconds=30,
            run_timeout_seconds=300,
            cancellation_grace_seconds=15,
        ),
    )


def _claim(store: SQLiteJobStore, *, job_id: str = "job-drain"):
    submitted = store.submit(_job(job_id), coordinator_id=COORDINATOR_ID, now=NOW)
    queued = store.queue(
        job_id,
        coordinator_id=COORDINATOR_ID,
        command_id=f"queue-{job_id}",
        expected_revision=submitted.snapshot.revision,
        now=NOW + timedelta(seconds=1),
    )
    return store.claim(
        job_id,
        coordinator_id=COORDINATOR_ID,
        owner_provider_id=PROVIDER_ID,
        attempt_id=f"attempt-{job_id}",
        lease_id=f"lease-{job_id}",
        command_id=f"claim-{job_id}",
        expected_revision=queued.snapshot.revision,
        lease_expires_at=NOW + timedelta(minutes=5),
        now=NOW + timedelta(seconds=2),
    )


def _binding() -> RegisteredComputeProviderBinding:
    descriptor = LocalComputeHandlerDescriptor(
        handler_id="background-analysis",
        capability_type="background-analysis",
        protocol="fcp-background-analysis",
        protocol_version="1.0",
        attributes={"kind": "drain-test"},
    )
    return RegisteredComputeProviderBinding(
        session_id=SESSION_ID,
        node_id=NODE_ID,
        capability_id=PROVIDER_ID,
        capability_type=descriptor.capability_type,
        protocol=descriptor.protocol,
        protocol_version=descriptor.protocol_version,
        handler_id=descriptor.handler_id,
        descriptor_fingerprint=descriptor.descriptor_fingerprint,
        inspection_revision=1,
        descriptor=descriptor,
    )


class _Enrollments:
    def __init__(self) -> None:
        self.coordinator = object()


class _CapturingHealth:
    def __init__(self, enrollments: _Enrollments) -> None:
        self.enrollments = enrollments
        self.published = []

    def publish(self, report, **_kwargs):
        self.published.append(report)
        return report


def _runtime(tmp_path):
    drains = SQLiteProviderUpdateDrainStore(tmp_path / "update-drain.sqlite3")
    enrollments = _Enrollments()
    health = _CapturingHealth(enrollments)
    runtime = RegisteredComputeProviderRuntime(
        enrollments=enrollments,  # type: ignore[arg-type]
        health=health,  # type: ignore[arg-type]
        clock=lambda: NOW,
        update_drain=drains,
    )
    return runtime, health, drains


def test_r1_drain_state_is_restart_safe_and_idempotent(tmp_path) -> None:
    database = tmp_path / "update-drain.sqlite3"
    store = SQLiteProviderUpdateDrainStore(database)

    first = store.request_drain(session_id=SESSION_ID, node_id=NODE_ID, now=NOW)
    replay = store.request_drain(
        session_id=SESSION_ID,
        node_id=NODE_ID,
        now=NOW + timedelta(seconds=1),
    )

    assert first.changed
    assert first.record.state is ProviderUpdateDrainState.DRAINING
    assert first.record.revision == 1
    assert not replay.changed
    assert replay.record.revision == 1

    reopened = SQLiteProviderUpdateDrainStore(database)
    persisted = reopened.get(session_id=SESSION_ID, node_id=NODE_ID)
    assert persisted is not None
    assert persisted.state is ProviderUpdateDrainState.DRAINING
    assert persisted.revision == 1

    cleared = reopened.clear_drain(
        session_id=SESSION_ID,
        node_id=NODE_ID,
        expected_revision=1,
        now=NOW + timedelta(seconds=2),
    )
    assert cleared.changed
    assert cleared.record.state is ProviderUpdateDrainState.READY
    assert cleared.record.revision == 2

    with pytest.raises(FederationValidationError) as stale:
        reopened.clear_drain(
            session_id=SESSION_ID,
            node_id=NODE_ID,
            expected_revision=1,
            now=NOW + timedelta(seconds=3),
        )
    assert stale.value.code == "update-drain-revision-conflict"


def test_r1_quiescence_uses_durable_ownership_until_terminal_completion(tmp_path) -> None:
    database = tmp_path / "jobs.sqlite3"
    jobs = SQLiteJobStore(database)
    claimed = _claim(jobs)
    query = DurableProviderQuiescenceQuery(jobs)

    active = query.probe((PROVIDER_ID,))
    assert not active.quiescent
    assert active.active_count == 1
    assert active.active_ownerships[0].job_id == "job-drain"
    assert active.active_ownerships[0].attempt_id == "attempt-job-drain"
    assert active.active_ownerships[0].provider_id == PROVIDER_ID

    completed = jobs.complete(
        "job-drain",
        coordinator_id=COORDINATOR_ID,
        owner_provider_id=PROVIDER_ID,
        attempt_id="attempt-job-drain",
        lease_id="lease-job-drain",
        command_id="complete-job-drain",
        expected_revision=claimed.snapshot.revision,
        terminal_status=AttemptStatus.FAILED,
        error_code="drain-test-complete",
        now=NOW + timedelta(minutes=1),
    )
    assert completed.snapshot.ownership is None
    assert query.probe((PROVIDER_ID,)).quiescent

    reopened = SQLiteJobStore(database)
    assert DurableProviderQuiescenceQuery(reopened).probe((PROVIDER_ID,)).quiescent


def test_r1_quiescence_is_scoped_to_the_supplied_provider_id_set(tmp_path) -> None:
    jobs = SQLiteJobStore(tmp_path / "jobs.sqlite3")
    _claim(jobs)
    query = DurableProviderQuiescenceQuery(jobs)

    assert query.probe(("different-provider",)).quiescent
    assert not query.probe(("different-provider", PROVIDER_ID)).quiescent


def test_r1_draining_health_preserves_workload_and_blocks_selection(tmp_path) -> None:
    runtime, health, drains = _runtime(tmp_path)
    binding = _binding()
    drains.request_drain(session_id=SESSION_ID, node_id=NODE_ID, now=NOW)

    runtime.publish_health(
        binding,
        status=ProviderStatus.READY,
        report_revision=7,
        max_concurrent_jobs=4,
        active_jobs=2,
        queue_depth=1,
        utilization_millis=500,
    )
    report = health.published[-1]

    assert report.status is ProviderStatus.DRAINING
    assert report.max_concurrent_jobs == 4
    assert report.active_jobs == 2
    assert report.available_slots == 2
    assert report.queue_depth == 1
    assert report.utilization_millis == 500

    candidate = evaluate_provider_candidate(_job(), report, evaluated_at=NOW)
    assert not candidate.eligible
    assert candidate.reasons == ("provider-status-draining",)


def test_r1_drain_does_not_upgrade_a_stronger_provider_failure_state(tmp_path) -> None:
    runtime, health, drains = _runtime(tmp_path)
    binding = _binding()
    drains.request_drain(session_id=SESSION_ID, node_id=NODE_ID, now=NOW)

    runtime.publish_health(binding, status=ProviderStatus.UNAVAILABLE)

    assert health.published[-1].status is ProviderStatus.UNAVAILABLE


def test_r1_clearing_exact_drain_revision_allows_ready_publication_again(tmp_path) -> None:
    runtime, health, drains = _runtime(tmp_path)
    binding = _binding()
    requested = drains.request_drain(session_id=SESSION_ID, node_id=NODE_ID, now=NOW)
    drains.clear_drain(
        session_id=SESSION_ID,
        node_id=NODE_ID,
        expected_revision=requested.record.revision,
        now=NOW + timedelta(seconds=1),
    )

    runtime.publish_health(binding, status=ProviderStatus.READY)

    assert health.published[-1].status is ProviderStatus.READY
