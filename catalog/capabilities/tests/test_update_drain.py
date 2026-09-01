from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from catalog.capabilities.jobs import (
    CapabilityRequirement,
    JobContract,
    RetryPolicy,
    TimeoutPolicy,
)
from catalog.capabilities.lifecycle_store import SQLiteJobLifecycleStore
from catalog.capabilities.provider_reports import ProviderResourceReport, ProviderStatus
from catalog.capabilities.provider_selection import evaluate_provider_candidate
from catalog.capabilities.update_drain import MAX_DRAIN_PROVIDER_IDS, NodeUpdateDrainTarget
from catalog.federation.errors import FederationValidationError

NOW = datetime(2026, 9, 1, 12, 0, tzinfo=timezone.utc)


def _job() -> JobContract:
    return JobContract(
        job_id="job-drain-1",
        session_id="session-drain-1",
        request_id="request-drain-1",
        idempotency_key="session-drain-1:request-drain-1",
        capability=CapabilityRequirement(
            capability_type="synthetic-compute",
            protocol="fcp-synthetic",
            protocol_version="1.0",
            requirements={},
        ),
        inputs=(),
        outputs=(),
        retry_policy=RetryPolicy(
            max_attempts=1,
            backoff_seconds=(),
            retryable_error_codes=(),
        ),
        timeout_policy=TimeoutPolicy(
            overall_timeout_seconds=120,
            queue_timeout_seconds=20,
            start_timeout_seconds=10,
            run_timeout_seconds=30,
            cancellation_grace_seconds=5,
        ),
    )


def _report(
    *,
    provider_id: str = "provider-a",
    node_id: str = "node-a",
    status: ProviderStatus = ProviderStatus.READY,
    active_jobs: int = 1,
) -> ProviderResourceReport:
    return ProviderResourceReport(
        capability_id=provider_id,
        node_id=node_id,
        session_id="session-drain-1",
        capability_type="synthetic-compute",
        protocol="fcp-synthetic",
        protocol_version="1.0",
        status=status,
        report_revision=7,
        max_concurrent_jobs=4,
        active_jobs=active_jobs,
        queue_depth=2,
        utilization_millis=500,
        attributes={},
        reported_at=NOW,
        expires_at=NOW + timedelta(seconds=30),
    )


def _claim(store: SQLiteJobLifecycleStore, provider_id: str = "provider-a") -> None:
    job = _job()
    store.submit(job, coordinator_id="coordinator", now=NOW)
    store.queue(
        job.job_id,
        coordinator_id="coordinator",
        command_id="queue-drain-1",
        expected_revision=0,
        now=NOW,
    )
    store.claim(
        job.job_id,
        coordinator_id="coordinator",
        owner_provider_id=provider_id,
        attempt_id="attempt-drain-1",
        lease_id="lease-drain-1",
        command_id="claim-drain-1",
        expected_revision=1,
        lease_expires_at=NOW + timedelta(seconds=60),
        now=NOW,
    )


def test_drain_fences_new_selection_without_erasing_live_work_metrics() -> None:
    target = NodeUpdateDrainTarget("node-a", ("provider-a",))
    ready = _report(active_jobs=1)

    draining = target.apply_to_report(ready)

    assert draining.status is ProviderStatus.DRAINING
    assert draining.active_jobs == ready.active_jobs == 1
    assert draining.queue_depth == ready.queue_depth
    assert draining.max_concurrent_jobs == ready.max_concurrent_jobs
    candidate = evaluate_provider_candidate(_job(), draining, evaluated_at=NOW)
    assert not candidate.eligible
    assert "provider-status-draining" in candidate.reasons


def test_drain_does_not_weaken_a_stronger_non_ready_state() -> None:
    target = NodeUpdateDrainTarget("node-a", ("provider-a",))
    unavailable = _report(status=ProviderStatus.UNAVAILABLE)

    assert target.apply_to_report(unavailable) is unavailable


def test_drain_does_not_touch_another_node_or_provider() -> None:
    target = NodeUpdateDrainTarget("node-a", ("provider-a",))
    other_node = _report(node_id="node-b")
    other_provider = _report(provider_id="provider-b")

    assert target.apply_to_report(other_node) is other_node
    assert target.apply_to_report(other_provider) is other_provider


def test_quiescence_uses_durable_active_ownership_not_health_counters(tmp_path) -> None:
    store = SQLiteJobLifecycleStore(tmp_path / "jobs.sqlite3")
    target = NodeUpdateDrainTarget("node-a", ("provider-a",))
    assert target.is_quiescent(store)

    _claim(store)

    # A health publisher could temporarily report zero or disappear entirely;
    # the durable ownership lease remains the activation fence.
    assert target.active_ownership_count(store) == 1
    assert not target.is_quiescent(store)


def test_unrelated_provider_ownership_does_not_block_this_node(tmp_path) -> None:
    store = SQLiteJobLifecycleStore(tmp_path / "jobs.sqlite3")
    _claim(store, provider_id="provider-b")

    assert NodeUpdateDrainTarget("node-a", ("provider-a",)).is_quiescent(store)


def test_provider_identity_set_is_bounded_and_unique() -> None:
    with pytest.raises(FederationValidationError) as duplicate:
        NodeUpdateDrainTarget("node-a", ("provider-a", "provider-a"))
    assert duplicate.value.code == "duplicate-drain-provider"

    with pytest.raises(FederationValidationError) as oversized:
        NodeUpdateDrainTarget(
            "node-a",
            tuple(f"provider-{index}" for index in range(MAX_DRAIN_PROVIDER_IDS + 1)),
        )
    assert oversized.value.code == "too-many-drain-providers"
