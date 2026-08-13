"""Provider selection, ownership, dispatch, and lifecycle for analysis jobs.

Every test drives the real components: F8.2 provider health feeds
``select_provider``, the F7.5 lifecycle store owns attempts and leases, and
``ResilientDispatchCoordinator`` applies the worker response.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest

from catalog.capabilities.analysis.contracts import analysis_grant_id
from catalog.capabilities.analysis.provisioning import analysis_handler_descriptor
from catalog.capabilities.analysis.scheduler import (
    DECISION_DISPATCHED,
    DECISION_NO_PROVIDER,
    DECISION_TERMINAL,
    FederatedAnalysisScheduler,
)
from catalog.capabilities.jobs import AttemptStatus, JobStatus
from catalog.capabilities.lifecycle_store import SQLiteJobLifecycleStore
from catalog.capabilities.provider_reports import ProviderStatus
from catalog.capabilities.tests.analysis_harness import (
    Clock,
    FailingLifecycleTransport,
    StaticReportSource,
    build_stack,
    provider_report,
)
from catalog.federation.errors import FederationValidationError


def _schedule(stack, job_id):
    return asyncio.run(stack.scheduler.schedule(job_id))


def _controlled_reports(stack, **overrides):
    """Replace health reports with one report under direct test control."""

    descriptor = analysis_handler_descriptor()
    stack.use_report_source(
        StaticReportSource(
            (
                provider_report(
                    capability_id=stack.provider_id,
                    node_id=stack.node_id,
                    session_id=overrides.pop("session_id", stack.session_id),
                    descriptor=descriptor,
                    now=stack.clock.now,
                    **overrides,
                ),
            )
        )
    )


# ----------------------------------------------------------------------
# Submission and deduplication
# ----------------------------------------------------------------------


def test_submission_queues_a_durable_job_without_owning_it(tmp_path) -> None:
    stack = build_stack(tmp_path)

    outcome = stack.submit()
    snapshot = stack.store.snapshot(outcome.job_id)

    assert outcome.created is True
    assert snapshot.job.status is JobStatus.QUEUED
    assert snapshot.ownership is None
    assert stack.executor.calls == []


def test_repeated_submission_recognizes_the_existing_job(tmp_path) -> None:
    stack = build_stack(tmp_path)

    first = stack.submit()
    second = stack.submit()
    third = stack.submit()

    assert first.job_id == second.job_id == third.job_id
    assert first.created is True
    assert second.created is False and third.created is False
    assert len(stack.service.registry.records()) == 1


def test_materially_changed_input_creates_new_work(tmp_path) -> None:
    stack = build_stack(tmp_path)

    first = stack.submit()
    second = stack.submit(source_signature="d" * 64)

    assert first.job_id != second.job_id
    assert len(stack.service.registry.records()) == 2


# ----------------------------------------------------------------------
# Provider selection
# ----------------------------------------------------------------------


def test_an_activated_provider_is_selected_and_executes(tmp_path) -> None:
    stack = build_stack(tmp_path)
    job_id = stack.submit().job_id

    outcome = _schedule(stack, job_id)
    snapshot = stack.store.snapshot(job_id)

    assert outcome.decision == DECISION_DISPATCHED
    assert outcome.provider_id == stack.provider_id
    assert outcome.node_id == stack.node_id
    assert snapshot.job.status is JobStatus.SUCCEEDED
    assert len(stack.executor.calls) == 1


def test_selection_uses_the_real_provider_health_authority(tmp_path) -> None:
    stack = build_stack(tmp_path)

    reports = stack.scheduler.report_source.reports(
        session_id=stack.session_id, capability_type="background-analysis"
    )

    assert [item.capability_id for item in reports] == [stack.provider_id]
    assert reports[0].node_id == stack.node_id
    # The report carries the F8.4 activation reference, not an invented model.
    assert "activation" in reports[0].attributes


def test_without_an_approved_provider_nothing_is_offered(tmp_path) -> None:
    stack = build_stack(tmp_path, activate_provider=False)
    job_id = stack.submit().job_id

    outcome = _schedule(stack, job_id)
    snapshot = stack.store.snapshot(job_id)

    assert outcome.decision == DECISION_NO_PROVIDER
    assert snapshot.job.status is JobStatus.QUEUED
    assert snapshot.ownership is None
    assert stack.executor.calls == []


@pytest.mark.parametrize(
    ("override", "reason"),
    (
        ({"protocol": "fcp-other-compute"}, "capability-protocol-mismatch"),
        ({"capability_type": "language-model"}, "capability-type-mismatch"),
        ({"protocol_version": "2.0"}, "capability-protocol-version-incompatible"),
        ({"status": ProviderStatus.DRAINING}, "provider-status-draining"),
        (
            {"attributes": {"analysis_contract": "something-else"}},
            "requirement-mismatch",
        ),
        ({"max_concurrent_jobs": 1, "active_jobs": 1}, "insufficient-available-slots"),
    ),
)
def test_incompatible_provider_is_rejected(tmp_path, override, reason) -> None:
    stack = build_stack(tmp_path)
    _controlled_reports(stack, **override)
    job_id = stack.submit().job_id

    outcome = _schedule(stack, job_id)

    assert outcome.decision == DECISION_NO_PROVIDER
    assert any(
        reason in item
        for reasons in outcome.selection.rejected.values()
        for item in reasons
    )
    assert stack.executor.calls == []


def test_provider_from_another_federation_session_is_rejected(tmp_path) -> None:
    stack = build_stack(tmp_path)
    _controlled_reports(stack, session_id="session-other-federation")
    job_id = stack.submit().job_id

    outcome = _schedule(stack, job_id)

    assert outcome.decision == DECISION_NO_PROVIDER
    assert stack.store.snapshot(job_id).job.status is JobStatus.QUEUED
    assert stack.executor.calls == []


def test_expired_provider_report_is_not_selectable(tmp_path) -> None:
    stack = build_stack(tmp_path)
    job_id = stack.submit().job_id
    stack.clock.advance(minutes=30)

    outcome = _schedule(stack, job_id)

    assert outcome.decision == DECISION_NO_PROVIDER
    assert stack.executor.calls == []


# ----------------------------------------------------------------------
# Ownership, grants and dispatch
# ----------------------------------------------------------------------


def test_ownership_and_grant_exist_before_the_worker_is_reached(tmp_path) -> None:
    stack = build_stack(tmp_path)
    job_id = stack.submit().job_id
    observed: list[tuple[str, str]] = []
    original = stack.transport.request

    async def observing(*, target_node_id, request):
        snapshot = stack.store.snapshot(request.job.job_id)
        assert snapshot.ownership is not None
        grant = stack.authority.grant(
            analysis_grant_id(job_id, snapshot.ownership.attempt_id)
        )
        observed.append((snapshot.ownership.attempt_id, grant.grant_id))
        assert grant.expires_at <= snapshot.ownership.lease_expires_at
        assert grant.lease_generation == snapshot.ownership.lease_generation
        return await original(target_node_id=target_node_id, request=request)

    stack.transport.request = observing
    _schedule(stack, job_id)

    assert len(observed) == 1


def test_duplicate_dispatch_of_one_attempt_executes_once(tmp_path) -> None:
    stack = build_stack(tmp_path)
    job_id = stack.submit().job_id
    _schedule(stack, job_id)

    replay = _schedule(stack, job_id)

    assert replay.decision == DECISION_TERMINAL
    assert len(stack.executor.calls) == 1


def test_expired_lease_is_rejected_by_the_worker(tmp_path) -> None:
    stack = build_stack(tmp_path)
    job_id = stack.submit().job_id
    original = stack.transport.request

    async def slow(*, target_node_id, request):
        stack.clock.advance(hours=2)
        return await original(target_node_id=target_node_id, request=request)

    stack.transport.request = slow
    outcome = _schedule(stack, job_id)

    assert outcome.reason == "stale-ignored"
    assert stack.executor.calls == []
    assert stack.store.snapshot(job_id).job.status is not JobStatus.SUCCEEDED


def test_a_second_coordinator_cannot_take_over_a_queued_job(tmp_path) -> None:
    stack = build_stack(tmp_path)
    job_id = stack.submit().job_id
    _schedule(stack, job_id)

    with pytest.raises(FederationValidationError):
        stack.store.claim(
            job_id,
            coordinator_id="node-intruder",
            owner_provider_id=stack.provider_id,
            attempt_id="attempt-intruder",
            lease_id="lease-intruder",
            command_id="intruder-claim",
            expected_revision=stack.store.snapshot(job_id).revision,
            lease_expires_at=stack.clock.now + timedelta(minutes=10),
            now=stack.clock.now,
        )


def test_only_the_granting_coordinator_may_dispatch_or_authorize(tmp_path) -> None:
    stack = build_stack(tmp_path)
    job_id = stack.submit().job_id
    intruder = FederatedAnalysisScheduler(
        store=stack.store,
        gateway=stack.gateway,
        transport=stack.transport,
        report_source=stack.scheduler.report_source,
        coordinator_node_id="node-intruder",
        clock=stack.clock,
    )
    captured: list[FederationValidationError] = []
    original = stack.transport.request

    async def capture(*, target_node_id, request):
        snapshot = stack.store.snapshot(request.job.job_id)
        with pytest.raises(FederationValidationError) as dispatch_error:
            await intruder.dispatcher.dispatch(
                request.job.job_id,
                dispatch_id="hijacked-dispatch",
                target_node_id=stack.node_id,
                now=stack.clock.now,
            )
        captured.append(dispatch_error.value)
        with pytest.raises(FederationValidationError) as grant_error:
            intruder._grant_input_access(
                snapshot, worker_node_id="node-intruder", now=stack.clock.now
            )
        captured.append(grant_error.value)
        return await original(target_node_id=target_node_id, request=request)

    stack.transport.request = capture
    _schedule(stack, job_id)

    assert [item.code for item in captured] == [
        "dispatch-coordinator-mismatch",
        "grant-coordinator-mismatch",
    ]


# ----------------------------------------------------------------------
# Lifecycle
# ----------------------------------------------------------------------


def test_worker_failure_marks_the_attempt_failed(tmp_path) -> None:
    stack = build_stack(tmp_path, succeed=False)
    job_id = stack.submit().job_id

    _schedule(stack, job_id)
    snapshot = stack.store.snapshot(job_id)

    assert snapshot.job.status in {JobStatus.RETRY_WAIT, JobStatus.FAILED}
    assert snapshot.job.attempts[-1].status is AttemptStatus.FAILED
    assert snapshot.job.attempts[-1].error_code == "analysis-script-failed"


def test_unreachable_worker_releases_ownership_for_another_attempt(tmp_path) -> None:
    stack = build_stack(tmp_path)
    job_id = stack.submit().job_id
    stack.scheduler.dispatcher.transport = FailingLifecycleTransport(
        TimeoutError("worker unreachable")
    )

    outcome = _schedule(stack, job_id)
    snapshot = stack.store.snapshot(job_id)

    assert outcome.decision == "dispatch-failed"
    assert snapshot.job.status is JobStatus.RETRY_WAIT
    assert snapshot.ownership is None
    assert stack.executor.calls == []


def test_successful_execution_commits_exactly_one_durable_result(tmp_path) -> None:
    stack = build_stack(tmp_path)
    job_id = stack.submit().job_id

    _schedule(stack, job_id)
    committed = stack.store.result_commit(job_id)
    snapshot = stack.store.snapshot(job_id)

    assert committed is not None
    assert committed.provider_id == stack.provider_id
    assert committed.attempt_id == snapshot.job.attempts[-1].attempt_id
    assert committed.lease_generation == snapshot.job.attempts[-1].attempt_number
    assert committed.reference.reference_id == snapshot.job.outputs[0].reference_id
    assert committed.reference.content_hash is not None

    # Replaying the same dispatch cannot commit a competing result.
    _schedule(stack, job_id)
    assert stack.store.result_commit(job_id) == committed


def test_restart_preserves_the_durable_job(tmp_path) -> None:
    stack = build_stack(tmp_path)
    job_id = stack.submit().job_id

    reopened = SQLiteJobLifecycleStore(stack.store.database)
    snapshot = reopened.snapshot(job_id)

    assert snapshot.job.status is JobStatus.QUEUED
    assert snapshot.job.job_id == job_id


def test_stale_attempt_cannot_overwrite_a_newer_attempt(tmp_path) -> None:
    stack = build_stack(tmp_path, succeed=False)
    job_id = stack.submit().job_id
    _schedule(stack, job_id)
    stale = stack.store.snapshot(job_id).job.attempts[-1]

    with pytest.raises(FederationValidationError):
        stack.store.record_attempt_status(
            job_id,
            coordinator_id=stack.node_id,
            owner_provider_id=stack.provider_id,
            attempt_id=stale.attempt_id,
            lease_id=f"{job_id}-lease-1",
            command_id="stale-running",
            expected_revision=stack.store.snapshot(job_id).revision,
            target_status=AttemptStatus.RUNNING,
            now=stack.clock.now,
        )


def test_scheduling_pass_reports_every_pending_job(tmp_path) -> None:
    stack = build_stack(tmp_path)
    first = stack.submit().job_id
    second = stack.submit(target_date="2026-08-14").job_id

    outcomes = stack.service.run_scheduling_pass()

    assert {item.job_id for item in outcomes} == {first, second}
    assert all(item.decision == DECISION_DISPATCHED for item in outcomes)


def test_clock_is_injected_rather_than_read_from_the_wall(tmp_path) -> None:
    clock = Clock()
    stack = build_stack(tmp_path, clock=clock)

    assert stack.scheduler.clock is clock
