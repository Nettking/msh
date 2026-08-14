"""Production lifecycle dispatches feed execution-efficiency learning safely."""

from __future__ import annotations

import asyncio
from datetime import timedelta

from catalog.capabilities.dispatch import (
    DispatchEvent,
    DispatchRequest,
    DispatchResponse,
    DispatchState,
)
from catalog.capabilities.efficiency import ExecutionEfficiencyRuntime
from catalog.capabilities.lifecycle_store import SQLiteJobLifecycleStore
from catalog.capabilities.tests.efficiency_harness import NOW, build_job


class _SuccessfulTransport:
    async def request(
        self, *, target_node_id: str, request: DispatchRequest
    ) -> DispatchResponse:
        assert target_node_id == request.target_node_id
        return DispatchResponse(
            dispatch_id=request.dispatch_id,
            session_id=request.session_id,
            job_id=request.job.job_id,
            provider_id=request.provider_id,
            attempt_id=request.attempt_id,
            events=(
                DispatchEvent(1, DispatchState.ACCEPTED, request.sent_at),
                DispatchEvent(
                    2, DispatchState.RUNNING, request.sent_at + timedelta(seconds=1)
                ),
                DispatchEvent(
                    3, DispatchState.SUCCEEDED, request.sent_at + timedelta(seconds=3)
                ),
            ),
        )

    async def cancel(self, *, target_node_id: str, request):  # pragma: no cover
        raise AssertionError("cancellation was not expected")


def _owned_request(tmp_path) -> DispatchRequest:
    job = build_job()
    store = SQLiteJobLifecycleStore(tmp_path / "jobs.sqlite3")
    snapshot = store.submit(job, coordinator_id="coordinator", now=NOW).snapshot
    snapshot = store.queue(
        job.job_id,
        coordinator_id="coordinator",
        command_id="queue",
        expected_revision=snapshot.revision,
        now=NOW,
    ).snapshot
    snapshot = store.claim(
        job.job_id,
        coordinator_id="coordinator",
        owner_provider_id="cap-node-a",
        attempt_id="attempt-1",
        lease_id="lease-1",
        command_id="claim",
        expected_revision=snapshot.revision,
        lease_expires_at=NOW + timedelta(minutes=10),
        now=NOW,
    ).snapshot
    return DispatchRequest.from_snapshot(
        snapshot,
        dispatch_id="dispatch-1",
        target_node_id="node-a",
        sent_at=NOW + timedelta(seconds=1),
    )


def test_real_lifecycle_transport_response_is_learned_once(tmp_path) -> None:
    efficiency = ExecutionEfficiencyRuntime(
        tmp_path / "efficiency.sqlite3", node_id="coordinator"
    )
    transport = efficiency.wrap_transport(_SuccessfulTransport())
    request = _owned_request(tmp_path)

    response = asyncio.run(
        transport.request(target_node_id="node-a", request=request)
    )
    assert response.state is DispatchState.SUCCEEDED
    assert efficiency.store.observation_count() == 1
    observation = efficiency.store.observations()[0]
    assert observation.node_id == "node-a"
    assert observation.capability_id == "cap-node-a"
    assert observation.attempt_id == "attempt-1"
    assert observation.measurements.succeeded is True
    assert observation.measurements.execution_millis == 2_000

    # A duplicate dispatch response for the same durable attempt cannot teach
    # the federation twice because observation identity is attempt-scoped.
    asyncio.run(transport.request(target_node_id="node-a", request=request))
    assert efficiency.store.observation_count() == 1
