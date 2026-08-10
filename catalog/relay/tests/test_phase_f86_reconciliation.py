from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from catalog.capabilities.dispatch import (
    CapabilityWorker,
    ExecutionResult,
    SQLiteDispatchInbox,
    WorkerRegistration,
)
from catalog.capabilities.relay_dispatch import RelayDispatchEndpoint

NOW = datetime(2026, 8, 2, 16, tzinfo=timezone.utc)
SESSION_ID = "session-f86-relay"
NODE_ID = "node-f86-relay-worker"


class StubRelayClient:
    node_id = NODE_ID


class SyntheticHandler:
    async def execute(self, job) -> ExecutionResult:
        return ExecutionResult(True, {"job_id": job.job_id})


def worker(tmp_path: Path, provider_id: str, revision: int) -> CapabilityWorker:
    return CapabilityWorker(
        WorkerRegistration(
            session_id=SESSION_ID,
            node_id=NODE_ID,
            provider_id=provider_id,
            capability_type="synthetic-compute",
            protocol="fcp-synthetic",
            protocol_version="1.0",
            attributes={"operation": "echo", "revision": revision},
        ),
        SyntheticHandler(),
        SQLiteDispatchInbox(tmp_path / f"{provider_id}-{revision}.sqlite3"),
        clock=lambda: NOW,
    )


def test_f86_replaces_only_reconciler_owned_relay_workers(tmp_path: Path) -> None:
    unrelated = worker(tmp_path, "provider-unrelated", 1)
    previous = worker(tmp_path, "provider-reconciled", 1)
    replacement = worker(tmp_path, "provider-reconciled", 2)
    endpoint = RelayDispatchEndpoint(
        StubRelayClient(),
        {
            unrelated.registration.provider_id: unrelated,
            previous.registration.provider_id: previous,
        },
    )

    rolled_back = endpoint.replace_workers(
        {replacement.registration.provider_id: replacement},
        replace_provider_ids=(replacement.registration.provider_id,),
    )

    assert rolled_back == {previous.registration.provider_id: previous}
    assert endpoint.workers == {
        unrelated.registration.provider_id: unrelated,
        replacement.registration.provider_id: replacement,
    }

    endpoint.replace_workers(
        rolled_back,
        replace_provider_ids=(replacement.registration.provider_id,),
    )

    assert endpoint.workers == {
        unrelated.registration.provider_id: unrelated,
        previous.registration.provider_id: previous,
    }
