from __future__ import annotations

import asyncio
import sqlite3
from collections.abc import Mapping
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path, PurePosixPath
from typing import Any

import pytest

from catalog.federation.commit_tracking import DurableAcknowledgementStore
from catalog.federation.errors import FederationValidationError
from catalog.federation.local_storage import FilesystemBatchStorageProvider
from catalog.federation.phase_d_client import PhaseDLogicalStorageClient
from catalog.federation.phase_d_control import PhaseDControlPlane
from catalog.federation.storage_catalog import (
    CommittedBatchPage,
    CommittedBatchReference,
)
from catalog.federation.storage_control_plane import StorageProviderRegistration
from catalog.federation.storage_protocol import (
    STORAGE_PROTOCOL,
    STORAGE_PROTOCOL_VERSION,
    BatchIngestRequest,
    BatchIngestState,
    StorageOperation,
    StorageRequestEnvelope,
    StorageResponseEnvelope,
    WriteAuthority,
)

NOW = datetime(2026, 8, 11, 12, 0, tzinfo=timezone.utc)
SESSION_ID = "session-storage-discovery"
GROUP_ID = "storage-main"
PROVIDER_ID = "provider-primary"
PROVIDER_NODE_ID = "node-storage-primary"
LEASE_EXPIRES_AT = NOW + timedelta(hours=1)


def _request(
    content: object,
    *,
    session_id: str = SESSION_ID,
    group_id: str = GROUP_ID,
    dataset_id: str = "telemetry",
    batch_id: str = "batch-1",
    idempotency_key: str | None = None,
    created_at: datetime = NOW,
) -> BatchIngestRequest:
    return BatchIngestRequest(
        authority=WriteAuthority(
            session_id=session_id,
            group_id=group_id,
            actor_node_id=PROVIDER_NODE_ID,
            grant_id="grant-1",
            term=1,
            fencing_token=1,
            lease_expires_at=LEASE_EXPIRES_AT,
        ),
        dataset_id=dataset_id,
        batch_id=batch_id,
        idempotency_key=idempotency_key or f"{session_id}:{dataset_id}:{batch_id}",
        content_hash=BatchIngestRequest.calculate_content_hash(content),
        content=content,
        created_at=created_at,
        dataset_schema_name="fcp.mtconnect.observations",
        dataset_schema_version=1,
    )


def _value(item: object, field: str) -> Any:
    if isinstance(item, Mapping):
        return item[field]
    return getattr(item, field)


def _batch_ids(page: Any) -> tuple[str, ...]:
    assert isinstance(page.batches, tuple)
    return tuple(str(_value(batch, "batch_id")) for batch in page.batches)


def _relative_path(provider: FilesystemBatchStorageProvider, batch_id: str) -> str:
    with closing(sqlite3.connect(provider.database_path)) as connection:
        row = connection.execute(
            "SELECT relative_path FROM committed_batches WHERE batch_id=?",
            (batch_id,),
        ).fetchone()
    assert row is not None
    return str(row[0])


def test_real_recorder_opaque_ids_are_portable_and_survive_restart(
    tmp_path: Path,
) -> None:
    """Recorder identities contain colons, which are illegal in Windows paths."""

    dataset_id = "mtconnect:node-recorder-1:Mazak"
    batch_id = f"Mazak:77:10:12:{'a' * 64}"
    content = {
        "schema": "fcp.mtconnect.observations.v1",
        "source_name": "Mazak",
        "machine_id": "machine-1",
        "first_sequence": 10,
        "last_sequence": 12,
        "observations": [
            {"sequence": sequence, "value": float(sequence)}
            for sequence in range(10, 13)
        ],
    }
    request = _request(
        content,
        dataset_id=dataset_id,
        batch_id=batch_id,
    )
    provider = FilesystemBatchStorageProvider(tmp_path / "storage")

    assert provider.ingest(request).state is BatchIngestState.STORED
    assert provider.read(
        session_id=SESSION_ID,
        group_id=GROUP_ID,
        batch_id=batch_id,
    ) == content

    relative_path = _relative_path(provider, batch_id)
    portable_path = PurePosixPath(relative_path)
    assert not portable_path.is_absolute()
    assert ".." not in portable_path.parts
    assert all(not set('<>:"\\|?*').intersection(part) for part in portable_path.parts)
    assert all(part == part.rstrip(" .") for part in portable_path.parts)
    assert dataset_id not in relative_path
    assert batch_id not in relative_path
    assert (provider.batch_root.joinpath(*portable_path.parts)).is_file()

    restarted = FilesystemBatchStorageProvider(tmp_path / "storage")
    assert restarted.read(
        session_id=SESSION_ID,
        group_id=GROUP_ID,
        batch_id=batch_id,
    ) == content


def test_opaque_ids_that_casefold_alike_do_not_share_a_physical_object(
    tmp_path: Path,
) -> None:
    """Distinct opaque identities must stay distinct on case-insensitive disks."""

    provider = FilesystemBatchStorageProvider(tmp_path / "storage")
    first = _request(
        {"value": "upper"},
        dataset_id="mtconnect:node-1:Mazak",
        batch_id="Mazak:77:1:1:ABC",
        idempotency_key="opaque-upper",
    )
    second = _request(
        {"value": "lower"},
        dataset_id="mtconnect:node-1:mazak",
        batch_id="mazak:77:1:1:abc",
        idempotency_key="opaque-lower",
        created_at=NOW + timedelta(seconds=1),
    )

    assert provider.ingest(first).state is BatchIngestState.STORED
    assert provider.ingest(second).state is BatchIngestState.STORED
    assert _relative_path(provider, first.batch_id) != _relative_path(
        provider,
        second.batch_id,
    )
    assert provider.read(
        session_id=SESSION_ID,
        group_id=GROUP_ID,
        batch_id=first.batch_id,
    ) == first.content
    assert provider.read(
        session_id=SESSION_ID,
        group_id=GROUP_ID,
        batch_id=second.batch_id,
    ) == second.content


class _ProviderReadTransport:
    def __init__(self, content_by_batch: Mapping[str, object]) -> None:
        self.content_by_batch = dict(content_by_batch)
        self.calls: list[tuple[str, StorageRequestEnvelope]] = []

    async def request(
        self,
        *,
        target_node_id: str,
        envelope: StorageRequestEnvelope,
    ) -> StorageResponseEnvelope:
        self.calls.append((target_node_id, envelope))
        assert envelope.operation is StorageOperation.BATCH_READ
        batch_id = str(envelope.payload["batch_id"])
        return StorageResponseEnvelope(
            request_id=envelope.request_id,
            protocol=STORAGE_PROTOCOL,
            protocol_version=STORAGE_PROTOCOL_VERSION,
            ok=True,
            result={"content": self.content_by_batch[batch_id]},
        )


def _storage_control(tmp_path: Path) -> PhaseDControlPlane:
    control = PhaseDControlPlane(tmp_path / "control.sqlite3")
    control.create_group(SESSION_ID, "coordinator", GROUP_ID)
    control.register_provider(
        SESSION_ID,
        "coordinator",
        StorageProviderRegistration(
            session_id=SESSION_ID,
            provider_id=PROVIDER_ID,
            node_id=PROVIDER_NODE_ID,
            protocol=STORAGE_PROTOCOL,
            protocol_version=STORAGE_PROTOCOL_VERSION,
            authorized=True,
            status="ready",
        ),
    )
    control.change_assignment(
        SESSION_ID,
        "coordinator",
        GROUP_ID,
        PROVIDER_ID,
    )
    control.grant_leader(
        SESSION_ID,
        "coordinator",
        GROUP_ID,
        PROVIDER_ID,
        "grant-1",
        1,
        1,
        lease_expires_at=LEASE_EXPIRES_AT,
        occurred_at=NOW,
    )
    return control


def _prepare_manifest_intent(
    control: PhaseDControlPlane,
    acknowledgements: DurableAcknowledgementStore,
    request: BatchIngestRequest,
) -> None:
    status = acknowledgements.prepare(
        request,
        policy=control.acknowledgement_policy(SESSION_ID, GROUP_ID),
        replica_provider_ids=(),
        now=request.created_at,
    )
    control.prepare_batch_manifest(
        request,
        status,
        primary_provider_id=PROVIDER_ID,
        now=request.created_at,
    )


def _commit_manifest_item(
    control: PhaseDControlPlane,
    acknowledgements: DurableAcknowledgementStore,
    request: BatchIngestRequest,
) -> None:
    _prepare_manifest_intent(control, acknowledgements, request)
    status = acknowledgements.mark_primary_committed(
        SESSION_ID,
        GROUP_ID,
        request.batch_id,
        now=request.created_at,
    )
    control.commit_batch_manifest(
        request,
        status,
        primary_provider_id=PROVIDER_ID,
        now=request.created_at,
    )


def test_logical_catalog_lists_only_authoritative_manifest_commits(
    tmp_path: Path,
) -> None:
    """Provider files and prepared intents are not Federation-visible commits."""

    control = _storage_control(tmp_path)
    acknowledgements = DurableAcknowledgementStore(tmp_path / "acks.sqlite3")
    provider = FilesystemBatchStorageProvider(tmp_path / "storage")

    provider_only = _request(
        {"state": "provider-only"},
        batch_id="provider-only",
        idempotency_key="provider-only",
    )
    prepared = _request(
        {"state": "prepared"},
        batch_id="prepared-only",
        idempotency_key="prepared-only",
        created_at=NOW + timedelta(seconds=1),
    )
    committed = tuple(
        _request(
            {"state": "committed", "sequence": index},
            batch_id=f"committed-{index}",
            idempotency_key=f"committed-{index}",
            created_at=NOW + timedelta(seconds=index + 2),
        )
        for index in range(3)
    )
    other_dataset = _request(
        {"state": "committed-other-dataset"},
        dataset_id="telemetry-other",
        batch_id="committed-other-dataset",
        idempotency_key="committed-other-dataset",
        created_at=NOW + timedelta(seconds=10),
    )

    provider.ingest(provider_only)
    provider.ingest(prepared)
    _prepare_manifest_intent(control, acknowledgements, prepared)
    for request in (*committed, other_dataset):
        provider.ingest(request)
        _commit_manifest_item(control, acknowledgements, request)

    transport = _ProviderReadTransport(
        {request.batch_id: request.content for request in (*committed, other_dataset)}
    )
    client = PhaseDLogicalStorageClient(
        session_id=SESSION_ID,
        actor_node_id="node-federation-reader",
        control_plane=control,
        transport=transport,
    )

    first = client.list_committed_batches(
        group_id=GROUP_ID,
        dataset_id="telemetry",
        limit=2,
    )
    repeated = client.list_committed_batches(
        group_id=GROUP_ID,
        dataset_id="telemetry",
        limit=2,
    )
    assert isinstance(first, CommittedBatchPage)
    assert all(isinstance(batch, CommittedBatchReference) for batch in first.batches)
    assert _batch_ids(first) == _batch_ids(repeated)
    assert len(first.batches) == 2
    assert first.next_cursor is not None

    second = client.list_committed_batches(
        group_id=GROUP_ID,
        dataset_id="telemetry",
        cursor=first.next_cursor,
        limit=2,
    )
    combined = (*_batch_ids(first), *_batch_ids(second))
    assert len(combined) == len(set(combined))
    assert set(combined) == {request.batch_id for request in committed}
    assert "provider-only" not in combined
    assert "prepared-only" not in combined
    assert "committed-other-dataset" not in combined
    assert second.next_cursor is None
    assert all(
        _value(batch, "dataset_id") == "telemetry"
        for batch in (*first.batches, *second.batches)
    )

    with pytest.raises(FederationValidationError) as rebound_cursor:
        client.list_committed_batches(
            group_id=GROUP_ID,
            dataset_id="telemetry-other",
            cursor=first.next_cursor,
            limit=2,
        )
    assert rebound_cursor.value.code == "invalid-batch-cursor"

    unfiltered = client.list_committed_batches(
        group_id=GROUP_ID,
        limit=10,
    )
    assert set(_batch_ids(unfiltered)) == {
        request.batch_id for request in (*committed, other_dataset)
    }
    assert "provider-only" not in _batch_ids(unfiltered)
    assert "prepared-only" not in _batch_ids(unfiltered)
    assert unfiltered.next_cursor is None
    assert transport.calls == []


def test_logical_committed_read_is_manifest_gated_and_hash_verified(
    tmp_path: Path,
) -> None:
    control = _storage_control(tmp_path)
    acknowledgements = DurableAcknowledgementStore(tmp_path / "acks.sqlite3")
    committed = _request(
        {"schema": "fcp.mtconnect.observations.v1", "observations": [{"sequence": 1}]},
        dataset_id="mtconnect:node-recorder-1:Mazak",
        batch_id=f"Mazak:77:1:1:{'b' * 64}",
        idempotency_key="committed-recorder-batch",
    )
    unmanifested = _request(
        {"state": "provider-only"},
        batch_id="provider-only",
        idempotency_key="provider-only",
    )
    _commit_manifest_item(control, acknowledgements, committed)
    transport = _ProviderReadTransport(
        {
            committed.batch_id: committed.content,
            unmanifested.batch_id: unmanifested.content,
        }
    )
    client = PhaseDLogicalStorageClient(
        session_id=SESSION_ID,
        actor_node_id="node-federation-reader",
        control_plane=control,
        transport=transport,
    )

    page = client.list_committed_batches(
        group_id=GROUP_ID,
        dataset_id=committed.dataset_id,
    )
    assert len(page.batches) == 1
    reference = page.batches[0]
    assert reference.batch_id == committed.batch_id
    assert reference.content_hash == committed.content_hash

    content = asyncio.run(client.read_committed_batch(reference=reference))
    assert content == committed.content
    assert len(transport.calls) == 1

    with pytest.raises(FederationValidationError) as not_committed:
        asyncio.run(
            client.read_committed_batch(
                group_id=GROUP_ID,
                batch_id=unmanifested.batch_id,
            )
        )
    assert not_committed.value.code == "batch-not-found"
    assert len(transport.calls) == 1

    transport.content_by_batch[committed.batch_id] = {
        "schema": "fcp.mtconnect.observations.v1",
        "observations": [{"sequence": 999, "tampered": True}],
    }
    with pytest.raises(FederationValidationError) as tampered:
        asyncio.run(
            client.read_committed_batch(
                group_id=GROUP_ID,
                batch_id=committed.batch_id,
            )
        )
    assert tampered.value.code == "content-hash-mismatch"
