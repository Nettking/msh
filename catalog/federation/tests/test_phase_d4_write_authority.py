from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from catalog.federation.errors import FederationValidationError
from catalog.federation.local_storage import FilesystemBatchStorageProvider
from catalog.federation.storage_control_plane import (
    StorageControlPlaneStore,
    StorageProviderRegistration,
)
from catalog.federation.storage_protocol import (
    STORAGE_PROTOCOL,
    STORAGE_PROTOCOL_VERSION,
    BatchIngestRequest,
    StorageErrorCode,
    StorageOperation,
    StorageRequestEnvelope,
    WriteAuthority,
)
from catalog.federation.write_authority import (
    AuthorizedStorageService,
    StorageWriteAuthorityValidator,
)

NOW = datetime(2026, 7, 29, 12, 0, tzinfo=timezone.utc)
EXPIRY = NOW + timedelta(minutes=5)


def _registration(provider_id: str, node_id: str) -> StorageProviderRegistration:
    return StorageProviderRegistration(
        session_id="session-1",
        provider_id=provider_id,
        node_id=node_id,
        protocol=STORAGE_PROTOCOL,
        protocol_version=STORAGE_PROTOCOL_VERSION,
        authorized=True,
        status="ready",
    )


def _control_plane(path: Path) -> StorageControlPlaneStore:
    store = StorageControlPlaneStore(path)
    store.create_group("session-1", "coordinator", "storage-main")
    store.register_provider("session-1", "coordinator", _registration("provider-a", "node-a"))
    store.register_provider("session-1", "coordinator", _registration("provider-b", "node-b"))
    store.change_assignment(
        "session-1", "coordinator", "storage-main", "provider-a", ("provider-b",)
    )
    store.grant_leader(
        "session-1",
        "coordinator",
        "storage-main",
        "provider-a",
        "grant-1",
        4,
        104,
        lease_expires_at=EXPIRY,
        occurred_at=NOW,
    )
    return store


def _request(**authority_overrides) -> BatchIngestRequest:
    authority = {
        "session_id": "session-1",
        "group_id": "storage-main",
        "actor_node_id": "node-a",
        "grant_id": "grant-1",
        "term": 4,
        "fencing_token": 104,
        "lease_expires_at": EXPIRY,
    }
    authority.update(authority_overrides)
    content = {"observations": [{"sequence": 1, "value": 12.5}]}
    return BatchIngestRequest(
        authority=WriteAuthority(**authority),
        dataset_id="telemetry",
        batch_id="batch-1",
        idempotency_key="idem-1",
        content_hash=BatchIngestRequest.calculate_content_hash(content),
        content=content,
        created_at=NOW,
    )


def _envelope(request: BatchIngestRequest) -> StorageRequestEnvelope:
    return StorageRequestEnvelope(
        request_id="request-1",
        protocol=STORAGE_PROTOCOL,
        protocol_version=STORAGE_PROTOCOL_VERSION,
        operation=StorageOperation.BATCH_INGEST,
        session_id=request.authority.session_id,
        actor_node_id=request.authority.actor_node_id,
        authorization_context={},
        payload=request.to_dict(),
    )


def _service(tmp_path: Path, store: StorageControlPlaneStore, *, now: datetime = NOW):
    validator = StorageWriteAuthorityValidator(store, now=lambda: now)
    provider = FilesystemBatchStorageProvider(tmp_path / "storage")
    return AuthorizedStorageService(provider, validator), provider


def test_valid_primary_write_is_committed(tmp_path: Path) -> None:
    store = _control_plane(tmp_path / "control.sqlite3")
    service, provider = _service(tmp_path, store)

    response = service.dispatch(_envelope(_request()))

    assert response.ok
    assert response.result["state"] == "stored"
    assert provider.exists(session_id="session-1", group_id="storage-main", batch_id="batch-1")


@pytest.mark.parametrize(
    ("overrides", "expected"),
    [
        ({"actor_node_id": "node-b"}, StorageErrorCode.NOT_PRIMARY),
        ({"grant_id": "grant-old"}, StorageErrorCode.UNKNOWN_GRANT),
        ({"term": 3}, StorageErrorCode.STALE_TERM),
        ({"fencing_token": 103}, StorageErrorCode.STALE_FENCING_TOKEN),
        ({"lease_expires_at": EXPIRY + timedelta(minutes=1)}, StorageErrorCode.UNKNOWN_GRANT),
    ],
)
def test_invalid_authority_is_rejected_before_provider_access(
    tmp_path: Path,
    overrides: dict[str, object],
    expected: StorageErrorCode,
) -> None:
    store = _control_plane(tmp_path / "control.sqlite3")
    service, provider = _service(tmp_path, store)

    response = service.dispatch(_envelope(_request(**overrides)))

    assert not response.ok
    assert response.error.code is expected
    assert not provider.exists(session_id="session-1", group_id="storage-main", batch_id="batch-1")


def test_expired_lease_is_rejected(tmp_path: Path) -> None:
    store = _control_plane(tmp_path / "control.sqlite3")
    service, provider = _service(tmp_path, store, now=EXPIRY)

    response = service.dispatch(_envelope(_request()))

    assert not response.ok
    assert response.error.code is StorageErrorCode.LEASE_EXPIRED
    assert not provider.exists(session_id="session-1", group_id="storage-main", batch_id="batch-1")


def test_revoked_grant_is_rejected(tmp_path: Path) -> None:
    store = _control_plane(tmp_path / "control.sqlite3")
    store.revoke_leader("session-1", "coordinator", "storage-main", "grant-1")
    service, _provider = _service(tmp_path, store)

    response = service.dispatch(_envelope(_request()))

    assert not response.ok
    assert response.error.code is StorageErrorCode.UNKNOWN_GRANT


def test_old_primary_is_fenced_after_new_grant(tmp_path: Path) -> None:
    store = _control_plane(tmp_path / "control.sqlite3")
    store.revoke_leader("session-1", "coordinator", "storage-main", "grant-1")
    store.change_assignment("session-1", "coordinator", "storage-main", "provider-b", ("provider-a",))
    store.grant_leader(
        "session-1",
        "coordinator",
        "storage-main",
        "provider-b",
        "grant-2",
        5,
        105,
        lease_expires_at=EXPIRY + timedelta(minutes=5),
        occurred_at=NOW + timedelta(minutes=1),
    )
    service, _provider = _service(tmp_path, store, now=NOW + timedelta(minutes=2))

    response = service.dispatch(_envelope(_request()))

    assert not response.ok
    assert response.error.code is StorageErrorCode.NOT_PRIMARY


def test_grant_scope_is_enforced(tmp_path: Path) -> None:
    store = StorageControlPlaneStore(tmp_path / "control.sqlite3")
    store.create_group("session-1", "coordinator", "storage-main")
    store.register_provider("session-1", "coordinator", _registration("provider-a", "node-a"))
    store.change_assignment("session-1", "coordinator", "storage-main", "provider-a")
    store.grant_leader(
        "session-1",
        "coordinator",
        "storage-main",
        "provider-a",
        "grant-1",
        4,
        104,
        lease_expires_at=EXPIRY,
        scopes=(StorageOperation.REPLICA_ACKNOWLEDGE.value,),
        occurred_at=NOW,
    )
    service, _provider = _service(tmp_path, store)

    response = service.dispatch(_envelope(_request()))

    assert not response.ok
    assert response.error.code is StorageErrorCode.UNAUTHORIZED


def test_replacement_grants_must_advance_term_and_fencing_token(tmp_path: Path) -> None:
    store = _control_plane(tmp_path / "control.sqlite3")

    with pytest.raises(FederationValidationError) as stale_term:
        store.grant_leader(
            "session-1",
            "coordinator",
            "storage-main",
            "provider-a",
            "grant-2",
            4,
            105,
            lease_expires_at=EXPIRY + timedelta(minutes=5),
            occurred_at=NOW + timedelta(minutes=1),
        )
    assert stale_term.value.code == StorageErrorCode.STALE_TERM.value

    with pytest.raises(FederationValidationError) as stale_token:
        store.grant_leader(
            "session-1",
            "coordinator",
            "storage-main",
            "provider-a",
            "grant-2",
            5,
            104,
            lease_expires_at=EXPIRY + timedelta(minutes=5),
            occurred_at=NOW + timedelta(minutes=1),
        )
    assert stale_token.value.code == StorageErrorCode.STALE_FENCING_TOKEN.value
