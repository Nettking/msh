from __future__ import annotations

from pathlib import Path

import pytest

from catalog.federation.errors import FederationValidationError
from catalog.federation.storage_control_plane import (
    StorageControlPlaneStore,
    StorageProviderRegistration,
)
from catalog.federation.storage_protocol import STORAGE_PROTOCOL, STORAGE_PROTOCOL_VERSION


def _provider(session_id: str, provider_id: str, node_id: str, **overrides):
    values = {
        "session_id": session_id,
        "provider_id": provider_id,
        "node_id": node_id,
        "protocol": STORAGE_PROTOCOL,
        "protocol_version": STORAGE_PROTOCOL_VERSION,
        "authorized": True,
        "status": "ready",
    }
    values.update(overrides)
    return StorageProviderRegistration(**values)


def test_registration_assignment_and_replay(tmp_path: Path) -> None:
    database = tmp_path / "control.sqlite3"
    store = StorageControlPlaneStore(database)
    store.create_group("session-1", "coordinator", "storage-main")
    store.register_provider("session-1", "coordinator", _provider("session-1", "provider-a", "node-a"))
    store.register_provider("session-1", "coordinator", _provider("session-1", "provider-b", "node-b"))
    store.change_assignment("session-1", "coordinator", "storage-main", "provider-a", ("provider-b",))

    expected = store.snapshot("session-1")
    reconstructed = StorageControlPlaneStore(database).snapshot("session-1")

    assert reconstructed.revision == 4
    assert reconstructed.groups == expected.groups
    assert reconstructed.providers == expected.providers
    assert reconstructed.groups["storage-main"].primary_provider_id == "provider-a"
    assert reconstructed.groups["storage-main"].replica_provider_ids == ("provider-b",)


def test_assignment_requires_ready_authorized_compatible_provider(tmp_path: Path) -> None:
    store = StorageControlPlaneStore(tmp_path / "control.sqlite3")
    store.create_group("session-1", "coordinator", "storage-main")
    store.register_provider(
        "session-1",
        "coordinator",
        _provider("session-1", "provider-a", "node-a", authorized=False),
    )

    with pytest.raises(FederationValidationError) as error:
        store.change_assignment("session-1", "coordinator", "storage-main", "provider-a")

    assert error.value.code == "provider-not-assignable"
    assert store.snapshot("session-1").revision == 2


def test_assignment_has_one_primary_and_distinct_replicas(tmp_path: Path) -> None:
    store = StorageControlPlaneStore(tmp_path / "control.sqlite3")
    store.create_group("session-1", "coordinator", "storage-main")
    store.register_provider("session-1", "coordinator", _provider("session-1", "provider-a", "node-a"))

    with pytest.raises(FederationValidationError) as error:
        store.change_assignment(
            "session-1",
            "coordinator",
            "storage-main",
            "provider-a",
            ("provider-a",),
        )

    assert error.value.code == "invalid-assignment"


def test_assigned_provider_cannot_be_removed(tmp_path: Path) -> None:
    store = StorageControlPlaneStore(tmp_path / "control.sqlite3")
    store.create_group("session-1", "coordinator", "storage-main")
    store.register_provider("session-1", "coordinator", _provider("session-1", "provider-a", "node-a"))
    store.change_assignment("session-1", "coordinator", "storage-main", "provider-a")

    with pytest.raises(FederationValidationError) as error:
        store.remove_provider("session-1", "coordinator", "provider-a")

    assert error.value.code == "provider-assigned"
    assert "provider-a" in store.snapshot("session-1").providers


def test_leader_event_requires_assigned_primary_and_replays(tmp_path: Path) -> None:
    database = tmp_path / "control.sqlite3"
    store = StorageControlPlaneStore(database)
    store.create_group("session-1", "coordinator", "storage-main")
    store.register_provider("session-1", "coordinator", _provider("session-1", "provider-a", "node-a"))

    with pytest.raises(FederationValidationError) as error:
        store.grant_leader("session-1", "coordinator", "storage-main", "provider-a", "grant-1", 1, 10)
    assert error.value.code == "not-assigned-primary"

    store.change_assignment("session-1", "coordinator", "storage-main", "provider-a")
    store.grant_leader("session-1", "coordinator", "storage-main", "provider-a", "grant-1", 1, 10)
    assert store.snapshot("session-1").leader_grants["storage-main"]["grant_id"] == "grant-1"

    store.revoke_leader("session-1", "coordinator", "storage-main", "grant-1")
    assert StorageControlPlaneStore(database).snapshot("session-1").leader_grants == {}


def test_sessions_are_isolated(tmp_path: Path) -> None:
    store = StorageControlPlaneStore(tmp_path / "control.sqlite3")
    store.create_group("session-a", "coordinator", "storage-main")
    store.create_group("session-b", "coordinator", "storage-other")

    assert set(store.snapshot("session-a").groups) == {"storage-main"}
    assert set(store.snapshot("session-b").groups) == {"storage-other"}
    assert [event.revision for event in store.events("session-a")] == [1]
    assert [event.revision for event in store.events("session-b")] == [1]
