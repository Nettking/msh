from __future__ import annotations

import sqlite3
from pathlib import Path

from catalog.federation.storage_control_plane import (
    StorageControlPlaneStore,
    StorageProviderRegistration,
)
from catalog.federation.storage_protocol import (
    STORAGE_PROTOCOL,
    STORAGE_PROTOCOL_VERSION,
)
from catalog.flask_app.services.read_only_storage_authority import (
    ReadOnlyStorageAuthorityStore,
)

SESSION = "session-storage-read-only"
ACTOR = "node-owner"
PROVIDER = "fcp-local-data-storage"
GROUP = "fcp-local-storage"


def _tables(path: Path) -> set[str]:
    with sqlite3.connect(path) as connection:
        return {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }


def test_empty_existing_coordinator_database_is_not_mutated(tmp_path: Path) -> None:
    database = tmp_path / "control.sqlite3"
    with sqlite3.connect(database) as connection:
        connection.execute("CREATE TABLE sentinel(value TEXT)")

    before = _tables(database)
    snapshot = ReadOnlyStorageAuthorityStore(database).snapshot(SESSION)
    after = _tables(database)

    assert snapshot.revision == 0
    assert snapshot.providers == {}
    assert snapshot.groups == {}
    assert before == {"sentinel"}
    assert after == before


def test_existing_storage_authority_is_replayed_without_writing(tmp_path: Path) -> None:
    database = tmp_path / "control.sqlite3"
    authority = StorageControlPlaneStore(database)
    authority.create_group(SESSION, ACTOR, GROUP)
    authority.register_provider(
        SESSION,
        ACTOR,
        StorageProviderRegistration(
            session_id=SESSION,
            provider_id=PROVIDER,
            node_id=ACTOR,
            protocol=STORAGE_PROTOCOL,
            protocol_version=STORAGE_PROTOCOL_VERSION,
            authorized=True,
            status="ready",
        ),
    )
    authority.change_assignment(
        SESSION,
        ACTOR,
        GROUP,
        PROVIDER,
    )
    before = database.read_bytes()

    snapshot = ReadOnlyStorageAuthorityStore(database).snapshot(SESSION)
    after = database.read_bytes()

    assert snapshot.revision == 3
    assert snapshot.providers[PROVIDER].assignable is True
    assert snapshot.groups[GROUP].primary_provider_id == PROVIDER
    assert after == before
