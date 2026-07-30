from __future__ import annotations

import json
import sqlite3
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from catalog.federation.errors import FederationValidationError
from catalog.federation.manifest import ManifestItemKind
from catalog.federation.manifest_store import (
    AuthoritativeManifestStore,
    ManifestCommitIntent,
    ManifestItemProposal,
)
from catalog.federation.storage_control_plane import StorageControlPlaneStore

NOW = datetime(2026, 7, 29, 12, 0, tzinfo=timezone.utc)


def create_groups(database: Path, *group_ids: str) -> int:
    control = StorageControlPlaneStore(database)
    revision = 0
    for group_id in group_ids:
        revision = control.create_group(
            "session-1",
            "coordinator-1",
            group_id,
        ).revision
    return revision


def proposal(
    *,
    group_id: str = "storage-main",
    item_id: str = "batch-1",
    idempotency_key: str = "recorder-a:batch-1",
    content_digit: str = "1",
    dataset_id: str = "telemetry",
    schema_name: str = "msh.telemetry",
    schema_version: int = 1,
    expected_control_revision: int = 1,
    committed_at: datetime = NOW,
) -> ManifestItemProposal:
    return ManifestItemProposal(
        session_id="session-1",
        group_id=group_id,
        item_id=item_id,
        kind=ManifestItemKind.BATCH,
        dataset_id=dataset_id,
        idempotency_key=idempotency_key,
        content_hash="sha256:" + content_digit * 64,
        size_bytes=128,
        schema_name=schema_name,
        schema_version=schema_version,
        source_id=None,
        first_sequence=None,
        last_sequence=None,
        dataset_required=True,
        acknowledged_provider_ids=("provider-b", "provider-a"),
        term=1,
        expected_control_revision=expected_control_revision,
        committed_at=committed_at,
    )


def intent(
    *,
    item_id: str = "batch-1",
    idempotency_key: str = "recorder-a:batch-1",
    content_digit: str = "1",
    expected_control_revision: int = 1,
) -> ManifestCommitIntent:
    value = proposal(
        item_id=item_id,
        idempotency_key=idempotency_key,
        content_digit=content_digit,
        expected_control_revision=expected_control_revision,
    )
    return ManifestCommitIntent(
        session_id=value.session_id,
        group_id=value.group_id,
        item_id=value.item_id,
        kind=value.kind,
        dataset_id=value.dataset_id,
        idempotency_key=value.idempotency_key,
        content_hash=value.content_hash,
        size_bytes=value.size_bytes,
        schema_name=value.schema_name,
        schema_version=value.schema_version,
        source_id=value.source_id,
        first_sequence=value.first_sequence,
        last_sequence=value.last_sequence,
        dataset_required=value.dataset_required,
        primary_provider_id="provider-a",
        assigned_replica_ids=("provider-b",),
        required_replica_acks=1,
        grant_id="grant-1",
        term=1,
        fencing_token=10,
        lease_expires_at=NOW + timedelta(minutes=5),
        prepared_control_revision=expected_control_revision,
        prepared_at=NOW,
    )


def test_e1_genesis_is_deterministic_idempotent_and_restart_safe(
    tmp_path: Path,
) -> None:
    database = tmp_path / "coordinator.sqlite3"
    control_revision = create_groups(database, "storage-main")
    store = AuthoritativeManifestStore(database)

    first = store.ensure_genesis(
        "session-1",
        "storage-main",
        term=0,
        expected_control_revision=control_revision,
        now=NOW,
    )
    repeated = store.ensure_genesis(
        "session-1",
        "storage-main",
        term=0,
        expected_control_revision=control_revision,
        now=NOW + timedelta(hours=1),
    )

    assert repeated == first
    assert first.revision == 0
    assert first.term == 0
    assert first.previous_manifest_hash is None
    assert first.datasets == ()
    assert first.items == ()
    assert AuthoritativeManifestStore(database).head(
        "session-1",
        "storage-main",
    ) == first
    assert AuthoritativeManifestStore(database).history(
        "session-1",
        "storage-main",
    ) == (first,)

    with sqlite3.connect(database) as connection:
        assert connection.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        assert connection.execute("PRAGMA synchronous").fetchone()[0] == 2


def test_e1_commits_hash_chained_revisions_and_reconstructs_them(
    tmp_path: Path,
) -> None:
    database = tmp_path / "coordinator.sqlite3"
    revision = create_groups(database, "storage-main")
    store = AuthoritativeManifestStore(database)
    genesis = store.ensure_genesis(
        "session-1",
        "storage-main",
        term=0,
        expected_control_revision=revision,
        now=NOW,
    )

    first = store.commit(proposal())
    second = store.commit(
        proposal(
            item_id="batch-2",
            idempotency_key="recorder-a:batch-2",
            content_digit="2",
            committed_at=NOW + timedelta(seconds=1),
        )
    )

    assert first.created is True
    assert first.item_revision == 1
    assert second.created is True
    assert second.item_revision == 2
    assert first.manifest.previous_manifest_hash == genesis.manifest_hash
    assert second.manifest.previous_manifest_hash == first.manifest.manifest_hash
    assert [item.item_id for item in second.manifest.items] == [
        "batch-1",
        "batch-2",
    ]
    assert second.manifest.datasets[0].last_contiguous_sequence is None
    assert second.manifest.datasets[0].missing_ranges == ()
    assert second.manifest.items[0].acknowledged_provider_ids == (
        "provider-a",
        "provider-b",
    )

    restarted = AuthoritativeManifestStore(database)
    assert restarted.history("session-1", "storage-main") == (
        genesis,
        first.manifest,
        second.manifest,
    )
    assert restarted.at_revision("session-1", "storage-main", 1) == first.manifest
    assert restarted.head("session-1", "storage-main") == second.manifest


def test_e1_exact_duplicate_is_idempotent_without_a_new_revision(
    tmp_path: Path,
) -> None:
    database = tmp_path / "coordinator.sqlite3"
    revision = create_groups(database, "storage-main")
    store = AuthoritativeManifestStore(database)
    store.ensure_genesis(
        "session-1",
        "storage-main",
        term=0,
        expected_control_revision=revision,
        now=NOW,
    )
    value = proposal()

    created = store.commit(value)
    duplicate = store.commit(value)

    assert created.created is True
    assert duplicate.created is False
    assert duplicate.item_revision == created.item_revision == 1
    assert duplicate.manifest == created.manifest
    assert len(store.history("session-1", "storage-main")) == 2


def test_e1_rejects_item_and_idempotency_identity_conflicts(
    tmp_path: Path,
) -> None:
    database = tmp_path / "coordinator.sqlite3"
    revision = create_groups(database, "storage-main")
    store = AuthoritativeManifestStore(database)
    store.ensure_genesis(
        "session-1",
        "storage-main",
        term=0,
        expected_control_revision=revision,
        now=NOW,
    )
    store.commit(proposal())

    with pytest.raises(FederationValidationError) as item_error:
        store.commit(proposal(content_digit="9"))
    assert item_error.value.code == "manifest-item-conflict"

    with pytest.raises(FederationValidationError) as key_error:
        store.commit(
            proposal(
                item_id="batch-2",
                idempotency_key="recorder-a:batch-1",
                content_digit="2",
            )
        )
    assert key_error.value.code == "manifest-idempotency-conflict"
    assert store.head("session-1", "storage-main").revision == 1


def test_e1_rejects_dataset_contract_conflicts(
    tmp_path: Path,
) -> None:
    database = tmp_path / "coordinator.sqlite3"
    revision = create_groups(database, "storage-main")
    store = AuthoritativeManifestStore(database)
    store.ensure_genesis(
        "session-1",
        "storage-main",
        term=0,
        expected_control_revision=revision,
        now=NOW,
    )
    store.commit(proposal())

    conflicting = proposal(
        item_id="batch-2",
        idempotency_key="recorder-a:batch-2",
        content_digit="2",
        schema_version=2,
    )
    with pytest.raises(FederationValidationError) as error:
        store.commit(conflicting)

    assert error.value.code == "manifest-dataset-conflict"
    assert store.head("session-1", "storage-main").revision == 1


def test_e1_storage_groups_have_isolated_manifest_chains(
    tmp_path: Path,
) -> None:
    database = tmp_path / "coordinator.sqlite3"
    revision = create_groups(database, "storage-a", "storage-b")
    store = AuthoritativeManifestStore(database)
    for group_id in ("storage-a", "storage-b"):
        store.ensure_genesis(
            "session-1",
            group_id,
            term=0,
            expected_control_revision=revision,
            now=NOW,
        )

    first = store.commit(
        proposal(group_id="storage-a", expected_control_revision=revision)
    )
    second = store.commit(
        proposal(group_id="storage-b", expected_control_revision=revision)
    )

    assert first.manifest.group_id == "storage-a"
    assert second.manifest.group_id == "storage-b"
    assert first.manifest.revision == second.manifest.revision == 1
    assert first.manifest.manifest_hash != second.manifest.manifest_hash
    assert len(store.history("session-1", "storage-a")) == 2
    assert len(store.history("session-1", "storage-b")) == 2


def test_e1_requires_registered_group_and_exact_control_revision(
    tmp_path: Path,
) -> None:
    database = tmp_path / "coordinator.sqlite3"
    revision = create_groups(database, "storage-main")
    store = AuthoritativeManifestStore(database)

    with pytest.raises(FederationValidationError) as unknown:
        store.ensure_genesis(
            "session-1",
            "unknown",
            term=0,
            expected_control_revision=revision,
            now=NOW,
        )
    assert unknown.value.code == "unknown-storage-group"

    with pytest.raises(FederationValidationError) as stale_genesis:
        store.ensure_genesis(
            "session-1",
            "storage-main",
            term=0,
            expected_control_revision=revision - 1,
            now=NOW,
        )
    assert stale_genesis.value.code == "concurrent-control-change"

    store.ensure_genesis(
        "session-1",
        "storage-main",
        term=0,
        expected_control_revision=revision,
        now=NOW,
    )
    current_revision = create_groups(database, "another-group")
    with pytest.raises(FederationValidationError) as stale_commit:
        store.commit(proposal(expected_control_revision=revision))
    assert stale_commit.value.code == "concurrent-control-change"

    result = store.commit(
        replace(
            proposal(expected_control_revision=revision),
            expected_control_revision=current_revision,
        )
    )
    assert result.created is True


def test_e1_corrupt_revision_fails_closed_after_restart(
    tmp_path: Path,
) -> None:
    database = tmp_path / "coordinator.sqlite3"
    revision = create_groups(database, "storage-main")
    store = AuthoritativeManifestStore(database)
    store.ensure_genesis(
        "session-1",
        "storage-main",
        term=0,
        expected_control_revision=revision,
        now=NOW,
    )
    store.commit(proposal())

    with sqlite3.connect(database) as connection:
        row = connection.execute(
            """SELECT manifest_json FROM storage_manifest_revisions
               WHERE session_id=? AND group_id=? AND revision=1""",
            ("session-1", "storage-main"),
        ).fetchone()
        encoded = json.loads(row[0])
        encoded["items"][0]["content_hash"] = "sha256:" + "f" * 64
        connection.execute(
            """UPDATE storage_manifest_revisions SET manifest_json=?
               WHERE session_id=? AND group_id=? AND revision=1""",
            (
                json.dumps(encoded, sort_keys=True, separators=(",", ":")),
                "session-1",
                "storage-main",
            ),
        )

    restarted = AuthoritativeManifestStore(database)
    with pytest.raises(FederationValidationError) as error:
        restarted.head("session-1", "storage-main")

    assert error.value.code == "manifest-hash-mismatch"


def test_e1_coordinator_intent_is_restart_safe_and_finalizes_once(
    tmp_path: Path,
) -> None:
    database = tmp_path / "coordinator.sqlite3"
    revision = create_groups(database, "storage-main")
    store = AuthoritativeManifestStore(database)
    store.ensure_genesis(
        "session-1",
        "storage-main",
        term=0,
        expected_control_revision=revision,
        now=NOW,
    )
    prepared = store.prepare_intent(intent())

    restarted = AuthoritativeManifestStore(database)
    assert restarted.prepare_intent(intent()) == prepared
    assert restarted.pending_intents(primary_provider_id="provider-a") == (
        prepared,
    )
    committed = restarted.finalize_intent(
        prepared,
        acknowledged_provider_ids=("provider-a", "provider-b"),
        expected_control_revision=revision,
        manifest_term=1,
        now=NOW + timedelta(seconds=1),
    )
    repeated = restarted.finalize_intent(
        prepared,
        acknowledged_provider_ids=("provider-a", "provider-b"),
        expected_control_revision=revision,
        manifest_term=1,
        now=NOW + timedelta(seconds=2),
    )

    assert committed.created
    assert not repeated.created
    assert repeated.item_revision == committed.item_revision == 1
    assert restarted.pending_intents(primary_provider_id="provider-a") == ()
    assert restarted.intent(
        "session-1",
        "storage-main",
        "batch-1",
    ).committed_revision == 1


def test_e1_projection_corruption_fails_closed(
    tmp_path: Path,
) -> None:
    database = tmp_path / "coordinator.sqlite3"
    revision = create_groups(database, "storage-main")
    store = AuthoritativeManifestStore(database)
    store.ensure_genesis(
        "session-1",
        "storage-main",
        term=0,
        expected_control_revision=revision,
        now=NOW,
    )
    store.commit(proposal())
    with sqlite3.connect(database) as connection:
        connection.execute(
            """DELETE FROM storage_manifest_items
               WHERE session_id='session-1' AND group_id='storage-main'
                 AND item_id='batch-1'"""
        )

    with pytest.raises(FederationValidationError) as error:
        store.commit(
            proposal(
                item_id="batch-2",
                idempotency_key="recorder-a:batch-2",
                content_digit="2",
            )
        )

    assert error.value.code == "manifest-projection-mismatch"


def test_e1_manifest_revision_and_head_roll_back_together(
    tmp_path: Path,
) -> None:
    database = tmp_path / "coordinator.sqlite3"
    revision = create_groups(database, "storage-main")
    store = AuthoritativeManifestStore(database)
    genesis = store.ensure_genesis(
        "session-1",
        "storage-main",
        term=0,
        expected_control_revision=revision,
        now=NOW,
    )
    with sqlite3.connect(database) as connection:
        connection.execute(
            """CREATE TRIGGER fail_manifest_head_update
               BEFORE UPDATE ON storage_manifest_heads
               BEGIN SELECT RAISE(ABORT, 'injected head failure'); END"""
        )

    with pytest.raises(sqlite3.IntegrityError):
        store.commit(proposal())

    with sqlite3.connect(database) as connection:
        connection.execute("DROP TRIGGER fail_manifest_head_update")
        assert connection.execute(
            "SELECT COUNT(*) FROM storage_manifest_items"
        ).fetchone()[0] == 0
        assert connection.execute(
            "SELECT COUNT(*) FROM storage_manifest_datasets"
        ).fetchone()[0] == 0
    assert store.head("session-1", "storage-main") == genesis
    assert store.history("session-1", "storage-main") == (genesis,)


def test_e1_legacy_phase_d_group_requires_verified_inventory(
    tmp_path: Path,
) -> None:
    database = tmp_path / "legacy-control.sqlite3"
    revision = create_groups(database, "storage-main")
    with sqlite3.connect(database) as connection:
        connection.execute(
            """CREATE TABLE storage_fencing_counters (
                   session_id TEXT NOT NULL,
                   group_id TEXT NOT NULL,
                   last_term INTEGER NOT NULL,
                   last_fencing_token INTEGER NOT NULL,
                   PRIMARY KEY(session_id, group_id)
               )"""
        )

    store = AuthoritativeManifestStore(database)
    with pytest.raises(FederationValidationError) as error:
        store.ensure_genesis(
            "session-1",
            "storage-main",
            term=0,
            expected_control_revision=revision,
            now=NOW,
        )

    assert error.value.code == "manifest-inventory-required"
