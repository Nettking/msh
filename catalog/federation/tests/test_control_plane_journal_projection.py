"""Canonical rows and full public metadata survive materialization unchanged."""

from __future__ import annotations

import copy
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from catalog.federation.control_plane_journal import (
    LEGACY_COORDINATOR_ID,
    REPLICATED_COORDINATOR_ID,
    journal_history_digest,
    public_rows_digest,
)
from catalog.federation.control_plane_journal_projection import (
    ROW_COLUMNS,
    JournalSessionLeadershipService,
    project_product_journal,
)
from catalog.federation.control_plane_readiness import BOOTSTRAP_SEAL_CAPABILITY_ID
from catalog.federation.control_plane_replication import ControlPlaneError
from catalog.federation.models import CapabilityAnnouncement, CapabilityStatus
from catalog.federation.persistence import CoordinatorStore, _request_key
from catalog.federation.session_leadership import SessionLeadershipService
from catalog.federation.tests.test_control_plane_product_journal import (
    LEADERSHIP_SCHEMA,
    SESSION,
    STAMP,
    command,
    initial,
    initialize,
    row,
)

HEARTBEAT = "2026-09-08T23:15:00+00:00"


def _created():
    return row(1, event_type="session.created", payload={"session_id": SESSION})


def _announcement():
    return CapabilityAnnouncement(
        capability_id="public-metadata",
        node_id="voter-a",
        session_id=SESSION,
        type="journal-regression",
        protocol="typed-public-stream",
        protocol_version="3.2",
        status=CapabilityStatus.READY,
        properties={
            "label": "Exact metadata",
            "supported": ["read", "metadata"],
            "nested": {"revision": 4},
        },
        announced_at=datetime(2026, 9, 8, 10, 4, tzinfo=timezone.utc),
    )


def _state_with_capability(*, health: bool = True):
    machine, state = initial()
    announcement = _announcement()
    state, _ = machine.apply(
        state,
        command(
            "CAPABILITY_DECLARE",
            {
                "session_id": SESSION,
                "capability_id": announcement.capability_id,
                "capability_type": announcement.type,
                "owner_node_id": announcement.node_id,
                "occurred_at": STAMP,
            },
        ),
    )
    changed = replace(
        announcement,
        protocol_version="3.3",
        properties={"label": "Updated", "limit": 17},
        announced_at=datetime(2026, 9, 8, 11, 5, tzinfo=timezone.utc),
    )
    rows = [
        _created(),
        row(2, event_type="capability.registered", payload=announcement.to_dict()),
        row(3, event_type="capability.status.changed", payload=changed.to_dict()),
    ]
    if health:
        rows.append(
            row(
                4,
                event_type="capability.health.changed",
                actor=REPLICATED_COORDINATOR_ID,
                payload={
                    "capability_id": changed.capability_id,
                    "node_id": changed.node_id,
                    "status": "unavailable",
                    "reason": "node-disconnected",
                },
            )
        )
    state, _ = machine.apply(state, initialize(rows))
    return (
        state,
        rows,
        replace(changed, status=CapabilityStatus.UNAVAILABLE) if health else changed,
    )


def _projection_store(root: Path, state):
    """Build the authority-only SQLite input provided by the base materializer."""

    store = CoordinatorStore(
        root / "coordinator.sqlite3", coordinator_id=REPLICATED_COORDINATOR_ID
    )
    with store.transaction() as database:
        for node_id, node in state["nodes"].items():
            database.execute(
                "INSERT INTO nodes(node_id,display_name,public_key,created_at,identity_version,enrolled_at) VALUES(?,?,?,?,1,?)",
                (node_id, node["display_name"], node["public_key"], STAMP, STAMP),
            )
            database.execute(
                "INSERT INTO node_connectivity(node_id,state,last_heartbeat_at,connection_id) VALUES(?,'connected',?,'local-connection')",
                (node_id, HEARTBEAT),
            )
        database.execute(
            "INSERT INTO sessions(session_id,display_name,state,revision,created_at,created_by_node_id,coordinator_id) VALUES(?,?,'active',0,?,?,?)",
            (
                SESSION,
                state["sessions"][SESSION]["display_name"],
                STAMP,
                "voter-a",
                REPLICATED_COORDINATOR_ID,
            ),
        )
        for node_id in state["memberships"][SESSION]:
            database.execute(
                "INSERT INTO session_memberships(session_id,node_id,joined_at) VALUES(?,?,?)",
                (SESSION, node_id, STAMP),
            )
        for capability_id, value in state["capabilities"].get(SESSION, {}).items():
            if capability_id != BOOTSTRAP_SEAL_CAPABILITY_ID:
                database.execute(
                    "INSERT INTO capabilities(session_id,capability_id,node_id,type,protocol,protocol_version,status,properties_json,announced_at,last_heartbeat_at) VALUES(?,?,?,?,'replicated-authority','1','ready','{}',?,?)",
                    (
                        SESSION,
                        capability_id,
                        value["owner_node_id"],
                        value["capability_type"],
                        STAMP,
                        HEARTBEAT,
                    ),
                )
    runtime = SimpleNamespace(
        node=SimpleNamespace(state=state),
        journal=SimpleNamespace(active=False),
        local=SimpleNamespace(store=store),
    )
    return store, runtime


def _insert(database, rows):
    for value in rows:
        database.execute(
            f"INSERT INTO session_events({','.join(ROW_COLUMNS)}) VALUES(?,?,?,?,?,?,?,?,?)",
            tuple(value[column] for column in ROW_COLUMNS),
        )


def _public_rows(store):
    with store.read_transaction() as database:
        return [
            dict(value)
            for value in database.execute(
                "SELECT * FROM session_events WHERE session_id=? ORDER BY revision",
                (SESSION,),
            )
        ]


def test_projection_preserves_exact_prefix_and_restores_full_capability_metadata(
    tmp_path: Path,
):
    state, rows, expected = _state_with_capability()
    store, runtime = _projection_store(tmp_path, state)
    with store.transaction() as database:
        _insert(database, rows[:2])
        database.execute(
            "UPDATE sessions SET revision=2 WHERE session_id=?", (SESSION,)
        )
    for _ in range(2):
        with store.transaction() as database:
            project_product_journal(runtime, database, state)
    assert _public_rows(store) == rows
    assert store.get_session(SESSION).revision == len(rows)
    assert store.get_session(SESSION).created_at.isoformat() == rows[0]["occurred_at"]
    assert store.list_capabilities(session_id=SESSION) == (expected,)
    with store.read_transaction() as database:
        heartbeat = database.execute(
            "SELECT last_heartbeat_at FROM capabilities"
        ).fetchone()[0]
        connectivity = dict(
            database.execute(
                "SELECT * FROM node_connectivity WHERE node_id='voter-a'"
            ).fetchone()
        )
    assert heartbeat == HEARTBEAT
    assert connectivity["state"] == "connected"
    assert connectivity["last_heartbeat_at"] == HEARTBEAT
    assert connectivity["connection_id"] == "local-connection"


@pytest.mark.parametrize("field", ROW_COLUMNS)
def test_any_existing_canonical_row_conflict_fails_without_rewriting(
    tmp_path: Path, field: str
):
    state, rows, _expected = _state_with_capability()
    store, runtime = _projection_store(tmp_path, state)
    changed = dict(rows[0])
    if field == "revision":
        changed[field] = 2
    elif field == "payload_json":
        # Keep the existing SQLite json_valid constraint satisfied so this
        # exercises prefix identity, rather than failing during fixture setup.
        changed[field] = '{"different":true}'
    elif field == "session_id":
        # A crossed session ID is tested by the canonical state validator; do
        # not invent a different SQLite session just to evade its foreign key.
        state = copy.deepcopy(state)
        state["product_journal"]["sessions"][SESSION]["rows"][0][field] = (
            "wrong-session"
        )
    else:
        changed[field] += "-different"
    with store.transaction() as database:
        _insert(database, [changed])
        database.execute(
            "UPDATE sessions SET revision=1 WHERE session_id=?", (SESSION,)
        )
    before = _public_rows(store)
    with pytest.raises(ControlPlaneError), store.transaction() as database:
        project_product_journal(runtime, database, state)
    assert _public_rows(store) == before
    assert store.get_session(SESSION).revision == 1


def test_projection_refuses_extra_local_rows_and_ahead_revision(tmp_path: Path):
    state, rows, _expected = _state_with_capability()
    store, runtime = _projection_store(tmp_path, state)
    with store.transaction() as database:
        _insert(database, rows + [row(len(rows) + 1)])
    before = _public_rows(store)
    with (
        pytest.raises(ControlPlaneError, match="canonical prefix"),
        store.transaction() as database,
    ):
        project_product_journal(runtime, database, state)
    assert _public_rows(store) == before
    with store.transaction() as database:
        database.execute(
            "UPDATE sessions SET revision=100 WHERE session_id=?", (SESSION,)
        )
    with (
        pytest.raises(ControlPlaneError, match="ahead"),
        store.transaction() as database,
    ):
        project_product_journal(runtime, database, state)
    assert store.get_session(SESSION).revision == 100


def test_committed_ready_status_is_not_downgraded_by_local_disconnection(
    tmp_path: Path,
):
    state, _rows, expected = _state_with_capability(health=False)
    store, runtime = _projection_store(tmp_path, state)
    with store.transaction() as database:
        database.execute("UPDATE capabilities SET status='unavailable'")
        database.execute("UPDATE node_connectivity SET state='disconnected'")
        project_product_journal(runtime, database, state)
    assert store.list_capabilities(session_id=SESSION) == (expected,)
    with store.read_transaction() as database:
        assert (
            database.execute("SELECT state FROM node_connectivity LIMIT 1").fetchone()[
                0
            ]
            == "disconnected"
        )


def test_canonical_capability_without_full_metadata_fails_closed(tmp_path: Path):
    state, _rows, _expected = _state_with_capability()
    state["product_journal"]["sessions"][SESSION].update(
        rows=[_created()], revision=1, prefix_digest=public_rows_digest([_created()])
    )
    store, runtime = _projection_store(tmp_path, state)
    with (
        pytest.raises(ControlPlaneError, match="full public metadata"),
        store.transaction() as database,
    ):
        project_product_journal(runtime, database, state)
    assert _public_rows(store) == []


def test_withdrawn_member_retains_full_historical_metadata_as_revoked(tmp_path: Path):
    state, rows, expected = _state_with_capability(health=False)
    # A later committed revocation need not retain the narrow capability entry;
    # the complete public declaration remains useful as revoked history.
    state["capabilities"][SESSION].clear()
    state["revocations"]["voter-a"] = {"reason": "withdrawn", "occurred_at": STAMP}
    store, runtime = _projection_store(tmp_path, state)
    with store.transaction() as database:
        project_product_journal(runtime, database, state)
    assert _public_rows(store) == rows
    assert store.list_capabilities(session_id=SESSION) == (
        replace(expected, status=CapabilityStatus.REVOKED),
    )


def test_witnessed_legacy_leader_works_without_global_coordinator_whitelist(
    tmp_path: Path,
):
    machine, state = initial()
    state, _ = machine.apply(
        state,
        command(
            "LEADER_TRANSITION",
            {
                "session_id": SESSION,
                "previous_leader_node_id": "voter-a",
                "leader_node_id": "voter-b",
                "term": 2,
                "occurred_at": STAMP,
            },
        ),
    )
    legacy = row(
        2,
        event_type="session.leader.changed",
        actor=LEGACY_COORDINATOR_ID,
        payload={
            "schema": LEADERSHIP_SCHEMA,
            "session_id": SESSION,
            "previous_leader_node_id": "voter-a",
            "leader_node_id": "voter-b",
            "term": 2,
            "reason": "witnessed-legacy-handover",
        },
    )
    member_lookalike = row(
        3,
        event_type="session.leader.changed",
        actor="voter-c",
        payload={
            "schema": LEADERSHIP_SCHEMA,
            "session_id": SESSION,
            "previous_leader_node_id": "voter-b",
            "leader_node_id": "voter-c",
            "term": 3,
        },
    )
    state, _ = machine.apply(state, initialize([_created(), legacy, member_lookalike]))
    store, runtime = _projection_store(tmp_path, state)
    with store.transaction() as database:
        project_product_journal(runtime, database, state)
    assert SessionLeadershipService(store).current(SESSION).leader_node_id == "voter-a"
    service = JournalSessionLeadershipService(store, runtime)
    leadership = service.current(SESSION)
    assert (leadership.leader_node_id, leadership.term) == ("voter-b", 2)
    assert leadership.leader_connected is True
    assert _public_rows(store) == [_created(), legacy, member_lookalike]
    with store.transaction() as database:
        database.execute(
            "UPDATE session_events SET event_id='unwitnessed-id' WHERE revision=2"
        )
    with pytest.raises(ControlPlaneError, match="exact canonical"):
        service.current(SESSION)


def test_leadership_staged_suffix_does_not_authorize_unwitnessed_legacy_actor(
    tmp_path: Path,
):
    machine, state = initial()
    state, _ = machine.apply(state, initialize([_created()]))
    store, runtime = _projection_store(tmp_path, state)
    with store.transaction() as database:
        project_product_journal(runtime, database, state)
        extra = row(
            2,
            event_type="session.leader.changed",
            actor=LEGACY_COORDINATOR_ID,
            payload={
                "schema": LEADERSHIP_SCHEMA,
                "session_id": SESSION,
                "previous_leader_node_id": "voter-a",
                "leader_node_id": "voter-c",
                "term": 2,
            },
        )
        _insert(database, [extra])
    service = JournalSessionLeadershipService(store, runtime)
    with pytest.raises(ControlPlaneError, match="uncommitted"):
        service.current(SESSION)
    runtime.journal.active = True
    assert service.current(SESSION).leader_node_id == "voter-a"
    with store.transaction() as database:
        database.execute(
            "UPDATE session_events SET actor_node_id=? WHERE revision=2",
            (REPLICATED_COORDINATOR_ID,),
        )
    assert service.current(SESSION).leader_node_id == "voter-c"


def test_projection_does_not_start_or_commit_its_own_transaction(tmp_path: Path):
    state, _rows, _expected = _state_with_capability()
    store, runtime = _projection_store(tmp_path, state)
    database = store._connect()
    try:
        with pytest.raises(ControlPlaneError, match="owned transaction"):
            project_product_journal(runtime, database, state)
    finally:
        database.close()


def _witnessed_rows(rows, *, source_revision=None):
    copied = copy.deepcopy(rows)
    source_revision = len(rows) if source_revision is None else source_revision
    for value in copied[:source_revision]:
        value["request_id"] = _request_key("witnessed-event:" + value["event_id"])
    # Bind the fixture marker to the same public bytes as real witnessed history.
    provenance = {
        "kind": "witnessed", "history_digest": journal_history_digest(copied[:source_revision]),
        "source_revision": source_revision,
    }
    return copied, provenance


def _witnessed_metadata_state(*, complete_later=False, health=False, aliases=False):
    machine, state = initial()
    expected = _announcement()
    state, _ = machine.apply(state, command("CAPABILITY_DECLARE", {
        "session_id": SESSION, "capability_id": expected.capability_id,
        "owner_node_id": expected.node_id, "capability_type": expected.type,
        "occurred_at": STAMP,
    }, identity="legacy-capability-authority"))
    old = expected.to_dict()
    old.pop("schema")
    if aliases:
        old = {
            "capability_id": expected.capability_id,
            "owner_node_id": expected.node_id,
            "capability_type": expected.type,
        }
    rows = [_created(), row(2, event_type="capability.registered", payload=old)]
    rows, provenance = _witnessed_rows(rows)
    if health:
        rows.append(row(3, event_type="capability.health.changed", actor=REPLICATED_COORDINATOR_ID, payload={
            "capability_id": expected.capability_id, "node_id": expected.node_id,
            "status": "ready", "reason": "heartbeat",
        }))
    if complete_later:
        rows.append(row(len(rows) + 1, event_type="capability.status.changed", payload=expected.to_dict()))
    state, _ = machine.apply(state, initialize(rows, provenance=provenance))
    return state, rows, expected


def test_witnessed_prefix_updates_only_two_internal_columns(tmp_path: Path):
    machine, state = initial()
    rows, provenance = _witnessed_rows([_created(), row(2)])
    state, _ = machine.apply(state, initialize(rows, provenance=provenance))
    store, runtime = _projection_store(tmp_path, state)
    legacy = copy.deepcopy(rows)
    for index, value in enumerate(legacy):
        value["request_id"] = _request_key("old-local-command-" + str(index))
        value["content_hash"] = "e" * 64
    with store.transaction() as database:
        _insert(database, legacy)
        database.execute(
            "UPDATE sessions SET revision=? WHERE session_id=?", (len(rows), SESSION)
        )
    public_before = store.replay_events(session_id=SESSION, last_applied_revision=0)
    for _ in range(2):
        with store.transaction() as database:
            project_product_journal(runtime, database, state)
    assert _public_rows(store) == rows
    assert store.replay_events(session_id=SESSION, last_applied_revision=0) == public_before


@pytest.mark.parametrize("field", [
    "event_id", "event_type", "occurred_at", "actor_node_id", "payload_json",
])
def test_witness_marker_never_allows_public_field_rewriting(tmp_path: Path, field: str):
    machine, state = initial()
    rows, provenance = _witnessed_rows([_created()])
    state, _ = machine.apply(state, initialize(rows, provenance=provenance))
    store, runtime = _projection_store(tmp_path, state)
    changed = dict(rows[0])
    changed[field] = '{"different":true}' if field == "payload_json" else changed[field] + "-different"
    with store.transaction() as database:
        _insert(database, [changed])
    before = _public_rows(store)
    with pytest.raises(ControlPlaneError, match="canonical prefix"), store.transaction() as database:
        project_product_journal(runtime, database, state)
    assert _public_rows(store) == before


def test_witness_marker_never_rewrites_internal_columns_beyond_source_prefix(tmp_path: Path):
    machine, state = initial()
    rows, provenance = _witnessed_rows([_created(), row(2)], source_revision=1)
    state, _ = machine.apply(state, initialize(rows, provenance=provenance))
    store, runtime = _projection_store(tmp_path, state)
    changed = copy.deepcopy(rows)
    changed[1]["request_id"] = _request_key("different-local-request")
    with store.transaction() as database:
        _insert(database, changed)
    with pytest.raises(ControlPlaneError, match="canonical prefix"), store.transaction() as database:
        project_product_journal(runtime, database, state)
    assert _public_rows(store) == changed


@pytest.mark.parametrize("aliases", [False, True])
def test_incomplete_witnessed_metadata_stays_unavailable_despite_ready_health(tmp_path: Path, aliases: bool):
    state, rows, expected = _witnessed_metadata_state(health=True, aliases=aliases)
    store, runtime = _projection_store(tmp_path, state)
    with store.transaction() as database:
        project_product_journal(runtime, database, state)
    projected, = store.list_capabilities(session_id=SESSION)
    assert (projected.capability_id, projected.node_id, projected.type) == (
        expected.capability_id, expected.node_id, expected.type
    )
    assert projected.status is CapabilityStatus.UNAVAILABLE
    assert projected.protocol == "metadata-incomplete"
    assert projected.protocol_version == "0"
    assert projected.properties == {"metadata_complete": False}
    assert _public_rows(store) == rows


def test_complete_new_announcement_replaces_incomplete_witnessed_metadata(tmp_path: Path):
    state, rows, expected = _witnessed_metadata_state(complete_later=True, health=True)
    store, runtime = _projection_store(tmp_path, state)
    with store.transaction() as database:
        project_product_journal(runtime, database, state)
    assert store.list_capabilities(session_id=SESSION) == (expected,)
    assert _public_rows(store) == rows


def test_incomplete_legacy_metadata_needs_explicit_witnessed_prefix(tmp_path: Path):
    state, _rows, _expected = _witnessed_metadata_state()
    state["product_journal"]["sessions"][SESSION].pop("provenance")
    store, runtime = _projection_store(tmp_path, state)
    with pytest.raises(ControlPlaneError), store.transaction() as database:
        project_product_journal(runtime, database, state)
    assert _public_rows(store) == []


def test_witnessed_identity_conflict_cannot_replace_committed_owner(tmp_path: Path):
    state, rows, _expected = _witnessed_metadata_state()
    changed = row(2, event_type="capability.registered", actor="voter-b", payload={
        "capability_id": "public-metadata", "node_id": "voter-b", "type": "journal-regression",
    })
    rows[1] = changed
    rows, provenance = _witnessed_rows(rows)
    journal = state["product_journal"]["sessions"][SESSION]
    journal.update(rows=rows, prefix_digest=public_rows_digest(rows), provenance=provenance)
    store, runtime = _projection_store(tmp_path, state)
    with pytest.raises(ControlPlaneError, match="identity conflicts"), store.transaction() as database:
        project_product_journal(runtime, database, state)
    assert _public_rows(store) == []
