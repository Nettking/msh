"""Closed C03 wire types and witnessed legacy authority stay narrowly scoped.

These are serialization/projection tests, not quorum validation. The companion
Flask pairing tests exercise this command through real authenticated relays.
"""

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
)
from catalog.federation.control_plane_journal_projection import (
    JournalSessionLeadershipService,
    project_product_journal,
)
from catalog.federation.errors import FederationOperationError
from catalog.federation.models import Session
from catalog.federation.session_leadership import LEADERSHIP_SCHEMA, SessionLeadership
from catalog.federation.tests.test_control_plane_journal_projection import (
    _created,
    _projection_store,
    _public_rows,
)
from catalog.federation.tests.test_control_plane_product_journal import (
    SESSION,
    STAMP,
    command,
    initial,
    initialize,
    row,
)
from catalog.flask_app.services.federation_leader_authority import (
    resolve_federation_leader,
)
from catalog.flask_app.services.resilient_pairing_runtime import (
    ResilientPairingRelayRuntime,
)


def _wire_authority():
    session = Session(
        session_id=SESSION,
        display_name="Committed session",
        state="active",
        revision=4,
        created_at=datetime(2026, 9, 8, 10, tzinfo=timezone.utc),
        created_by_node_id="voter-a",
        coordinator_id=REPLICATED_COORDINATOR_ID,
    )
    leadership = SessionLeadership(SESSION, "voter-a", "voter-c", 3, True)
    return {"session": session.to_dict(), "leadership": leadership.to_dict()}


def test_closed_authority_response_retains_typed_session_creation():
    value = _wire_authority()
    session, leadership = ResilientPairingRelayRuntime._authority_result(
        value, session_id=SESSION
    )
    assert isinstance(session.created_at, datetime)
    assert session.to_dict() == value["session"]
    assert leadership.to_dict() == value["leadership"]


@pytest.mark.parametrize(
    "object_name,field,value",
    [
        ("session", "session_id", "different-session"),
        ("session", "coordinator_id", LEGACY_COORDINATOR_ID),
        ("session", "created_at", None),
        ("session", "created_by_node_id", "different-creator"),
        ("session", "unknown", True),
        ("leadership", "session_id", "different-session"),
        ("leadership", "creator_node_id", "different-creator"),
        ("leadership", "schema", "different-schema"),
        ("leadership", "term", True),
        ("leadership", "term", 0),
        ("leadership", "term", "3"),
        ("leadership", "leader_node_id", " "),
        ("leadership", "leader_connected", 1),
        ("leadership", "unknown", True),
    ],
)
def test_authority_response_rejects_mixed_or_open_metadata(object_name, field, value):
    response = _wire_authority()
    response[object_name][field] = value
    with pytest.raises(FederationOperationError):
        ResilientPairingRelayRuntime._authority_result(response, session_id=SESSION)


@pytest.mark.parametrize("missing", ["session", "leadership"])
def test_authority_response_requires_both_complete_objects(missing):
    response = _wire_authority()
    response.pop(missing)
    with pytest.raises(FederationOperationError):
        ResilientPairingRelayRuntime._authority_result(response, session_id=SESSION)


def test_witnessed_legacy_then_replicated_term_uses_scoped_authority_hook(tmp_path: Path):
    machine, state = initial()
    rows = [_created()]
    for term, previous, leader, actor in (
        (2, "voter-a", "voter-b", LEGACY_COORDINATOR_ID),
        (3, "voter-b", "voter-c", REPLICATED_COORDINATOR_ID),
    ):
        state, _ = machine.apply(state, command("LEADER_TRANSITION", {
            "session_id": SESSION,
            "previous_leader_node_id": previous,
            "leader_node_id": leader,
            "term": term,
            "occurred_at": STAMP,
        }, identity=f"authority-transition-{term}"))
        rows.append(row(term, event_type="session.leader.changed", actor=actor, payload={
            "schema": LEADERSHIP_SCHEMA,
            "session_id": SESSION,
            "previous_leader_node_id": previous,
            "leader_node_id": leader,
            "term": term,
            "reason": "witnessed-authority",
        }))
    state, _ = machine.apply(state, initialize(rows))
    store, runtime = _projection_store(tmp_path, state)
    with store.transaction() as database:
        project_product_journal(runtime, database, state)
    leadership = JournalSessionLeadershipService(store, runtime)
    calls = []

    def authenticated_read(*, session_id, actor_node_id):
        calls.append((session_id, actor_node_id))
        with store.read_transaction() as database:
            store._require_membership(
                database, session_id=session_id, node_id=actor_node_id
            )
            wire = {
                "session": store._session_tx(database, session_id).to_dict(),
                "leadership": leadership._snapshot_tx(database, session_id).to_dict(),
            }
        return ResilientPairingRelayRuntime._authority_result(wire, session_id=session_id)

    def replay_page(**kwargs):
        return (
            store.replay_events(
                session_id=kwargs["session_id"],
                last_applied_revision=kwargs["last_applied_revision"],
            ),
            store.get_session(SESSION).revision,
        )

    generic = SimpleNamespace(
        store=store, coordinator_id=REPLICATED_COORDINATOR_ID, replay_page=replay_page
    )
    context = SimpleNamespace(
        binding=SimpleNamespace(internal_session_id=SESSION),
        credentials=SimpleNamespace(identity=SimpleNamespace(node_id="voter-c")),
        coordinator=generic,
    )
    # The default reader still rejects a term gap. It has no evidence that this
    # other coordinator ID was witnessed during migration.
    with pytest.raises(FederationOperationError, match="contiguous monotonic"):
        resolve_federation_leader(context)
    configured = copy.copy(generic)
    configured.authenticated_session_authority = authenticated_read
    context.coordinator = configured
    authority = resolve_federation_leader(context)
    assert (authority.creator_node_id, authority.leader_node_id, authority.term) == (
        "voter-a", "voter-c", 3
    )
    assert calls == [(SESSION, "voter-c")]
    assert _public_rows(store) == rows

    session, current = authenticated_read(session_id=SESSION, actor_node_id="voter-c")
    configured.authenticated_session_authority = lambda **_: (
        session, replace(current, creator_node_id="different-creator")
    )
    with pytest.raises(FederationOperationError, match="metadata disagree"):
        resolve_federation_leader(context)
