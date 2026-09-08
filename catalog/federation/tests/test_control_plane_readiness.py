"""A bootstrap seal belongs to the leader that completed quorum bootstrap."""

from copy import deepcopy

import pytest

from catalog.federation.control_plane_readiness import (
    BOOTSTRAP_SEAL_CAPABILITY_ID,
    BOOTSTRAP_SEAL_CAPABILITY_TYPE,
    authority_ready,
)
from catalog.federation.control_plane_replication import (
    AuthorityStateMachine,
    ControlPlaneError,
)
from catalog.federation.tests.test_control_plane_replication import (
    CONFIGURATION,
    _command,
    _genesis,
)


def _seal(owner="voter-a", capability_type=BOOTSTRAP_SEAL_CAPABILITY_TYPE):
    return _command(
        "seal",
        "CAPABILITY_DECLARE",
        {
            "session_id": "session-stable",
            "capability_id": BOOTSTRAP_SEAL_CAPABILITY_ID,
            "capability_type": capability_type,
            "owner_node_id": owner,
            "occurred_at": "2026-09-08T00:00:00Z",
        },
        issued_by=owner,
    )


def _transfer(previous, successor, term):
    return _command(
        f"transfer-{term}",
        "LEADER_TRANSITION",
        {
            "session_id": "session-stable",
            "previous_leader_node_id": previous,
            "leader_node_id": successor,
            "term": term,
            "occurred_at": "2026-09-08T00:00:00Z",
        },
        issued_by=successor,
    )


def test_successor_seal_survives_subsequent_leadership_change_and_snapshot():
    machine = AuthorityStateMachine(CONFIGURATION)
    state, _ = machine.apply(machine.initial_state(), _genesis())
    assert not authority_ready(state)
    state, _ = machine.apply(state, _transfer("voter-a", "voter-b", 2))
    state, _ = machine.apply(state, _seal("voter-b"))
    assert authority_ready(state)
    state, _ = machine.apply(state, _transfer("voter-b", "voter-c", 3))
    state, _ = machine.apply(
        state,
        _command(
            "revoke-b",
            "NODE_REVOKE",
            {
                "node_id": "voter-b",
                "reason": "retired",
                "occurred_at": "2026-09-08T00:00:00Z",
            },
            issued_by="voter-c",
        ),
    )
    assert authority_ready(deepcopy(state))
    assert state["sessions"]["session-stable"]["creator_node_id"] == "voter-a"


@pytest.mark.parametrize(
    "owner,capability_type",
    [
        ("voter-b", BOOTSTRAP_SEAL_CAPABILITY_TYPE),
        ("voter-a", "ordinary-capability"),
    ],
)
def test_reserved_seal_rejects_nonleader_and_wrong_type(owner, capability_type):
    machine = AuthorityStateMachine(CONFIGURATION)
    state, _ = machine.apply(machine.initial_state(), _genesis())
    with pytest.raises(ControlPlaneError, match="bootstrap seal"):
        machine.apply(state, _seal(owner, capability_type))
    assert not authority_ready(state)


def test_seal_without_matching_committed_leadership_provenance_is_not_ready():
    machine = AuthorityStateMachine(CONFIGURATION)
    state, _ = machine.apply(machine.initial_state(), _genesis())
    state, _ = machine.apply(state, _seal())
    assert authority_ready(state)
    state["session_events"]["session-stable"][-1]["actor_node_id"] = "voter-b"
    assert not authority_ready(state)
    state["session_events"]["session-stable"] = []
    assert not authority_ready(state)
