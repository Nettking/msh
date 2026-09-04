"""Two legitimate recorders must coexist in one Federation session.

The physical rig ran three hosts on one candidate build. Nitro owned the
capability ID ``recorder-local`` and the separately paired MSH recorder
announced the *same* fixed ID, so the coordinator rejected every MSH
announcement with ``capability-identity-conflict`` -- thousands of them --
even though the pairing was valid and the node was a joined member.

``fcp.capability.v1`` states the invariant these tests hold the product to:
``(session_id, capability_id)`` is unique, while *several nodes may announce
the same capability type*. A recorder therefore stays type ``recorder`` and
gets a capability *instance* identity scoped to its own durable node ID.
"""

from __future__ import annotations

import threading
from datetime import datetime, timezone
from pathlib import Path

import pytest

from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.errors import FederationOperationError
from catalog.federation.models import CapabilityAnnouncement, CapabilityStatus
from catalog.federation.onboarding_models import (
    FederationConnectionState,
    FederationSessionBinding,
)
from catalog.flask_app.services.federation_pairing_service import RemotePairingState
from catalog.flask_app.services.federation_recorder_control_service import (
    FederationRecorderControlService,
)
from catalog.mtconnect_recorder.federation_node import (
    LEGACY_RECORDER_CAPABILITY_ID,
    RECORDER_CAPABILITY_TYPE,
    RecorderFederationNode,
    recorder_capability_id,
)
from catalog.node.identity import IdentityStore, NodeCredentials

NOW = datetime(2026, 9, 4, 9, 0, tzinfo=timezone.utc)
SESSION_ID = "session-recorders"


def _credentials(tmp_path: Path, name: str) -> NodeCredentials:
    return IdentityStore(
        tmp_path / f"identity-{name.casefold()}",
        display_name=name,
    ).create(now=NOW)


def _fixture(tmp_path: Path) -> tuple[SessionCoordinator, NodeCredentials, ...]:
    """One session, three enrolled nodes: the owner plus two joined recorders."""

    coordinator = SessionCoordinator(
        tmp_path / "coordinator.sqlite3",
        clock=lambda: NOW,
    )
    owner = _credentials(tmp_path, "owner")
    nitro = _credentials(tmp_path, "nitro")
    msh = _credentials(tmp_path, "msh")
    for node in (owner, nitro, msh):
        token = coordinator.create_enrollment_token(ttl_seconds=60, max_uses=1)["token"]
        coordinator.enroll_node(node.identity, token=token)
    coordinator.create_session(
        actor_node_id=owner.identity.node_id,
        display_name="Physical rig",
        request_id="create-session",
        session_id=SESSION_ID,
    )
    for index, node in enumerate((nitro, msh)):
        invitation = coordinator.create_invitation(
            session_id=SESSION_ID,
            actor_node_id=owner.identity.node_id,
            ttl_seconds=60,
            max_uses=1,
            request_id=f"invite-{index}",
        )
        coordinator.join_session(
            node_id=node.identity.node_id,
            token=invitation["token"],
            request_id=f"join-{index}",
            expected_session_id=SESSION_ID,
        )
    return coordinator, owner, nitro, msh


def _recorder_announcement(
    node: NodeCredentials,
    *,
    capability_id: str | None = None,
    source_names: tuple[str, ...] = ("machine-1",),
    announced_at: datetime = NOW,
) -> CapabilityAnnouncement:
    node_id = node.identity.node_id
    return CapabilityAnnouncement(
        capability_id=capability_id or recorder_capability_id(node_id),
        node_id=node_id,
        session_id=SESSION_ID,
        type=RECORDER_CAPABILITY_TYPE,
        protocol="mtconnect",
        protocol_version="1",
        status=CapabilityStatus.READY,
        properties={
            "kind": "standalone-recorder",
            "source_count": len(source_names),
            "source_names": list(source_names),
            "dataset_schema": "fcp.mtconnect.observations.v1",
            "logical_storage": True,
        },
        announced_at=announced_at,
    )


def _announce(
    coordinator: SessionCoordinator,
    node: NodeCredentials,
    *,
    request_id: str,
    **kwargs: object,
) -> CapabilityAnnouncement:
    accepted, _ = coordinator.announce_capability(
        _recorder_announcement(node, **kwargs),
        actor_node_id=node.identity.node_id,
        request_id=request_id,
    )
    return accepted


def _recorder_capabilities(
    coordinator: SessionCoordinator,
    actor: NodeCredentials,
) -> tuple[dict[str, object], ...]:
    status = coordinator.status(actor_node_id=actor.identity.node_id)
    return tuple(
        value
        for value in status["capabilities"]
        if value.get("type") == RECORDER_CAPABILITY_TYPE
    )


# --- the physical failure, and that it is genuinely fixed -------------------


def test_fixed_recorder_id_reproduces_the_physical_conflict(tmp_path: Path) -> None:
    """The pre-fix identity is what the coordinator rejected, not the pairing."""

    coordinator, _, nitro, msh = _fixture(tmp_path)
    _announce(
        coordinator,
        nitro,
        request_id="nitro-legacy",
        capability_id=LEGACY_RECORDER_CAPABILITY_ID,
    )

    with pytest.raises(FederationOperationError) as conflict:
        _announce(
            coordinator,
            msh,
            request_id="msh-legacy",
            capability_id=LEGACY_RECORDER_CAPABILITY_ID,
        )

    assert conflict.value.code == "capability-identity-conflict"


def test_two_joined_recorders_are_both_accepted(tmp_path: Path) -> None:
    coordinator, owner, nitro, msh = _fixture(tmp_path)

    accepted_nitro = _announce(coordinator, nitro, request_id="nitro-1")
    accepted_msh = _announce(
        coordinator,
        msh,
        request_id="msh-1",
        source_names=("machine-2",),
    )

    assert accepted_nitro.capability_id != accepted_msh.capability_id
    assert accepted_nitro.type == accepted_msh.type == RECORDER_CAPABILITY_TYPE

    published = _recorder_capabilities(coordinator, owner)
    assert len(published) == 2
    assert {value["node_id"] for value in published} == {
        nitro.identity.node_id,
        msh.identity.node_id,
    }
    assert len({value["capability_id"] for value in published}) == 2
    assert all(value["status"] == CapabilityStatus.READY.value for value in published)


def test_recorder_capability_id_is_derived_from_the_node_identity() -> None:
    assert recorder_capability_id("node-abc") == "recorder-node-abc"
    assert recorder_capability_id("node-abc") != recorder_capability_id("node-def")


def test_reconnect_and_restart_reuse_one_stable_identity(tmp_path: Path) -> None:
    """Replayed announcements must not fork identity or conflict with itself."""

    coordinator, owner, nitro, msh = _fixture(tmp_path)
    first = _announce(coordinator, nitro, request_id="nitro-1")
    _announce(coordinator, msh, request_id="msh-1", source_names=("machine-2",))

    # Every reconnect issues a fresh request ID with identical content, which
    # is exactly what the recorder does on each restart and relay reconnect.
    for attempt in range(5):
        again = _announce(coordinator, nitro, request_id=f"nitro-reconnect-{attempt}")
        assert again.capability_id == first.capability_id

    published = _recorder_capabilities(coordinator, owner)
    assert len(published) == 2, "reconnect must not create duplicate identities"

    events = tuple(
        event.event_type
        for event in coordinator.store.replay_events(
            session_id=SESSION_ID,
            last_applied_revision=0,
        )
        if event.event_type.startswith("capability.")
    )
    assert events == ("capability.registered", "capability.registered")


def test_a_recorder_may_change_its_sources_without_conflict(tmp_path: Path) -> None:
    coordinator, owner, nitro, msh = _fixture(tmp_path)
    _announce(coordinator, nitro, request_id="nitro-1")
    _announce(coordinator, msh, request_id="msh-1", source_names=("machine-2",))

    updated = _announce(
        coordinator,
        nitro,
        request_id="nitro-2",
        source_names=("machine-1", "machine-3"),
    )

    assert updated.capability_id == recorder_capability_id(nitro.identity.node_id)
    published = {
        value["capability_id"]: value
        for value in _recorder_capabilities(coordinator, owner)
    }
    assert len(published) == 2
    assert published[updated.capability_id]["properties"]["source_names"] == [
        "machine-1",
        "machine-3",
    ]


def test_both_recorders_stay_independently_targetable_by_recorder_control(
    tmp_path: Path,
) -> None:
    coordinator, owner, nitro, msh = _fixture(tmp_path)
    for node in (nitro, msh):
        coordinator.connected(
            node_id=node.identity.node_id,
            connection_id=f"connection-{node.identity.node_id}",
        )
    _announce(coordinator, nitro, request_id="nitro-1", source_names=("machine-1",))
    _announce(coordinator, msh, request_id="msh-1", source_names=("machine-2",))

    status = coordinator.status(actor_node_id=owner.identity.node_id)
    recorders = FederationRecorderControlService._recorders(status)

    assert set(recorders) == {nitro.identity.node_id, msh.identity.node_id}
    assert recorders[nitro.identity.node_id]["source_names"] == ["machine-1"]
    assert recorders[msh.identity.node_id]["source_names"] == ["machine-2"]
    assert all(entry["connected"] for entry in recorders.values())


# --- what the recorder itself announces ------------------------------------


class _NodeStateStub:
    def __init__(self, advertised: tuple[CapabilityAnnouncement, ...] = ()) -> None:
        self._advertised = advertised

    def advertised_capabilities(
        self, *, session_id: str | None = None
    ) -> tuple[CapabilityAnnouncement, ...]:
        return tuple(
            item
            for item in self._advertised
            if session_id is None or item.session_id == session_id
        )


class _ClientStub:
    def __init__(self, state: _NodeStateStub) -> None:
        self.state = state


class _RuntimeStub:
    def __init__(self, state: _NodeStateStub | None = None) -> None:
        self._client = None if state is None else _ClientStub(state)

    def _connected_client(self) -> _ClientStub:
        if self._client is None:
            raise FederationOperationError(
                "connection-not-active",
                "no authenticated relay connection",
            )
        return self._client


def _state(node_id: str) -> RemotePairingState:
    return RemotePairingState(
        relay_url="https://relay.example:8443",
        binding=FederationSessionBinding(
            federation_id="federation-1",
            internal_session_id=SESSION_ID,
            device_id=node_id,
            state=FederationConnectionState.CONNECTED,
            revision=1,
            trusted=True,
            created_at=NOW,
        ),
    )


def _node(runtime: _RuntimeStub) -> RecorderFederationNode:
    node = RecorderFederationNode.__new__(RecorderFederationNode)
    node.runtime = runtime
    node.source_names = ("machine-1",)
    node._lock = threading.RLock()
    return node


def test_recorder_announces_a_node_scoped_identity(tmp_path: Path) -> None:
    node = _node(_RuntimeStub(_NodeStateStub()))

    announcement = node._announcement(_state("node-msh"))

    assert announcement.capability_id == "recorder-node-msh"
    assert announcement.type == RECORDER_CAPABILITY_TYPE
    assert announcement.properties["kind"] == "standalone-recorder"


def test_two_recorder_processes_announce_distinct_identities() -> None:
    nitro = _node(_RuntimeStub(_NodeStateStub()))
    msh = _node(_RuntimeStub(_NodeStateStub()))

    assert (
        nitro._announcement(_state("node-nitro")).capability_id
        != msh._announcement(_state("node-msh")).capability_id
    )


def test_identity_is_stable_across_repeated_announcements() -> None:
    node = _node(_RuntimeStub(_NodeStateStub()))
    state = _state("node-nitro")

    assert {node._announcement(state).capability_id for _ in range(5)} == {
        "recorder-node-nitro"
    }


def test_an_existing_single_recorder_keeps_its_accepted_identity() -> None:
    """Upgrading the only recorder must not orphan its durable capability row.

    A capability row is heartbeated for as long as its node is connected, so an
    abandoned ``recorder-local`` row would never age out of Federation views.
    """

    node_id = "node-nitro"
    state = _state(node_id)
    node = _node(
        _RuntimeStub(
            _NodeStateStub(
                (
                    CapabilityAnnouncement(
                        capability_id=LEGACY_RECORDER_CAPABILITY_ID,
                        node_id=node_id,
                        session_id=SESSION_ID,
                        type=RECORDER_CAPABILITY_TYPE,
                        protocol="mtconnect",
                        protocol_version="1",
                        status=CapabilityStatus.READY,
                        properties={"kind": "standalone-recorder"},
                        announced_at=NOW,
                    ),
                )
            )
        )
    )

    assert node._announcement(state).capability_id == LEGACY_RECORDER_CAPABILITY_ID


def test_a_legacy_row_owned_by_another_node_does_not_leak_into_this_identity() -> None:
    """The rejected second recorder must not inherit the first recorder's ID."""

    node = _node(
        _RuntimeStub(
            _NodeStateStub(
                (
                    CapabilityAnnouncement(
                        capability_id=LEGACY_RECORDER_CAPABILITY_ID,
                        node_id="node-nitro",
                        session_id=SESSION_ID,
                        type=RECORDER_CAPABILITY_TYPE,
                        protocol="mtconnect",
                        protocol_version="1",
                        status=CapabilityStatus.READY,
                        properties={"kind": "standalone-recorder"},
                        announced_at=NOW,
                    ),
                )
            )
        )
    )

    assert node._announcement(_state("node-msh")).capability_id == "recorder-node-msh"


def test_identity_falls_back_to_node_scope_without_a_connected_client() -> None:
    node = _node(_RuntimeStub(None))

    assert node._announcement(_state("node-msh")).capability_id == "recorder-node-msh"
