"""One authoritative Phase-D control plane for trusted storage.

Physical acceptance testing left the standalone recorder at
``authority-unavailable`` while the beast relay database already held everything
the recorder needs:

* a READY ``storage`` capability from the session creator carrying
  ``storage_authority.group_id='fcp-local-storage'`` and ``role='primary'``,
* a ``fcp-local-storage`` group whose primary provider is that creator,
* the leader grant for that group.

The creator's announced ``logical-storage-authority`` capability was
nevertheless ``unavailable`` with ``group_ids: []``.

``ProviderAuthorityRelayServer`` reconciles trusted GREEN storage into
``PhaseDControlPlane(coordinator.store.database)``. The supervised authority
composed ``PhaseDControlPlane`` from a *separate* configured path, so it read an
empty control plane and truthfully advertised no groups. These tests pin the two
sides to one database and drive the whole physical path across a restart.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from flask import Flask

from catalog.capabilities.storage_authority_enrollment import (
    STORAGE_AUTHORITY_SCHEMA,
    TrustedGreenStorageAuthority,
)
from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.models import CapabilityAnnouncement, CapabilityStatus
from catalog.federation.phase_d_control import PhaseDControlPlane
from catalog.federation.storage_protocol import (
    STORAGE_PROTOCOL,
    STORAGE_PROTOCOL_VERSION,
)
from catalog.flask_app.services.federation_storage_authority_install import (
    install_federation_storage_authority,
)
from catalog.mtconnect_recorder.federation_node import select_storage_authority
from catalog.node.identity import IdentityStore, NodeCredentials
from catalog.node.storage_failover import _recorder_storage_capability

NOW = datetime(2026, 8, 20, 9, 0, tzinfo=timezone.utc)
SESSION = "session-beast"
GROUP = "fcp-local-storage"
PROVIDER = "provider-creator"
RELAY = "ws://100.64.0.1:8765"


@dataclass(frozen=True)
class _Binding:
    internal_session_id: str
    device_id: str


@dataclass(frozen=True)
class _Context:
    binding: _Binding
    coordinator: SessionCoordinator


class _Onboarding:
    def __init__(self, context: _Context) -> None:
        self._context = context

    def authorized_context(self) -> _Context:
        return self._context


class _Client:
    """Only what the capability builder reads off the relay client."""

    def __init__(self, node_id: str) -> None:
        self.node_id = node_id


def _creator(tmp_path: Path) -> NodeCredentials:
    return IdentityStore(
        tmp_path / "creator-identity", display_name="Creator"
    ).load_or_create(now=NOW)


def _green_storage_announcement(node_id: str) -> CapabilityAnnouncement:
    """The trusted GREEN storage candidate the beast relay database holds."""

    return CapabilityAnnouncement(
        capability_id=PROVIDER,
        node_id=node_id,
        session_id=SESSION,
        type="storage",
        protocol=STORAGE_PROTOCOL,
        protocol_version=STORAGE_PROTOCOL_VERSION,
        status=CapabilityStatus.READY,
        properties={
            "kind": "capability-first-candidate",
            "candidate_id": PROVIDER,
            "storage_authority": {
                "schema": STORAGE_AUTHORITY_SCHEMA,
                "eligible": True,
                "benchmark_state": "green",
                "provider_id": PROVIDER,
                "group_id": GROUP,
                "role": "primary",
                "inspection_revision": 3,
                "decision_revision": 4,
                "benchmark_run_ids": ["benchmark-storage-green-1"],
                "desired_state": "enabled",
                "policy_state": "allowed",
            },
        },
        announced_at=NOW,
    )


def _federation(tmp_path: Path) -> tuple[SessionCoordinator, NodeCredentials]:
    coordinator = SessionCoordinator(
        tmp_path / "relay" / "control.sqlite3", clock=lambda: NOW
    )
    creator = _creator(tmp_path)
    token = coordinator.create_enrollment_token(ttl_seconds=60, max_uses=1)["token"]
    coordinator.enroll_node(creator.identity, token=token)
    coordinator.create_session(
        actor_node_id=creator.identity.node_id,
        display_name="Beast Federation",
        request_id="create-session",
        session_id=SESSION,
    )
    coordinator.connected(node_id=creator.identity.node_id, connection_id="creator-live")
    return coordinator, creator


def _auto_enroll(coordinator: SessionCoordinator, creator: NodeCredentials) -> None:
    """Exactly what ProviderAuthorityRelayServer does on a GREEN announcement."""

    announcement = _green_storage_announcement(creator.identity.node_id)
    coordinator.announce_capability(
        announcement,
        actor_node_id=announcement.node_id,
        request_id="announce-storage",
    )
    decision = TrustedGreenStorageAuthority(
        coordinator,
        PhaseDControlPlane(coordinator.store.database),
        clock=lambda: NOW,
    ).reconcile(announcement)
    assert decision.outcome == "active"
    assert decision.reason_code == "trusted-green-storage-authority"


def _monitor(coordinator: SessionCoordinator, creator: NodeCredentials, tmp_path: Path):
    app = Flask(__name__)
    app.config.update(
        FEDERATION_STORAGE_AUTHORITY_ENABLED=True,
        FEDERATION_STORAGE_AUTHORITY_RELAY_URL=RELAY,
        FEDERATION_STORAGE_AUTHORITY_CONTROL_DATABASE=coordinator.store.database,
        FEDERATION_STORAGE_AUTHORITY_STATE_DIR=str(tmp_path / "device"),
    )
    context = _Context(
        _Binding(SESSION, creator.identity.node_id),
        coordinator,
    )
    return install_federation_storage_authority(
        app, onboarding_service=_Onboarding(context)
    )


def _announced(monitor, creator: NodeCredentials) -> CapabilityAnnouncement:
    """Build the capability the supervised authority would announce."""

    with monitor.app.app_context():
        settings = monitor.build_settings()
    return _recorder_storage_capability(
        PhaseDControlPlane(Path(settings.storage_control_database)),
        _Client(creator.identity.node_id),
        settings.session_id,
    )


def _recorder_status(
    capability: CapabilityAnnouncement, creator: NodeCredentials
) -> dict[str, object]:
    """The coordinator status a standalone recorder selects storage from."""

    return {
        "sessions": [
            {
                "session_id": SESSION,
                "created_by_node_id": creator.identity.node_id,
            }
        ],
        "capabilities": [
            {
                "session_id": capability.session_id,
                "node_id": capability.node_id,
                "capability_id": capability.capability_id,
                "type": capability.type,
                "protocol": capability.protocol,
                "status": capability.status.value,
                "properties": capability.properties,
            }
        ],
    }


def test_the_authority_reads_the_coordinator_control_plane(tmp_path: Path) -> None:
    """The composed settings must name the coordinator's own database."""

    coordinator, creator = _federation(tmp_path)
    monitor = _monitor(coordinator, creator, tmp_path)

    with monitor.app.app_context():
        settings = monitor.build_settings()

    assert settings.storage_control_database == coordinator.store.database
    assert settings.relay_control_database == coordinator.store.database
    # Every in-process reader of the Phase-D control plane resolves one file.
    assert (
        monitor.app.config["FEDERATION_STORAGE_AUTHORITY_STORAGE_DATABASE"]
        == coordinator.store.database
    )


def test_authority_local_sidecars_stay_out_of_the_coordinator_database(
    tmp_path: Path,
) -> None:
    """Only the control plane is shared; the authority keeps its own journals."""

    coordinator, creator = _federation(tmp_path)
    monitor = _monitor(coordinator, creator, tmp_path)

    with monitor.app.app_context():
        settings = monitor.build_settings()

    assert settings.publication_database != coordinator.store.database
    assert settings.failover_database != coordinator.store.database
    assert settings.acknowledgements_database is not None
    assert settings.acknowledgements_database != coordinator.store.database
    assert Path(settings.acknowledgements_database).name == (
        "recorder_ingest_acknowledgements.sqlite3"
    )


def test_green_auto_enrollment_reaches_the_announced_authority(
    tmp_path: Path,
) -> None:
    """The whole physical path: GREEN storage -> READY authority -> recorder."""

    coordinator, creator = _federation(tmp_path)
    _auto_enroll(coordinator, creator)

    # The relay's control plane holds the group, its primary, and the grant.
    snapshot = PhaseDControlPlane(coordinator.store.database).snapshot(SESSION)
    assert GROUP in snapshot.groups
    assert snapshot.groups[GROUP].primary_provider_id == PROVIDER
    grant = snapshot.leader_grants.get(GROUP)
    assert grant is not None
    assert grant["provider_id"] == PROVIDER
    assert grant["node_id"] == creator.identity.node_id

    monitor = _monitor(coordinator, creator, tmp_path)
    capability = _announced(monitor, creator)

    assert capability.status is CapabilityStatus.READY
    assert capability.properties["group_ids"] == [GROUP]

    selection = select_storage_authority(
        _recorder_status(capability, creator),
        session_id=SESSION,
        requested_group=None,
    )

    assert selection.state == "ready"
    assert selection.authority_node_id == creator.identity.node_id
    assert selection.group_id == GROUP


def test_the_requested_storage_group_is_selectable_by_name(tmp_path: Path) -> None:
    coordinator, creator = _federation(tmp_path)
    _auto_enroll(coordinator, creator)
    monitor = _monitor(coordinator, creator, tmp_path)

    selection = select_storage_authority(
        _recorder_status(_announced(monitor, creator), creator),
        session_id=SESSION,
        requested_group=GROUP,
    )

    assert selection.state == "ready"
    assert selection.group_id == GROUP


def test_the_group_survives_a_process_restart(tmp_path: Path) -> None:
    """Restart durability: the same file, reopened by fresh objects."""

    coordinator, creator = _federation(tmp_path)
    _auto_enroll(coordinator, creator)
    database = coordinator.store.database

    # Drop every in-memory handle, as a container restart would.
    del coordinator

    restarted = SessionCoordinator(database, clock=lambda: NOW)
    assert restarted.store.database == database

    monitor = _monitor(restarted, creator, tmp_path)
    capability = _announced(monitor, creator)

    assert capability.status is CapabilityStatus.READY
    assert capability.properties["group_ids"] == [GROUP]

    selection = select_storage_authority(
        _recorder_status(capability, creator),
        session_id=SESSION,
        requested_group=None,
    )

    assert selection.state == "ready"
    assert selection.group_id == GROUP


def test_a_separate_control_plane_advertises_nothing(tmp_path: Path) -> None:
    """Pin the defect itself: a second database is always empty.

    This is what the beast relay database showed — a READY primary provider and
    a leader grant on one side, ``group_ids: []`` and ``unavailable`` on the
    other. It must stay a demonstration of why the split is wrong, not a
    supported configuration.
    """

    coordinator, creator = _federation(tmp_path)
    _auto_enroll(coordinator, creator)

    split = PhaseDControlPlane(tmp_path / "federation" / "storage" / "control.sqlite3")
    capability = _recorder_storage_capability(
        split, _Client(creator.identity.node_id), SESSION
    )

    assert capability.status is CapabilityStatus.UNAVAILABLE
    assert capability.properties["group_ids"] == []
    assert (
        select_storage_authority(
            _recorder_status(capability, creator),
            session_id=SESSION,
            requested_group=None,
        ).state
        == "authority-unavailable"
    )

    # The supervised composition never selects that database.
    monitor = _monitor(coordinator, creator, tmp_path)
    with monitor.app.app_context():
        assert monitor.build_settings().storage_control_database != str(split.database)
