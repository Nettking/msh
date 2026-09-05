"""Recorder capability identity must converge from authoritative state.

The physical rig ran three hosts on one candidate build. Nitro owned the
capability ID ``recorder-local`` and the separately paired MSH recorder
announced the same fixed ID, so the coordinator rejected every MSH
announcement with ``capability-identity-conflict`` -- thousands of them --
while the pairing itself was valid and connected.

``fcp.capability.v1`` states the invariant these tests hold the product to:
``(session_id, capability_id)`` is unique, while *several nodes may announce
the same capability type*. A recorder therefore stays type ``recorder`` and
takes a capability *instance* identity scoped to its own durable node ID.

Selection is decided from the coordinator, never from the local capability
cache: that cache can be missing (a response lost between coordinator
acceptance and the local save), stale (restored from a backup), or flatly
contradicted by the coordinator. These tests drive the real relay server, the
real relay client, the real pairing runtime and the real coordinator, so the
durable capability rows asserted here are the rows a physical rig would hold.
"""

from __future__ import annotations

import asyncio
import shutil
import sqlite3
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.models import CapabilityAnnouncement, CapabilityStatus
from catalog.federation.onboarding_models import (
    FederationConnectionState,
    FederationSessionBinding,
)
from catalog.flask_app.services.federated_data_runtime import (
    FederatedDataPairingRelayRuntime,
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
from catalog.node.client import RelayNodeClient, RelayRemoteError
from catalog.node.identity import IdentityStore
from catalog.relay.service import RelayServer

NOW = datetime(2026, 9, 4, 9, 0, tzinfo=timezone.utc)
TIMEOUT = 5.0
SESSION_ID = "session-recorders"


# --- real rig ---------------------------------------------------------------


def _copy_node_state(source: Path, destination: Path) -> None:
    """Copy a node state database as a whole, WAL sidecar included.

    ``node_state.sqlite3`` runs in WAL mode, so its committed state is split
    between the database file and its ``-wal`` sidecar. Copying the database
    file alone leaves the destination's own sidecar in place, and the reopened
    database is then a mixture of the two: the event log can hold revisions the
    session watermark does not. The node's replay check refuses that -- rightly
    -- with ``invalid-replay-completion``, so a partial copy tests the refusal
    rather than the restore this scenario is about. Move the whole set, and drop
    the destination's stale shared-memory index so it is rebuilt from the copied
    WAL rather than from the replaced one.
    """

    for suffix in ("", "-wal"):
        origin = Path(str(source) + suffix)
        target = Path(str(destination) + suffix)
        target.unlink(missing_ok=True)
        if origin.exists():
            shutil.copy(origin, target)
    Path(str(destination) + "-shm").unlink(missing_ok=True)



@dataclass
class Recorder:
    """One physical recorder host: its own state directory and identity."""

    name: str
    node_id: str
    state_directory: Path
    runtime: FederatedDataPairingRelayRuntime
    node: RecorderFederationNode
    state: RemotePairingState

    async def connect(self) -> None:
        await self.runtime._ensure_connected(self.state)

    async def announce(self) -> None:
        await self.node._announce_now(self.state)

    async def reconnect(self) -> None:
        await self.runtime._disconnect_current()
        await self.connect()

    async def close(self) -> None:
        await self.runtime._disconnect_current()

    @property
    def scoped_id(self) -> str:
        return recorder_capability_id(self.node_id)

    def local_capability_ids(self) -> tuple[str, ...]:
        client = self.runtime._connected_client()
        return tuple(
            item.capability_id
            for item in client.state.advertised_capabilities(session_id=SESSION_ID)
        )


class Rig:
    """A running relay plus the coordinator behind it."""

    def __init__(self, tmp_path: Path) -> None:
        self.tmp_path = tmp_path
        self.control = tmp_path / "relay" / "control.sqlite3"
        self.coordinator: SessionCoordinator | None = None
        self.relay: RelayServer | None = None
        self.owner_node_id = ""
        self.recorders: list[Recorder] = []

    async def start(self) -> None:
        self.control.parent.mkdir(parents=True, exist_ok=True)
        self.coordinator = SessionCoordinator(self.control, clock=lambda: NOW)
        self.relay = RelayServer(
            self.coordinator,
            host="127.0.0.1",
            port=0,
            auth_timeout_seconds=TIMEOUT,
            send_timeout_seconds=TIMEOUT,
            heartbeat_timeout_seconds=300,
            sweep_interval_seconds=300,
        )
        await self.relay.start()

    async def restart_coordinator(self) -> None:
        """Restart the coordinator process over the same durable database."""

        for recorder in self.recorders:
            await recorder.close()
        assert self.relay is not None
        await self.relay.stop()
        await self.start()
        for recorder in self.recorders:
            recorder.state = RemotePairingState(
                relay_url=self.url,
                binding=recorder.state.binding,
            )
            recorder.runtime = _runtime(recorder.state_directory, recorder.name)
            recorder.node.runtime = recorder.runtime

    async def stop(self) -> None:
        for recorder in self.recorders:
            await recorder.close()
        if self.relay is not None:
            await self.relay.stop()

    @property
    def url(self) -> str:
        assert self.relay is not None
        return self.relay.url

    def enroll(self, state_directory: Path, display_name: str) -> str:
        assert self.coordinator is not None
        identity = IdentityStore(
            state_directory, display_name=display_name
        ).load_or_create(now=NOW).identity
        token = self.coordinator.create_enrollment_token(
            ttl_seconds=600, max_uses=1
        )["token"]
        self.coordinator.enroll_node(identity, token=str(token))
        return identity.node_id

    def create_session(self, owner_directory: Path) -> None:
        assert self.coordinator is not None
        self.owner_node_id = self.enroll(owner_directory, "Owner")
        self.coordinator.create_session(
            actor_node_id=self.owner_node_id,
            display_name="Physical rig",
            request_id="create-session",
            session_id=SESSION_ID,
        )

    def join(self, node_id: str, tag: str) -> None:
        assert self.coordinator is not None
        invitation = self.coordinator.create_invitation(
            session_id=SESSION_ID,
            actor_node_id=self.owner_node_id,
            ttl_seconds=600,
            max_uses=1,
            request_id=f"invite-{tag}",
        )
        self.coordinator.join_session(
            node_id=node_id,
            token=str(invitation["token"]),
            request_id=f"join-{tag}",
            expected_session_id=SESSION_ID,
        )

    def rows(self) -> dict[str, dict[str, Any]]:
        """The durable recorder capability rows, as the coordinator holds them."""

        assert self.coordinator is not None
        status = self.coordinator.status(actor_node_id=self.owner_node_id)
        return {
            str(value["capability_id"]): value
            for value in status["capabilities"]
            if value.get("session_id") == SESSION_ID
            and value.get("type") == RECORDER_CAPABILITY_TYPE
        }

    def ready_rows(self) -> dict[str, dict[str, Any]]:
        return {
            capability_id: row
            for capability_id, row in self.rows().items()
            if row.get("status") == CapabilityStatus.READY.value
        }

    def seed_capability(
        self,
        *,
        node_id: str,
        capability_id: str,
        source_names: tuple[str, ...] = ("machine-seed",),
        request_id: str | None = None,
    ) -> None:
        """Accept a capability at the coordinator without any local save.

        This is exactly a response lost between coordinator acceptance and the
        announcing node's local persistence.
        """

        assert self.coordinator is not None
        self.coordinator.announce_capability(
            CapabilityAnnouncement(
                capability_id=capability_id,
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
                announced_at=NOW,
            ),
            actor_node_id=node_id,
            request_id=request_id or f"seed-{capability_id}-{node_id}",
        )


def _runtime(state_directory: Path, name: str) -> FederatedDataPairingRelayRuntime:
    return FederatedDataPairingRelayRuntime(
        state_directory=state_directory,
        display_name=name,
        timeout_seconds=TIMEOUT,
    )


def _recorder(
    rig: Rig,
    name: str,
    *,
    sources: tuple[str, ...] = ("machine-1",),
) -> Recorder:
    state_directory = rig.tmp_path / f"recorder-{name.casefold()}"
    node_id = rig.enroll(state_directory, name)
    rig.join(node_id, name.casefold())
    runtime = _runtime(state_directory, name)
    node = RecorderFederationNode.__new__(RecorderFederationNode)
    node.runtime = runtime
    node.source_names = sources
    node._lock = threading.RLock()
    recorder = Recorder(
        name=name,
        node_id=node_id,
        state_directory=state_directory,
        runtime=runtime,
        node=node,
        state=RemotePairingState(
            relay_url=rig.url,
            binding=FederationSessionBinding(
                federation_id="federation-1",
                internal_session_id=SESSION_ID,
                device_id=node_id,
                state=FederationConnectionState.CONNECTED,
                revision=1,
                trusted=True,
                created_at=NOW,
            ),
        ),
    )
    rig.recorders.append(recorder)
    return recorder


def _run(scenario) -> None:
    asyncio.run(asyncio.wait_for(scenario(), timeout=60.0))


# --- the physical failure ---------------------------------------------------


def test_a_fixed_recorder_id_is_what_the_coordinator_rejected(tmp_path: Path) -> None:
    """The pre-fix identity is what was rejected, not the pairing."""

    async def scenario() -> None:
        rig = Rig(tmp_path)
        await rig.start()
        try:
            rig.create_session(tmp_path / "owner")
            nitro = _recorder(rig, "Nitro")
            msh = _recorder(rig, "MSH")
            rig.seed_capability(
                node_id=nitro.node_id,
                capability_id=LEGACY_RECORDER_CAPABILITY_ID,
            )

            with pytest.raises(Exception) as conflict:
                rig.seed_capability(
                    node_id=msh.node_id,
                    capability_id=LEGACY_RECORDER_CAPABILITY_ID,
                )

            assert getattr(conflict.value, "code", "") == "capability-identity-conflict"
        finally:
            await rig.stop()

    _run(scenario)


def test_clean_federation_accepts_two_recorders(tmp_path: Path) -> None:
    async def scenario() -> None:
        rig = Rig(tmp_path)
        await rig.start()
        try:
            rig.create_session(tmp_path / "owner")
            nitro = _recorder(rig, "Nitro", sources=("machine-1",))
            msh = _recorder(rig, "MSH", sources=("machine-2",))
            for recorder in (nitro, msh):
                await recorder.connect()
                await recorder.announce()

            rows = rig.rows()
            assert set(rows) == {nitro.scoped_id, msh.scoped_id}
            assert rows[nitro.scoped_id]["node_id"] == nitro.node_id
            assert rows[msh.scoped_id]["node_id"] == msh.node_id
            assert all(
                row["status"] == CapabilityStatus.READY.value
                for row in rows.values()
            )
        finally:
            await rig.stop()

    _run(scenario)


def test_existing_single_recorder_keeps_its_accepted_legacy_identity(
    tmp_path: Path,
) -> None:
    """The only recorder in a session must not orphan its durable row."""

    async def scenario() -> None:
        rig = Rig(tmp_path)
        await rig.start()
        try:
            rig.create_session(tmp_path / "owner")
            nitro = _recorder(rig, "Nitro")
            rig.seed_capability(
                node_id=nitro.node_id,
                capability_id=LEGACY_RECORDER_CAPABILITY_ID,
            )

            await nitro.connect()
            await nitro.announce()

            rows = rig.rows()
            assert set(rows) == {LEGACY_RECORDER_CAPABILITY_ID}
            assert rows[LEGACY_RECORDER_CAPABILITY_ID]["node_id"] == nitro.node_id
        finally:
            await rig.stop()

    _run(scenario)


# --- the three adversarial cases from independent review --------------------


def test_case_1_coordinator_legacy_row_with_missing_local_row(
    tmp_path: Path,
) -> None:
    """Acceptance recorded, local save lost: must not fork into a second row."""

    async def scenario() -> None:
        rig = Rig(tmp_path)
        await rig.start()
        try:
            rig.create_session(tmp_path / "owner")
            nitro = _recorder(rig, "Nitro")
            # Coordinator accepted it; the response never reached the node, so
            # nothing was saved locally.
            rig.seed_capability(
                node_id=nitro.node_id,
                capability_id=LEGACY_RECORDER_CAPABILITY_ID,
            )
            await nitro.connect()
            assert nitro.local_capability_ids() == ()

            await nitro.announce()

            assert set(rig.rows()) == {LEGACY_RECORDER_CAPABILITY_ID}
            assert len(rig.ready_rows()) == 1
        finally:
            await rig.stop()

    _run(scenario)


def test_case_2_stale_local_legacy_after_migration(tmp_path: Path) -> None:
    """A restored backup must not drag a converged node back to the legacy ID."""

    async def scenario() -> None:
        rig = Rig(tmp_path)
        await rig.start()
        try:
            rig.create_session(tmp_path / "owner")
            nitro = _recorder(rig, "Nitro")
            await nitro.connect()
            await nitro.announce()
            assert set(rig.rows()) == {nitro.scoped_id}

            # A backup taken before migration still claims the legacy ID.
            client = nitro.runtime._connected_client()
            client.state.save_capability(
                CapabilityAnnouncement(
                    capability_id=LEGACY_RECORDER_CAPABILITY_ID,
                    node_id=nitro.node_id,
                    session_id=SESSION_ID,
                    type=RECORDER_CAPABILITY_TYPE,
                    protocol="mtconnect",
                    protocol_version="1",
                    status=CapabilityStatus.READY,
                    properties={"kind": "standalone-recorder"},
                    announced_at=NOW,
                ),
                now=NOW,
            )

            await nitro.announce()

            rows = rig.rows()
            assert rows[nitro.scoped_id]["status"] == CapabilityStatus.READY.value
            assert set(rig.ready_rows()) == {nitro.scoped_id}
            # The retired row must also leave the local cache, or the client
            # would replay and resurrect it on the next connect.
            assert LEGACY_RECORDER_CAPABILITY_ID not in nitro.local_capability_ids()
        finally:
            await rig.stop()

    _run(scenario)


def test_case_3_local_ownership_disagreement_still_converges(
    tmp_path: Path,
) -> None:
    """A stale local claim on another node's row must not strand the recorder."""

    async def scenario() -> None:
        rig = Rig(tmp_path)
        await rig.start()
        try:
            rig.create_session(tmp_path / "owner")
            nitro = _recorder(rig, "Nitro")
            msh = _recorder(rig, "MSH", sources=("machine-2",))

            # MSH legitimately owns the legacy ID at the coordinator.
            rig.seed_capability(
                node_id=msh.node_id,
                capability_id=LEGACY_RECORDER_CAPABILITY_ID,
                source_names=("machine-2",),
            )
            # Nitro is restored from a backup that still claims that same ID.
            await nitro.connect()
            client = nitro.runtime._connected_client()
            client.state.save_capability(
                CapabilityAnnouncement(
                    capability_id=LEGACY_RECORDER_CAPABILITY_ID,
                    node_id=nitro.node_id,
                    session_id=SESSION_ID,
                    type=RECORDER_CAPABILITY_TYPE,
                    protocol="mtconnect",
                    protocol_version="1",
                    status=CapabilityStatus.READY,
                    properties={"kind": "standalone-recorder"},
                    announced_at=NOW,
                ),
                now=NOW,
            )

            # The reconnect replays that stale row. It can never be accepted,
            # and it must not strand the node offline.
            await nitro.reconnect()
            await nitro.announce()

            rows = rig.rows()
            assert rows[LEGACY_RECORDER_CAPABILITY_ID]["node_id"] == msh.node_id
            assert rows[nitro.scoped_id]["node_id"] == nitro.node_id
            assert LEGACY_RECORDER_CAPABILITY_ID not in nitro.local_capability_ids()
        finally:
            await rig.stop()

    _run(scenario)


def test_both_identities_for_one_node_converge_on_the_scoped_row(
    tmp_path: Path,
) -> None:
    """Deterministic behavior when a node already holds both durable rows."""

    async def scenario() -> None:
        rig = Rig(tmp_path)
        await rig.start()
        try:
            rig.create_session(tmp_path / "owner")
            nitro = _recorder(rig, "Nitro")
            rig.seed_capability(
                node_id=nitro.node_id,
                capability_id=LEGACY_RECORDER_CAPABILITY_ID,
            )
            rig.seed_capability(
                node_id=nitro.node_id,
                capability_id=recorder_capability_id(nitro.node_id),
            )
            assert len(rig.ready_rows()) == 2

            await nitro.connect()
            await nitro.announce()

            assert set(rig.ready_rows()) == {nitro.scoped_id}
            legacy = rig.rows()[LEGACY_RECORDER_CAPABILITY_ID]
            assert legacy["status"] == CapabilityStatus.UNAVAILABLE.value
            assert legacy["node_id"] == nitro.node_id
        finally:
            await rig.stop()

    _run(scenario)


def test_another_nodes_legacy_row_is_never_taken_over(tmp_path: Path) -> None:
    async def scenario() -> None:
        rig = Rig(tmp_path)
        await rig.start()
        try:
            rig.create_session(tmp_path / "owner")
            nitro = _recorder(rig, "Nitro")
            msh = _recorder(rig, "MSH", sources=("machine-2",))
            rig.seed_capability(
                node_id=nitro.node_id,
                capability_id=LEGACY_RECORDER_CAPABILITY_ID,
            )

            await msh.connect()
            await msh.announce()

            rows = rig.rows()
            assert rows[LEGACY_RECORDER_CAPABILITY_ID]["node_id"] == nitro.node_id
            assert (
                rows[LEGACY_RECORDER_CAPABILITY_ID]["status"]
                == CapabilityStatus.READY.value
            )
            assert rows[msh.scoped_id]["node_id"] == msh.node_id
        finally:
            await rig.stop()

    _run(scenario)


# --- convergence across restarts -------------------------------------------


def test_convergence_is_stable_across_reconnect_and_restarts(
    tmp_path: Path,
) -> None:
    """Reconnect, recorder restart and coordinator restart must all be no-ops."""

    async def scenario() -> None:
        rig = Rig(tmp_path)
        await rig.start()
        try:
            rig.create_session(tmp_path / "owner")
            nitro = _recorder(rig, "Nitro")
            msh = _recorder(rig, "MSH", sources=("machine-2",))
            rig.seed_capability(
                node_id=nitro.node_id,
                capability_id=LEGACY_RECORDER_CAPABILITY_ID,
            )
            for recorder in (nitro, msh):
                await recorder.connect()
                await recorder.announce()
            converged = set(rig.rows())

            for _ in range(3):
                for recorder in (nitro, msh):
                    await recorder.reconnect()
                    await recorder.announce()
            assert set(rig.rows()) == converged

            # Recorder restart: a brand new runtime over the same durable state.
            for recorder in (nitro, msh):
                await recorder.close()
                recorder.runtime = _runtime(recorder.state_directory, recorder.name)
                recorder.node.runtime = recorder.runtime
                await recorder.connect()
                await recorder.announce()
            assert set(rig.rows()) == converged

            await rig.restart_coordinator()
            for recorder in (nitro, msh):
                await recorder.connect()
                await recorder.announce()
            assert set(rig.rows()) == converged
            assert set(rig.ready_rows()) == {
                LEGACY_RECORDER_CAPABILITY_ID,
                msh.scoped_id,
            }
        finally:
            await rig.stop()

    _run(scenario)


def test_no_conflict_retry_loop_after_convergence(tmp_path: Path) -> None:
    """The rejected-announcement flood must not survive convergence."""

    async def scenario() -> None:
        rig = Rig(tmp_path)
        await rig.start()
        try:
            rig.create_session(tmp_path / "owner")
            nitro = _recorder(rig, "Nitro")
            msh = _recorder(rig, "MSH", sources=("machine-2",))
            rig.seed_capability(
                node_id=nitro.node_id,
                capability_id=LEGACY_RECORDER_CAPABILITY_ID,
            )
            for recorder in (nitro, msh):
                await recorder.connect()

            for _ in range(25):
                for recorder in (nitro, msh):
                    await recorder.announce()

            rows = rig.rows()
            assert len(rows) == 2
            assert set(rig.ready_rows()) == set(rows)
        finally:
            await rig.stop()

    _run(scenario)


def test_both_recorders_remain_independently_targetable(tmp_path: Path) -> None:
    async def scenario() -> None:
        rig = Rig(tmp_path)
        await rig.start()
        try:
            rig.create_session(tmp_path / "owner")
            nitro = _recorder(rig, "Nitro", sources=("machine-1",))
            msh = _recorder(rig, "MSH", sources=("machine-2",))
            rig.seed_capability(
                node_id=nitro.node_id,
                capability_id=LEGACY_RECORDER_CAPABILITY_ID,
                source_names=("machine-1",),
            )
            for recorder in (nitro, msh):
                await recorder.connect()
                await recorder.announce()

            assert rig.coordinator is not None
            status = rig.coordinator.status(actor_node_id=rig.owner_node_id)
            recorders = FederationRecorderControlService._recorders(status)

            assert set(recorders) == {nitro.node_id, msh.node_id}
            assert recorders[nitro.node_id]["source_names"] == ["machine-1"]
            assert recorders[msh.node_id]["source_names"] == ["machine-2"]
        finally:
            await rig.stop()

    _run(scenario)


def test_a_retired_legacy_row_is_not_targetable(tmp_path: Path) -> None:
    async def scenario() -> None:
        rig = Rig(tmp_path)
        await rig.start()
        try:
            rig.create_session(tmp_path / "owner")
            nitro = _recorder(rig, "Nitro")
            rig.seed_capability(
                node_id=nitro.node_id,
                capability_id=LEGACY_RECORDER_CAPABILITY_ID,
            )
            rig.seed_capability(
                node_id=nitro.node_id,
                capability_id=recorder_capability_id(nitro.node_id),
            )
            await nitro.connect()
            await nitro.announce()

            assert rig.coordinator is not None
            status = rig.coordinator.status(actor_node_id=rig.owner_node_id)
            recorders = FederationRecorderControlService._recorders(status)

            assert set(recorders) == {nitro.node_id}
        finally:
            await rig.stop()

    _run(scenario)


def test_local_state_restored_from_a_stale_backup_reconverges(
    tmp_path: Path,
) -> None:
    """A literal file-level restore of the pre-migration node state."""

    async def scenario() -> None:
        rig = Rig(tmp_path)
        await rig.start()
        try:
            rig.create_session(tmp_path / "owner")
            nitro = _recorder(rig, "Nitro")
            rig.seed_capability(
                node_id=nitro.node_id,
                capability_id=LEGACY_RECORDER_CAPABILITY_ID,
            )
            await nitro.connect()
            client = nitro.runtime._connected_client()
            client.state.save_capability(
                CapabilityAnnouncement(
                    capability_id=LEGACY_RECORDER_CAPABILITY_ID,
                    node_id=nitro.node_id,
                    session_id=SESSION_ID,
                    type=RECORDER_CAPABILITY_TYPE,
                    protocol="mtconnect",
                    protocol_version="1",
                    status=CapabilityStatus.READY,
                    properties={"kind": "standalone-recorder"},
                    announced_at=NOW,
                ),
                now=NOW,
            )
            database = nitro.state_directory / "node_state.sqlite3"
            backup = tmp_path / "nitro-node-state.backup"
            await nitro.close()
            _copy_node_state(database, backup)

            # Move on: the node converges to the scoped identity.
            nitro.runtime = _runtime(nitro.state_directory, nitro.name)
            nitro.node.runtime = nitro.runtime
            await nitro.connect()
            rig.seed_capability(
                node_id=nitro.node_id,
                capability_id=recorder_capability_id(nitro.node_id),
            )
            await nitro.announce()
            assert set(rig.ready_rows()) == {nitro.scoped_id}

            # Now restore the pre-migration backup over the live state.
            await nitro.close()
            _copy_node_state(backup, database)
            nitro.runtime = _runtime(nitro.state_directory, nitro.name)
            nitro.node.runtime = nitro.runtime
            await nitro.connect()
            await nitro.announce()

            assert set(rig.ready_rows()) == {nitro.scoped_id}
            assert LEGACY_RECORDER_CAPABILITY_ID not in nitro.local_capability_ids()
        finally:
            await rig.stop()

    _run(scenario)


# --- relay client capability replay ----------------------------------------


def test_connect_drops_a_cached_identity_the_coordinator_reassigned(
    tmp_path: Path,
) -> None:
    """A stale cached capability must not strand a node offline.

    ``connect()`` replays every locally cached capability. Before this change a
    single cached row the coordinator had since assigned to another node failed
    the whole connection, so the node could never reach the point of
    reconciling its own identity.
    """

    async def scenario() -> None:
        rig = Rig(tmp_path)
        await rig.start()
        try:
            rig.create_session(tmp_path / "owner")
            owner = rig.owner_node_id
            state_directory = tmp_path / "stale-node"
            node_id = rig.enroll(state_directory, "Stale")
            rig.join(node_id, "stale")
            rig.seed_capability(
                node_id=owner,
                capability_id=LEGACY_RECORDER_CAPABILITY_ID,
            )

            client = RelayNodeClient(
                state_directory=state_directory,
                relay_url=rig.url,
                display_name="Stale",
                allow_insecure_local=True,
                heartbeat_interval=300,
                request_timeout=TIMEOUT,
                clock=lambda: NOW,
            )
            await client.connect()
            client.state.save_capability(
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
                now=NOW,
            )
            await client.disconnect()

            reconnected = RelayNodeClient(
                state_directory=state_directory,
                relay_url=rig.url,
                display_name="Stale",
                allow_insecure_local=True,
                heartbeat_interval=300,
                request_timeout=TIMEOUT,
                clock=lambda: NOW,
            )
            await reconnected.connect()
            try:
                assert reconnected.connected_event.is_set()
                assert reconnected.state.advertised_capabilities(
                    session_id=SESSION_ID
                ) == ()
                assert (
                    rig.rows()[LEGACY_RECORDER_CAPABILITY_ID]["node_id"] == owner
                )
            finally:
                await reconnected.disconnect()
        finally:
            await rig.stop()

    _run(scenario)


def test_connect_still_fails_closed_on_an_unrelated_rejection(
    tmp_path: Path,
) -> None:
    """Only an identity conflict is tolerated; other rejections stay fatal."""

    async def scenario() -> None:
        rig = Rig(tmp_path)
        await rig.start()
        try:
            rig.create_session(tmp_path / "owner")
            state_directory = tmp_path / "unrelated-node"
            node_id = rig.enroll(state_directory, "Unrelated")
            rig.join(node_id, "unrelated")

            client = RelayNodeClient(
                state_directory=state_directory,
                relay_url=rig.url,
                display_name="Unrelated",
                allow_insecure_local=True,
                heartbeat_interval=300,
                request_timeout=TIMEOUT,
                clock=lambda: NOW,
            )
            await client.connect()
            client.state.save_capability(
                CapabilityAnnouncement(
                    capability_id="recorder-orphan",
                    node_id=node_id,
                    session_id=SESSION_ID,
                    type=RECORDER_CAPABILITY_TYPE,
                    protocol="mtconnect",
                    protocol_version="1",
                    status=CapabilityStatus.READY,
                    properties={"kind": "standalone-recorder"},
                    announced_at=NOW,
                ),
                now=NOW,
            )
            await client.disconnect()

            reconnected = RelayNodeClient(
                state_directory=state_directory,
                relay_url=rig.url,
                display_name="Unrelated",
                allow_insecure_local=True,
                heartbeat_interval=300,
                request_timeout=TIMEOUT,
                clock=lambda: NOW,
            )

            async def rejected_announce(capability):
                raise RelayRemoteError(
                    "session-capacity-exhausted",
                    "the coordinator refused this announcement",
                )

            reconnected.announce_capability = rejected_announce
            with pytest.raises(RelayRemoteError) as rejected:
                await reconnected.connect()
            assert rejected.value.code == "session-capacity-exhausted"
            # The cache entry survives: only an identity conflict is stale.
            assert "recorder-orphan" in tuple(
                item.capability_id
                for item in reconnected.state.advertised_capabilities(
                    session_id=SESSION_ID
                )
            )
        finally:
            await rig.stop()

    _run(scenario)


def test_malformed_local_capability_state_is_a_separate_connect_failure(
    tmp_path: Path,
) -> None:
    """Classification pin: corrupt local rows fail connect, before migration.

    This is NOT the identity defect and is not repaired by identity
    reconciliation: ``advertised_capabilities()`` raises for the whole table on
    any row it cannot decode, so the connection fails before any recorder code
    runs.

    Its reachable surface is narrow. ``advertised_capabilities`` carries a
    ``json_valid`` CHECK constraint, so truncated or non-JSON content cannot be
    stored at all -- only well-formed JSON that is not a valid announcement gets
    this far, which is what this test writes. Pinned here so the boundary is
    explicit and a future robustness change is a deliberate contract decision
    rather than an accident.
    """

    async def scenario() -> None:
        rig = Rig(tmp_path)
        await rig.start()
        try:
            rig.create_session(tmp_path / "owner")
            state_directory = tmp_path / "corrupt-node"
            node_id = rig.enroll(state_directory, "Corrupt")
            rig.join(node_id, "corrupt")

            client = RelayNodeClient(
                state_directory=state_directory,
                relay_url=rig.url,
                display_name="Corrupt",
                allow_insecure_local=True,
                heartbeat_interval=300,
                request_timeout=TIMEOUT,
                clock=lambda: NOW,
            )
            await client.connect()
            await client.disconnect()

            database = sqlite3.connect(state_directory / "node_state.sqlite3")
            try:
                database.execute(
                    """
                    INSERT INTO advertised_capabilities(
                        session_id,capability_id,capability_json,updated_at
                    ) VALUES(?,?,?,?)
                    """,
                    (SESSION_ID, "recorder-broken", "{}", "2026-09-04T09:00:00Z"),
                )
                database.commit()
            finally:
                database.close()

            reconnected = RelayNodeClient(
                state_directory=state_directory,
                relay_url=rig.url,
                display_name="Corrupt",
                allow_insecure_local=True,
                heartbeat_interval=300,
                request_timeout=TIMEOUT,
                clock=lambda: NOW,
            )
            with pytest.raises(Exception) as failure:
                await reconnected.connect()
            assert getattr(failure.value, "code", "") == "malformed-node-state"
        finally:
            await rig.stop()

    _run(scenario)


# --- pure identity derivation ----------------------------------------------


def test_recorder_capability_id_is_derived_from_the_node_identity() -> None:
    assert recorder_capability_id("node-abc") == "recorder-node-abc"
    assert recorder_capability_id("node-abc") != recorder_capability_id("node-def")
