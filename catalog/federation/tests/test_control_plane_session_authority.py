"""Current session authority requires readiness and a real live voter quorum."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from catalog.federation.control_plane_replication import ControlPlaneError
from catalog.federation.errors import FederationOperationError
from catalog.federation.tests.test_control_plane_public_journal import (
    FEDERATION,
    SESSION,
    _Cluster,
    _cluster,
    _journal,
    _populate,
)


def test_reachable_isolated_leader_cannot_report_current_session_authority(tmp_path: Path):
    async def scenario():
        async with _cluster(tmp_path) as cluster:
            await _populate(cluster, tmp_path)
            original = cluster.runtimes[0]
            coordinator = cluster.relays[0].coordinator
            actor = original.node.voter_id
            session, authority = await asyncio.to_thread(
                coordinator.session_authority, session_id=SESSION, actor_node_id=actor
            )
            assert authority.leader_node_id == actor
            assert session.created_by_node_id == actor

            # A follower's retained complete snapshot is useful history, but
            # cannot itself prove the current quorum's authority.
            with pytest.raises((ControlPlaneError, FederationOperationError)):
                await asyncio.to_thread(
                    cluster.relays[1].coordinator.session_authority,
                    session_id=SESSION, actor_node_id=actor,
                )

            # Stop actual voter services. The old leader and its client-facing
            # relay remain running and retain a valid, sealed local history.
            await cluster.stop_runtime(1)
            await cluster.stop_runtime(2)
            assert original.ready
            retained = original.local.session_leadership(SESSION)
            assert retained.leader_node_id == actor
            before = _journal(original)
            with pytest.raises((ControlPlaneError, FederationOperationError)):
                await asyncio.to_thread(
                    coordinator.session_authority, session_id=SESSION, actor_node_id=actor
                )
            assert _journal(original) == before
            assert 0 in cluster.running_runtimes
            assert 0 in cluster.running_relays

    asyncio.run(scenario())


def test_real_committed_unsealed_bootstrap_cannot_grant_session_authority(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
):
    cluster = _Cluster(tmp_path)
    original = cluster.runtimes[0]
    coordinator = cluster.relays[0].coordinator
    actor = original.node.voter_id
    observed = {}
    started = []

    def interrupt_before_seal(**_kwargs):
        # This hook runs only after actual genesis and the complete public
        # prefix have committed on the real voters. Readiness is not mocked.
        journal = original.node.state["product_journal"]["sessions"][SESSION]
        assert journal["revision"] >= 1
        assert original.local.store.get_session(SESSION) is not None
        assert not original.ready
        with pytest.raises((ControlPlaneError, FederationOperationError)):
            coordinator.session_authority(session_id=SESSION, actor_node_id=actor)
        observed["refused"] = True
        original._stop.set()
        raise RuntimeError("injected pre-seal interruption")

    monkeypatch.setattr(original, "_commit_readiness_seal", interrupt_before_seal)
    try:
        for runtime in cluster.runtimes:
            runtime.start()
            started.append(runtime)
        with pytest.raises(RuntimeError, match="injected pre-seal interruption"):
            original.bootstrap_new_federation(
                federation_id=FEDERATION, session_id=SESSION,
                creator_node_id=actor, display_name="Unsealed authority regression",
            )
        assert observed == {"refused": True}
    finally:
        for runtime in reversed(started):
            runtime.close()
