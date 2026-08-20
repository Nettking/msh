"""Recorder recovery of an omitted remote Federation session creator.

Recorder storage selection trusts only the session creator's advertised
logical-storage authority, and remote coordinator status may omit
``created_by_node_id``. The launcher recovers that immutable identity from the
authoritative revision-1 ``session.created`` event.

Reading that event is a bounded authenticated replay, which the node layer only
allows for a session the local state has joined. ``RelayNodeClient.connect``
reads coordinator status *before* it reconciles local membership, so the
recovery has to stay a read-only probe that can never be the reason a recorder
fails to connect.
"""

from __future__ import annotations

import asyncio
import ipaddress
import os
from pathlib import Path
from types import SimpleNamespace

from catalog.federation.errors import AuthorizationError
from catalog.node.client import RelayNodeClient
from scripts import start_tailscale_recorder as launcher


def _client(*, joined: tuple[str, ...], replay=None) -> object:
    class Client:
        state = SimpleNamespace(
            joined_sessions=lambda: tuple(
                SimpleNamespace(session_id=item) for item in joined
            )
        )

        async def coordinator_replay_page(self, **kwargs):
            if replay is None:
                raise AssertionError("replay must not be attempted")
            return replay(**kwargs)

    return Client()


def _created_event(session_id: str, actor_node_id: str):
    return SimpleNamespace(
        session_id=session_id,
        revision=1,
        event_type="session.created",
        actor_node_id=actor_node_id,
    )


def test_a_joined_session_recovers_its_creator_from_revision_one() -> None:
    def replay(*, session_id: str, last_applied_revision: int, limit: int):
        assert session_id == "session-1"
        assert last_applied_revision == 0
        assert limit == 1
        return ((_created_event("session-1", "node-owner"),), 7)

    status = {
        "schema": "fcp.coordinator_status.v1",
        "coordinator_id": "coordinator-1",
        "sessions": [{"session_id": "session-1"}],
        "nodes": [],
        "capabilities": [],
    }

    restored = asyncio.run(
        launcher._restore_remote_session_creators(
            _client(joined=("session-1",), replay=replay), status
        )
    )

    assert restored["sessions"] == [
        {"session_id": "session-1", "created_by_node_id": "node-owner"}
    ]
    assert "created_by_node_id" not in status["sessions"][0]


def test_a_session_this_node_has_not_joined_is_left_untouched() -> None:
    """connect() reads status before it reconciles local membership.

    Replaying a session the local state does not know raises
    session-not-joined, which would stop the recorder from connecting at all
    on a fresh data directory.
    """

    status = {
        "sessions": [{"session_id": "session-1"}],
        "capabilities": [],
    }

    restored = asyncio.run(
        launcher._restore_remote_session_creators(_client(joined=()), status)
    )

    assert restored is status


def test_an_unreadable_replay_leaves_the_owner_absent_rather_than_raising() -> None:
    """A status probe must never be what stops a node from connecting."""

    def replay(**_kwargs):
        raise AuthorizationError(
            "not-session-member", "not joined", "session_id"
        )

    status = {
        "sessions": [{"session_id": "session-1"}],
        "capabilities": [],
    }

    restored = asyncio.run(
        launcher._restore_remote_session_creators(
            _client(joined=("session-1",), replay=replay), status
        )
    )

    # Absent, not guessed: recorder selection reports owner-unavailable.
    assert restored is status


def test_a_status_that_already_names_its_owner_is_never_replayed() -> None:
    status = {
        "sessions": [
            {"session_id": "session-1", "created_by_node_id": "node-owner"}
        ],
        "capabilities": [],
    }

    restored = asyncio.run(
        launcher._restore_remote_session_creators(
            _client(joined=("session-1",)), status
        )
    )

    assert restored is status


def test_installing_the_fallback_returns_its_own_restore() -> None:
    original = RelayNodeClient.coordinator_status

    restore = launcher._install_remote_session_creator_fallback()
    try:
        assert RelayNodeClient.coordinator_status is not original
    finally:
        restore()

    assert RelayNodeClient.coordinator_status is original


def test_the_launcher_never_leaves_the_client_class_patched(
    tmp_path: Path, monkeypatch
) -> None:
    """A class patch outliving its caller changes every later client."""

    original = RelayNodeClient.coordinator_status
    monkeypatch.delenv("FCP_RECORDER_FEDERATION_KEY", raising=False)
    monkeypatch.setattr(
        launcher,
        "_tailscale_ipv4",
        lambda: ipaddress.ip_address("100.100.0.44"),
    )
    monkeypatch.setattr(
        launcher, "_pairing_key", lambda _args: "FCP1-private-test-key"
    )
    monkeypatch.setattr(launcher, "_relay_url", lambda _args, _key: "ws://tail:8765")
    monkeypatch.setattr(launcher, "_require_tailnet_relay", lambda _url: None)

    patched_during_run: list[bool] = []

    def fake_start(_arguments: list[str]) -> int:
        patched_during_run.append(
            RelayNodeClient.coordinator_status is not original
        )
        return 0

    monkeypatch.setattr(launcher.start_recorder, "main", fake_start)

    assert (
        launcher.main(["--data-dir", str(tmp_path / "recorder-data")]) == 0
    )

    assert patched_during_run == [True]
    assert RelayNodeClient.coordinator_status is original
    assert "FCP_RECORDER_FEDERATION_KEY" not in os.environ
