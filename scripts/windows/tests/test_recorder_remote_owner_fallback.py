from __future__ import annotations

import asyncio
from types import SimpleNamespace

from scripts import start_tailscale_recorder as launcher


def test_restore_remote_session_creator_from_revision_one_event() -> None:
    class Client:
        async def coordinator_replay_page(
            self,
            *,
            session_id: str,
            last_applied_revision: int,
            limit: int,
        ):
            assert session_id == "session-1"
            assert last_applied_revision == 0
            assert limit == 1
            return (
                (
                    SimpleNamespace(
                        session_id="session-1",
                        revision=1,
                        event_type="session.created",
                        actor_node_id="node-owner",
                    ),
                ),
                7,
            )

    status = {
        "schema": "fcp.coordinator_status.v1",
        "coordinator_id": "coordinator-1",
        "sessions": [{"session_id": "session-1"}],
        "nodes": [],
        "capabilities": [],
    }

    restored = asyncio.run(
        launcher._restore_remote_session_creators(Client(), status)
    )

    assert restored["sessions"] == [
        {"session_id": "session-1", "created_by_node_id": "node-owner"}
    ]
    assert "created_by_node_id" not in status["sessions"][0]


def test_restore_remote_session_creator_never_overwrites_status_owner() -> None:
    class Client:
        async def coordinator_replay_page(self, **_kwargs):
            raise AssertionError("complete status must not replay creator")

    status = {
        "sessions": [
            {"session_id": "session-1", "created_by_node_id": "node-owner"}
        ],
        "capabilities": [],
    }

    restored = asyncio.run(
        launcher._restore_remote_session_creators(Client(), status)
    )

    assert restored is status
