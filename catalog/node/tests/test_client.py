from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest

import catalog.node.client as node_client_module
from catalog.federation.models import SessionEvent
from catalog.node.client import RelayNodeClient, RelayRemoteError
from catalog.node.state import (
    ConnectionState,
    EnrollmentState,
    ReconnectPolicy,
)


def _client(
    tmp_path: Path,
    *,
    sleep=asyncio.sleep,
    reconnect_policy: ReconnectPolicy | None = None,
) -> RelayNodeClient:
    return RelayNodeClient(
        state_directory=tmp_path / "node",
        relay_url="ws://127.0.0.1:8765",
        display_name="Node",
        allow_insecure_local=True,
        sleep=sleep,
        reconnect_policy=reconnect_policy,
    )


def _join_connected_session(
    client: RelayNodeClient, session_id: str = "session-a"
) -> None:
    now = client._clock()
    client.state.set_enrollment_state(EnrollmentState.ENROLLED, now=now)
    client.state.set_connection_state(ConnectionState.CONNECTED, now=now)
    client.state.join_session(session_id, now=now)


def _gap_event(client: RelayNodeClient, session_id: str = "session-a") -> SessionEvent:
    return SessionEvent(
        session_id=session_id,
        revision=2,
        event_id=f"{session_id}-event-2",
        event_type="test.gap",
        occurred_at=client._clock(),
        actor_node_id="remote-node",
        payload={"revision": 2},
    )


def test_run_forever_uses_finite_capped_backoff(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        delays: list[float] = []

        async def record_delay(delay: float) -> None:
            delays.append(delay)

        client = _client(
            tmp_path,
            sleep=record_delay,
            reconnect_policy=ReconnectPolicy(
                initial_delay_seconds=0.5,
                multiplier=2,
                max_delay_seconds=2,
                max_attempts=4,
            ),
        )
        attempts = 0

        async def unavailable(
            *, enrollment_token: str | None = None
        ) -> None:
            nonlocal attempts
            assert enrollment_token is None
            attempts += 1
            raise OSError("relay unavailable")

        client.connect = unavailable  # type: ignore[method-assign]
        await client.run_forever()

        assert attempts == 4
        assert delays == [0.5, 1.0, 2]
        assert client.state.status()["connection_state"] == "error"

    asyncio.run(scenario())


def test_run_forever_bounds_repeated_short_lived_connections(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        delays: list[float] = []

        async def record_delay(delay: float) -> None:
            delays.append(delay)

        client = _client(
            tmp_path,
            sleep=record_delay,
            reconnect_policy=ReconnectPolicy(
                initial_delay_seconds=0.5,
                multiplier=2,
                max_delay_seconds=2,
                max_attempts=4,
            ),
        )
        attempts = 0

        async def short_lived_connection(
            *, enrollment_token: str | None = None
        ) -> None:
            nonlocal attempts
            assert enrollment_token is None
            attempts += 1
            client.disconnected_event.set()

        client.connect = short_lived_connection  # type: ignore[method-assign]
        await client.run_forever()

        assert attempts == 4
        assert delays == [0.5, 1.0, 2]
        assert client.state.status()["connection_state"] == "error"
        assert (
            client.state.status()["last_error_code"]
            == "relay-disconnected"
        )

    asyncio.run(scenario())


def test_gap_replay_has_one_tracked_task_per_session_and_disconnect_awaits_it(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        client = _client(tmp_path)
        _join_connected_session(client)
        started = asyncio.Event()
        cleaned_up = asyncio.Event()
        replay_calls = 0

        async def blocked_replay(session_id: str) -> dict[str, object]:
            nonlocal replay_calls
            assert session_id == "session-a"
            replay_calls += 1
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cleaned_up.set()

        client.request_replay = blocked_replay  # type: ignore[method-assign]
        envelope = client._envelope(
            message_type="event.replay.event",
            session_id="session-a",
            payload={"event": _gap_event(client).to_dict()},
        )

        await client._apply_replayed_event(envelope)
        first_task = client._gap_replay_tasks["session-a"]
        await client._apply_replayed_event(envelope)
        second_task = client._gap_replay_tasks["session-a"]
        await asyncio.wait_for(started.wait(), timeout=1)

        assert first_task is second_task
        assert replay_calls == 1
        assert client.state.get_session("session-a").replaying is True

        await asyncio.wait_for(client.disconnect(), timeout=1)

        assert first_task.cancelled()
        assert cleaned_up.is_set()
        assert client._gap_replay_tasks == {}

    asyncio.run(scenario())


def test_gap_replay_failure_is_observed_and_retains_durable_error_state(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        client = _client(tmp_path)
        _join_connected_session(client)

        async def failed_replay(session_id: str) -> dict[str, object]:
            assert session_id == "session-a"
            raise RelayRemoteError(
                "revision-gap", "revision 1 is still unavailable"
            )

        client.request_replay = failed_replay  # type: ignore[method-assign]
        envelope = client._envelope(
            message_type="event.replay.event",
            session_id="session-a",
            payload={"event": _gap_event(client).to_dict()},
        )

        await client._apply_replayed_event(envelope)
        task = client._gap_replay_tasks["session-a"]
        await asyncio.wait_for(task, timeout=1)

        status = client.state.status()
        assert task.exception() is None
        assert client._gap_replay_tasks == {}
        assert client.state.get_session("session-a").replaying is True
        assert status["connection_state"] == "error"
        assert status["last_error_code"] == "revision-gap"
        assert client.disconnected_event.is_set()

    asyncio.run(scenario())


def test_heartbeat_disconnect_cannot_deadlock_receiver_cleanup(
    tmp_path: Path,
) -> None:
    class FakeWebSocket:
        def __init__(self) -> None:
            self.closed = False

        async def close(self) -> None:
            self.closed = True

    async def scenario() -> None:
        client = _client(tmp_path)
        websocket = FakeWebSocket()
        client._websocket = websocket  # type: ignore[assignment]

        async def receiver() -> None:
            try:
                await asyncio.Event().wait()
            finally:
                if client._websocket is websocket:
                    await client.disconnect(
                        error_code="receiver-failed"
                    )

        receiver_task = asyncio.create_task(receiver())
        client._receiver_task = receiver_task

        async def heartbeat_failure() -> None:
            await client.disconnect(error_code="heartbeat-failed")

        heartbeat_task = asyncio.create_task(heartbeat_failure())
        client._heartbeat_task = heartbeat_task

        await asyncio.wait_for(heartbeat_task, timeout=1)

        assert receiver_task.done()
        assert websocket.closed
        assert client._websocket is None
        assert client.state.status()["connection_state"] == "error"

    asyncio.run(scenario())


def test_coordinator_status_restarts_changed_snapshot_and_aggregates_pages(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        client = _client(tmp_path)
        calls: list[dict[str, Any]] = []
        responses: list[dict[str, Any] | RelayRemoteError] = [
            {
                "schema": "msh.coordinator_status.v1",
                "coordinator_id": "coordinator",
                "sessions": [{"session_id": "stale-session"}],
                "nodes": [],
                "capabilities": [],
                "pagination": {
                    "schema": "msh.coordinator_status.pagination.v1",
                    "page_start": 0,
                    "item_count": 1,
                    "total_items": 2,
                    "has_more": True,
                    "next_cursor": "stale-cursor",
                },
            },
            RelayRemoteError(
                "status-snapshot-changed",
                "membership changed between pages",
            ),
            {
                "schema": "msh.coordinator_status.v1",
                "coordinator_id": "coordinator",
                "sessions": [{"session_id": "current-session"}],
                "nodes": [],
                "capabilities": [],
                "pagination": {
                    "schema": "msh.coordinator_status.pagination.v1",
                    "page_start": 0,
                    "item_count": 1,
                    "total_items": 2,
                    "has_more": True,
                    "next_cursor": "current-cursor",
                },
            },
            {
                "schema": "msh.coordinator_status.v1",
                "coordinator_id": "coordinator",
                "sessions": [],
                "nodes": [{"node_id": "current-node"}],
                "capabilities": [],
                "pagination": {
                    "schema": "msh.coordinator_status.pagination.v1",
                    "page_start": 1,
                    "item_count": 1,
                    "total_items": 2,
                    "has_more": False,
                    "next_cursor": None,
                },
            },
        ]

        async def request(
            message_type: str,
            *,
            payload: dict[str, Any],
            **_: Any,
        ) -> dict[str, Any]:
            assert message_type == "status.get"
            calls.append(payload)
            response = responses.pop(0)
            if isinstance(response, RelayRemoteError):
                raise response
            return response

        client.request = request  # type: ignore[method-assign]
        status = await client.coordinator_status()

        assert calls == [
            {},
            {"cursor": "stale-cursor"},
            {},
            {"cursor": "current-cursor"},
        ]
        assert status == {
            "schema": "msh.coordinator_status.v1",
            "coordinator_id": "coordinator",
            "sessions": [{"session_id": "current-session"}],
            "nodes": [{"node_id": "current-node"}],
            "capabilities": [],
        }
        assert responses == []

    asyncio.run(scenario())


def test_replay_pass_has_a_finite_page_bound_and_keeps_durable_progress(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        monkeypatch.setattr(
            node_client_module,
            "MAX_REPLAY_PAGES_PER_PASS",
            3,
        )
        client = _client(tmp_path)
        _join_connected_session(client)
        calls = 0

        async def progressing_replay(
            message_type: str,
            *,
            payload: dict[str, Any],
            session_id: str | None = None,
            **_: Any,
        ) -> dict[str, Any]:
            nonlocal calls
            assert message_type == "event.replay"
            assert session_id == "session-a"
            before = payload["last_applied_revision"]
            calls += 1
            applied = client.state.apply_event(
                SessionEvent(
                    session_id="session-a",
                    revision=before + 1,
                    event_id=f"event-{before + 1}",
                    event_type="test.progress",
                    occurred_at=client._clock(),
                    actor_node_id="remote-node",
                    payload={"page": calls},
                ),
                now=client._clock(),
            )
            assert applied.last_applied_revision == before + 1
            return {
                "current_revision": before + 2,
                "last_revision": before + 1,
                "has_more": True,
            }

        client.request = progressing_replay  # type: ignore[method-assign]
        with pytest.raises(RelayRemoteError) as rejected:
            await client.request_replay("session-a")

        assert rejected.value.code == "replay-page-limit-exceeded"
        assert calls == 3
        assert client.state.last_applied_revision("session-a") == 3
        assert client.state.get_session("session-a").replaying is True

    asyncio.run(scenario())
