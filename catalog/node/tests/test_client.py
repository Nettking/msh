from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest

import catalog.node.client as node_client_module
from catalog.federation.models import SessionEvent
from catalog.federation.protocol import RelayEnvelope
from catalog.node.client import RelayNodeClient, RelayRemoteError
from catalog.node.state import (
    ConnectionState,
    EnrollmentState,
    NodeStateError,
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


def _relay_message(
    client: RelayNodeClient,
    *,
    request_id: str,
    session_id: str,
    actor_node_id: str,
    correlation_id: str,
) -> RelayEnvelope:
    return RelayEnvelope(
        request_id=request_id,
        session_id=session_id,
        actor_node_id=actor_node_id,
        target_node_id=client.node_id,
        message_type="relay.message",
        authorization_context={"kind": "authenticated-node"},
        payload={"correlation_id": correlation_id, "status": "ok"},
        sent_at=client._clock(),
    )


def test_message_response_is_claimed_before_the_shared_inbound_queue(
    tmp_path: Path,
) -> None:
    class FakeWebSocket:
        def __init__(self, messages: list[str]) -> None:
            self._messages = iter(messages)

        def __aiter__(self) -> FakeWebSocket:
            return self

        async def __anext__(self) -> str:
            try:
                return next(self._messages)
            except StopIteration as error:
                raise StopAsyncIteration from error

    async def scenario() -> None:
        client = _client(tmp_path)
        sent = asyncio.Event()
        release_delivery = asyncio.Event()
        sent_payload: dict[str, Any] = {}

        async def send_message(
            *,
            session_id: str,
            target_node_id: str,
            payload: dict[str, Any],
            request_id: str | None = None,
        ) -> dict[str, Any]:
            assert session_id == "session-a"
            assert target_node_id == "remote-node"
            assert request_id is None
            sent_payload.update(payload)
            sent.set()
            await release_delivery.wait()
            return {"delivered": True}

        client.send_message = send_message  # type: ignore[method-assign]
        request = asyncio.create_task(
            client.request_message_response(
                "session-a",
                "remote-node",
                {"kind": "test.request"},
                "correlation-a",
            )
        )
        await asyncio.wait_for(sent.wait(), timeout=1)

        wrong_actor = _relay_message(
            client,
            request_id="wrong-actor",
            session_id="session-a",
            actor_node_id="other-node",
            correlation_id="correlation-a",
        )
        wrong_session = _relay_message(
            client,
            request_id="wrong-session",
            session_id="session-b",
            actor_node_id="remote-node",
            correlation_id="correlation-a",
        )
        valid = _relay_message(
            client,
            request_id="valid-reply",
            session_id="session-a",
            actor_node_id="remote-node",
            correlation_id="correlation-a",
        )
        websocket = FakeWebSocket(
            [
                wrong_actor.to_json(),
                wrong_session.to_json(),
                valid.to_json(),
            ]
        )
        disconnect_codes: list[str | None] = []

        async def disconnect(*, error_code: str | None = None) -> None:
            disconnect_codes.append(error_code)

        client._websocket = websocket  # type: ignore[assignment]
        client.disconnect = disconnect  # type: ignore[method-assign]
        await client._receiver_loop()
        release_delivery.set()

        assert await asyncio.wait_for(request, timeout=1) == valid
        assert sent_payload == {
            "kind": "test.request",
            "correlation_id": "correlation-a",
        }
        assert await client.receive_message(timeout=1) == wrong_actor
        assert await client.receive_message(timeout=1) == wrong_session
        assert client._inbound.empty()
        assert client._pending_message_responses == {}
        assert disconnect_codes == [None]

    asyncio.run(scenario())


def test_message_response_rejects_duplicate_and_bounded_pending_ids(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        monkeypatch.setattr(
            node_client_module,
            "MAX_PENDING_MESSAGE_RESPONSES",
            1,
        )
        client = _client(tmp_path)
        sending = asyncio.Event()

        async def blocked_send(**_: Any) -> dict[str, Any]:
            sending.set()
            await asyncio.Event().wait()

        client.send_message = blocked_send  # type: ignore[method-assign]
        first = asyncio.create_task(
            client.request_message_response(
                "session-a",
                "remote-node",
                {},
                "correlation-a",
            )
        )
        await asyncio.wait_for(sending.wait(), timeout=1)

        with pytest.raises(NodeStateError) as duplicate:
            await client.request_message_response(
                "session-a",
                "remote-node",
                {},
                "correlation-a",
            )
        assert duplicate.value.code == "duplicate-local-correlation-id"

        with pytest.raises(NodeStateError) as bounded:
            await client.request_message_response(
                "session-a",
                "remote-node",
                {},
                "correlation-b",
            )
        assert bounded.value.code == "too-many-pending-message-responses"

        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        assert client._pending_message_responses == {}

    asyncio.run(scenario())


def test_message_response_requires_delivery_confirmation(tmp_path: Path) -> None:
    async def scenario() -> None:
        client = _client(tmp_path)

        async def undelivered(**_: Any) -> dict[str, Any]:
            return {"delivered": False}

        client.send_message = undelivered  # type: ignore[method-assign]
        with pytest.raises(RelayRemoteError) as rejected:
            await client.request_message_response(
                "session-a",
                "remote-node",
                {},
                "correlation-a",
            )

        assert rejected.value.code == "message-delivery-not-confirmed"
        assert client._pending_message_responses == {}

    asyncio.run(scenario())


def test_disconnect_fails_and_clears_pending_message_responses(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        client = _client(tmp_path)
        _join_connected_session(client)
        delivered = asyncio.Event()

        async def send_message(**_: Any) -> dict[str, Any]:
            delivered.set()
            return {"delivered": True}

        client.send_message = send_message  # type: ignore[method-assign]
        request = asyncio.create_task(
            client.request_message_response(
                "session-a",
                "remote-node",
                {},
                "correlation-a",
            )
        )
        await asyncio.wait_for(delivered.wait(), timeout=1)
        await asyncio.sleep(0)

        await client.disconnect(error_code="relay-lost")

        with pytest.raises(RelayRemoteError) as rejected:
            await request
        assert rejected.value.code == "relay-lost"
        assert client._pending_message_responses == {}

    asyncio.run(scenario())


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
                "schema": "fcp.coordinator_status.v1",
                "coordinator_id": "coordinator",
                "sessions": [{"session_id": "stale-session"}],
                "nodes": [],
                "capabilities": [],
                "pagination": {
                    "schema": "fcp.coordinator_status.pagination.v1",
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
                "schema": "fcp.coordinator_status.v1",
                "coordinator_id": "coordinator",
                "sessions": [{"session_id": "current-session"}],
                "nodes": [],
                "capabilities": [],
                "pagination": {
                    "schema": "fcp.coordinator_status.pagination.v1",
                    "page_start": 0,
                    "item_count": 1,
                    "total_items": 2,
                    "has_more": True,
                    "next_cursor": "current-cursor",
                },
            },
            {
                "schema": "fcp.coordinator_status.v1",
                "coordinator_id": "coordinator",
                "sessions": [],
                "nodes": [{"node_id": "current-node"}],
                "capabilities": [],
                "pagination": {
                    "schema": "fcp.coordinator_status.pagination.v1",
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
            "schema": "fcp.coordinator_status.v1",
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


def test_concurrent_replay_requests_share_the_authorized_completion(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        client = _client(tmp_path)
        started = asyncio.Event()
        release = asyncio.Event()
        calls = 0

        async def blocked_replay(session_id: str) -> dict[str, Any]:
            nonlocal calls
            assert session_id == "session-a"
            calls += 1
            started.set()
            await release.wait()
            return {
                "session_id": session_id,
                "current_revision": 3,
                "last_revision": 3,
                "has_more": False,
            }

        client._request_replay_pass = blocked_replay  # type: ignore[method-assign]
        first = asyncio.create_task(client.request_replay("session-a"))
        await asyncio.wait_for(started.wait(), timeout=1)
        second = asyncio.create_task(client.request_replay("session-a"))
        await asyncio.sleep(0)

        assert calls == 1
        assert len(client._replay_tasks) == 1

        release.set()
        first_result, second_result = await asyncio.gather(first, second)

        assert first_result == second_result
        assert calls == 1
        assert client._replay_tasks == {}

    asyncio.run(scenario())


def test_coordinator_replay_page_rejects_an_incomplete_cached_page(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        client = _client(tmp_path)

        async def completed_replay(session_id: str) -> dict[str, Any]:
            assert session_id == "session-a"
            return {
                "session_id": session_id,
                "current_revision": 1,
                "last_revision": 1,
                "has_more": False,
            }

        client.request_replay = completed_replay  # type: ignore[method-assign]
        client.state.applied_event_page = (  # type: ignore[method-assign]
            lambda *_args, **_kwargs: ()
        )

        with pytest.raises(RelayRemoteError) as rejected:
            await client.coordinator_replay_page(
                session_id="session-a",
                last_applied_revision=0,
                limit=1,
            )

        assert rejected.value.code == "revision-gap"

    asyncio.run(scenario())


def test_replay_request_never_reuses_a_completed_task(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        client = _client(tmp_path)
        stale = asyncio.create_task(
            asyncio.sleep(0, result={"current_revision": 1})
        )
        await stale
        client._replay_tasks["session-a"] = stale
        calls = 0

        async def fresh_replay(session_id: str) -> dict[str, Any]:
            nonlocal calls
            assert session_id == "session-a"
            calls += 1
            return {"current_revision": 2}

        client._request_replay_pass = fresh_replay  # type: ignore[method-assign]

        assert await client.request_replay("session-a") == {
            "current_revision": 2
        }
        assert calls == 1

    asyncio.run(scenario())


def test_replay_teardown_is_reported_as_a_structured_relay_failure(
    tmp_path: Path,
) -> None:
    """A closed connection must not reach callers as a bare cancellation.

    ``asyncio.shield`` reports a cancelled shared task with the same
    ``CancelledError`` it uses for the caller's own cancellation. That is a
    ``BaseException``, so before this every supervised Federation runtime that
    retried on ``except Exception`` was killed outright by an ordinary relay
    disconnect.
    """

    async def scenario() -> None:
        client = _client(tmp_path)
        _join_connected_session(client)
        started = asyncio.Event()

        async def blocked_replay(session_id: str) -> dict[str, Any]:
            started.set()
            await asyncio.Event().wait()
            raise AssertionError("unreachable")

        client._request_replay_pass = blocked_replay  # type: ignore[method-assign]
        waiter = asyncio.create_task(client.request_replay("session-a"))
        await asyncio.wait_for(started.wait(), timeout=1)

        await client.disconnect(error_code="connection-replaced")

        with pytest.raises(RelayRemoteError) as rejected:
            await waiter

        assert isinstance(rejected.value, Exception)
        assert rejected.value.code == "connection-replaced"
        assert client._replay_tasks == {}
        assert client._gap_replay_tasks == {}

    asyncio.run(scenario())


def test_replay_teardown_without_an_error_code_still_fails_closed(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        client = _client(tmp_path)
        _join_connected_session(client)
        started = asyncio.Event()

        async def blocked_replay(session_id: str) -> dict[str, Any]:
            started.set()
            await asyncio.Event().wait()
            raise AssertionError("unreachable")

        client._request_replay_pass = blocked_replay  # type: ignore[method-assign]
        waiter = asyncio.create_task(client.request_replay("session-a"))
        await asyncio.wait_for(started.wait(), timeout=1)

        await client.disconnect()

        with pytest.raises(RelayRemoteError) as rejected:
            await waiter

        assert rejected.value.code == "connection-closed"

    asyncio.run(scenario())


def test_every_shared_replay_waiter_learns_the_same_teardown_reason(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        client = _client(tmp_path)
        _join_connected_session(client)
        started = asyncio.Event()

        async def blocked_replay(session_id: str) -> dict[str, Any]:
            started.set()
            await asyncio.Event().wait()
            raise AssertionError("unreachable")

        client._request_replay_pass = blocked_replay  # type: ignore[method-assign]
        first = asyncio.create_task(client.request_replay("session-a"))
        await asyncio.wait_for(started.wait(), timeout=1)
        second = asyncio.create_task(client.request_replay("session-a"))
        await asyncio.sleep(0)

        assert len(client._replay_tasks) == 1

        await client.disconnect(error_code="revoked-node")
        outcomes = await asyncio.gather(
            first, second, return_exceptions=True
        )

        assert [getattr(item, "code", None) for item in outcomes] == [
            "revoked-node",
            "revoked-node",
        ]

    asyncio.run(scenario())


def test_a_cancelled_replay_caller_still_observes_its_own_cancellation(
    tmp_path: Path,
) -> None:
    """Only teardown becomes an error; a real cancellation must propagate."""

    async def scenario() -> None:
        client = _client(tmp_path)
        _join_connected_session(client)
        started = asyncio.Event()

        async def blocked_replay(session_id: str) -> dict[str, Any]:
            started.set()
            await asyncio.Event().wait()
            raise AssertionError("unreachable")

        client._request_replay_pass = blocked_replay  # type: ignore[method-assign]
        waiter = asyncio.create_task(client.request_replay("session-a"))
        await asyncio.wait_for(started.wait(), timeout=1)
        shared = client._replay_tasks["session-a"]

        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter

        # The shared pass is shielded from one caller going away.
        assert not shared.done()
        assert client._replay_tasks["session-a"] is shared

        await client.disconnect(error_code="test-teardown")
        assert client._replay_tasks == {}

    asyncio.run(scenario())


def test_a_caller_cancelled_during_teardown_is_not_downgraded(
    tmp_path: Path,
) -> None:
    """A task cancelled while the shared pass dies keeps its cancellation."""

    async def scenario() -> None:
        client = _client(tmp_path)
        _join_connected_session(client)
        started = asyncio.Event()

        async def blocked_replay(session_id: str) -> dict[str, Any]:
            started.set()
            await asyncio.Event().wait()
            raise AssertionError("unreachable")

        client._request_replay_pass = blocked_replay  # type: ignore[method-assign]
        waiter = asyncio.create_task(client.request_replay("session-a"))
        await asyncio.wait_for(started.wait(), timeout=1)
        shared = client._replay_tasks["session-a"]

        shared.cancel()
        waiter.cancel()

        with pytest.raises(asyncio.CancelledError):
            await waiter

    asyncio.run(scenario())


def test_a_reconnected_replay_never_reuses_stale_teardown_state(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        client = _client(tmp_path)
        _join_connected_session(client)
        started = asyncio.Event()

        async def blocked_replay(session_id: str) -> dict[str, Any]:
            started.set()
            await asyncio.Event().wait()
            raise AssertionError("unreachable")

        client._request_replay_pass = blocked_replay  # type: ignore[method-assign]
        waiter = asyncio.create_task(client.request_replay("session-a"))
        await asyncio.wait_for(started.wait(), timeout=1)
        await client.disconnect(error_code="connection-replaced")
        with pytest.raises(RelayRemoteError):
            await waiter

        assert client._replay_teardown_codes == {
            "session-a": "connection-replaced"
        }

        async def completed_replay(session_id: str) -> dict[str, Any]:
            return {"current_revision": 4}

        client._request_replay_pass = completed_replay  # type: ignore[method-assign]

        assert await client.request_replay("session-a") == {
            "current_revision": 4
        }
        assert client._replay_teardown_codes == {}
        assert client._replay_tasks == {}

    asyncio.run(scenario())


def test_teardown_leaves_no_pending_replay_or_gap_task(
    tmp_path: Path,
) -> None:
    """No task may still be pending when the client's loop is torn down."""

    async def scenario() -> None:
        client = _client(tmp_path)
        _join_connected_session(client)
        started = asyncio.Event()

        async def blocked_replay(session_id: str) -> dict[str, Any]:
            started.set()
            await asyncio.Event().wait()
            raise AssertionError("unreachable")

        client._request_replay_pass = blocked_replay  # type: ignore[method-assign]
        waiter = asyncio.create_task(client.request_replay("session-a"))
        await asyncio.wait_for(started.wait(), timeout=1)
        shared = client._replay_tasks["session-a"]
        gap = client._schedule_gap_replay("session-a")
        await asyncio.sleep(0)

        await client.disconnect(error_code="connection-replaced")
        with pytest.raises(RelayRemoteError):
            await waiter

        assert shared.done()
        assert gap.done()
        assert client._replay_tasks == {}
        assert client._gap_replay_tasks == {}
        pending = {
            task
            for task in asyncio.all_tasks()
            if task is not asyncio.current_task()
        }
        assert pending == set()

    asyncio.run(scenario())
