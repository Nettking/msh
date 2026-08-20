from __future__ import annotations

import asyncio
import threading
from contextlib import suppress
from concurrent.futures import Future
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from catalog.federation.errors import FederationOperationError
from catalog.federation.outbox import SQLiteOutbox
from catalog.federation.phase_d_client import PhaseDIngestOutcome
from catalog.federation.recorder_delivery import (
    RECORDER_STORAGE_SCHEMA,
    DurableRecorderDeliveryQueue,
    RecorderDeliveryRunResult,
)
from catalog.mtconnect_recorder import federation_node as federation_node_module
from catalog.mtconnect_recorder.federation_node import (
    RecorderFederationNode,
    RecorderFederationSnapshot,
    _publication_cycle_status,
    select_storage_authority,
)
from start_recorder import build_parser


def _status(*capabilities: dict[str, object]) -> dict[str, object]:
    return {
        "sessions": [
            {
                "session_id": "session-1",
                "created_by_node_id": "node-owner",
            }
        ],
        "capabilities": list(capabilities),
    }


def _authority(
    *,
    node_id: str = "node-owner",
    group_ids: list[str] | None = None,
    status: str = "ready",
) -> dict[str, object]:
    return {
        "capability_id": "logical-storage-authority",
        "node_id": node_id,
        "session_id": "session-1",
        "type": "storage-control",
        "protocol": "fcp.storage-control",
        "protocol_version": "1",
        "status": status,
        "properties": {
            "kind": "recorder-logical-storage-authority",
            "group_ids": group_ids if group_ids is not None else ["telemetry"],
        },
    }


def test_select_storage_authority_auto_selects_single_owner_group() -> None:
    selected = select_storage_authority(
        _status(_authority()),
        session_id="session-1",
        requested_group=None,
    )

    assert selected.state == "ready"
    assert selected.authority_node_id == "node-owner"
    assert selected.group_id == "telemetry"


def test_select_storage_authority_rejects_non_owner_self_advertisement() -> None:
    selected = select_storage_authority(
        _status(_authority(node_id="node-untrusted")),
        session_id="session-1",
        requested_group=None,
    )

    assert selected.state == "authority-unavailable"
    assert selected.authority_node_id is None
    assert selected.group_id is None


def test_select_storage_authority_requires_explicit_choice_for_multiple_groups() -> None:
    selected = select_storage_authority(
        _status(_authority(group_ids=["telemetry", "archive"])),
        session_id="session-1",
        requested_group=None,
    )

    assert selected.state == "storage-group-required"
    assert selected.authority_node_id == "node-owner"
    assert selected.group_id is None


def test_select_storage_authority_accepts_requested_advertised_group() -> None:
    selected = select_storage_authority(
        _status(_authority(group_ids=["telemetry", "archive"])),
        session_id="session-1",
        requested_group="archive",
    )

    assert selected.state == "ready"
    assert selected.authority_node_id == "node-owner"
    assert selected.group_id == "archive"


def test_select_storage_authority_refuses_requested_unadvertised_group() -> None:
    selected = select_storage_authority(
        _status(_authority()),
        session_id="session-1",
        requested_group="unknown",
    )

    assert selected.state == "storage-group-unavailable"
    assert selected.authority_node_id == "node-owner"
    assert selected.group_id is None


def test_start_recorder_parser_accepts_first_join_and_storage_arguments() -> None:
    args = build_parser().parse_args(
        [
            "Mazak=http://127.0.0.1:5000",
            "--federation-key",
            "FCP1-test-key",
            "--device-name",
            "Machine recorder",
            "--storage-group",
            "telemetry",
            "--require-federation",
            "--require-data-sharing",
            "--sharing-timeout",
            "30",
        ]
    )

    assert args.inputs == ["Mazak=http://127.0.0.1:5000"]
    assert args.federation_key == "FCP1-test-key"
    assert args.device_name == "Machine recorder"
    assert args.storage_group == "telemetry"
    assert args.require_federation is True
    assert args.require_data_sharing is True
    assert args.sharing_timeout == 30.0


def test_start_recorder_parser_accepts_pairing_key_as_only_input() -> None:
    args = build_parser().parse_args(["FCP1-test-key"])

    assert args.inputs == ["FCP1-test-key"]
    assert args.federation_key is None
    assert args.storage_group is None


def test_start_recorder_parser_remains_backward_compatible_local_only() -> None:
    args = build_parser().parse_args(["Mazak=http://127.0.0.1:5000"])

    assert args.federation_key is None
    assert args.storage_group is None
    assert args.require_federation is False
    assert args.require_data_sharing is False


def test_start_recorder_parser_rejects_abbreviated_security_options() -> None:
    parser = build_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(["--federation-k=FCP1-secret"])
    with pytest.raises(SystemExit):
        parser.parse_args(["--data-d", "other-data"])


@pytest.mark.parametrize("timeout", [float("nan"), float("inf"), 121.0])
def test_federation_node_rejects_unbounded_request_timeout(
    timeout: float,
    tmp_path,
) -> None:
    with pytest.raises(ValueError, match="finite"):
        RecorderFederationNode(
            data_directory=tmp_path,
            display_name="recorder",
            source_names=(),
            request_timeout=timeout,
        )


def _sharing_node(
    storage_state: str,
    *,
    last_committed_count: int = 0,
    jsonl_state: str = "ready",
) -> RecorderFederationNode:
    node = RecorderFederationNode.__new__(RecorderFederationNode)
    node._lock = threading.RLock()
    node._stop = threading.Event()
    node._publication_future = Future()
    node._snapshot = RecorderFederationSnapshot(
        status="connected",
        storage_state=storage_state,
        storage_group="telemetry" if storage_state == "up-to-date" else None,
        last_committed_count=last_committed_count,
        jsonl_state=jsonl_state,
    )
    return node


def test_sharing_readiness_accepts_only_an_active_storage_path() -> None:
    node = _sharing_node("up-to-date")

    snapshot = node.wait_until_sharing_ready(timeout_seconds=0.1)

    assert snapshot.storage_group == "telemetry"


def test_sharing_readiness_fails_closed_with_actionable_group_state() -> None:
    node = _sharing_node("storage-group-required")

    with pytest.raises(FederationOperationError) as excinfo:
        node.wait_until_sharing_ready(timeout_seconds=0.01)

    assert excinfo.value.code == "recorder-sharing-not-ready"
    assert "--storage-group" in excinfo.value.message


def test_pending_failed_outbox_is_not_mistaken_for_active_sharing() -> None:
    node = _sharing_node("publishing", last_committed_count=0)

    with pytest.raises(FederationOperationError) as excinfo:
        node.wait_until_sharing_ready(timeout_seconds=0.01)

    assert excinfo.value.code == "recorder-sharing-not-ready"


def test_sharing_readiness_requires_headless_jsonl_publication() -> None:
    node = _sharing_node("up-to-date", jsonl_state="backlogged")

    with pytest.raises(FederationOperationError) as excinfo:
        node.wait_until_sharing_ready(timeout_seconds=0.01)

    assert excinfo.value.code == "recorder-sharing-not-ready"
    assert "JSONL" in excinfo.value.message


def test_committed_backlog_cycle_proves_active_sharing() -> None:
    node = _sharing_node("publishing", last_committed_count=1)

    snapshot = node.wait_until_sharing_ready(timeout_seconds=0.1)

    assert snapshot.last_committed_count == 1


@pytest.mark.parametrize("timeout", [float("nan"), float("inf"), 601.0])
def test_sharing_readiness_timeout_is_finite_and_bounded(timeout: float) -> None:
    node = _sharing_node("discovering")

    with pytest.raises(ValueError, match="finite"):
        node.wait_until_sharing_ready(timeout_seconds=timeout)


def test_sharing_readiness_rejects_stopped_or_finished_worker() -> None:
    stopped = _sharing_node("up-to-date")
    stopped._stop.set()
    with pytest.raises(FederationOperationError) as stopped_error:
        stopped.wait_until_sharing_ready(timeout_seconds=0.1)
    assert stopped_error.value.code == "recorder-publication-stopped"

    finished = _sharing_node("up-to-date")
    finished._publication_future.set_result(None)
    with pytest.raises(FederationOperationError) as finished_error:
        finished.wait_until_sharing_ready(timeout_seconds=0.1)
    assert finished_error.value.code == "recorder-publication-stopped"


def test_sharing_readiness_rechecks_worker_after_ready_snapshot() -> None:
    node = _sharing_node("up-to-date")
    snapshot = node._snapshot

    def snapshot_during_worker_exit() -> RecorderFederationSnapshot:
        node._publication_future.set_result(None)
        return snapshot

    node.snapshot = snapshot_during_worker_exit

    with pytest.raises(FederationOperationError) as excinfo:
        node.wait_until_sharing_ready(timeout_seconds=0.1)

    assert excinfo.value.code == "recorder-publication-stopped"


def test_headless_jsonl_publisher_uses_the_selected_storage_route() -> None:
    context = object()
    state = object()
    calls: list[tuple[object, object, str, str]] = []

    class Service:
        @staticmethod
        def authorized_context() -> object:
            return context

    class Publisher:
        @staticmethod
        def publish_local_once(
            runtime_state: object,
            trusted_context: object,
            *,
            authority_node_id: str,
            group_id: str,
        ) -> object:
            calls.append(
                (
                    runtime_state,
                    trusted_context,
                    authority_node_id,
                    group_id,
                )
            )
            return SimpleNamespace(published_chunks=2)

    node = RecorderFederationNode.__new__(RecorderFederationNode)
    node.service = Service()
    node.jsonl_publisher = Publisher()

    result = asyncio.run(
        node._publish_jsonl_once(
            state,
            authority_node_id="node-owner",
            group_id="fcp-local-storage",
        )
    )

    assert result.published_chunks == 2
    assert calls == [
        (state, context, "node-owner", "fcp-local-storage"),
    ]


@pytest.mark.parametrize(
    "jsonl_failure",
    [False, True],
    ids=["ready", "jsonl-failure"],
)
def test_publication_loop_proves_each_dataset_before_full_backlog_drain(
    tmp_path,
    monkeypatch,
    jsonl_failure: bool,
) -> None:
    class _LoopGuardOutbox(SQLiteOutbox):
        def __init__(self, database) -> None:
            super().__init__(database)
            self.forbidden_thread_id: int | None = None
            self.pending_thread_ids: list[int] = []

        def pending(self, *args, **kwargs):
            thread_id = threading.get_ident()
            self.pending_thread_ids.append(thread_id)
            assert thread_id != self.forbidden_thread_id, (
                "the durable backlog was read on the relay event-loop thread"
            )
            return super().pending(*args, **kwargs)

    class _StatusClient:
        async def coordinator_status(self):
            return _status(_authority())

    class _Runtime:
        def __init__(self) -> None:
            self.client = _StatusClient()

        async def _ensure_connected(self, _state) -> None:
            return None

        def _connected_client(self):
            return self.client

    class _StorageClient:
        def __init__(self) -> None:
            self.batch_ids: list[str] = []
            self.first_a_started = asyncio.Event()
            self.release_first_a = asyncio.Event()
            self.closed = False

        async def start(self) -> None:
            return None

        async def close(self) -> None:
            self.closed = True

        async def ingest_batch(self, **kwargs):
            batch_id = str(kwargs["batch_id"])
            self.batch_ids.append(batch_id)
            if batch_id == "dataset-a-batch-1":
                self.first_a_started.set()
                await self.release_first_a.wait()
            elif batch_id.startswith("dataset-a-"):
                raise AssertionError(
                    "dataset A drained again before dataset B was probed"
                )
            return PhaseDIngestOutcome(committed=True)

    async def scenario() -> None:
        storage_client = _StorageClient()
        outbox = _LoopGuardOutbox(tmp_path / "outbox.sqlite3")
        seed_queue = DurableRecorderDeliveryQueue(
            outbox=outbox,
            client=storage_client,
            session_id="session-1",
            destination_id="telemetry",
        )
        created_at = datetime(2026, 8, 20, 17, 21, tzinfo=timezone.utc)
        for index in range(1, 5):
            seed_queue.enqueue(
                session_id="session-1",
                group_id="telemetry",
                dataset_id="dataset-a",
                batch_id=f"dataset-a-batch-{index}",
                idempotency_key=f"dataset-a:{index}",
                content={"dataset": "dataset-a", "index": index},
                created_at=created_at,
            )
        seed_queue.enqueue(
            session_id="session-1",
            group_id="telemetry",
            dataset_id="dataset-b",
            batch_id="dataset-b-batch-1",
            idempotency_key="dataset-b:1",
            content={"dataset": "dataset-b", "index": 1},
            created_at=created_at,
        )

        monkeypatch.setattr(
            federation_node_module,
            "SQLiteOutbox",
            lambda _database: outbox,
        )
        monkeypatch.setattr(
            federation_node_module,
            "RelayRecorderStorageClient",
            lambda *_args, **_kwargs: storage_client,
        )

        node = RecorderFederationNode.__new__(RecorderFederationNode)
        node.data_directory = tmp_path
        node.source_names = ("dataset-a", "dataset-b")
        node.requested_storage_group = None
        node.request_timeout = 1.0
        node.publication_poll_seconds = 60.0
        node.runtime = _Runtime()
        node._lock = threading.RLock()
        node._stop = threading.Event()
        node._publication_future = Future()
        node._snapshot = RecorderFederationSnapshot(
            status="connected",
            node_id="node-recorder",
            federation_id="federation-1",
            session_id="session-1",
            storage_state="discovering",
            jsonl_state="discovering",
        )

        async def announce_connected(_state) -> None:
            return None

        async def publish_jsonl(
            _state,
            *,
            authority_node_id: str,
            group_id: str,
        ):
            assert authority_node_id == "node-owner"
            assert group_id == "telemetry"
            if jsonl_failure:
                raise FederationOperationError(
                    "test-jsonl-failure",
                    "the synthetic JSONL publication failed",
                )
            return SimpleNamespace(published_chunks=0)

        node._announce_connected = announce_connected
        node._publish_jsonl_once = publish_jsonl
        state = SimpleNamespace(
            binding=SimpleNamespace(
                internal_session_id="session-1",
                device_id="node-recorder",
            )
        )
        outbox.forbidden_thread_id = threading.get_ident()
        runner = asyncio.create_task(node._publication_loop(state))
        try:
            await asyncio.wait_for(storage_client.first_a_started.wait(), timeout=1.0)
            readiness = (
                None
                if jsonl_failure
                else asyncio.create_task(
                    asyncio.to_thread(
                        node.wait_until_sharing_ready,
                        timeout_seconds=2.0,
                    )
                )
            )
            storage_client.release_first_a.set()
            if readiness is not None:
                snapshot = await asyncio.wait_for(readiness, timeout=3.0)
            else:
                deadline = asyncio.get_running_loop().time() + 2.0
                while node.snapshot().status != "retrying":
                    if runner.done():
                        await runner
                    if asyncio.get_running_loop().time() >= deadline:
                        pytest.fail("publication failure snapshot was not reported")
                    await asyncio.sleep(0.01)
                snapshot = node.snapshot()

            assert storage_client.batch_ids == [
                "dataset-a-batch-1",
                "dataset-b-batch-1",
            ]
            if jsonl_failure:
                assert snapshot.status == "retrying"
                assert snapshot.storage_state == "backlogged"
                assert snapshot.jsonl_state == "backlogged"
                assert snapshot.pending_batches == 3
                assert snapshot.last_error_code == "test-jsonl-failure"
            else:
                assert snapshot.storage_state == "publishing"
                assert snapshot.storage_group == "telemetry"
                assert snapshot.last_committed_count == 2
                assert snapshot.jsonl_state == "ready"
            assert len(outbox.pending_thread_ids) >= 3
            assert all(
                thread_id != outbox.forbidden_thread_id
                for thread_id in outbox.pending_thread_ids
            )
        finally:
            node._stop.set()
            runner.cancel()
            with suppress(asyncio.CancelledError):
                await runner
            node._publication_future.cancel()

        assert storage_client.closed is True

    asyncio.run(scenario())


def test_publication_cycle_status_is_session_scoped_and_failure_aware() -> None:
    old_session = SimpleNamespace(
        session_id="session-old",
        schema_id=RECORDER_STORAGE_SCHEMA,
        destination_id="group-current",
        last_error="old failure",
    )
    current_failure = SimpleNamespace(
        session_id="session-current",
        schema_id=RECORDER_STORAGE_SCHEMA,
        destination_id="group-current",
        last_error="storage rejected",
    )
    old_group_failure = SimpleNamespace(
        session_id="session-current",
        schema_id=RECORDER_STORAGE_SCHEMA,
        destination_id="group-old",
        last_error="old group rejected",
    )

    assert _publication_cycle_status(
        pending_entries=(old_session,),
        session_id="session-current",
        group_id="group-current",
        delivery=RecorderDeliveryRunResult(attempted=0, committed=0, pending=0),
    ) == ("up-to-date", 0, None)
    assert _publication_cycle_status(
        pending_entries=(old_session, current_failure),
        session_id="session-current",
        group_id="group-current",
        delivery=RecorderDeliveryRunResult(attempted=1, committed=0, pending=1),
    ) == ("backlogged", 1, "recorder-delivery-pending")

    current_backlog = SimpleNamespace(
        session_id="session-current",
        schema_id=RECORDER_STORAGE_SCHEMA,
        destination_id="group-current",
        last_error=None,
    )
    assert _publication_cycle_status(
        pending_entries=(old_group_failure, current_backlog),
        session_id="session-current",
        group_id="group-current",
        delivery=RecorderDeliveryRunResult(attempted=1, committed=1, pending=0),
    ) == ("publishing", 1, None)
