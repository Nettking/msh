"""A durable-store fault must not silently retire the recorder publication driver.

`RecorderFederationNode._publication_loop` is the standalone recorder's only
route from locally committed capture to Federation logical storage. It runs as
a detached coroutine whose future nobody reads after startup, so anything the
per-cycle handler does not classify ends the loop for the life of the process
without a log line, a snapshot change, or a restart.

`SQLiteOutbox` raises `sqlite3.Error` -- not `OSError` -- when the durable
outbox cannot be read or written: a full or failing recorder disk, a database
locked past its busy timeout, or a malformed image after an unclean stop. These
tests hold the externally meaningful outcome: evidence that is already durable
locally must still reach the Federation once the store recovers, and while it
cannot, the recorder's published Federation health must not keep reporting the
last good publication cycle.
"""

from __future__ import annotations

import asyncio
import sqlite3
import threading
from concurrent.futures import Future
from contextlib import suppress
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from catalog.federation.errors import FederationValidationError
from catalog.federation.local_storage import FilesystemBatchStorageProvider
from catalog.federation.outbox import SQLiteOutbox
from catalog.federation.phase_d_client import PhaseDIngestOutcome
from catalog.federation.recorder_delivery import DurableRecorderDeliveryQueue
from catalog.federation.storage_protocol import (
    BatchIngestRequest,
    BatchIngestState,
    WriteAuthority,
)
from catalog.mtconnect_recorder import federation_node as federation_node_module
from catalog.mtconnect_recorder.federation_node import (
    LEGACY_RECORDER_CAPABILITY_ID,
    RecorderFederationNode,
    RecorderFederationSnapshot,
    recorder_capability_id,
    sharing_state_detail,
)

SESSION = "session-1"
GROUP = "telemetry"
OWNER = "node-owner"
CREATED_AT = datetime(2026, 8, 26, 9, 30, tzinfo=timezone.utc)


def _status() -> dict[str, object]:
    return {
        "sessions": [{"session_id": SESSION, "created_by_node_id": OWNER}],
        "capabilities": [
            {
                "capability_id": "logical-storage-authority",
                "node_id": OWNER,
                "session_id": SESSION,
                "type": "storage-control",
                "protocol": "fcp.storage-control",
                "protocol_version": "1",
                "status": "ready",
                "properties": {
                    "kind": "recorder-logical-storage-authority",
                    "group_ids": [GROUP],
                },
            }
        ],
    }


class _FaultyOutbox(SQLiteOutbox):
    """A durable outbox whose store can be made to fail like a real one.

    ``read_fault`` stands in for whatever ``pending()`` can actually raise:
    ``sqlite3.Error`` from the store itself, and ``FederationValidationError``
    (``malformed-outbox-row``) from its own row decoding.
    """

    def __init__(self, database) -> None:
        super().__init__(database)
        self.read_fault: BaseException | None = None
        self.acknowledge_fault: BaseException | None = None
        self.acknowledged: list[int] = []

    def pending(self, *args, **kwargs):
        if self.read_fault is not None:
            raise self.read_fault
        return super().pending(*args, **kwargs)

    def acknowledge(self, outbox_id: int, **kwargs):
        if self.acknowledge_fault is not None:
            raise self.acknowledge_fault
        self.acknowledged.append(int(outbox_id))
        return super().acknowledge(outbox_id, **kwargs)


class _StatusClient:
    async def coordinator_status(self):
        return _status()


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
        self.closed = False

    async def start(self) -> None:
        return None

    async def close(self) -> None:
        self.closed = True

    async def ingest_batch(self, **kwargs):
        self.batch_ids.append(str(kwargs["batch_id"]))
        return PhaseDIngestOutcome(committed=True)


class _ProviderBackedStorageClient:
    """Deliver into the real storage provider instead of asserting on a fake.

    The recorder's at-least-once outbox is only half of the exactly-once
    property; the other half is the provider's idempotent commit. Counting
    offers against a fake proves neither. This drives the production
    ``FilesystemBatchStorageProvider.ingest`` so a redelivery has to be
    resolved by the real ``(session_id, group_id, idempotency_key)`` path.
    """

    def __init__(self, provider: FilesystemBatchStorageProvider) -> None:
        self.provider = provider
        self.batch_ids: list[str] = []
        self.states: list[BatchIngestState] = []
        self.closed = False

    async def start(self) -> None:
        return None

    async def close(self) -> None:
        self.closed = True

    async def ingest_batch(
        self,
        *,
        group_id: str,
        dataset_id: str,
        batch_id: str,
        idempotency_key: str,
        content: object,
        created_at,
        dataset_schema_name: str = "fcp.storage.dataset.opaque",
        dataset_schema_version: int = 1,
    ):
        self.batch_ids.append(batch_id)
        request = BatchIngestRequest(
            authority=WriteAuthority(
                session_id=SESSION,
                group_id=group_id,
                actor_node_id=OWNER,
                grant_id="grant-1",
                term=1,
                fencing_token=1,
                lease_expires_at=CREATED_AT + timedelta(hours=1),
            ),
            dataset_id=dataset_id,
            batch_id=batch_id,
            idempotency_key=idempotency_key,
            content_hash=BatchIngestRequest.calculate_content_hash(content),
            content=content,
            created_at=created_at,
            dataset_schema_name=dataset_schema_name,
            dataset_schema_version=dataset_schema_version,
        )
        result = self.provider.ingest(request)
        self.states.append(result.state)
        return PhaseDIngestOutcome(committed=True, result=result)


def _node(tmp_path, outbox, storage_client, monkeypatch):
    monkeypatch.setattr(
        federation_node_module, "SQLiteOutbox", lambda _database: outbox
    )
    monkeypatch.setattr(
        federation_node_module,
        "RelayRecorderStorageClient",
        lambda *_args, **_kwargs: storage_client,
    )
    node = RecorderFederationNode.__new__(RecorderFederationNode)
    node.data_directory = tmp_path
    node.source_names = ("dataset-a",)
    node.requested_storage_group = None
    node.request_timeout = 1.0
    node.publication_poll_seconds = 0.01
    node.runtime = _Runtime()
    node._lock = threading.RLock()
    node._stop = threading.Event()
    node._publication_future = Future()
    node._snapshot = RecorderFederationSnapshot(
        status="connected",
        node_id="node-recorder",
        federation_id="federation-1",
        session_id=SESSION,
        storage_state="discovering",
        jsonl_state="discovering",
    )

    async def announce_connected(_state) -> None:
        return None

    async def publish_jsonl(_state, *, authority_node_id: str, group_id: str):
        return SimpleNamespace(published_chunks=0)

    node._announce_connected = announce_connected
    node._publish_jsonl_once = publish_jsonl
    return node


def _state():
    return SimpleNamespace(
        binding=SimpleNamespace(
            internal_session_id=SESSION,
            device_id="node-recorder",
        )
    )


def _seed(outbox, storage_client, *, index: int) -> None:
    DurableRecorderDeliveryQueue(
        outbox=outbox,
        client=storage_client,
        session_id=SESSION,
        destination_id=GROUP,
    ).enqueue(
        session_id=SESSION,
        group_id=GROUP,
        dataset_id="dataset-a",
        batch_id=f"dataset-a-batch-{index}",
        idempotency_key=f"dataset-a:{index}",
        content={"dataset": "dataset-a", "index": index},
        created_at=CREATED_AT,
    )


def _driver_state(runner) -> str:
    """Describe the publication driver for an assertion message."""

    if not runner.done():
        return "publication driver still running"
    if runner.cancelled():
        return "publication driver cancelled"
    failure = runner.exception()
    if failure is None:
        return "publication driver returned"
    return f"publication driver died on {type(failure).__name__}"


async def _until(predicate, runner, *, timeout: float = 5.0) -> bool:
    """Wait for a condition, giving up as soon as the driver can no longer meet it."""

    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if runner.done() or loop.time() >= deadline:
            return predicate()
        await asyncio.sleep(0.01)
    return True


def _quiesce(node, runner):
    """Stop the driver without letting its fault replace an assertion failure."""

    node._stop.set()
    runner.cancel()
    node._publication_future.cancel()


def test_durable_store_failure_does_not_strand_recorder_evidence(
    tmp_path, monkeypatch
) -> None:
    """Evidence committed before the fault must publish once the store returns."""

    async def scenario() -> None:
        storage_client = _StorageClient()
        outbox = _FaultyOutbox(tmp_path / "outbox.sqlite3")
        node = _node(tmp_path, outbox, storage_client, monkeypatch)
        _seed(outbox, storage_client, index=1)

        runner = asyncio.create_task(node._publication_loop(_state()))
        try:
            assert await _until(
                lambda: node.snapshot().storage_state == "up-to-date",
                runner,
            ), f"the first healthy publication cycle never completed ({_driver_state(runner)})"
            assert storage_client.batch_ids == ["dataset-a-batch-1"]

            # The recorder disk starts refusing durable reads while capture
            # keeps committing locally.
            outbox.read_fault = sqlite3.OperationalError(
                "database or disk is full"
            )
            _seed(outbox, storage_client, index=2)
            await asyncio.sleep(0.2)

            # The store recovers. No process restart, no operator action.
            outbox.read_fault = None

            assert await _until(
                lambda: storage_client.batch_ids
                == ["dataset-a-batch-1", "dataset-a-batch-2"],
                runner,
            ), (
                "recorder evidence committed during the durable-store fault "
                "was never published after the store recovered "
                f"({_driver_state(runner)}); delivered={storage_client.batch_ids}"
            )
        finally:
            _quiesce(node, runner)
            with suppress(BaseException):
                await runner

    asyncio.run(scenario())


def test_durable_store_failure_is_not_reported_as_healthy_publication(
    tmp_path, monkeypatch
) -> None:
    """A recorder that cannot publish must not keep advertising the last good cycle."""

    async def scenario() -> None:
        storage_client = _StorageClient()
        outbox = _FaultyOutbox(tmp_path / "outbox.sqlite3")
        node = _node(tmp_path, outbox, storage_client, monkeypatch)
        _seed(outbox, storage_client, index=1)

        runner = asyncio.create_task(node._publication_loop(_state()))
        try:
            assert await _until(
                lambda: node.snapshot().storage_state == "up-to-date",
                runner,
            ), f"the first healthy publication cycle never completed ({_driver_state(runner)})"

            outbox.read_fault = sqlite3.OperationalError("disk I/O error")
            _seed(outbox, storage_client, index=2)

            assert await _until(
                lambda: node.snapshot().storage_state != "up-to-date",
                runner,
            ), (
                "the recorder kept publishing its last healthy storage state "
                "while its durable outbox could not be read "
                f"({_driver_state(runner)}); snapshot={node.snapshot()}"
            )
            snapshot = node.snapshot()
            assert snapshot.last_error_code == "OperationalError"
            assert snapshot.status == "retrying"
        finally:
            _quiesce(node, runner)
            with suppress(BaseException):
                await runner

    asyncio.run(scenario())


def test_failed_local_acknowledgement_commits_the_batch_exactly_once(
    tmp_path, monkeypatch
) -> None:
    """A durable ack fault must redeliver, and the provider must commit once.

    The remote ingest succeeds and the local acknowledgement then fails, so the
    row stays pending and the recovered driver offers it again. That is correct
    at-least-once delivery -- but only if the storage provider resolves the
    repeat idempotently. This exercises the real
    ``FilesystemBatchStorageProvider`` rather than a fake that returns
    ``committed=True`` unconditionally, so "exactly one commit" is a property
    of the durable catalogue and the stored batch files, not of the test double.
    """

    async def scenario() -> None:
        provider = FilesystemBatchStorageProvider(tmp_path / "storage")
        storage_client = _ProviderBackedStorageClient(provider)
        outbox = _FaultyOutbox(tmp_path / "outbox.sqlite3")
        outbox.acknowledge_fault = sqlite3.OperationalError(
            "attempt to write a readonly database"
        )
        node = _node(tmp_path, outbox, storage_client, monkeypatch)
        _seed(outbox, storage_client, index=1)

        runner = asyncio.create_task(node._publication_loop(_state()))
        try:
            assert await _until(
                lambda: len(storage_client.batch_ids) >= 1,
                runner,
            ), f"the batch was never offered to the storage authority ({_driver_state(runner)})"

            outbox.acknowledge_fault = None

            assert await _until(
                lambda: outbox.acknowledged != [], runner
            ), (
                "the delivered batch was never acknowledged after the durable "
                "store recovered, so it stays pending forever "
                f"({_driver_state(runner)})"
            )
            assert await _until(
                lambda: node.snapshot().storage_state == "up-to-date",
                runner,
            ), f"publication never returned to up-to-date ({_driver_state(runner)})"

            # The fault has to have produced an actual redelivery, or this test
            # would prove nothing about idempotency.
            assert len(storage_client.batch_ids) >= 2, (
                "the acknowledgement fault did not cause a redelivery, so the "
                f"idempotent commit path was never exercised: {storage_client.batch_ids}"
            )
            assert set(storage_client.batch_ids) == {"dataset-a-batch-1"}

            # Exactly one commit, proven at the provider rather than the fake.
            assert storage_client.states.count(BatchIngestState.STORED) == 1, (
                f"the batch was stored more than once: {storage_client.states}"
            )
            assert all(
                state is BatchIngestState.ALREADY_STORED
                for state in storage_client.states[1:]
            ), f"a redelivery was not resolved idempotently: {storage_client.states}"
            stored_files = sorted(provider.batch_root.rglob("*.json"))
            assert len(stored_files) == 1, (
                f"redelivery produced more than one stored batch: {stored_files}"
            )
            assert provider.read(
                session_id=SESSION, group_id=GROUP, batch_id="dataset-a-batch-1"
            ) == {"dataset": "dataset-a", "index": 1}
        finally:
            _quiesce(node, runner)
            with suppress(BaseException):
                await runner

    asyncio.run(scenario())


def test_malformed_outbox_row_keeps_the_driver_retrying(tmp_path, monkeypatch) -> None:
    """A retryable decode failure must not become a terminal driver death.

    ``SQLiteOutbox.pending()`` raises ``FederationValidationError`` when a row
    fails to decode, and the per-cycle boundary already treats that as
    retryable. The failure handler then re-reads the same outbox. If that
    second read were guarded more narrowly than the first, the retryable
    condition would escape to the terminal boundary and kill the driver -- so
    this holds the whole round trip: alive and retrying while the fault
    persists, publishing again once it clears, with no process restart.
    """

    async def scenario() -> None:
        storage_client = _StorageClient()
        outbox = _FaultyOutbox(tmp_path / "outbox.sqlite3")
        node = _node(tmp_path, outbox, storage_client, monkeypatch)
        _seed(outbox, storage_client, index=1)

        runner = asyncio.create_task(node._publication_loop(_state()))
        try:
            assert await _until(
                lambda: node.snapshot().storage_state == "up-to-date",
                runner,
            ), f"the first healthy publication cycle never completed ({_driver_state(runner)})"

            outbox.read_fault = FederationValidationError(
                "malformed-outbox-row", "outbox", "persisted timestamps must be timezone-aware"
            )
            _seed(outbox, storage_client, index=2)

            assert await _until(
                lambda: node.snapshot().last_error_code == "malformed-outbox-row",
                runner,
            ), (
                "a retryable outbox decode failure was not reported as a "
                f"retrying cycle ({_driver_state(runner)}); "
                f"snapshot={node.snapshot()}"
            )
            assert not runner.done(), (
                "a retryable outbox decode failure ended the publication "
                f"driver ({_driver_state(runner)})"
            )
            assert node.snapshot().status == "retrying"
            # It must stay retryable, not die on a later cycle: the handler
            # re-reads the same failing outbox on every one of them.
            await asyncio.sleep(0.3)
            assert not runner.done(), (
                "the publication driver survived the first retryable decode "
                f"failure but not a later one ({_driver_state(runner)})"
            )

            outbox.read_fault = None

            assert await _until(
                lambda: storage_client.batch_ids
                == ["dataset-a-batch-1", "dataset-a-batch-2"],
                runner,
            ), (
                "recorder evidence committed during the retryable decode "
                "failure was never published after it cleared "
                f"({_driver_state(runner)}); delivered={storage_client.batch_ids}"
            )
        finally:
            _quiesce(node, runner)
            with suppress(BaseException):
                await runner

    asyncio.run(scenario())


def test_unopenable_outbox_at_startup_recovers_without_a_process_restart(
    tmp_path, monkeypatch
) -> None:
    """A store that cannot be opened yet must not strand the route half-built.

    The durable outbox is opened while the publication route is being built, so
    a locked, full, readonly or malformed SQLite file raises before a worker
    exists. Retrying that is only a recovery if the next cycle rebuilds the
    route: recording the authority and the relay client before the worker
    existed made the next cycle believe the route was already there, skip
    rebuilding it, and die -- reporting an `AssertionError` instead of the
    store fault that actually happened.
    """

    opens: list[int] = []

    async def scenario() -> None:
        storage_client = _StorageClient()
        real_outbox = _FaultyOutbox(tmp_path / "outbox.sqlite3")
        _seed(real_outbox, storage_client, index=1)

        def flaky_outbox(_database):
            opens.append(1)
            if len(opens) <= 2:
                raise sqlite3.OperationalError("unable to open database file")
            return real_outbox

        node = _node(tmp_path, real_outbox, storage_client, monkeypatch)
        # _node installs a always-succeeding factory; replace it with one that
        # fails the way a real store does before it becomes available.
        monkeypatch.setattr(federation_node_module, "SQLiteOutbox", flaky_outbox)

        runner = asyncio.create_task(node._publication_loop(_state()))
        try:
            assert await _until(lambda: len(opens) >= 2, runner), (
                "the outbox was not reopened after the first failure, so the "
                f"route was never rebuilt ({_driver_state(runner)})"
            )
            assert not runner.done(), (
                "an unopenable durable outbox ended the publication driver "
                f"({_driver_state(runner)}); snapshot={node.snapshot()}"
            )
            assert node.snapshot().last_error_code == "OperationalError", (
                "the driver reported something other than the store fault: "
                f"{node.snapshot()}"
            )

            assert await _until(
                lambda: storage_client.batch_ids == ["dataset-a-batch-1"],
                runner,
            ), (
                "evidence was never published after the durable outbox became "
                f"openable ({_driver_state(runner)}); "
                f"delivered={storage_client.batch_ids}"
            )
            assert await _until(
                lambda: node.snapshot().storage_state == "up-to-date",
                runner,
            ), f"publication never reached up-to-date ({_driver_state(runner)})"
        finally:
            _quiesce(node, runner)
            with suppress(BaseException):
                await runner

    asyncio.run(scenario())


def test_an_unclassified_publication_fault_stops_advertising_health(
    tmp_path, monkeypatch
) -> None:
    """Secondary behaviour of the fix: no fault ends the driver in silence.

    An unclassified fault is still terminal -- inventing a retry for a fault the
    loop does not understand would be guessing. What must not survive it is the
    recorder continuing to advertise the last successful publication cycle to
    the Federation and to its own operator.
    """

    async def scenario() -> None:
        storage_client = _StorageClient()
        outbox = _FaultyOutbox(tmp_path / "outbox.sqlite3")
        node = _node(tmp_path, outbox, storage_client, monkeypatch)
        _seed(outbox, storage_client, index=1)

        runner = asyncio.create_task(node._publication_loop(_state()))
        try:
            assert await _until(
                lambda: node.snapshot().storage_state == "up-to-date",
                runner,
            ), f"the first healthy publication cycle never completed ({_driver_state(runner)})"

            outbox.read_fault = KeyError("unclassified-store-defect")

            assert await _until(lambda: runner.done(), runner), (
                "the unclassified fault did not reach the driver boundary"
            )
            snapshot = node.snapshot()
            assert snapshot.storage_state == "publication-driver-failed"
            assert snapshot.jsonl_state == "publication-driver-failed"
            assert snapshot.status == "failed"
            assert snapshot.last_error_code == "KeyError"
            # The fault is still reported to whoever does read the future.
            assert isinstance(runner.exception(), KeyError)
            # And the state reaches the operator with an action, not bare.
            assert "restart the recorder" in sharing_state_detail(
                snapshot.storage_state
            )
        finally:
            _quiesce(node, runner)
            with suppress(BaseException):
                await runner

    asyncio.run(scenario())


def test_capability_reconciliation_retries_before_publication_on_same_client(
    tmp_path, monkeypatch
) -> None:
    """One status timeout must not leave a duplicate READY recorder behind."""

    async def scenario() -> None:
        storage_client = _StorageClient()
        outbox = _FaultyOutbox(tmp_path / "outbox.sqlite3")
        node = _node(tmp_path, outbox, storage_client, monkeypatch)
        state = _state()
        scoped_id = recorder_capability_id(state.binding.device_id)
        # A reconnect can replay an old backup's legacy announcement alongside
        # the accepted scoped identity. Exercise the real reconciliation path,
        # with only the coordinator boundary replaced by local fixture state.
        status = _status()
        rows = {
            capability_id: node._announcement(state, capability_id).to_dict()
            for capability_id in (LEGACY_RECORDER_CAPABILITY_ID, scoped_id)
        }
        connected_client = node.runtime.client
        status_timeout_pending = True

        async def coordinator_status():
            nonlocal status_timeout_pending
            if status_timeout_pending:
                status_timeout_pending = False
                raise TimeoutError("one coordinator status response timed out")
            return {
                **status,
                "capabilities": [*status["capabilities"], *rows.values()],
            }

        async def announce_capability(_state, capability, *, request_id):
            rows[capability.capability_id] = capability.to_dict()
            return capability

        connected_client.coordinator_status = coordinator_status
        node.runtime._announce_capability = announce_capability
        del node._announce_connected
        _seed(outbox, storage_client, index=1)

        runner = asyncio.create_task(node._publication_loop(state))
        try:
            assert await _until(
                lambda: node.snapshot().last_error_code == "TimeoutError",
                runner,
            ), f"the transient status failure was not reported ({_driver_state(runner)})"
            assert node.snapshot().status == "retrying"
            assert storage_client.batch_ids == []

            assert await _until(
                lambda: node.snapshot().storage_state == "up-to-date",
                runner,
            ), f"publication did not recover from the timeout ({_driver_state(runner)})"
            assert node.runtime._connected_client() is connected_client
            assert rows[LEGACY_RECORDER_CAPABILITY_ID]["status"] == "unavailable", (
                "publication reported healthy after the timeout without "
                "retrying reconciliation of the duplicate legacy recorder"
            )
            assert rows[scoped_id]["status"] == "ready"
            assert storage_client.batch_ids == ["dataset-a-batch-1"]
        finally:
            _quiesce(node, runner)
            with suppress(BaseException):
                await runner

    asyncio.run(scenario())
