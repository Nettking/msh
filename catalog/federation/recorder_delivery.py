"""Durable recorder-to-logical-storage delivery for Phase D."""

from __future__ import annotations

import asyncio
import inspect
import logging
import sqlite3
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Protocol

from .errors import FederationValidationError
from .outbox import OutboxEntry, SQLiteOutbox
from .phase_d_client import PhaseDIngestOutcome
from .storage_protocol import BatchIngestRequest

RECORDER_STORAGE_SCHEMA = "fcp.recorder.storage_delivery.v1"
_LOGGER = logging.getLogger(__name__)


def _storage_error_code(error: BaseException) -> str:
    """Return a bounded error identifier without persisting exception text."""
    sqlite_name = getattr(error, "sqlite_errorname", None)
    if (
        isinstance(sqlite_name, str)
        and sqlite_name.startswith("SQLITE_")
        and sqlite_name.isascii()
        and sqlite_name.replace("_", "").isalnum()
    ):
        return sqlite_name
    error_number = getattr(error, "errno", None)
    if isinstance(error_number, int) and not isinstance(error_number, bool):
        return f"errno-{error_number}"
    return type(error).__name__


#: Payload fields a durable row must carry before a request can even be built.
_REQUIRED_PAYLOAD_FIELDS = (
    "group_id",
    "dataset_id",
    "batch_id",
    "idempotency_key",
    "content",
    "created_at",
)


class RecorderPayloadDefect(Exception):
    """A durable row cannot be turned into a delivery request at all.

    This is deliberately a different kind of failure from "delivery did not
    succeed".  It is decided before any network call, purely from the row's own
    payload, so it is a property of the stored evidence rather than of the
    remote authority, the transport or the disk.  The same row fails the same
    way on every future attempt, on every restart, forever -- which is exactly
    the condition that makes unbounded retrying pointless and makes durable
    isolation the honest answer.

    ``reason`` is the bounded token persisted on the tombstone.
    """

    def __init__(self, reason: str, message: str) -> None:
        self.reason = reason
        self.message = message
        super().__init__(f"{message} ({reason})")


def _delivery_request(payload: object) -> dict[str, object]:
    """Build one ingest request from a durable payload, or name the defect.

    Pure and total with respect to the payload: no clock, no filesystem, no
    network.  That is what lets the caller re-run it against the freshly
    re-read durable row inside the retirement transaction and prove the defect
    is still there.

    It raises on exactly the conditions the delivery path already failed on, so
    no row that used to be deliverable becomes a defect here.
    """

    if not isinstance(payload, dict):
        raise RecorderPayloadDefect(
            "payload-not-an-object",
            "durable delivery payload is not an object",
        )
    for field in _REQUIRED_PAYLOAD_FIELDS:
        if field not in payload:
            raise RecorderPayloadDefect(
                "payload-field-missing",
                f"durable delivery payload has no {field}",
            )
    try:
        created_at = datetime.fromisoformat(
            str(payload["created_at"]).replace("Z", "+00:00")
        )
        dataset_schema_version = int(payload.get("dataset_schema_version", 1))
    except (TypeError, ValueError) as exc:
        raise RecorderPayloadDefect(
            "payload-field-invalid",
            f"durable delivery payload cannot be decoded: {exc}",
        ) from exc
    return {
        "group_id": str(payload["group_id"]),
        "dataset_id": str(payload["dataset_id"]),
        "dataset_schema_name": str(
            payload.get("dataset_schema_name", "fcp.storage.dataset.opaque")
        ),
        "dataset_schema_version": dataset_schema_version,
        "batch_id": str(payload["batch_id"]),
        "idempotency_key": str(payload["idempotency_key"]),
        "content": payload["content"],
        "created_at": created_at,
    }


def _is_still_defective(entry: OutboxEntry) -> bool:
    """Whether the durable row still cannot produce a request."""

    try:
        _delivery_request(entry.payload)
    except RecorderPayloadDefect:
        return True
    return False


def _now() -> datetime:
    return datetime.now(timezone.utc)


class RecorderStorageClient(Protocol):
    async def ingest_batch(
        self,
        *,
        group_id: str,
        dataset_id: str,
        batch_id: str,
        idempotency_key: str,
        content: object,
        created_at: datetime,
        dataset_schema_name: str = "fcp.storage.dataset.opaque",
        dataset_schema_version: int = 1,
    ) -> PhaseDIngestOutcome: ...


@dataclass(frozen=True)
class RecorderDeliveryProgress:
    """A durable delivery boundary reached during one bounded cycle.

    The progress signal is emitted only after the outbox acknowledgement has
    committed.  It is intentionally small: the owner needs to know that a
    route has made durable forward progress, while the outbox remains the
    source of truth for every row and its ordering.
    """

    committed: int
    dataset_id: str | None = None
    pending: int = 0


RecorderDeliveryProgressObserver = Callable[
    [RecorderDeliveryProgress], Awaitable[None] | None
]


@dataclass(frozen=True)
class RecorderDeliveryRunResult:
    attempted: int
    committed: int
    pending: int
    # Datasets whose oldest row did not commit this cycle, so their newer rows
    # stayed fenced behind it. Recomputed from durable rows every cycle rather
    # than stored, so it can never go stale or need its own retirement. This is
    # how a stuck historical item is surfaced while it is still being retried.
    blocked_datasets: tuple[str, ...] = ()
    # Rows withdrawn from delivery during this cycle, and the datasets whose
    # ordering they punched a hole in. Both describe this cycle only; the
    # authoritative, restart-surviving answer is the durable tombstone set,
    # which health surfaces read from the outbox rather than from here.
    retired: int = 0
    retired_datasets: tuple[str, ...] = ()


class DurableRecorderDeliveryQueue:
    """Retain recorder batches locally until the remote policy reports commit."""

    def __init__(
        self,
        *,
        outbox: SQLiteOutbox,
        client: RecorderStorageClient,
        session_id: str,
        destination_id: str | None = None,
        clock=_now,
    ) -> None:
        if not isinstance(session_id, str) or not session_id.strip():
            raise FederationValidationError(
                "invalid-recorder-delivery",
                "session_id",
                "must identify the authenticated Federation session",
            )
        self.outbox = outbox
        self.client = client
        self.session_id = session_id.strip()
        if destination_id is not None and (
            not isinstance(destination_id, str) or not destination_id.strip()
        ):
            raise FederationValidationError(
                "invalid-recorder-delivery",
                "destination_id",
                "must identify a logical storage group when supplied",
            )
        self.destination_id = (
            destination_id.strip() if isinstance(destination_id, str) else None
        )
        self.clock = clock
        self._startup_probe_available = True

    def enqueue(
        self,
        *,
        session_id: str,
        group_id: str,
        dataset_id: str,
        batch_id: str,
        idempotency_key: str,
        content: object,
        created_at: datetime,
        dataset_schema_name: str = "fcp.storage.dataset.opaque",
        dataset_schema_version: int = 1,
    ):
        if session_id != self.session_id:
            raise FederationValidationError(
                "recorder-session-mismatch",
                "session_id",
                "must match the queue's authenticated Federation session",
            )
        if self.destination_id is not None and group_id != self.destination_id:
            raise FederationValidationError(
                "recorder-destination-mismatch",
                "group_id",
                "must match the queue's selected logical storage group",
            )
        content_hash = BatchIngestRequest.calculate_content_hash(content)
        return self.outbox.enqueue(
            session_id=session_id,
            destination_id=group_id,
            schema_id=RECORDER_STORAGE_SCHEMA,
            payload={
                "group_id": group_id,
                "dataset_id": dataset_id,
                "dataset_schema_name": dataset_schema_name,
                "dataset_schema_version": dataset_schema_version,
                "batch_id": batch_id,
                "idempotency_key": idempotency_key,
                "content": content,
                "created_at": created_at.isoformat(),
            },
            idempotency_key=idempotency_key,
            content_hash=content_hash,
            now=self.clock(),
        )

    @property
    def startup_probe_pending(self) -> bool:
        """Whether this queue still owes its first bounded route probe.

        A new queue object is a new process/runtime delivery session, and its
        first pass deliberately retries one deferred head per ordered dataset
        so an outage that has since been repaired is proven quickly rather
        than waited out. The worker asks this to decide whether a backlog
        that is not yet due is still worth deferring reconciliation for.
        """

        return self._startup_probe_available

    @staticmethod
    def _ordering_key(entry) -> tuple[str, str, str] | None:
        payload = entry.payload
        if not isinstance(payload, dict):
            return None
        dataset_id = payload.get("dataset_id")
        if not isinstance(dataset_id, str) or not dataset_id:
            return None
        return entry.session_id, entry.destination_id, dataset_id

    async def run_once(
        self,
        *,
        limit: int = 100,
        retry_deferred_heads: bool | None = None,
        progress_observer: RecorderDeliveryProgressObserver | None = None,
    ) -> RecorderDeliveryRunResult:
        if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
            raise FederationValidationError(
                "invalid-limit",
                "limit",
                "must be a positive integer",
            )
        if retry_deferred_heads is not None and not isinstance(
            retry_deferred_heads, bool
        ):
            raise FederationValidationError(
                "invalid-recorder-delivery",
                "retry_deferred_heads",
                "must be a boolean when supplied",
            )
        if progress_observer is not None and not callable(progress_observer):
            raise FederationValidationError(
                "invalid-recorder-delivery",
                "progress_observer",
                "must be callable when supplied",
            )

        now = self.clock()
        # Read only a bounded, fair delivery window. The SQLite implementation
        # includes the oldest row for every ordered dataset before filling the
        # remaining window, prioritizing due heads before backoff-delayed heads
        # when there are more datasets than the window can hold. A bad head
        # still fences only its own dataset. Keep the blocking query and bounded
        # row decoding off the relay event loop.
        pending_for_delivery = getattr(self.outbox, "pending_for_delivery", None)
        if callable(pending_for_delivery):
            pending_snapshot = await asyncio.to_thread(
                pending_for_delivery,
                session_id=self.session_id,
                destination_id=self.destination_id,
                schema_id=RECORDER_STORAGE_SCHEMA,
                limit=limit,
                now=now,
            )
        else:
            # Compatibility for small test/durable-store adapters that expose
            # only the original outbox protocol. The installed SQLite outbox
            # always takes the bounded path above.
            pending_snapshot = await asyncio.to_thread(self.outbox.pending)

        # A new queue object is a new process/runtime delivery session. Its
        # automatic first pass is a bounded route proof: try no more than the
        # oldest row of each ordered dataset, including one row whose durable
        # backoff is not yet due. This prevents one large dataset from occupying
        # the whole startup cycle before the recorder can report a proven route,
        # while later passes retain the configured backlog-drain throughput.
        startup_probe = (
            retry_deferred_heads is None and self._startup_probe_available
        )
        if retry_deferred_heads is None:
            retry_deferred_heads = startup_probe
        self._startup_probe_available = False

        # Preserve recorder sequence order independently per logical dataset.
        # A failed or not-yet-due older entry fences newer entries for that same
        # session/group/dataset until the older entry commits. Other datasets
        # remain free to make progress.
        pending_entries = tuple(
            sorted(
                (
                    entry
                    for entry in pending_snapshot
                    if entry.schema_id == RECORDER_STORAGE_SCHEMA
                    # The client is authenticated for exactly this session.
                    # Rows from prior sessions remain durable for a worker
                    # holding the matching authority; they are never sent here.
                    and entry.session_id == self.session_id
                    # A relay client targets the authority selected for exactly
                    # one logical group. Rows for a prior group remain durable
                    # until a worker is started for that destination.
                    and (
                        self.destination_id is None
                        or entry.destination_id == self.destination_id
                    )
                ),
                key=lambda entry: entry.outbox_id,
            )
        )
        blocked: set[tuple[str, str, str]] = set()
        deferred_retry_used: set[tuple[str, str, str]] = set()
        startup_probe_used: set[tuple[str, str, str]] = set()
        retired_datasets: set[str] = set()
        attempted = 0
        committed = 0
        pending = 0
        retired = 0

        for entry in pending_entries:
            if attempted >= limit:
                break
            ordering_key = self._ordering_key(entry)
            if ordering_key is not None and ordering_key in blocked:
                continue
            if startup_probe and ordering_key is not None:
                if ordering_key in startup_probe_used:
                    continue
                startup_probe_used.add(ordering_key)
            if entry.next_attempt_at > now:
                # Exponential backoff survives process restarts. That is correct
                # during an unchanged outage, but after an operator has upgraded
                # or repaired the remote authority it can leave a zero-touch
                # recorder waiting an hour before proving the new path. On the
                # queue's first pass, allow exactly the oldest deferred row of
                # each ordered dataset one immediate attempt. No timestamp,
                # attempt counter, error, payload, or later row is rewritten.
                if (
                    not retry_deferred_heads
                    or ordering_key is None
                    or ordering_key in deferred_retry_used
                ):
                    if ordering_key is not None:
                        blocked.add(ordering_key)
                    continue
                deferred_retry_used.add(ordering_key)

            attempted += 1
            failed = False
            # The network call is the boundary between "this row is wrong" and
            # "the world is wrong". Everything above it is a pure function of
            # the durable payload; everything at or below it depends on the
            # remote authority, the transport and the disk. Only the first kind
            # can ever justify withdrawing evidence from delivery, so the two
            # are separated here rather than being caught together.
            try:
                request = _delivery_request(entry.payload)
            except RecorderPayloadDefect as defect:
                if self._withdraw(entry, defect, ordering_key):
                    retired += 1
                    if ordering_key is not None:
                        retired_datasets.add(ordering_key[2])
                    # The fence this row held is released with it: the dataset
                    # is deliberately *not* marked blocked, so newer evidence
                    # behind it moves in this same cycle.
                    continue
                # The withdrawal itself did not commit. The row is exactly as
                # it was -- durable, pending, fenced and retried -- which is the
                # safe outcome, never a silent drop.
                pending += 1
                failed = True
            else:
                # Build diagnostic context only after validating the decoded
                # durable payload. SQLiteOutbox can contain any JSON value;
                # malformed values must be retired without aborting the cycle.
                entry_context = {
                    "storage_outbox_id": entry.outbox_id,
                    "storage_batch_id": request["batch_id"],
                    "storage_dataset_id": request["dataset_id"],
                    "storage_session_id": entry.session_id,
                    "storage_destination_id": entry.destination_id,
                    "storage_content_sha256": entry.content_hash,
                }
                try:
                    outcome = await self.client.ingest_batch(**request)
                except (
                    FederationValidationError,
                    KeyError,
                    TypeError,
                    ValueError,
                    OSError,
                    RuntimeError,
                    sqlite3.Error,
                ) as exc:
                    # Storage/database/transport failure. It stays pending and
                    # retryable however long it persists; time and attempt count
                    # never promote it to a terminal state, and it is never
                    # reported as a commit.
                    self.outbox.record_failure(
                        entry.outbox_id,
                        error=str(exc).strip() or type(exc).__name__,
                        now=self.clock(),
                    )
                    _LOGGER.error(
                        "recorder delivery attempt failed",
                        extra={
                            "storage_stage": "delivery_attempt_failed",
                            **entry_context,
                            "storage_error_code": _storage_error_code(exc),
                            "storage_exception_type": type(exc).__name__,
                        },
                    )
                    pending += 1
                    failed = True
                else:
                    if outcome.committed:
                        acknowledged = False
                        try:
                            self.outbox.acknowledge(
                                entry.outbox_id, now=self.clock()
                            )
                        except (sqlite3.Error, OSError) as exc:
                            try:
                                current = self.outbox.get(entry.outbox_id)
                            except (sqlite3.Error, OSError) as state_error:
                                _LOGGER.error(
                                    "recorder outbox acknowledgement state unavailable",
                                    extra={
                                        "storage_stage": "delivery_ack_state_unavailable",
                                        **entry_context,
                                        "storage_error_code": _storage_error_code(exc),
                                        "storage_ack_exception_type": type(exc).__name__,
                                        "storage_state_exception_type": type(state_error).__name__,
                                    },
                                )
                                raise exc from state_error

                            state = (
                                None
                                if current is None
                                else getattr(current.state, "value", current.state)
                            )
                            same_identity = (
                                current is not None
                                and current.outbox_id == entry.outbox_id
                                and current.session_id == entry.session_id
                                and current.destination_id == entry.destination_id
                                and current.schema_id == entry.schema_id
                                and current.idempotency_key == entry.idempotency_key
                                and current.content_hash == entry.content_hash
                            )
                            if state == "completed" and same_identity:
                                acknowledged = True
                                _LOGGER.warning(
                                    "recorder outbox acknowledgement raised after durable completion",
                                    extra={
                                        "storage_stage": "delivery_ack_error_but_completed",
                                        **entry_context,
                                        "storage_error_code": _storage_error_code(exc),
                                        "storage_exception_type": type(exc).__name__,
                                    },
                                )
                            elif state == "pending" and same_identity:
                                try:
                                    self.outbox.record_failure(
                                        entry.outbox_id,
                                        error=f"sqlite3.{type(exc).__name__}",
                                        now=self.clock(),
                                    )
                                except (sqlite3.Error, OSError) as persist_error:
                                    _LOGGER.error(
                                        "recorder acknowledgement failure could not be persisted",
                                        extra={
                                            "storage_stage": "delivery_ack_failure_not_persisted",
                                            **entry_context,
                                            "storage_error_code": _storage_error_code(exc),
                                            "storage_ack_exception_type": type(exc).__name__,
                                            "storage_persist_exception_type": (
                                                type(persist_error).__name__
                                            ),
                                        },
                                    )
                                    raise
                                failed = True
                                pending += 1
                                _LOGGER.error(
                                    "recorder outbox acknowledgement failed; row remains retryable",
                                    extra={
                                        "storage_stage": "delivery_ack_failed",
                                        **entry_context,
                                        "storage_error_code": _storage_error_code(exc),
                                        "storage_exception_type": type(exc).__name__,
                                        "storage_retry_state_persisted": True,
                                    },
                                )
                            else:
                                _LOGGER.error(
                                    "recorder outbox acknowledgement has unexpected durable state",
                                    extra={
                                        "storage_stage": "delivery_ack_state_unexpected",
                                        **entry_context,
                                        "storage_row_state": state,
                                        "storage_identity_matches": same_identity,
                                        "storage_error_code": _storage_error_code(exc),
                                        "storage_exception_type": type(exc).__name__,
                                    },
                                )
                                raise FederationValidationError(
                                    "outbox-acknowledgement-state-unknown",
                                    "outbox_id",
                                    "durable row state or identity does not match the attempted batch",
                                )
                        else:
                            acknowledged = True

                        if acknowledged:
                            committed += 1
                            if progress_observer is not None:
                                try:
                                    progress = progress_observer(
                                        RecorderDeliveryProgress(
                                            committed=committed,
                                            dataset_id=(
                                                None
                                                if ordering_key is None
                                                else ordering_key[2]
                                            ),
                                            pending=pending,
                                        )
                                    )
                                    if inspect.isawaitable(progress):
                                        await progress
                                except Exception as exc:  # noqa: BLE001 - observer cannot undo durable ack
                                    _LOGGER.error(
                                        "recorder delivery progress observer failed "
                                        "after durable commit",
                                        extra={
                                            "storage_stage": "delivery_progress_observer_failed",
                                            "storage_committed_count": committed,
                                            "storage_exception_type": type(exc).__name__,
                                        },
                                    )
                    else:
                        self.outbox.record_failure(
                            entry.outbox_id,
                            error=(
                                outcome.message
                                or "storage acknowledgement is pending"
                            ),
                            now=self.clock(),
                        )
                        pending += 1
                        failed = True

            if failed and ordering_key is not None:
                blocked.add(ordering_key)

        return RecorderDeliveryRunResult(
            attempted,
            committed,
            pending,
            tuple(sorted({key[2] for key in blocked})),
            retired,
            tuple(sorted(retired_datasets)),
        )

    def _withdraw(
        self,
        entry,
        defect: RecorderPayloadDefect,
        ordering_key: tuple[str, str, str] | None,
    ) -> bool:
        """Durably retire one deterministically undeliverable row.

        The cause is recorded first and the withdrawal second, as two separate
        durable transactions. A crash between them leaves an ordinary pending
        row that has recorded why it failed; the next cycle re-derives the same
        defect from the same payload and retires it then. A crash inside either
        transaction rolls that transaction back. There is no ordering of these
        two writes that can lose the row or fabricate a delivery.

        The retirement re-checks the defect against the freshly re-read durable
        row inside the transaction, so a row repaired underneath this cycle is
        not retired on the strength of a stale snapshot.
        """

        try:
            self.outbox.record_failure(
                entry.outbox_id,
                error=defect.message,
                now=self.clock(),
            )
            self.outbox.retire(
                entry.outbox_id,
                reason=defect.reason,
                dataset_id=None if ordering_key is None else ordering_key[2],
                now=self.clock(),
                verify=_is_still_defective,
            )
            return True
        except (FederationValidationError, OSError, RuntimeError):
            # Could not withdraw -- for instance the row was completed by
            # another worker, its retirement metadata is corrupt, or the
            # database is unavailable. Leaving it pending is always safe.
            return False
