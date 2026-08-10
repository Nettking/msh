"""F7.5 worker cancellation and durable dispatch tombstones."""

from __future__ import annotations

import asyncio
from datetime import datetime
from pathlib import Path

from catalog.federation.errors import FederationValidationError

from .dispatch import (
    CapabilityWorker,
    DispatchEvent,
    DispatchRequest,
    DispatchResponse,
    DispatchState,
    ExecutionResult,
    SQLiteDispatchInbox,
)
from .job_store import _timestamp, _utc
from .lifecycle_contracts import (
    CancellationRequest,
    CancellationResponse,
    CancellationState,
)


class SQLiteLifecycleDispatchInbox(SQLiteDispatchInbox):
    """Pair F7.4 receipts with durable cancellation tombstones."""

    def __init__(self, database: Path | str) -> None:
        super().__init__(database)
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS capability_dispatch_cancellations (
                    session_id TEXT NOT NULL,
                    cancellation_id TEXT NOT NULL,
                    fingerprint TEXT NOT NULL,
                    job_id TEXT NOT NULL,
                    attempt_id TEXT NOT NULL,
                    provider_id TEXT NOT NULL,
                    lease_id TEXT NOT NULL,
                    lease_generation INTEGER NOT NULL
                        CHECK(lease_generation > 0),
                    response_json TEXT NOT NULL CHECK(json_valid(response_json)),
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY(session_id, cancellation_id)
                );
                CREATE INDEX IF NOT EXISTS dispatch_cancel_attempt
                    ON capability_dispatch_cancellations(
                        session_id, job_id, attempt_id, lease_generation
                    );
                """
            )

    def cancelled_for(self, request: DispatchRequest) -> bool:
        with self._connect() as connection:
            row = connection.execute(
                """SELECT 1 FROM capability_dispatch_cancellations
                   WHERE session_id=? AND job_id=? AND attempt_id=?
                     AND provider_id=? AND lease_id=? AND lease_generation=?""",
                (
                    request.session_id,
                    request.job.job_id,
                    request.attempt_id,
                    request.provider_id,
                    request.lease_id,
                    request.lease_generation,
                ),
            ).fetchone()
        return row is not None

    def record_cancellation(
        self,
        request: CancellationRequest,
        response: CancellationResponse,
        *,
        now: datetime,
    ) -> CancellationResponse:
        now = _utc(now, "now")
        response.validate_for(request)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    """SELECT * FROM capability_dispatch_cancellations
                       WHERE session_id=? AND cancellation_id=?""",
                    (request.session_id, request.cancellation_id),
                ).fetchone()
                if row is not None:
                    if row["fingerprint"] != request.fingerprint():
                        raise FederationValidationError(
                            "cancellation-id-conflict",
                            "cancellation_id",
                            "ID identifies different cancellation content",
                        )
                    replay = CancellationResponse.from_json(
                        row["response_json"]
                    ).validate_for(request)
                    connection.commit()
                    return replay
                connection.execute(
                    """INSERT INTO capability_dispatch_cancellations
                       (session_id, cancellation_id, fingerprint, job_id,
                        attempt_id, provider_id, lease_id, lease_generation,
                        response_json, created_at, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        request.session_id,
                        request.cancellation_id,
                        request.fingerprint(),
                        request.job_id,
                        request.attempt_id,
                        request.provider_id,
                        request.lease_id,
                        request.lease_generation,
                        response.to_json(),
                        _timestamp(now),
                        _timestamp(now),
                    ),
                )
                connection.commit()
                return response
            except Exception:
                connection.rollback()
                raise


class CancellableCapabilityWorker(CapabilityWorker):
    """F7.4 worker with durable before-start and in-flight cancellation."""

    def __init__(
        self,
        registration,
        handler,
        inbox: SQLiteLifecycleDispatchInbox,
        *,
        clock,
    ) -> None:
        super().__init__(registration, handler, inbox, clock=clock)
        self.inbox: SQLiteLifecycleDispatchInbox
        self._active: dict[
            tuple[str, str, str, int], asyncio.Task[ExecutionResult]
        ] = {}

    @staticmethod
    def _dispatch_key(
        request: DispatchRequest,
    ) -> tuple[str, str, str, int]:
        return (
            request.session_id,
            request.job.job_id,
            request.attempt_id,
            request.lease_generation,
        )

    @staticmethod
    def _cancellation_key(
        request: CancellationRequest,
    ) -> tuple[str, str, str, int]:
        return (
            request.session_id,
            request.job_id,
            request.attempt_id,
            request.lease_generation,
        )

    async def handle(
        self,
        request: DispatchRequest,
        *,
        authenticated_actor: str,
        authenticated_session: str,
    ) -> DispatchResponse:
        started_at = _utc(self.clock(), "now")
        reason = self._reason(
            request,
            authenticated_actor=authenticated_actor,
            authenticated_session=authenticated_session,
            now=started_at,
        )
        if reason is not None:
            return self._reject(request, reason, started_at)
        if self.inbox.cancelled_for(request):
            return self._reject(request, "cancelled-before-start", started_at)
        running = DispatchResponse(
            request.dispatch_id,
            request.session_id,
            request.job.job_id,
            request.provider_id,
            request.attempt_id,
            (
                DispatchEvent(1, DispatchState.ACCEPTED, started_at),
                DispatchEvent(2, DispatchState.RUNNING, started_at),
            ),
        )
        replay = self.inbox.reserve(request, running, now=started_at)
        if replay is not None:
            return replay.validate_for(request)
        task = asyncio.create_task(
            self.handler.execute(request.job),
            name=f"fcp-capability-{request.job.job_id}-{request.attempt_id}",
        )
        key = self._dispatch_key(request)
        self._active[key] = task
        try:
            raw = await task
            result = (
                raw
                if isinstance(raw, ExecutionResult)
                else ExecutionResult.from_dict(raw)
            )
            if self.inbox.cancelled_for(request):
                result = ExecutionResult(False, {}, "worker-cancelled")
        except asyncio.CancelledError:
            result = ExecutionResult(False, {}, "worker-cancelled")
        except Exception:  # noqa: BLE001
            result = ExecutionResult(False, {}, "worker-handler-failed")
        finally:
            if self._active.get(key) is task:
                self._active.pop(key, None)
        finished_at = max(started_at, _utc(self.clock(), "finished_at"))
        terminal = DispatchEvent(
            3,
            (
                DispatchState.SUCCEEDED
                if result.succeeded
                else DispatchState.FAILED
            ),
            finished_at,
            reason_code=result.reason_code,
            details=result.details,
        )
        response = DispatchResponse(
            request.dispatch_id,
            request.session_id,
            request.job.job_id,
            request.provider_id,
            request.attempt_id,
            (*running.events, terminal),
        ).validate_for(request)
        return self.inbox.finish(request, response, now=finished_at)

    async def cancel(
        self,
        request: CancellationRequest,
        *,
        authenticated_actor: str,
        authenticated_session: str,
    ) -> CancellationResponse:
        now = _utc(self.clock(), "now")
        checks = (
            (
                authenticated_session == request.session_id,
                "session-mismatch",
            ),
            (
                authenticated_actor == request.coordinator_node_id,
                "coordinator-mismatch",
            ),
            (
                self.registration.session_id == request.session_id,
                "worker-session-mismatch",
            ),
            (
                self.registration.node_id == request.target_node_id,
                "target-node-mismatch",
            ),
            (
                self.registration.provider_id == request.provider_id,
                "provider-mismatch",
            ),
        )
        reason = next(
            (reason for valid, reason in checks if not valid),
            None,
        )
        if reason is not None:
            return CancellationResponse(
                request.cancellation_id,
                request.session_id,
                request.provider_id,
                request.job_id,
                request.attempt_id,
                CancellationState.REJECTED,
                now,
                reason_code=reason,
            )
        key = self._cancellation_key(request)
        task = self._active.get(key)
        state = (
            CancellationState.CANCELLED
            if task is not None
            else CancellationState.NOT_RUNNING
        )
        response = CancellationResponse(
            request.cancellation_id,
            request.session_id,
            request.provider_id,
            request.job_id,
            request.attempt_id,
            state,
            now,
        )
        replay = self.inbox.record_cancellation(request, response, now=now)
        if replay != response:
            return replay
        if task is not None and not task.done():
            task.cancel()
        return response
