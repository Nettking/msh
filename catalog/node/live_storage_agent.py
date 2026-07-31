"""Storage node that receives signed Phase D control snapshots over the relay."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import uuid
from contextlib import suppress
from dataclasses import replace
from typing import Any

from catalog.federation.control_sync import (
    STORAGE_CONTROL_RELAY_KIND,
    StorageControlPlan,
    StorageControlReplicaStore,
)
from catalog.federation.errors import FederationValidationError
from catalog.federation.live_failover import STORAGE_CONTROL_REFRESH_MESSAGE
from catalog.federation.models import CapabilityStatus
from catalog.federation.storage_protocol import (
    STORAGE_PROTOCOL,
    BatchIngestRequest,
    StorageError,
    StorageErrorCode,
    StorageOperation,
    StorageRequestEnvelope,
    StorageResponseEnvelope,
)

from .state import EnrollmentState, NodeStateError
from .storage_agent import (
    DEFAULT_ENROLLMENT_TOKEN_ENV,
    DEFAULT_SESSION_INVITATION_ENV,
    StorageNodeAgent,
    StorageNodeAgentError,
    StorageNodeConfig,
)

DEFAULT_CONTROL_SYNC_TIMEOUT = 30.0


class _TrustedControlWriteGate:
    """Reject every local mutation while trusted control is unavailable."""

    def __init__(self, service: Any, ready_event: asyncio.Event) -> None:
        self.service = service
        self.ready_event = ready_event

    async def dispatch(
        self,
        envelope: StorageRequestEnvelope,
    ) -> StorageResponseEnvelope:
        if (
            envelope.operation is StorageOperation.BATCH_INGEST
            and not self.ready_event.is_set()
        ):
            return StorageResponseEnvelope(
                request_id=envelope.request_id,
                protocol=STORAGE_PROTOCOL,
                protocol_version=envelope.protocol_version,
                ok=False,
                error=StorageError(
                    code=StorageErrorCode.NOT_PRIMARY,
                    message=(
                        "storage provider is locally fenced until fresh signed "
                        "control is verified"
                    ),
                    field="provider_id",
                    retryable=True,
                ),
            )
        return await self.service.dispatch(envelope)


class LiveStorageNodeAgent:
    """Compose the F1 storage service with authenticated live control updates.

    A node that loses its relay connection immediately closes a provider-local
    write gate and advertises itself as unavailable.  The gate opens only after
    a signed control publication from the pinned authority has been verified.
    This blocks stale-primary revival without rewriting the signed local control
    history, so an unchanged authority can safely be reconfirmed after a short
    connection interruption.
    """

    def __init__(
        self,
        config: StorageNodeConfig,
        *,
        control_authority_node_id: str,
        control_sync_timeout: float = DEFAULT_CONTROL_SYNC_TIMEOUT,
        clock: Any | None = None,
    ) -> None:
        if not isinstance(control_authority_node_id, str) or not control_authority_node_id:
            raise ValueError("control_authority_node_id must be non-empty text")
        if control_sync_timeout <= 0:
            raise ValueError("control_sync_timeout must be positive")
        self.storage = StorageNodeAgent(config, clock=clock)
        self.control_authority_node_id = control_authority_node_id
        self.control_sync_timeout = float(control_sync_timeout)
        self.replica_store = StorageControlReplicaStore(config.control_database)
        self.control_ready_event = asyncio.Event()
        self.control_waiting_event = asyncio.Event()
        self.storage.endpoint.register_service(
            config.provider_id,
            _TrustedControlWriteGate(
                self.storage.service,
                self.control_ready_event,
            ),
        )
        self._control_task: asyncio.Task[None] | None = None
        self._connection_task: asyncio.Task[None] | None = None
        self._refresh_lock = asyncio.Lock()
        self._closed = False
        self._self_fenced_groups: tuple[str, ...] = ()
        self._last_self_fence_reason: str | None = None

    @property
    def node_id(self) -> str:
        return self.storage.node_id

    @property
    def client(self):
        return self.storage.client

    @property
    def provider(self):
        return self.storage.provider

    @property
    def control(self):
        return self.storage.control

    async def bootstrap(
        self,
        *,
        enrollment_token: str | None = None,
        session_invitation: str | None = None,
    ) -> None:
        state = self.client.state.status()
        enrollment_state = state["enrollment_state"]
        if enrollment_state == EnrollmentState.REVOKED.value:
            raise StorageNodeAgentError(
                "storage-node-revoked",
                "node_id",
                "a revoked node cannot start a storage provider",
            )
        if enrollment_state == EnrollmentState.UNENROLLED.value and not enrollment_token:
            raise StorageNodeAgentError(
                "storage-node-enrollment-required",
                DEFAULT_ENROLLMENT_TOKEN_ENV,
                "first startup requires a protected enrollment token",
            )

        latest = self.replica_store.latest(self.storage.config.session_id)
        cached_primary = False
        if latest is not None:
            latest.verify(expected_authority_node_id=self.control_authority_node_id)
            self.storage.validate_control_state()
            cached_primary = self._local_is_primary()
            if cached_primary:
                self._persist_unavailable_capability()

        await self.client.connect(
            enrollment_token=(
                enrollment_token
                if enrollment_state == EnrollmentState.UNENROLLED.value
                else None
            )
        )
        try:
            joined = {item.session_id for item in self.client.state.joined_sessions()}
            if self.storage.config.session_id not in joined:
                if not session_invitation:
                    raise StorageNodeAgentError(
                        "storage-node-session-invitation-required",
                        DEFAULT_SESSION_INVITATION_ENV,
                        "first session join requires a protected invitation token",
                    )
                await self._join_configured_session(session_invitation)
            await self.storage.endpoint.start()
            self._control_task = asyncio.create_task(
                self._control_loop(),
                name=f"msh-storage-control-{self.node_id}",
            )
            if latest is None:
                self.control_waiting_event.set()
                await asyncio.wait_for(
                    self.control_ready_event.wait(),
                    timeout=self.control_sync_timeout,
                )
            elif cached_primary:
                await self._enter_safe_state(
                    reason="primary-bootstrap-refresh",
                    announce_unavailable=True,
                )
                await self._request_control_refresh()
                await asyncio.wait_for(
                    self.control_ready_event.wait(),
                    timeout=self.control_sync_timeout,
                )
            else:
                self._validate_local_control()
                self.control_ready_event.set()
                self.control_waiting_event.clear()
            self.storage.service.reconcile_prepared()
            await self.client.announce_capability(self.storage.capability())
            self._connection_task = asyncio.create_task(
                self._connection_watch_loop(),
                name=f"msh-storage-connection-watch-{self.node_id}",
            )
        except BaseException:
            await self.close(error_code="storage-control-bootstrap-failed")
            raise

    async def _join_configured_session(self, invitation: str) -> None:
        """Make live join replay-safe while retaining fail-closed rollback."""

        session_id = self.storage.config.session_id
        self.client.state.join_session(session_id, now=self.storage._clock())
        try:
            session = await self.client.join_session(invitation)
            if session.get("session_id") != session_id:
                raise StorageNodeAgentError(
                    "storage-node-session-mismatch",
                    "session_id",
                    "the invitation joined a different session than configured",
                )
        except BaseException:
            try:
                self.client.state.remove_session(
                    session_id,
                    now=self.storage._clock(),
                )
            except NodeStateError:
                pass
            raise

    def _local_is_primary(self) -> bool:
        snapshot = self.control.snapshot(self.storage.config.session_id)
        return any(
            assignment.primary_provider_id == self.storage.config.provider_id
            for assignment in snapshot.groups.values()
        )

    def _validate_local_control(self) -> None:
        self.storage.validate_control_state()
        snapshot = self.control.snapshot(self.storage.config.session_id)
        for group_id, assignment in snapshot.groups.items():
            if assignment.primary_provider_id != self.storage.config.provider_id:
                continue
            grant = snapshot.leader_grants.get(group_id)
            if grant is None or grant.get("provider_id") != self.storage.config.provider_id:
                raise StorageNodeAgentError(
                    "storage-primary-fenced",
                    "group_id",
                    "a local primary requires a fresh coordinator grant",
                )

    def _persist_unavailable_capability(self) -> None:
        capability = replace(
            self.storage.capability(),
            status=CapabilityStatus.UNAVAILABLE,
            announced_at=self.storage._clock(),
        )
        save = getattr(self.client.state, "save_capability", None)
        if callable(save):
            save(capability, now=self.storage._clock())

    async def _enter_safe_state(
        self,
        *,
        reason: str,
        announce_unavailable: bool,
    ) -> None:
        self.control_ready_event.clear()
        self.control_waiting_event.set()
        snapshot = self.control.snapshot(self.storage.config.session_id)
        self._self_fenced_groups = tuple(
            sorted(
                group_id
                for group_id, assignment in snapshot.groups.items()
                if assignment.primary_provider_id == self.storage.config.provider_id
            )
        )
        self._last_self_fence_reason = reason
        self._persist_unavailable_capability()
        if announce_unavailable and self.client.connected_event.is_set():
            capability = replace(
                self.storage.capability(),
                status=CapabilityStatus.UNAVAILABLE,
                announced_at=self.storage._clock(),
            )
            await self.client.announce_capability(capability)

    async def _request_control_refresh(self) -> None:
        async with self._refresh_lock:
            latest = self.replica_store.latest(self.storage.config.session_id)
            delivery = await self.client.send_message(
                session_id=self.storage.config.session_id,
                target_node_id=self.control_authority_node_id,
                request_id=f"control-refresh-{self.node_id}-{uuid.uuid4().hex}",
                payload={
                    "kind": STORAGE_CONTROL_RELAY_KIND,
                    "message": STORAGE_CONTROL_REFRESH_MESSAGE,
                    "provider_id": self.storage.config.provider_id,
                    "known_publication_revision": (
                        None if latest is None else latest.publication_revision
                    ),
                },
            )
            if not isinstance(delivery, dict) or delivery.get("delivered") is not True:
                raise StorageNodeAgentError(
                    "storage-control-refresh-delivery-failed",
                    "control_authority_node_id",
                    "relay did not deliver the refresh request to the control authority",
                )

    async def _connection_watch_loop(self) -> None:
        while not self._closed:
            await self.client.disconnected_event.wait()
            if self._closed:
                return
            await self._enter_safe_state(
                reason="relay-disconnected",
                announce_unavailable=False,
            )
            await self.client.connected_event.wait()
            if self._closed:
                return
            try:
                await self._request_control_refresh()
                await asyncio.wait_for(
                    self.control_ready_event.wait(),
                    timeout=self.control_sync_timeout,
                )
            except (FederationValidationError, TimeoutError):
                if self.client.connected_event.is_set():
                    await self.client.disconnect(
                        error_code="storage-control-refresh-failed"
                    )

    async def _control_loop(self) -> None:
        while True:
            message = await self.storage.endpoint.receive_other()
            payload = getattr(message, "payload", None)
            if (
                not isinstance(payload, dict)
                or payload.get("kind") != STORAGE_CONTROL_RELAY_KIND
                or payload.get("message") != "plan"
            ):
                continue
            actor_node_id = getattr(message, "actor_node_id", None)
            session_id = getattr(message, "session_id", None)
            if (
                actor_node_id != self.control_authority_node_id
                or session_id != self.storage.config.session_id
            ):
                continue
            await self._apply_control_message(actor_node_id, payload)

    async def _apply_control_message(
        self,
        actor_node_id: str,
        payload: dict[str, Any],
    ) -> None:
        plan_value: Any = None
        publication_id = None
        publication_revision = None
        content_hash = None
        try:
            frame = payload.get("frame")
            if not isinstance(frame, str):
                raise FederationValidationError(
                    "invalid-storage-control-frame",
                    "frame",
                    "signed control plan must be a JSON frame",
                )
            try:
                plan_value = json.loads(frame)
            except json.JSONDecodeError as exc:
                raise FederationValidationError(
                    "invalid-storage-control-frame",
                    "frame",
                    "signed control plan frame is not valid JSON",
                ) from exc
            plan = StorageControlPlan.from_dict(plan_value)
            publication_id = plan.publication_id
            publication_revision = plan.publication_revision
            content_hash = plan.content_hash
            result = self.replica_store.apply(
                plan,
                self.control,
                expected_authority_node_id=self.control_authority_node_id,
            )
            self._validate_local_control()
            response: dict[str, Any] = {
                "kind": STORAGE_CONTROL_RELAY_KIND,
                "message": "response",
                "status": result.status,
                "publication_id": result.publication_id,
                "publication_revision": result.publication_revision,
                "content_hash": result.content_hash,
                "local_control_revision": result.local_control_revision,
            }
            self.control_ready_event.set()
            self.control_waiting_event.clear()
            self._self_fenced_groups = ()
            if self.client.connected_event.is_set():
                await self.client.announce_capability(self.storage.capability())
        except FederationValidationError as exc:
            if isinstance(plan_value, dict):
                publication_id = plan_value.get("publication_id")
                publication_revision = plan_value.get("publication_revision")
                content_hash = plan_value.get("content_hash")
            response = {
                "kind": STORAGE_CONTROL_RELAY_KIND,
                "message": "response",
                "status": "rejected",
                "publication_id": publication_id,
                "publication_revision": publication_revision,
                "content_hash": content_hash,
                "error": {
                    "code": exc.code,
                    "field": exc.field,
                    "message": exc.message,
                },
            }
        await self.client.send_message(
            session_id=self.storage.config.session_id,
            target_node_id=actor_node_id,
            request_id=f"control-response-{publication_id or 'invalid'}-{self.node_id}",
            payload=response,
        )

    async def run(
        self,
        *,
        enrollment_token: str | None = None,
        session_invitation: str | None = None,
    ) -> None:
        await self.bootstrap(
            enrollment_token=enrollment_token,
            session_invitation=session_invitation,
        )
        try:
            await self.client.disconnected_event.wait()
            if (
                self.client.state.status()["enrollment_state"]
                != EnrollmentState.REVOKED.value
            ):
                await self.client.run_forever()
        finally:
            await self.close()

    async def close(self, *, error_code: str | None = None) -> None:
        if self._closed:
            return
        with suppress(Exception):
            await self._enter_safe_state(
                reason=error_code or "storage-node-stopped",
                announce_unavailable=True,
            )
        self._closed = True
        tasks = tuple(
            task
            for task in (self._connection_task, self._control_task)
            if task is not None
        )
        self._connection_task = None
        self._control_task = None
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        await self.storage.endpoint.close()
        if self.client.connected_event.is_set():
            await self.client.disconnect(error_code=error_code)

    def status(self) -> dict[str, Any]:
        value = self.storage.status()
        latest = self.replica_store.latest(self.storage.config.session_id)
        value["control_sync"] = {
            "authority_node_id": self.control_authority_node_id,
            "ready": self.control_ready_event.is_set(),
            "publication_id": None if latest is None else latest.publication_id,
            "publication_revision": (
                None if latest is None else latest.publication_revision
            ),
            "content_hash": None if latest is None else latest.content_hash,
            "self_fenced_groups": list(self._self_fenced_groups),
            "last_self_fence_reason": self._last_self_fence_reason,
        }
        return value


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("--control-authority-node-id", required=True)
    parser.add_argument(
        "--control-sync-timeout",
        type=float,
        default=DEFAULT_CONTROL_SYNC_TIMEOUT,
    )
    parser.add_argument("command", choices=("status", "run"))
    return parser


async def _run(arguments: argparse.Namespace) -> int:
    config = StorageNodeConfig.load(arguments.config)
    agent = LiveStorageNodeAgent(
        config,
        control_authority_node_id=arguments.control_authority_node_id,
        control_sync_timeout=arguments.control_sync_timeout,
    )
    if arguments.command == "status":
        print(json.dumps(agent.status(), ensure_ascii=False, sort_keys=True))
        return 0
    try:
        await agent.run(
            enrollment_token=os.environ.get(DEFAULT_ENROLLMENT_TOKEN_ENV),
            session_invitation=os.environ.get(DEFAULT_SESSION_INVITATION_ENV),
        )
    finally:
        await agent.close()
    print(json.dumps(agent.status(), ensure_ascii=False, sort_keys=True))
    return 0


def main() -> int:
    arguments = _parser().parse_args()
    try:
        return asyncio.run(_run(arguments))
    except KeyboardInterrupt:
        return 130
    except (FederationValidationError, TimeoutError) as exc:
        if isinstance(exc, FederationValidationError):
            error = {"code": exc.code, "field": exc.field, "message": exc.message}
        else:
            error = {
                "code": "storage-control-timeout",
                "field": "control_sync",
                "message": str(exc),
            }
        print(json.dumps({"error": error}, sort_keys=True), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
