"""Runnable relay-first storage node for physical multi-machine federation tests.

The agent composes the existing authenticated node client, relay storage endpoint,
Phase D/E control plane, durable outbox, acknowledgement store, and filesystem
provider.  It opens no inbound listener; all application traffic uses the existing
outbound WebSocket connection to the relay.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from catalog.federation.commit_tracking import DurableAcknowledgementStore
from catalog.federation.errors import FederationValidationError
from catalog.federation.local_storage import FilesystemBatchStorageProvider
from catalog.federation.models import CapabilityAnnouncement
from catalog.federation.outbox import SQLiteOutbox
from catalog.federation.phase_d_control import PhaseDControlPlane
from catalog.federation.phase_d_service import PhaseDStorageService
from catalog.federation.relay_storage import RelayStorageEndpoint
from catalog.federation.storage_protocol import (
    STORAGE_PROTOCOL,
    STORAGE_PROTOCOL_VERSION,
)

from .client import RelayNodeClient
from .identity import IdentityStore
from .state import EnrollmentState

STORAGE_NODE_CONFIG_SCHEMA = "msh.storage_node_config.v1"
STORAGE_NODE_STATUS_SCHEMA = "msh.storage_node_status.v1"
DEFAULT_ENROLLMENT_TOKEN_ENV = "MSH_ENROLLMENT_TOKEN"
DEFAULT_SESSION_INVITATION_ENV = "MSH_SESSION_INVITATION"
_MAX_INTERVAL_SECONDS = 3_600.0


class StorageNodeAgentError(FederationValidationError):
    """Configuration or lifecycle failure for the runnable storage node."""


def _required_text(value: Any, field: str) -> str:
    if (
        not isinstance(value, str)
        or not value.strip()
        or any(ord(character) < 32 for character in value)
    ):
        raise StorageNodeAgentError(
            "invalid-storage-node-config",
            field,
            "must be non-empty text",
        )
    return value


def _positive_interval(value: Any, field: str, default: float) -> float:
    selected = default if value is None else value
    if (
        isinstance(selected, bool)
        or not isinstance(selected, (int, float))
        or not math.isfinite(selected)
        or not 0 < selected <= _MAX_INTERVAL_SECONDS
    ):
        raise StorageNodeAgentError(
            "invalid-storage-node-config",
            field,
            f"must be a finite positive number no greater than {_MAX_INTERVAL_SECONDS:g}",
        )
    return float(selected)


def _path(value: Any, field: str, base_directory: Path) -> Path:
    raw = Path(_required_text(value, field)).expanduser()
    return (raw if raw.is_absolute() else base_directory / raw).resolve()


@dataclass(frozen=True)
class StorageNodeConfig:
    """Strict local configuration without credentials or public path disclosure."""

    source: Path
    state_directory: Path
    relay_url: str
    display_name: str
    provider_id: str
    session_id: str
    control_database: Path
    storage_directory: Path
    outbox_database: Path
    acknowledgements_database: Path
    allow_insecure_local: bool = False
    heartbeat_interval: float = 10.0
    request_timeout: float = 15.0
    capability_id: str = ""

    @classmethod
    def load(cls, source: Path | str) -> StorageNodeConfig:
        config_path = Path(source).expanduser().resolve()
        try:
            value = json.loads(config_path.read_text(encoding="utf-8"))
        except OSError as exc:
            raise StorageNodeAgentError(
                "storage-node-config-unavailable",
                "config",
                "could not read the storage node configuration",
            ) from exc
        except json.JSONDecodeError as exc:
            raise StorageNodeAgentError(
                "invalid-storage-node-config",
                "config",
                "must contain valid JSON",
            ) from exc
        if not isinstance(value, dict):
            raise StorageNodeAgentError(
                "invalid-storage-node-config",
                "config",
                "must be a JSON object",
            )
        forbidden = sorted(
            key
            for key in value
            if any(marker in str(key).lower() for marker in ("token", "password", "secret", "private_key"))
        )
        if forbidden:
            raise StorageNodeAgentError(
                "credential-in-storage-node-config",
                forbidden[0],
                "credentials must be supplied through protected environment input",
            )
        allowed = {
            "schema",
            "state_directory",
            "relay_url",
            "display_name",
            "provider_id",
            "session_id",
            "control_database",
            "storage_directory",
            "outbox_database",
            "acknowledgements_database",
            "allow_insecure_local",
            "heartbeat_interval",
            "request_timeout",
            "capability_id",
        }
        unknown = sorted(set(value).difference(allowed))
        if unknown:
            raise StorageNodeAgentError(
                "invalid-storage-node-config",
                unknown[0],
                "unknown configuration field",
            )
        if value.get("schema") != STORAGE_NODE_CONFIG_SCHEMA:
            raise StorageNodeAgentError(
                "unsupported-storage-node-config",
                "schema",
                f"expected {STORAGE_NODE_CONFIG_SCHEMA}",
            )
        allow_insecure_local = value.get("allow_insecure_local", False)
        if not isinstance(allow_insecure_local, bool):
            raise StorageNodeAgentError(
                "invalid-storage-node-config",
                "allow_insecure_local",
                "must be boolean",
            )
        provider_id = _required_text(value.get("provider_id"), "provider_id")
        capability_id = value.get("capability_id")
        if capability_id is None:
            capability_id = f"storage-{provider_id}"
        capability_id = _required_text(capability_id, "capability_id")
        base = config_path.parent
        return cls(
            source=config_path,
            state_directory=_path(value.get("state_directory"), "state_directory", base),
            relay_url=_required_text(value.get("relay_url"), "relay_url"),
            display_name=_required_text(value.get("display_name"), "display_name"),
            provider_id=provider_id,
            session_id=_required_text(value.get("session_id"), "session_id"),
            control_database=_path(value.get("control_database"), "control_database", base),
            storage_directory=_path(value.get("storage_directory"), "storage_directory", base),
            outbox_database=_path(value.get("outbox_database"), "outbox_database", base),
            acknowledgements_database=_path(
                value.get("acknowledgements_database"),
                "acknowledgements_database",
                base,
            ),
            allow_insecure_local=allow_insecure_local,
            heartbeat_interval=_positive_interval(
                value.get("heartbeat_interval"), "heartbeat_interval", 10.0
            ),
            request_timeout=_positive_interval(
                value.get("request_timeout"), "request_timeout", 15.0
            ),
            capability_id=capability_id,
        )

    def public_summary(self) -> dict[str, Any]:
        return {
            "schema": STORAGE_NODE_CONFIG_SCHEMA,
            "display_name": self.display_name,
            "provider_id": self.provider_id,
            "session_id": self.session_id,
            "relay_url": self.relay_url,
            "allow_insecure_local": self.allow_insecure_local,
            "heartbeat_interval": self.heartbeat_interval,
            "request_timeout": self.request_timeout,
            "capability_id": self.capability_id,
            "backend": "filesystem",
        }


class StorageNodeAgent:
    """One persistent storage provider reachable only through the relay."""

    def __init__(
        self,
        config: StorageNodeConfig,
        *,
        clock: Any | None = None,
    ) -> None:
        self.config = config
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self.client = RelayNodeClient(
            state_directory=config.state_directory,
            relay_url=config.relay_url,
            display_name=config.display_name,
            allow_insecure_local=config.allow_insecure_local,
            heartbeat_interval=config.heartbeat_interval,
            request_timeout=config.request_timeout,
            clock=self._clock,
        )
        self.control = PhaseDControlPlane(config.control_database)
        self.provider = FilesystemBatchStorageProvider(config.storage_directory)
        self.outbox = SQLiteOutbox(config.outbox_database)
        self.acknowledgements = DurableAcknowledgementStore(
            config.acknowledgements_database
        )
        self.endpoint = RelayStorageEndpoint(
            self.client,
            request_timeout=config.request_timeout,
        )
        self.service = PhaseDStorageService(
            provider_id=config.provider_id,
            provider=self.provider,
            control_plane=self.control,
            outbox=self.outbox,
            acknowledgements=self.acknowledgements,
            replication_transport=self.endpoint,
            clock=self._clock,
        )
        self.endpoint.register_service(config.provider_id, self.service)
        self._closed = False

    @property
    def node_id(self) -> str:
        return self.client.node_id

    @property
    def inbound_listener_ports(self) -> tuple[int, ...]:
        return self.client.inbound_listener_ports

    def _registration(self):
        return self.control.snapshot(self.config.session_id).providers.get(
            self.config.provider_id
        )

    def validate_control_state(self) -> None:
        registration = self._registration()
        if registration is None:
            raise StorageNodeAgentError(
                "storage-provider-not-provisioned",
                "provider_id",
                "the local control plane does not contain this provider",
            )
        if registration.node_id != self.node_id:
            raise StorageNodeAgentError(
                "storage-provider-node-mismatch",
                "node_id",
                "the provider registration belongs to another persistent node identity",
            )
        if not registration.assignable:
            raise StorageNodeAgentError(
                "storage-provider-not-assignable",
                "provider_id",
                "the provider must be authorized, ready, and protocol-compatible",
            )

    def capability(self) -> CapabilityAnnouncement:
        snapshot = self.control.snapshot(self.config.session_id)
        return CapabilityAnnouncement(
            capability_id=self.config.capability_id,
            node_id=self.node_id,
            session_id=self.config.session_id,
            type="storage-provider",
            protocol=STORAGE_PROTOCOL,
            protocol_version=STORAGE_PROTOCOL_VERSION,
            status="ready",
            properties={
                "provider_id": self.config.provider_id,
                "backend": "filesystem",
                "durable": True,
                "relay_first": True,
                "control_revision": snapshot.revision,
            },
            announced_at=self._clock(),
        )

    async def bootstrap(
        self,
        *,
        enrollment_token: str | None = None,
        session_invitation: str | None = None,
    ) -> None:
        """Enroll if needed, join the configured session, and expose the service."""

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
        selected_enrollment = (
            enrollment_token
            if enrollment_state == EnrollmentState.UNENROLLED.value
            else None
        )
        await self.client.connect(enrollment_token=selected_enrollment)
        try:
            joined = {
                item.session_id for item in self.client.state.joined_sessions()
            }
            if self.config.session_id not in joined:
                if not session_invitation:
                    raise StorageNodeAgentError(
                        "storage-node-session-invitation-required",
                        DEFAULT_SESSION_INVITATION_ENV,
                        "first session join requires a protected invitation token",
                    )
                session = await self.client.join_session(session_invitation)
                if session.get("session_id") != self.config.session_id:
                    raise StorageNodeAgentError(
                        "storage-node-session-mismatch",
                        "session_id",
                        "the invitation joined a different session than configured",
                    )
            self.validate_control_state()
            self.service.reconcile_prepared()
            await self.endpoint.start()
            await self.client.announce_capability(self.capability())
        except BaseException:
            await self.endpoint.close()
            if self.client.connected_event.is_set():
                await self.client.disconnect(error_code="storage-bootstrap-failed")
            raise

    async def run(
        self,
        *,
        enrollment_token: str | None = None,
        session_invitation: str | None = None,
    ) -> None:
        """Serve until revoked, reconnect exhaustion, cancellation, or shutdown."""

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

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        await self.endpoint.close()
        if self.client.connected_event.is_set():
            await self.client.disconnect()

    def status(self) -> dict[str, Any]:
        snapshot = self.control.snapshot(self.config.session_id)
        registration = snapshot.providers.get(self.config.provider_id)
        groups: list[dict[str, Any]] = []
        for group_id, assignment in sorted(snapshot.groups.items()):
            if assignment.primary_provider_id == self.config.provider_id:
                role = "primary"
            elif self.config.provider_id in assignment.replica_provider_ids:
                role = "replica"
            else:
                role = "unassigned"
            groups.append({"group_id": group_id, "role": role})
        return {
            "schema": STORAGE_NODE_STATUS_SCHEMA,
            "node": self.client.state.status(),
            "provider": {
                "provider_id": self.config.provider_id,
                "session_id": self.config.session_id,
                "registered": registration is not None,
                "assignable": bool(registration and registration.assignable),
                "control_revision": snapshot.revision,
                "groups": groups,
                "description": self.provider.describe(),
                "health": self.provider.health(),
                "inbound_listener_ports": list(self.inbound_listener_ports),
            },
        }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument(
        "command",
        choices=("validate", "initialize", "status", "run"),
    )
    return parser


async def _run(arguments: argparse.Namespace) -> int:
    config = StorageNodeConfig.load(arguments.config)
    if arguments.command == "validate":
        print(json.dumps(config.public_summary(), ensure_ascii=False, sort_keys=True))
        return 0
    if arguments.command == "initialize":
        credentials = IdentityStore(
            config.state_directory,
            display_name=config.display_name,
        ).load_or_create()
        print(
            json.dumps(
                {
                    "schema": "msh.storage_node_initialization.v1",
                    "identity": credentials.identity.to_dict(),
                    "provider_id": config.provider_id,
                    "session_id": config.session_id,
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 0
    agent = StorageNodeAgent(config)
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
    except FederationValidationError as exc:
        print(
            json.dumps(
                {
                    "error": {
                        "code": exc.code,
                        "field": exc.field,
                        "message": exc.message,
                    }
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
