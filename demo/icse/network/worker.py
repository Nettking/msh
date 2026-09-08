"""One disposable node process, controlled through its parent's private pipes.

The JSON pipe protocol is demonstration orchestration, not an FCP network API.
Federation traffic uses the unchanged production consensus and WebSocket code.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path

from catalog.federation.control_plane_facade import (
    PhysicalReadyReplicatedSessionCoordinator,
)
from catalog.federation.control_plane_product import ReplicatedControlPlaneDeployment
from catalog.federation.control_plane_status import status_document
from catalog.federation.federation_v1_release_runtime import FederationV1ReleaseRuntime
from catalog.federation.models import CapabilityAnnouncement, CapabilityStatus
from catalog.node.client import RelayNodeClient
from catalog.relay.provider_service import ProviderAuthorityRelayServer

from .provenance import verify_source


class Node:
    def __init__(self, config: dict) -> None:
        self.config = config
        self.runtime = None
        self.coordinator = None
        self.relay = None
        self.client = None
        self.runtime_started = False

    async def start(self) -> dict:
        root = Path(__file__).resolve().parents[3]
        source_archive = self.config["source_archive"]
        identity = await asyncio.to_thread(
            verify_source, root, self.config["source_sha"], self.config["source_tag"],
            source_archive=Path(source_archive) if source_archive else None,
            archive_sha256=self.config["archive_sha256"],
        )
        if self.config.get("deployment"):
            deployment = ReplicatedControlPlaneDeployment.from_file(
                self.config["deployment"]
            )
            # The release's production composition and default timing are used.
            # No accelerated election clock or fixture transport is supplied.
            self.runtime = FederationV1ReleaseRuntime(
                deployment,
                legacy_node_state_database=Path(self.config["state"]) / "legacy.sqlite3",
                legacy_pairing_state_path=Path(self.config["state"]) / "legacy.json",
            )
            self.coordinator = PhysicalReadyReplicatedSessionCoordinator(self.runtime)
            self.relay = ProviderAuthorityRelayServer(
                self.coordinator, host="127.0.0.1", port=0
            )
            self.runtime.start()
            self.runtime_started = True
            await self.relay.start()
        return {
            "pid": os.getpid(),
            "label": self.config["label"],
            "node_id": self.config["node_id"],
            "relay_url": self.relay.url if self.relay else None,
            "source_sha": identity["source_sha"],
        }

    async def command(self, value: dict):
        operation = value["operation"]
        if operation == "status":
            if self.runtime is None:
                raise ValueError("status requires a voter")
            state = self.runtime.node.state
            return {
                "control_plane": status_document(
                    self.runtime.node, ready=self.runtime.ready, voter_only=False
                ),
                "memberships": state["memberships"],
                "capabilities": state["capabilities"],
                "lifecycle_error": self.runtime.lifecycle_error,
            }
        if operation == "bootstrap":
            if self.runtime is None:
                raise ValueError("bootstrap requires a voter")
            self.runtime.bootstrap_new_federation(
                federation_id=value["federation_id"],
                session_id=value["session_id"],
                creator_node_id=self.config["node_id"],
                display_name="ICSE local network demonstration",
            )
            return {"created": True}
        if operation == "enrollment_token":
            if self.coordinator is None:
                raise ValueError("enrollment requires a coordinator")
            # Secret goes only through the parent's pipe, never into artifacts.
            return self.coordinator.create_enrollment_token(ttl_seconds=300, max_uses=1)
        if operation == "connect":
            if self.client is not None:
                await self.client.disconnect()
            self.client = RelayNodeClient(
                state_directory=self.config["identity"],
                relay_url=value["relay_url"],
                display_name=self.config["label"],
                allow_insecure_local=True,
            )
            await self.client.connect(enrollment_token=value.get("enrollment_token"))
            return {"connected": True, "node_id": self.client.node_id}
        if self.client is None:
            raise ValueError("operation requires a connected member")
        if operation == "invite":
            return await self.client.create_invitation(
                value["session_id"], ttl_seconds=300, max_uses=1
            )
        if operation == "join":
            return await self.client.join_session(value["invitation"])
        if operation == "discover":
            return await self.client.coordinator_status()
        if operation == "announce":
            announcement = CapabilityAnnouncement(
                capability_id=value["capability_id"],
                node_id=value.get("owner_node_id", self.client.node_id),
                session_id=value["session_id"],
                type=value["capability_type"],
                protocol="icse-illustrative-payload",
                protocol_version="1",
                status=CapabilityStatus.READY,
                properties={"purpose": "synthetic reviewer dataset exchange"},
                announced_at=datetime.now(timezone.utc),
            )
            return await self.client.announce_capability(announcement)
        if operation == "send":
            return await self.client.send_message(
                session_id=value["session_id"],
                target_node_id=value["target_node_id"],
                payload=value["payload"],
            )
        if operation == "receive":
            message = await self.client.receive_message(timeout=15)
            return message.to_dict()
        raise ValueError("unknown demonstration operation")

    async def close(self) -> None:
        try:
            if self.client is not None:
                await self.client.disconnect()
        finally:
            try:
                if self.relay is not None:
                    await self.relay.stop()
            finally:
                if self.runtime is not None and self.runtime_started:
                    self.runtime.close()


def emit(value: dict) -> None:
    print(json.dumps(value, sort_keys=True), flush=True)


async def serve(config: dict) -> int:
    node = Node(config)
    try:
        emit({"startup": await node.start()})
        while line := await asyncio.to_thread(sys.stdin.readline):
            command = json.loads(line)
            if command["operation"] == "shutdown":
                emit({"id": command["id"], "ok": True, "result": {"stopping": True}})
                break
            try:
                result = await node.command(command)
                emit({"id": command["id"], "ok": True, "result": result})
            except Exception as error:  # noqa: BLE001 - protocol boundary returns failure, never success on exceptions
                # Return bounded error identity, never raw request/token text.
                traceback.print_exc(file=sys.stderr)
                emit({
                    "id": command["id"],
                    "ok": False,
                    "error_type": type(error).__name__,
                    "error_code": getattr(error, "code", None),
                })
        return 0
    finally:
        await node.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path)
    args = parser.parse_args()
    config = json.loads(args.config.read_text(encoding="utf-8"))
    try:
        return asyncio.run(serve(config))
    except Exception as error:  # noqa: BLE001 - process boundary records the failure and exits nonzero
        print(f"node process failed: {type(error).__name__}", file=sys.stderr)
        # stderr is retained only in private-state, never in the public report.
        traceback.print_exc(file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
