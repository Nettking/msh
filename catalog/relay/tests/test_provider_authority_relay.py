from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path

from catalog.capabilities.provider_auto_enrollment import AUTO_ENROLLMENT_SCHEMA
from catalog.capabilities.provider_health import ProviderHealthRecord
from catalog.capabilities.provider_reports import ProviderResourceReport, ProviderStatus
from catalog.capabilities.storage_authority_enrollment import (
    DEFAULT_STORAGE_GROUP_ID,
    STORAGE_AUTHORITY_SCHEMA,
    federation_storage_provider_id,
)
from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.models import CapabilityAnnouncement, CapabilityStatus
from catalog.federation.storage_protocol import (
    STORAGE_PROTOCOL,
    STORAGE_PROTOCOL_VERSION,
)
from catalog.node.client import RelayNodeClient
from catalog.relay.provider_service import ProviderAuthorityRelayServer

CAPABILITY_ID = "physical-ai-provider"
STORAGE_CAPABILITY_ID = "physical-storage-provider"


def _report(
    *,
    capability_id: str,
    node_id: str,
    session_id: str,
    now: datetime,
) -> ProviderResourceReport:
    return ProviderResourceReport(
        capability_id=capability_id,
        node_id=node_id,
        session_id=session_id,
        capability_type="language-model",
        protocol="fcp-language-model",
        protocol_version="1.0",
        status=ProviderStatus.READY,
        report_revision=0,
        max_concurrent_jobs=1,
        active_jobs=0,
        queue_depth=0,
        utilization_millis=0,
        attributes={
            "label": "Physical AI provider",
            "models": ["llama3.2:3b"],
            "modalities": ["text"],
            "features": {"timeout": True, "cancellation": False},
        },
        reported_at=now,
        expires_at=now + timedelta(seconds=60),
    )


def test_authenticated_relay_composes_provider_enrollment_and_health(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        current = [datetime.now(timezone.utc)]
        relay = ProviderAuthorityRelayServer(
            SessionCoordinator(
                tmp_path / "relay" / "control.sqlite3",
                clock=lambda: current[0],
            ),
            host="127.0.0.1",
            port=0,
            auth_timeout_seconds=10,
            send_timeout_seconds=10,
            heartbeat_timeout_seconds=300,
            sweep_interval_seconds=300,
        )
        owner = RelayNodeClient(
            state_directory=tmp_path / "owner",
            relay_url="ws://127.0.0.1:1",
            display_name="Owner",
            allow_insecure_local=True,
            clock=lambda: current[0],
        )
        provider = RelayNodeClient(
            state_directory=tmp_path / "provider",
            relay_url="ws://127.0.0.1:1",
            display_name="Provider",
            allow_insecure_local=True,
            clock=lambda: current[0],
        )
        await relay.start()
        # Replace the placeholder URL only after the ephemeral relay port exists.
        owner.relay_url = relay.url
        provider.relay_url = relay.url
        try:
            for client in (owner, provider):
                token = relay.coordinator.create_enrollment_token(
                    ttl_seconds=60,
                    max_uses=1,
                )["token"]
                await client.connect(enrollment_token=token)

            session = await owner.create_session("Provider authority relay")
            session_id = str(session["session_id"])
            invitation = await owner.create_invitation(
                session_id,
                ttl_seconds=60,
                max_uses=1,
            )
            joined = await provider.join_session(str(invitation["token"]))
            assert joined["session_id"] == session_id

            announcement = CapabilityAnnouncement(
                capability_id=CAPABILITY_ID,
                node_id=provider.node_id,
                session_id=session_id,
                type="language-model",
                protocol="fcp-language-model",
                protocol_version="1.0",
                status=CapabilityStatus.READY,
                properties={"label": "Physical AI provider"},
                announced_at=current[0],
            )
            await provider.announce_capability(announcement)

            requested = await provider.request(
                "provider.enrollment.request",
                session_id=session_id,
                payload={"capability_id": CAPABILITY_ID},
                request_id="product-provider-request",
            )
            assert requested["enrollment"]["state"] == "pending"
            revision = int(requested["enrollment"]["revision"])

            approved = await owner.request(
                "provider.enrollment.approve",
                session_id=session_id,
                payload={
                    "capability_id": CAPABILITY_ID,
                    "expected_revision": revision,
                },
                request_id="product-provider-approve",
            )
            assert approved["enrollment"]["state"] == "approved"

            report = _report(
                capability_id=CAPABILITY_ID,
                node_id=provider.node_id,
                session_id=session_id,
                now=current[0],
            )
            published = await provider.request(
                "provider.health.publish",
                session_id=session_id,
                payload={
                    "report": report.to_dict(),
                    "provider_generation": 1,
                },
                request_id="product-provider-health",
            )
            accepted = ProviderHealthRecord.from_dict(published["health"])
            assert accepted.report == report
            assert accepted.provider_generation == 1

            observed = await owner.request(
                "provider.health.current",
                session_id=session_id,
                payload={"capability_type": "language-model"},
                request_id="product-provider-current",
            )
            assert len(observed["health"]) == 1
            current_record = ProviderHealthRecord.from_dict(observed["health"][0])
            assert current_record.capability_id == CAPABILITY_ID
            assert current_record.node_id == provider.node_id
            assert current_record.report.attributes["models"] == ["llama3.2:3b"]
        finally:
            await provider.disconnect()
            await owner.disconnect()
            await relay.stop()

    asyncio.run(scenario())


def test_trusted_green_contribution_needs_no_manual_owner_approval(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        current = [datetime.now(timezone.utc)]
        relay = ProviderAuthorityRelayServer(
            SessionCoordinator(
                tmp_path / "relay-auto" / "control.sqlite3",
                clock=lambda: current[0],
            ),
            host="127.0.0.1",
            port=0,
            auth_timeout_seconds=10,
            send_timeout_seconds=10,
            heartbeat_timeout_seconds=300,
            sweep_interval_seconds=300,
        )
        owner = RelayNodeClient(
            state_directory=tmp_path / "owner-auto",
            relay_url="ws://127.0.0.1:1",
            display_name="Owner",
            allow_insecure_local=True,
            clock=lambda: current[0],
        )
        provider = RelayNodeClient(
            state_directory=tmp_path / "provider-auto",
            relay_url="ws://127.0.0.1:1",
            display_name="Provider",
            allow_insecure_local=True,
            clock=lambda: current[0],
        )
        await relay.start()
        owner.relay_url = relay.url
        provider.relay_url = relay.url
        try:
            for client in (owner, provider):
                token = relay.coordinator.create_enrollment_token(
                    ttl_seconds=60,
                    max_uses=1,
                )["token"]
                await client.connect(enrollment_token=token)
            session = await owner.create_session("Auto provider authority")
            session_id = str(session["session_id"])
            invitation = await owner.create_invitation(
                session_id,
                ttl_seconds=60,
                max_uses=1,
            )
            await provider.join_session(str(invitation["token"]))

            properties = {
                "kind": "capability-first-candidate",
                "candidate_id": CAPABILITY_ID,
                "label": "Physical AI provider",
                "auto_enrollment": {
                    "schema": AUTO_ENROLLMENT_SCHEMA,
                    "eligible": True,
                    "benchmark_state": "green",
                    "inspection_revision": 2,
                    "benchmark_run_ids": ["benchmark-green-1"],
                    "decision_revision": 3,
                    "desired_state": "enabled",
                    "policy_state": "allowed",
                },
            }
            registering = CapabilityAnnouncement(
                capability_id=CAPABILITY_ID,
                node_id=provider.node_id,
                session_id=session_id,
                type="language-model",
                protocol="fcp-language-model",
                protocol_version="1.0",
                status=CapabilityStatus.REGISTERING,
                properties=properties,
                announced_at=current[0],
            )
            await provider.announce_capability(registering)

            # A later request on the same authenticated connection can only run
            # after the relay-side auto reconciliation completed.
            enrolled = await provider.request(
                "provider.enrollment.request",
                session_id=session_id,
                payload={"capability_id": CAPABILITY_ID},
                request_id="inspect-auto-enrollment",
            )
            assert enrolled["enrollment"]["state"] == "approved"
            assert enrolled["enrollment"]["approved_by_node_id"] == owner.node_id
            assert enrolled["enrollment"]["reason_code"] == (
                "trusted-green-auto-enrollment"
            )
            record = relay.provider_enrollment.store.get(
                session_id=session_id,
                capability_id=CAPABILITY_ID,
            )
            assert record is not None
            assert record.eligible_for_resource_binding is False

            # The status transition itself changes the authoritative fingerprint;
            # the clock need not be advanced. Keeping it at the actual current
            # time also exercises the health authority's future-report guard.
            ready = CapabilityAnnouncement(
                capability_id=CAPABILITY_ID,
                node_id=provider.node_id,
                session_id=session_id,
                type="language-model",
                protocol="fcp-language-model",
                protocol_version="1.0",
                status=CapabilityStatus.READY,
                properties=properties,
                announced_at=current[0],
            )
            await provider.announce_capability(ready)

            report = _report(
                capability_id=CAPABILITY_ID,
                node_id=provider.node_id,
                session_id=session_id,
                now=current[0],
            )
            published = await provider.request(
                "provider.health.publish",
                session_id=session_id,
                payload={
                    "report": report.to_dict(),
                    "provider_generation": 1,
                },
                request_id="auto-provider-health",
            )
            accepted = ProviderHealthRecord.from_dict(published["health"])
            assert accepted.report == report
            assert accepted.provider_generation == 1
        finally:
            await provider.disconnect()
            await owner.disconnect()
            await relay.stop()

    asyncio.run(scenario())


def test_trusted_green_storage_takes_authority_without_manual_assignment(
    tmp_path: Path,
) -> None:
    """A joined member's GREEN storage must not wait for a coordinator click."""

    async def scenario() -> None:
        current = [datetime.now(timezone.utc)]
        relay = ProviderAuthorityRelayServer(
            SessionCoordinator(
                tmp_path / "relay-storage" / "control.sqlite3",
                clock=lambda: current[0],
            ),
            host="127.0.0.1",
            port=0,
            auth_timeout_seconds=10,
            send_timeout_seconds=10,
            heartbeat_timeout_seconds=300,
            sweep_interval_seconds=300,
        )
        owner = RelayNodeClient(
            state_directory=tmp_path / "owner-storage",
            relay_url="ws://127.0.0.1:1",
            display_name="Owner",
            allow_insecure_local=True,
            clock=lambda: current[0],
        )
        member = RelayNodeClient(
            state_directory=tmp_path / "member-storage",
            relay_url="ws://127.0.0.1:1",
            display_name="Member",
            allow_insecure_local=True,
            clock=lambda: current[0],
        )
        await relay.start()
        owner.relay_url = relay.url
        member.relay_url = relay.url
        try:
            for client in (owner, member):
                token = relay.coordinator.create_enrollment_token(
                    ttl_seconds=60,
                    max_uses=1,
                )["token"]
                await client.connect(enrollment_token=token)
            session = await owner.create_session("Storage authority")
            session_id = str(session["session_id"])
            invitation = await owner.create_invitation(
                session_id,
                ttl_seconds=60,
                max_uses=1,
            )
            await member.join_session(str(invitation["token"]))

            provider_id = federation_storage_provider_id(
                member.node_id,
                "fcp-local-data-storage",
            )
            announcement = CapabilityAnnouncement(
                capability_id=STORAGE_CAPABILITY_ID,
                node_id=member.node_id,
                session_id=session_id,
                type="storage",
                protocol=STORAGE_PROTOCOL,
                protocol_version=STORAGE_PROTOCOL_VERSION,
                status=CapabilityStatus.READY,
                properties={
                    "kind": "capability-first-candidate",
                    "candidate_id": "fcp-local-data-storage",
                    "label": "Local FCP data storage",
                    "storage_authority": {
                        "schema": STORAGE_AUTHORITY_SCHEMA,
                        "eligible": True,
                        "benchmark_state": "green",
                        "provider_id": provider_id,
                        "group_id": DEFAULT_STORAGE_GROUP_ID,
                        "role": "primary",
                        "inspection_revision": 2,
                        "benchmark_run_ids": ["benchmark-storage-green-1"],
                        "decision_revision": 3,
                        "desired_state": "enabled",
                        "policy_state": "allowed",
                    },
                },
                announced_at=current[0],
            )
            await member.announce_capability(announcement)

            # A later request on the same authenticated connection can only run
            # after the relay-side storage reconciliation completed.
            await member.request(
                "provider.enrollment.request",
                session_id=session_id,
                payload={"capability_id": STORAGE_CAPABILITY_ID},
                request_id="flush-storage-reconciliation",
            )

            snapshot = relay.storage_authority.control_plane.snapshot(session_id)
            registration = snapshot.providers[provider_id]
            assert registration.node_id == member.node_id
            assert registration.assignable is True
            assignment = snapshot.groups[DEFAULT_STORAGE_GROUP_ID]
            assert assignment.primary_provider_id == provider_id
            grant = snapshot.leader_grants[DEFAULT_STORAGE_GROUP_ID]
            assert grant["provider_id"] == provider_id
            assert grant["node_id"] == member.node_id

            # Storage authority is granted by the storage control plane, never by
            # F8.1 provider enrollment.
            enrollment = relay.provider_enrollment.store.get(
                session_id=session_id,
                capability_id=STORAGE_CAPABILITY_ID,
            )
            assert enrollment is None or enrollment.state.value != "approved"
        finally:
            await member.disconnect()
            await owner.disconnect()
            await relay.stop()

    asyncio.run(scenario())
