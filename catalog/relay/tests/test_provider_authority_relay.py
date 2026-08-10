from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path

from catalog.capabilities.provider_health import ProviderHealthRecord
from catalog.capabilities.provider_reports import ProviderResourceReport, ProviderStatus
from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.models import CapabilityAnnouncement, CapabilityStatus
from catalog.node.client import RelayNodeClient
from catalog.relay.provider_service import ProviderAuthorityRelayServer

CAPABILITY_ID = "physical-ai-provider"


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

            report = ProviderResourceReport(
                capability_id=CAPABILITY_ID,
                node_id=provider.node_id,
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
                reported_at=current[0],
                expires_at=current[0] + timedelta(seconds=60),
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
