from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from catalog.federation.models import CapabilityAnnouncement, CapabilityStatus
from catalog.relay.provider_service import ProviderAuthorityRelayServer

NOW = datetime(2026, 8, 20, 12, 0, tzinfo=timezone.utc)


def test_any_accepted_capability_refreshes_current_storage_for_that_node() -> None:
    provider_calls: list[CapabilityAnnouncement] = []
    storage_calls: list[dict[str, str]] = []

    server = ProviderAuthorityRelayServer.__new__(ProviderAuthorityRelayServer)
    server.provider_auto_enrollment = SimpleNamespace(
        reconcile=lambda announcement: provider_calls.append(announcement)
    )
    server.storage_authority = SimpleNamespace(
        reconcile_current_node=lambda **kwargs: storage_calls.append(kwargs)
    )
    server.coordinator = SimpleNamespace(
        audit_rejection=lambda **_kwargs: None,
    )

    announcement = CapabilityAnnouncement(
        capability_id="logical-storage-authority",
        node_id="node-creator",
        session_id="session-a",
        type="storage-control",
        protocol="fcp.storage-control",
        protocol_version="1",
        status=CapabilityStatus.READY,
        properties={
            "kind": "recorder-logical-storage-authority",
            "group_ids": ["fcp-local-storage"],
        },
        announced_at=NOW,
    )
    request = SimpleNamespace(
        payload={"announcement": announcement.to_dict()},
        session_id="session-a",
        request_id="announce-logical-storage",
    )
    record = SimpleNamespace(node_id="node-creator")

    server._auto_enroll_accepted_announcement(record=record, request=request)

    assert len(provider_calls) == 1
    assert provider_calls[0].capability_id == "logical-storage-authority"
    assert storage_calls == [
        {"session_id": "session-a", "node_id": "node-creator"}
    ]
