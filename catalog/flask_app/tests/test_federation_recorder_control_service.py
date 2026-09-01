from __future__ import annotations

from types import SimpleNamespace

import pytest

from catalog.flask_app.services import federation_recorder_control_service as service_module
from catalog.flask_app.services.federation_recorder_control_service import (
    FederationRecorderControlError,
    FederationRecorderControlService,
)


class _Coordinator:
    def __init__(self, events=()) -> None:
        self.events = tuple(events)

    def status(self, *, actor_node_id: str, cursor=None):
        assert actor_node_id == "node-member"
        assert cursor is None
        return {
            "nodes": [
                {
                    "node_id": "node-recorder",
                    "display_name": "Workshop recorder",
                    "connection_state": "connected",
                }
            ],
            "capabilities": [
                {
                    "node_id": "node-recorder",
                    "capability_id": "recorder-node-recorder",
                    "type": "recorder",
                    "protocol": "mtconnect",
                    "protocol_version": "2.0",
                    "status": "ready",
                    "properties": {
                        "kind": "standalone-recorder",
                        "source_names": ["machine-old"],
                    },
                }
            ],
        }

    def replay_page(
        self,
        *,
        session_id: str,
        actor_node_id: str,
        last_applied_revision: int,
        limit: int,
    ):
        assert session_id == "session-one"
        assert actor_node_id == "node-member"
        assert limit == 1000
        remaining = tuple(
            event for event in self.events if event.revision > last_applied_revision
        )
        return remaining, max((event.revision for event in self.events), default=0)


class _Runtime:
    def __init__(self) -> None:
        self.appended: list[dict] = []

    def append_session_event(
        self,
        remote,
        *,
        session_id: str,
        event_type: str,
        payload: dict,
        request_id: str,
    ) -> None:
        assert remote is not None
        assert session_id == "session-one"
        self.appended.append(
            {
                "event_type": event_type,
                "payload": payload,
                "request_id": request_id,
            }
        )


class _Onboarding:
    def __init__(self, events=()) -> None:
        self.coordinator = _Coordinator(events)
        self.context = SimpleNamespace(
            credentials=SimpleNamespace(
                identity=SimpleNamespace(node_id="node-member")
            ),
            binding=SimpleNamespace(internal_session_id="session-one"),
            coordinator=self.coordinator,
        )
        self.remote_store = SimpleNamespace(load=lambda: SimpleNamespace())
        self.relay_runtime = _Runtime()

    def authorized_context(self):
        return self.context


def _scan_report_event():
    return SimpleNamespace(
        revision=4,
        event_type="recorder.control.scan.reported",
        actor_node_id="node-recorder",
        payload={
            "schema": "fcp.recorder-control.v1",
            "command": "scan",
            "request_id": "scan-request",
            "target_node_id": "node-recorder",
            "scan_id": "scan-current",
            "state": "complete",
            "cidr": "192.168.1.0/24",
            "port": 5000,
            "results": [
                {
                    "source_id": "source-new",
                    "source_name": "machine-new",
                    "display_name": "Machine new",
                    "machine_count": 1,
                }
            ],
            "configured_source_names": ["machine-old"],
            "message": "Scan complete",
            "error_code": None,
            "completed_at": "2026-08-10T21:30:00Z",
        },
    )


def test_non_owner_federation_member_can_request_recorder_scan(monkeypatch) -> None:
    onboarding = _Onboarding()
    monkeypatch.setattr(
        service_module,
        "get_capability_onboarding_service",
        lambda: onboarding,
    )

    request_id = FederationRecorderControlService().request_scan(
        "node-recorder",
        cidr="",
        port=5000,
    )

    assert request_id.startswith("recorder-scan-")
    [event] = onboarding.relay_runtime.appended
    assert event["event_type"] == "recorder.control.scan.requested"
    assert event["payload"]["target_node_id"] == "node-recorder"
    assert event["payload"]["cidr"] == ""
    assert "url" not in str(event["payload"]).casefold()


def test_member_can_add_only_source_from_latest_recorder_scan(monkeypatch) -> None:
    onboarding = _Onboarding(events=(_scan_report_event(),))
    monkeypatch.setattr(
        service_module,
        "get_capability_onboarding_service",
        lambda: onboarding,
    )
    service = FederationRecorderControlService()

    service.request_source_change(
        "node-recorder",
        scan_id="scan-current",
        add_source_ids=["source-new"],
        remove_source_names=["machine-old"],
    )

    [event] = onboarding.relay_runtime.appended
    assert event["event_type"] == "recorder.control.sources.requested"
    assert event["payload"]["add_source_ids"] == ["source-new"]
    assert event["payload"]["remove_source_names"] == ["machine-old"]


def test_member_cannot_inject_source_id_not_discovered_by_recorder(monkeypatch) -> None:
    onboarding = _Onboarding(events=(_scan_report_event(),))
    monkeypatch.setattr(
        service_module,
        "get_capability_onboarding_service",
        lambda: onboarding,
    )

    with pytest.raises(FederationRecorderControlError):
        FederationRecorderControlService().request_source_change(
            "node-recorder",
            scan_id="scan-current",
            add_source_ids=["source-attacker"],
            remove_source_names=[],
        )

    assert onboarding.relay_runtime.appended == []
