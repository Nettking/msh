from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from catalog.federation.errors import FederationValidationError
from catalog.federation.projections.authority_adapter import FederationAuthorityAdapter
from catalog.flask_app.services import federation_device_names as naming

NOW = datetime(2026, 8, 11, 11, 30, tzinfo=timezone.utc)


class _Coordinator:
    def __init__(self) -> None:
        self.events = [
            SimpleNamespace(
                revision=1,
                event_type="node.joined",
                actor_node_id="node-leader",
                occurred_at=NOW,
                payload={"node_id": "node-remote"},
            ),
            SimpleNamespace(
                revision=2,
                event_type=naming.DEVICE_NAME_EVENT,
                actor_node_id="node-leader",
                occurred_at=NOW,
                payload={
                    "node_id": "node-remote",
                    "display_name": "Office laptop",
                },
            ),
            # A member cannot override the leader-owned label merely by writing a
            # similarly shaped event into shared history.
            SimpleNamespace(
                revision=3,
                event_type=naming.DEVICE_NAME_EVENT,
                actor_node_id="node-remote",
                occurred_at=NOW,
                payload={
                    "node_id": "node-remote",
                    "display_name": "Spoofed name",
                },
            ),
        ]
        self.appended: list[dict[str, object]] = []
        self.store = SimpleNamespace(
            get_session=lambda _session_id: SimpleNamespace(
                display_name="Test federation",
                state="active",
                revision=len(self.events),
                created_by_node_id="node-leader",
            )
        )

    def status(self, *, actor_node_id: str, cursor: str | None = None) -> dict[str, object]:
        assert actor_node_id == "node-leader"
        assert cursor is None
        return {
            "nodes": [
                {
                    "node_id": "node-leader",
                    "display_name": "Leader computer",
                    "connection_state": "connected",
                },
                {
                    "node_id": "node-remote",
                    "display_name": "This FCP device",
                    "connection_state": "disconnected",
                },
            ],
            "capabilities": [],
            "pagination": {"has_more": False, "next_cursor": None},
        }

    def replay_page(
        self,
        *,
        session_id: str,
        actor_node_id: str,
        last_applied_revision: int,
        limit: int,
    ) -> tuple[tuple[object, ...], int]:
        assert session_id == "session-test"
        assert actor_node_id == "node-leader"
        page = tuple(
            event
            for event in self.events
            if event.revision > last_applied_revision
        )[:limit]
        return page, len(self.events)

    def append_event(self, **kwargs: object) -> tuple[object, bool]:
        self.appended.append(dict(kwargs))
        event = SimpleNamespace(
            revision=len(self.events) + 1,
            event_type=kwargs["event_type"],
            actor_node_id=kwargs["actor_node_id"],
            occurred_at=NOW,
            payload=kwargs["payload"],
        )
        self.events.append(event)
        return event, True


def test_authority_projection_uses_only_creator_assigned_device_name() -> None:
    coordinator = _Coordinator()
    snapshot = FederationAuthorityAdapter(
        coordinator,
        actor_node_id="node-leader",
        internal_session_id="session-test",
    ).snapshot()

    assert snapshot.available is True
    by_id = {device.node_id: device for device in snapshot.devices}
    assert by_id["node-remote"].label == "Office laptop"
    assert by_id["node-remote"].state == "disconnected"
    renamed = [event for event in snapshot.activity if event.event_type == naming.DEVICE_NAME_EVENT]
    assert renamed
    assert renamed[0].title == "Device renamed"


def _context(coordinator: _Coordinator) -> object:
    return SimpleNamespace(
        binding=SimpleNamespace(internal_session_id="session-test"),
        credentials=SimpleNamespace(
            identity=SimpleNamespace(node_id="node-leader")
        ),
        coordinator=coordinator,
    )


def test_leader_can_rename_offline_member_and_event_is_durable(monkeypatch) -> None:
    coordinator = _Coordinator()
    monkeypatch.setattr(
        naming,
        "get_capability_onboarding_service",
        lambda: SimpleNamespace(authorized_context=lambda: _context(coordinator)),
    )

    result = naming.FederationDeviceNamingService().rename(
        target_node_id="node-remote",
        display_name="  CNC recorder PC  ",
    )

    assert result == "CNC recorder PC"
    assert coordinator.appended
    written = coordinator.appended[-1]
    assert written["event_type"] == naming.DEVICE_NAME_EVENT
    assert written["actor_node_id"] == "node-leader"
    assert written["payload"] == {
        "node_id": "node-remote",
        "display_name": "CNC recorder PC",
    }


def test_federation_device_names_are_unique(monkeypatch) -> None:
    coordinator = _Coordinator()
    monkeypatch.setattr(
        naming,
        "get_capability_onboarding_service",
        lambda: SimpleNamespace(authorized_context=lambda: _context(coordinator)),
    )

    with pytest.raises(FederationValidationError) as exc_info:
        naming.FederationDeviceNamingService().rename(
            target_node_id="node-remote",
            display_name="leader COMPUTER",
        )

    assert exc_info.value.code == "duplicate-device-name"
    assert not coordinator.appended


def test_reserved_generic_device_name_is_rejected(monkeypatch) -> None:
    coordinator = _Coordinator()
    monkeypatch.setattr(
        naming,
        "get_capability_onboarding_service",
        lambda: SimpleNamespace(authorized_context=lambda: _context(coordinator)),
    )

    with pytest.raises(FederationValidationError) as exc_info:
        naming.FederationDeviceNamingService().rename(
            target_node_id="node-remote",
            display_name="Trusted FCP device",
        )

    assert exc_info.value.code == "reserved-device-name"
