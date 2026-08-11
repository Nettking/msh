from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from catalog.federation.errors import AuthorizationError, FederationValidationError
from catalog.federation.projections.authority_adapter import FederationAuthorityAdapter
from catalog.flask_app.services import federation_device_names as naming

NOW = datetime(2026, 8, 11, 11, 30, tzinfo=timezone.utc)


class _Coordinator:
    def __init__(self, *, expose_creator: bool = True) -> None:
        self.events = [
            SimpleNamespace(
                revision=1,
                event_type="session.created",
                actor_node_id="node-leader",
                occurred_at=NOW,
                payload={"display_name": "Test federation"},
            ),
            SimpleNamespace(
                revision=2,
                event_type="node.joined",
                actor_node_id="node-remote",
                occurred_at=NOW,
                payload={"node_id": "node-remote"},
            ),
            SimpleNamespace(
                revision=3,
                event_type=naming.DEVICE_NAME_EVENT,
                actor_node_id="node-leader",
                occurred_at=NOW,
                payload={
                    "node_id": "node-remote",
                    "display_name": "Office laptop",
                },
            ),
        ]
        self.appended: list[dict[str, object]] = []
        self.expose_creator = expose_creator
        self.store = self

    def get_session(self, _session_id: str) -> object:
        values: dict[str, object] = {
            "display_name": "Test federation",
            "state": "active",
            "revision": len(self.events),
        }
        if self.expose_creator:
            values["created_by_node_id"] = "node-leader"
        return SimpleNamespace(**values)

    def status(self, *, actor_node_id: str, cursor: str | None = None) -> dict[str, object]:
        assert actor_node_id in {"node-leader", "node-remote"}
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
        assert actor_node_id in {"node-leader", "node-remote"}
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


class _ThreeNodeCoordinator(_Coordinator):
    def __init__(self, *, expose_creator: bool = True) -> None:
        super().__init__(expose_creator=expose_creator)
        self.events.append(
            SimpleNamespace(
                revision=4,
                event_type="node.joined",
                actor_node_id="node-other",
                occurred_at=NOW,
                payload={"node_id": "node-other"},
            )
        )

    def status(self, *, actor_node_id: str, cursor: str | None = None) -> dict[str, object]:
        value = super().status(actor_node_id=actor_node_id, cursor=cursor)
        value["nodes"].append(
            {
                "node_id": "node-other",
                "display_name": "Other computer",
                "connection_state": "disconnected",
            }
        )
        return value


class _ConcurrentClaimCoordinator(_ThreeNodeCoordinator):
    def append_event(self, **kwargs: object) -> tuple[object, bool]:
        competing = SimpleNamespace(
            revision=len(self.events) + 1,
            event_type=naming.DEVICE_NAME_EVENT,
            actor_node_id="node-other",
            occurred_at=NOW,
            payload={
                "node_id": "node-other",
                "display_name": kwargs["payload"]["display_name"],
            },
        )
        self.events.append(competing)
        return super().append_event(**kwargs)


def _context(coordinator: object, *, actor: str = "node-leader") -> object:
    return SimpleNamespace(
        binding=SimpleNamespace(
            internal_session_id="session-test",
            device_id=actor,
        ),
        credentials=SimpleNamespace(identity=SimpleNamespace(node_id=actor)),
        coordinator=coordinator,
    )


def test_remote_projection_recovers_creator_and_shares_leader_assigned_name() -> None:
    coordinator = _Coordinator(expose_creator=False)
    snapshot = FederationAuthorityAdapter(
        coordinator,
        actor_node_id="node-remote",
        internal_session_id="session-test",
    ).snapshot()

    assert snapshot.available is True
    by_id = {device.node_id: device for device in snapshot.devices}
    assert by_id["node-remote"].label == "Office laptop"
    assert by_id["node-remote"].state == "disconnected"


def test_projection_accepts_self_name_and_ignores_third_party_spoof() -> None:
    coordinator = _Coordinator()
    coordinator.events.extend(
        [
            SimpleNamespace(
                revision=4,
                event_type=naming.DEVICE_NAME_EVENT,
                actor_node_id="node-remote",
                occurred_at=NOW,
                payload={
                    "node_id": "node-remote",
                    "display_name": "Workshop laptop",
                },
            ),
            SimpleNamespace(
                revision=5,
                event_type=naming.DEVICE_NAME_EVENT,
                actor_node_id="node-other",
                occurred_at=NOW,
                payload={
                    "node_id": "node-remote",
                    "display_name": "Spoofed name",
                },
            ),
        ]
    )

    snapshot = FederationAuthorityAdapter(
        coordinator,
        actor_node_id="node-remote",
        internal_session_id="session-test",
    ).snapshot()

    assert snapshot.available is True
    by_id = {device.node_id: device for device in snapshot.devices}
    assert by_id["node-remote"].label == "Workshop laptop"


def test_concurrent_duplicate_name_claims_resolve_by_revision_order() -> None:
    coordinator = _ThreeNodeCoordinator()
    coordinator.events.extend(
        [
            SimpleNamespace(
                revision=5,
                event_type=naming.DEVICE_NAME_EVENT,
                actor_node_id="node-remote",
                occurred_at=NOW,
                payload={"node_id": "node-remote", "display_name": "Shared name"},
            ),
            SimpleNamespace(
                revision=6,
                event_type=naming.DEVICE_NAME_EVENT,
                actor_node_id="node-other",
                occurred_at=NOW,
                payload={"node_id": "node-other", "display_name": "shared NAME"},
            ),
        ]
    )

    snapshot = FederationAuthorityAdapter(
        coordinator,
        actor_node_id="node-remote",
        internal_session_id="session-test",
    ).snapshot()

    assert snapshot.available is True
    by_id = {device.node_id: device for device in snapshot.devices}
    assert by_id["node-remote"].label == "Shared name"
    assert by_id["node-other"].label == "Other computer"
    assert len({device.label.casefold() for device in snapshot.devices}) == len(snapshot.devices)


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


def test_concurrent_losing_claim_is_reported_as_duplicate(monkeypatch) -> None:
    coordinator = _ConcurrentClaimCoordinator()
    monkeypatch.setattr(
        naming,
        "get_capability_onboarding_service",
        lambda: SimpleNamespace(authorized_context=lambda: _context(coordinator)),
    )

    with pytest.raises(FederationValidationError) as exc_info:
        naming.FederationDeviceNamingService().rename(
            target_node_id="node-remote",
            display_name="Concurrent name",
        )

    assert exc_info.value.code == "duplicate-device-name"
    snapshot = FederationAuthorityAdapter(
        coordinator,
        actor_node_id="node-leader",
        internal_session_id="session-test",
    ).snapshot()
    by_id = {device.node_id: device for device in snapshot.devices}
    assert by_id["node-other"].label == "Concurrent name"
    assert by_id["node-remote"].label == "Office laptop"


class _RemoteRuntime:
    def __init__(self, coordinator: _Coordinator) -> None:
        self.coordinator = coordinator
        self.calls: list[dict[str, object]] = []

    def append_session_event(
        self,
        state: object,
        *,
        session_id: str,
        event_type: str,
        payload: dict[str, object],
        request_id: str,
    ) -> object:
        assert session_id == state.binding.internal_session_id
        call = {
            "session_id": session_id,
            "event_type": event_type,
            "payload": payload,
            "request_id": request_id,
        }
        self.calls.append(call)
        event = SimpleNamespace(
            revision=len(self.coordinator.events) + 1,
            event_type=event_type,
            actor_node_id=state.binding.device_id,
            occurred_at=NOW,
            payload=payload,
        )
        self.coordinator.events.append(event)
        return event


class _RemoteCoordinator(_Coordinator):
    def __init__(self) -> None:
        super().__init__(expose_creator=False)
        self.state = SimpleNamespace(
            binding=SimpleNamespace(
                device_id="node-remote",
                internal_session_id="session-test",
            )
        )
        self.runtime = _RemoteRuntime(self)
        # The real RemoteCoordinatorFacade intentionally has no append_event.
        self.append_event = None  # type: ignore[assignment]


def test_remote_member_uses_bounded_pairing_runtime_to_publish_own_name(monkeypatch) -> None:
    coordinator = _RemoteCoordinator()
    context = _context(coordinator, actor="node-remote")
    monkeypatch.setattr(
        naming,
        "get_capability_onboarding_service",
        lambda: SimpleNamespace(authorized_context=lambda: context),
    )

    result = naming.FederationDeviceNamingService().rename_self("Lab laptop")

    assert result == "Lab laptop"
    assert coordinator.runtime.calls == [
        {
            "session_id": "session-test",
            "event_type": naming.DEVICE_NAME_EVENT,
            "payload": {
                "node_id": "node-remote",
                "display_name": "Lab laptop",
            },
            "request_id": coordinator.runtime.calls[0]["request_id"],
        }
    ]
    assert str(coordinator.runtime.calls[0]["request_id"]).startswith("device-name-")


def test_nonleader_cannot_rename_another_member(monkeypatch) -> None:
    coordinator = _RemoteCoordinator()
    context = _context(coordinator, actor="node-remote")
    monkeypatch.setattr(
        naming,
        "get_capability_onboarding_service",
        lambda: SimpleNamespace(authorized_context=lambda: context),
    )

    with pytest.raises(AuthorizationError) as exc_info:
        naming.FederationDeviceNamingService().rename(
            target_node_id="node-leader",
            display_name="Not allowed",
        )

    assert exc_info.value.code == "federation-device-name-owner-required"
    assert not coordinator.runtime.calls


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
