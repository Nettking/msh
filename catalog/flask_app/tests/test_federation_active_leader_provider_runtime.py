from __future__ import annotations

from types import SimpleNamespace

import pytest

from catalog.capabilities.operator_surface import ProviderOperatorSurface
from catalog.capabilities.provider_enrollment import FederatedProviderEnrollmentService
from catalog.federation.errors import AuthorizationError
from catalog.flask_app.services.federation_active_leader_provider_runtime import (
    install_active_leader_provider_runtime,
)

SESSION = "session-one"
CREATOR = "node-creator"
SUCCESSOR = "node-successor"


class _Store:
    def require_membership(self, *, session_id: str, node_id: str) -> None:
        assert session_id == SESSION
        assert node_id in {CREATOR, SUCCESSOR}

    def get_session(self, session_id: str) -> object:
        assert session_id == SESSION
        return SimpleNamespace(created_by_node_id=CREATOR)


class _Coordinator:
    def __init__(self) -> None:
        self.store = _Store()

    def require_session_leader(self, *, session_id: str, actor_node_id: str) -> object:
        assert session_id == SESSION
        if actor_node_id != SUCCESSOR:
            raise AuthorizationError(
                "federation-leader-required",
                "operation requires the current Federation leader",
                "actor_node_id",
            )
        return SimpleNamespace(leader_node_id=SUCCESSOR, creator_node_id=CREATOR, term=2)

    def session_leadership(self, session_id: str) -> object:
        assert session_id == SESSION
        return SimpleNamespace(leader_node_id=SUCCESSOR, creator_node_id=CREATOR, term=2)


def test_provider_enrollment_management_follows_successor_leader() -> None:
    install_active_leader_provider_runtime()
    service = object.__new__(FederatedProviderEnrollmentService)
    service.coordinator = _Coordinator()

    service._require_session_owner(
        session_id=SESSION,
        actor_node_id=SUCCESSOR,
    )

    with pytest.raises(AuthorizationError) as former_creator:
        service._require_session_owner(
            session_id=SESSION,
            actor_node_id=CREATOR,
        )
    assert former_creator.value.code == "provider-enrollment-not-authorized"


def test_provider_operator_actions_follow_successor_leader() -> None:
    install_active_leader_provider_runtime()
    coordinator = _Coordinator()
    announcements = (SimpleNamespace(capability_id="provider-one"),)
    enrollment = SimpleNamespace(
        coordinator=coordinator,
        discover=lambda **_kwargs: announcements,
    )

    former_creator = object.__new__(ProviderOperatorSurface)
    former_creator.enrollment = enrollment
    former_creator.session_id = SESSION
    former_creator.actor_node_id = CREATOR
    discovered, can_manage = former_creator._authorized_context()
    assert discovered == announcements
    assert can_manage is False

    successor = object.__new__(ProviderOperatorSurface)
    successor.enrollment = enrollment
    successor.session_id = SESSION
    successor.actor_node_id = SUCCESSOR
    discovered, can_manage = successor._authorized_context()
    assert discovered == announcements
    assert can_manage is True
