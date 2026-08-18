from __future__ import annotations

from types import SimpleNamespace

import pytest

from catalog.federation.errors import AuthenticationError
from catalog.flask_app.services import trusted_storage_authority_runtime as runtime


def test_existing_creator_membership_mints_no_new_material() -> None:
    created: list[str] = []

    class Store:
        @staticmethod
        def get_node(node_id):
            assert node_id == "creator"
            return {"revoked_at": None}

        @staticmethod
        def require_membership(*, session_id, node_id):
            assert session_id == "session-a"
            assert node_id == "creator"

    coordinator = SimpleNamespace(
        store=Store(),
        create_enrollment_token=lambda **_kwargs: created.append("enroll"),
        create_invitation=lambda **_kwargs: created.append("invite"),
    )

    assert runtime._local_pairing_material(
        coordinator,
        node_id="creator",
        session_id="session-a",
    ) == (None, None)
    assert created == []


def test_missing_local_relay_state_uses_bounded_one_use_material() -> None:
    seen: list[tuple[str, dict[str, object]]] = []

    class Store:
        @staticmethod
        def get_node(_node_id):
            return None

        @staticmethod
        def require_membership(*, session_id, node_id):
            raise runtime.AuthorizationError(
                "not-session-member",
                "not joined",
                "session_id",
            )

    def enrollment(**kwargs):
        seen.append(("enroll", kwargs))
        return {"token": "fcp_enroll_private"}

    def invitation(**kwargs):
        seen.append(("invite", kwargs))
        return {"token": "fcp_invite_private"}

    coordinator = SimpleNamespace(
        store=Store(),
        create_enrollment_token=enrollment,
        create_invitation=invitation,
    )

    material = runtime._local_pairing_material(
        coordinator,
        node_id="creator",
        session_id="session-a",
    )

    assert material == ("fcp_enroll_private", "fcp_invite_private")
    assert seen[0] == (
        "enroll",
        {"ttl_seconds": 300, "max_uses": 1},
    )
    assert seen[1][0] == "invite"
    assert seen[1][1]["ttl_seconds"] == 300
    assert seen[1][1]["max_uses"] == 1
    assert seen[1][1]["session_id"] == "session-a"
    assert seen[1][1]["actor_node_id"] == "creator"


def test_revoked_creator_cannot_self_bootstrap() -> None:
    coordinator = SimpleNamespace(
        store=SimpleNamespace(
            get_node=lambda _node_id: {"revoked_at": "2026-08-18T00:00:00Z"}
        )
    )

    with pytest.raises(AuthenticationError) as caught:
        runtime._local_pairing_material(
            coordinator,
            node_id="creator",
            session_id="session-a",
        )

    assert caught.value.code == "revoked-node"
